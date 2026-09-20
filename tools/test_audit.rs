#!/usr/bin/env -S cargo +nightly-2026-05-22 -q -Zscript
---
[package]
edition = "2024"

[dependencies]
proc-macro2 = { version = "=1.0.107", features = ["span-locations"] }
quote = "=1.0.47"
syn = { version = "=2.0.119", features = ["full", "visit"] }

[dev-dependencies]
tempfile = "=3.27.0"
---

use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};
use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::{ErrorKind, Write};
use std::mem::{swap, take};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio, exit};
use std::thread;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Attribute, Expr, ItemFn, ItemMod, Lit, Meta, Token};

const REPORT_NAMES: [&str; 2] = ["test-inventory.csv", "test-inventory.md"];
const CANDIDATE_REPORT_NAME: &str = "test-duplicate-candidates.md";
const MIN_BODY_TOKENS: usize = 30;
const CSV_HEADER: &str =
    "schema_version,file,line,test,source_conditions,purpose,expected,contract_status,issues\n";

#[derive(Debug, PartialEq, Eq)]
enum Selection {
    Inventory,
    Staged,
    Diff(String),
    Forced(Vec<PathBuf>),
}

#[derive(Debug)]
struct Args {
    selection: Selection,
    output_dir: PathBuf,
    analyze_duplicates: bool,
}

#[derive(Default)]
struct Snapshot {
    sources: BTreeMap<String, String>,
    selected: BTreeSet<String>,
    extra_sources: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Diagnostic {
    line: usize,
    rule: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestRecord {
    file: String,
    line: usize,
    test: String,
    conditions: Vec<String>,
    purpose: String,
    expected: String,
    diagnostics: Vec<Diagnostic>,
    body: Option<TestBody>,
}

impl TestRecord {
    fn status(&self) -> &'static str {
        if self.diagnostics.iter().any(|d| {
            !matches!(
                d.rule,
                "test-contract-missing-purpose" | "test-contract-missing-expected"
            )
        }) {
            "invalid"
        } else if self.diagnostics.is_empty() {
            "documented"
        } else {
            "missing"
        }
    }

    fn issues(&self) -> String {
        self.diagnostics
            .iter()
            .map(|d| d.rule)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join("; ")
    }
}

struct Collector<'a> {
    file: &'a str,
    path: Vec<String>,
    conditions: Vec<String>,
    records: Vec<TestRecord>,
    modules: Vec<ModuleIdentity>,
    analyze: bool,
}

impl<'ast> Visit<'ast> for Collector<'_> {
    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        let previous = self.conditions.len();
        self.conditions.extend(source_conditions(&item.attrs));
        self.path.push(item.ident.to_string());
        self.modules.push(ModuleIdentity {
            name: item.ident.to_string(),
            line: item.ident.span().start().line,
            column: item.ident.span().start().column + 1,
        });
        visit::visit_item_mod(self, item);
        self.modules.pop();
        self.path.pop();
        self.conditions.truncate(previous);
    }

    fn visit_item_fn(&mut self, item: &'ast ItemFn) {
        self.path.push(item.sig.ident.to_string());
        let previous = self.conditions.len();
        self.conditions.extend(source_conditions(&item.attrs));
        if item.attrs.iter().any(|attr| contains_test(&attr.meta)) {
            let mut conditions = self.conditions.clone();
            conditions.sort();
            conditions.dedup();
            let (purpose, expected, diagnostics) =
                contract(&item.attrs, item.sig.ident.span().start().line);
            self.records.push(TestRecord {
                file: self.file.to_string(),
                line: item.sig.ident.span().start().line,
                test: self.path.join("::"),
                conditions,
                purpose,
                expected,
                diagnostics,
                body: self.analyze.then(|| test_body(item, &self.modules)),
            });
        }
        visit::visit_item_fn(self, item);
        self.conditions.truncate(previous);
        self.path.pop();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ModuleIdentity {
    name: String,
    line: usize,
    column: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestBody {
    module: Vec<ModuleIdentity>,
    tokens: Vec<String>,
    attributes: Vec<String>,
    summary: BodySummary,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BodySummary {
    calls: BTreeSet<String>,
    assertions: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for BodySummary {
    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        self.calls
            .insert(canonical_tokens(node.func.to_token_stream()).join(" "));
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        self.calls.insert(format!(".{}", node.method));
        visit::visit_expr_method_call(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if node.path.segments.last().is_some_and(|segment| {
            let name = segment.ident.to_string();
            name.starts_with("assert") || name.starts_with("debug_assert")
        }) {
            self.assertions
                .insert(canonical_tokens(node.to_token_stream()).join(" "));
        }
        // Macro arguments stay opaque: this is a syntactic summary, not expansion.
    }

    fn visit_item(&mut self, _node: &'ast syn::Item) {
        // Calls inside a nested item do not belong to the enclosing test's flow.
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MatchingBlock {
    left: usize,
    right: usize,
    len: usize,
}

#[derive(Clone, Copy)]
struct AnalyzedTest<'a> {
    record: &'a TestRecord,
    body: &'a TestBody,
}

struct DuplicateCandidate<'a> {
    left: AnalyzedTest<'a>,
    right: AnalyzedTest<'a>,
    blocks: Vec<MatchingBlock>,
    matched: usize,
}

#[derive(Default)]
struct DuplicateAnalysis<'a> {
    test_count: usize,
    short_count: usize,
    pair_count: usize,
    candidates: Vec<DuplicateCandidate<'a>>,
}

fn main() {
    let result = run();
    exit(match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("test-audit: {err}");
            2
        }
    });
}

fn usage() -> &'static str {
    "Usage: tools/test_audit.rs inventory [--analyze-duplicates] [--output-dir <dir>]\n       tools/test_audit.rs check (--staged | --diff-base <rev> | --force-path <file-or-dir> ...) [--analyze-duplicates] [--output-dir <dir>]"
}

fn run() -> Result<i32, String> {
    let raw = env::args().skip(1).collect::<Vec<_>>();
    if raw == ["--help"] || raw == ["-h"] {
        println!("{}", usage());
        return Ok(0);
    }
    let args = parse_args(raw)?;
    let root = git(Path::new("."), &["rev-parse", "--show-toplevel"])?;
    let root = PathBuf::from(root.trim_end_matches('\n'));
    run_audit(&root, &args)
}

fn parse_args(raw: Vec<String>) -> Result<Args, String> {
    let mut args = raw.into_iter();
    let command = args.next().ok_or_else(|| usage().to_string())?;
    if command != "inventory" && command != "check" {
        return Err(usage().to_string());
    }
    let mut selection = None;
    let mut output_dir = None;
    let mut analyze_duplicates = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--analyze-duplicates" if !analyze_duplicates => analyze_duplicates = true,
            "--output-dir" => {
                let path = args.next().ok_or("missing --output-dir value")?;
                if output_dir.replace(PathBuf::from(path)).is_some() {
                    return Err("duplicate --output-dir".into());
                }
            }
            "--staged" if selection.is_none() => selection = Some(Selection::Staged),
            "--diff-base" if selection.is_none() => {
                selection = Some(Selection::Diff(
                    args.next().ok_or("missing --diff-base value")?,
                ));
            }
            "--force-path" => {
                let path = PathBuf::from(args.next().ok_or("missing --force-path value")?);
                match &mut selection {
                    None => selection = Some(Selection::Forced(vec![path])),
                    Some(Selection::Forced(paths)) => paths.push(path),
                    _ => return Err("mixed selectors are not allowed".into()),
                }
            }
            _ => {
                return Err(format!(
                    "unknown, duplicate, or mixed selector: {arg}\n{}",
                    usage()
                ));
            }
        }
    }
    let selection = match (command.as_str(), selection) {
        ("inventory", None) => Selection::Inventory,
        ("check", Some(selection)) => selection,
        _ => {
            return Err(format!(
                "inventory takes no selector; check requires one selector\n{}",
                usage()
            ));
        }
    };
    Ok(Args {
        selection,
        output_dir: output_dir.unwrap_or_else(|| PathBuf::from("target/test-audit")),
        analyze_duplicates,
    })
}

fn run_audit(root: &Path, args: &Args) -> Result<i32, String> {
    let output_dir = root.join(&args.output_dir);
    // Invalidate only our derived reports before extraction. A failed parse must
    // never leave an older inventory looking like this invocation's result.
    for name in REPORT_NAMES.into_iter().chain([CANDIDATE_REPORT_NAME]) {
        match fs::remove_file(output_dir.join(name)) {
            Ok(()) => (),
            Err(err) if err.kind() == ErrorKind::NotFound => (),
            Err(err) => return Err(format!("cannot clear {name}: {err}")),
        }
    }
    let snapshot = collect_snapshot(root, &args.selection)?;
    let analyze_files = if !args.analyze_duplicates {
        BTreeSet::new()
    } else if args.selection == Selection::Inventory {
        snapshot.sources.keys().cloned().collect()
    } else {
        snapshot.selected.clone()
    };
    let mut records = extract_sources(&snapshot.sources, &analyze_files)?;
    sort_records(&mut records);
    let extras = extract_sources(&snapshot.extra_sources, &analyze_files)?;
    let selected = records
        .iter()
        .chain(&extras)
        .filter(|record| snapshot.selected.contains(&record.file))
        .collect::<Vec<_>>();
    let mut failures = 0;
    for record in &selected {
        for diagnostic in &record.diagnostics {
            failures += 1;
            eprintln!(
                "{}:{} {} - {}",
                record.file, diagnostic.line, diagnostic.rule, record.test
            );
        }
    }
    let (csv, markdown) = render_reports(snapshot.sources.len(), &records);
    fs::create_dir_all(&output_dir).map_err(|err| format!("create report directory: {err}"))?;
    for (name, contents) in REPORT_NAMES.into_iter().zip([csv, markdown]) {
        fs::write(output_dir.join(name), contents).map_err(|err| format!("write {name}: {err}"))?;
    }
    if args.analyze_duplicates {
        let analysis = analyze_duplicates(records.iter().chain(&extras));
        fs::write(
            output_dir.join(CANDIDATE_REPORT_NAME),
            render_candidates(&analysis),
        )
        .map_err(|err| format!("write {CANDIDATE_REPORT_NAME}: {err}"))?;
        println!(
            "test-audit: duplicate analysis: {} tests; {} short bodies excluded; {} pairs; {} candidates",
            analysis.test_count,
            analysis.short_count,
            analysis.pair_count,
            analysis.candidates.len()
        );
    }
    println!(
        "test-audit: inventory: {} files, {} tests; selected: {} files, {} tests; {failures} violations",
        snapshot.sources.len(),
        records.len(),
        snapshot.selected.len(),
        selected.len()
    );
    println!("test-audit: reports: {}", output_dir.display());
    Ok(i32::from(failures != 0))
}

fn git(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .stdin(Stdio::null())
        .output()
        .map_err(|err| format!("execute git: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout).map_err(|err| format!("Git output is not UTF-8: {err}"))
}

fn index_entries(root: &Path) -> Result<BTreeMap<String, String>, String> {
    let index = git(root, &["ls-files", "--stage", "-z"])?;
    let mut entries = BTreeMap::new();
    for entry in index.split('\0').filter(|entry| !entry.is_empty()) {
        let (metadata, path) = entry.split_once('\t').ok_or("invalid Git index entry")?;
        let fields = metadata.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 3 || fields[2] != "0" {
            return Err("unmerged or invalid Git index; resolve conflicts before auditing".into());
        }
        // Tool scripts are opt-in forced targets, never part of the inventory.
        if path.ends_with(".rs") && !path.starts_with("tools/") {
            entries.insert(path.to_string(), fields[1].to_string());
        }
    }
    Ok(entries)
}

fn index_sources(
    root: &Path,
    entries: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, String> {
    let mut child = Command::new("git")
        .args(["cat-file", "--batch"])
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("execute git cat-file: {err}"))?;
    let mut input = child.stdin.take().ok_or("missing Git stdin")?;
    let requests = entries
        .values()
        .map(|id| format!("{id}\n"))
        .collect::<String>();
    // Feed requests concurrently with reading stdout to avoid pipe deadlocks
    // for large repositories. Request order is the sorted repository path order.
    let output = thread::scope(|scope| {
        let writer = scope.spawn(move || input.write_all(requests.as_bytes()));
        let output = child
            .wait_with_output()
            .map_err(|err| format!("read Git blobs: {err}"))?;
        writer
            .join()
            .map_err(|_| "Git input writer panicked")?
            .map_err(|err| format!("write Git blob requests: {err}"))?;
        Ok::<_, String>(output)
    })?;
    if !output.status.success() {
        return Err(format!(
            "read Git blobs: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let mut remaining = output.stdout.as_slice();
    let mut sources = BTreeMap::new();
    for path in entries.keys() {
        let end = remaining
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or("missing Git blob header")?;
        let header = String::from_utf8_lossy(&remaining[..end]);
        let fields = header.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 3 || fields[1] != "blob" {
            return Err(format!("invalid Git blob for {path}: {header}"));
        }
        let size = fields[2]
            .parse::<usize>()
            .map_err(|err| format!("invalid blob size: {err}"))?;
        remaining = &remaining[end + 1..];
        if remaining.len() <= size || remaining[size] != b'\n' {
            return Err(format!("truncated Git blob for {path}"));
        }
        let source = String::from_utf8(remaining[..size].to_vec())
            .map_err(|err| format!("{path}: {err}"))?;
        sources.insert(path.clone(), source);
        remaining = &remaining[size + 1..];
    }
    Ok(sources)
}

fn collect_snapshot(root: &Path, selection: &Selection) -> Result<Snapshot, String> {
    let entries = index_entries(root)?;
    let mut snapshot = Snapshot::default();
    if *selection == Selection::Staged {
        snapshot.sources = index_sources(root, &entries)?;
    } else {
        for path in entries.keys() {
            match fs::read_to_string(root.join(path)) {
                Ok(source) => {
                    snapshot.sources.insert(path.clone(), source);
                }
                Err(err) if err.kind() == ErrorKind::NotFound => (),
                Err(err) => return Err(format!("{path}: {err}")),
            }
        }
    }
    let changed = match selection {
        Selection::Inventory => return Ok(snapshot),
        Selection::Staged => git(
            root,
            &[
                "diff",
                "--cached",
                "--name-only",
                "--diff-filter=ACMRT",
                "-z",
                "--",
                "*.rs",
            ],
        )?,
        Selection::Diff(base) => {
            let commit = git(
                root,
                &[
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    &format!("{base}^{{commit}}"),
                ],
            )?;
            git(
                root,
                &[
                    "diff",
                    "--name-only",
                    "--diff-filter=ACMRT",
                    "-z",
                    commit.trim(),
                    "--",
                    "*.rs",
                ],
            )?
        }
        Selection::Forced(paths) => {
            for path in forced_files(root, paths)? {
                let name = path
                    .strip_prefix(root)
                    .unwrap_or(&path)
                    .to_str()
                    .ok_or("non-UTF-8 forced path")?
                    .to_string();
                if !snapshot.sources.contains_key(&name) {
                    let source =
                        fs::read_to_string(&path).map_err(|err| format!("{name}: {err}"))?;
                    snapshot.extra_sources.insert(name.clone(), source);
                }
                snapshot.selected.insert(name);
            }
            return Ok(snapshot);
        }
    };
    snapshot.selected = changed
        .split('\0')
        .filter(|path| snapshot.sources.contains_key(*path))
        .map(str::to_string)
        .collect();
    Ok(snapshot)
}

fn forced_files(root: &Path, paths: &[PathBuf]) -> Result<BTreeSet<PathBuf>, String> {
    let mut selected = BTreeSet::new();
    for path in paths {
        let full = root
            .join(path)
            .canonicalize()
            .map_err(|err| format!("--force-path {}: {err}", path.display()))?;
        let candidates = if full.is_dir() {
            fs::read_dir(&full)
                .map_err(|err| format!("{}: {err}", full.display()))?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| format!("{}: {err}", full.display()))?
        } else if full.is_file() {
            vec![full]
        } else {
            return Err(format!("not a file or directory: {}", full.display()));
        };
        for candidate in candidates {
            if candidate.is_file() && candidate.extension().is_some_and(|ext| ext == "rs") {
                selected.insert(
                    candidate
                        .canonicalize()
                        .map_err(|err| format!("{}: {err}", candidate.display()))?,
                );
            }
        }
    }
    Ok(selected)
}

fn parseable_source(source: &str) -> Result<String, String> {
    let mut lines = source
        .split_inclusive('\n')
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut index = 0;
    if lines
        .first()
        .is_some_and(|line| line.starts_with("#!") && !line.starts_with("#!["))
    {
        lines[0] = "\n".into();
        index = 1;
    }
    if lines.get(index).is_some_and(|line| line.trim() == "---") {
        lines[index] = "\n".into();
        index += 1;
        let mut closed = false;
        while index < lines.len() {
            closed = lines[index].trim() == "---";
            lines[index] = "\n".into();
            index += 1;
            if closed {
                break;
            }
        }
        if !closed {
            return Err("unterminated embedded manifest".into());
        }
    }
    Ok(lines.concat())
}

fn extract_sources(
    sources: &BTreeMap<String, String>,
    analyze_files: &BTreeSet<String>,
) -> Result<Vec<TestRecord>, String> {
    let mut records = Vec::new();
    for (file, source) in sources {
        records.extend(extract(file, source, analyze_files.contains(file))?);
    }
    Ok(records)
}

fn extract(file: &str, source: &str, analyze: bool) -> Result<Vec<TestRecord>, String> {
    let source = parseable_source(source).map_err(|err| format!("{file}: {err}"))?;
    let syntax = syn::parse_file(&source)
        .map_err(|err| format!("{file}:{}: parse: {err}", err.span().start().line))?;
    let mut collector = Collector {
        file,
        path: Vec::new(),
        conditions: source_conditions(&syntax.attrs),
        records: Vec::new(),
        modules: Vec::new(),
        analyze,
    };
    collector.visit_file(&syntax);
    Ok(collector.records)
}

fn canonical_tokens(stream: TokenStream) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut punctuation = String::new();
    for token in stream {
        if let TokenTree::Punct(punct) = token {
            punctuation.push(punct.as_char());
            if punct.spacing() == Spacing::Alone {
                push_punctuation(&mut tokens, &mut punctuation);
            }
            continue;
        }
        if !punctuation.is_empty() {
            push_punctuation(&mut tokens, &mut punctuation);
        }
        match token {
            TokenTree::Group(group) => {
                let delimiters = match group.delimiter() {
                    Delimiter::Parenthesis => Some(("(", ")")),
                    Delimiter::Brace => Some(("{", "}")),
                    Delimiter::Bracket => Some(("[", "]")),
                    Delimiter::None => None,
                };
                if let Some((open, _)) = delimiters {
                    tokens.push(open.into());
                }
                tokens.extend(canonical_tokens(group.stream()));
                if let Some((_, close)) = delimiters {
                    tokens.push(close.into());
                }
            }
            token => tokens.push(token.to_string()),
        }
    }
    if !punctuation.is_empty() {
        push_punctuation(&mut tokens, &mut punctuation);
    }
    tokens
}

fn push_punctuation(tokens: &mut Vec<String>, punctuation: &mut String) {
    // Adjacent independent operators such as &* must not gain a different
    // fingerprint merely because whitespace is inserted between them in a macro.
    let run = take(punctuation);
    let mut remaining = run.as_str();
    while !remaining.is_empty() {
        let len = [
            "<<=", ">>=", "..=", "...", "::", "->", "=>", "..", "==", "!=", "<=", ">=", "&&", "||",
            "<<", ">>", "+=", "-=", "*=", "/=", "%=", "^=", "&=", "|=",
        ]
        .into_iter()
        .find(|operator| remaining.starts_with(operator))
        .map_or(1, str::len);
        tokens.push(remaining[..len].into());
        remaining = &remaining[len..];
    }
}

fn test_body(item: &ItemFn, module: &[ModuleIdentity]) -> TestBody {
    let mut summary = BodySummary::default();
    summary.visit_block(&item.block);
    let stream = item
        .block
        .stmts
        .iter()
        .flat_map(ToTokens::to_token_stream)
        .collect();
    let mut attributes = item
        .attrs
        .iter()
        .filter(|attr| !attr.path().is_ident("doc") && !attr.path().is_ident("test"))
        .map(|attr| attr.meta.to_token_stream().to_string())
        .collect::<Vec<_>>();
    attributes.sort();
    attributes.dedup();
    TestBody {
        module: module.to_vec(),
        tokens: canonical_tokens(stream),
        attributes,
        summary,
    }
}

fn longest_match(
    left: &[String],
    right_positions: &BTreeMap<&str, Vec<usize>>,
    left_range: Range<usize>,
    right_range: Range<usize>,
) -> MatchingBlock {
    let mut best = MatchingBlock {
        left: left_range.start,
        right: right_range.start,
        len: 0,
    };
    let mut previous = vec![0; right_range.len() + 1];
    let mut current = previous.clone();
    for i in left_range {
        current.fill(0);
        if let Some(positions) = right_positions.get(left[i].as_str()) {
            let start = positions.partition_point(|position| *position < right_range.start);
            for &j in &positions[start..] {
                if j >= right_range.end {
                    break;
                }
                let offset = j - right_range.start;
                let len = previous[offset] + 1;
                current[offset + 1] = len;
                // Ascending positions plus strict improvement select the earliest
                // left start, then the earliest right start when lengths tie.
                if len > best.len {
                    best = MatchingBlock {
                        left: i + 1 - len,
                        right: j + 1 - len,
                        len,
                    };
                }
            }
        }
        swap(&mut previous, &mut current);
    }
    best
}

fn matching_blocks(left: &[String], right: &[String]) -> Vec<MatchingBlock> {
    let mut positions: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, token) in right.iter().enumerate() {
        positions.entry(token).or_default().push(index);
    }
    // Use a work stack rather than recursion for bodies with many small matches.
    let mut pending = vec![(0..left.len(), 0..right.len())];
    let mut blocks = Vec::new();
    while let Some((left_range, right_range)) = pending.pop() {
        let block = longest_match(left, &positions, left_range.clone(), right_range.clone());
        if block.len == 0 {
            continue;
        }
        if left_range.start < block.left && right_range.start < block.right {
            pending.push((left_range.start..block.left, right_range.start..block.right));
        }
        let left_end = block.left + block.len;
        let right_end = block.right + block.len;
        if left_end < left_range.end && right_end < right_range.end {
            pending.push((left_end..left_range.end, right_end..right_range.end));
        }
        blocks.push(block);
    }
    blocks.sort();
    blocks
}

fn candidate_signals(matched: usize, left: usize, right: usize) -> (bool, bool) {
    if left.min(right) < MIN_BODY_TOKENS {
        return (false, false);
    }
    let matched = matched as u128;
    (
        200 * matched >= 85 * (left as u128 + right as u128),
        100 * matched >= 95 * left.min(right) as u128,
    )
}

fn analyze_duplicates<'a>(
    records: impl IntoIterator<Item = &'a TestRecord>,
) -> DuplicateAnalysis<'a> {
    let mut analysis = DuplicateAnalysis::default();
    let mut modules: BTreeMap<_, Vec<AnalyzedTest<'_>>> = BTreeMap::new();
    for record in records {
        let Some(body) = &record.body else { continue };
        analysis.test_count += 1;
        if body.tokens.len() < MIN_BODY_TOKENS {
            analysis.short_count += 1;
            continue;
        }
        modules
            .entry((&record.file, &body.module))
            .or_default()
            .push(AnalyzedTest { record, body });
    }
    for tests in modules.values_mut() {
        tests.sort_by_key(|test| (&test.record.test, &test.record.conditions, test.record.line));
        for (index, &left) in tests.iter().enumerate() {
            for &right in &tests[index + 1..] {
                analysis.pair_count += 1;
                let blocks = matching_blocks(&left.body.tokens, &right.body.tokens);
                let matched = blocks.iter().map(|block| block.len).sum();
                let signals =
                    candidate_signals(matched, left.body.tokens.len(), right.body.tokens.len());
                if signals.0 || signals.1 {
                    analysis.candidates.push(DuplicateCandidate {
                        left,
                        right,
                        blocks,
                        matched,
                    });
                }
            }
        }
    }
    analysis
}

fn percentage(numerator: usize, denominator: usize) -> String {
    let basis_points = numerator as u128 * 10_000 / denominator as u128;
    format!("{}.{:02}%", basis_points / 100, basis_points % 100)
}

fn evidence_values(values: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    let values = values
        .into_iter()
        .map(|value| markdown_text(value.as_ref()))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.is_empty() {
        "(none)".into()
    } else {
        values.join("; ")
    }
}

fn render_token_differences(candidate: &DuplicateCandidate<'_>, output: &mut String) {
    let left = &candidate.left.body.tokens;
    let right = &candidate.right.body.tokens;
    let mut previous = (0, 0);
    let mut different = false;
    for block in candidate.blocks.iter().copied().chain([MatchingBlock {
        left: left.len(),
        right: right.len(),
        len: 0,
    }]) {
        if previous != (block.left, block.right) {
            different = true;
            output.push_str(&format!(
                "- Left tokens {}..{}: {}\n- Right tokens {}..{}: {}\n",
                previous.0,
                block.left,
                evidence_values([left[previous.0..block.left].join(" ")]),
                previous.1,
                block.right,
                evidence_values([right[previous.1..block.right].join(" ")]),
            ));
        }
        previous = (block.left + block.len, block.right + block.len);
    }
    if !different {
        output.push_str("Identical body tokens.\n");
    }
}

fn render_candidates(analysis: &DuplicateAnalysis<'_>) -> String {
    let mut output = format!(
        "# Test duplicate candidates\n\nAnalyzed tests: {}; short bodies excluded: {}; compared pairs: {}; candidates: {}.\n\n",
        analysis.test_count,
        analysis.short_count,
        analysis.pair_count,
        analysis.candidates.len()
    );
    output.push_str("Informational structural evidence, not semantic equivalence or permission to delete tests. Compare only within the same source file and module declaration. Bodies under 30 tokens are excluded. Matching is ordered and literal-preserving: body similarity >=85% or shorter-body overlap >=95%. Scores are not probabilities.\n\nCalls are syntactic paths/method names outside nested items; macros and helpers are not expanded or resolved. Assertion-like macro text is preserved. Token ranges below are zero-based, end-exclusive.\n");
    let mut previous_module = None;
    for candidate in &analysis.candidates {
        let left = candidate.left;
        let right = candidate.right;
        let module = (&left.record.file, &left.body.module);
        if previous_module != Some(module) {
            let path = if module.1.is_empty() {
                "(file root)".into()
            } else {
                module
                    .1
                    .iter()
                    .map(|part| format!("{}@{}:{}", part.name, part.line, part.column))
                    .collect::<Vec<_>>()
                    .join("::")
            };
            output.push_str(&format!(
                "\n## {} — {}\n",
                markdown_text(module.0),
                markdown_text(&path)
            ));
            previous_module = Some(module);
        }
        output.push_str(&format!(
            "\n### {} ↔ {}\n\n",
            markdown_text(&left.record.test),
            markdown_text(&right.record.test)
        ));
        let signals = candidate_signals(
            candidate.matched,
            left.body.tokens.len(),
            right.body.tokens.len(),
        );
        output.push_str(&format!(
            "Body similarity: {}; shorter-body overlap: {}. Signals: {}.\n\n| Evidence | Left | Right |\n| --- | --- | --- |\n",
            percentage(2 * candidate.matched, left.body.tokens.len() + right.body.tokens.len()),
            percentage(candidate.matched, left.body.tokens.len().min(right.body.tokens.len())),
            match signals { (true, true) => "body similarity, shorter-body overlap", (true, false) => "body similarity", _ => "shorter-body overlap" }
        ));
        for (label, a, b) in [
            (
                "Location",
                format!("{}:{}", left.record.file, left.record.line),
                format!("{}:{}", right.record.file, right.record.line),
            ),
            (
                "Purpose",
                left.record.purpose.clone(),
                right.record.purpose.clone(),
            ),
            (
                "Expected",
                left.record.expected.clone(),
                right.record.expected.clone(),
            ),
            (
                "Contract status",
                left.record.status().into(),
                right.record.status().into(),
            ),
        ] {
            output.push_str(&format!(
                "| {label} | {} | {} |\n",
                markdown_text(&a),
                markdown_text(&b)
            ));
        }
        for (label, a, b) in [
            (
                "Conditions",
                evidence_values(&left.record.conditions),
                evidence_values(&right.record.conditions),
            ),
            (
                "Attributes",
                evidence_values(&left.body.attributes),
                evidence_values(&right.body.attributes),
            ),
            (
                "Only these calls",
                evidence_values(
                    left.body
                        .summary
                        .calls
                        .difference(&right.body.summary.calls),
                ),
                evidence_values(
                    right
                        .body
                        .summary
                        .calls
                        .difference(&left.body.summary.calls),
                ),
            ),
            (
                "Only these assertions",
                evidence_values(
                    left.body
                        .summary
                        .assertions
                        .difference(&right.body.summary.assertions),
                ),
                evidence_values(
                    right
                        .body
                        .summary
                        .assertions
                        .difference(&left.body.summary.assertions),
                ),
            ),
        ] {
            output.push_str(&format!("| {label} | {a} | {b} |\n"));
        }
        output.push_str(&format!(
            "\nShared syntactic calls: {}.\n\nDiffering token excerpts:\n\n",
            evidence_values(
                left.body
                    .summary
                    .calls
                    .intersection(&right.body.summary.calls)
            )
        ));
        render_token_differences(candidate, &mut output);
    }
    if analysis.candidates.is_empty() {
        output.push_str("\nNone.\n");
    }
    output
}

fn contains_test(meta: &Meta) -> bool {
    if matches!(meta, Meta::Path(path) if path.is_ident("test")) {
        return true;
    }
    if let Meta::List(list) = meta
        && list.path.is_ident("cfg_attr")
        && let Ok(nested) = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
    {
        return nested.iter().skip(1).any(contains_test);
    }
    false
}

fn source_conditions(attrs: &[Attribute]) -> Vec<String> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"))
        .map(|attr| attr.meta.to_token_stream().to_string())
        .collect()
}

fn contract(attrs: &[Attribute], declaration_line: usize) -> (String, String, Vec<Diagnostic>) {
    let mut fields: [Vec<(usize, String)>; 2] = [Vec::new(), Vec::new()];
    let mut current = None;
    let mut after_attribute = false;
    let mut diagnostics = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("doc") {
            after_attribute = true;
            continue;
        }
        let line = attr.span().start().line;
        if after_attribute {
            diagnostics.push(Diagnostic {
                line,
                rule: "test-contract-doc-placement",
            });
        }
        let value = match &attr.meta {
            Meta::NameValue(value) => match &value.value {
                Expr::Lit(value) => match &value.lit {
                    Lit::Str(value) => Some(value.value()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };
        let Some(value) = value else {
            diagnostics.push(Diagnostic {
                line,
                rule: "test-contract-nonliteral-doc",
            });
            continue;
        };
        for (offset, text) in value.lines().enumerate() {
            let text = text.trim();
            let label = text
                .strip_prefix("Purpose:")
                .map(|rest| (0, rest))
                .or_else(|| text.strip_prefix("Expected:").map(|rest| (1, rest)));
            if let Some((field, rest)) = label {
                let location = line + offset.min(attr.span().end().line - line);
                fields[field].push((location, rest.to_string()));
                current = Some(field);
            } else if let Some(field) = current
                && let Some((_, value)) = fields[field].last_mut()
            {
                value.push(' ');
                value.push_str(text);
            }
        }
    }
    let rules = [
        (
            "test-contract-missing-purpose",
            "test-contract-empty-purpose",
            "test-contract-duplicate-purpose",
        ),
        (
            "test-contract-missing-expected",
            "test-contract-empty-expected",
            "test-contract-duplicate-expected",
        ),
    ];
    let mut values = [String::new(), String::new()];
    for (field, occurrences) in fields.iter_mut().enumerate() {
        let (missing, empty, duplicate) = rules[field];
        if occurrences.is_empty() {
            diagnostics.push(Diagnostic {
                line: declaration_line,
                rule: missing,
            });
        }
        for (index, (line, value)) in occurrences.iter_mut().enumerate() {
            *value = value.split_whitespace().collect::<Vec<_>>().join(" ");
            if value.is_empty() {
                diagnostics.push(Diagnostic {
                    line: *line,
                    rule: empty,
                });
            }
            if index > 0 {
                diagnostics.push(Diagnostic {
                    line: *line,
                    rule: duplicate,
                });
            }
        }
        if let Some((_, value)) = occurrences.first() {
            values[field] = value.clone();
        }
    }
    diagnostics.sort();
    diagnostics.dedup();
    let [purpose, expected] = values;
    (purpose, expected, diagnostics)
}

fn sort_records(records: &mut [TestRecord]) {
    records.sort_by(|a, b| {
        (&a.file, &a.test, &a.conditions, a.line).cmp(&(&b.file, &b.test, &b.conditions, b.line))
    });
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\r', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn markdown_text(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '|' => escaped.push_str("&#124;"),
            '\n' => escaped.push_str("&#10;"),
            '\r' => escaped.push_str("&#13;"),
            '\\' | '`' | '*' | '_' | '[' | ']' | '#' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn duplicate_groups(records: &[TestRecord]) -> BTreeMap<(&str, &str), Vec<&TestRecord>> {
    let mut groups: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for record in records
        .iter()
        .filter(|record| record.status() == "documented")
    {
        groups
            .entry((record.purpose.as_str(), record.expected.as_str()))
            .or_default()
            .push(record);
    }
    groups.retain(|_, members| members.len() > 1);
    for members in groups.values_mut() {
        members.sort_by(|a, b| {
            (&a.file, &a.test, &a.conditions, a.line).cmp(&(
                &b.file,
                &b.test,
                &b.conditions,
                b.line,
            ))
        });
    }
    groups
}

fn render_reports(file_count: usize, records: &[TestRecord]) -> (String, String) {
    let mut sorted = records.to_vec();
    sort_records(&mut sorted);
    let records = &sorted;
    let mut csv = CSV_HEADER.to_string();
    for record in records {
        let row = [
            "1".into(),
            record.file.clone(),
            record.line.to_string(),
            record.test.clone(),
            record.conditions.join("; "),
            record.purpose.clone(),
            record.expected.clone(),
            record.status().into(),
            record.issues(),
        ];
        csv.push_str(
            &row.iter()
                .map(|value| csv_field(value))
                .collect::<Vec<_>>()
                .join(","),
        );
        csv.push('\n');
    }
    let count = |status| {
        records
            .iter()
            .filter(|record| record.status() == status)
            .count()
    };
    let mut markdown = format!(
        "# Test inventory\n\nSource files: {file_count}; tests: {}; documented: {}; missing: {}; invalid: {}.\n\n",
        records.len(),
        count("documented"),
        count("missing"),
        count("invalid")
    );
    markdown.push_str("Source declarations only: conditions are unevaluated and source-local. External module conditions and macro-generated tests are not inferred. Documented means structurally valid, not semantically verified.\n");
    let mut previous = None;
    for record in records {
        if previous != Some(&record.file) {
            markdown.push_str(&format!("\n## {}\n\n| Location | Test | Source conditions | Purpose | Expected | Status | Issues |\n| --- | --- | --- | --- | --- | --- | --- |\n", markdown_text(&record.file)));
            previous = Some(&record.file);
        }
        let row = [
            format!("{}:{}", record.file, record.line),
            record.test.clone(),
            record.conditions.join("; "),
            record.purpose.clone(),
            record.expected.clone(),
            record.status().into(),
            record.issues(),
        ];
        markdown.push_str(&format!(
            "| {} |\n",
            row.iter()
                .map(|value| markdown_text(value))
                .collect::<Vec<_>>()
                .join(" | ")
        ));
    }
    markdown.push_str("\n## Duplicate-contract candidates\n\nExact normalized Purpose/Expected matches are review prompts, not proof of redundancy or permission to delete tests.\n");
    let groups = duplicate_groups(records);
    if groups.is_empty() {
        markdown.push_str("\nNone.\n");
    }
    for ((purpose, expected), members) in groups {
        markdown.push_str(&format!(
            "\nPurpose: {}\n\nExpected: {}\n\n",
            markdown_text(purpose),
            markdown_text(expected)
        ));
        for member in members {
            let conditions = if member.conditions.is_empty() {
                "(none)".to_string()
            } else {
                markdown_text(&member.conditions.join("; "))
            };
            markdown.push_str(&format!(
                "- {}:{} — {}; conditions: {}\n",
                markdown_text(&member.file),
                member.line,
                markdown_text(&member.test),
                conditions
            ));
        }
    }
    (csv, markdown)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    const VALID: &str = "/// Purpose: Protect a boundary.\n/// Expected: Value equals 7.\n#[test]\nfn case() { assert_eq!(7, 7); }\n";

    const ANALYSIS_SOURCE: &str = r#"
mod tests {
    /// Purpose: Transform a fixed input.
    /// Expected: The transformed values match the explicit sequence.
    #[test]
    fn first() {
        let input = vec![1, 2, 3, 4];
        let actual = transform(&input);
        assert_eq!(actual, vec![2, 3, 4, 5]);
    }
    /// Purpose: Transform another fixed input.
    /// Expected: The transformed values match another explicit sequence.
    #[test]
    fn second() {
        let input = vec![1, 2, 3, 6];
        let actual = transform(&input);
        assert_eq!(actual, vec![2, 3, 4, 7]);
    }
}
"#;

    type ContractCase<'a> = (&'a str, &'a str, &'a str, &'a [(usize, &'a str)]);

    struct Repo(TempDir);

    impl Repo {
        fn new() -> Self {
            let repo = Self(tempfile::tempdir().unwrap());
            repo.git(&["init", "-b", "main"]);
            repo.git(&["config", "user.email", "audit@example.invalid"]);
            repo.git(&["config", "user.name", "Audit Fixture"]);
            repo.git(&["config", "core.hooksPath", "/dev/null"]);
            repo
        }

        fn root(&self) -> &Path {
            self.0.path()
        }

        fn git(&self, args: &[&str]) -> String {
            git(self.root(), args).unwrap()
        }

        fn write(&self, name: &str, source: &str) {
            let path = self.root().join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, source).unwrap();
        }

        fn commit(&self) -> String {
            self.git(&["add", "-A"]);
            self.git(&["commit", "-qm", "fixture"]);
            self.git(&["rev-parse", "HEAD"]).trim().to_string()
        }

        fn audit(&self, selection: Selection, destination: &str) -> Result<i32, String> {
            self.audit_with_analysis(selection, destination, false)
        }

        fn audit_with_analysis(
            &self,
            selection: Selection,
            destination: &str,
            analyze_duplicates: bool,
        ) -> Result<i32, String> {
            run_audit(
                self.root(),
                &Args {
                    selection,
                    output_dir: destination.into(),
                    analyze_duplicates,
                },
            )
        }

        fn candidates(&self, destination: &str) -> String {
            fs::read_to_string(self.root().join(destination).join(CANDIDATE_REPORT_NAME)).unwrap()
        }

        fn reports(&self, destination: &str) -> Vec<Vec<u8>> {
            REPORT_NAMES
                .iter()
                .map(|name| fs::read(self.root().join(destination).join(name)).unwrap())
                .collect()
        }
    }

    fn extract(file: &str, source: &str) -> Result<Vec<TestRecord>, String> {
        super::extract(file, source, true)
    }

    fn record(source: &str) -> TestRecord {
        let mut records = extract("sample.rs", source).unwrap();
        assert_eq!(records.len(), 1, "{records:?}");
        records.pop().unwrap()
    }

    fn force(paths: &[&str]) -> Selection {
        Selection::Forced(paths.iter().map(PathBuf::from).collect())
    }

    /// Purpose: Extract equivalent contracts from comments and literal attributes with wrapping.
    /// Expected: Whitespace collapses, preamble is excluded, and all forms yield the same fields.
    #[test]
    fn extracts_literal_and_wrapped_contracts() {
        for source in [
            "/// Preamble.\n/// Purpose: Protect\n/// a boundary.\n/// Expected:\n/// Value equals 7.\n#[test]\nfn case() {}",
            "#[doc = \"Preamble.\\nPurpose: Protect a boundary.\\nExpected: Value equals 7.\"]\n#[test]\nfn case() {}",
            "/** Purpose: Protect a boundary.\nExpected: Value equals 7. */\n#[test]\nfn case() {}",
            VALID,
        ] {
            let record = record(source);
            assert_eq!(
                (
                    record.purpose.as_str(),
                    record.expected.as_str(),
                    record.status()
                ),
                ("Protect a boundary.", "Value equals 7.", "documented"),
                "{source}"
            );
        }
    }

    /// Purpose: Diagnose every malformed contract field and documentation placement at its source.
    /// Expected: Named cases produce exact rule/line pairs with invalid status taking precedence.
    #[test]
    fn diagnoses_contract_rules_and_locations() {
        let cases: &[ContractCase<'_>] = &[
            (
                "missing",
                "#[test]\nfn case() {}",
                "missing",
                &[
                    (2, "test-contract-missing-expected"),
                    (2, "test-contract-missing-purpose"),
                ],
            ),
            (
                "case-sensitive",
                "/// purpose: no\n/// Expected: yes\n#[test]\nfn case() {}",
                "missing",
                &[(4, "test-contract-missing-purpose")],
            ),
            (
                "empty",
                "/// Purpose: \n/// Expected:\n#[test]\nfn case() {}",
                "invalid",
                &[
                    (1, "test-contract-empty-purpose"),
                    (2, "test-contract-empty-expected"),
                ],
            ),
            (
                "duplicate",
                "/// Purpose: one\n/// Purpose: two\n/// Expected: one\n/// Expected: two\n#[test]\nfn case() {}",
                "invalid",
                &[
                    (2, "test-contract-duplicate-purpose"),
                    (4, "test-contract-duplicate-expected"),
                ],
            ),
            (
                "nonliteral",
                "#[doc = include_str!(\"missing\")]\n#[test]\nfn case() {}",
                "invalid",
                &[
                    (1, "test-contract-nonliteral-doc"),
                    (3, "test-contract-missing-expected"),
                    (3, "test-contract-missing-purpose"),
                ],
            ),
            (
                "placement",
                "#[test]\n/// Purpose: one\n/// Expected: two\nfn case() {}",
                "invalid",
                &[
                    (2, "test-contract-doc-placement"),
                    (3, "test-contract-doc-placement"),
                ],
            ),
            (
                "empty-and-missing",
                "/// Expected:\n#[test]\nfn case() {}",
                "invalid",
                &[
                    (1, "test-contract-empty-expected"),
                    (3, "test-contract-missing-purpose"),
                ],
            ),
        ];
        for (name, source, status, expected) in cases {
            let record = record(source);
            assert_eq!(record.status(), *status, "{name}");
            assert_eq!(
                record
                    .diagnostics
                    .iter()
                    .map(|d| (d.line, d.rule))
                    .collect::<Vec<_>>(),
                *expected,
                "{name}"
            );
        }
    }

    /// Purpose: Inventory source declarations without evaluating cfgs or expanding macros and strings.
    /// Expected: Nested conditional, ignored, panic, and same-name tests retain paths, conditions, and lines.
    #[test]
    fn discovers_source_tests_and_local_conditions() {
        let source = r##"#!/usr/bin/env cargo
---
[package]
edition = "2024"
---
#![cfg(unix)]
#[cfg(test)]
mod tests {
    #[cfg_attr(feature = "one", cfg_attr(unix, test))]
    fn same() {}
    #[cfg(windows)]
    #[test]
    #[ignore]
    #[should_panic]
    fn same() {}
    mod nested {
        #[test]
        fn case() {}
    }
    fn helper() { let _s = "#[test] fn fake() {}"; }
    macro_rules! cases { () => { #[test] fn generated() {} }; }
}
#[cfg(never)]
mod external;
"##;
        let records = extract("tool.rs", source).unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| (r.test.as_str(), r.line))
                .collect::<Vec<_>>(),
            [
                ("tests::same", 10),
                ("tests::same", 15),
                ("tests::nested::case", 18)
            ]
        );
        assert_eq!(
            records[0].conditions,
            [
                "cfg (test)",
                "cfg (unix)",
                "cfg_attr (feature = \"one\" , cfg_attr (unix , test))"
            ]
        );
        assert_eq!(
            records[1].conditions,
            ["cfg (test)", "cfg (unix)", "cfg (windows)"]
        );
        let standalone = extract("external.rs", "#[test]\nfn same() {}").unwrap();
        assert!(standalone[0].conditions.is_empty());
        assert_eq!(standalone[0].test, "same");
        assert_ne!(standalone[0].file, records[0].file);
        assert!(!contains_test(
            &syn::parse_str::<Meta>("cfg_attr(test, allow(unused))").unwrap()
        ));
        assert!(extract("bad.rs", "fn broken(").is_err());
        assert!(extract("bad.rs", "#!/bin/cargo\n---\nmanifest").is_err());
    }

    /// Purpose: Lock down report schema, escaping, statuses, sorting, and exact duplicate membership.
    /// Expected: Fields and counts match explicit assertions, reversed input is identical, and invalid contracts never group.
    #[test]
    fn renders_report_fields_independent_of_order() {
        let mut records = extract(
            "a.rs",
            r#"/// Purpose: Protect a boundary.
/// Expected: Value equals 7.
#[test]
fn case() {}

/// Purpose: Check "quotes", commas | <tags> & `ticks`.
/// Expected: Preserve [case] and backslash \.
#[cfg(feature = "special")]
#[test]
fn escaped() {}

#[test]
fn missing() {}

/// Purpose: Protect a boundary.
/// Purpose: duplicate
/// Expected: Value equals 7.
#[test]
fn invalid() {}
"#,
        )
        .unwrap();
        records.extend(extract("dir/b.rs", VALID).unwrap());
        let (csv, markdown) = render_reports(2, &records);
        assert_eq!(
            csv.lines().next().unwrap(),
            "schema_version,file,line,test,source_conditions,purpose,expected,contract_status,issues"
        );
        assert_eq!(
            csv.lines()
                .skip(1)
                .map(|line| line.split(',').nth(3).unwrap())
                .collect::<Vec<_>>(),
            ["case", "escaped", "invalid", "missing", "case"]
        );
        assert_eq!(
            csv.lines()
                .filter(|line| line.ends_with(",documented,"))
                .count(),
            3
        );
        assert!(csv.contains(",invalid,test-contract-duplicate-purpose\n"));
        assert!(
            csv.contains(
                ",missing,test-contract-missing-expected; test-contract-missing-purpose\n"
            )
        );
        assert!(csv.contains("\"cfg (feature = \"\"special\"\")\""));
        assert!(csv.contains("\"Check \"\"quotes\"\", commas | <tags> & `ticks`.\""));
        assert!(
            markdown.contains("Source files: 2; tests: 5; documented: 3; missing: 1; invalid: 1.")
        );
        assert!(
            markdown.contains(r#"Check "quotes", commas &#124; &lt;tags&gt; &amp; \`ticks\`."#)
        );
        assert!(markdown.contains(r"Preserve \[case\] and backslash \\."));
        assert!(markdown.contains("a.rs:10 | escaped | cfg (feature = \"special\")"));
        let candidates = markdown
            .split("## Duplicate-contract candidates")
            .nth(1)
            .unwrap();
        assert_eq!(
            candidates
                .lines()
                .filter(|line| line.starts_with("- "))
                .collect::<Vec<_>>(),
            [
                "- a.rs:4 — case; conditions: (none)",
                "- dir/b.rs:4 — case; conditions: (none)",
            ]
        );
        for report in [&csv, &markdown] {
            assert!(report.ends_with('\n'));
            assert!(!report.contains('\r'));
        }
        records.reverse();
        assert_eq!(render_reports(2, &records), (csv, markdown));
        let groups = duplicate_groups(&records);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups
                .values()
                .next()
                .unwrap()
                .iter()
                .map(|r| (r.file.as_str(), r.test.as_str()))
                .collect::<Vec<_>>(),
            [("a.rs", "case"), ("dir/b.rs", "case")]
        );
        let (empty_csv, empty_markdown) = render_reports(0, &[]);
        assert_eq!(empty_csv, CSV_HEADER);
        assert!(
            empty_markdown
                .contains("Source files: 0; tests: 0; documented: 0; missing: 0; invalid: 0.")
        );
        assert!(empty_markdown.ends_with("\nNone.\n"));
    }

    /// Purpose: Read an unborn branch's complete index without substituting working-tree files.
    /// Expected: Staged additions pass despite broken replacements; inventory ignores untracked files.
    #[test]
    fn staged_snapshot_uses_index_blobs_and_worktree_inventory_uses_tracked_files() {
        let repo = Repo::new();
        repo.write("case.rs", VALID);
        repo.git(&["add", "case.rs"]);
        repo.write("case.rs", "not Rust!");
        repo.write("untracked.rs", "not Rust!");
        let snapshot = collect_snapshot(repo.root(), &Selection::Staged).unwrap();
        assert_eq!(
            snapshot.sources,
            BTreeMap::from([("case.rs".into(), VALID.into())])
        );
        assert_eq!(snapshot.selected, BTreeSet::from(["case.rs".into()]));
        assert_eq!(repo.audit(Selection::Staged, "stage").unwrap(), 0);
        assert!(repo.audit(Selection::Inventory, "work").is_err());
        repo.write("case.rs", VALID);
        assert_eq!(repo.audit(Selection::Inventory, "work").unwrap(), 0);
        assert_eq!(repo.reports("stage"), repo.reports("work"));
        repo.write("case.rs", "#[test]\nfn case() {}\n");
        repo.git(&["add", "case.rs"]);
        repo.write("case.rs", VALID);
        assert_eq!(repo.audit(Selection::Staged, "stage").unwrap(), 1);
        assert_eq!(repo.audit(Selection::Inventory, "work").unwrap(), 0);
        repo.git(&["add", "case.rs"]);
        repo.git(&["rm", "--cached", "case.rs"]);
        assert!(
            collect_snapshot(repo.root(), &Selection::Staged)
                .unwrap()
                .sources
                .is_empty()
        );
    }

    /// Purpose: Exclude the root tools directory from normal inventory and checks while allowing explicit audits.
    /// Expected: Inventory, staged, and diff modes ignore undocumented or malformed tools; forced tools report violations without entering inventory.
    #[test]
    fn excludes_tools_from_inventory_and_automatic_checks() {
        let repo = Repo::new();
        repo.write("README.md", "fixture");
        let base = repo.commit();
        let included = ["src/case.rs", "src/tools/case.rs", "toolsmith/case.rs"];
        for path in included {
            repo.write(path, VALID);
        }
        repo.write("tools/check.rs", "#[test]\nfn missing_contract() {}\n");
        repo.write("tools/nested/broken.rs", "fn invalid(");
        repo.git(&["add", "-A"]);

        let expected_sources = included
            .map(|path| (path.to_string(), VALID.to_string()))
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for (name, selection, selected_count) in [
            ("inventory", Selection::Inventory, 0),
            ("staged", Selection::Staged, 3),
            ("diff", Selection::Diff(base), 3),
            ("source", force(&["src/case.rs"]), 1),
        ] {
            let snapshot = collect_snapshot(repo.root(), &selection).unwrap();
            assert_eq!(snapshot.sources, expected_sources, "{name}");
            assert_eq!(snapshot.selected.len(), selected_count, "{name}");
            assert_eq!(repo.audit(selection, name).unwrap(), 0, "{name}");
            assert_eq!(repo.reports(name), repo.reports("inventory"), "{name}");
        }
        let reports = repo.reports("inventory");
        let csv = String::from_utf8(reports[0].clone()).unwrap();
        let markdown = String::from_utf8(reports[1].clone()).unwrap();
        assert_eq!(csv.lines().count(), 4);
        assert!(
            markdown.contains("Source files: 3; tests: 3; documented: 3; missing: 0; invalid: 0.")
        );

        for target in ["tools/check.rs", "tools"] {
            let selection = force(&[target]);
            let snapshot = collect_snapshot(repo.root(), &selection).unwrap();
            assert_eq!(snapshot.sources, expected_sources, "{target}");
            assert_eq!(snapshot.selected, BTreeSet::from(["tools/check.rs".into()]));
            assert_eq!(snapshot.extra_sources.len(), 1);
            assert_eq!(repo.audit(selection, "forced").unwrap(), 1, "{target}");
            assert_eq!(repo.reports("forced"), reports, "{target}");
        }
        assert!(
            repo.audit(force(&["tools/nested/broken.rs"]), "broken")
                .unwrap_err()
                .contains("tools/nested/broken.rs:1: parse")
        );
    }

    /// Purpose: Enforce whole changed files across complete PR and push bases, including production edits.
    /// Expected: Earlier violations survive later doc commits; unrelated missing contracts do not fail checks.
    #[test]
    fn explicit_bases_cover_complete_changes_and_whole_files() {
        let repo = Repo::new();
        repo.write(
            "changed.rs",
            "fn production() {}\n#[test]\nfn legacy() {}\n",
        );
        repo.write("untouched.rs", "#[test]\nfn legacy() {}\n");
        let base = repo.commit();
        repo.write(
            "changed.rs",
            "fn production() { let _value = 1; }\n#[test]\nfn legacy() {}\n",
        );
        let push_base = repo.commit();
        repo.write("README.md", "documentation-only update\n");
        repo.commit();
        assert_eq!(repo.audit(Selection::Diff(base.clone()), "pr").unwrap(), 1);
        assert_eq!(repo.audit(Selection::Diff(push_base), "push").unwrap(), 0);
        assert_eq!(repo.reports("pr"), repo.reports("push"));
        assert_eq!(repo.audit(Selection::Inventory, "inventory").unwrap(), 0);
        assert_eq!(repo.reports("pr"), repo.reports("inventory"));
        repo.write("changed.rs", VALID);
        assert_eq!(repo.audit(Selection::Diff(base), "fixed").unwrap(), 0);
        assert!(
            repo.audit(Selection::Diff("missing-base".into()), "fixed")
                .is_err()
        );
        assert!(
            REPORT_NAMES
                .iter()
                .all(|name| !repo.root().join("fixed").join(name).exists())
        );
    }

    /// Purpose: Track staged renames, additions, copies, and removals without retaining deleted tests.
    /// Expected: The complete index and selected paths contain only surviving Rust destinations.
    #[test]
    fn staged_path_changes_and_deletions() {
        let repo = Repo::new();
        repo.write("old.rs", VALID);
        repo.write("removed.rs", VALID);
        repo.commit();
        repo.git(&["mv", "old.rs", "renamed.rs"]);
        repo.git(&["rm", "removed.rs"]);
        repo.write("copied.rs", VALID);
        repo.write("added.rs", VALID);
        repo.git(&["add", "copied.rs", "added.rs"]);
        let snapshot = collect_snapshot(repo.root(), &Selection::Staged).unwrap();
        let expected = BTreeSet::from(["added.rs".into(), "copied.rs".into(), "renamed.rs".into()]);
        assert_eq!(snapshot.selected, expected);
        assert_eq!(
            snapshot.sources.keys().cloned().collect::<BTreeSet<_>>(),
            expected
        );
        fs::remove_file(repo.root().join("added.rs")).unwrap();
        assert!(
            !collect_snapshot(repo.root(), &Selection::Inventory)
                .unwrap()
                .sources
                .contains_key("added.rs")
        );
        assert!(
            collect_snapshot(repo.root(), &Selection::Staged)
                .unwrap()
                .sources
                .contains_key("added.rs")
        );
    }

    /// Purpose: Enforce contracts when a tracked Rust symlink becomes a regular source file.
    /// Expected: Unstaged diff and staged/diff type changes select the replacement, fail missing contracts, and pass documented replacements.
    #[cfg(unix)]
    #[test]
    fn type_changes_select_regular_rust_replacements() {
        let repo = Repo::new();
        repo.write("target.txt", VALID);
        symlink("target.txt", repo.root().join("case.rs")).unwrap();
        let base = repo.commit();
        fs::remove_file(repo.root().join("case.rs")).unwrap();
        let undocumented = "#[test]\nfn undocumented() {}\n";
        repo.write("case.rs", undocumented);

        let expected = BTreeSet::from(["case.rs".to_string()]);
        let selection = Selection::Diff(base.clone());
        let snapshot = collect_snapshot(repo.root(), &selection).unwrap();
        assert_eq!(snapshot.selected, expected);
        assert_eq!(snapshot.sources["case.rs"], undocumented);
        assert_eq!(repo.audit(selection, "unstaged").unwrap(), 1);

        repo.git(&["add", "case.rs"]);
        assert_eq!(
            repo.git(&["diff", "--cached", "--name-status", "--", "case.rs"]),
            "T\tcase.rs\n"
        );
        for (source, expected_code) in [(undocumented, 1), (VALID, 0)] {
            repo.write("case.rs", source);
            repo.git(&["add", "case.rs"]);
            for selection in [Selection::Staged, Selection::Diff(base.clone())] {
                let snapshot = collect_snapshot(repo.root(), &selection).unwrap();
                assert_eq!(snapshot.selected, expected, "{selection:?}");
                assert_eq!(snapshot.sources["case.rs"], source, "{selection:?}");
                assert_eq!(repo.audit(selection, "reports").unwrap(), expected_code);
            }
        }
    }

    /// Purpose: Reject ambiguous staged snapshots when Git contains unmerged entries.
    /// Expected: A real merge conflict produces an execution error and no fresh inventory.
    #[test]
    fn rejects_unmerged_index() {
        let repo = Repo::new();
        repo.write("case.rs", VALID);
        repo.commit();
        repo.git(&["checkout", "-qb", "side"]);
        repo.write("case.rs", "fn side() {}\n");
        repo.commit();
        repo.git(&["checkout", "main"]);
        repo.write("case.rs", "fn main_side() {}\n");
        repo.commit();
        assert!(git(repo.root(), &["merge", "side"]).is_err());
        assert!(
            repo.audit(Selection::Staged, "reports")
                .unwrap_err()
                .contains("unmerged")
        );
        assert!(!repo.root().join("reports/test-inventory.csv").exists());
    }

    /// Purpose: Bound explicit directories to direct Rust children and keep extra files out of inventory.
    /// Expected: Targets deduplicate, nested files stay unselected, extra violations fail, and missing paths error.
    #[test]
    fn forced_scope_is_direct_and_deduplicated() {
        let repo = Repo::new();
        repo.write("src/good.rs", VALID);
        repo.write("src/nested/bad.rs", "#[test]\nfn bad() {}\n");
        repo.commit();
        repo.write("src/extra.rs", "#[test]\nfn extra() {}\n");
        let snapshot = collect_snapshot(
            repo.root(),
            &force(&["src", "src/good.rs", "src/../src/good.rs"]),
        )
        .unwrap();
        assert_eq!(
            snapshot.selected,
            BTreeSet::from(["src/extra.rs".into(), "src/good.rs".into()])
        );
        assert_eq!(snapshot.extra_sources.len(), 1);
        assert_eq!(repo.audit(force(&["src"]), "forced").unwrap(), 1);
        assert_eq!(repo.audit(force(&["src/good.rs"]), "good").unwrap(), 0);
        assert_eq!(repo.reports("forced"), repo.reports("good"));
        repo.write("empty/readme.txt", "not Rust");
        assert_eq!(repo.audit(force(&["empty"]), "empty").unwrap(), 0);
        assert_eq!(repo.reports("empty"), repo.reports("good"));
        assert!(repo.audit(force(&["absent"]), "absent").is_err());
    }

    /// Purpose: Preserve report bytes across repeated runs, checkout locations, destinations, and source insertion orders.
    /// Expected: Repeated inventory runs and identical snapshots in independent repos produce byte-identical reports.
    #[test]
    fn inventories_are_relocatable() {
        let first = Repo::new();
        let second = Repo::new();
        for (repo, names) in [(&first, ["a.rs", "z.rs"]), (&second, ["z.rs", "a.rs"])] {
            for name in names {
                repo.write(name, VALID);
            }
            repo.commit();
            assert_eq!(repo.audit(Selection::Inventory, "one").unwrap(), 0);
            assert_eq!(repo.audit(Selection::Inventory, "repeat").unwrap(), 0);
            assert_eq!(repo.reports("one"), repo.reports("repeat"));
            assert_eq!(repo.audit(force(&["a.rs"]), "two").unwrap(), 0);
            assert_eq!(repo.reports("one"), repo.reports("two"));
        }
        assert_eq!(first.reports("one"), second.reports("one"));
    }

    /// Purpose: Fail extraction for any invalid tracked source even outside the selected scope.
    /// Expected: A parse error removes both prior reports and returns an error, while an empty repo reports zero.
    #[test]
    fn failed_extraction_clears_old_reports() {
        let repo = Repo::new();
        assert_eq!(repo.audit(Selection::Inventory, "out").unwrap(), 0);
        assert_eq!(repo.reports("out")[0], CSV_HEADER.as_bytes());
        repo.write("good.rs", VALID);
        repo.write("bad.rs", "fn invalid(");
        repo.git(&["add", "good.rs", "bad.rs"]);
        assert!(
            repo.audit(force(&["good.rs"]), "out")
                .unwrap_err()
                .contains("bad.rs:1: parse")
        );
        assert!(
            REPORT_NAMES
                .iter()
                .all(|name| !repo.root().join("out").join(name).exists())
        );
    }

    /// Purpose: Canonicalize formatting while preserving values, operators, calls, and panic conditions.
    /// Expected: Spacing/comments produce identical tokens; candidate evidence retains differing literals, assertions, and attributes.
    #[test]
    fn duplicate_evidence_preserves_semantic_differences() {
        let compact = syn::parse_str::<ItemFn>("fn a() { assert_eq!(&*input, 2); }").unwrap();
        let formatted =
            syn::parse_str::<ItemFn>("fn b() { /* comment */ assert_eq!( & * input , 2 ); }")
                .unwrap();
        assert_eq!(
            test_body(&compact, &[]).tokens,
            test_body(&formatted, &[]).tokens
        );
        let continued_literal = "\"first\\\n    second\"";
        let continued = syn::parse_str::<ItemFn>(&format!(
            "fn continued() {{ verify({continued_literal}); }}"
        ))
        .unwrap();
        assert_eq!(
            test_body(&continued, &[]).tokens,
            ["verify", "(", continued_literal, ")", ";"]
        );
        let source = ANALYSIS_SOURCE.replace("    fn second()", "    #[cfg(unix)]\n    #[ignore]\n    #[should_panic(expected = \"different\")]\n    fn second()");
        let records = extract("sample.rs", &source).unwrap();
        let analysis = analyze_duplicates(&records);
        assert_eq!(analysis.candidates.len(), 1);
        let report = render_candidates(&analysis);
        for evidence in [
            "cfg (unix)",
            "ignore",
            "should\\_panic",
            "different",
            "transform",
            "Only these assertions",
            "assert\\_eq ! ( actual , vec ! \\[ 2 , 3 , 4 , 5 \\] )",
            "assert\\_eq ! ( actual , vec ! \\[ 2 , 3 , 4 , 7 \\] )",
            "Differing token excerpts",
        ] {
            assert!(report.contains(evidence), "{evidence}: {report}");
        }
        assert!(
            super::extract("sample.rs", &source, false)
                .unwrap()
                .iter()
                .all(|record| record.body.is_none())
        );
        let mut unrelated = records;
        let different = syn::parse_str::<ItemFn>(
            "fn unrelated() { loop { match receive() { Some(event) => dispatch(event), None => break } } shutdown(); finish(); wait_until_idle(); ensure_closed(); }",
        )
        .unwrap();
        let module = &unrelated[1].body.as_ref().unwrap().module;
        unrelated[1].body = Some(test_body(&different, module));
        let analysis = analyze_duplicates(&unrelated);
        assert_eq!(
            (
                analysis.test_count,
                analysis.short_count,
                analysis.pair_count
            ),
            (2, 0, 1)
        );
        assert!(analysis.candidates.is_empty());
    }

    /// Purpose: Keep sibling, nested, same-name conditional, and external modules out of each other's candidate pairs.
    /// Expected: Only the two tests in each actual module declaration compare; function nesting does not create a module boundary.
    #[test]
    fn duplicate_analysis_respects_module_declarations() {
        let nested = format!("mod outer {{ {ANALYSIS_SOURCE} }}");
        let mut records = extract(
            "a.rs",
            &format!("#[cfg(unix)] {ANALYSIS_SOURCE}\n#[cfg(windows)] {ANALYSIS_SOURCE}\n{nested}"),
        )
        .unwrap();
        records.extend(extract("b.rs", ANALYSIS_SOURCE).unwrap());
        let analysis = analyze_duplicates(&records);
        assert_eq!(
            (
                analysis.test_count,
                analysis.pair_count,
                analysis.candidates.len()
            ),
            (8, 4, 4)
        );
        for pair in &analysis.candidates {
            assert_eq!(pair.left.body.module, pair.right.body.module);
            assert_eq!(pair.left.record.file, pair.right.record.file);
        }
        let nested_functions =
            extract("a.rs", &format!("fn owner() {{ {ANALYSIS_SOURCE} }}")).unwrap();
        let body = nested_functions[0].body.as_ref().unwrap();
        assert_eq!(
            body.module
                .iter()
                .map(|part| part.name.as_str())
                .collect::<Vec<_>>(),
            ["tests"]
        );
        assert_eq!(nested_functions[0].test, "owner::tests::first");
        let report = render_candidates(&analysis);
        records.reverse();
        assert_eq!(render_candidates(&analyze_duplicates(&records)), report);
        let inline = "mod tests { #[test] fn first() { assert!(true); } }";
        let records = extract(
            "a.rs",
            &format!("#[cfg(unix)] {inline} #[cfg(windows)] {inline}"),
        )
        .unwrap();
        let left = &records[0].body.as_ref().unwrap().module;
        let right = &records[1].body.as_ref().unwrap().module;
        assert_eq!(left[0].name, right[0].name);
        assert_eq!(left[0].line, right[0].line);
        assert_ne!(left, right);
    }

    /// Purpose: Match ordered token blocks with deterministic ties and evaluate exact pilot thresholds.
    /// Expected: Earliest-left ties win, disjoint blocks add, 85%/95% boundaries are inclusive, and bodies under 30 tokens are excluded.
    #[test]
    fn matching_blocks_and_thresholds_are_deterministic() {
        let tokens = |values: &[&str]| {
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            matching_blocks(
                &tokens(&["a", "b", "a", "b"]),
                &tokens(&["b", "a", "b", "a"])
            ),
            [MatchingBlock {
                left: 0,
                right: 1,
                len: 3
            }]
        );
        assert_eq!(
            matching_blocks(
                &tokens(&["a", "b", "x", "c", "d"]),
                &tokens(&["a", "b", "y", "c", "d"])
            ),
            [
                MatchingBlock {
                    left: 0,
                    right: 0,
                    len: 2
                },
                MatchingBlock {
                    left: 3,
                    right: 3,
                    len: 2
                }
            ]
        );
        assert!(matching_blocks(&tokens(&["a"]), &tokens(&["b"])).is_empty());
        for (matched, left, right, expected) in [
            (85, 100, 100, (true, false)),
            (84, 100, 100, (false, false)),
            (95, 100, 300, (false, true)),
            (94, 100, 300, (false, false)),
            (29, 29, 29, (false, false)),
            (30, 30, 30, (true, true)),
        ] {
            assert_eq!(
                candidate_signals(matched, left, right),
                expected,
                "{matched}/{left}/{right}"
            );
        }
        let short = extract(
            "short.rs",
            "#[test] fn first() { helper(false); } #[test] fn second() { helper(true); }",
        )
        .unwrap();
        let analysis = analyze_duplicates(&short);
        assert_eq!(
            (
                analysis.test_count,
                analysis.short_count,
                analysis.pair_count
            ),
            (2, 2, 0)
        );
        assert!(render_candidates(&analysis).contains("short bodies excluded: 2"));
    }

    /// Purpose: Recognize the reviewed benchmark insert, read-offset, and CLI-containment candidates without equating their inputs.
    /// Expected: Each named pair is reported and different enum values, expected sequences, or option spellings remain in its evidence.
    #[test]
    fn duplicate_analysis_recovers_benchmark_examples() {
        let insert = "let plan = SessionPlan { session_index: 0, key_start: 10, number: 64 }; let keys = generate_insert_keys(true, IndexMode::None, 2, &plan).unwrap(); let unique: HashSet<_> = keys.iter().copied().collect(); assert!(unique.len() < keys.len());";
        let sequential = "let plan = SessionPlan { session_index: 0, key_start: 0, number: 8 }; assert_eq!(generate_sequential_read_keys(loaded_range(), &plan).unwrap(), vec![0, 1, 2, 0, 1, 2, 0, 1]);";
        let short_cli = r#"let plan = Cli::try_parse_from(["doradb-bench", "-r", "root", "-p", "p.toml"]).unwrap(); assert_eq!(plan.root, PathBuf::from("root")); assert_eq!(plan.plan, PathBuf::from("p.toml"));"#;
        for (name, left, right, evidence) in [
            (
                "replacement modes",
                insert.to_string(),
                insert.replace("IndexMode::None", "IndexMode::NonUnique"),
                "NonUnique",
            ),
            (
                "read offsets",
                sequential.to_string(),
                sequential
                    .replace(
                        "session_index: 0, key_start: 0, number: 8",
                        "session_index: 1, key_start: 4, number: 4",
                    )
                    .replace("vec![0, 1, 2, 0, 1, 2, 0, 1]", "vec![1, 2, 0, 1]"),
                "Differing token excerpts",
            ),
            (
                "CLI containment",
                format!(
                    "Cli::command().debug_assert(); {} assert!(Cli::try_parse_from([\"doradb-bench\", \"--root\", \"root\"]).is_err());",
                    short_cli
                        .replace("\"-r\"", "\"--root\"")
                        .replace("\"-p\"", "\"--plan\"")
                ),
                short_cli.to_string(),
                "--root",
            ),
        ] {
            let records = extract("bench.rs", &format!("mod tests {{ #[test] fn first() {{ {left} }} #[test] fn second() {{ {right} }} }}")).unwrap();
            let analysis = analyze_duplicates(&records);
            assert_eq!(analysis.candidates.len(), 1, "{name}");
            assert!(render_candidates(&analysis).contains(evidence), "{name}");
        }
    }

    /// Purpose: Analyze index blobs and selected forced extras without adding tools or untracked files to inventory.
    /// Expected: Staged analysis ignores broken replacements; forced tools/untracked tests appear only in candidate output and diff checks stay selected.
    #[test]
    fn duplicate_analysis_follows_snapshot_and_selection() {
        let repo = Repo::new();
        repo.write("README.md", "fixture");
        let base = repo.commit();
        repo.write("src/cases.rs", ANALYSIS_SOURCE);
        repo.write("tools/cases.rs", ANALYSIS_SOURCE);
        repo.git(&["add", "-A"]);
        repo.write("src/cases.rs", "fn broken(");
        assert_eq!(
            repo.audit_with_analysis(Selection::Staged, "out", true)
                .unwrap(),
            0
        );
        assert!(repo.candidates("out").contains("src/cases.rs"));
        assert!(!repo.candidates("out").contains("tools/cases.rs"));
        repo.write("src/cases.rs", ANALYSIS_SOURCE);
        assert_eq!(
            repo.audit_with_analysis(Selection::Diff(base), "out", true)
                .unwrap(),
            0
        );
        let inventory = repo.reports("out");
        repo.write("extra.rs", ANALYSIS_SOURCE);
        for target in ["tools", "extra.rs"] {
            assert_eq!(
                repo.audit_with_analysis(force(&[target]), "out", true)
                    .unwrap(),
                0
            );
            let report = repo.candidates("out");
            assert!(report.contains(target));
            assert!(!report.contains("src/cases.rs"));
            assert_eq!(repo.reports("out"), inventory);
        }
        repo.commit();
        assert_eq!(
            repo.audit_with_analysis(Selection::Staged, "out", true)
                .unwrap(),
            0
        );
        assert!(repo.candidates("out").contains("Analyzed tests: 0"));
    }

    /// Purpose: Preserve normal reports and report bytes across relocation while clearing stale optional analysis output.
    /// Expected: Opt-in analysis has identical inventory bytes, repeat/relocated candidate reports match, and disabling analysis or failing extraction removes stale candidates.
    #[test]
    fn duplicate_reports_are_optional_relocatable_and_fresh() {
        let first = Repo::new();
        let second = Repo::new();
        for (repo, names) in [(&first, ["a.rs", "b.rs"]), (&second, ["b.rs", "a.rs"])] {
            for name in names {
                repo.write(name, ANALYSIS_SOURCE);
            }
            repo.commit();
            assert_eq!(repo.audit(Selection::Inventory, "normal").unwrap(), 0);
            let normal = repo.reports("normal");
            assert!(
                !repo
                    .root()
                    .join("normal")
                    .join(CANDIDATE_REPORT_NAME)
                    .exists()
            );
            assert_eq!(
                repo.audit_with_analysis(Selection::Inventory, "analysis", true)
                    .unwrap(),
                0
            );
            assert_eq!(repo.reports("analysis"), normal);
            assert_eq!(
                repo.audit_with_analysis(Selection::Inventory, "repeat", true)
                    .unwrap(),
                0
            );
            assert_eq!(repo.candidates("analysis"), repo.candidates("repeat"));
            assert_eq!(repo.audit(Selection::Inventory, "repeat").unwrap(), 0);
            assert!(
                !repo
                    .root()
                    .join("repeat")
                    .join(CANDIDATE_REPORT_NAME)
                    .exists()
            );
        }
        assert_eq!(first.candidates("analysis"), second.candidates("analysis"));
        assert!(first.candidates("analysis").contains("candidates: 2"));
        first.write("a.rs", "fn broken(");
        assert!(
            first
                .audit_with_analysis(Selection::Inventory, "analysis", true)
                .is_err()
        );
        for name in REPORT_NAMES.into_iter().chain([CANDIDATE_REPORT_NAME]) {
            assert!(!first.root().join("analysis").join(name).exists(), "{name}");
        }
    }

    /// Purpose: Reject incomplete, mixed, duplicate, and unknown CLI selectors before auditing.
    /// Expected: Invalid named inputs error and every supported mode retains its explicit selection and output.
    #[test]
    fn cli_selectors_are_unambiguous() {
        for raw in [
            "",
            "check",
            "inventory --staged",
            "check --staged --staged",
            "check --staged --diff-base HEAD",
            "check --force-path a --staged",
            "check --staged --force-path a",
            "check --diff-base",
            "check --force-path",
            "inventory --output-dir",
            "inventory --wat",
            "inventory --analyze-duplicates --analyze-duplicates",
            "inventory --output-dir a --output-dir b",
        ] {
            assert!(
                parse_args(raw.split_whitespace().map(str::to_string).collect()).is_err(),
                "{raw}"
            );
        }
        for (raw, selection) in [
            ("inventory", Selection::Inventory),
            ("check --staged", Selection::Staged),
            ("check --diff-base HEAD", Selection::Diff("HEAD".into())),
            ("check --force-path a --force-path b", force(&["a", "b"])),
        ] {
            let parsed = parse_args(
                format!("{raw} --output-dir reports")
                    .split_whitespace()
                    .map(str::to_string)
                    .collect(),
            )
            .unwrap();
            assert_eq!(parsed.selection, selection);
            assert_eq!(parsed.output_dir, PathBuf::from("reports"));
            assert!(!parsed.analyze_duplicates);
            let analyzed = parse_args(
                format!("{raw} --analyze-duplicates")
                    .split_whitespace()
                    .map(str::to_string)
                    .collect(),
            )
            .unwrap();
            assert_eq!(analyzed.selection, selection);
            assert!(analyzed.analyze_duplicates);
        }
    }

    /// Purpose: Exercise real CLI exit codes and locale-independent reports in a scratch repository.
    /// Expected: Inventory/check/error return 0/1/2, locale changes preserve bytes, and Git errors clear reports.
    #[test]
    fn command_exit_codes_and_locales() {
        let repo = Repo::new();
        repo.write(
            "case.rs",
            &ANALYSIS_SOURCE
                .lines()
                .filter(|line| !line.trim().starts_with("///"))
                .collect::<Vec<_>>()
                .join("\n"),
        );
        repo.git(&["add", "case.rs"]);
        let tool = Path::new(file!()).canonicalize().unwrap();
        for (locale, command, code) in [
            ("C", vec!["inventory"], 0),
            ("C.UTF-8", vec!["check", "--staged"], 1),
        ] {
            let output = Command::new(&tool)
                .current_dir(repo.root())
                .env("LC_ALL", locale)
                .args(command)
                .arg("--analyze-duplicates")
                .args(["--output-dir", locale])
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(code), "{output:?}");
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("inventory: 1 files, 2 tests")
            );
        }
        assert_eq!(repo.reports("C"), repo.reports("C.UTF-8"));
        assert_eq!(repo.candidates("C"), repo.candidates("C.UTF-8"));
        assert!(repo.candidates("C").contains("candidates: 1"));
        let output = Command::new(&tool)
            .current_dir(repo.root())
            .args(["check", "--diff-base", "missing-base", "--output-dir", "C"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(!repo.root().join("C/test-inventory.csv").exists());
        assert!(!repo.root().join("C").join(CANDIDATE_REPORT_NAME).exists());
        let output = Command::new(&tool)
            .current_dir(repo.root())
            .arg("check")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
    }
}
