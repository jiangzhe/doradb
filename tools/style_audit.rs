#!/usr/bin/env -S cargo +nightly-2026-05-22 -q -Zscript
---
[package]
edition = "2024"

[dependencies]
proc-macro2 = { version = "1", features = ["span-locations"] }
quote = "1"
syn = { version = "2", features = ["full", "visit"] }
---

use proc_macro2::Span;
use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio, exit};
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{
    Attribute, Fields, GenericParam, Generics, ImplItem, Item, ItemImpl, ItemMod, ItemTrait,
    ItemUse, Path as SynPath, TraitItem, Type, UseTree, Visibility,
};

const FMT_COMMAND: &[&str] = &["fmt", "--all", "--", "--check"];
const CLIPPY_COMMAND: &[&str] = &[
    "clippy",
    "--workspace",
    "--all-targets",
    "--",
    "-D",
    "warnings",
];
const DEFAULT_DIFF_BASE: &str = "origin/main";

#[derive(Debug, Clone)]
struct Violation {
    path: String,
    line: usize,
    rule: &'static str,
    message: String,
}

#[derive(Debug)]
struct CommandResult {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ItemGroup {
    Use = 1,
    Constant = 2,
    Type = 3,
    PublicFunction = 4,
    PrivateFunction = 5,
    Tests = 6,
}

#[derive(Debug, Clone)]
struct AuditFile {
    display_path: String,
    full_path: PathBuf,
}

#[derive(Debug, Default)]
struct Args {
    help: bool,
    diff_base: Option<String>,
    force_paths: Vec<PathBuf>,
}

#[derive(Debug)]
struct TestImportCandidate {
    line: usize,
    names: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InherentImplContext {
    self_ty: String,
    display_name: String,
    generic_params: String,
    where_clause: Option<String>,
    attrs: Vec<String>,
    is_default: bool,
    is_unsafe: bool,
}

#[derive(Debug, Default)]
struct TestImportUsage {
    test_module_refs: usize,
    non_test_module_refs: usize,
}

impl TestImportUsage {
    fn is_test_module_only(&self) -> bool {
        self.test_module_refs > 0 && self.non_test_module_refs == 0
    }
}

struct TestImportScopeVisitor<'a> {
    imported_names: &'a BTreeSet<String>,
    usage: BTreeMap<String, TestImportUsage>,
    test_module_depth: usize,
}

impl<'ast> Visit<'ast> for TestImportScopeVisitor<'_> {
    fn visit_item_mod(&mut self, item_mod: &'ast ItemMod) {
        let entered_test_module = has_cfg_test(&item_mod.attrs);
        if entered_test_module {
            self.test_module_depth += 1;
        }
        visit::visit_item_mod(self, item_mod);
        if entered_test_module {
            self.test_module_depth -= 1;
        }
    }

    fn visit_path(&mut self, path: &'ast SynPath) {
        if let Some(segment) = path.segments.first() {
            let name = segment.ident.to_string();
            if self.imported_names.contains(&name) {
                let usage = self.usage.entry(name).or_default();
                if self.test_module_depth > 0 {
                    usage.test_module_refs += 1;
                } else {
                    usage.non_test_module_refs += 1;
                }
            }
        }
        visit::visit_path(self, path);
    }
}

struct QualifiedPathVisitor<'a> {
    file_path: &'a str,
    seen: BTreeSet<(usize, String)>,
    generic_type_params: Vec<BTreeSet<String>>,
    violations: Vec<Violation>,
}

impl QualifiedPathVisitor<'_> {
    fn with_generic_type_params<F>(&mut self, generics: &Generics, visit: F)
    where
        F: FnOnce(&mut Self),
    {
        let params = generics
            .params
            .iter()
            .filter_map(|param| match param {
                GenericParam::Type(type_param) => Some(type_param.ident.to_string()),
                GenericParam::Const(_) | GenericParam::Lifetime(_) => None,
            })
            .collect();
        self.generic_type_params.push(params);
        visit(self);
        self.generic_type_params.pop();
    }

    fn name_is_type_root(&self, name: &str) -> bool {
        name == "Self"
            || self
                .generic_type_params
                .iter()
                .rev()
                .any(|params| params.contains(name))
            || name.chars().next().is_some_and(char::is_uppercase)
    }

    fn path_has_short_type_root(&self, path: &SynPath) -> bool {
        path.segments
            .iter()
            .take(2)
            .map(|segment| segment.ident.to_string())
            .any(|name| self.name_is_type_root(&name))
    }
}

impl<'ast> Visit<'ast> for QualifiedPathVisitor<'_> {
    fn visit_item_fn(&mut self, item_fn: &'ast syn::ItemFn) {
        self.with_generic_type_params(&item_fn.sig.generics, |this| {
            visit::visit_item_fn(this, item_fn);
        });
    }

    fn visit_item_impl(&mut self, item_impl: &'ast ItemImpl) {
        self.with_generic_type_params(&item_impl.generics, |this| {
            visit::visit_item_impl(this, item_impl);
        });
    }

    fn visit_item_trait(&mut self, item_trait: &'ast ItemTrait) {
        self.with_generic_type_params(&item_trait.generics, |this| {
            visit::visit_item_trait(this, item_trait);
        });
    }

    fn visit_impl_item_fn(&mut self, method: &'ast syn::ImplItemFn) {
        self.with_generic_type_params(&method.sig.generics, |this| {
            visit::visit_impl_item_fn(this, method);
        });
    }

    fn visit_trait_item_fn(&mut self, method: &'ast syn::TraitItemFn) {
        self.with_generic_type_params(&method.sig.generics, |this| {
            visit::visit_trait_item_fn(this, method);
        });
    }

    fn visit_path(&mut self, path: &'ast SynPath) {
        let separators = path.segments.len().saturating_sub(1);
        if separators > 1 && !self.path_has_short_type_root(path) {
            let line = span_line(path.span());
            let path_text = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::");
            if self.seen.insert((line, path_text.clone())) {
                self.violations.push(violation(
                    self.file_path,
                    line,
                    "qualified-path",
                    format!(
                        "`{path_text}` contains more than one `::`; import it and use a shorter name"
                    ),
                ));
            }
        }
        visit::visit_path(self, path);
    }
}

fn usage() -> &'static str {
    "Usage: tools/style_audit.rs [--diff-base <rev> | --force-path <file-or-dir> ...]\nDefault: audit Rust files changed against origin/main."
}

fn main() {
    let code = match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err}");
            2
        }
    };
    exit(code);
}

fn run() -> Result<i32, String> {
    let args = parse_args()?;
    if args.help {
        println!("{}", usage());
        return Ok(0);
    }
    run_audit(&args)
}

fn parse_args() -> Result<Args, String> {
    let mut parsed = Args::default();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--force-path" => {
                let Some(path) = args.next() else {
                    return Err(format!("missing value for --force-path\n{}", usage()));
                };
                parsed.force_paths.push(PathBuf::from(path));
            }
            "--diff-base" => {
                let Some(diff_base) = args.next() else {
                    return Err(format!("missing value for --diff-base\n{}", usage()));
                };
                if parsed.diff_base.replace(diff_base).is_some() {
                    return Err(format!(
                        "--diff-base can only be provided once\n{}",
                        usage()
                    ));
                }
            }
            "--help" | "-h" => {
                if args.next().is_some() {
                    return Err(format!("unexpected arg\n{}", usage()));
                }
                if parsed.diff_base.is_some() || !parsed.force_paths.is_empty() {
                    return Err(format!(
                        "--help cannot be combined with other args\n{}",
                        usage()
                    ));
                }
                parsed.help = true;
                return Ok(parsed);
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", usage())),
        }
    }
    if parsed.diff_base.is_some() && !parsed.force_paths.is_empty() {
        return Err(format!(
            "--diff-base cannot be combined with --force-path\n{}",
            usage()
        ));
    }
    Ok(parsed)
}

fn run_audit(args: &Args) -> Result<i32, String> {
    let repo_root = repo_root()?;
    match (args.force_paths.is_empty(), args.diff_base.as_deref()) {
        (true, None) => run_branch_diff_audit(&repo_root, DEFAULT_DIFF_BASE),
        (true, Some(diff_base)) => run_branch_diff_audit(&repo_root, diff_base),
        (false, None) => run_forced_audit(&repo_root, &args.force_paths),
        (false, Some(_)) => unreachable!("validated by parse_args"),
    }
}

fn run_branch_diff_audit(repo_root: &Path, diff_base: &str) -> Result<i32, String> {
    let files = branch_diff_rust_files(repo_root, diff_base)?;
    if files.is_empty() {
        println!("style-audit: no Rust files changed against {diff_base}");
        return Ok(0);
    }

    if let Some(result) = run_cargo_gate(repo_root, FMT_COMMAND)? {
        print_gate_failure("cargo fmt --all -- --check", &result);
        return Ok(1);
    }

    if let Some(result) = run_cargo_gate(repo_root, CLIPPY_COMMAND)? {
        print_gate_failure(
            "cargo clippy --workspace --all-targets -- -D warnings",
            &result,
        );
        return Ok(1);
    }

    let label = format!("branch-diff against {diff_base}");
    audit_files(&label, &files)
}

fn run_forced_audit(repo_root: &Path, force_paths: &[PathBuf]) -> Result<i32, String> {
    let files = forced_rust_files(repo_root, force_paths)?;
    if files.is_empty() {
        println!("style-audit: no Rust files matched --force-path");
        return Ok(0);
    }

    if let Some(result) = run_rustfmt_gate(repo_root, &files)? {
        print_gate_failure("rustfmt --edition 2024 --check <forced files>", &result);
        return Ok(1);
    }

    if let Some(result) = run_cargo_gate(repo_root, CLIPPY_COMMAND)? {
        print_gate_failure(
            "cargo clippy --workspace --all-targets -- -D warnings",
            &result,
        );
        return Ok(1);
    }

    audit_files("forced-path", &files)
}

fn audit_files(label: &str, files: &[AuditFile]) -> Result<i32, String> {
    let mut violations = Vec::new();
    for file in files {
        let content = fs::read_to_string(&file.full_path).map_err(|e| {
            format!(
                "failed to read {} ({}): {e}",
                file.display_path,
                file.full_path.display()
            )
        })?;
        violations.extend(audit_content(&file.display_path, &content));
    }

    print_style_summary(label, files.len(), &violations);
    Ok(if violations.is_empty() { 0 } else { 1 })
}

fn repo_root() -> Result<PathBuf, String> {
    let result = run_command(Path::new("."), "git", &["rev-parse", "--show-toplevel"])?;
    if result.code != Some(0) {
        return Err(format!(
            "failed to resolve git repository root\n{}{}",
            result.stdout, result.stderr
        ));
    }
    Ok(PathBuf::from(result.stdout.trim()))
}

fn branch_diff_rust_files(repo_root: &Path, diff_base: &str) -> Result<Vec<AuditFile>, String> {
    let merge_base = merge_base(repo_root, diff_base)?;
    let result = run_command(
        repo_root,
        "git",
        &[
            "diff",
            "--name-only",
            "--diff-filter=ACMR",
            "-z",
            &merge_base,
            "--",
            "*.rs",
        ],
    )?;
    if result.code != Some(0) {
        return Err(format!(
            "failed to collect Rust files changed against {diff_base}\n{}{}",
            result.stdout, result.stderr
        ));
    }

    let mut files = result
        .stdout
        .split('\0')
        .filter(|path| !path.is_empty() && path.ends_with(".rs"))
        .map(|path| AuditFile {
            display_path: normalize_path(Path::new(path)),
            full_path: repo_root.join(path),
        })
        .filter(|file| file.full_path.is_file())
        .collect::<Vec<_>>();
    files.sort_by(|a, b| a.display_path.cmp(&b.display_path));
    files.dedup_by(|a, b| a.display_path == b.display_path);
    Ok(files)
}

fn merge_base(repo_root: &Path, diff_base: &str) -> Result<String, String> {
    let result = run_command(repo_root, "git", &["merge-base", diff_base, "HEAD"])?;
    if result.code != Some(0) {
        return Err(format!(
            "failed to resolve merge-base for {diff_base} and HEAD\n{}{}",
            result.stdout, result.stderr
        ));
    }
    Ok(result.stdout.trim().to_string())
}

fn forced_rust_files(repo_root: &Path, force_paths: &[PathBuf]) -> Result<Vec<AuditFile>, String> {
    let mut files = Vec::new();
    for force_path in force_paths {
        let full_path = if force_path.is_absolute() {
            force_path.clone()
        } else {
            repo_root.join(force_path)
        };

        if full_path.is_file() {
            if is_rust_file(&full_path) {
                files.push(AuditFile {
                    display_path: display_path(repo_root, &full_path),
                    full_path,
                });
            }
            continue;
        }

        if full_path.is_dir() {
            let mut children = fs::read_dir(&full_path)
                .map_err(|e| format!("failed to read {}: {e}", full_path.display()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("failed to read {}: {e}", full_path.display()))?;
            children.sort_by_key(|entry| entry.path());
            for child in children {
                let child_path = child.path();
                if child_path.is_file() && is_rust_file(&child_path) {
                    files.push(AuditFile {
                        display_path: display_path(repo_root, &child_path),
                        full_path: child_path,
                    });
                }
            }
            continue;
        }

        return Err(format!(
            "--force-path target does not exist or is not a file/directory: {}",
            force_path.display()
        ));
    }

    files.sort_by(|a, b| a.display_path.cmp(&b.display_path));
    files.dedup_by(|a, b| a.display_path == b.display_path);
    Ok(files)
}

fn is_rust_file(path: &Path) -> bool {
    path.extension().and_then(|value| value.to_str()) == Some("rs")
}

fn display_path(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .map(normalize_path)
        .unwrap_or_else(|_| normalize_path(path))
}

fn run_cargo_gate(cwd: &Path, args: &[&str]) -> Result<Option<CommandResult>, String> {
    let result = run_command(cwd, "cargo", args)?;
    Ok((result.code != Some(0)).then_some(result))
}

fn run_rustfmt_gate(cwd: &Path, files: &[AuditFile]) -> Result<Option<CommandResult>, String> {
    let mut command = Command::new("rustfmt");
    command
        .current_dir(cwd)
        .stdin(Stdio::null())
        .args(["--edition", "2024", "--check"]);
    for file in files {
        command.arg(&file.full_path);
    }
    let output = command
        .output()
        .map_err(|e| format!("failed to run rustfmt: {e}"))?;
    let result = CommandResult {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    };
    Ok((result.code != Some(0)).then_some(result))
}

fn run_command(cwd: &Path, program: &str, args: &[&str]) -> Result<CommandResult, String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("failed to run {program}: {e}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    Ok(CommandResult {
        code: output.status.code(),
        stdout,
        stderr,
    })
}

fn print_gate_failure(command: &str, result: &CommandResult) {
    eprintln!("style-audit: gate failed: {command}");
    if !result.stdout.trim().is_empty() {
        eprintln!("{}", trim_output(&result.stdout));
    }
    if !result.stderr.trim().is_empty() {
        eprintln!("{}", trim_output(&result.stderr));
    }
}

fn trim_output(output: &str) -> String {
    const MAX_LINES: usize = 160;
    let lines = output.lines().collect::<Vec<_>>();
    if lines.len() <= MAX_LINES {
        return output.trim_end().to_string();
    }
    let mut trimmed = lines[..MAX_LINES].join("\n");
    trimmed.push_str("\n... output truncated ...");
    trimmed
}

fn print_style_summary(label: &str, file_count: usize, violations: &[Violation]) {
    if violations.is_empty() {
        println!("style-audit: passed ({file_count} {label} Rust file(s) checked)");
        return;
    }

    println!(
        "style-audit: failed ({} violation(s) in {file_count} {label} Rust file(s))",
        violations.len()
    );
    let mut by_file: BTreeMap<&str, Vec<&Violation>> = BTreeMap::new();
    for violation in violations {
        by_file.entry(&violation.path).or_default().push(violation);
    }
    for (path, file_violations) in by_file {
        println!("{path}");
        for violation in file_violations {
            println!(
                "  {}: {} - {}",
                violation.line, violation.rule, violation.message
            );
        }
    }
}

fn audit_content(path: &str, content: &str) -> Vec<Violation> {
    let lines = content.lines().map(str::to_string).collect::<Vec<_>>();
    let parse_content = parseable_rust_content(content);
    let parsed = match syn::parse_file(&parse_content) {
        Ok(parsed) => parsed,
        Err(err) => {
            return vec![Violation {
                path: path.to_string(),
                line: err.span().start().line.max(1),
                rule: "parse",
                message: err.to_string(),
            }];
        }
    };

    let mut out = Vec::new();
    check_top_level_order(path, &parsed.items, &mut out);
    check_tests_module(path, &parsed.items, &mut out);
    check_test_import_scope(path, &parsed.items, &mut out);
    check_impl_adjacency(path, &parsed.items, &mut out);
    check_visible_docs(path, &parsed.items, &lines, &mut out);
    check_function_attributes(path, &parsed.items, &mut out);
    check_qualified_paths(path, &parsed.items, &mut out);
    out.sort_by(|a, b| {
        a.path
            .cmp(&b.path)
            .then(a.line.cmp(&b.line))
            .then(a.rule.cmp(b.rule))
            .then(a.message.cmp(&b.message))
    });
    out
}

fn parseable_rust_content(content: &str) -> String {
    let had_trailing_newline = content.ends_with('\n');
    let mut lines = content.lines().map(str::to_string).collect::<Vec<_>>();
    let mut idx = 0;
    if lines
        .first()
        .is_some_and(|line| line.starts_with("#!") && !line.starts_with("#!["))
    {
        lines[0].clear();
        idx = 1;
    }
    if lines.get(idx).is_some_and(|line| line.trim() == "---") {
        lines[idx].clear();
        idx += 1;
        while idx < lines.len() {
            let closes_front_matter = lines[idx].trim() == "---";
            lines[idx].clear();
            idx += 1;
            if closes_front_matter {
                break;
            }
        }
    }
    let mut content = lines.join("\n");
    if had_trailing_newline {
        content.push('\n');
    }
    content
}

fn check_top_level_order(path: &str, items: &[Item], out: &mut Vec<Violation>) {
    let mut max_group: Option<ItemGroup> = None;
    for item in items {
        let Some((group, label)) = item_group(item) else {
            continue;
        };
        if let Some(previous) = max_group {
            if group < previous {
                out.push(violation(
                    path,
                    span_line(item.span()),
                    "top-level-order",
                    format!(
                        "{label} appears after {}; expected order is uses, constants, types, public/high-level functions, private helpers, tests",
                        group_label(previous)
                    ),
                ));
            }
        }
        max_group = Some(max_group.map_or(group, |previous| previous.max(group)));
    }
}

fn check_tests_module(path: &str, items: &[Item], out: &mut Vec<Violation>) {
    let mut tests_modules = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        match item {
            Item::Mod(item_mod) if is_tests_module(item_mod) => tests_modules.push((idx, item_mod)),
            Item::Mod(item_mod) if has_cfg_test(&item_mod.attrs) => out.push(violation(
                path,
                span_line(item_mod.mod_token.span),
                "tests-module",
                format!(
                    "test-only module `{}` should be the single `tests` module",
                    item_mod.ident
                ),
            )),
            Item::Fn(item_fn) if has_test_attr(&item_fn.attrs) => out.push(violation(
                path,
                span_line(item_fn.sig.fn_token.span),
                "tests-module",
                "top-level #[test] functions should live inside `#[cfg(test)] mod tests`"
                    .to_string(),
            )),
            _ => {}
        }
    }

    if tests_modules.len() > 1 {
        for (_, item_mod) in tests_modules.iter().skip(1) {
            out.push(violation(
                path,
                span_line(item_mod.mod_token.span),
                "tests-module",
                "only one top-level `#[cfg(test)] mod tests` module is allowed".to_string(),
            ));
        }
    }

    if let Some((idx, item_mod)) = tests_modules.first() {
        for item in items.iter().skip(idx + 1) {
            out.push(violation(
                path,
                span_line(item.span()),
                "tests-bottom",
                format!(
                    "`#[cfg(test)] mod {}` should be the last top-level item",
                    item_mod.ident
                ),
            ));
        }
    }
}

fn check_test_import_scope(path: &str, items: &[Item], out: &mut Vec<Violation>) {
    let candidates = test_import_candidates(items);
    if candidates.is_empty() {
        return;
    }

    let imported_names = candidates
        .iter()
        .flat_map(|candidate| candidate.names.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut visitor = TestImportScopeVisitor {
        imported_names: &imported_names,
        usage: BTreeMap::new(),
        test_module_depth: 0,
    };
    for item in items {
        visitor.visit_item(item);
    }

    for candidate in candidates {
        let test_only_names = candidate
            .names
            .iter()
            .filter(|name| {
                visitor
                    .usage
                    .get(*name)
                    .is_some_and(TestImportUsage::is_test_module_only)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !test_only_names.is_empty() {
            out.push(violation(
                path,
                candidate.line,
                "test-import-scope",
                format!(
                    "{} only used from test-only modules; move the import into the module that directly uses it",
                    format_imported_names(&test_only_names)
                ),
            ));
        }
    }
}

fn test_import_candidates(items: &[Item]) -> Vec<TestImportCandidate> {
    items
        .iter()
        .filter_map(|item| match item {
            Item::Use(item_use)
                if has_cfg_test(&item_use.attrs)
                    && matches!(item_use.vis, Visibility::Inherited) =>
            {
                test_import_candidate(item_use)
            }
            _ => None,
        })
        .collect()
}

fn test_import_candidate(item_use: &ItemUse) -> Option<TestImportCandidate> {
    let mut names = BTreeSet::new();
    collect_use_tree_names(&item_use.tree, &mut names);
    (!names.is_empty()).then(|| TestImportCandidate {
        line: span_line(item_use.use_token.span),
        names,
    })
}

fn collect_use_tree_names(tree: &UseTree, names: &mut BTreeSet<String>) {
    match tree {
        UseTree::Path(use_path) => collect_use_tree_names(&use_path.tree, names),
        UseTree::Name(use_name) => {
            let ident = use_name.ident.to_string();
            if !ignored_import_name(&ident) {
                names.insert(ident);
            }
        }
        UseTree::Rename(use_rename) => {
            let ident = use_rename.rename.to_string();
            if !ignored_import_name(&ident) {
                names.insert(ident);
            }
        }
        UseTree::Group(use_group) => {
            for tree in &use_group.items {
                collect_use_tree_names(tree, names);
            }
        }
        UseTree::Glob(_) => {}
    }
}

fn ignored_import_name(ident: &str) -> bool {
    matches!(ident, "_" | "self")
}

fn format_imported_names(names: &[String]) -> String {
    match names {
        [] => "import".to_string(),
        [name] => format!("`{name}` is"),
        _ => format!(
            "imports {} are",
            names
                .iter()
                .map(|name| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn check_impl_adjacency(path: &str, items: &[Item], out: &mut Vec<Violation>) {
    let local_types = items
        .iter()
        .filter_map(type_def_name)
        .collect::<BTreeSet<_>>();
    let mut inherent_impls: BTreeMap<InherentImplContext, Vec<usize>> = BTreeMap::new();

    for (idx, item) in items.iter().enumerate() {
        let Item::Impl(item_impl) = item else {
            continue;
        };
        if item_impl.trait_.is_none() {
            inherent_impls
                .entry(inherent_impl_context(item_impl))
                .or_default()
                .push(span_line(item_impl.impl_token.span));
        }

        let Some(self_name) = impl_self_name(item_impl) else {
            continue;
        };
        if !local_types.contains(&self_name) {
            continue;
        }

        let previous = idx.checked_sub(1).and_then(|prev| items.get(prev));
        let adjacent = previous.is_some_and(|prev| {
            type_def_name(prev).as_deref() == Some(self_name.as_str())
                || matches!(prev, Item::Impl(prev_impl) if impl_self_name(prev_impl).as_deref() == Some(self_name.as_str()))
        });
        if !adjacent {
            out.push(violation(
                path,
                span_line(item_impl.impl_token.span),
                "impl-adjacency",
                format!("impl for `{self_name}` should directly follow its type definition"),
            ));
        }
    }

    for (context, lines) in inherent_impls {
        if lines.len() > 1 {
            for line in lines.into_iter().skip(1) {
                out.push(violation(
                    path,
                    line,
                    "impl-merge",
                    format!(
                        "multiple inherent impl blocks for `{}` have identical constraints; merge them",
                        context.display_name
                    ),
                ));
            }
        }
    }
}

fn inherent_impl_context(item_impl: &ItemImpl) -> InherentImplContext {
    InherentImplContext {
        self_ty: normalized_tokens(item_impl.self_ty.as_ref()),
        display_name: impl_self_name(item_impl)
            .unwrap_or_else(|| normalized_tokens(item_impl.self_ty.as_ref())),
        generic_params: normalized_tokens(&item_impl.generics.params),
        where_clause: item_impl
            .generics
            .where_clause
            .as_ref()
            .map(normalized_tokens),
        attrs: item_impl
            .attrs
            .iter()
            .filter(|attr| !is_doc_attr(attr))
            .map(normalized_tokens)
            .collect(),
        is_default: item_impl.defaultness.is_some(),
        is_unsafe: item_impl.unsafety.is_some(),
    }
}

fn normalized_tokens<T: ToTokens + ?Sized>(value: &T) -> String {
    value.to_token_stream().to_string()
}

fn check_visible_docs(path: &str, items: &[Item], lines: &[String], out: &mut Vec<Violation>) {
    for item in items {
        match item {
            Item::Const(item_const) if is_public_or_crate(&item_const.vis) => check_doc_target(
                path,
                lines,
                &item_const.attrs,
                span_line(item_const.const_token.span),
                "public-doc",
                format!(
                    "visible const `{}` requires a `///` doc comment",
                    item_const.ident
                ),
                out,
            ),
            Item::Enum(item_enum) if is_public_or_crate(&item_enum.vis) => check_doc_target(
                path,
                lines,
                &item_enum.attrs,
                span_line(item_enum.enum_token.span),
                "public-doc",
                format!(
                    "visible enum `{}` requires a `///` doc comment",
                    item_enum.ident
                ),
                out,
            ),
            Item::Fn(item_fn) if is_public_or_crate(&item_fn.vis) => check_doc_target(
                path,
                lines,
                &item_fn.attrs,
                span_line(item_fn.sig.fn_token.span),
                "public-doc",
                format!(
                    "visible function `{}` requires a `///` doc comment",
                    item_fn.sig.ident
                ),
                out,
            ),
            Item::Static(item_static) if is_public_or_crate(&item_static.vis) => check_doc_target(
                path,
                lines,
                &item_static.attrs,
                span_line(item_static.static_token.span),
                "public-doc",
                format!(
                    "visible static `{}` requires a `///` doc comment",
                    item_static.ident
                ),
                out,
            ),
            Item::Struct(item_struct) => {
                if is_public_or_crate(&item_struct.vis) {
                    check_doc_target(
                        path,
                        lines,
                        &item_struct.attrs,
                        span_line(item_struct.struct_token.span),
                        "public-doc",
                        format!(
                            "visible struct `{}` requires a `///` doc comment",
                            item_struct.ident
                        ),
                        out,
                    );
                }
                check_fields(path, lines, &item_struct.fields, out);
            }
            Item::Trait(item_trait) => {
                if is_public_or_crate(&item_trait.vis) {
                    check_trait_docs(path, lines, item_trait, out);
                }
            }
            Item::Type(item_type) if is_public_or_crate(&item_type.vis) => check_doc_target(
                path,
                lines,
                &item_type.attrs,
                span_line(item_type.type_token.span),
                "public-doc",
                format!(
                    "visible type alias `{}` requires a `///` doc comment",
                    item_type.ident
                ),
                out,
            ),
            Item::Union(item_union) => {
                if is_public_or_crate(&item_union.vis) {
                    check_doc_target(
                        path,
                        lines,
                        &item_union.attrs,
                        span_line(item_union.union_token.span),
                        "public-doc",
                        format!(
                            "visible union `{}` requires a `///` doc comment",
                            item_union.ident
                        ),
                        out,
                    );
                }
                check_fields(path, lines, &Fields::Named(item_union.fields.clone()), out);
            }
            Item::Impl(item_impl) => check_impl_method_docs(path, lines, item_impl, out),
            _ => {}
        }
    }
}

fn check_fields(path: &str, lines: &[String], fields: &Fields, out: &mut Vec<Violation>) {
    match fields {
        Fields::Named(named) => {
            for field in &named.named {
                if !is_public_or_crate(&field.vis) {
                    continue;
                }
                let name = field
                    .ident
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<unnamed>".to_string());
                check_doc_target(
                    path,
                    lines,
                    &field.attrs,
                    span_line(field.span()),
                    "public-doc",
                    format!("visible field `{name}` requires a `///` doc comment"),
                    out,
                );
            }
        }
        Fields::Unnamed(unnamed) => {
            for (idx, field) in unnamed.unnamed.iter().enumerate() {
                if !is_public_or_crate(&field.vis) {
                    continue;
                }
                check_doc_target(
                    path,
                    lines,
                    &field.attrs,
                    span_line(field.span()),
                    "public-doc",
                    format!("visible tuple field #{idx} requires a `///` doc comment"),
                    out,
                );
            }
        }
        Fields::Unit => {}
    }
}

fn check_trait_docs(
    path: &str,
    lines: &[String],
    item_trait: &ItemTrait,
    out: &mut Vec<Violation>,
) {
    check_doc_target(
        path,
        lines,
        &item_trait.attrs,
        span_line(item_trait.trait_token.span),
        "public-doc",
        format!(
            "visible trait `{}` requires a `///` doc comment",
            item_trait.ident
        ),
        out,
    );
    for item in &item_trait.items {
        if let TraitItem::Fn(method) = item {
            check_doc_target(
                path,
                lines,
                &method.attrs,
                span_line(method.sig.fn_token.span),
                "public-doc",
                format!(
                    "visible trait method `{}` requires a `///` doc comment",
                    method.sig.ident
                ),
                out,
            );
        }
    }
}

fn check_impl_method_docs(
    path: &str,
    lines: &[String],
    item_impl: &ItemImpl,
    out: &mut Vec<Violation>,
) {
    for item in &item_impl.items {
        if let ImplItem::Fn(method) = item {
            if is_public_or_crate(&method.vis) {
                check_doc_target(
                    path,
                    lines,
                    &method.attrs,
                    span_line(method.sig.fn_token.span),
                    "public-doc",
                    format!(
                        "visible method `{}` requires a `///` doc comment",
                        method.sig.ident
                    ),
                    out,
                );
            }
        }
    }
}

fn check_doc_target(
    path: &str,
    lines: &[String],
    attrs: &[Attribute],
    line: usize,
    rule: &'static str,
    missing_message: String,
    out: &mut Vec<Violation>,
) {
    if !has_triple_slash_doc(attrs, lines) {
        out.push(violation(path, line, rule, missing_message));
    }
    check_doc_before_attrs(path, lines, attrs, line, out);
}

fn check_doc_before_attrs(
    path: &str,
    lines: &[String],
    attrs: &[Attribute],
    fallback_line: usize,
    out: &mut Vec<Violation>,
) {
    let first_non_doc = attrs
        .iter()
        .filter(|attr| !is_doc_attr(attr))
        .map(|attr| span_line(attr.span()))
        .min();
    let Some(first_non_doc) = first_non_doc else {
        return;
    };
    for attr in attrs.iter().filter(|attr| is_doc_attr(attr)) {
        let doc_line = span_line(attr.span());
        if doc_line > first_non_doc {
            out.push(violation(
                path,
                doc_line.max(fallback_line),
                "doc-attribute-order",
                "documentation comments must be above attributes".to_string(),
            ));
            return;
        }
        if !line_starts_with(lines, doc_line, "///") {
            out.push(violation(
                path,
                doc_line,
                "public-doc",
                "documentation must use `///` comments, not #[doc] attributes".to_string(),
            ));
        }
    }
}

fn check_function_attributes(path: &str, items: &[Item], out: &mut Vec<Violation>) {
    for item in items {
        match item {
            Item::Fn(item_fn) => check_attrs_adjacent(
                path,
                &item_fn.attrs,
                span_line(item_fn.sig.fn_token.span),
                out,
            ),
            Item::Impl(item_impl) => {
                for item in &item_impl.items {
                    if let ImplItem::Fn(method) = item {
                        check_attrs_adjacent(
                            path,
                            &method.attrs,
                            span_line(method.sig.fn_token.span),
                            out,
                        );
                    }
                }
            }
            Item::Trait(item_trait) => {
                for item in &item_trait.items {
                    if let TraitItem::Fn(method) = item {
                        check_attrs_adjacent(
                            path,
                            &method.attrs,
                            span_line(method.sig.fn_token.span),
                            out,
                        );
                    }
                }
            }
            _ => {}
        }
    }
}

fn check_attrs_adjacent(
    path: &str,
    attrs: &[Attribute],
    signature_line: usize,
    out: &mut Vec<Violation>,
) {
    let Some(last_attr_end) = attrs.iter().map(|attr| attr.span().end().line).max() else {
        return;
    };
    if last_attr_end + 1 != signature_line {
        out.push(violation(
            path,
            signature_line,
            "attribute-adjacency",
            "function attributes must be adjacent to the function signature".to_string(),
        ));
    }
}

fn check_qualified_paths(path: &str, items: &[Item], out: &mut Vec<Violation>) {
    let mut visitor = QualifiedPathVisitor {
        file_path: path,
        seen: BTreeSet::new(),
        generic_type_params: Vec::new(),
        violations: Vec::new(),
    };
    for item in items {
        if matches!(item, Item::Use(_)) {
            continue;
        }
        visitor.visit_item(item);
    }
    out.extend(visitor.violations);
}

fn item_group(item: &Item) -> Option<(ItemGroup, &'static str)> {
    match item {
        Item::Use(_) => Some((ItemGroup::Use, "use import")),
        Item::Const(_) | Item::Static(_) => Some((ItemGroup::Constant, "constant")),
        Item::Enum(_) | Item::Struct(_) | Item::Trait(_) | Item::Type(_) | Item::Union(_) => {
            Some((ItemGroup::Type, "type definition"))
        }
        Item::Fn(item_fn) if is_public_or_crate(&item_fn.vis) => {
            Some((ItemGroup::PublicFunction, "public/high-level function"))
        }
        Item::Fn(_) => Some((ItemGroup::PrivateFunction, "private helper function")),
        Item::Mod(item_mod) if is_tests_module(item_mod) => Some((ItemGroup::Tests, "tests")),
        _ => None,
    }
}

fn group_label(group: ItemGroup) -> &'static str {
    match group {
        ItemGroup::Use => "use imports",
        ItemGroup::Constant => "constants",
        ItemGroup::Type => "type definitions",
        ItemGroup::PublicFunction => "public/high-level functions",
        ItemGroup::PrivateFunction => "private helper functions",
        ItemGroup::Tests => "tests",
    }
}

fn type_def_name(item: &Item) -> Option<String> {
    match item {
        Item::Enum(item) => Some(item.ident.to_string()),
        Item::Struct(item) => Some(item.ident.to_string()),
        Item::Trait(item) => Some(item.ident.to_string()),
        Item::Type(item) => Some(item.ident.to_string()),
        Item::Union(item) => Some(item.ident.to_string()),
        _ => None,
    }
}

fn impl_self_name(item_impl: &ItemImpl) -> Option<String> {
    match item_impl.self_ty.as_ref() {
        Type::Path(type_path) if type_path.qself.is_none() => type_path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string()),
        Type::Reference(reference) => match reference.elem.as_ref() {
            Type::Path(type_path) if type_path.qself.is_none() => type_path
                .path
                .segments
                .last()
                .map(|segment| segment.ident.to_string()),
            _ => None,
        },
        _ => None,
    }
}

fn is_tests_module(item_mod: &ItemMod) -> bool {
    item_mod.ident == "tests" && has_cfg_test(&item_mod.attrs)
}

fn has_cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .meta
                .require_list()
                .is_ok_and(|list| list.tokens.to_string().contains("test"))
    })
}

fn has_test_attr(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident("test"))
}

fn has_triple_slash_doc(attrs: &[Attribute], lines: &[String]) -> bool {
    attrs
        .iter()
        .filter(|attr| is_doc_attr(attr))
        .any(|attr| line_starts_with(lines, span_line(attr.span()), "///"))
}

fn is_doc_attr(attr: &Attribute) -> bool {
    attr.path().is_ident("doc")
}

fn is_public_or_crate(vis: &Visibility) -> bool {
    match vis {
        Visibility::Public(_) => true,
        Visibility::Restricted(restricted) => restricted
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "crate"),
        Visibility::Inherited => false,
    }
}

fn line_starts_with(lines: &[String], line: usize, prefix: &str) -> bool {
    lines
        .get(line.saturating_sub(1))
        .is_some_and(|value| value.trim_start().starts_with(prefix))
}

fn span_line(span: Span) -> usize {
    span.start().line.max(1)
}

fn violation(path: &str, line: usize, rule: &'static str, message: String) -> Violation {
    Violation {
        path: path.to_string(),
        line,
        rule,
        message,
    }
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
