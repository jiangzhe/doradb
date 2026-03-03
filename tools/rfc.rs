#!/usr/bin/env -S cargo +nightly -q -Zscript
---
[package]
edition = "2024"

[dependencies]
serde_json = "1"
---

use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const RFC_DIR: &str = "docs/rfcs";
const RFC_TEMPLATE: &str = "docs/rfcs/0000-template.md";
const TASK_DIR: &str = "docs/tasks";
const BACKLOG_DIR: &str = "docs/backlogs";
const BACKLOG_CLOSED_DIR: &str = "docs/backlogs/closed";
const DOC_ID_TOOL: &str = "tools/doc-id.rs";

#[derive(Debug, Clone)]
struct PhaseTracking {
    phase_title: String,
    task_doc: Option<String>,
    task_issue: Option<i64>,
    phase_status: Option<String>,
    implementation_summary: Option<String>,
    related_backlogs: Vec<String>,
}

#[derive(Debug, Clone)]
struct TrackingExtract {
    legacy: bool,
    phases: Vec<PhaseTracking>,
    inferred_tasks: Vec<String>,
    inferred_backlogs: Vec<String>,
}

#[derive(Debug, Clone)]
struct ValidateResult {
    ok: bool,
    legacy: bool,
    status: Option<String>,
    errors: Vec<String>,
    warnings: Vec<String>,
}

fn usage() -> &'static str {
    "Usage: tools/rfc.rs <subcommand> [options]\n\n\
Subcommands:\n\
  next-rfc-id           Print next RFC id from docs/rfcs/<id>-<slug>.md\n\
  create-rfc-doc        Create docs/rfcs RFC doc from template\n\
  validate-rfc-doc      Validate RFC structure and evidence gates\n\
  extract-rfc-tracking  Extract implementation phase/task/backlog tracking as JSON\n\
  precheck-rfc-resolve  Strict resolve readiness checks for RFC issue closure\n"
}

fn next_rfc_id_usage() -> &'static str {
    "Usage: tools/rfc.rs next-rfc-id [--dir <path>]"
}

fn create_rfc_doc_usage() -> &'static str {
    "Usage: tools/rfc.rs create-rfc-doc --title <title> --slug <slug> (--id <4digits> | --auto-id) [--template <path>] [--output-dir <path>] [--force]"
}

fn validate_rfc_doc_usage() -> &'static str {
    "Usage: tools/rfc.rs validate-rfc-doc --doc <docs/rfcs/<4digits>-<slug>.md> [--stage draft|formal|resolve] [--allow-legacy]"
}

fn extract_rfc_tracking_usage() -> &'static str {
    "Usage: tools/rfc.rs extract-rfc-tracking --doc <docs/rfcs/<4digits>-<slug>.md> [--allow-legacy]"
}

fn precheck_rfc_resolve_usage() -> &'static str {
    "Usage: tools/rfc.rs precheck-rfc-resolve --doc <docs/rfcs/<4digits>-<slug>.md> [--allow-legacy]"
}

fn main() {
    if let Err(code) = run() {
        std::process::exit(code);
    }
}

fn run() -> Result<(), i32> {
    let mut args = env::args().skip(1);
    let Some(subcommand) = args.next() else {
        eprintln!("{}", usage());
        return Err(1);
    };

    match subcommand.as_str() {
        "-h" | "--help" => {
            println!("{}", usage());
            Ok(())
        }
        "next-rfc-id" => cmd_next_rfc_id(args),
        "create-rfc-doc" => cmd_create_rfc_doc(args),
        "validate-rfc-doc" => cmd_validate_rfc_doc(args),
        "extract-rfc-tracking" => cmd_extract_rfc_tracking(args),
        "precheck-rfc-resolve" => cmd_precheck_rfc_resolve(args),
        _ => {
            eprintln!("unknown subcommand: {subcommand}\n{}", usage());
            Err(1)
        }
    }
}

fn cmd_next_rfc_id(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dir" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --dir\n{}", next_rfc_id_usage());
                    return Err(1);
                };
                if v != RFC_DIR {
                    eprintln!("--dir is no longer configurable; use default {RFC_DIR}");
                    return Err(1);
                }
            }
            "-h" | "--help" => {
                println!("{}", next_rfc_id_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", next_rfc_id_usage());
                return Err(1);
            }
        }
    }

    let id =
        run_doc_id_and_capture(["peek-next-id", "--kind", "rfc"]).map_err(print_and_exit)?;
    println!("{id}");
    Ok(())
}

fn cmd_create_rfc_doc(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut title: Option<String> = None;
    let mut slug: Option<String> = None;
    let mut rfc_id: Option<String> = None;
    let mut auto_id = false;
    let mut template = PathBuf::from(RFC_TEMPLATE);
    let mut output_dir = PathBuf::from(RFC_DIR);
    let mut force = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--title" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --title\n{}", create_rfc_doc_usage());
                    return Err(1);
                };
                title = Some(v);
            }
            "--slug" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --slug\n{}", create_rfc_doc_usage());
                    return Err(1);
                };
                slug = Some(v);
            }
            "--id" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --id\n{}", create_rfc_doc_usage());
                    return Err(1);
                };
                rfc_id = Some(validate_fixed_id(&v, 4).map_err(print_and_exit)?);
            }
            "--auto-id" => {
                auto_id = true;
            }
            "--template" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --template\n{}", create_rfc_doc_usage());
                    return Err(1);
                };
                template = PathBuf::from(v);
            }
            "--output-dir" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --output-dir\n{}", create_rfc_doc_usage());
                    return Err(1);
                };
                output_dir = PathBuf::from(v);
            }
            "--force" => {
                force = true;
            }
            "-h" | "--help" => {
                println!("{}", create_rfc_doc_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", create_rfc_doc_usage());
                return Err(1);
            }
        }
    }

    let Some(title) = title else {
        eprintln!("missing required arg: --title\n{}", create_rfc_doc_usage());
        return Err(1);
    };
    let Some(slug_raw) = slug else {
        eprintln!("missing required arg: --slug\n{}", create_rfc_doc_usage());
        return Err(1);
    };
    let slug = validate_slug(&slug_raw).map_err(print_and_exit)?;

    if auto_id && rfc_id.is_some() {
        eprintln!("use either --id or --auto-id, not both");
        return Err(1);
    }
    if !auto_id && rfc_id.is_none() {
        eprintln!(
            "one of --id or --auto-id is required\n{}",
            create_rfc_doc_usage()
        );
        return Err(1);
    }

    let rfc_id = if auto_id {
        run_doc_id_and_capture(["alloc-id", "--kind", "rfc"]).map_err(print_and_exit)?
    } else {
        rfc_id.expect("checked is_some")
    };

    let template_text = fs::read_to_string(&template)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(&template)))
        .map_err(print_and_exit)?;

    let mut content = apply_title(template_text, &rfc_id, &title).map_err(print_and_exit)?;
    content = set_frontmatter_field(&content, "id", &rfc_id).map_err(print_and_exit)?;
    content = set_frontmatter_field(&content, "title", &title).map_err(print_and_exit)?;
    content = set_frontmatter_field(&content, "status", "draft").map_err(print_and_exit)?;

    let out_path = output_dir.join(format!("{rfc_id}-{slug}.md"));
    if out_path.exists() && !force {
        eprintln!(
            "output file already exists: {} (use --force to overwrite)",
            normalize_path(&out_path)
        );
        return Err(1);
    }

    fs::write(&out_path, content)
        .map_err(|e| format!("failed to write {}: {e}", normalize_path(&out_path)))
        .map_err(print_and_exit)?;
    println!("{}", normalize_path(&out_path));
    Ok(())
}

fn cmd_validate_rfc_doc(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut doc: Option<PathBuf> = None;
    let mut stage = "formal".to_string();
    let mut allow_legacy = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--doc" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --doc\n{}", validate_rfc_doc_usage());
                    return Err(1);
                };
                doc = Some(PathBuf::from(v));
            }
            "--stage" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --stage\n{}", validate_rfc_doc_usage());
                    return Err(1);
                };
                if v != "draft" && v != "formal" && v != "resolve" {
                    eprintln!("invalid --stage: {v}");
                    return Err(1);
                }
                stage = v;
            }
            "--allow-legacy" => {
                allow_legacy = true;
            }
            "-h" | "--help" => {
                println!("{}", validate_rfc_doc_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", validate_rfc_doc_usage());
                return Err(1);
            }
        }
    }

    let Some(doc) = doc else {
        eprintln!("missing required arg: --doc\n{}", validate_rfc_doc_usage());
        return Err(1);
    };

    let result = validate_rfc_document(&doc, &stage, allow_legacy);
    print_json(&json!({
        "ok": result.ok,
        "legacy": result.legacy,
        "status": result.status,
        "errors": result.errors,
        "warnings": result.warnings,
        "doc": normalize_path(&doc),
        "stage": stage,
    }));

    if result.ok {
        Ok(())
    } else {
        Err(1)
    }
}

fn cmd_extract_rfc_tracking(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut doc: Option<PathBuf> = None;
    let mut allow_legacy = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--doc" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --doc\n{}", extract_rfc_tracking_usage());
                    return Err(1);
                };
                doc = Some(PathBuf::from(v));
            }
            "--allow-legacy" => {
                allow_legacy = true;
            }
            "-h" | "--help" => {
                println!("{}", extract_rfc_tracking_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", extract_rfc_tracking_usage());
                return Err(1);
            }
        }
    }

    let Some(doc) = doc else {
        eprintln!("missing required arg: --doc\n{}", extract_rfc_tracking_usage());
        return Err(1);
    };

    let text = fs::read_to_string(&doc)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(&doc)))
        .map_err(print_and_exit)?;
    validate_rfc_doc_path(&doc).map_err(print_and_exit)?;

    let tracking = extract_tracking(&text, allow_legacy).map_err(print_and_exit)?;
    print_json(&tracking_to_json(&doc, &text, &tracking));
    Ok(())
}

fn cmd_precheck_rfc_resolve(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut doc: Option<PathBuf> = None;
    let mut allow_legacy = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--doc" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --doc\n{}", precheck_rfc_resolve_usage());
                    return Err(1);
                };
                doc = Some(PathBuf::from(v));
            }
            "--allow-legacy" => {
                allow_legacy = true;
            }
            "-h" | "--help" => {
                println!("{}", precheck_rfc_resolve_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", precheck_rfc_resolve_usage());
                return Err(1);
            }
        }
    }

    let Some(doc) = doc else {
        eprintln!("missing required arg: --doc\n{}", precheck_rfc_resolve_usage());
        return Err(1);
    };

    let mut failures = Vec::new();
    let mut warnings = Vec::new();

    validate_rfc_doc_path(&doc).map_err(print_and_exit)?;
    let text = fs::read_to_string(&doc)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(&doc)))
        .map_err(print_and_exit)?;

    let val = validate_rfc_document(&doc, "resolve", allow_legacy);
    if !val.ok {
        failures.extend(val.errors.clone());
    }
    warnings.extend(val.warnings.clone());

    let frontmatter = parse_frontmatter(&text);
    let status = frontmatter.get("status").map(|x| normalize_frontmatter_value(x));
    match status.as_deref() {
        Some("implemented") | Some("superseded") => {}
        Some(v) => failures.push(format!(
            "RFC status must be implemented or superseded for resolve precheck; found {v}"
        )),
        None => failures.push("missing frontmatter `status`".to_string()),
    }

    let tracking = match extract_tracking(&text, allow_legacy) {
        Ok(v) => v,
        Err(e) => {
            failures.push(e);
            TrackingExtract {
                legacy: allow_legacy,
                phases: Vec::new(),
                inferred_tasks: Vec::new(),
                inferred_backlogs: Vec::new(),
            }
        }
    };

    let mut task_docs = BTreeSet::new();
    let mut task_issues = BTreeSet::new();
    let mut backlog_refs = BTreeSet::new();

    if tracking.legacy {
        task_docs.extend(tracking.inferred_tasks.iter().cloned());
        backlog_refs.extend(tracking.inferred_backlogs.iter().cloned());
        if task_docs.is_empty() {
            warnings.push("legacy RFC has no inferred task docs".to_string());
        }
    } else {
        if tracking.phases.is_empty() {
            failures.push("no parseable implementation phases found".to_string());
        }
        for phase in &tracking.phases {
            match phase.task_doc.as_deref() {
                Some(path) => {
                    task_docs.insert(path.to_string());
                }
                None => {
                    failures.push(format!(
                        "phase `{}` missing `Task Doc` entry",
                        phase.phase_title
                    ));
                }
            }
            if let Some(issue) = phase.task_issue {
                task_issues.insert(issue);
            } else {
                failures.push(format!(
                    "phase `{}` missing `Task Issue` entry",
                    phase.phase_title
                ));
            }

            let summary = phase
                .implementation_summary
                .as_deref()
                .map(str::trim)
                .unwrap_or("");
            if summary.is_empty() || summary.eq_ignore_ascii_case("pending") {
                failures.push(format!(
                    "phase `{}` has empty `Implementation Summary`",
                    phase.phase_title
                ));
            }

            for b in &phase.related_backlogs {
                backlog_refs.insert(b.clone());
            }
        }
    }

    for task_doc in &task_docs {
        let path = PathBuf::from(task_doc);
        if let Err(e) = validate_task_doc_path(&path) {
            failures.push(format!("task doc `{task_doc}` invalid: {e}"));
            continue;
        }
        let task_text = match fs::read_to_string(&path) {
            Ok(v) => v,
            Err(e) => {
                failures.push(format!("failed to read {task_doc}: {e}"));
                continue;
            }
        };
        let notes = extract_section_text(&task_text, "Implementation Notes").unwrap_or_default();
        if is_empty_implementation_notes(&notes) {
            failures.push(format!(
                "task doc `{task_doc}` has empty `Implementation Notes`"
            ));
        }
    }

    for issue_id in &task_issues {
        match issue_state(*issue_id) {
            Ok(state) => {
                if state != "CLOSED" {
                    failures.push(format!(
                        "task issue #{issue_id} is not closed (state={state})"
                    ));
                }
            }
            Err(e) => failures.push(format!("failed to check task issue #{issue_id}: {e}")),
        }
    }

    for backlog in &backlog_refs {
        match backlog_close_state(backlog) {
            Ok(BacklogState::Closed(path)) => {
                let _ = path;
            }
            Ok(BacklogState::Open(path)) => failures.push(format!(
                "related backlog still open: {}",
                normalize_path(&path)
            )),
            Ok(BacklogState::Missing) => {
                failures.push(format!("related backlog not found: {backlog}"))
            }
            Err(e) => failures.push(format!("failed backlog check for {backlog}: {e}")),
        }
    }

    let ok = failures.is_empty();
    print_json(&json!({
        "ok": ok,
        "doc": normalize_path(&doc),
        "legacy": tracking.legacy,
        "status": status,
        "failures": failures,
        "warnings": warnings,
        "checked": {
            "task_docs": task_docs,
            "task_issues": task_issues,
            "related_backlogs": backlog_refs,
        }
    }));

    if ok {
        Ok(())
    } else {
        Err(1)
    }
}

fn print_and_exit(err: String) -> i32 {
    eprintln!("{err}");
    1
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn run_doc_id_and_capture<const N: usize>(args: [&str; N]) -> Result<String, String> {
    let out = Command::new(DOC_ID_TOOL)
        .args(args)
        .output()
        .map_err(|e| format!("failed to execute {DOC_ID_TOOL}: {e}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !stderr.is_empty() {
            return Err(stderr);
        }
        if !stdout.is_empty() {
            return Err(stdout);
        }
        return Err(format!("{DOC_ID_TOOL} returned non-zero exit code"));
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if text.is_empty() {
        return Err(format!("{DOC_ID_TOOL} returned empty output"));
    }
    Ok(text)
}

fn normalize_ref_path(path: &Path) -> PathBuf {
    let text = normalize_path(path);
    if let Some(stripped) = text.strip_prefix("./") {
        PathBuf::from(stripped)
    } else {
        PathBuf::from(text)
    }
}

fn validate_rfc_doc_path(path: &Path) -> Result<(), String> {
    let path = normalize_ref_path(path);
    if !path.exists() {
        return Err(format!("RFC doc not found: {}", normalize_path(&path)));
    }
    if !path.is_file() {
        return Err(format!("RFC doc is not a file: {}", normalize_path(&path)));
    }

    let parent = path
        .parent()
        .ok_or_else(|| format!("invalid RFC path: {}", normalize_path(&path)))?;
    if normalize_path(parent) != RFC_DIR {
        return Err(format!(
            "RFC doc must be under {RFC_DIR}: {}",
            normalize_path(&path)
        ));
    }

    let name = path
        .file_name()
        .ok_or_else(|| format!("invalid RFC path: {}", normalize_path(&path)))?
        .to_string_lossy()
        .to_string();
    if parse_rfc_name(&name).is_none() {
        return Err(format!(
            "invalid RFC file name: {} (expected <4digits>-<slug>.md)",
            normalize_path(&path)
        ));
    }

    Ok(())
}

fn validate_task_doc_path(path: &Path) -> Result<(), String> {
    let path = normalize_ref_path(path);
    if !path.exists() {
        return Err(format!("task doc not found: {}", normalize_path(&path)));
    }
    if !path.is_file() {
        return Err(format!("task doc is not a file: {}", normalize_path(&path)));
    }

    let parent = path
        .parent()
        .ok_or_else(|| format!("invalid task path: {}", normalize_path(&path)))?;
    if normalize_path(parent) != TASK_DIR {
        return Err(format!(
            "task doc must be under {TASK_DIR}: {}",
            normalize_path(&path)
        ));
    }

    let name = path
        .file_name()
        .ok_or_else(|| format!("invalid task path: {}", normalize_path(&path)))?
        .to_string_lossy()
        .to_string();
    if parse_task_name(&name).is_none() {
        return Err(format!(
            "invalid task file name: {} (expected <6digits>-<slug>.md)",
            normalize_path(&path)
        ));
    }

    Ok(())
}

fn parse_rfc_name(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let stem = &name[..name.len() - 3];
    let (id, slug) = stem.split_once('-')?;
    if id.len() != 4 || !id.bytes().all(|b| b.is_ascii_digit()) || slug.is_empty() {
        return None;
    }
    id.parse::<u32>().ok()
}

fn parse_task_name(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let stem = &name[..name.len() - 3];
    let (id, slug) = stem.split_once('-')?;
    if id.len() != 6 || !id.bytes().all(|b| b.is_ascii_digit()) || slug.is_empty() {
        return None;
    }
    id.parse::<u32>().ok()
}

fn validate_fixed_id(id: &str, width: usize) -> Result<String, String> {
    if id.len() != width || !id.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("id must be exactly {width} digits"));
    }
    Ok(id.to_string())
}

fn validate_slug(slug: &str) -> Result<String, String> {
    if slug.is_empty() {
        return Err("slug must not be empty".to_string());
    }
    let bytes = slug.as_bytes();
    if bytes.first() == Some(&b'-') || bytes.last() == Some(&b'-') {
        return Err("slug must not start/end with '-'".to_string());
    }
    if slug.contains("--") {
        return Err("slug must not contain consecutive '-'".to_string());
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
    {
        return Err("slug must use lowercase letters, digits, '-'".to_string());
    }
    Ok(slug.to_string())
}

fn apply_title(template_text: String, rfc_id: &str, title: &str) -> Result<String, String> {
    let clean = title.trim();
    if clean.is_empty() {
        return Err("title must not be empty".to_string());
    }
    let heading = format!("# RFC-{rfc_id}: {clean}");
    let trailing_newline = template_text.ends_with('\n');
    let mut lines: Vec<String> = template_text.lines().map(|s| s.to_string()).collect();
    if let Some(idx) = lines.iter().position(|line| line.starts_with("# RFC-")) {
        lines[idx] = heading;
        let mut out = lines.join("\n");
        if trailing_newline {
            out.push('\n');
        }
        return Ok(out);
    }
    Ok(format!("{heading}\n\n{template_text}"))
}

fn set_frontmatter_field(content: &str, key: &str, value: &str) -> Result<String, String> {
    let lines: Vec<&str> = content.lines().collect();
    if lines.len() < 3 || lines[0].trim() != "---" {
        return Err("missing YAML frontmatter".to_string());
    }
    let Some(end_idx) = lines.iter().enumerate().skip(1).find(|(_, v)| v.trim() == "---").map(|(i, _)| i) else {
        return Err("unclosed YAML frontmatter".to_string());
    };

    let mut fm: Vec<String> = lines[1..end_idx].iter().map(|x| (*x).to_string()).collect();
    let mut found = false;
    for line in &mut fm {
        let trimmed = line.trim_start();
        if trimmed.starts_with(&format!("{key}:")) {
            *line = format!("{key}: {value}");
            found = true;
        }
    }
    if !found {
        fm.push(format!("{key}: {value}"));
    }

    let mut out = Vec::new();
    out.push("---".to_string());
    out.extend(fm);
    out.push("---".to_string());
    out.extend(lines[end_idx + 1..].iter().map(|x| (*x).to_string()));

    let mut rebuilt = out.join("\n");
    if content.ends_with('\n') {
        rebuilt.push('\n');
    }
    Ok(rebuilt)
}

fn parse_frontmatter(content: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() || lines[0].trim() != "---" {
        return map;
    }
    let Some(end) = lines
        .iter()
        .enumerate()
        .skip(1)
        .find(|(_, v)| v.trim() == "---")
        .map(|(i, _)| i)
    else {
        return map;
    };

    for line in &lines[1..end] {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = trimmed.split_once(':') {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    map
}

fn normalize_frontmatter_value(v: &str) -> String {
    let left = v.split('#').next().unwrap_or(v).trim();
    left.trim_matches('`').trim_matches('"').to_string()
}

fn validate_rfc_document(doc: &Path, stage: &str, allow_legacy: bool) -> ValidateResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    if let Err(e) = validate_rfc_doc_path(doc) {
        errors.push(e);
        return ValidateResult {
            ok: false,
            legacy: false,
            status: None,
            errors,
            warnings,
        };
    }

    let text = match fs::read_to_string(doc) {
        Ok(v) => v,
        Err(e) => {
            errors.push(format!("failed to read {}: {e}", normalize_path(doc)));
            return ValidateResult {
                ok: false,
                legacy: false,
                status: None,
                errors,
                warnings,
            };
        }
    };

    let fm = parse_frontmatter(&text);
    let status = fm.get("status").map(|x| normalize_frontmatter_value(x));

    if status.is_none() {
        errors.push("missing frontmatter `status`".to_string());
    }

    match (stage, status.as_deref()) {
        ("draft", Some("draft")) => {}
        ("draft", Some(v)) => errors.push(format!(
            "draft validation requires status=draft (found {v})"
        )),
        ("formal", Some("proposal" | "accepted" | "implemented" | "superseded")) => {}
        ("formal", Some(v)) => errors.push(format!(
            "formal validation requires status in proposal|accepted|implemented|superseded (found {v})"
        )),
        ("resolve", Some("implemented" | "superseded")) => {}
        ("resolve", Some(v)) => warnings.push(format!(
            "resolve validation usually expects status implemented|superseded (found {v})"
        )),
        _ => {}
    }

    let required_sections: Vec<&str> = if stage == "resolve" && allow_legacy {
        vec!["Summary", "Context", "Implementation Phases", "References"]
    } else {
        vec![
            "Summary",
            "Context",
            "Design Inputs",
            "Decision",
            "Alternatives Considered",
            "Implementation Phases",
            "Consequences",
            "Open Questions",
            "Future Work",
            "References",
        ]
    };

    for required in required_sections {
        if extract_section_text(&text, required).is_none() {
            errors.push(format!("missing section: ## {required}"));
        }
    }

    if stage != "resolve" || !allow_legacy {
        if let Some(inputs) = extract_section_text(&text, "Design Inputs") {
            let docs = count_subsection_items(&inputs, "Documents");
            let code = count_subsection_items(&inputs, "Code References");
            let conv = count_subsection_items(&inputs, "Conversation References");

            if docs == 0 {
                errors.push("Design Inputs -> Documents must include at least one item".to_string());
            }
            if code == 0 {
                errors.push("Design Inputs -> Code References must include at least one item".to_string());
            }
            if conv == 0 {
                errors.push(
                    "Design Inputs -> Conversation References must include at least one item".to_string(),
                );
            }

            if stage != "draft" {
                if !subsection_has_token(&inputs, "Documents", "[D") {
                    errors.push("Documents subsection must include [D#] references".to_string());
                }
                if !subsection_has_token(&inputs, "Code References", "[C") {
                    errors.push("Code References subsection must include [C#] references".to_string());
                }
                if !subsection_has_token(&inputs, "Conversation References", "[U") {
                    errors.push("Conversation References subsection must include [U#] references".to_string());
                }
            }
        }
    } else {
        warnings.push("legacy resolve mode skips Design Inputs evidence checks".to_string());
    }

    if stage != "draft" && !(stage == "resolve" && allow_legacy) {
        if let Some(decision) = extract_section_text(&text, "Decision") {
            if !has_any_reference_token(&decision) {
                errors.push("Decision section must include input reference tokens".to_string());
            }
        }

        if let Some(alts) = extract_section_text(&text, "Alternatives Considered") {
            let alt_count = alts
                .lines()
                .filter(|line| line.trim_start().starts_with("### "))
                .count();
            if alt_count == 0 {
                errors.push(
                    "Alternatives Considered must include at least one `###` alternative subsection"
                        .to_string(),
                );
            }
            if !alts.contains("Why Not Chosen") {
                errors.push(
                    "Alternatives Considered must include explicit `Why Not Chosen` rationale"
                        .to_string(),
                );
            }
            if !has_any_reference_token(&alts) {
                errors.push(
                    "Alternatives Considered must include references to design inputs".to_string(),
                );
            }
        }
    } else if stage == "resolve" && allow_legacy {
        warnings.push("legacy resolve mode skips Decision/Alternatives evidence checks".to_string());
    }

    let tracking = extract_tracking(&text, allow_legacy);
    let mut legacy = false;
    match tracking {
        Ok(v) => {
            legacy = v.legacy;
            if !v.legacy {
                for phase in &v.phases {
                    if stage != "draft" && phase.task_doc.is_none() {
                        errors.push(format!(
                            "phase `{}` missing Task Doc entry",
                            phase.phase_title
                        ));
                    }
                    if stage != "draft" && phase.task_issue.is_none() {
                        errors.push(format!(
                            "phase `{}` missing Task Issue entry",
                            phase.phase_title
                        ));
                    }
                    if stage != "draft" {
                        let pstatus = phase.phase_status.as_deref().unwrap_or("");
                        if pstatus.is_empty() {
                            errors.push(format!(
                                "phase `{}` missing Phase Status entry",
                                phase.phase_title
                            ));
                        }
                        let isummary = phase.implementation_summary.as_deref().unwrap_or("");
                        if isummary.is_empty() {
                            errors.push(format!(
                                "phase `{}` missing Implementation Summary entry",
                                phase.phase_title
                            ));
                        }
                    }
                }
            } else {
                warnings.push("legacy RFC phase parsing fallback enabled".to_string());
            }
        }
        Err(e) => errors.push(e),
    }

    ValidateResult {
        ok: errors.is_empty(),
        legacy,
        status,
        errors,
        warnings,
    }
}

fn extract_tracking(text: &str, allow_legacy: bool) -> Result<TrackingExtract, String> {
    let phases = parse_implementation_phases(text);
    if !phases.is_empty() {
        let modern = phases.iter().all(|p| {
            p.task_doc.is_some()
                && p.task_issue.is_some()
                && p.phase_status.is_some()
                && p.implementation_summary.is_some()
        });
        if modern || !allow_legacy {
            return Ok(TrackingExtract {
                legacy: false,
                phases,
                inferred_tasks: Vec::new(),
                inferred_backlogs: Vec::new(),
            });
        }

        let inferred_tasks = extract_paths_by_prefix(text, "docs/tasks/")
            .into_iter()
            .filter(|p| p.ends_with(".md"))
            .collect::<Vec<_>>();
        let mut inferred_backlogs = extract_paths_by_prefix(text, "docs/backlogs/")
            .into_iter()
            .filter(|p| p.ends_with(".md"))
            .collect::<Vec<_>>();
        inferred_backlogs.sort();
        inferred_backlogs.dedup();

        return Ok(TrackingExtract {
            legacy: true,
            phases: Vec::new(),
            inferred_tasks,
            inferred_backlogs,
        });
    }

    if !allow_legacy {
        return Err("implementation phases are not parseable with modern phase format; use --allow-legacy for old RFC docs".to_string());
    }

    let inferred_tasks = extract_paths_by_prefix(text, "docs/tasks/")
        .into_iter()
        .filter(|p| p.ends_with(".md"))
        .collect::<Vec<_>>();

    let mut inferred_backlogs = extract_paths_by_prefix(text, "docs/backlogs/")
        .into_iter()
        .filter(|p| p.ends_with(".md"))
        .collect::<Vec<_>>();
    inferred_backlogs.sort();
    inferred_backlogs.dedup();

    Ok(TrackingExtract {
        legacy: true,
        phases: Vec::new(),
        inferred_tasks,
        inferred_backlogs,
    })
}

fn parse_implementation_phases(text: &str) -> Vec<PhaseTracking> {
    let Some(section) = extract_section_text(text, "Implementation Phases") else {
        return Vec::new();
    };

    let lines: Vec<&str> = section.lines().collect();
    let mut phase_starts = Vec::new();
    for (idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("- **Phase ") || trimmed.starts_with("### Phase ") {
            phase_starts.push(idx);
        }
    }

    if phase_starts.is_empty() {
        return Vec::new();
    }

    let mut phases = Vec::new();
    for (i, start) in phase_starts.iter().enumerate() {
        let end = phase_starts.get(i + 1).copied().unwrap_or(lines.len());
        let block = &lines[*start..end];
        phases.push(parse_phase_block(block));
    }

    phases
}

fn parse_phase_block(lines: &[&str]) -> PhaseTracking {
    let title = lines
        .first()
        .map(|x| x.trim().trim_start_matches("- ").trim().trim_matches('*').trim().to_string())
        .unwrap_or_else(|| "Phase".to_string());

    let mut task_doc = None;
    let mut task_issue = None;
    let mut phase_status = None;
    let mut implementation_summary = None;
    let mut related_backlogs = Vec::new();
    let mut in_related_backlogs = false;

    for line in lines.iter().skip(1) {
        let trimmed = line.trim();

        if trimmed.starts_with("- Related Backlogs:") {
            in_related_backlogs = true;
            continue;
        }

        if in_related_backlogs {
            if trimmed.is_empty() {
                continue;
            }
            if trimmed.starts_with("- ") || trimmed.starts_with("* ") {
                let cand = trimmed
                    .trim_start_matches('-')
                    .trim_start_matches('*')
                    .trim()
                    .trim_matches('`');
                if cand.starts_with("docs/backlogs/") && cand.ends_with(".md") {
                    related_backlogs.push(cand.to_string());
                    continue;
                }
            }
            in_related_backlogs = false;
        }

        if trimmed.to_ascii_lowercase().starts_with("- task doc:") {
            task_doc = extract_first_doc_path(trimmed, "docs/tasks/");
        } else if trimmed.to_ascii_lowercase().starts_with("- task issue:") {
            task_issue = extract_issue_number(trimmed);
        } else if trimmed.to_ascii_lowercase().starts_with("- phase status:") {
            phase_status = parse_field_value(trimmed, "- Phase Status:");
        } else if trimmed
            .to_ascii_lowercase()
            .starts_with("- implementation summary:")
        {
            implementation_summary = parse_field_value(trimmed, "- Implementation Summary:");
        }
    }

    related_backlogs.sort();
    related_backlogs.dedup();

    PhaseTracking {
        phase_title: title,
        task_doc,
        task_issue,
        phase_status,
        implementation_summary,
        related_backlogs,
    }
}

fn parse_field_value(line: &str, prefix: &str) -> Option<String> {
    let v = if let Some(rest) = line.strip_prefix(prefix) {
        rest.trim()
    } else {
        let prefix_lower = prefix.to_ascii_lowercase();
        let line_lower = line.to_ascii_lowercase();
        if !line_lower.starts_with(&prefix_lower) {
            return None;
        }
        line.split_once(':').map(|(_, right)| right.trim())?
    };
    let out = v.trim_matches('`').trim();
    if out.is_empty() {
        None
    } else {
        Some(out.to_string())
    }
}

fn extract_first_doc_path(line: &str, prefix: &str) -> Option<String> {
    extract_paths_by_prefix(line, prefix)
        .into_iter()
        .find(|p| p.ends_with(".md"))
}

fn extract_issue_number(line: &str) -> Option<i64> {
    for token in line.split(|c: char| c.is_whitespace() || c == ')' || c == '(' || c == ',') {
        let t = token.trim_matches('`').trim();
        if let Some(rest) = t.strip_prefix('#') {
            if !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()) {
                if let Ok(v) = rest.parse::<i64>() {
                    return Some(v);
                }
            }
        }
    }
    None
}

fn tracking_to_json(doc: &Path, text: &str, tracking: &TrackingExtract) -> serde_json::Value {
    let fm = parse_frontmatter(text);
    let status = fm.get("status").map(|x| normalize_frontmatter_value(x));

    let phases = tracking
        .phases
        .iter()
        .map(|p| {
            json!({
                "phase_title": p.phase_title,
                "task_doc": p.task_doc,
                "task_issue": p.task_issue,
                "phase_status": p.phase_status,
                "implementation_summary": p.implementation_summary,
                "related_backlogs": p.related_backlogs,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "ok": true,
        "doc": normalize_path(doc),
        "status": status,
        "legacy": tracking.legacy,
        "phases": phases,
        "inferred_tasks": tracking.inferred_tasks,
        "inferred_backlogs": tracking.inferred_backlogs,
    })
}

fn extract_section_text(content: &str, section: &str) -> Option<String> {
    let header = format!("## {section}");
    let lines: Vec<&str> = content.lines().collect();
    let start = lines.iter().position(|line| line.trim() == header)?;
    let end = lines
        .iter()
        .enumerate()
        .skip(start + 1)
        .find(|(_, line)| line.starts_with("## "))
        .map(|(idx, _)| idx)
        .unwrap_or(lines.len());
    Some(lines[start + 1..end].join("\n"))
}

fn count_subsection_items(section_body: &str, subsection: &str) -> usize {
    let Some(body) = extract_subsection(section_body, subsection) else {
        return 0;
    };
    body.lines()
        .filter(|line| {
            let trimmed = line.trim();
            (trimmed.starts_with("- ") || trimmed.starts_with("* ")) && !trimmed.eq("- ...")
        })
        .count()
}

fn subsection_has_token(section_body: &str, subsection: &str, token: &str) -> bool {
    let Some(body) = extract_subsection(section_body, subsection) else {
        return false;
    };
    body.contains(token)
}

fn extract_subsection(section_body: &str, subsection: &str) -> Option<String> {
    let header = format!("### {subsection}");
    let lines: Vec<&str> = section_body.lines().collect();
    let start = lines.iter().position(|line| line.trim() == header)?;
    let end = lines
        .iter()
        .enumerate()
        .skip(start + 1)
        .find(|(_, line)| line.starts_with("### ") || line.starts_with("## "))
        .map(|(idx, _)| idx)
        .unwrap_or(lines.len());
    Some(lines[start + 1..end].join("\n"))
}

fn has_any_reference_token(text: &str) -> bool {
    text.contains("[D") || text.contains("[C") || text.contains("[U") || text.contains("[B")
}

fn extract_paths_by_prefix(text: &str, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut pos = 0;

    while let Some(found) = text[pos..].find(prefix) {
        let start = pos + found;
        let mut end = start;
        for (idx, ch) in text[start..].char_indices() {
            if ch.is_whitespace() || matches!(ch, ')' | ']' | '>' | '"' | '\'' | ',') {
                end = start + idx;
                break;
            }
        }
        if end == start {
            end = text.len();
        }

        let mut cand = text[start..end].trim().trim_matches('`').to_string();
        while cand.ends_with('.') || cand.ends_with(':') || cand.ends_with(';') {
            cand.pop();
        }
        if cand.starts_with(prefix) && cand.ends_with(".md") {
            out.push(cand);
        }

        pos = start + prefix.len();
        if pos >= text.len() {
            break;
        }
    }

    out.sort();
    out.dedup();
    out
}

fn is_empty_implementation_notes(notes: &str) -> bool {
    let trimmed = notes.trim();
    if trimmed.is_empty() {
        return true;
    }
    let lowercase = trimmed.to_ascii_lowercase();
    lowercase.contains("keep this section blank")
}

fn issue_state(issue: i64) -> Result<String, String> {
    let cmd = vec![
        "gh".to_string(),
        "issue".to_string(),
        "view".to_string(),
        issue.to_string(),
        "--json".to_string(),
        "state".to_string(),
    ];
    let res = run_command(&cmd);
    if res.returncode != 0 {
        return Err(format!(
            "gh issue view failed: {}",
            res.stderr.trim().to_string()
        ));
    }

    let payload: serde_json::Value = serde_json::from_str(res.stdout.trim())
        .map_err(|e| format!("failed to parse gh output: {e}"))?;
    let state = payload
        .get("state")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing state in gh output".to_string())?;
    Ok(state.to_string())
}

enum BacklogState {
    Open(PathBuf),
    Closed(PathBuf),
    Missing,
}

fn backlog_close_state(reference: &str) -> Result<BacklogState, String> {
    let normalized = normalize_ref_path(Path::new(reference));
    let normalized_text = normalize_path(&normalized);

    if normalized_text.starts_with(&format!("{BACKLOG_CLOSED_DIR}/")) {
        if normalized.exists() {
            return Ok(BacklogState::Closed(normalized));
        }
        return Ok(BacklogState::Missing);
    }

    if normalized_text.starts_with(&format!("{BACKLOG_DIR}/")) {
        if normalized.exists() {
            return Ok(BacklogState::Open(normalized));
        }

        if let Some(id) = extract_backlog_id(&normalized_text) {
            if let Some(closed) = find_backlog_by_id(Path::new(BACKLOG_CLOSED_DIR), &id)? {
                return Ok(BacklogState::Closed(closed));
            }
            if let Some(open) = find_backlog_by_id(Path::new(BACKLOG_DIR), &id)? {
                return Ok(BacklogState::Open(open));
            }
        }
    }

    Ok(BacklogState::Missing)
}

fn extract_backlog_id(path_text: &str) -> Option<String> {
    let name = Path::new(path_text).file_name()?.to_string_lossy().to_string();
    if !name.ends_with(".md") {
        return None;
    }
    let stem = &name[..name.len() - 3];
    let (id, _) = stem.split_once('-')?;
    if id.len() != 6 || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(id.to_string())
}

fn find_backlog_by_id(dir: &Path, id: &str) -> Result<Option<PathBuf>, String> {
    if !dir.exists() || !dir.is_dir() {
        return Ok(None);
    }

    let mut matches = Vec::new();
    let entries = fs::read_dir(dir).map_err(|e| format!("failed to read {}: {e}", normalize_path(dir)))?;
    for entry in entries {
        let entry = match entry {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        if !ft.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(extracted) = extract_backlog_id(&name) {
            if extracted == id {
                matches.push(entry.path());
            }
        }
    }

    matches.sort();
    Ok(matches.into_iter().next())
}

#[derive(Debug, Clone)]
struct CommandResult {
    returncode: i32,
    stdout: String,
    stderr: String,
}

fn run_command(cmd: &[String]) -> CommandResult {
    if cmd.is_empty() {
        return CommandResult {
            returncode: 1,
            stdout: String::new(),
            stderr: "empty command".to_string(),
        };
    }

    let output = match Command::new(&cmd[0]).args(&cmd[1..]).output() {
        Ok(o) => o,
        Err(e) => {
            return CommandResult {
                returncode: 1,
                stdout: String::new(),
                stderr: e.to_string(),
            };
        }
    };

    CommandResult {
        returncode: output.status.code().unwrap_or(1),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }
}

fn print_json(value: &serde_json::Value) {
    let out = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string());
    println!("{out}");
}
