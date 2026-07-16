#!/usr/bin/env -S cargo +nightly -q -Zscript
---
[package]
edition = "2024"

[dependencies]
serde_json = "1"
---

use serde_json::json;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone)]
struct CommandResult {
    returncode: i32,
    stdout: String,
    stderr: String,
}

#[derive(Debug, Clone)]
struct ValidatedDoc {
    path: String,
    doc_type: String,
    doc_id: String,
    title_hint: Option<String>,
    github_issue: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct LabelSelection {
    type_label: Option<String>,
    priority_label: Option<String>,
    codex: bool,
}

#[derive(Debug, Clone)]
struct DocSyncResult {
    changed: bool,
    mode: &'static str,
}

const ALLOWED_TYPE_LABELS: &[&str] = &[
    "type:doc",
    "type:perf",
    "type:feature",
    "type:question",
    "type:bug",
    "type:chore",
    "type:epic",
    "type:task",
];

const ALLOWED_PRIORITY_LABELS: &[&str] = &[
    "priority:low",
    "priority:medium",
    "priority:high",
    "priority:critical",
];

const SPECIAL_LABEL_CODEX: &str = "codex";

fn usage() -> &'static str {
    "Usage: tools/issue.rs <subcommand> [options]\n\n\
Subcommands:\n\
  validate-doc-path\n\
  create-issue-from-doc\n\
  list-issues\n"
}

fn main() {
    let code = match run() {
        Ok(()) => 0,
        Err(code) => code,
    };
    if code != 0 {
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
        "validate-doc-path" => cmd_validate_doc_path(args),
        "create-issue-from-doc" => cmd_create_issue_from_doc(args),
        "list-issues" => cmd_list_issues(args),
        _ => {
            eprintln!("unknown subcommand: {subcommand}\n{}", usage());
            Err(1)
        }
    }
}

fn cmd_validate_doc_path(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut path: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --path");
                    return Err(1);
                };
                path = Some(v);
            }
            "-h" | "--help" => {
                println!("Usage: ... issue.rs validate-doc-path --path <task-or-rfc-doc>");
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}");
                return Err(1);
            }
        }
    }

    let Some(path) = path else {
        eprintln!("missing required arg: --path");
        return Err(1);
    };

    let validated = validate_doc_path(Path::new(&path));
    print_json(&validated);
    if validated.get("valid").and_then(|v| v.as_bool()).unwrap_or(false) {
        Ok(())
    } else {
        Err(1)
    }
}

fn cmd_create_issue_from_doc(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut doc: Option<String> = None;
    let mut title: Option<String> = None;
    let mut labels_csv: Option<String> = None;
    let mut assignee = "@me".to_string();
    let mut parent: Option<i64> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--doc" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --doc");
                    return Err(1);
                };
                doc = Some(v);
            }
            "--title" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --title");
                    return Err(1);
                };
                title = Some(v);
            }
            "--labels" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --labels");
                    return Err(1);
                };
                labels_csv = Some(v);
            }
            "--assignee" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --assignee");
                    return Err(1);
                };
                assignee = v;
            }
            "--parent" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --parent");
                    return Err(1);
                };
                let parsed = match v.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("invalid --parent value: {v}");
                        return Err(1);
                    }
                };
                parent = Some(parsed);
            }
            "-h" | "--help" => {
                println!("Usage: ... issue.rs create-issue-from-doc --doc <path> [--labels <csv>] [--title <title>] [--assignee <assignee>] [--parent <issue>]\n");
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}");
                return Err(1);
            }
        }
    }

    let Some(doc) = doc else {
        eprintln!("missing required arg: --doc");
        return Err(1);
    };
    if assignee != "@me" {
        print_json(&json!({
            "created": false,
            "error": "create-issue-from-doc requires --assignee @me (or omit to use default)",
        }));
        return Err(1);
    }

    let doc_path = Path::new(&doc);
    let validated_value = validate_doc_path(doc_path);
    let Some(valid) = validated_value.get("valid").and_then(|v| v.as_bool()) else {
        print_json(&validated_value);
        return Err(1);
    };
    if !valid {
        print_json(&validated_value);
        return Err(1);
    }

    let validated = match validated_from_json(&validated_value) {
        Some(v) => v,
        None => {
            print_json(&json!({"created": false, "error": "internal error: invalid validated doc payload"}));
            return Err(1);
        }
    };

    let metadata_labels = match parse_doc_issue_labels(doc_path) {
        Ok(v) => v,
        Err(e) => {
            print_json(&json!({"created": false, "error": e}));
            return Err(1);
        }
    };
    let cli_labels = labels_csv.map(|v| split_labels(&v)).unwrap_or_default();
    let labels = match finalize_issue_labels(&validated.doc_type, cli_labels, metadata_labels) {
        Ok(v) => v,
        Err(e) => {
            print_json(&json!({"created": false, "error": e}));
            return Err(1);
        }
    };
    if labels.is_empty() {
        print_json(&json!({"created": false, "error": "internal error: no labels selected"}));
        return Err(1);
    }

    let issue_title = title
        .or_else(|| validated.title_hint.clone())
        .unwrap_or_else(|| format!("{} {}", validated.doc_type.to_uppercase(), validated.doc_id));

    let doc_content = match fs::read_to_string(doc_path) {
        Ok(v) => v,
        Err(e) => {
            print_json(&json!({
                "created": false,
                "error": format!("failed to read {}: {e}", normalize_path(doc_path)),
            }));
            return Err(1);
        }
    };

    let body = build_body(
        &validated.path,
        &validated.doc_type,
        &validated.doc_id,
        validated.title_hint.as_deref(),
        parent,
        &doc_content,
    );

    let temp_path = match write_temp_issue_body(&body) {
        Ok(p) => p,
        Err(e) => {
            print_json(&json!({"created": false, "error": e}));
            return Err(1);
        }
    };

    let create_cmd = vec![
        "gh".to_string(),
        "issue".to_string(),
        "create".to_string(),
        "--title".to_string(),
        issue_title,
        "--body-file".to_string(),
        normalize_path(&temp_path),
        "--label".to_string(),
        labels.join(","),
        "--assignee".to_string(),
        assignee.clone(),
    ];

    let create_res = run_command(&create_cmd);

    let _ = fs::remove_file(&temp_path);

    if create_res.returncode != 0 {
        print_json(&json!({
            "created": false,
            "command": create_cmd,
            "stderr": create_res.stderr.trim(),
            "stdout": create_res.stdout.trim(),
        }));
        return Err(if create_res.returncode == 0 { 1 } else { create_res.returncode });
    }

    let issue_url = create_res
        .stdout
        .lines()
        .last()
        .unwrap_or_default()
        .trim()
        .to_string();
    let issue_no = match parse_issue_number(&issue_url) {
        Some(v) => v,
        None => {
            print_json(&json!({
                "created": false,
                "error": "failed to parse issue number from gh output",
                "stdout": create_res.stdout.trim(),
            }));
            return Err(1);
        }
    };

    let doc_sync = match upsert_doc_github_issue(doc_path, issue_no, validated.github_issue) {
        Ok(v) => v,
        Err(e) => {
            print_json(&json!({
                "created": true,
                "issue_number": issue_no,
                "issue_url": issue_url,
                "doc": validated.path,
                "labels": labels,
                "assignee": assignee,
                "parent": parent,
                "doc_synced": false,
                "doc_sync_error": e,
            }));
            return Ok(());
        }
    };

    print_json(&json!({
        "created": true,
        "issue_number": issue_no,
        "issue_url": issue_url,
        "doc": validated.path,
        "labels": labels,
        "assignee": assignee,
        "parent": parent,
        "doc_synced": true,
        "doc_sync": {
            "path": normalize_path(doc_path),
            "changed": doc_sync.changed,
            "mode": doc_sync.mode,
        }
    }));
    Ok(())
}

fn cmd_list_issues(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut state = "open".to_string();
    let mut assignee: Option<String> = None;
    let mut search: Option<String> = None;
    let mut labels: Vec<String> = Vec::new();
    let mut limit = "100".to_string();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--state" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --state");
                    return Err(1);
                };
                if v != "open" && v != "closed" && v != "all" {
                    eprintln!("invalid --state value: {v}");
                    return Err(1);
                }
                state = v;
            }
            "--assignee" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --assignee");
                    return Err(1);
                };
                assignee = Some(v);
            }
            "--search" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --search");
                    return Err(1);
                };
                search = Some(v);
            }
            "--label" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --label");
                    return Err(1);
                };
                labels.push(v);
            }
            "--limit" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --limit");
                    return Err(1);
                };
                if v.parse::<i64>().is_err() {
                    eprintln!("invalid --limit value: {v}");
                    return Err(1);
                }
                limit = v;
            }
            "-h" | "--help" => {
                println!("Usage: ... issue.rs list-issues [--state open|closed|all] [--assignee <a>] [--search <q>] [--label <label>]... [--limit <n>]");
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}");
                return Err(1);
            }
        }
    }

    let mut cmd = vec![
        "gh".to_string(),
        "issue".to_string(),
        "list".to_string(),
        "--state".to_string(),
        state,
        "--limit".to_string(),
        limit,
        "--json".to_string(),
        "number,title,state,labels,assignees,url".to_string(),
    ];

    if let Some(a) = assignee {
        cmd.push("--assignee".to_string());
        cmd.push(a);
    }
    if let Some(s) = search {
        cmd.push("-S".to_string());
        cmd.push(s);
    }
    for label in labels {
        cmd.push("--label".to_string());
        cmd.push(label);
    }

    let res = run_command(&cmd);
    if res.returncode != 0 {
        print_json(&json!({
            "ok": false,
            "command": cmd,
            "stderr": res.stderr.trim(),
            "stdout": res.stdout.trim(),
        }));
        return Err(if res.returncode == 0 { 1 } else { res.returncode });
    }

    let payload = if res.stdout.trim().is_empty() {
        "[]"
    } else {
        res.stdout.as_str()
    };
    let parsed: serde_json::Value = match serde_json::from_str(payload) {
        Ok(v) => v,
        Err(e) => {
            print_json(&json!({
                "ok": false,
                "error": format!("failed to parse gh json output: {e}"),
                "stdout": res.stdout.trim(),
            }));
            return Err(1);
        }
    };

    let count = parsed.as_array().map(|x| x.len()).unwrap_or(0);
    print_json(&json!({"ok": true, "count": count, "issues": parsed}));
    Ok(())
}

fn validate_doc_path(path: &Path) -> serde_json::Value {
    let cmd = vec![
        "tools/doc-id.rs".to_string(),
        "validate-path".to_string(),
        "--path".to_string(),
        normalize_path(path),
    ];
    let res = run_command(&cmd);
    let payload = if res.stdout.trim().is_empty() {
        json!({
            "valid": false,
            "path": normalize_for_doc_rule(path),
            "error": "doc-id tool returned empty output",
        })
    } else {
        match serde_json::from_str::<serde_json::Value>(res.stdout.trim()) {
            Ok(v) => v,
            Err(e) => json!({
                "valid": false,
                "path": normalize_for_doc_rule(path),
                "error": format!("failed to parse doc-id output: {e}"),
                "stdout": res.stdout.trim(),
                "stderr": res.stderr.trim(),
            }),
        }
    };

    let valid = payload
        .get("valid")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !valid {
        return payload;
    }

    let kind = payload
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if kind != "task" && kind != "rfc" {
        let fallback_path = normalize_for_doc_rule(path);
        let err_path = payload
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or(fallback_path.as_str());
        return json!({
            "valid": false,
            "path": err_path,
            "error": "invalid path pattern; expected docs/tasks/<6 digits>-<slug>.md or docs/rfcs/<4 digits>-<slug>.md",
        });
    }

    let doc_id = payload
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let rel_path = payload
        .get("path")
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
        .unwrap_or_else(|| normalize_for_doc_rule(path));

    json!({
        "valid": true,
        "path": rel_path,
        "doc_type": kind,
        "doc_id": doc_id,
        "title_hint": title_hint(path),
        "github_issue": parse_doc_github_issue_hint(path),
    })
}

fn title_hint(path: &Path) -> Option<String> {
    let text = fs::read_to_string(path).ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# ") {
            let trimmed = rest.trim();
            if trimmed.is_empty() {
                return None;
            }
            return Some(trimmed.to_string());
        }
    }
    None
}

fn parse_doc_github_issue_hint(path: &Path) -> Option<i64> {
    let text = fs::read_to_string(path).ok()?;
    let lines: Vec<&str> = text.lines().collect();
    parse_github_issue_from_frontmatter_lines(&lines)
}

fn parse_github_issue_from_frontmatter_lines(lines: &[&str]) -> Option<i64> {
    if lines.first().map(|v| v.trim()) != Some("---") {
        return None;
    }
    let end = lines
        .iter()
        .enumerate()
        .skip(1)
        .find(|(_, v)| v.trim() == "---")
        .map(|(idx, _)| idx)?;

    for line in &lines[1..end] {
        let trimmed = line.trim();
        if !trimmed.starts_with("github_issue:") {
            continue;
        }
        let value = trimmed
            .split_once(':')
            .map(|(_, right)| right.trim())
            .unwrap_or("");
        let token = value.split_whitespace().next().unwrap_or("");
        if !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit()) {
            if let Ok(parsed) = token.parse::<i64>() {
                return Some(parsed);
            }
        }
    }
    None
}

fn upsert_doc_github_issue(
    path: &Path,
    issue_no: i64,
    validated_issue: Option<i64>,
) -> Result<DocSyncResult, String> {
    if issue_no <= 0 {
        return Err("issue number must be positive".to_string());
    }
    if let Some(existing) = validated_issue
        && existing != issue_no
    {
        return Err(format!(
            "conflict: validated github_issue is {existing}, refusing to overwrite with {issue_no}"
        ));
    }

    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(path)))?;
    let lines: Vec<&str> = content.lines().collect();
    if let Some(existing) = parse_github_issue_from_frontmatter_lines(&lines)
        && existing != issue_no
    {
        return Err(format!(
            "conflict: stored github_issue is {existing}, refusing to overwrite with {issue_no}"
        ));
    }
    let (updated, changed, mode) = upsert_frontmatter_github_issue(&content, issue_no)?;

    if changed {
        fs::write(path, updated)
            .map_err(|e| format!("failed to write {}: {e}", normalize_path(path)))?;
    }

    Ok(DocSyncResult { changed, mode })
}

fn upsert_frontmatter_github_issue(
    content: &str,
    issue_no: i64,
) -> Result<(String, bool, &'static str), String> {
    let lines: Vec<&str> = content.lines().collect();
    let issue_line = format!("github_issue: {issue_no}");

    if lines.first().map(|v| v.trim()) == Some("---") {
        let Some(end_idx) = lines
            .iter()
            .enumerate()
            .skip(1)
            .find(|(_, v)| v.trim() == "---")
            .map(|(idx, _)| idx) else {
            return Err("unterminated frontmatter: missing closing `---`".to_string());
        };

        let mut out: Vec<String> = lines.iter().map(|v| (*v).to_string()).collect();
        let mut found_idx = None;
        for (idx, line) in out.iter().enumerate().take(end_idx).skip(1) {
            if line.trim().starts_with("github_issue:") {
                found_idx = Some(idx);
                break;
            }
        }

        if let Some(idx) = found_idx {
            let old = out[idx].trim().to_string();
            if old == issue_line {
                return Ok((content.to_string(), false, "already_set"));
            }
            out[idx] = issue_line;
            let mut rebuilt = out.join("\n");
            if content.ends_with('\n') {
                rebuilt.push('\n');
            }
            return Ok((rebuilt, true, "updated"));
        }

        out.insert(end_idx, issue_line);
        let mut rebuilt = out.join("\n");
        if content.ends_with('\n') {
            rebuilt.push('\n');
        }
        return Ok((rebuilt, true, "inserted"));
    }

    // No YAML frontmatter found. Prepend a minimal frontmatter block so both
    // task and RFC docs can carry deterministic issue linkage metadata.
    let mut rebuilt = String::new();
    rebuilt.push_str("---\n");
    rebuilt.push_str(&issue_line);
    rebuilt.push_str("\n---\n\n");
    rebuilt.push_str(content.trim_start_matches('\n'));
    if content.ends_with('\n') && !rebuilt.ends_with('\n') {
        rebuilt.push('\n');
    }
    Ok((rebuilt, true, "prepended"))
}

fn normalize_for_doc_rule(path: &Path) -> String {
    let raw = normalize_path(path);
    raw.trim_start_matches(|c| c == '.' || c == '/')
        .to_string()
}

fn validated_from_json(value: &serde_json::Value) -> Option<ValidatedDoc> {
    Some(ValidatedDoc {
        path: value.get("path")?.as_str()?.to_string(),
        doc_type: value.get("doc_type")?.as_str()?.to_string(),
        doc_id: value.get("doc_id")?.as_str()?.to_string(),
        title_hint: value
            .get("title_hint")
            .and_then(|v| v.as_str().map(ToString::to_string)),
        github_issue: value.get("github_issue").and_then(|v| v.as_i64()),
    })
}

fn split_labels(csv: &str) -> Vec<String> {
    csv.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_doc_issue_labels(path: &Path) -> Result<Vec<String>, String> {
    let text = fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(path)))?;
    let mut labels = Vec::new();
    let mut in_block = false;
    let mut found_block = false;

    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if !in_block {
            if is_issue_labels_header(trimmed) {
                if found_block {
                    return Err(format!(
                        "multiple `Issue Labels:` sections found in {}",
                        normalize_path(path)
                    ));
                }
                found_block = true;
                in_block = true;
            }
            continue;
        }

        if trimmed.is_empty() {
            break;
        }
        if trimmed.starts_with('#') {
            break;
        }

        let Some(rest) = trimmed
            .strip_prefix("- ")
            .or_else(|| trimmed.strip_prefix("* "))
        else {
            return Err(format!(
                "invalid `Issue Labels:` entry at {}:{}; expected `- <label>`",
                normalize_path(path),
                idx + 1
            ));
        };
        let normalized = normalize_label_token(rest);
        if normalized.is_empty() {
            return Err(format!(
                "empty `Issue Labels:` entry at {}:{}",
                normalize_path(path),
                idx + 1
            ));
        }
        labels.push(normalized);
    }

    Ok(labels)
}

fn is_issue_labels_header(line: &str) -> bool {
    line == "Issue Labels:"
}

fn normalize_label_token(value: &str) -> String {
    let trimmed = value.trim();
    let without_backticks = trimmed
        .strip_prefix('`')
        .and_then(|v| v.strip_suffix('`'))
        .unwrap_or(trimmed);
    without_backticks.trim().to_string()
}

fn finalize_issue_labels(
    doc_type: &str,
    cli_labels: Vec<String>,
    metadata_labels: Vec<String>,
) -> Result<Vec<String>, String> {
    let cli = parse_labels(cli_labels, "cli")?;
    let metadata = parse_labels(metadata_labels, "metadata")?;

    let type_label = match cli.type_label.or(metadata.type_label) {
        Some(v) => v,
        None => default_type_label(doc_type).to_string(),
    };
    let priority_label = match cli.priority_label.or(metadata.priority_label) {
        Some(v) => v,
        None => "priority:medium".to_string(),
    };
    let codex = cli.codex || metadata.codex;

    if !is_allowed_type_label(&type_label) {
        return Err(format!(
            "invalid type label: {type_label}; allowed: {}",
            ALLOWED_TYPE_LABELS.join(", ")
        ));
    }
    if !is_allowed_priority_label(&priority_label) {
        return Err(format!(
            "invalid priority label: {priority_label}; allowed: {}",
            ALLOWED_PRIORITY_LABELS.join(", ")
        ));
    }

    let mut labels = vec![type_label, priority_label];
    if codex {
        labels.push(SPECIAL_LABEL_CODEX.to_string());
    }
    Ok(labels)
}

fn parse_labels(labels: Vec<String>, source: &str) -> Result<LabelSelection, String> {
    let mut selected = LabelSelection::default();
    for raw in labels {
        let label = normalize_label_token(&raw);
        if label.is_empty() {
            continue;
        }

        if label.starts_with("type:") {
            if !is_allowed_type_label(&label) {
                return Err(format!(
                    "unsupported label in {source} labels: {label}; allowed type labels: {}",
                    ALLOWED_TYPE_LABELS.join(", ")
                ));
            }
            if selected.type_label.is_some() {
                return Err(format!(
                    "{source} labels must include at most one type:* label"
                ));
            }
            selected.type_label = Some(label);
            continue;
        }

        if label.starts_with("priority:") {
            if !is_allowed_priority_label(&label) {
                return Err(format!(
                    "unsupported label in {source} labels: {label}; allowed priority labels: {}",
                    ALLOWED_PRIORITY_LABELS.join(", ")
                ));
            }
            if selected.priority_label.is_some() {
                return Err(format!(
                    "{source} labels must include at most one priority:* label"
                ));
            }
            selected.priority_label = Some(label);
            continue;
        }

        if label == SPECIAL_LABEL_CODEX {
            selected.codex = true;
            continue;
        }

        return Err(format!(
            "unsupported label in {source} labels: {label}; allowed labels: {}, {}, {}",
            ALLOWED_TYPE_LABELS.join(", "),
            ALLOWED_PRIORITY_LABELS.join(", "),
            SPECIAL_LABEL_CODEX,
        ));
    }
    Ok(selected)
}

fn is_allowed_type_label(label: &str) -> bool {
    ALLOWED_TYPE_LABELS.contains(&label)
}

fn is_allowed_priority_label(label: &str) -> bool {
    ALLOWED_PRIORITY_LABELS.contains(&label)
}

fn default_type_label(doc_type: &str) -> &'static str {
    match doc_type {
        "task" => "type:task",
        "rfc" => "type:epic",
        _ => "type:task",
    }
}

fn build_body(
    doc_path: &str,
    doc_type: &str,
    doc_id: &str,
    doc_title: Option<&str>,
    parent: Option<i64>,
    doc_content: &str,
) -> String {
    let mut lines = vec![
        "Planning document:".to_string(),
        format!("- `{doc_path}`"),
        format!("- Type: {doc_type}"),
        format!("- ID: {doc_id}"),
    ];
    if let Some(title) = doc_title {
        lines.push(format!("- Title: {title}"));
    }
    lines.push(String::new());
    lines.push("Generated from document-first issue workflow.".to_string());
    if let Some(p) = parent {
        lines.push(format!("Part of #{p}"));
    }
    let mut body = lines.join("\n") + "\n";
    for section in issue_body_sections(doc_type) {
        append_issue_body_section(&mut body, doc_content, section);
    }
    body
}

fn issue_body_sections(doc_type: &str) -> &'static [&'static str] {
    match doc_type {
        "task" => &["Summary", "Context", "Goals", "Non-Goals"],
        "rfc" => &["Summary", "Context", "Decision"],
        _ => &[],
    }
}

fn append_issue_body_section(body: &mut String, doc_content: &str, section: &str) {
    body.push('\n');
    body.push_str("## ");
    body.push_str(section);
    body.push_str("\n\n");
    match extract_markdown_section(doc_content, section) {
        Some(text) if !text.trim().is_empty() => {
            body.push_str(text.trim_matches('\n'));
            body.push('\n');
        }
        Some(_) => {
            body.push_str("_Section is empty in the planning document._\n");
        }
        None => {
            body.push_str("_Section not found in the planning document._\n");
        }
    }
}

fn extract_markdown_section(content: &str, section: &str) -> Option<String> {
    let lines: Vec<&str> = content.lines().collect();
    let start = lines
        .iter()
        .position(|line| is_markdown_section_heading(line, section))?;
    let end = lines
        .iter()
        .enumerate()
        .skip(start + 1)
        .find(|(_, line)| is_markdown_level_two_heading(line))
        .map(|(idx, _)| idx)
        .unwrap_or(lines.len());
    Some(lines[start + 1..end].join("\n"))
}

fn is_markdown_section_heading(line: &str, section: &str) -> bool {
    let trimmed = line.trim();
    let Some(heading) = trimmed.strip_prefix("## ") else {
        return false;
    };
    heading.trim().eq_ignore_ascii_case(section)
}

fn is_markdown_level_two_heading(line: &str) -> bool {
    line.trim().starts_with("## ")
}

fn parse_issue_number(issue_url_or_text: &str) -> Option<i64> {
    let text = issue_url_or_text.trim();
    if let Some(pos) = text.rfind("/issues/") {
        let candidate = &text[pos + "/issues/".len()..];
        if !candidate.is_empty() && candidate.bytes().all(|b| b.is_ascii_digit()) {
            return candidate.parse::<i64>().ok();
        }
    }
    if let Some(pos) = text.rfind('#') {
        let candidate = &text[pos + 1..];
        if !candidate.is_empty() && candidate.bytes().all(|b| b.is_ascii_digit()) {
            return candidate.parse::<i64>().ok();
        }
    }
    None
}

fn write_temp_issue_body(body: &str) -> Result<PathBuf, String> {
    let mut attempt: u32 = 0;
    loop {
        let filename = format!(
            "issue-body-{}-{}-{}.md",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| format!("system clock error: {e}"))?
                .as_nanos(),
            attempt,
        );
        let path = env::temp_dir().join(filename);
        if path.exists() {
            attempt += 1;
            if attempt > 100 {
                return Err("failed to allocate temporary issue body file".to_string());
            }
            continue;
        }
        fs::write(&path, body)
            .map_err(|e| format!("failed to create temp body file {}: {e}", normalize_path(&path)))?;
        return Ok(path);
    }
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

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_upsert_frontmatter_github_issue_already_set() {
        let input = "---\nowner: alice\ngithub_issue: 123\n---\n\n# Title\n";
        let (updated, changed, mode) = upsert_frontmatter_github_issue(input, 123).unwrap();
        assert_eq!(updated, input);
        assert!(!changed);
        assert_eq!(mode, "already_set");
    }

    #[test]
    fn test_upsert_frontmatter_github_issue_updated() {
        let input = "---\nowner: alice\ngithub_issue: 99\n---\n\n# Title\n";
        let expected = "---\nowner: alice\ngithub_issue: 123\n---\n\n# Title\n";
        let (updated, changed, mode) = upsert_frontmatter_github_issue(input, 123).unwrap();
        assert_eq!(updated, expected);
        assert!(changed);
        assert_eq!(mode, "updated");
    }

    #[test]
    fn test_upsert_frontmatter_github_issue_inserted() {
        let input = "---\nowner: alice\nstatus: open\n---\n\n# Title\n";
        let expected = "---\nowner: alice\nstatus: open\ngithub_issue: 123\n---\n\n# Title\n";
        let (updated, changed, mode) = upsert_frontmatter_github_issue(input, 123).unwrap();
        assert_eq!(updated, expected);
        assert!(changed);
        assert_eq!(mode, "inserted");
    }

    #[test]
    fn test_upsert_frontmatter_github_issue_prepended_no_frontmatter() {
        let input = "# Task: Example\n\nBody\n";
        let expected = "---\ngithub_issue: 123\n---\n\n# Task: Example\n\nBody\n";
        let (updated, changed, mode) = upsert_frontmatter_github_issue(input, 123).unwrap();
        assert_eq!(updated, expected);
        assert!(changed);
        assert_eq!(mode, "prepended");
    }

    #[test]
    fn test_upsert_frontmatter_github_issue_unterminated_frontmatter_error() {
        let input = "---\nowner: alice\ngithub_issue: 9\n# Missing closing fence\n";
        let err = upsert_frontmatter_github_issue(input, 123).unwrap_err();
        assert!(err.contains("unterminated frontmatter"));
    }

    #[test]
    fn test_upsert_doc_github_issue_conflict_does_not_overwrite() {
        let path = unique_temp_path("issue-script-upsert-conflict");
        let input = "---\ngithub_issue: 7\n---\n\n# Doc\n";
        fs::write(&path, input).unwrap();

        let err = upsert_doc_github_issue(&path, 9, Some(7)).unwrap_err();
        assert!(err.contains("conflict"));
        let after = fs::read_to_string(&path).unwrap();
        assert_eq!(after, input);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_upsert_doc_github_issue_unterminated_frontmatter_does_not_overwrite() {
        let path = unique_temp_path("issue-script-upsert-unterminated");
        let input = "---\ngithub_issue: 7\n# Missing closing fence\n";
        fs::write(&path, input).unwrap();

        let err = upsert_doc_github_issue(&path, 9, None).unwrap_err();
        assert!(err.contains("unterminated frontmatter"));
        let after = fs::read_to_string(&path).unwrap();
        assert_eq!(after, input);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_build_body_includes_task_context_sections() {
        let doc = "\
---
id: 000123
---

# Task: Example

## Summary

Task summary.

## Context

Task context.

## Goals

1. Goal one.

## Non-Goals

1. Non-goal one.

## Plan

Do the work.
";

        let body = build_body(
            "docs/tasks/000123-example.md",
            "task",
            "000123",
            Some("Task: Example"),
            Some(42),
            doc,
        );

        assert!(body.contains("Planning document:\n- `docs/tasks/000123-example.md`"));
        assert!(body.contains("Part of #42"));
        assert!(body.contains("## Summary\n\nTask summary.\n"));
        assert!(body.contains("## Context\n\nTask context.\n"));
        assert!(body.contains("## Goals\n\n1. Goal one.\n"));
        assert!(body.contains("## Non-Goals\n\n1. Non-goal one.\n"));
        assert!(!body.contains("## Plan\n\nDo the work."));
    }

    #[test]
    fn test_build_body_includes_rfc_context_sections() {
        let doc = "\
---
id: 0012
---

# RFC-0012: Example

## Summary

RFC summary.

## Context

RFC context.

## Design Inputs

- [D1] `docs/architecture.md`

## Decision

Choose the selected direction.

### Detail

Decision detail stays attached.

## Consequences

Expected outcomes.
";

        let body = build_body(
            "docs/rfcs/0012-example.md",
            "rfc",
            "0012",
            Some("RFC-0012: Example"),
            None,
            doc,
        );

        assert!(body.contains("Planning document:\n- `docs/rfcs/0012-example.md`"));
        assert!(body.contains("## Summary\n\nRFC summary.\n"));
        assert!(body.contains("## Context\n\nRFC context.\n"));
        assert!(body.contains("## Decision\n\nChoose the selected direction.\n\n### Detail\n\nDecision detail stays attached.\n"));
        assert!(!body.contains("## Design Inputs\n\n- [D1]"));
        assert!(!body.contains("## Consequences\n\nExpected outcomes."));
    }

    #[test]
    fn test_build_body_marks_missing_requested_sections() {
        let doc = "# Task: Example\n\n## Summary\n\nTask summary.\n";

        let body = build_body(
            "docs/tasks/000124-example.md",
            "task",
            "000124",
            None,
            None,
            doc,
        );

        assert!(body.contains("## Summary\n\nTask summary.\n"));
        assert!(body.contains("## Context\n\n_Section not found in the planning document._\n"));
        assert!(body.contains("## Goals\n\n_Section not found in the planning document._\n"));
        assert!(body.contains("## Non-Goals\n\n_Section not found in the planning document._\n"));
    }

    fn unique_temp_path(prefix: &str) -> PathBuf {
        for attempt in 0..100u32 {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let filename = format!("{prefix}-{}-{nanos}-{attempt}.md", std::process::id());
            let path = env::temp_dir().join(filename);
            if !path.exists() {
                return path;
            }
        }
        panic!("failed to allocate unique temp path");
    }
}
