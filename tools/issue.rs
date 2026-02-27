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
}

fn usage() -> &'static str {
    "Usage: tools/issue.rs <subcommand> [options]\n\n\
Subcommands:\n\
  validate-doc-path\n\
  create-issue-from-doc\n\
  list-issues\n\
  update-issue\n\
  close-issue\n\
  link-pr-guidance\n"
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
        "update-issue" => cmd_update_issue(args),
        "close-issue" => cmd_close_issue(args),
        "link-pr-guidance" => cmd_link_pr_guidance(args),
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
                println!("Usage: ... issue.rs create-issue-from-doc --doc <path> --labels <csv> [--title <title>] [--assignee <assignee>] [--parent <issue>]\n");
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
    let Some(labels_csv) = labels_csv else {
        eprintln!("missing required arg: --labels");
        return Err(1);
    };

    let validated_value = validate_doc_path(Path::new(&doc));
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

    let labels = split_labels(&labels_csv);
    let (labels, ok, err) = normalize_labels(labels);
    if !ok {
        print_json(&json!({"created": false, "error": err.unwrap_or("invalid labels")}));
        return Err(1);
    }

    let issue_title = title
        .or_else(|| validated.title_hint.clone())
        .unwrap_or_else(|| format!("{} {}", validated.doc_type.to_uppercase(), validated.doc_id));

    let body = build_body(
        &validated.path,
        &validated.doc_type,
        &validated.doc_id,
        validated.title_hint.as_deref(),
        parent,
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

    if !assignee.is_empty() {
        let edit_cmd = vec![
            "gh".to_string(),
            "issue".to_string(),
            "edit".to_string(),
            issue_no.to_string(),
            "--add-assignee".to_string(),
            assignee.clone(),
        ];
        let edit_res = run_command(&edit_cmd);
        if edit_res.returncode != 0 {
            print_json(&json!({
                "created": true,
                "issue_number": issue_no,
                "issue_url": issue_url,
                "assigned": false,
                "assign_stderr": edit_res.stderr.trim(),
                "assign_stdout": edit_res.stdout.trim(),
            }));
            return Err(if edit_res.returncode == 0 { 1 } else { edit_res.returncode });
        }
    }

    print_json(&json!({
        "created": true,
        "issue_number": issue_no,
        "issue_url": issue_url,
        "doc": validated.path,
        "labels": labels,
        "assignee": assignee,
        "parent": parent,
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

fn cmd_update_issue(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut issue: Option<i64> = None;
    let mut add_label: Option<String> = None;
    let mut remove_label: Option<String> = None;
    let mut add_assignee: Option<String> = None;
    let mut remove_assignee: Option<String> = None;
    let mut body: Option<String> = None;
    let mut body_file: Option<String> = None;
    let mut comment: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--issue" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --issue");
                    return Err(1);
                };
                let parsed = match v.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("invalid --issue value: {v}");
                        return Err(1);
                    }
                };
                issue = Some(parsed);
            }
            "--add-label" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --add-label");
                    return Err(1);
                };
                add_label = Some(v);
            }
            "--remove-label" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --remove-label");
                    return Err(1);
                };
                remove_label = Some(v);
            }
            "--add-assignee" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --add-assignee");
                    return Err(1);
                };
                add_assignee = Some(v);
            }
            "--remove-assignee" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --remove-assignee");
                    return Err(1);
                };
                remove_assignee = Some(v);
            }
            "--body" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --body");
                    return Err(1);
                };
                body = Some(v);
            }
            "--body-file" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --body-file");
                    return Err(1);
                };
                body_file = Some(v);
            }
            "--comment" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --comment");
                    return Err(1);
                };
                comment = Some(v);
            }
            "-h" | "--help" => {
                println!("Usage: ... issue.rs update-issue --issue <n> [--add-label <csv>] [--remove-label <csv>] [--add-assignee <a>] [--remove-assignee <a>] [--body <text>|--body-file <path>] [--comment <text>]");
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}");
                return Err(1);
            }
        }
    }

    let Some(issue) = issue else {
        eprintln!("missing required arg: --issue");
        return Err(1);
    };

    if body.is_some() && body_file.is_some() {
        print_json(&json!({"ok": false, "error": "use either --body or --body-file, not both"}));
        return Err(1);
    }

    let mut commands: Vec<Vec<String>> = Vec::new();
    let mut edit_cmd = vec!["gh".to_string(), "issue".to_string(), "edit".to_string(), issue.to_string()];
    let mut has_edit = false;

    if let Some(v) = add_label {
        edit_cmd.push("--add-label".to_string());
        edit_cmd.push(v);
        has_edit = true;
    }
    if let Some(v) = remove_label {
        edit_cmd.push("--remove-label".to_string());
        edit_cmd.push(v);
        has_edit = true;
    }
    if let Some(v) = add_assignee {
        edit_cmd.push("--add-assignee".to_string());
        edit_cmd.push(v);
        has_edit = true;
    }
    if let Some(v) = remove_assignee {
        edit_cmd.push("--remove-assignee".to_string());
        edit_cmd.push(v);
        has_edit = true;
    }
    if let Some(v) = body {
        edit_cmd.push("--body".to_string());
        edit_cmd.push(v);
        has_edit = true;
    }
    if let Some(v) = body_file {
        let body_path = PathBuf::from(&v);
        if !body_path.exists() || !body_path.is_file() {
            print_json(&json!({"ok": false, "error": format!("body file not found: {v}")}));
            return Err(1);
        }
        edit_cmd.push("--body-file".to_string());
        edit_cmd.push(v);
        has_edit = true;
    }
    if has_edit {
        commands.push(edit_cmd);
    }

    if let Some(v) = comment {
        commands.push(vec![
            "gh".to_string(),
            "issue".to_string(),
            "comment".to_string(),
            issue.to_string(),
            "--body".to_string(),
            v,
        ]);
    }

    if commands.is_empty() {
        print_json(&json!({"ok": false, "error": "no operation requested"}));
        return Err(1);
    }

    let mut executed = Vec::new();
    for cmd in commands {
        let res = run_command(&cmd);
        executed.push(json!({
            "command": cmd,
            "returncode": res.returncode,
            "stdout": res.stdout.trim(),
            "stderr": res.stderr.trim(),
        }));
        if res.returncode != 0 {
            print_json(&json!({"ok": false, "results": executed}));
            return Err(if res.returncode == 0 { 1 } else { res.returncode });
        }
    }

    print_json(&json!({"ok": true, "issue_number": issue, "results": executed}));
    Ok(())
}

fn cmd_close_issue(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut issue: Option<i64> = None;
    let mut comment: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--issue" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --issue");
                    return Err(1);
                };
                let parsed = match v.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("invalid --issue value: {v}");
                        return Err(1);
                    }
                };
                issue = Some(parsed);
            }
            "--comment" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --comment");
                    return Err(1);
                };
                comment = Some(v);
            }
            "-h" | "--help" => {
                println!("Usage: ... issue.rs close-issue --issue <n> --comment <text>");
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}");
                return Err(1);
            }
        }
    }

    let Some(issue) = issue else {
        eprintln!("missing required arg: --issue");
        return Err(1);
    };
    let Some(comment) = comment else {
        eprintln!("missing required arg: --comment");
        return Err(1);
    };

    let cmd = vec![
        "gh".to_string(),
        "issue".to_string(),
        "close".to_string(),
        issue.to_string(),
        "--comment".to_string(),
        comment,
    ];
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

    print_json(&json!({
        "ok": true,
        "issue_number": issue,
        "stdout": res.stdout.trim(),
    }));
    Ok(())
}

fn cmd_link_pr_guidance(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut issue: Option<i64> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--issue" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --issue");
                    return Err(1);
                };
                let parsed = match v.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        eprintln!("invalid --issue value: {v}");
                        return Err(1);
                    }
                };
                issue = Some(parsed);
            }
            "-h" | "--help" => {
                println!("Usage: ... issue.rs link-pr-guidance --issue <n>");
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}");
                return Err(1);
            }
        }
    }

    let Some(issue) = issue else {
        eprintln!("missing required arg: --issue");
        return Err(1);
    };

    let fix = format!("Fixes #{issue}");
    let close = format!("Closes #{issue}");
    print_json(&json!({
        "issue_number": issue,
        "recommended": fix,
        "alternatives": [close],
        "examples": {
            "pr_body_line": format!("Fixes #{issue}"),
            "completion_comment": format!("Completed via PR #<number>. Closes #{issue}"),
        },
    }));
    Ok(())
}

fn validate_doc_path(path: &Path) -> serde_json::Value {
    let rel = normalize_for_doc_rule(path);

    if !path.exists() {
        return json!({"valid": false, "error": format!("path not found: {rel}")});
    }
    if !path.is_file() {
        return json!({"valid": false, "error": format!("path is not a file: {rel}")});
    }

    if let Some(id) = parse_task_doc_id(&rel) {
        return json!({
            "valid": true,
            "path": rel,
            "doc_type": "task",
            "doc_id": id,
            "title_hint": title_hint(path),
        });
    }
    if let Some(id) = parse_rfc_doc_id(&rel) {
        return json!({
            "valid": true,
            "path": rel,
            "doc_type": "rfc",
            "doc_id": id,
            "title_hint": title_hint(path),
        });
    }

    json!({
        "valid": false,
        "path": rel,
        "error": "invalid path pattern; expected docs/tasks/<6 digits>-<slug>.md or docs/rfcs/<4 digits>-<slug>.md",
    })
}

fn parse_task_doc_id(rel: &str) -> Option<String> {
    parse_doc_id(rel, "docs/tasks/", 6)
}

fn parse_rfc_doc_id(rel: &str) -> Option<String> {
    parse_doc_id(rel, "docs/rfcs/", 4)
}

fn parse_doc_id(rel: &str, prefix: &str, width: usize) -> Option<String> {
    if !rel.starts_with(prefix) || !rel.ends_with(".md") {
        return None;
    }
    let tail = &rel[prefix.len()..rel.len() - 3];
    if tail.contains('/') {
        return None;
    }
    let (id, slug) = tail.split_once('-')?;
    if id.len() != width || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if slug.is_empty() {
        return None;
    }
    Some(id.to_string())
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
    })
}

fn split_labels(csv: &str) -> Vec<String> {
    csv.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn normalize_labels(mut labels: Vec<String>) -> (Vec<String>, bool, Option<&'static str>) {
    let has_type = labels.iter().any(|label| label.starts_with("type:"));
    let has_priority = labels.iter().any(|label| label.starts_with("priority:"));
    if !has_type {
        return (
            labels,
            false,
            Some("labels must include at least one type:* label"),
        );
    }
    if !has_priority {
        labels.push("priority:medium".to_string());
    }
    (labels, true, None)
}

fn build_body(
    doc_path: &str,
    doc_type: &str,
    doc_id: &str,
    doc_title: Option<&str>,
    parent: Option<i64>,
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
    lines.join("\n") + "\n"
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
