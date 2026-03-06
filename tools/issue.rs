#!/usr/bin/env -S cargo +nightly -q -Zscript
---
[package]
edition = "2024"

[dependencies]
serde_json = "1"
---

use serde_json::json;
use std::collections::BTreeSet;
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

#[derive(Debug, Clone, Default)]
struct LabelSelection {
    type_label: Option<String>,
    priority_label: Option<String>,
    codex: bool,
}

#[derive(Debug, Clone)]
struct PrDocCandidate {
    path: String,
    doc_type: String,
    doc_id: String,
}

#[derive(Debug, Clone)]
struct AutoPrTitle {
    title: String,
    doc_path: String,
    doc_type: String,
    doc_id: String,
    doc_heading: Option<String>,
    doc_title: String,
    prefix: String,
    type_label: Option<String>,
    selection_reason: String,
    changed_doc_paths: Vec<String>,
    warnings: Vec<String>,
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
  create-pr-from-branch\n\
  list-issues\n\
  update-issue\n\
  close-issue\n\
  resolve-rfc\n\
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
        "create-pr-from-branch" => cmd_create_pr_from_branch(args),
        "list-issues" => cmd_list_issues(args),
        "update-issue" => cmd_update_issue(args),
        "close-issue" => cmd_close_issue(args),
        "resolve-rfc" => cmd_resolve_rfc(args),
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

fn cmd_create_pr_from_branch(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut issue: Option<i64> = None;
    let mut base = "main".to_string();
    let mut head: Option<String> = None;
    let mut title: Option<String> = None;
    let mut body: Option<String> = None;
    let mut push = false;
    let mut assignee = "@me".to_string();
    let mut allow_dirty = false;

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
            "--base" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --base");
                    return Err(1);
                };
                base = v;
            }
            "--head" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --head");
                    return Err(1);
                };
                head = Some(v);
            }
            "--title" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --title");
                    return Err(1);
                };
                title = Some(v);
            }
            "--body" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --body");
                    return Err(1);
                };
                body = Some(v);
            }
            "--push" => {
                push = true;
            }
            "--assignee" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --assignee");
                    return Err(1);
                };
                assignee = v;
            }
            "--allow-dirty" => {
                allow_dirty = true;
            }
            "-h" | "--help" => {
                println!("Usage: ... issue.rs create-pr-from-branch --issue <n> [--base <branch>] [--head <branch>] [--title <title>] [--body <text>] [--push] [--assignee @me] [--allow-dirty]");
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}");
                return Err(1);
            }
        }
    }

    let Some(issue_no) = issue else {
        eprintln!("missing required arg: --issue");
        return Err(1);
    };
    if assignee != "@me" {
        print_json(&json!({
            "ok": false,
            "created": false,
            "error": "create-pr-from-branch requires --assignee @me (or omit to use default)",
        }));
        return Err(1);
    }

    let dirty_cmd = vec![
        "git".to_string(),
        "status".to_string(),
        "--porcelain".to_string(),
    ];
    let dirty_res = run_command(&dirty_cmd);
    if dirty_res.returncode != 0 {
        print_json(&json!({
            "ok": false,
            "created": false,
            "error": "failed to inspect git working tree status",
            "command": dirty_cmd,
            "stdout": dirty_res.stdout.trim(),
            "stderr": dirty_res.stderr.trim(),
        }));
        return Err(if dirty_res.returncode == 0 { 1 } else { dirty_res.returncode });
    }
    let dirty_entries: Vec<String> = dirty_res
        .stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(ToString::to_string)
        .collect();
    if !allow_dirty && !dirty_entries.is_empty() {
        print_json(&json!({
            "ok": false,
            "created": false,
            "error": "working tree has uncommitted changes; manually commit desired changes, or rerun with --allow-dirty to ignore",
            "dirty_entries": dirty_entries,
        }));
        return Err(1);
    }

    let head_branch = if let Some(h) = head {
        h
    } else {
        let branch_cmd = vec![
            "git".to_string(),
            "rev-parse".to_string(),
            "--abbrev-ref".to_string(),
            "HEAD".to_string(),
        ];
        let branch_res = run_command(&branch_cmd);
        if branch_res.returncode != 0 {
            print_json(&json!({
                "ok": false,
                "created": false,
                "error": "failed to determine current git branch",
                "command": branch_cmd,
                "stdout": branch_res.stdout.trim(),
                "stderr": branch_res.stderr.trim(),
            }));
            return Err(if branch_res.returncode == 0 { 1 } else { branch_res.returncode });
        }
        let branch = branch_res.stdout.trim().to_string();
        if branch.is_empty() {
            print_json(&json!({
                "ok": false,
                "created": false,
                "error": "current git branch is empty",
            }));
            return Err(1);
        }
        branch
    };

    if push {
        let push_cmd = vec![
            "git".to_string(),
            "push".to_string(),
            "-u".to_string(),
            "origin".to_string(),
            head_branch.clone(),
        ];
        let push_res = run_command(&push_cmd);
        if push_res.returncode != 0 {
            print_json(&json!({
                "ok": false,
                "created": false,
                "error": "failed to push branch",
                "command": push_cmd,
                "stdout": push_res.stdout.trim(),
                "stderr": push_res.stderr.trim(),
            }));
            return Err(if push_res.returncode == 0 { 1 } else { push_res.returncode });
        }
    }

    let mut title_source = "explicit".to_string();
    let mut auto_title: Option<AutoPrTitle> = None;
    let mut title_warning: Option<String> = None;

    let pr_title = if let Some(explicit) = title {
        explicit
    } else {
        match derive_pr_title_from_docs(&base, &head_branch) {
            Ok(Some(derived)) => {
                title_source = "planning-doc".to_string();
                let title = derived.title.clone();
                auto_title = Some(derived);
                title
            }
            Ok(None) => {
                title_source = "branch-default".to_string();
                format!("chore: {}", head_branch.replace('_', "-"))
            }
            Err(e) => {
                title_source = "branch-default".to_string();
                title_warning = Some(e);
                format!("chore: {}", head_branch.replace('_', "-"))
            }
        }
    };
    let close_line = format!("Closes #{issue_no}");
    let pr_body = match body {
        Some(text) => format!("{}\n\n{}", text.trim(), close_line),
        None => close_line.clone(),
    };

    let create_cmd = vec![
        "gh".to_string(),
        "pr".to_string(),
        "create".to_string(),
        "--base".to_string(),
        base.clone(),
        "--head".to_string(),
        head_branch.clone(),
        "--title".to_string(),
        pr_title.clone(),
        "--body".to_string(),
        pr_body.clone(),
        "--assignee".to_string(),
        assignee.clone(),
    ];
    let create_res = run_command(&create_cmd);
    if create_res.returncode != 0 {
        print_json(&json!({
            "ok": false,
            "created": false,
            "command": create_cmd,
            "title": pr_title,
            "title_source": title_source,
            "auto_title": auto_title.as_ref().map(|v| json!({
                "doc": {
                    "path": v.doc_path,
                    "doc_type": v.doc_type,
                    "doc_id": v.doc_id,
                },
                "doc_heading": v.doc_heading,
                "doc_title": v.doc_title,
                "prefix": v.prefix,
                "type_label": v.type_label,
                "selection_reason": v.selection_reason,
                "changed_doc_paths": v.changed_doc_paths,
                "warnings": v.warnings,
            })),
            "title_warning": title_warning,
            "stdout": create_res.stdout.trim(),
            "stderr": create_res.stderr.trim(),
        }));
        return Err(if create_res.returncode == 0 { 1 } else { create_res.returncode });
    }

    let pr_url = create_res
        .stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .last()
        .unwrap_or_default()
        .trim()
        .to_string();

    print_json(&json!({
        "ok": true,
        "created": true,
        "issue_number": issue_no,
        "base": base,
        "head": head_branch,
        "title": pr_title,
        "title_source": title_source,
        "auto_title": auto_title.as_ref().map(|v| json!({
            "doc": {
                "path": v.doc_path,
                "doc_type": v.doc_type,
                "doc_id": v.doc_id,
            },
            "doc_heading": v.doc_heading,
            "doc_title": v.doc_title,
            "prefix": v.prefix,
            "type_label": v.type_label,
            "selection_reason": v.selection_reason,
            "changed_doc_paths": v.changed_doc_paths,
            "warnings": v.warnings,
        })),
        "title_warning": title_warning,
        "close_line": close_line,
        "assignee": assignee,
        "dirty_ignored": allow_dirty && !dirty_entries.is_empty(),
        "pr_url": pr_url,
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

fn cmd_resolve_rfc(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut doc: Option<String> = None;
    let mut issue: Option<i64> = None;
    let mut close = false;
    let mut comment: Option<String> = None;
    let mut allow_legacy = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--doc" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --doc");
                    return Err(1);
                };
                doc = Some(v);
            }
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
            "--close" => {
                close = true;
            }
            "--comment" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --comment");
                    return Err(1);
                };
                comment = Some(v);
            }
            "--allow-legacy" => {
                allow_legacy = true;
            }
            "-h" | "--help" => {
                println!("Usage: ... issue.rs resolve-rfc --doc <docs/rfcs/<id>-<slug>.md> [--issue <n>] [--allow-legacy] [--close] [--comment <text>]");
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
    let doc_type = validated_value
        .get("doc_type")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if doc_type != "rfc" {
        print_json(&json!({
            "ok": false,
            "error": "resolve-rfc only accepts docs/rfcs documents",
            "doc": normalize_path(doc_path),
        }));
        return Err(1);
    }

    let mut precheck_cmd = vec![
        "tools/rfc.rs".to_string(),
        "precheck-rfc-resolve".to_string(),
        "--doc".to_string(),
        normalize_path(doc_path),
    ];
    if allow_legacy {
        precheck_cmd.push("--allow-legacy".to_string());
    }
    let precheck_res = run_command(&precheck_cmd);
    if precheck_res.returncode != 0 {
        print_json(&json!({
            "ok": false,
            "precheck_ok": false,
            "closed": false,
            "doc": normalize_path(doc_path),
            "precheck_command": precheck_cmd,
            "precheck_stdout": precheck_res.stdout.trim(),
            "precheck_stderr": precheck_res.stderr.trim(),
        }));
        return Err(if precheck_res.returncode == 0 { 1 } else { precheck_res.returncode });
    }

    if !close {
        print_json(&json!({
            "ok": true,
            "precheck_ok": true,
            "closed": false,
            "doc": normalize_path(doc_path),
            "message": "RFC resolve precheck passed. No issue closure performed (use --close for explicit closure).",
            "precheck_stdout": precheck_res.stdout.trim(),
        }));
        return Ok(());
    }

    let issue = issue.or_else(|| parse_rfc_issue_hint(doc_path));
    let Some(issue_no) = issue else {
        print_json(&json!({
            "ok": false,
            "precheck_ok": true,
            "closed": false,
            "doc": normalize_path(doc_path),
            "error": "issue number is required for closure (pass --issue or add github_issue in RFC frontmatter)",
        }));
        return Err(1);
    };

    let close_comment = comment.unwrap_or_else(|| {
        format!(
            "RFC resolve precheck passed for `{}`. Closing RFC issue.",
            normalize_path(doc_path)
        )
    });
    let close_cmd = vec![
        "gh".to_string(),
        "issue".to_string(),
        "close".to_string(),
        issue_no.to_string(),
        "--comment".to_string(),
        close_comment,
    ];
    let close_res = run_command(&close_cmd);
    if close_res.returncode != 0 {
        print_json(&json!({
            "ok": false,
            "precheck_ok": true,
            "closed": false,
            "doc": normalize_path(doc_path),
            "issue_number": issue_no,
            "close_command": close_cmd,
            "close_stdout": close_res.stdout.trim(),
            "close_stderr": close_res.stderr.trim(),
        }));
        return Err(if close_res.returncode == 0 { 1 } else { close_res.returncode });
    }

    print_json(&json!({
        "ok": true,
        "precheck_ok": true,
        "closed": true,
        "doc": normalize_path(doc_path),
        "issue_number": issue_no,
        "precheck_stdout": precheck_res.stdout.trim(),
        "close_stdout": close_res.stdout.trim(),
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

fn derive_pr_title_from_docs(base: &str, head: &str) -> Result<Option<AutoPrTitle>, String> {
    let candidates = collect_pr_doc_candidates_from_diff(base, head)?;
    if candidates.is_empty() {
        return Ok(None);
    }

    let task_count = candidates.iter().filter(|v| v.doc_type == "task").count();
    let rfc_count = candidates.iter().filter(|v| v.doc_type == "rfc").count();
    let has_task = task_count > 0;
    let has_rfc = rfc_count > 0;

    let mut preferred: Vec<PrDocCandidate> = if has_rfc {
        candidates
            .iter()
            .filter(|v| v.doc_type == "rfc")
            .cloned()
            .collect()
    } else {
        candidates
            .iter()
            .filter(|v| v.doc_type == "task")
            .cloned()
            .collect()
    };
    preferred.sort_by(|a, b| {
        let a_id = a.doc_id.parse::<u64>().unwrap_or(0);
        let b_id = b.doc_id.parse::<u64>().unwrap_or(0);
        b_id.cmp(&a_id).then_with(|| a.path.cmp(&b.path))
    });
    let Some(selected) = preferred.into_iter().next() else {
        return Ok(None);
    };

    let selection_reason = if has_rfc && has_task {
        "rfc_preferred_when_task_and_rfc_present"
    } else if has_rfc && rfc_count > 1 {
        "multiple_rfc_docs_highest_id"
    } else if has_rfc {
        "single_rfc_doc"
    } else if task_count > 1 {
        "multiple_task_docs_highest_id"
    } else {
        "single_task_doc"
    };

    let selected_path = Path::new(&selected.path);
    let doc_heading = title_hint(selected_path);
    let mut warnings = Vec::new();

    let doc_title = match doc_heading
        .as_deref()
        .and_then(|v| normalize_doc_title_for_pr(v, &selected.doc_type, &selected.doc_id))
    {
        Some(v) => v,
        None => {
            warnings.push(format!(
                "missing or invalid markdown heading in {}",
                selected.path
            ));
            default_doc_title_for_pr(&selected.doc_type, &selected.doc_id)
        }
    };

    let (type_label, label_warnings) = detect_doc_type_label_for_pr(selected_path);
    warnings.extend(label_warnings);
    let prefix = type_label
        .as_deref()
        .map(pr_prefix_from_type_label)
        .unwrap_or_else(|| default_pr_prefix_for_doc_type(&selected.doc_type))
        .to_string();

    Ok(Some(AutoPrTitle {
        title: format!("{prefix}: {doc_title}"),
        doc_path: selected.path.clone(),
        doc_type: selected.doc_type,
        doc_id: selected.doc_id,
        doc_heading,
        doc_title,
        prefix,
        type_label,
        selection_reason: selection_reason.to_string(),
        changed_doc_paths: candidates.into_iter().map(|v| v.path).collect(),
        warnings,
    }))
}

fn collect_pr_doc_candidates_from_diff(base: &str, head: &str) -> Result<Vec<PrDocCandidate>, String> {
    let effective_base = resolve_diff_base_ref(base);
    let range = format!("{effective_base}...{head}");
    let cmd = vec![
        "git".to_string(),
        "diff".to_string(),
        "--name-only".to_string(),
        range.clone(),
    ];
    let res = run_command(&cmd);
    if res.returncode != 0 {
        return Err(format!(
            "failed to inspect changed files for range `{range}` (stdout: {}; stderr: {})",
            res.stdout.trim(),
            res.stderr.trim()
        ));
    }

    let mut seen = BTreeSet::new();
    let mut docs = Vec::new();
    for line in res.stdout.lines() {
        let raw = line.trim();
        if raw.is_empty() {
            continue;
        }
        if !seen.insert(raw.to_string()) {
            continue;
        }
        let Some(candidate) = parse_pr_doc_candidate(raw) else {
            continue;
        };
        if Path::new(&candidate.path).is_file() {
            docs.push(candidate);
        }
    }

    docs.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(docs)
}

fn resolve_diff_base_ref(base: &str) -> String {
    if base.trim().is_empty() || base.starts_with("origin/") {
        return base.to_string();
    }
    let candidate = format!("origin/{base}");
    let cmd = vec![
        "git".to_string(),
        "rev-parse".to_string(),
        "--verify".to_string(),
        "--quiet".to_string(),
        candidate.clone(),
    ];
    let res = run_command(&cmd);
    if res.returncode == 0 && !res.stdout.trim().is_empty() {
        return candidate;
    }
    base.to_string()
}

fn parse_pr_doc_candidate(path: &str) -> Option<PrDocCandidate> {
    let normalized = path
        .trim()
        .trim_start_matches("./")
        .replace('\\', "/");

    if let Some(filename) = normalized.strip_prefix("docs/tasks/") {
        let (doc_id, _) = parse_doc_filename(filename, 6, "000000-template.md")?;
        return Some(PrDocCandidate {
            path: normalized,
            doc_type: "task".to_string(),
            doc_id,
        });
    }

    if let Some(filename) = normalized.strip_prefix("docs/rfcs/") {
        let (doc_id, _) = parse_doc_filename(filename, 4, "0000-template.md")?;
        return Some(PrDocCandidate {
            path: normalized,
            doc_type: "rfc".to_string(),
            doc_id,
        });
    }

    None
}

fn parse_doc_filename(filename: &str, id_width: usize, template_name: &str) -> Option<(String, String)> {
    if filename == template_name {
        return None;
    }
    if filename.contains('/') {
        return None;
    }
    let stem = filename.strip_suffix(".md")?;
    let (doc_id, slug) = stem.split_once('-')?;
    if doc_id.len() != id_width || !doc_id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if slug.is_empty() {
        return None;
    }
    Some((doc_id.to_string(), slug.to_string()))
}

fn normalize_doc_title_for_pr(heading: &str, doc_type: &str, doc_id: &str) -> Option<String> {
    let trimmed = heading.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some((left, right)) = trimmed.split_once(':') {
        let prefix = left.trim();
        let title = right.trim();
        if !title.is_empty()
            && ((doc_type == "task" && is_task_heading_prefix(prefix))
                || (doc_type == "rfc" && is_rfc_heading_prefix(prefix, doc_id)))
        {
            return Some(title.to_string());
        }
    }

    Some(trimmed.to_string())
}

fn is_task_heading_prefix(prefix: &str) -> bool {
    prefix.eq_ignore_ascii_case("task") || prefix.to_ascii_lowercase().starts_with("task ")
}

fn is_rfc_heading_prefix(prefix: &str, doc_id: &str) -> bool {
    if prefix.eq_ignore_ascii_case("rfc") {
        return true;
    }
    let upper = prefix.to_ascii_uppercase();
    if upper == format!("RFC-{doc_id}") {
        return true;
    }
    match upper.strip_prefix("RFC-") {
        Some(rest) => !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    }
}

fn default_doc_title_for_pr(doc_type: &str, doc_id: &str) -> String {
    match doc_type {
        "rfc" => format!("RFC-{doc_id}"),
        _ => format!("Task {doc_id}"),
    }
}

fn detect_doc_type_label_for_pr(path: &Path) -> (Option<String>, Vec<String>) {
    match parse_doc_issue_labels(path) {
        Ok(labels) => {
            let mut type_label = None;
            for raw in labels {
                let label = normalize_label_token(&raw);
                if label.starts_with("type:") && is_allowed_type_label(&label) {
                    type_label = Some(label);
                    break;
                }
            }
            (type_label, Vec::new())
        }
        Err(e) => (None, vec![e]),
    }
}

fn pr_prefix_from_type_label(type_label: &str) -> &'static str {
    match type_label {
        "type:feature" | "type:epic" => "feat",
        "type:bug" => "fix",
        "type:perf" => "perf",
        "type:doc" => "docs",
        "type:question" | "type:chore" | "type:task" => "chore",
        _ => "chore",
    }
}

fn default_pr_prefix_for_doc_type(doc_type: &str) -> &'static str {
    match doc_type {
        "rfc" => "feat",
        _ => "chore",
    }
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

fn parse_rfc_issue_hint(path: &Path) -> Option<i64> {
    let text = fs::read_to_string(path).ok()?;
    let lines: Vec<&str> = text.lines().collect();
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
        if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
            if let Ok(parsed) = value.parse::<i64>() {
                return Some(parsed);
            }
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
