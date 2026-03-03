#!/usr/bin/env -S cargo +nightly -q -Zscript
---
[package]
edition = "2024"
---

#![allow(dead_code)]

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const BACKLOG_DIR: &str = "docs/backlogs";
const BACKLOG_CLOSED_DIR: &str = "docs/backlogs/closed";
const BACKLOG_NEXT_ID_FILE: &str = "docs/backlogs/next-id";
const RFC_DIR: &str = "docs/rfcs";
const DOC_ID_TOOL: &str = "tools/doc-id.rs";

fn usage() -> &'static str {
    "Usage: tools/task.rs <subcommand> [options]\n\n\
Subcommands:\n\
  next-task-id          Print the next task id from docs/tasks/next-id\n\
  create-task-doc       Create a docs/tasks task document from template with validated id and slug\n\
  resolve-task-rfc      Sync task resolve outcome into parent RFC Implementation Phases\n"
}

fn next_task_id_usage() -> &'static str {
    "Usage: tools/task.rs next-task-id"
}

fn create_task_doc_usage() -> &'static str {
    "Usage: tools/task.rs create-task-doc --title <title> --slug <slug> (--id <6digits> | --auto-id) [--template <path>] [--output-dir <path>] [--force]"
}

fn init_backlog_next_id_usage() -> &'static str {
    "Usage: tools/task.rs init-backlog-next-id [--path <docs/backlogs/next-id>] [--value <6digits>] [--force]"
}

fn alloc_backlog_id_usage() -> &'static str {
    "Usage: tools/task.rs alloc-backlog-id [--path <docs/backlogs/next-id>]"
}

fn close_backlog_doc_usage() -> &'static str {
    "Usage: tools/task.rs close-backlog-doc (--path <docs/backlogs/<id>-<slug>.md> | --id <6digits>) --type <type> --detail <text> [--reference <text>] [--date <YYYY-MM-DD>] [--force-reason-update]"
}

fn complete_backlog_doc_usage() -> &'static str {
    "Usage: tools/task.rs complete-backlog-doc (--path <docs/backlogs/<id>-<slug>.md> | --id <6digits>) --task <docs/tasks/<id>-<slug>.md> [--detail <text>] [--date <YYYY-MM-DD>] [--force-reason-update]"
}

fn resolve_task_backlogs_usage() -> &'static str {
    "Usage: tools/task.rs resolve-task-backlogs --task <docs/tasks/<id>-<slug>.md> [--date <YYYY-MM-DD>] [--allow-missing]"
}

fn resolve_task_rfc_usage() -> &'static str {
    "Usage: tools/task.rs resolve-task-rfc --task <docs/tasks/<id>-<slug>.md> [--date <YYYY-MM-DD>] [--summary <text>] [--rfc <docs/rfcs/<id>-<slug>.md>]"
}

#[derive(Clone)]
struct CloseReason {
    reason_type: String,
    detail: String,
    closed_by: String,
    reference: String,
    closed_at: String,
}

struct ResolveSummary {
    task: String,
    closed: Vec<String>,
    already_closed: Vec<String>,
    missing: Vec<String>,
    rfc_sync: RfcSyncSummary,
}

#[derive(Clone)]
struct RfcSyncSummary {
    checked: bool,
    has_parent_rfc: bool,
    rfc_doc: Option<String>,
    updated: bool,
    phase: Option<String>,
    detail: Option<String>,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let Some(subcommand) = args.next() else {
        return Err(usage().to_string());
    };

    match subcommand.as_str() {
        "-h" | "--help" => {
            println!("{}", usage());
            Ok(())
        }
        "next-task-id" => run_next_task_id(args),
        "create-task-doc" => run_create_task_doc(args),
        "resolve-task-rfc" => run_resolve_task_rfc(args),
        _ => Err(format!("unknown subcommand: {subcommand}\n{}", usage())),
    }
}

fn run_next_task_id(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    if let Some(arg) = args.next() {
        if arg == "-h" || arg == "--help" {
            println!("{}", next_task_id_usage());
            return Ok(());
        }
        return Err(format!("unknown arg: {arg}\n{}", next_task_id_usage()));
    }

    let id = run_doc_id_and_capture(["peek-next-id", "--kind", "task"])?;
    println!("{id}");
    Ok(())
}

fn run_create_task_doc(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut title: Option<String> = None;
    let mut slug: Option<String> = None;
    let mut task_id: Option<String> = None;
    let mut auto_id = false;
    let mut template = PathBuf::from("docs/tasks/000000-template.md");
    let mut output_dir = PathBuf::from("docs/tasks");
    let mut force = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--title" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --title\n{}",
                        create_task_doc_usage()
                    ));
                };
                title = Some(v);
            }
            "--slug" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --slug\n{}",
                        create_task_doc_usage()
                    ));
                };
                slug = Some(v);
            }
            "--id" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --id\n{}",
                        create_task_doc_usage()
                    ));
                };
                task_id = Some(v);
            }
            "--auto-id" => {
                auto_id = true;
            }
            "--template" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --template\n{}",
                        create_task_doc_usage()
                    ));
                };
                template = PathBuf::from(v);
            }
            "--output-dir" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --output-dir\n{}",
                        create_task_doc_usage()
                    ));
                };
                output_dir = PathBuf::from(v);
            }
            "--force" => {
                force = true;
            }
            "-h" | "--help" => {
                println!("{}", create_task_doc_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", create_task_doc_usage())),
        }
    }

    let title = title
        .ok_or_else(|| format!("missing required arg: --title\n{}", create_task_doc_usage()))?;
    let slug =
        slug.ok_or_else(|| format!("missing required arg: --slug\n{}", create_task_doc_usage()))?;
    let slug = validate_slug(&slug)?;

    if auto_id && task_id.is_some() {
        return Err("use either --id or --auto-id, not both".to_string());
    }
    if !auto_id && task_id.is_none() {
        return Err(format!(
            "one of --id or --auto-id is required\n{}",
            create_task_doc_usage()
        ));
    }

    let task_id = if auto_id {
        run_doc_id_and_capture(["alloc-id", "--kind", "task"])?
    } else {
        validate_fixed_id(&task_id.expect("checked is_some"))?
    };

    let template_text = load_template(&template)?;
    let content = apply_title(template_text, &title)?;
    let out_path = output_dir.join(format!("{task_id}-{slug}.md"));

    if out_path.exists() && !force {
        return Err(format!(
            "output file already exists: {} (use --force to overwrite)",
            normalize_path(&out_path)
        ));
    }

    fs::write(&out_path, content)
        .map_err(|e| format!("failed to write {}: {e}", normalize_path(&out_path)))?;

    println!("{}", normalize_path(&out_path));
    Ok(())
}

fn run_init_backlog_next_id(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut path = PathBuf::from(BACKLOG_NEXT_ID_FILE);
    let mut value: Option<String> = None;
    let mut force = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --path\n{}",
                        init_backlog_next_id_usage()
                    ));
                };
                path = PathBuf::from(v);
            }
            "--value" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --value\n{}",
                        init_backlog_next_id_usage()
                    ));
                };
                value = Some(validate_fixed_id(&v)?);
            }
            "--force" => {
                force = true;
            }
            "-h" | "--help" => {
                println!("{}", init_backlog_next_id_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", init_backlog_next_id_usage())),
        }
    }

    if path.exists() && !force {
        return Err(format!(
            "{} already exists (use --force to overwrite)",
            normalize_path(&path)
        ));
    }

    let next = match value {
        Some(v) => v,
        None => {
            let max_id = detect_max_backlog_id(Path::new(BACKLOG_DIR), Path::new(BACKLOG_CLOSED_DIR))?;
            format!("{:06}", max_id + 1)
        }
    };

    ensure_parent_dir_exists(&path)?;
    fs::write(&path, format!("{next}\n"))
        .map_err(|e| format!("failed to write {}: {e}", normalize_path(&path)))?;
    println!("{}", normalize_path(&path));
    Ok(())
}

fn run_alloc_backlog_id(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut path = PathBuf::from(BACKLOG_NEXT_ID_FILE);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --path\n{}",
                        alloc_backlog_id_usage()
                    ));
                };
                path = PathBuf::from(v);
            }
            "-h" | "--help" => {
                println!("{}", alloc_backlog_id_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", alloc_backlog_id_usage())),
        }
    }

    let current = read_next_backlog_id(&path)?;
    let current_num = current
        .parse::<u32>()
        .map_err(|_| format!("invalid next-id content in {}", normalize_path(&path)))?;
    if current_num >= 999_999 {
        return Err(format!("next-id overflow in {}", normalize_path(&path)));
    }
    let next_num = current_num + 1;
    fs::write(&path, format!("{:06}\n", next_num))
        .map_err(|e| format!("failed to update {}: {e}", normalize_path(&path)))?;
    println!("{current}");
    Ok(())
}

fn run_close_backlog_doc(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut path: Option<PathBuf> = None;
    let mut backlog_id: Option<String> = None;
    let mut reason_type: Option<String> = None;
    let mut detail: Option<String> = None;
    let mut reference: Option<String> = None;
    let mut date: Option<String> = None;
    let mut force_reason_update = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --path\n{}",
                        close_backlog_doc_usage()
                    ));
                };
                path = Some(PathBuf::from(v));
            }
            "--id" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --id\n{}",
                        close_backlog_doc_usage()
                    ));
                };
                backlog_id = Some(validate_fixed_id(&v)?);
            }
            "--type" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --type\n{}",
                        close_backlog_doc_usage()
                    ));
                };
                reason_type = Some(v);
            }
            "--detail" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --detail\n{}",
                        close_backlog_doc_usage()
                    ));
                };
                detail = Some(v);
            }
            "--reference" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --reference\n{}",
                        close_backlog_doc_usage()
                    ));
                };
                reference = Some(v);
            }
            "--date" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --date\n{}",
                        close_backlog_doc_usage()
                    ));
                };
                date = Some(v);
            }
            "--force-reason-update" => {
                force_reason_update = true;
            }
            "-h" | "--help" => {
                println!("{}", close_backlog_doc_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", close_backlog_doc_usage())),
        }
    }

    let open_path = resolve_open_backlog_path(path, backlog_id, close_backlog_doc_usage())?;
    let reason = CloseReason {
        reason_type: reason_type
            .ok_or_else(|| format!("missing required arg: --type\n{}", close_backlog_doc_usage()))?,
        detail: detail
            .ok_or_else(|| format!("missing required arg: --detail\n{}", close_backlog_doc_usage()))?,
        closed_by: "task close".to_string(),
        reference: reference.unwrap_or_else(|| "User decision".to_string()),
        closed_at: date.unwrap_or_else(today_yyyy_mm_dd),
    };

    let closed_path = archive_backlog_with_reason(&open_path, &reason, force_reason_update)?;
    println!("{}", normalize_path(&closed_path));
    Ok(())
}

fn run_complete_backlog_doc(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut path: Option<PathBuf> = None;
    let mut backlog_id: Option<String> = None;
    let mut task_path: Option<PathBuf> = None;
    let mut detail: Option<String> = None;
    let mut date: Option<String> = None;
    let mut force_reason_update = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --path\n{}",
                        complete_backlog_doc_usage()
                    ));
                };
                path = Some(PathBuf::from(v));
            }
            "--id" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --id\n{}",
                        complete_backlog_doc_usage()
                    ));
                };
                backlog_id = Some(validate_fixed_id(&v)?);
            }
            "--task" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --task\n{}",
                        complete_backlog_doc_usage()
                    ));
                };
                task_path = Some(PathBuf::from(v));
            }
            "--detail" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --detail\n{}",
                        complete_backlog_doc_usage()
                    ));
                };
                detail = Some(v);
            }
            "--date" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --date\n{}",
                        complete_backlog_doc_usage()
                    ));
                };
                date = Some(v);
            }
            "--force-reason-update" => {
                force_reason_update = true;
            }
            "-h" | "--help" => {
                println!("{}", complete_backlog_doc_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", complete_backlog_doc_usage())),
        }
    }

    let open_path = resolve_open_backlog_path(path, backlog_id, complete_backlog_doc_usage())?;
    let task_path = task_path
        .ok_or_else(|| format!("missing required arg: --task\n{}", complete_backlog_doc_usage()))?;
    validate_task_doc_path(&task_path)?;

    let task_ref = normalize_path(&task_path);
    let reason = CloseReason {
        reason_type: "implemented".to_string(),
        detail: detail.unwrap_or_else(|| format!("Implemented via {task_ref}")),
        closed_by: "task resolve".to_string(),
        reference: task_ref,
        closed_at: date.unwrap_or_else(today_yyyy_mm_dd),
    };

    let closed_path = archive_backlog_with_reason(&open_path, &reason, force_reason_update)?;
    println!("{}", normalize_path(&closed_path));
    Ok(())
}

fn run_resolve_task_backlogs(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut task_path: Option<PathBuf> = None;
    let mut date: Option<String> = None;
    let mut allow_missing = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--task" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --task\n{}",
                        resolve_task_backlogs_usage()
                    ));
                };
                task_path = Some(PathBuf::from(v));
            }
            "--date" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --date\n{}",
                        resolve_task_backlogs_usage()
                    ));
                };
                date = Some(v);
            }
            "--allow-missing" => {
                allow_missing = true;
            }
            "-h" | "--help" => {
                println!("{}", resolve_task_backlogs_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", resolve_task_backlogs_usage())),
        }
    }

    let task_path = task_path
        .ok_or_else(|| format!("missing required arg: --task\n{}", resolve_task_backlogs_usage()))?;
    validate_task_doc_path(&task_path)?;

    let task_text = fs::read_to_string(&task_path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(&task_path)))?;

    let refs = extract_source_backlog_paths(&task_text);
    let task_ref = normalize_path(&task_path);
    let closed_at = date.unwrap_or_else(today_yyyy_mm_dd);

    let mut summary = ResolveSummary {
        task: task_ref.clone(),
        closed: Vec::new(),
        already_closed: Vec::new(),
        missing: Vec::new(),
        rfc_sync: RfcSyncSummary {
            checked: false,
            has_parent_rfc: false,
            rfc_doc: None,
            updated: false,
            phase: None,
            detail: None,
        },
    };

    for raw_ref in refs {
        let resolved = resolve_backlog_ref(&raw_ref)?;
        match resolved {
            BacklogRefResolution::Open(open_path) => {
                let reason = CloseReason {
                    reason_type: "implemented".to_string(),
                    detail: format!("Implemented via {task_ref}"),
                    closed_by: "task resolve".to_string(),
                    reference: task_ref.clone(),
                    closed_at: closed_at.clone(),
                };
                let closed_path = archive_backlog_with_reason(&open_path, &reason, false)?;
                summary.closed.push(normalize_path(&closed_path));
            }
            BacklogRefResolution::Closed(closed_path) => {
                summary.already_closed.push(normalize_path(&closed_path));
            }
            BacklogRefResolution::Missing(info) => {
                summary.missing.push(info);
            }
        }
    }

    summary.rfc_sync = sync_task_into_parent_rfc(&task_path, None, Some(closed_at.clone()), None)?;

    println!("{}", resolve_summary_json(&summary));

    if !allow_missing && !summary.missing.is_empty() {
        return Err("one or more referenced source backlogs are missing".to_string());
    }

    Ok(())
}

fn run_resolve_task_rfc(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut task_path: Option<PathBuf> = None;
    let mut date: Option<String> = None;
    let mut summary: Option<String> = None;
    let mut rfc_override: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--task" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --task\n{}",
                        resolve_task_rfc_usage()
                    ));
                };
                task_path = Some(PathBuf::from(v));
            }
            "--date" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --date\n{}",
                        resolve_task_rfc_usage()
                    ));
                };
                date = Some(v);
            }
            "--summary" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --summary\n{}",
                        resolve_task_rfc_usage()
                    ));
                };
                summary = Some(v);
            }
            "--rfc" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --rfc\n{}",
                        resolve_task_rfc_usage()
                    ));
                };
                rfc_override = Some(PathBuf::from(v));
            }
            "-h" | "--help" => {
                println!("{}", resolve_task_rfc_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", resolve_task_rfc_usage())),
        }
    }

    let task_path = task_path
        .ok_or_else(|| format!("missing required arg: --task\n{}", resolve_task_rfc_usage()))?;
    validate_task_doc_path(&task_path)?;

    let sync = sync_task_into_parent_rfc(&task_path, summary, date, rfc_override)?;
    println!("{}", rfc_sync_summary_json(&sync));
    Ok(())
}

fn validate_task_doc_path(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Err(format!("task doc not found: {}", normalize_path(path)));
    }
    if !path.is_file() {
        return Err(format!("task doc is not a file: {}", normalize_path(path)));
    }

    let parent = path
        .parent()
        .ok_or_else(|| format!("invalid task doc path: {}", normalize_path(path)))?;
    if normalize_path(parent) != "docs/tasks" {
        return Err(format!(
            "task doc must be under docs/tasks: {}",
            normalize_path(path)
        ));
    }

    let name = path
        .file_name()
        .ok_or_else(|| format!("invalid task doc path: {}", normalize_path(path)))?
        .to_string_lossy()
        .to_string();

    if parse_strict_six_digit_task_name(&name).is_none() {
        return Err(format!(
            "invalid task doc name: {} (expected <6digits>-<slug>.md)",
            normalize_path(path)
        ));
    }

    Ok(())
}

fn resolve_open_backlog_path(
    path: Option<PathBuf>,
    backlog_id: Option<String>,
    usage_msg: &str,
) -> Result<PathBuf, String> {
    if path.is_some() && backlog_id.is_some() {
        return Err(format!("use either --path or --id, not both\n{usage_msg}"));
    }
    if path.is_none() && backlog_id.is_none() {
        return Err(format!("one of --path or --id is required\n{usage_msg}"));
    }

    if let Some(path) = path {
        let path = normalize_reference_path(&path);
        if !path.exists() {
            return Err(format!("backlog file not found: {}", normalize_path(&path)));
        }
        ensure_open_backlog_path(&path)?;
        return Ok(path);
    }

    let id = backlog_id.expect("checked is_some");
    resolve_open_backlog_by_id(&id)
}

fn resolve_open_backlog_by_id(id: &str) -> Result<PathBuf, String> {
    let open_matches = find_backlog_by_id(Path::new(BACKLOG_DIR), id)?;
    if open_matches.len() == 1 {
        return Ok(open_matches[0].clone());
    }
    if open_matches.len() > 1 {
        return Err(format!(
            "multiple open backlogs found for id {id}: {}",
            open_matches
                .iter()
                .map(|p| normalize_path(p))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let closed_matches = find_backlog_by_id(Path::new(BACKLOG_CLOSED_DIR), id)?;
    if closed_matches.len() == 1 {
        return Err(format!(
            "backlog {id} is already closed: {}",
            normalize_path(&closed_matches[0])
        ));
    }

    Err(format!("no open backlog found for id {id}"))
}

fn ensure_open_backlog_path(path: &Path) -> Result<(), String> {
    if !path.is_file() {
        return Err(format!("not a file: {}", normalize_path(path)));
    }

    let parent = path
        .parent()
        .ok_or_else(|| format!("invalid path: {}", normalize_path(path)))?;
    if normalize_path(parent) != BACKLOG_DIR {
        return Err(format!(
            "backlog path must be under {}: {}",
            BACKLOG_DIR,
            normalize_path(path)
        ));
    }

    let name = path
        .file_name()
        .ok_or_else(|| format!("invalid backlog path: {}", normalize_path(path)))?
        .to_string_lossy()
        .to_string();

    if name == "000000-template.md" {
        return Err("template backlog doc cannot be closed".to_string());
    }

    if parse_backlog_name_new(&name).is_none() {
        return Err(format!(
            "invalid backlog file name: {} (expected <6digits>-<slug>.md)",
            normalize_path(path)
        ));
    }

    Ok(())
}

fn archive_backlog_with_reason(
    open_path: &Path,
    reason: &CloseReason,
    force_reason_update: bool,
) -> Result<PathBuf, String> {
    ensure_open_backlog_path(open_path)?;

    let content = fs::read_to_string(open_path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(open_path)))?;
    let updated = upsert_close_reason(&content, reason, force_reason_update)?;
    fs::write(open_path, updated)
        .map_err(|e| format!("failed to write {}: {e}", normalize_path(open_path)))?;

    let closed_dir = Path::new(BACKLOG_CLOSED_DIR);
    if !closed_dir.exists() {
        fs::create_dir_all(closed_dir)
            .map_err(|e| format!("failed to create {}: {e}", normalize_path(closed_dir)))?;
    }

    let file_name = open_path
        .file_name()
        .ok_or_else(|| format!("invalid file name: {}", normalize_path(open_path)))?;
    let closed_path = closed_dir.join(file_name);
    if closed_path.exists() {
        return Err(format!(
            "closed backlog file already exists: {}",
            normalize_path(&closed_path)
        ));
    }

    fs::rename(open_path, &closed_path).map_err(|e| {
        format!(
            "failed to move {} -> {}: {e}",
            normalize_path(open_path),
            normalize_path(&closed_path)
        )
    })?;

    Ok(closed_path)
}

fn upsert_close_reason(
    content: &str,
    reason: &CloseReason,
    force_reason_update: bool,
) -> Result<String, String> {
    let section = render_close_reason(reason);
    let marker = "\n## Close Reason\n";
    let marker_at_start = "## Close Reason\n";

    let existing_idx = content
        .find(marker)
        .map(|idx| idx + 1)
        .or_else(|| content.find(marker_at_start));

    let base = if let Some(idx) = existing_idx {
        if !force_reason_update {
            return Err(
                "Close Reason already exists (use --force-reason-update to replace)".to_string(),
            );
        }
        content[..idx].trim_end().to_string()
    } else {
        content.trim_end().to_string()
    };

    Ok(format!("{}\n\n{}\n", base, section))
}

fn render_close_reason(reason: &CloseReason) -> String {
    format!(
        "## Close Reason\n\n- Type: {}\n- Detail: {}\n- Closed By: {}\n- Reference: {}\n- Closed At: {}",
        reason.reason_type,
        reason.detail,
        reason.closed_by,
        reason.reference,
        reason.closed_at
    )
}

fn extract_source_backlog_paths(task_text: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut in_source_backlogs = false;

    for line in task_text.lines() {
        let trimmed = line.trim();

        if trimmed == "Source Backlogs:" || trimmed == "`Source Backlogs:`" {
            in_source_backlogs = true;
            continue;
        }

        if in_source_backlogs {
            if trimmed.is_empty() {
                continue;
            }
            // Accept markdown list markers and code ticks.
            let cand = trimmed
                .trim_start_matches('-')
                .trim_start_matches('*')
                .trim()
                .trim_matches('`');
            if cand.starts_with("docs/backlogs/") && cand.ends_with(".md") {
                refs.push(cand.to_string());
                continue;
            }
            // Leave source backlog section when first non-list/non-path line appears.
            if trimmed.starts_with("## ") {
                in_source_backlogs = false;
            }
        }
    }

    refs.sort();
    refs.dedup();
    refs
}

enum BacklogRefResolution {
    Open(PathBuf),
    Closed(PathBuf),
    Missing(String),
}

fn resolve_backlog_ref(raw_ref: &str) -> Result<BacklogRefResolution, String> {
    let raw = PathBuf::from(raw_ref);
    let normalized = normalize_reference_path(&raw);
    let normalized_text = normalize_path(&normalized);

    if normalized_text.starts_with(&format!("{BACKLOG_CLOSED_DIR}/")) {
        if normalized.exists() {
            return Ok(BacklogRefResolution::Closed(normalized));
        }
        return Ok(BacklogRefResolution::Missing(normalized_text));
    }

    if normalized.exists() {
        ensure_open_backlog_path(&normalized)?;
        return Ok(BacklogRefResolution::Open(normalized));
    }

    let candidates = backlog_ref_candidates(&normalized_text);
    for open in &candidates.open_candidates {
        let p = PathBuf::from(open);
        if p.exists() {
            ensure_open_backlog_path(&p)?;
            return Ok(BacklogRefResolution::Open(p));
        }
    }
    for closed in &candidates.closed_candidates {
        let p = PathBuf::from(closed);
        if p.exists() {
            return Ok(BacklogRefResolution::Closed(p));
        }
    }

    if let Some(id) = candidates.id.as_deref() {
        if let Ok(open_path) = resolve_open_backlog_by_id(id) {
            return Ok(BacklogRefResolution::Open(open_path));
        }
        let closed_matches = find_backlog_by_id(Path::new(BACKLOG_CLOSED_DIR), id)?;
        if let Some(closed) = closed_matches.first() {
            return Ok(BacklogRefResolution::Closed(closed.clone()));
        }
    }

    Ok(BacklogRefResolution::Missing(normalized_text))
}

fn sync_task_into_parent_rfc(
    task_path: &Path,
    summary_override: Option<String>,
    date: Option<String>,
    rfc_override: Option<PathBuf>,
) -> Result<RfcSyncSummary, String> {
    let task_path = normalize_reference_path(task_path);
    validate_task_doc_path(&task_path)?;
    let task_text = fs::read_to_string(&task_path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(&task_path)))?;
    let task_ref = normalize_path(&task_path);

    let parent_rfc = if let Some(override_path) = rfc_override {
        let normalized = normalize_reference_path(&override_path);
        validate_rfc_doc_path(&normalized)?;
        normalized
    } else {
        let refs = extract_doc_refs_by_prefix(&task_text, "docs/rfcs/")
            .into_iter()
            .filter(|ref_path| {
                let name = Path::new(ref_path)
                    .file_name()
                    .map(|x| x.to_string_lossy().to_string())
                    .unwrap_or_default();
                match parse_strict_four_digit_rfc_name(&name) {
                    Some(id) => id != 0,
                    None => false,
                }
            })
            .collect::<Vec<_>>();
        if refs.is_empty() {
            return Ok(RfcSyncSummary {
                checked: true,
                has_parent_rfc: false,
                rfc_doc: None,
                updated: false,
                phase: None,
                detail: Some("no parent RFC reference found in task doc".to_string()),
            });
        }
        if refs.len() > 1 {
            return Err(format!(
                "multiple RFC references found in task doc: {} (use --rfc to disambiguate)",
                refs.join(", ")
            ));
        }
        let p = PathBuf::from(&refs[0]);
        validate_rfc_doc_path(&p)?;
        p
    };

    let notes = extract_markdown_section(&task_text, "Implementation Notes").unwrap_or_default();
    if is_blank_implementation_notes(&notes) {
        return Err(format!(
            "task resolve requires non-empty `Implementation Notes` before RFC sync: {}",
            task_ref
        ));
    }

    let summary = match summary_override {
        Some(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => first_meaningful_line(&notes)
            .unwrap_or_else(|| "Implementation completed; see task implementation notes".to_string()),
    };
    let sync_date = date.unwrap_or_else(today_yyyy_mm_dd);

    let (updated, phase, detail) =
        apply_task_sync_into_rfc(&parent_rfc, &task_ref, &summary, &sync_date)?;

    Ok(RfcSyncSummary {
        checked: true,
        has_parent_rfc: true,
        rfc_doc: Some(normalize_path(&parent_rfc)),
        updated,
        phase: Some(phase),
        detail: Some(detail),
    })
}

fn apply_task_sync_into_rfc(
    rfc_path: &Path,
    task_ref: &str,
    summary: &str,
    date: &str,
) -> Result<(bool, String, String), String> {
    let original = fs::read_to_string(rfc_path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(rfc_path)))?;
    let mut lines: Vec<String> = original.lines().map(|x| x.to_string()).collect();

    let section_start = lines
        .iter()
        .position(|line| line.trim() == "## Implementation Phases")
        .ok_or_else(|| {
            format!(
                "missing `## Implementation Phases` in parent RFC: {}",
                normalize_path(rfc_path)
            )
        })?;
    let section_end = lines
        .iter()
        .enumerate()
        .skip(section_start + 1)
        .find(|(_, line)| line.starts_with("## "))
        .map(|(idx, _)| idx)
        .unwrap_or(lines.len());

    let task_line_idx = lines
        .iter()
        .enumerate()
        .skip(section_start + 1)
        .take(section_end.saturating_sub(section_start + 1))
        .find(|(_, line)| line.contains(task_ref))
        .map(|(idx, _)| idx)
        .ok_or_else(|| {
            format!(
                "cannot find task reference `{}` inside RFC Implementation Phases: {}",
                task_ref,
                normalize_path(rfc_path)
            )
        })?;

    let phase_start = (section_start + 1..=task_line_idx)
        .rev()
        .find(|idx| {
            let trimmed = lines[*idx].trim_start();
            trimmed.starts_with("- **Phase ") || trimmed.starts_with("### Phase ")
        })
        .ok_or_else(|| {
            format!(
                "cannot locate parent phase heading for task `{task_ref}` in {}",
                normalize_path(rfc_path)
            )
        })?;

    let phase_end = (task_line_idx + 1..section_end)
        .find(|idx| {
            let trimmed = lines[*idx].trim_start();
            trimmed.starts_with("- **Phase ") || trimmed.starts_with("### Phase ")
        })
        .unwrap_or(section_end);

    let phase_title = lines[phase_start].trim().to_string();
    let status_line = "  - Phase Status: done".to_string();
    let impl_line = format!(
        "  - Implementation Summary: {} [Task Resolve Sync: {} @ {}]",
        collapse_inline(summary),
        task_ref,
        date
    );

    let mut status_idx = None;
    let mut impl_idx = None;
    for idx in phase_start + 1..phase_end {
        let trimmed = lines[idx].trim_start();
        if trimmed.starts_with("- Phase Status:") {
            status_idx = Some(idx);
        } else if trimmed.starts_with("- Implementation Summary:") {
            impl_idx = Some(idx);
        }
    }

    if let Some(idx) = status_idx {
        lines[idx] = status_line;
    } else {
        let insert_at = task_line_idx + 1;
        lines.insert(insert_at, status_line);
    }

    let phase_end_adjusted = phase_end + if status_idx.is_none() { 1 } else { 0 };
    if let Some(idx) = impl_idx {
        lines[idx + if status_idx.is_none() && idx >= task_line_idx + 1 { 1 } else { 0 }] =
            impl_line;
    } else {
        let status_pos = (phase_start + 1..phase_end_adjusted)
            .find(|idx| lines[*idx].trim_start().starts_with("- Phase Status:"))
            .unwrap_or(task_line_idx + 1);
        lines.insert(status_pos + 1, impl_line);
    }

    let mut rebuilt = lines.join("\n");
    if original.ends_with('\n') {
        rebuilt.push('\n');
    }

    let changed = rebuilt != original;
    if changed {
        fs::write(rfc_path, rebuilt)
            .map_err(|e| format!("failed to write {}: {e}", normalize_path(rfc_path)))?;
    }

    let detail = if changed {
        "updated RFC phase status and implementation summary from task resolve".to_string()
    } else {
        "RFC phase already up to date".to_string()
    };

    Ok((changed, phase_title, detail))
}

fn validate_rfc_doc_path(path: &Path) -> Result<(), String> {
    let path = normalize_reference_path(path);
    if !path.exists() {
        return Err(format!("RFC doc not found: {}", normalize_path(&path)));
    }
    if !path.is_file() {
        return Err(format!("RFC doc is not a file: {}", normalize_path(&path)));
    }

    let parent = path
        .parent()
        .ok_or_else(|| format!("invalid RFC doc path: {}", normalize_path(&path)))?;
    if normalize_path(parent) != RFC_DIR {
        return Err(format!(
            "RFC doc must be under {}: {}",
            RFC_DIR,
            normalize_path(&path)
        ));
    }

    let name = path
        .file_name()
        .ok_or_else(|| format!("invalid RFC doc path: {}", normalize_path(&path)))?
        .to_string_lossy()
        .to_string();
    if parse_strict_four_digit_rfc_name(&name).is_none() {
        return Err(format!(
            "invalid RFC doc name: {} (expected <4digits>-<slug>.md)",
            normalize_path(&path)
        ));
    }
    Ok(())
}

fn extract_doc_refs_by_prefix(text: &str, prefix: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut pos = 0;
    while let Some(found) = text[pos..].find(prefix) {
        let start = pos + found;
        let mut end = text.len();
        for (idx, ch) in text[start..].char_indices() {
            if ch.is_whitespace() || matches!(ch, ')' | ']' | '>' | '"' | '\'' | ',') {
                end = start + idx;
                break;
            }
        }
        let mut cand = text[start..end].trim().trim_matches('`').to_string();
        while cand.ends_with('.') || cand.ends_with(':') || cand.ends_with(';') {
            cand.pop();
        }
        if cand.starts_with(prefix) && cand.ends_with(".md") {
            refs.push(cand);
        }
        pos = start + prefix.len();
        if pos >= text.len() {
            break;
        }
    }
    refs.sort();
    refs.dedup();
    refs
}

fn extract_markdown_section(content: &str, section: &str) -> Option<String> {
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

fn is_blank_implementation_notes(notes: &str) -> bool {
    let trimmed = notes.trim();
    if trimmed.is_empty() {
        return true;
    }
    trimmed
        .to_ascii_lowercase()
        .contains("keep this section blank in design phase")
}

fn first_meaningful_line(text: &str) -> Option<String> {
    for line in text.lines() {
        let trimmed = line
            .trim()
            .trim_start_matches('-')
            .trim_start_matches('*')
            .trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with("```") || trimmed.starts_with('#') {
            continue;
        }
        return Some(trimmed.to_string());
    }
    None
}

fn collapse_inline(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

struct BacklogRefCandidates {
    id: Option<String>,
    open_candidates: Vec<String>,
    closed_candidates: Vec<String>,
}

fn backlog_ref_candidates(normalized: &str) -> BacklogRefCandidates {
    let mut open_candidates = vec![normalized.to_string()];
    let mut closed_candidates = Vec::new();

    if let Some(base) = normalized.strip_suffix(".todo.md") {
        open_candidates.push(format!("{base}.md"));
    }
    if let Some(base) = normalized.strip_suffix(".done.md") {
        let plain = format!("{base}.md");
        open_candidates.push(plain.clone());
        if let Some((_, tail)) = plain.split_once("docs/backlogs/") {
            closed_candidates.push(format!("{BACKLOG_CLOSED_DIR}/{tail}"));
        }
    }

    if normalized.starts_with("docs/backlogs/") && !normalized.starts_with(&format!("{BACKLOG_CLOSED_DIR}/")) {
        if let Some((_, tail)) = normalized.split_once("docs/backlogs/") {
            closed_candidates.push(format!("{BACKLOG_CLOSED_DIR}/{tail}"));
        }
    }

    open_candidates.sort();
    open_candidates.dedup();
    closed_candidates.sort();
    closed_candidates.dedup();

    let id = extract_id_from_backlog_path(normalized);

    BacklogRefCandidates {
        id,
        open_candidates,
        closed_candidates,
    }
}

fn extract_id_from_backlog_path(path: &str) -> Option<String> {
    let name = Path::new(path)
        .file_name()?
        .to_string_lossy()
        .to_string();
    parse_backlog_name_any(&name).map(|(id, _, _)| format!("{id:06}"))
}

fn resolve_summary_json(summary: &ResolveSummary) -> String {
    format!(
        "{{\"task\":\"{}\",\"closed\":{},\"already_closed\":{},\"missing\":{},\"rfc_sync\":{}}}",
        json_escape(&summary.task),
        json_array(&summary.closed),
        json_array(&summary.already_closed),
        json_array(&summary.missing),
        rfc_sync_summary_json(&summary.rfc_sync)
    )
}

fn rfc_sync_summary_json(summary: &RfcSyncSummary) -> String {
    format!(
        "{{\"checked\":{},\"has_parent_rfc\":{},\"rfc_doc\":{},\"updated\":{},\"phase\":{},\"detail\":{}}}",
        if summary.checked { "true" } else { "false" },
        if summary.has_parent_rfc { "true" } else { "false" },
        json_nullable(&summary.rfc_doc),
        if summary.updated { "true" } else { "false" },
        json_nullable(&summary.phase),
        json_nullable(&summary.detail),
    )
}

fn json_array(items: &[String]) -> String {
    let body = items
        .iter()
        .map(|s| format!("\"{}\"", json_escape(s)))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{body}]")
}

fn json_nullable(value: &Option<String>) -> String {
    match value {
        Some(v) => format!("\"{}\"", json_escape(v)),
        None => "null".to_string(),
    }
}

fn json_escape(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn read_next_backlog_id(path: &Path) -> Result<String, String> {
    if !path.exists() {
        return Err(format!(
            "next-id file not found: {} (run init-backlog-next-id first)",
            normalize_path(path)
        ));
    }
    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(path)))?;
    let trimmed = content.trim();
    validate_fixed_id(trimmed)
}

fn detect_max_backlog_id(open_dir: &Path, closed_dir: &Path) -> Result<u32, String> {
    let mut max_id = 0_u32;
    for dir in [open_dir, closed_dir] {
        if !dir.exists() {
            continue;
        }
        if !dir.is_dir() {
            return Err(format!("not a directory: {}", normalize_path(dir)));
        }
        let entries =
            fs::read_dir(dir).map_err(|e| format!("failed to read {}: {e}", normalize_path(dir)))?;
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
            if name == "000000-template.md" {
                continue;
            }
            if let Some((id, _, _)) = parse_backlog_name_any(&name) {
                if id > max_id {
                    max_id = id;
                }
            }
        }
    }
    Ok(max_id)
}

fn find_backlog_by_id(dir: &Path, id: &str) -> Result<Vec<PathBuf>, String> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    let prefix = format!("{id}-");
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
        if !name.starts_with(&prefix) {
            continue;
        }
        if parse_backlog_name_new(&name).is_some() {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

fn parse_positive_width(raw: &str) -> Result<usize, String> {
    let parsed = raw
        .parse::<i64>()
        .map_err(|_| "width must be a positive integer".to_string())?;
    if parsed <= 0 {
        return Err("width must be a positive integer".to_string());
    }
    usize::try_from(parsed).map_err(|_| "width must be a positive integer".to_string())
}

fn detect_next_task_id(task_dir: &Path, width: usize) -> Result<String, String> {
    if width == 0 {
        return Err("width must be a positive integer".to_string());
    }
    if !task_dir.exists() {
        return Err(format!(
            "task directory not found: {}",
            normalize_path(task_dir)
        ));
    }
    if !task_dir.is_dir() {
        return Err(format!("not a directory: {}", normalize_path(task_dir)));
    }

    let mut max_id: u64 = 0;
    let entries = fs::read_dir(task_dir)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(task_dir)))?;
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
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(v) = parse_any_digit_task_file_id(&name) {
            if v > max_id {
                max_id = v;
            }
        }
    }

    let next_id = max_id + 1;
    let text = next_id.to_string();
    if text.len() > width {
        return Err(format!("next id {next_id} does not fit width {width}"));
    }
    Ok(format!("{next_id:0width$}"))
}

fn parse_any_digit_task_file_id(name: &str) -> Option<u64> {
    if !name.ends_with(".md") {
        return None;
    }
    let body = &name[..name.len() - 3];
    let (left, right) = body.split_once('-')?;
    if left.is_empty() || right.is_empty() || right.contains('/') {
        return None;
    }
    if !left.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    left.parse::<u64>().ok()
}

fn validate_slug(slug: &str) -> Result<String, String> {
    if is_valid_slug(slug) {
        Ok(slug.to_string())
    } else {
        Err("slug must be kebab-case: [a-z0-9]+(?:-[a-z0-9]+)*".to_string())
    }
}

fn is_valid_slug(slug: &str) -> bool {
    if slug.is_empty() {
        return false;
    }
    for part in slug.split('-') {
        if part.is_empty() {
            return false;
        }
        if !part
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return false;
        }
    }
    true
}

fn validate_fixed_id(task_id: &str) -> Result<String, String> {
    if task_id.len() == 6 && task_id.bytes().all(|b| b.is_ascii_digit()) {
        Ok(task_id.to_string())
    } else {
        Err("id must be exactly 6 digits".to_string())
    }
}

fn detect_next_id_from_output_dir(output_dir: &Path) -> Result<String, String> {
    if !output_dir.exists() {
        return Err(format!(
            "output directory not found: {}",
            normalize_path(output_dir)
        ));
    }
    if !output_dir.is_dir() {
        return Err(format!("not a directory: {}", normalize_path(output_dir)));
    }

    let mut max_value: u32 = 0;
    let entries = fs::read_dir(output_dir)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(output_dir)))?;
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
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(value) = parse_strict_six_digit_task_name(&name) {
            if value > max_value {
                max_value = value;
            }
        }
    }

    Ok(format!("{:06}", max_value + 1))
}

fn parse_strict_six_digit_task_name(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let body = &name[..name.len() - 3];
    let (id, slug) = body.split_once('-')?;
    if id.len() != 6 || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !is_valid_slug(slug) {
        return None;
    }
    id.parse::<u32>().ok()
}

fn parse_strict_four_digit_rfc_name(name: &str) -> Option<u32> {
    if !name.ends_with(".md") {
        return None;
    }
    let body = &name[..name.len() - 3];
    let (id, slug) = body.split_once('-')?;
    if id.len() != 4 || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !is_valid_slug(slug) {
        return None;
    }
    id.parse::<u32>().ok()
}

fn parse_backlog_name_new(name: &str) -> Option<(u32, &str)> {
    if !name.ends_with(".md") {
        return None;
    }
    let body = &name[..name.len() - 3];
    let (id, slug) = body.split_once('-')?;
    if id.len() != 6 || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !is_valid_slug(slug) {
        return None;
    }
    Some((id.parse::<u32>().ok()?, slug))
}

fn parse_backlog_name_any(name: &str) -> Option<(u32, &str, Option<&str>)> {
    if let Some((id, slug)) = parse_backlog_name_new(name) {
        return Some((id, slug, None));
    }

    if !name.ends_with(".md") {
        return None;
    }
    let body = &name[..name.len() - 3];
    let (left, status) = body.rsplit_once('.')?;
    if status != "todo" && status != "done" {
        return None;
    }
    let (id, slug) = left.split_once('-')?;
    if id.len() != 6 || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !is_valid_slug(slug) {
        return None;
    }
    Some((id.parse::<u32>().ok()?, slug, Some(status)))
}

fn load_template(template_path: &Path) -> Result<String, String> {
    if !template_path.exists() {
        return Err(format!(
            "template not found: {}",
            normalize_path(template_path)
        ));
    }
    if !template_path.is_file() {
        return Err(format!(
            "template is not a file: {}",
            normalize_path(template_path)
        ));
    }
    fs::read_to_string(template_path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(template_path)))
}

fn apply_title(template_text: String, title: &str) -> Result<String, String> {
    let clean_title = title.trim();
    if clean_title.is_empty() {
        return Err("title must not be empty".to_string());
    }

    let heading = format!("# Task: {clean_title}");
    let trailing_newline = template_text.ends_with('\n');

    let mut lines: Vec<String> = template_text.lines().map(|s| s.to_string()).collect();
    if !lines.is_empty() && lines[0].starts_with("# Task:") {
        lines[0] = heading;
        let mut out = lines.join("\n");
        if trailing_newline {
            out.push('\n');
        }
        return Ok(out);
    }

    Ok(format!("{heading}\n\n{template_text}"))
}

fn ensure_parent_dir_exists(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", normalize_path(parent)))?;
        }
    }
    Ok(())
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

fn today_yyyy_mm_dd() -> String {
    let out = Command::new("date").arg("+%F").output();
    if let Ok(o) = out {
        if o.status.success() {
            let text = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if text.len() == 10 {
                return text;
            }
        }
    }
    "1970-01-01".to_string()
}

fn normalize_reference_path(path: &Path) -> PathBuf {
    let raw = normalize_path(path);
    if let Some(base) = raw.strip_suffix(".todo.md") {
        return PathBuf::from(format!("{base}.md"));
    }
    if let Some(base) = raw.strip_suffix(".done.md") {
        return PathBuf::from(format!("{base}.md"));
    }
    path.to_path_buf()
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
