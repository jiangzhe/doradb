#!/usr/bin/env -S cargo +nightly -q -Zscript
---
[package]
edition = "2024"

[dependencies]
serde_json = "1"
---

use serde_json::Value;
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const BACKLOG_DIR: &str = "docs/backlogs";
const BACKLOG_CLOSED_DIR: &str = "docs/backlogs/closed";
const BACKLOG_NEXT_ID_FILE: &str = "docs/backlogs/next-id";
const BACKLOG_TEMPLATE_FILE: &str = "docs/backlogs/000000-template.md";
const DOC_ID_TOOL: &str = "tools/doc-id.rs";

fn usage() -> &'static str {
    "Usage: tools/backlog.rs <subcommand> [options]\n\n\
Subcommands:\n\
  init-next-id      Initialize docs/backlogs/next-id (single 6-digit id)\n\
  alloc-id          Allocate and consume next backlog id from docs/backlogs/next-id\n\
  create-doc        Create a docs/backlogs backlog doc from template\n\
  find-duplicates   Find duplicate candidates in open backlog docs only\n\
  close-doc         Move an open backlog doc to docs/backlogs/closed with Close Reason\n"
}

fn init_next_id_usage() -> &'static str {
    "Usage: tools/backlog.rs init-next-id [--path <docs/backlogs/next-id>] [--value <6digits>] [--force]"
}

fn alloc_id_usage() -> &'static str {
    "Usage: tools/backlog.rs alloc-id [--path <docs/backlogs/next-id>]"
}

fn create_doc_usage() -> &'static str {
    "Usage: tools/backlog.rs create-doc --title <title> --slug <slug> (--summary <text> | --summary-file <path>) (--reference <text> | --reference-file <path>) (--scope-hint <text> | --scope-hint-file <path>) (--acceptance-hint <text> | --acceptance-hint-file <path>) [--notes <text> | --notes-file <path>] [(--deferred-from <text> | --deferred-from-file <path>) (--defer-reason <text> | --defer-reason-file <path>) (--findings <text> | --findings-file <path>) (--direction-hint <text> | --direction-hint-file <path>)] (--id <6digits> | --auto-id) [--template <path>] [--output-dir <path>] [--next-id-path <path>] [--force]"
}

fn find_duplicates_usage() -> &'static str {
    "Usage: tools/backlog.rs find-duplicates --title <title> [--slug <slug>] [--dir <docs/backlogs>]"
}

fn close_doc_usage() -> &'static str {
    "Usage: tools/backlog.rs close-doc (--path <docs/backlogs/<id>-<slug>.md> | --id <6digits>) --type <type> (--detail <text> | --detail-file <path>) [--reference <text> | --reference-file <path>] [--date <YYYY-MM-DD>] [--force-reason-update]"
}

#[derive(Clone)]
struct CloseReason {
    reason_type: String,
    detail: String,
    closed_by: String,
    reference: String,
    closed_at: String,
}

#[derive(Clone)]
struct DuplicateCandidate {
    path: String,
    slug: String,
    title: String,
    reasons: Vec<String>,
    token_overlap: f64,
}

#[derive(Clone)]
struct DeferredContext {
    deferred_from: String,
    defer_reason: String,
    findings: String,
    direction_hint: String,
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
        "init-next-id" => run_init_next_id(args),
        "alloc-id" => run_alloc_id(args),
        "create-doc" => run_create_doc(args),
        "find-duplicates" => run_find_duplicates(args),
        "close-doc" => run_close_doc(args),
        _ => Err(format!("unknown subcommand: {subcommand}\n{}", usage())),
    }
}

fn run_init_next_id(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut path = PathBuf::from(BACKLOG_NEXT_ID_FILE);
    let mut value: Option<String> = None;
    let mut force = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --path\n{}",
                        init_next_id_usage()
                    ));
                };
                path = PathBuf::from(v);
            }
            "--value" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --value\n{}",
                        init_next_id_usage()
                    ));
                };
                value = Some(validate_fixed_id(&v)?);
            }
            "--force" => {
                force = true;
            }
            "-h" | "--help" => {
                println!("{}", init_next_id_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", init_next_id_usage())),
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
            let max_id =
                detect_max_backlog_id(Path::new(BACKLOG_DIR), Path::new(BACKLOG_CLOSED_DIR))?;
            format!("{:06}", max_id + 1)
        }
    };

    ensure_parent_dir_exists(&path)?;
    fs::write(&path, format!("{next}\n"))
        .map_err(|e| format!("failed to write {}: {e}", normalize_path(&path)))?;
    println!("{}", normalize_path(&path));
    Ok(())
}

fn run_alloc_id(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut path = PathBuf::from(BACKLOG_NEXT_ID_FILE);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --path\n{}", alloc_id_usage()));
                };
                path = PathBuf::from(v);
            }
            "-h" | "--help" => {
                println!("{}", alloc_id_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", alloc_id_usage())),
        }
    }

    let current = alloc_backlog_id(&path)?;
    println!("{current}");
    Ok(())
}

fn run_create_doc(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut title: Option<String> = None;
    let mut slug: Option<String> = None;
    let mut summary: Option<String> = None;
    let mut summary_file: Option<String> = None;
    let mut reference: Option<String> = None;
    let mut reference_file: Option<String> = None;
    let mut scope_hint: Option<String> = None;
    let mut scope_hint_file: Option<String> = None;
    let mut acceptance_hint: Option<String> = None;
    let mut acceptance_hint_file: Option<String> = None;
    let mut notes: Option<String> = None;
    let mut notes_file: Option<String> = None;
    let mut deferred_from: Option<String> = None;
    let mut deferred_from_file: Option<String> = None;
    let mut defer_reason: Option<String> = None;
    let mut defer_reason_file: Option<String> = None;
    let mut findings: Option<String> = None;
    let mut findings_file: Option<String> = None;
    let mut direction_hint: Option<String> = None;
    let mut direction_hint_file: Option<String> = None;
    let mut backlog_id: Option<String> = None;
    let mut auto_id = false;
    let mut template = PathBuf::from(BACKLOG_TEMPLATE_FILE);
    let mut output_dir = PathBuf::from(BACKLOG_DIR);
    let mut next_id_path = PathBuf::from(BACKLOG_NEXT_ID_FILE);
    let mut force = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--title" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --title\n{}", create_doc_usage()));
                };
                title = Some(v);
            }
            "--slug" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --slug\n{}", create_doc_usage()));
                };
                slug = Some(v);
            }
            "--summary" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --summary\n{}",
                        create_doc_usage()
                    ));
                };
                summary = Some(v);
            }
            "--summary-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --summary-file\n{}",
                        create_doc_usage()
                    ));
                };
                summary_file = Some(v);
            }
            "--reference" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --reference\n{}",
                        create_doc_usage()
                    ));
                };
                reference = Some(v);
            }
            "--reference-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --reference-file\n{}",
                        create_doc_usage()
                    ));
                };
                reference_file = Some(v);
            }
            "--scope-hint" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --scope-hint\n{}",
                        create_doc_usage()
                    ));
                };
                scope_hint = Some(v);
            }
            "--scope-hint-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --scope-hint-file\n{}",
                        create_doc_usage()
                    ));
                };
                scope_hint_file = Some(v);
            }
            "--acceptance-hint" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --acceptance-hint\n{}",
                        create_doc_usage()
                    ));
                };
                acceptance_hint = Some(v);
            }
            "--acceptance-hint-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --acceptance-hint-file\n{}",
                        create_doc_usage()
                    ));
                };
                acceptance_hint_file = Some(v);
            }
            "--notes" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --notes\n{}", create_doc_usage()));
                };
                notes = Some(v);
            }
            "--notes-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --notes-file\n{}",
                        create_doc_usage()
                    ));
                };
                notes_file = Some(v);
            }
            "--deferred-from" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --deferred-from\n{}",
                        create_doc_usage()
                    ));
                };
                deferred_from = Some(v);
            }
            "--deferred-from-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --deferred-from-file\n{}",
                        create_doc_usage()
                    ));
                };
                deferred_from_file = Some(v);
            }
            "--defer-reason" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --defer-reason\n{}",
                        create_doc_usage()
                    ));
                };
                defer_reason = Some(v);
            }
            "--defer-reason-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --defer-reason-file\n{}",
                        create_doc_usage()
                    ));
                };
                defer_reason_file = Some(v);
            }
            "--findings" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --findings\n{}",
                        create_doc_usage()
                    ));
                };
                findings = Some(v);
            }
            "--findings-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --findings-file\n{}",
                        create_doc_usage()
                    ));
                };
                findings_file = Some(v);
            }
            "--direction-hint" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --direction-hint\n{}",
                        create_doc_usage()
                    ));
                };
                direction_hint = Some(v);
            }
            "--direction-hint-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --direction-hint-file\n{}",
                        create_doc_usage()
                    ));
                };
                direction_hint_file = Some(v);
            }
            "--id" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --id\n{}", create_doc_usage()));
                };
                backlog_id = Some(validate_fixed_id(&v)?);
            }
            "--auto-id" => {
                auto_id = true;
            }
            "--template" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --template\n{}",
                        create_doc_usage()
                    ));
                };
                template = PathBuf::from(v);
            }
            "--output-dir" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --output-dir\n{}",
                        create_doc_usage()
                    ));
                };
                output_dir = PathBuf::from(v);
            }
            "--next-id-path" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --next-id-path\n{}",
                        create_doc_usage()
                    ));
                };
                next_id_path = PathBuf::from(v);
            }
            "--force" => {
                force = true;
            }
            "-h" | "--help" => {
                println!("{}", create_doc_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", create_doc_usage())),
        }
    }

    let title =
        title.ok_or_else(|| format!("missing required arg: --title\n{}", create_doc_usage()))?;
    let slug =
        slug.ok_or_else(|| format!("missing required arg: --slug\n{}", create_doc_usage()))?;
    let slug = validate_slug(&slug)?;
    let summary = resolve_required_text_arg(
        summary,
        summary_file,
        "--summary",
        "--summary-file",
        create_doc_usage(),
    )?;
    let reference = resolve_required_text_arg(
        reference,
        reference_file,
        "--reference",
        "--reference-file",
        create_doc_usage(),
    )?;
    let scope_hint = resolve_required_text_arg(
        scope_hint,
        scope_hint_file,
        "--scope-hint",
        "--scope-hint-file",
        create_doc_usage(),
    )?;
    let acceptance_hint = resolve_required_text_arg(
        acceptance_hint,
        acceptance_hint_file,
        "--acceptance-hint",
        "--acceptance-hint-file",
        create_doc_usage(),
    )?;
    let notes = resolve_optional_text_arg(
        notes,
        notes_file,
        "--notes",
        "--notes-file",
        create_doc_usage(),
    )?;
    let deferred_from = resolve_optional_text_arg(
        deferred_from,
        deferred_from_file,
        "--deferred-from",
        "--deferred-from-file",
        create_doc_usage(),
    )?;
    let defer_reason = resolve_optional_text_arg(
        defer_reason,
        defer_reason_file,
        "--defer-reason",
        "--defer-reason-file",
        create_doc_usage(),
    )?;
    let findings = resolve_optional_text_arg(
        findings,
        findings_file,
        "--findings",
        "--findings-file",
        create_doc_usage(),
    )?;
    let direction_hint = resolve_optional_text_arg(
        direction_hint,
        direction_hint_file,
        "--direction-hint",
        "--direction-hint-file",
        create_doc_usage(),
    )?;
    let deferred_context = parse_deferred_context(
        deferred_from,
        defer_reason,
        findings,
        direction_hint,
    )?;

    if auto_id && backlog_id.is_some() {
        return Err("use either --id or --auto-id, not both".to_string());
    }
    if !auto_id && backlog_id.is_none() {
        return Err(format!(
            "one of --id or --auto-id is required\n{}",
            create_doc_usage()
        ));
    }

    let backlog_id = if auto_id {
        alloc_backlog_id(&next_id_path)?
    } else {
        backlog_id.expect("checked is_some")
    };

    let template_text = load_template(&template)?;
    let content = render_backlog_doc(
        template_text,
        &title,
        &summary,
        &reference,
        &scope_hint,
        &acceptance_hint,
        notes.as_deref(),
        deferred_context.as_ref(),
    )?;

    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)
            .map_err(|e| format!("failed to create {}: {e}", normalize_path(&output_dir)))?;
    }
    if !output_dir.is_dir() {
        return Err(format!("not a directory: {}", normalize_path(&output_dir)));
    }

    let out_path = output_dir.join(format!("{backlog_id}-{slug}.md"));
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

fn run_find_duplicates(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut title: Option<String> = None;
    let mut slug: Option<String> = None;
    let mut dir = PathBuf::from(BACKLOG_DIR);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--title" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --title\n{}",
                        find_duplicates_usage()
                    ));
                };
                title = Some(v);
            }
            "--slug" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --slug\n{}",
                        find_duplicates_usage()
                    ));
                };
                slug = Some(validate_slug(&v)?);
            }
            "--dir" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --dir\n{}",
                        find_duplicates_usage()
                    ));
                };
                dir = PathBuf::from(v);
            }
            "-h" | "--help" => {
                println!("{}", find_duplicates_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", find_duplicates_usage())),
        }
    }

    let title = title
        .ok_or_else(|| format!("missing required arg: --title\n{}", find_duplicates_usage()))?;

    let entries = collect_open_backlog_docs(&dir)?;
    let query_tokens = tokenize(&title);
    let query_title_norm = normalize_text(&title);
    let mut duplicates = Vec::new();

    for path in &entries {
        let file_name = path
            .file_name()
            .ok_or_else(|| format!("invalid file path: {}", normalize_path(path)))?
            .to_string_lossy()
            .to_string();

        let Some((_, cand_slug)) = parse_backlog_name_new(&file_name) else {
            continue;
        };

        let text = fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {e}", normalize_path(path)))?;
        let cand_title = extract_backlog_title(&text).unwrap_or_default();
        let cand_summary = extract_section_text(&text, "Summary").unwrap_or_default();
        let cand_tokens = tokenize(&format!("{cand_title} {cand_summary}"));
        let (token_overlap, common_count) = token_overlap_score(&query_tokens, &cand_tokens);

        let mut reasons = Vec::new();
        if let Some(s) = slug.as_deref() {
            if s == cand_slug {
                reasons.push("same-slug".to_string());
            }
        }
        if !query_title_norm.is_empty() && query_title_norm == normalize_text(&cand_title) {
            reasons.push("same-title".to_string());
        }
        if token_overlap >= 0.60 || common_count >= 3 {
            reasons.push("title-summary-token-overlap".to_string());
        }

        if !reasons.is_empty() {
            duplicates.push(DuplicateCandidate {
                path: normalize_path(path),
                slug: cand_slug.to_string(),
                title: cand_title,
                reasons,
                token_overlap,
            });
        }
    }

    duplicates.sort_by(|a, b| a.path.cmp(&b.path));
    println!(
        "{}",
        render_duplicates_json(&title, slug.as_deref(), entries.len(), &duplicates)
    );
    Ok(())
}

fn run_close_doc(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut path: Option<PathBuf> = None;
    let mut backlog_id: Option<String> = None;
    let mut reason_type: Option<String> = None;
    let mut detail: Option<String> = None;
    let mut detail_file: Option<String> = None;
    let mut reference: Option<String> = None;
    let mut reference_file: Option<String> = None;
    let mut date: Option<String> = None;
    let mut force_reason_update = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --path\n{}", close_doc_usage()));
                };
                path = Some(PathBuf::from(v));
            }
            "--id" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --id\n{}", close_doc_usage()));
                };
                backlog_id = Some(validate_fixed_id(&v)?);
            }
            "--type" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --type\n{}", close_doc_usage()));
                };
                reason_type = Some(v);
            }
            "--detail" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --detail\n{}", close_doc_usage()));
                };
                detail = Some(v);
            }
            "--detail-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --detail-file\n{}",
                        close_doc_usage()
                    ));
                };
                detail_file = Some(v);
            }
            "--reference" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --reference\n{}",
                        close_doc_usage()
                    ));
                };
                reference = Some(v);
            }
            "--reference-file" => {
                let Some(v) = args.next() else {
                    return Err(format!(
                        "missing value for --reference-file\n{}",
                        close_doc_usage()
                    ));
                };
                reference_file = Some(v);
            }
            "--date" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --date\n{}", close_doc_usage()));
                };
                date = Some(v);
            }
            "--force-reason-update" => {
                force_reason_update = true;
            }
            "-h" | "--help" => {
                println!("{}", close_doc_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", close_doc_usage())),
        }
    }

    let open_path = resolve_open_backlog_path(path, backlog_id, close_doc_usage())?;
    let detail = resolve_required_text_arg(
        detail,
        detail_file,
        "--detail",
        "--detail-file",
        close_doc_usage(),
    )?;
    let reference = resolve_optional_text_arg(
        reference,
        reference_file,
        "--reference",
        "--reference-file",
        close_doc_usage(),
    )?;
    let reason = CloseReason {
        reason_type: reason_type
            .ok_or_else(|| format!("missing required arg: --type\n{}", close_doc_usage()))?,
        detail,
        closed_by: "backlog close".to_string(),
        reference: reference.unwrap_or_else(|| "User decision".to_string()),
        closed_at: date.unwrap_or_else(today_yyyy_mm_dd),
    };

    let closed_path = archive_backlog_with_reason(&open_path, &reason, force_reason_update)?;
    println!("{}", normalize_path(&closed_path));
    Ok(())
}

fn read_text_file(path_text: &str, file_flag: &str) -> Result<String, String> {
    let path = Path::new(path_text);
    if !path.exists() {
        return Err(format!("{file_flag} path not found: {path_text}"));
    }
    if !path.is_file() {
        return Err(format!("{file_flag} is not a file: {}", normalize_path(path)));
    }
    fs::read_to_string(path)
        .map_err(|e| format!("failed to read {} for {file_flag}: {e}", normalize_path(path)))
}

fn resolve_optional_text_arg(
    value: Option<String>,
    file: Option<String>,
    flag: &str,
    file_flag: &str,
    usage: &str,
) -> Result<Option<String>, String> {
    if value.is_some() && file.is_some() {
        return Err(format!(
            "use either {flag} or {file_flag}, not both\n{usage}"
        ));
    }
    match (value, file) {
        (Some(v), None) => Ok(Some(v)),
        (None, Some(path)) => Ok(Some(read_text_file(&path, file_flag)?)),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => unreachable!("checked above"),
    }
}

fn resolve_required_text_arg(
    value: Option<String>,
    file: Option<String>,
    flag: &str,
    file_flag: &str,
    usage: &str,
) -> Result<String, String> {
    resolve_optional_text_arg(value, file, flag, file_flag, usage)?
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing required arg: {flag} or {file_flag}\n{usage}"))
}

fn alloc_backlog_id(path: &Path) -> Result<String, String> {
    let current = read_next_backlog_id(path)?;
    let current_num = current
        .parse::<u32>()
        .map_err(|_| format!("invalid next-id content in {}", normalize_path(path)))?;
    if current_num >= 999_999 {
        return Err(format!("next-id overflow in {}", normalize_path(path)));
    }
    let next_num = current_num + 1;
    fs::write(path, format!("{:06}\n", next_num))
        .map_err(|e| format!("failed to update {}: {e}", normalize_path(path)))?;
    Ok(current)
}

fn render_backlog_doc(
    template: String,
    title: &str,
    summary: &str,
    reference: &str,
    scope_hint: &str,
    acceptance_hint: &str,
    notes: Option<&str>,
    deferred_context: Option<&DeferredContext>,
) -> Result<String, String> {
    let mut text = apply_backlog_title(template, title)?;
    text = replace_section_body(&text, "Summary", summary)?;
    text = replace_section_body(&text, "Reference", reference)?;
    let deferred_from = deferred_context
        .map(|ctx| ctx.deferred_from.as_str())
        .unwrap_or("");
    text = replace_optional_section_body(&text, "Deferred From (Optional)", deferred_from)?;
    let deferral_context = deferred_context
        .map(render_deferral_context)
        .unwrap_or_default();
    text = replace_optional_section_body(
        &text,
        "Deferral Context (Optional)",
        &deferral_context,
    )?;
    text = replace_section_body(&text, "Scope Hint", scope_hint)?;
    text = replace_section_body(&text, "Acceptance Hint", acceptance_hint)?;
    text = replace_section_body(&text, "Notes (Optional)", notes.unwrap_or(""))?;
    if !text.ends_with('\n') {
        text.push('\n');
    }
    Ok(text)
}

fn apply_backlog_title(template_text: String, title: &str) -> Result<String, String> {
    let clean_title = title.trim();
    if clean_title.is_empty() {
        return Err("title must not be empty".to_string());
    }

    let heading = format!("# Backlog: {clean_title}");
    let trailing_newline = template_text.ends_with('\n');
    let mut lines: Vec<String> = template_text.lines().map(|s| s.to_string()).collect();
    if !lines.is_empty() && lines[0].starts_with("# Backlog:") {
        lines[0] = heading;
        let mut out = lines.join("\n");
        if trailing_newline {
            out.push('\n');
        }
        return Ok(out);
    }
    Ok(format!("{heading}\n\n{template_text}"))
}

fn replace_section_body(content: &str, section: &str, body: &str) -> Result<String, String> {
    let header = format!("## {section}");
    let lines: Vec<&str> = content.lines().collect();
    let Some(start_idx) = lines.iter().position(|line| line.trim() == header) else {
        return Err(format!("missing section header: {header}"));
    };

    let end_idx = lines
        .iter()
        .enumerate()
        .skip(start_idx + 1)
        .find(|(_, line)| line.starts_with("## "))
        .map(|(idx, _)| idx)
        .unwrap_or(lines.len());

    let mut out = Vec::new();
    out.extend(lines[..=start_idx].iter().map(|s| (*s).to_string()));
    out.push(String::new());
    let trimmed = body.trim();
    if !trimmed.is_empty() {
        out.extend(trimmed.lines().map(|s| s.to_string()));
    }
    out.push(String::new());
    out.extend(lines[end_idx..].iter().map(|s| (*s).to_string()));

    let mut rebuilt = out.join("\n");
    if content.ends_with('\n') {
        rebuilt.push('\n');
    }
    Ok(rebuilt)
}

fn replace_optional_section_body(content: &str, section: &str, body: &str) -> Result<String, String> {
    if extract_section_text(content, section).is_none() {
        if body.trim().is_empty() {
            return Ok(content.to_string());
        }
        return Err(format!("missing section header: ## {section}"));
    }
    replace_section_body(content, section, body)
}

fn collect_open_backlog_docs(dir: &Path) -> Result<Vec<PathBuf>, String> {
    if !dir.exists() {
        return Err(format!("directory not found: {}", normalize_path(dir)));
    }
    if !dir.is_dir() {
        return Err(format!("not a directory: {}", normalize_path(dir)));
    }

    let mut out = Vec::new();
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
        if parse_backlog_name_new(&name).is_none() {
            continue;
        }
        out.push(entry.path());
    }
    out.sort();
    Ok(out)
}

fn extract_backlog_title(text: &str) -> Option<String> {
    let line = text.lines().next()?.trim();
    line.strip_prefix("# Backlog:")
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn extract_section_text(text: &str, section: &str) -> Option<String> {
    let header = format!("## {section}");
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.iter().position(|line| line.trim() == header)?;
    let end = lines
        .iter()
        .enumerate()
        .skip(start + 1)
        .find(|(_, line)| line.starts_with("## "))
        .map(|(idx, _)| idx)
        .unwrap_or(lines.len());
    let body = lines[start + 1..end].join("\n");
    Some(body.trim().to_string())
}

fn parse_deferred_context(
    deferred_from: Option<String>,
    defer_reason: Option<String>,
    findings: Option<String>,
    direction_hint: Option<String>,
) -> Result<Option<DeferredContext>, String> {
    let invalid = || {
        format!(
            "deferred-work context requires all of --deferred-from/--deferred-from-file, --defer-reason/--defer-reason-file, --findings/--findings-file, and --direction-hint/--direction-hint-file together\n{}",
            create_doc_usage()
        )
    };
    let fields_present = [
        deferred_from.is_some(),
        defer_reason.is_some(),
        findings.is_some(),
        direction_hint.is_some(),
    ]
    .into_iter()
    .any(|v| v);
    let provided = [
        deferred_from
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false),
        defer_reason
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false),
        findings
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false),
        direction_hint
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false),
    ]
    .into_iter()
    .filter(|v| *v)
    .count();
    if provided == 0 && !fields_present {
        return Ok(None);
    }
    if provided != 4 {
        return Err(invalid());
    }
    Ok(Some(DeferredContext {
        deferred_from: deferred_from.expect("checked count"),
        defer_reason: defer_reason.expect("checked count"),
        findings: findings.expect("checked count"),
        direction_hint: direction_hint.expect("checked count"),
    }))
}

fn render_deferral_context(ctx: &DeferredContext) -> String {
    [
        render_named_bullet("Defer Reason", &ctx.defer_reason),
        render_named_bullet("Findings", &ctx.findings),
        render_named_bullet("Direction Hint", &ctx.direction_hint),
    ]
    .join("\n")
}

fn render_named_bullet(label: &str, value: &str) -> String {
    let trimmed = value.trim();
    if !trimmed.contains('\n') {
        return format!("- {label}: {trimmed}");
    }
    let indented = trimmed
        .lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("- {label}:\n{indented}")
}

fn tokenize(text: &str) -> Vec<String> {
    let normalized = normalize_text(text);
    let mut set = BTreeSet::new();
    for token in normalized.split_whitespace() {
        if token.len() >= 3 {
            set.insert(token.to_string());
        }
    }
    set.into_iter().collect()
}

fn normalize_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(' ');
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn token_overlap_score(lhs: &[String], rhs: &[String]) -> (f64, usize) {
    if lhs.is_empty() || rhs.is_empty() {
        return (0.0, 0);
    }
    let left: BTreeSet<&str> = lhs.iter().map(String::as_str).collect();
    let right: BTreeSet<&str> = rhs.iter().map(String::as_str).collect();
    let common = left.intersection(&right).count();
    let union = left.union(&right).count();
    if union == 0 {
        return (0.0, common);
    }
    (common as f64 / union as f64, common)
}

fn render_duplicates_json(
    query_title: &str,
    query_slug: Option<&str>,
    scanned_open_docs: usize,
    duplicates: &[DuplicateCandidate],
) -> String {
    let dup_body = duplicates
        .iter()
        .map(|d| {
            let reasons = d
                .reasons
                .iter()
                .map(|r| format!("\"{}\"", json_escape(r)))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "{{\"path\":\"{}\",\"slug\":\"{}\",\"title\":\"{}\",\"reasons\":[{}],\"token_overlap\":{:.6}}}",
                json_escape(&d.path),
                json_escape(&d.slug),
                json_escape(&d.title),
                reasons,
                d.token_overlap
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "{{\"query_title\":\"{}\",\"query_slug\":{},\"scanned_open_docs\":{},\"duplicates\":[{}]}}",
        json_escape(query_title),
        query_slug
            .map(|s| format!("\"{}\"", json_escape(s)))
            .unwrap_or_else(|| "null".to_string()),
        scanned_open_docs,
        dup_body
    )
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
    let out = Command::new(DOC_ID_TOOL)
        .args([
            "search-by-id",
            "--kind",
            "backlog",
            "--id",
            id,
            "--scope",
            "open",
        ])
        .output()
        .map_err(|e| format!("failed to execute {DOC_ID_TOOL}: {e}"))?;

    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    let payload: Value = serde_json::from_str(&stdout).map_err(|e| {
        format!(
            "failed to parse {DOC_ID_TOOL} output: {e}; stdout={stdout}; stderr={stderr}"
        )
    })?;

    if !out.status.success() {
        let error = payload
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                if !stderr.is_empty() {
                    stderr.as_str()
                } else {
                    "backlog id lookup failed"
                }
            })
            .to_string();
        return Err(error);
    }

    let path = payload
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("{DOC_ID_TOOL} output missing path field"))?;
    let resolved = PathBuf::from(path);
    ensure_open_backlog_path(&resolved)?;
    Ok(resolved)
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
    let existing_idx = find_close_reason_section_start(content);

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

fn find_close_reason_section_start(content: &str) -> Option<usize> {
    let mut offset = 0usize;
    let mut in_fence = false;
    for line in content.split_inclusive('\n') {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_fence = !in_fence;
        } else if !in_fence && trimmed == "## Close Reason" {
            return Some(offset);
        }
        offset += line.len();
    }

    // Handle final line without trailing newline.
    if !content.ends_with('\n') {
        let line = content.lines().last().unwrap_or_default().trim();
        if !in_fence && line == "## Close Reason" {
            return Some(content.len() - content.lines().last().unwrap_or_default().len());
        }
    }
    None
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

fn read_next_backlog_id(path: &Path) -> Result<String, String> {
    if !path.exists() {
        return Err(format!(
            "next-id file not found: {} (run tools/backlog.rs init-next-id first)",
            normalize_path(path)
        ));
    }
    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(path)))?;
    validate_fixed_id(content.trim())
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
        let entries = fs::read_dir(dir)
            .map_err(|e| format!("failed to read {}: {e}", normalize_path(dir)))?;
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

fn validate_fixed_id(backlog_id: &str) -> Result<String, String> {
    if backlog_id.len() == 6 && backlog_id.bytes().all(|b| b.is_ascii_digit()) {
        Ok(backlog_id.to_string())
    } else {
        Err("id must be exactly 6 digits".to_string())
    }
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

fn ensure_parent_dir_exists(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", normalize_path(parent)))?;
        }
    }
    Ok(())
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

fn json_escape(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        env::temp_dir().join(format!("backlog-rs-{name}-{nanos}.txt"))
    }

    fn deferred_context_usage_error() -> String {
        format!(
            "deferred-work context requires all of --deferred-from/--deferred-from-file, --defer-reason/--defer-reason-file, --findings/--findings-file, and --direction-hint/--direction-hint-file together\n{}",
            create_doc_usage()
        )
    }

    #[test]
    fn resolve_required_text_arg_rejects_blank_inline_value() {
        let err = resolve_required_text_arg(
            Some("  \n\t  ".to_string()),
            None,
            "--summary",
            "--summary-file",
            "usage",
        )
        .expect_err("blank inline value should be rejected");

        assert_eq!(err, "missing required arg: --summary or --summary-file\nusage");
    }

    #[test]
    fn resolve_required_text_arg_rejects_blank_file_value() {
        let path = temp_file_path("blank");
        fs::write(&path, "  \n\n").expect("write temp file");

        let err = resolve_required_text_arg(
            None,
            Some(normalize_path(&path)),
            "--detail",
            "--detail-file",
            "usage",
        )
        .expect_err("blank file value should be rejected");

        let _ = fs::remove_file(&path);
        assert_eq!(err, "missing required arg: --detail or --detail-file\nusage");
    }

    #[test]
    fn resolve_required_text_arg_trims_non_empty_inline_value() {
        let value = resolve_required_text_arg(
            Some("  keep this text  \n".to_string()),
            None,
            "--summary",
            "--summary-file",
            "usage",
        )
        .expect("non-empty inline value should succeed");

        assert_eq!(value, "keep this text");
    }

    #[test]
    fn resolve_required_text_arg_trims_non_empty_file_value() {
        let path = temp_file_path("trimmed");
        fs::write(&path, "  preserved text  \n").expect("write temp file");

        let value = resolve_required_text_arg(
            None,
            Some(normalize_path(&path)),
            "--detail",
            "--detail-file",
            "usage",
        )
        .expect("non-empty file value should succeed");

        let _ = fs::remove_file(&path);
        assert_eq!(value, "preserved text");
    }

    #[test]
    fn parse_deferred_context_returns_none_when_all_fields_absent() {
        let value = parse_deferred_context(None, None, None, None)
            .expect("all absent deferred context should be allowed");

        assert!(value.is_none());
    }

    #[test]
    fn parse_deferred_context_rejects_blank_field_when_others_are_present() {
        let err = parse_deferred_context(
            Some("   \n".to_string()),
            Some("valid reason".to_string()),
            Some("valid findings".to_string()),
            Some("valid direction".to_string()),
        )
        .err()
        .expect("blank deferred field should be rejected");

        assert_eq!(err, deferred_context_usage_error());
    }

    #[test]
    fn parse_deferred_context_rejects_partial_non_empty_context() {
        let err = parse_deferred_context(
            Some("docs/tasks/000001-demo.md".to_string()),
            Some("valid reason".to_string()),
            None,
            None,
        )
        .err()
        .expect("partial deferred context should be rejected");

        assert_eq!(err, deferred_context_usage_error());
    }

    #[test]
    fn parse_deferred_context_preserves_original_non_empty_values() {
        let value = parse_deferred_context(
            Some("  docs/tasks/000001-demo.md  ".to_string()),
            Some("  valid reason  ".to_string()),
            Some("  valid findings  ".to_string()),
            Some("  valid direction  ".to_string()),
        )
        .expect("non-empty deferred context should succeed")
        .expect("deferred context should be present");

        assert_eq!(value.deferred_from, "  docs/tasks/000001-demo.md  ");
        assert_eq!(value.defer_reason, "  valid reason  ");
        assert_eq!(value.findings, "  valid findings  ");
        assert_eq!(value.direction_hint, "  valid direction  ");
    }

    #[test]
    fn parse_deferred_context_rejects_all_blank_values() {
        let err = parse_deferred_context(
            Some("  ".to_string()),
            Some("\n".to_string()),
            Some("\t".to_string()),
            Some("   ".to_string()),
        )
        .err()
        .expect("all blank deferred values should be rejected");

        assert_eq!(err, deferred_context_usage_error());
    }
}
