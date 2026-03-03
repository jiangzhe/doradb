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

const TASK_DIR: &str = "docs/tasks";
const TASK_NEXT_ID_FILE: &str = "docs/tasks/next-id";
const TASK_TEMPLATE_FILE: &str = "000000-template.md";

const BACKLOG_DIR: &str = "docs/backlogs";
const BACKLOG_CLOSED_DIR: &str = "docs/backlogs/closed";
const BACKLOG_NEXT_ID_FILE: &str = "docs/backlogs/next-id";
const BACKLOG_TEMPLATE_FILE: &str = "000000-template.md";

const RFC_DIR: &str = "docs/rfcs";
const RFC_NEXT_ID_FILE: &str = "docs/rfcs/next-id";
const RFC_TEMPLATE_FILE: &str = "0000-template.md";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Task,
    Backlog,
    Rfc,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Open,
    Closed,
    All,
}

#[derive(Clone)]
struct SearchMatch {
    path: String,
    state: String,
}

#[derive(Clone)]
struct DocInfo {
    kind: Kind,
    id: String,
    slug: String,
    path: String,
    state: String,
}

fn usage() -> &'static str {
    "Usage: tools/doc-id.rs <subcommand> [options]\n\n\
Subcommands:\n\
  init-next-id    Initialize next-id file for a doc kind\n\
  peek-next-id    Print current next id for a doc kind\n\
  alloc-id        Allocate and consume next id for a doc kind\n\
  validate-path   Validate a planning/backlog doc path\n\
  inspect-path    Inspect a planning/backlog doc path and print metadata\n\
  search-by-id    Resolve exactly one document path from kind + id\n"
}

fn init_next_id_usage() -> &'static str {
    "Usage: tools/doc-id.rs init-next-id --kind <task|backlog|rfc> [--value <id>] [--force]"
}

fn peek_next_id_usage() -> &'static str {
    "Usage: tools/doc-id.rs peek-next-id --kind <task|backlog|rfc>"
}

fn alloc_id_usage() -> &'static str {
    "Usage: tools/doc-id.rs alloc-id --kind <task|backlog|rfc>"
}

fn validate_path_usage() -> &'static str {
    "Usage: tools/doc-id.rs validate-path --path <doc-path>"
}

fn inspect_path_usage() -> &'static str {
    "Usage: tools/doc-id.rs inspect-path --path <doc-path>"
}

fn search_by_id_usage() -> &'static str {
    "Usage: tools/doc-id.rs search-by-id --kind <task|backlog|rfc> --id <digits> [--scope <open|closed|all>]"
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
        "init-next-id" => cmd_init_next_id(args),
        "peek-next-id" => cmd_peek_next_id(args),
        "alloc-id" => cmd_alloc_id(args),
        "validate-path" => cmd_validate_path(args),
        "inspect-path" => cmd_inspect_path(args),
        "search-by-id" => cmd_search_by_id(args),
        _ => {
            eprintln!("unknown subcommand: {subcommand}\n{}", usage());
            Err(1)
        }
    }
}

fn cmd_init_next_id(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut kind: Option<Kind> = None;
    let mut value: Option<String> = None;
    let mut force = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--kind" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --kind\n{}", init_next_id_usage());
                    return Err(1);
                };
                kind = parse_kind(&v);
                if kind.is_none() {
                    eprintln!("invalid --kind value: {v}");
                    return Err(1);
                }
            }
            "--value" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --value\n{}", init_next_id_usage());
                    return Err(1);
                };
                value = Some(v);
            }
            "--force" => {
                force = true;
            }
            "-h" | "--help" => {
                println!("{}", init_next_id_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", init_next_id_usage());
                return Err(1);
            }
        }
    }

    let Some(kind) = kind else {
        eprintln!("missing required arg: --kind\n{}", init_next_id_usage());
        return Err(1);
    };

    let path = next_id_path(kind);
    let width = id_width(kind);

    if path.exists() && !force {
        eprintln!(
            "{} already exists (use --force to overwrite)",
            normalize_path(path)
        );
        return Err(1);
    }

    let next_id = if let Some(v) = value {
        match validate_fixed_id(&v, width) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("{e}");
                return Err(1);
            }
        }
    } else {
        match detect_next_id(kind) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("{e}");
                return Err(1);
            }
        }
    };

    if let Err(e) = ensure_parent_dir_exists(path) {
        eprintln!("{e}");
        return Err(1);
    }
    if let Err(e) = fs::write(path, format!("{next_id}\n")) {
        eprintln!("failed to write {}: {e}", normalize_path(path));
        return Err(1);
    }

    println!("{}", normalize_path(path));
    Ok(())
}

fn cmd_peek_next_id(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut kind: Option<Kind> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--kind" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --kind\n{}", peek_next_id_usage());
                    return Err(1);
                };
                kind = parse_kind(&v);
                if kind.is_none() {
                    eprintln!("invalid --kind value: {v}");
                    return Err(1);
                }
            }
            "-h" | "--help" => {
                println!("{}", peek_next_id_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", peek_next_id_usage());
                return Err(1);
            }
        }
    }

    let Some(kind) = kind else {
        eprintln!("missing required arg: --kind\n{}", peek_next_id_usage());
        return Err(1);
    };

    match read_next_id(next_id_path(kind), id_width(kind)) {
        Ok(id) => {
            println!("{id}");
            Ok(())
        }
        Err(e) => {
            eprintln!("{e}");
            Err(1)
        }
    }
}

fn cmd_alloc_id(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut kind: Option<Kind> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--kind" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --kind\n{}", alloc_id_usage());
                    return Err(1);
                };
                kind = parse_kind(&v);
                if kind.is_none() {
                    eprintln!("invalid --kind value: {v}");
                    return Err(1);
                }
            }
            "-h" | "--help" => {
                println!("{}", alloc_id_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", alloc_id_usage());
                return Err(1);
            }
        }
    }

    let Some(kind) = kind else {
        eprintln!("missing required arg: --kind\n{}", alloc_id_usage());
        return Err(1);
    };

    let width = id_width(kind);
    let path = next_id_path(kind);
    let current = match read_next_id(path, width) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("{e}");
            return Err(1);
        }
    };

    let current_num = match current.parse::<u64>() {
        Ok(v) => v,
        Err(_) => {
            eprintln!("invalid next-id content in {}", normalize_path(path));
            return Err(1);
        }
    };
    let max = 10_u64.pow(width as u32) - 1;
    if current_num >= max {
        eprintln!("next-id overflow in {}", normalize_path(path));
        return Err(1);
    }

    let next_num = current_num + 1;
    let next_text = format!("{next_num:0width$}");
    if let Err(e) = fs::write(path, format!("{next_text}\n")) {
        eprintln!("failed to update {}: {e}", normalize_path(path));
        return Err(1);
    }

    println!("{current}");
    Ok(())
}

fn cmd_validate_path(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --path\n{}", validate_path_usage());
                    return Err(1);
                };
                path = Some(PathBuf::from(v));
            }
            "-h" | "--help" => {
                println!("{}", validate_path_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", validate_path_usage());
                return Err(1);
            }
        }
    }

    let Some(path) = path else {
        eprintln!("missing required arg: --path\n{}", validate_path_usage());
        return Err(1);
    };

    match inspect_doc_path(&path) {
        Ok(info) => {
            print_json(&json!({
                "valid": true,
                "kind": kind_name(info.kind),
                "id": info.id,
                "slug": info.slug,
                "path": info.path,
                "state": info.state,
            }));
            Ok(())
        }
        Err(e) => {
            print_json(&json!({
                "valid": false,
                "path": normalize_for_doc_rule(&path),
                "error": e,
            }));
            Err(1)
        }
    }
}

fn cmd_inspect_path(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --path\n{}", inspect_path_usage());
                    return Err(1);
                };
                path = Some(PathBuf::from(v));
            }
            "-h" | "--help" => {
                println!("{}", inspect_path_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", inspect_path_usage());
                return Err(1);
            }
        }
    }

    let Some(path) = path else {
        eprintln!("missing required arg: --path\n{}", inspect_path_usage());
        return Err(1);
    };

    match inspect_doc_path(&path) {
        Ok(info) => {
            print_json(&json!({
                "ok": true,
                "valid": true,
                "kind": kind_name(info.kind),
                "id": info.id,
                "slug": info.slug,
                "path": info.path,
                "state": info.state,
            }));
            Ok(())
        }
        Err(e) => {
            print_json(&json!({
                "ok": false,
                "valid": false,
                "path": normalize_for_doc_rule(&path),
                "error": e,
            }));
            Err(1)
        }
    }
}

fn cmd_search_by_id(mut args: impl Iterator<Item = String>) -> Result<(), i32> {
    let mut kind: Option<Kind> = None;
    let mut id: Option<String> = None;
    let mut scope_raw = "open".to_string();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--kind" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --kind\n{}", search_by_id_usage());
                    return Err(1);
                };
                kind = parse_kind(&v);
                if kind.is_none() {
                    eprintln!("invalid --kind value: {v}");
                    return Err(1);
                }
            }
            "--id" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --id\n{}", search_by_id_usage());
                    return Err(1);
                };
                id = Some(v);
            }
            "--scope" => {
                let Some(v) = args.next() else {
                    eprintln!("missing value for --scope\n{}", search_by_id_usage());
                    return Err(1);
                };
                scope_raw = v;
            }
            "-h" | "--help" => {
                println!("{}", search_by_id_usage());
                return Ok(());
            }
            _ => {
                eprintln!("unknown arg: {arg}\n{}", search_by_id_usage());
                return Err(1);
            }
        }
    }

    let Some(kind) = kind else {
        eprintln!("missing required arg: --kind\n{}", search_by_id_usage());
        return Err(1);
    };
    let Some(id_raw) = id else {
        eprintln!("missing required arg: --id\n{}", search_by_id_usage());
        return Err(1);
    };

    let id = match validate_fixed_id(&id_raw, id_width(kind)) {
        Ok(v) => v,
        Err(e) => {
            print_json(&json!({
                "ok": false,
                "kind": kind_name(kind),
                "id": id_raw,
                "scope": scope_raw,
                "error": e,
            }));
            return Err(1);
        }
    };

    let scope = match parse_scope(&scope_raw) {
        Some(v) => v,
        None => {
            print_json(&json!({
                "ok": false,
                "kind": kind_name(kind),
                "id": id,
                "scope": scope_raw,
                "error": "scope must be one of: open, closed, all",
            }));
            return Err(1);
        }
    };

    if (kind == Kind::Task || kind == Kind::Rfc) && scope != Scope::Open && scope != Scope::All {
        print_json(&json!({
            "ok": false,
            "kind": kind_name(kind),
            "id": id,
            "scope": scope_name(scope),
            "error": "scope=closed is only valid for backlog kind",
        }));
        return Err(1);
    }

    let result = match search_by_id(kind, &id, scope) {
        Ok(v) => v,
        Err(e) => {
            print_json(&json!({
                "ok": false,
                "kind": kind_name(kind),
                "id": id,
                "scope": scope_name(scope),
                "error": e,
            }));
            return Err(1);
        }
    };

    print_json(&json!({
        "ok": true,
        "kind": kind_name(kind),
        "id": id,
        "scope": scope_name(scope),
        "path": result.path,
        "state": result.state,
    }));
    Ok(())
}

fn search_by_id(kind: Kind, id: &str, scope: Scope) -> Result<SearchMatch, String> {
    let mut matches: Vec<SearchMatch> = Vec::new();

    match kind {
        Kind::Task => {
            matches.extend(scan_dir_for_id(
                Path::new(TASK_DIR),
                id,
                id_width(kind),
                TASK_TEMPLATE_FILE,
                "open",
            )?);
        }
        Kind::Rfc => {
            matches.extend(scan_dir_for_id(
                Path::new(RFC_DIR),
                id,
                id_width(kind),
                RFC_TEMPLATE_FILE,
                "open",
            )?);
        }
        Kind::Backlog => match scope {
            Scope::Open => matches.extend(scan_dir_for_id(
                Path::new(BACKLOG_DIR),
                id,
                id_width(kind),
                BACKLOG_TEMPLATE_FILE,
                "open",
            )?),
            Scope::Closed => matches.extend(scan_dir_for_id(
                Path::new(BACKLOG_CLOSED_DIR),
                id,
                id_width(kind),
                BACKLOG_TEMPLATE_FILE,
                "closed",
            )?),
            Scope::All => {
                matches.extend(scan_dir_for_id(
                    Path::new(BACKLOG_DIR),
                    id,
                    id_width(kind),
                    BACKLOG_TEMPLATE_FILE,
                    "open",
                )?);
                matches.extend(scan_dir_for_id(
                    Path::new(BACKLOG_CLOSED_DIR),
                    id,
                    id_width(kind),
                    BACKLOG_TEMPLATE_FILE,
                    "closed",
                )?);
            }
        },
    }

    matches.sort_by(|a, b| a.path.cmp(&b.path));
    matches.dedup_by(|a, b| a.path == b.path);

    if matches.is_empty() {
        if kind == Kind::Backlog && scope == Scope::Open {
            let closed = scan_dir_for_id(
                Path::new(BACKLOG_CLOSED_DIR),
                id,
                id_width(kind),
                BACKLOG_TEMPLATE_FILE,
                "closed",
            )?;
            if !closed.is_empty() {
                let paths: Vec<String> = closed.iter().map(|m| m.path.clone()).collect();
                return Err(format!(
                    "backlog id {id} is closed; found only under docs/backlogs/closed: {}",
                    paths.join(", ")
                ));
            }
        }
        return Err(format!("no document found for kind={} id={id}", kind_name(kind)));
    }

    if matches.len() > 1 {
        let paths: Vec<String> = matches.iter().map(|m| m.path.clone()).collect();
        return Err(format!(
            "ambiguous id {id} for kind={}: {}",
            kind_name(kind),
            paths.join(", ")
        ));
    }

    Ok(matches.remove(0))
}

fn scan_dir_for_id(
    dir: &Path,
    id: &str,
    width: usize,
    template_name: &str,
    state: &str,
) -> Result<Vec<SearchMatch>, String> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
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
        if name == template_name {
            continue;
        }
        let Some((found_id, _slug)) = parse_doc_name(&name, width) else {
            continue;
        };
        if found_id != id {
            continue;
        }
        out.push(SearchMatch {
            path: normalize_path(&entry.path()),
            state: state.to_string(),
        });
    }

    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

fn inspect_doc_path(path: &Path) -> Result<DocInfo, String> {
    let normalized = normalize_ref_path(path);
    let rel = normalize_for_doc_rule(&normalized);

    if !normalized.exists() {
        return Err(format!("path not found: {rel}"));
    }
    if !normalized.is_file() {
        return Err(format!("path is not a file: {rel}"));
    }

    if let Some(rest) = rel.strip_prefix(&format!("{TASK_DIR}/")) {
        if let Some((id, slug)) = parse_doc_name(rest, 6) {
            if rest == TASK_TEMPLATE_FILE {
                return Err("task template path is not a planning doc".to_string());
            }
            return Ok(DocInfo {
                kind: Kind::Task,
                id,
                slug,
                path: rel,
                state: "open".to_string(),
            });
        }
        return Err(
            "invalid task path pattern; expected docs/tasks/<6 digits>-<slug>.md".to_string(),
        );
    }

    if let Some(rest) = rel.strip_prefix(&format!("{BACKLOG_CLOSED_DIR}/")) {
        if let Some((id, slug)) = parse_doc_name(rest, 6) {
            if rest == BACKLOG_TEMPLATE_FILE {
                return Err("backlog template path is not a backlog doc".to_string());
            }
            return Ok(DocInfo {
                kind: Kind::Backlog,
                id,
                slug,
                path: rel,
                state: "closed".to_string(),
            });
        }
        return Err("invalid closed backlog path pattern; expected docs/backlogs/closed/<6 digits>-<slug>.md".to_string());
    }

    if let Some(rest) = rel.strip_prefix(&format!("{BACKLOG_DIR}/")) {
        if let Some((id, slug)) = parse_doc_name(rest, 6) {
            if rest == BACKLOG_TEMPLATE_FILE {
                return Err("backlog template path is not a backlog doc".to_string());
            }
            return Ok(DocInfo {
                kind: Kind::Backlog,
                id,
                slug,
                path: rel,
                state: "open".to_string(),
            });
        }
        return Err(
            "invalid backlog path pattern; expected docs/backlogs/<6 digits>-<slug>.md".to_string(),
        );
    }

    if let Some(rest) = rel.strip_prefix(&format!("{RFC_DIR}/")) {
        if let Some((id, slug)) = parse_doc_name(rest, 4) {
            if rest == RFC_TEMPLATE_FILE {
                return Err("rfc template path is not an RFC doc".to_string());
            }
            return Ok(DocInfo {
                kind: Kind::Rfc,
                id,
                slug,
                path: rel,
                state: "open".to_string(),
            });
        }
        return Err("invalid rfc path pattern; expected docs/rfcs/<4 digits>-<slug>.md".to_string());
    }

    Err("unsupported path; expected docs/tasks, docs/backlogs, docs/backlogs/closed, or docs/rfcs".to_string())
}

fn parse_doc_name(name: &str, width: usize) -> Option<(String, String)> {
    if !name.ends_with(".md") {
        return None;
    }
    if name.contains('/') || name.contains('\\') {
        return None;
    }
    let stem = &name[..name.len() - 3];
    let (id, slug) = stem.split_once('-')?;
    if id.len() != width || !id.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !is_valid_slug(slug) {
        return None;
    }
    Some((id.to_string(), slug.to_string()))
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

fn detect_next_id(kind: Kind) -> Result<String, String> {
    let width = id_width(kind);
    let mut max_id: u64 = 0;
    let dirs: Vec<(&str, usize, &str)> = match kind {
        Kind::Task => vec![(TASK_DIR, width, TASK_TEMPLATE_FILE)],
        Kind::Backlog => vec![
            (BACKLOG_DIR, width, BACKLOG_TEMPLATE_FILE),
            (BACKLOG_CLOSED_DIR, width, BACKLOG_TEMPLATE_FILE),
        ],
        Kind::Rfc => vec![(RFC_DIR, width, RFC_TEMPLATE_FILE)],
    };

    for (dir_text, dir_width, template) in dirs {
        let dir = Path::new(dir_text);
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
            if name == template {
                continue;
            }
            let Some((id_text, _slug)) = parse_doc_name(&name, dir_width) else {
                continue;
            };
            let Ok(id_num) = id_text.parse::<u64>() else {
                continue;
            };
            if id_num > max_id {
                max_id = id_num;
            }
        }
    }

    let next = max_id + 1;
    let max = 10_u64.pow(width as u32) - 1;
    if next > max {
        return Err(format!("id overflow for kind={}", kind_name(kind)));
    }
    Ok(format!("{next:0width$}"))
}

fn parse_kind(text: &str) -> Option<Kind> {
    match text {
        "task" => Some(Kind::Task),
        "backlog" => Some(Kind::Backlog),
        "rfc" => Some(Kind::Rfc),
        _ => None,
    }
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Task => "task",
        Kind::Backlog => "backlog",
        Kind::Rfc => "rfc",
    }
}

fn parse_scope(text: &str) -> Option<Scope> {
    match text {
        "open" => Some(Scope::Open),
        "closed" => Some(Scope::Closed),
        "all" => Some(Scope::All),
        _ => None,
    }
}

fn scope_name(scope: Scope) -> &'static str {
    match scope {
        Scope::Open => "open",
        Scope::Closed => "closed",
        Scope::All => "all",
    }
}

fn id_width(kind: Kind) -> usize {
    match kind {
        Kind::Task | Kind::Backlog => 6,
        Kind::Rfc => 4,
    }
}

fn next_id_path(kind: Kind) -> &'static Path {
    match kind {
        Kind::Task => Path::new(TASK_NEXT_ID_FILE),
        Kind::Backlog => Path::new(BACKLOG_NEXT_ID_FILE),
        Kind::Rfc => Path::new(RFC_NEXT_ID_FILE),
    }
}

fn validate_fixed_id(id: &str, width: usize) -> Result<String, String> {
    if id.len() != width || !id.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("id must be exactly {width} digits"));
    }
    Ok(id.to_string())
}

fn read_next_id(path: &Path, width: usize) -> Result<String, String> {
    if !path.exists() {
        return Err(format!(
            "next-id file not found: {} (run tools/doc-id.rs init-next-id --kind ... first)",
            normalize_path(path)
        ));
    }
    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", normalize_path(path)))?;
    validate_fixed_id(content.trim(), width)
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

fn normalize_ref_path(path: &Path) -> PathBuf {
    let text = normalize_path(path);
    if let Some(stripped) = text.strip_prefix("./") {
        PathBuf::from(stripped)
    } else {
        PathBuf::from(text)
    }
}

fn normalize_for_doc_rule(path: &Path) -> String {
    let raw = normalize_path(path);
    raw.trim_start_matches(|c| c == '.' || c == '/')
        .to_string()
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn print_json(value: &serde_json::Value) {
    let out = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string());
    println!("{out}");
}
