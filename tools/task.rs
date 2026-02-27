#!/usr/bin/env -S cargo +nightly -q -Zscript
---
[package]
edition = "2024"
---

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn usage() -> &'static str {
    "Usage: tools/task.rs <subcommand> [options]\n\n\
Subcommands:\n\
  next-task-id     Print the next task id based on docs/tasks/<id>-<slug>.md files\n\
  create-task-doc  Create a docs/tasks task document from template with validated id and slug\n"
}

fn next_task_id_usage() -> &'static str {
    "Usage: tools/task.rs next-task-id [--dir <path>] [--width <n>]"
}

fn create_task_doc_usage() -> &'static str {
    "Usage: tools/task.rs create-task-doc --title <title> --slug <slug> (--id <6digits> | --auto-id) [--template <path>] [--output-dir <path>] [--force]"
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
        _ => Err(format!("unknown subcommand: {subcommand}\n{}", usage())),
    }
}

fn run_next_task_id(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut task_dir = PathBuf::from("docs/tasks");
    let mut width_raw = "6".to_string();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dir" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --dir\n{}", next_task_id_usage()));
                };
                task_dir = PathBuf::from(v);
            }
            "--width" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --width\n{}", next_task_id_usage()));
                };
                width_raw = v;
            }
            "-h" | "--help" => {
                println!("{}", next_task_id_usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", next_task_id_usage())),
        }
    }

    let width = parse_positive_width(&width_raw)?;
    let id = detect_next_task_id(&task_dir, width)?;
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
                    return Err(format!("missing value for --title\n{}", create_task_doc_usage()));
                };
                title = Some(v);
            }
            "--slug" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --slug\n{}", create_task_doc_usage()));
                };
                slug = Some(v);
            }
            "--id" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --id\n{}", create_task_doc_usage()));
                };
                task_id = Some(v);
            }
            "--auto-id" => {
                auto_id = true;
            }
            "--template" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --template\n{}", create_task_doc_usage()));
                };
                template = PathBuf::from(v);
            }
            "--output-dir" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --output-dir\n{}", create_task_doc_usage()));
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

    let title = title.ok_or_else(|| format!("missing required arg: --title\n{}", create_task_doc_usage()))?;
    let slug = slug.ok_or_else(|| format!("missing required arg: --slug\n{}", create_task_doc_usage()))?;
    let slug = validate_slug(&slug)?;

    if auto_id && task_id.is_some() {
        return Err("use either --id or --auto-id, not both".to_string());
    }
    if !auto_id && task_id.is_none() {
        return Err(format!("one of --id or --auto-id is required\n{}", create_task_doc_usage()));
    }

    let task_id = if auto_id {
        detect_next_id_from_output_dir(&output_dir)?
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
        return Err(format!("task directory not found: {}", normalize_path(task_dir)));
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

fn load_template(template_path: &Path) -> Result<String, String> {
    if !template_path.exists() {
        return Err(format!("template not found: {}", normalize_path(template_path)));
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

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
