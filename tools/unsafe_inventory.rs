#!/usr/bin/env -S cargo +nightly -Zscript
---
[package]
edition = "2024"
---

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

const MODULES: [&str; 8] = ["buffer", "latch", "row", "index", "io", "trx", "lwc", "file"];

#[derive(Debug, Clone, Default)]
struct FileMetrics {
    path: String,
    module: String,
    unsafe_count: usize,
    transmute_count: usize,
    new_unchecked_count: usize,
    assume_init_count: usize,
    safety_comment_count: usize,
}

#[derive(Debug, Clone, Default)]
struct ModuleMetrics {
    module: String,
    file_count: usize,
    unsafe_count: usize,
    transmute_count: usize,
    new_unchecked_count: usize,
    assume_init_count: usize,
    safety_comment_count: usize,
}

fn usage() -> &'static str {
    "Usage: cargo +nightly -Zscript tools/unsafe_inventory.rs [--write <path>] [--top <n>]"
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut write_path: Option<PathBuf> = None;
    let mut top: usize = 40;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--write" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --write\n{}", usage()));
                };
                write_path = Some(PathBuf::from(v));
            }
            "--top" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --top\n{}", usage()));
                };
                top = v
                    .parse::<usize>()
                    .map_err(|_| format!("invalid --top value: {v}"))?;
            }
            "--help" | "-h" => {
                println!("{}", usage());
                return Ok(());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", usage())),
        }
    }

    let src_root = Path::new("doradb-storage/src");
    if !src_root.exists() {
        return Err("expected path not found: doradb-storage/src".to_string());
    }

    let mut module_rows = Vec::new();
    let mut all_file_rows = Vec::new();
    for module in MODULES {
        let module_dir = src_root.join(module);
        let files = collect_rs_files(&module_dir)
            .map_err(|e| format!("failed to scan {}: {e}", module_dir.display()))?;

        let mut mm = ModuleMetrics {
            module: module.to_string(),
            file_count: files.len(),
            ..ModuleMetrics::default()
        };

        for file in files {
            let content = fs::read_to_string(&file)
                .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
            let rel_path = normalize_path(&file);
            let row = FileMetrics {
                path: rel_path,
                module: module.to_string(),
                unsafe_count: count_word(&content, "unsafe"),
                transmute_count: count_substring(&content, "transmute"),
                new_unchecked_count: count_substring(&content, "new_unchecked"),
                assume_init_count: count_substring(&content, "assume_init"),
                safety_comment_count: count_substring(&content, "// SAFETY:"),
            };
            mm.unsafe_count += row.unsafe_count;
            mm.transmute_count += row.transmute_count;
            mm.new_unchecked_count += row.new_unchecked_count;
            mm.assume_init_count += row.assume_init_count;
            mm.safety_comment_count += row.safety_comment_count;
            all_file_rows.push(row);
        }

        module_rows.push(mm);
    }

    let markdown = render_markdown(&module_rows, &all_file_rows, top);
    if let Some(path) = write_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
        }
        fs::write(&path, &markdown).map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    }
    print!("{markdown}");
    Ok(())
}

fn collect_rs_files(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = fs::read_dir(&dir)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.path());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn count_substring(input: &str, needle: &str) -> usize {
    if needle.is_empty() {
        return 0;
    }
    let mut count = 0usize;
    let mut start = 0usize;
    while let Some(pos) = input[start..].find(needle) {
        count += 1;
        start += pos + needle.len();
    }
    count
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn count_word(input: &str, needle: &str) -> usize {
    if needle.is_empty() {
        return 0;
    }
    let bytes = input.as_bytes();
    let nbytes = needle.as_bytes();
    if bytes.len() < nbytes.len() {
        return 0;
    }
    let mut count = 0usize;
    for i in 0..=bytes.len() - nbytes.len() {
        if &bytes[i..i + nbytes.len()] != nbytes {
            continue;
        }
        let left_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
        let right_pos = i + nbytes.len();
        let right_ok = right_pos == bytes.len() || !is_ident_byte(bytes[right_pos]);
        if left_ok && right_ok {
            count += 1;
        }
    }
    count
}

fn generated_at() -> String {
    // Use current UTC day to reflect when inventory is generated.
    let output = Command::new("date")
        .args(["-u", "+%F"])
        .output();
    if let Ok(out) = output {
        if out.status.success() {
            let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !s.is_empty() {
                return s;
            }
        }
    }
    "unknown".to_string()
}

fn render_markdown(module_rows: &[ModuleMetrics], file_rows: &[FileMetrics], top: usize) -> String {
    let mut out = String::new();
    out.push_str("# Unsafe Usage Baseline\n\n");
    out.push_str(&format!("- Generated on: `{}`\n", generated_at()));
    out.push_str("- Command: `cargo +nightly -Zscript tools/unsafe_inventory.rs`\n");
    out.push_str("- Scope: `doradb-storage/src/{buffer,latch,row,index,io,trx,lwc,file}`\n\n");

    out.push_str("## Module Summary\n\n");
    out.push_str("| module | files | unsafe | transmute | new_unchecked | assume_init | // SAFETY: |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
    let mut total = ModuleMetrics::default();
    for m in module_rows {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} |\n",
            m.module,
            m.file_count,
            m.unsafe_count,
            m.transmute_count,
            m.new_unchecked_count,
            m.assume_init_count,
            m.safety_comment_count
        ));
        total.file_count += m.file_count;
        total.unsafe_count += m.unsafe_count;
        total.transmute_count += m.transmute_count;
        total.new_unchecked_count += m.new_unchecked_count;
        total.assume_init_count += m.assume_init_count;
        total.safety_comment_count += m.safety_comment_count;
    }
    out.push_str(&format!(
        "| **total** | **{}** | **{}** | **{}** | **{}** | **{}** | **{}** |\n\n",
        total.file_count,
        total.unsafe_count,
        total.transmute_count,
        total.new_unchecked_count,
        total.assume_init_count,
        total.safety_comment_count
    ));

    let mut hotspots: Vec<&FileMetrics> = file_rows.iter().filter(|r| r.unsafe_count > 0).collect();
    hotspots.sort_by(|a, b| {
        b.unsafe_count
            .cmp(&a.unsafe_count)
            .then_with(|| a.path.cmp(&b.path))
    });
    out.push_str(&format!("## File Hotspots (top {})\n\n", top));
    out.push_str("| file | module | unsafe | // SAFETY: |\n");
    out.push_str("|---|---|---:|---:|\n");
    for row in hotspots.into_iter().take(top) {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} |\n",
            row.path, row.module, row.unsafe_count, row.safety_comment_count
        ));
    }
    out.push('\n');

    let mut cast_risks: Vec<&FileMetrics> = file_rows
        .iter()
        .filter(|r| r.transmute_count + r.new_unchecked_count + r.assume_init_count > 0)
        .collect();
    cast_risks.sort_by(|a, b| {
        let ascore = a.transmute_count + a.new_unchecked_count + a.assume_init_count;
        let bscore = b.transmute_count + b.new_unchecked_count + b.assume_init_count;
        bscore.cmp(&ascore).then_with(|| a.path.cmp(&b.path))
    });

    out.push_str("## Cast-Risk Candidates\n\n");
    out.push_str("| file | module | transmute | new_unchecked | assume_init | total |\n");
    out.push_str("|---|---|---:|---:|---:|---:|\n");
    for row in cast_risks.into_iter().take(top) {
        let total = row.transmute_count + row.new_unchecked_count + row.assume_init_count;
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} |\n",
            row.path,
            row.module,
            row.transmute_count,
            row.new_unchecked_count,
            row.assume_init_count,
            total
        ));
    }
    out
}
