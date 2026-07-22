#!/usr/bin/env -S cargo +nightly -q -Zscript
---
[package]
edition = "2024"

[dependencies]
syn = { version = "2", features = ["full", "visit"] }
---

use std::collections::BTreeSet;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use syn::visit::{self, Visit};
use syn::{Attribute, ImplItem, ItemImpl, ItemMod, ItemStruct, ItemTrait, TraitItem, Type};

const DEFAULT_ROOT: &str = "doradb-storage";

#[derive(Debug)]
struct Args {
    root: PathBuf,
    write_path: Option<PathBuf>,
    help: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuditRow {
    path: String,
    function: String,
    disclose_calls: usize,
}

#[derive(Debug, Default)]
struct TestOnlyStructCollector {
    module_path: Vec<String>,
    test_structs: BTreeSet<String>,
    production_structs: BTreeSet<String>,
}

impl TestOnlyStructCollector {
    fn struct_key(&self, name: &str) -> String {
        qualified_name(&self.module_path, name)
    }

    fn test_only_structs(mut self) -> BTreeSet<String> {
        self.test_structs
            .retain(|name| !self.production_structs.contains(name));
        self.test_structs
    }
}

impl<'ast> Visit<'ast> for TestOnlyStructCollector {
    fn visit_item_mod(&mut self, item_mod: &'ast ItemMod) {
        if has_cfg_test(&item_mod.attrs) {
            return;
        }
        let Some((_, items)) = &item_mod.content else {
            return;
        };
        self.module_path.push(item_mod.ident.to_string());
        for item in items {
            self.visit_item(item);
        }
        self.module_path.pop();
    }

    fn visit_item_struct(&mut self, item_struct: &'ast ItemStruct) {
        let key = self.struct_key(&item_struct.ident.to_string());
        if has_cfg_test(&item_struct.attrs) {
            self.test_structs.insert(key);
        } else {
            self.production_structs.insert(key);
        }
    }
}

struct ErrorAuditVisitor<'a> {
    path: &'a str,
    module_path: Vec<String>,
    test_only_structs: &'a BTreeSet<String>,
    rows: Vec<AuditRow>,
}

impl ErrorAuditVisitor<'_> {
    fn record(&mut self, function: String, block: &syn::Block) {
        let disclose_calls = count_disclose_calls(block);
        if disclose_calls > 0 {
            self.rows.push(AuditRow {
                path: self.path.to_string(),
                function,
                disclose_calls,
            });
        }
    }

    fn struct_key(&self, name: &str) -> String {
        qualified_name(&self.module_path, name)
    }
}

impl<'ast> Visit<'ast> for ErrorAuditVisitor<'_> {
    fn visit_item_mod(&mut self, item_mod: &'ast ItemMod) {
        if has_cfg_test(&item_mod.attrs) {
            return;
        }
        let Some((_, items)) = &item_mod.content else {
            return;
        };
        self.module_path.push(item_mod.ident.to_string());
        for item in items {
            self.visit_item(item);
        }
        self.module_path.pop();
    }

    fn visit_item_fn(&mut self, item_fn: &'ast syn::ItemFn) {
        if is_test_only_callable(&item_fn.attrs) {
            return;
        }
        self.record(item_fn.sig.ident.to_string(), &item_fn.block);
        visit::visit_block(self, &item_fn.block);
    }

    fn visit_item_impl(&mut self, item_impl: &'ast ItemImpl) {
        if has_cfg_test(&item_impl.attrs) {
            return;
        }
        let owner = impl_owner_name(&item_impl.self_ty);
        if self.test_only_structs.contains(&self.struct_key(&owner)) {
            return;
        }
        for item in &item_impl.items {
            let ImplItem::Fn(method) = item else {
                continue;
            };
            if is_test_only_callable(&method.attrs) {
                continue;
            }
            self.record(format!("{owner}::{}", method.sig.ident), &method.block);
            visit::visit_block(self, &method.block);
        }
    }

    fn visit_item_trait(&mut self, item_trait: &'ast ItemTrait) {
        if has_cfg_test(&item_trait.attrs) {
            return;
        }
        let owner = item_trait.ident.to_string();
        for item in &item_trait.items {
            let TraitItem::Fn(method) = item else {
                continue;
            };
            if is_test_only_callable(&method.attrs) {
                continue;
            }
            let Some(block) = &method.default else {
                continue;
            };
            self.record(format!("{owner}::{}", method.sig.ident), block);
            visit::visit_block(self, block);
        }
    }
}

#[derive(Debug, Default)]
struct DiscloseCallCounter {
    count: usize,
}

impl<'ast> Visit<'ast> for DiscloseCallCounter {
    fn visit_arm(&mut self, arm: &'ast syn::Arm) {
        if has_cfg_test(&arm.attrs) {
            return;
        }
        visit::visit_arm(self, arm);
    }

    fn visit_expr(&mut self, expr: &'ast syn::Expr) {
        if expression_has_cfg_test(expr) {
            return;
        }
        visit::visit_expr(self, expr);
    }

    fn visit_expr_method_call(&mut self, method_call: &'ast syn::ExprMethodCall) {
        if method_call.method == "disclose" {
            self.count += 1;
        }
        visit::visit_expr_method_call(self, method_call);
    }

    fn visit_field_value(&mut self, field_value: &'ast syn::FieldValue) {
        if has_cfg_test(&field_value.attrs) {
            return;
        }
        visit::visit_field_value(self, field_value);
    }

    fn visit_item(&mut self, _item: &'ast syn::Item) {}

    fn visit_local(&mut self, local: &'ast syn::Local) {
        if has_cfg_test(&local.attrs) {
            return;
        }
        visit::visit_local(self, local);
    }
}

fn usage() -> &'static str {
    "Usage: tools/error_audit.rs [--root <dir>] [--write <csv-path>]"
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = parse_args(env::args().skip(1))?;
    if args.help {
        println!("{}", usage());
        return Ok(());
    }

    let repo_root = env::current_dir()
        .map_err(|e| format!("failed to resolve current directory: {e}"))?
        .canonicalize()
        .map_err(|e| format!("failed to canonicalize current directory: {e}"))?;
    let root = if args.root.is_absolute() {
        args.root.clone()
    } else {
        repo_root.join(&args.root)
    };
    let root = root
        .canonicalize()
        .map_err(|e| format!("failed to resolve root {}: {e}", root.display()))?;
    if !root.is_dir() {
        return Err(format!("root is not a directory: {}", root.display()));
    }
    let root_display = relative_path(&repo_root, &root)?;

    let files = collect_rs_files(&repo_root, &root)
        .map_err(|e| format!("failed to scan {}: {e}", root.display()))?;
    let mut rows = Vec::new();
    for file in files {
        let path = relative_path(&repo_root, &file)?;
        let content = fs::read_to_string(&file)
            .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
        rows.extend(analyze_source(&path, &content)?);
    }
    sort_rows(&mut rows);

    if let Some(path) = args.write_path {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
        }
        fs::write(&path, render_csv(&rows))
            .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    } else {
        print!("{}", render_table(&root_display, &rows));
    }
    Ok(())
}

fn parse_args<I>(args: I) -> Result<Args, String>
where
    I: IntoIterator<Item = String>,
{
    let mut root: Option<PathBuf> = None;
    let mut write_path: Option<PathBuf> = None;
    let mut help = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                let Some(value) = args.next() else {
                    return Err(format!("missing value for --root\n{}", usage()));
                };
                if root.replace(PathBuf::from(value)).is_some() {
                    return Err(format!("--root can only be provided once\n{}", usage()));
                }
            }
            "--write" => {
                let Some(value) = args.next() else {
                    return Err(format!("missing value for --write\n{}", usage()));
                };
                if write_path.replace(PathBuf::from(value)).is_some() {
                    return Err(format!("--write can only be provided once\n{}", usage()));
                }
            }
            "--help" | "-h" => help = true,
            _ => return Err(format!("unknown arg: {arg}\n{}", usage())),
        }
    }
    if help && (root.is_some() || write_path.is_some()) {
        return Err(format!(
            "--help cannot be combined with other args\n{}",
            usage()
        ));
    }
    Ok(Args {
        root: root.unwrap_or_else(|| PathBuf::from(DEFAULT_ROOT)),
        write_path,
        help,
    })
}

fn collect_rs_files(repo_root: &Path, root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        if is_excluded_directory(repo_root, &dir) {
            continue;
        }
        let mut entries = fs::read_dir(&dir)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.path());
        for entry in entries {
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file()
                && path.extension().and_then(|extension| extension.to_str()) == Some("rs")
            {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn is_excluded_directory(repo_root: &Path, dir: &Path) -> bool {
    let Ok(relative) = dir.strip_prefix(repo_root) else {
        return false;
    };
    relative.components().any(|component| {
        let Component::Normal(name) = component else {
            return false;
        };
        name == "tests" || name == "examples"
    })
}

fn relative_path(repo_root: &Path, path: &Path) -> Result<String, String> {
    path.strip_prefix(repo_root)
        .map(normalize_path)
        .map_err(|_| {
            format!(
                "path is outside repository root {}: {}",
                repo_root.display(),
                path.display()
            )
        })
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn analyze_source(path: &str, content: &str) -> Result<Vec<AuditRow>, String> {
    let file = syn::parse_file(content).map_err(|e| format!("failed to parse {path}: {e}"))?;
    let mut collector = TestOnlyStructCollector::default();
    collector.visit_file(&file);
    let test_only_structs = collector.test_only_structs();
    let mut visitor = ErrorAuditVisitor {
        path,
        module_path: Vec::new(),
        test_only_structs: &test_only_structs,
        rows: Vec::new(),
    };
    visitor.visit_file(&file);
    Ok(visitor.rows)
}

fn count_disclose_calls(block: &syn::Block) -> usize {
    let mut counter = DiscloseCallCounter::default();
    counter.visit_block(block);
    counter.count
}

fn has_cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<syn::Path>()
                .is_ok_and(|path| path.is_ident("test"))
    })
}

fn expression_has_cfg_test(expr: &syn::Expr) -> bool {
    macro_rules! check_attrs {
        ($($variant:ident),+ $(,)?) => {
            match expr {
                $(syn::Expr::$variant(expr) => has_cfg_test(&expr.attrs),)+
                _ => false,
            }
        };
    }

    check_attrs!(
        Array, Assign, Async, Await, Binary, Block, Break, Call, Cast, Closure, Const, Continue,
        Field, ForLoop, Group, If, Index, Infer, Let, Lit, Loop, Macro, Match, MethodCall, Paren,
        Path, Range, RawAddr, Reference, Repeat, Return, Struct, Try, TryBlock, Tuple, Unary,
        Unsafe, While, Yield,
    )
}

fn is_test_only_callable(attrs: &[Attribute]) -> bool {
    has_cfg_test(attrs) || attrs.iter().any(|attr| attr.path().is_ident("test"))
}

fn qualified_name(module_path: &[String], name: &str) -> String {
    if module_path.is_empty() {
        return name.to_string();
    }
    format!("{}::{name}", module_path.join("::"))
}

fn impl_owner_name(ty: &Type) -> String {
    match ty {
        Type::Group(group) => impl_owner_name(&group.elem),
        Type::Paren(paren) => impl_owner_name(&paren.elem),
        Type::Path(path) => path
            .path
            .segments
            .last()
            .map_or_else(|| "<impl>".to_string(), |segment| segment.ident.to_string()),
        Type::Reference(reference) => impl_owner_name(&reference.elem),
        _ => "<impl>".to_string(),
    }
}

fn sort_rows(rows: &mut [AuditRow]) {
    rows.sort_by(|a, b| {
        a.path
            .cmp(&b.path)
            .then_with(|| a.function.cmp(&b.function))
    });
}

fn render_table(root: &str, rows: &[AuditRow]) -> String {
    let mut out = String::new();
    let total_calls = rows.iter().map(|row| row.disclose_calls).sum::<usize>();
    out.push_str("# Public Error Conversion Audit\n\n");
    writeln!(out, "- Root: `{root}`").unwrap();
    writeln!(out, "- Functions/methods: {}", rows.len()).unwrap();
    writeln!(out, "- Disclose calls: {total_calls}\n").unwrap();
    out.push_str("| file | function/method | disclose calls |\n");
    out.push_str("|---|---|---:|\n");
    for row in rows {
        writeln!(
            out,
            "| `{}` | `{}` | {} |",
            escape_table_cell(&row.path),
            escape_table_cell(&row.function),
            row.disclose_calls
        )
        .unwrap();
    }
    out
}

fn escape_table_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('`', "\\`")
}

fn render_csv(rows: &[AuditRow]) -> String {
    let mut out = String::from("file,function_or_method,disclose_calls\n");
    for row in rows {
        writeln!(
            out,
            "{},{},{}",
            csv_field(&row.path),
            csv_field(&row.function),
            row.disclose_calls
        )
        .unwrap();
    }
    out
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row<'a>(rows: &'a [AuditRow], function: &str) -> &'a AuditRow {
        rows.iter()
            .find(|row| row.function == function)
            .unwrap_or_else(|| panic!("missing row for {function}"))
    }

    #[test]
    fn audit_counts_calls_at_function_and_method_level() {
        let source = r#"
            fn free_function() {
                first.disclose();
                let closure = || second.disclose();
                async move { third.disclose() };
            }

            fn no_disclosure() {}

            struct Worker;

            impl Worker {
                fn inherent_method(&self) {
                    value.disclose();
                    value.disclose();
                }
            }

            trait Runner {
                fn default_method(&self) {
                    value.disclose();
                }

                fn implemented_method(&self);
            }

            impl Runner for Worker {
                fn implemented_method(&self) {
                    value.disclose();
                }
            }
        "#;

        let rows = analyze_source("doradb-storage/src/sample.rs", source).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(row(&rows, "free_function").disclose_calls, 3);
        assert_eq!(row(&rows, "Worker::inherent_method").disclose_calls, 2);
        assert_eq!(row(&rows, "Runner::default_method").disclose_calls, 1);
        assert_eq!(row(&rows, "Worker::implemented_method").disclose_calls, 1);
        assert!(!rows.iter().any(|row| row.function == "no_disclosure"));
    }

    #[test]
    fn audit_ignores_test_only_items_and_struct_implementations() {
        let source = r#"
            #[cfg(test)]
            mod tests {
                fn module_test() { value.disclose(); }
            }

            #[cfg(test)]
            fn cfg_test_function() { value.disclose(); }

            #[test]
            fn test_function() { value.disclose(); }

            #[cfg(test)]
            struct TestHelper;

            impl TestHelper {
                fn helper_method(&self) { value.disclose(); }
            }

            struct Production;

            #[cfg(test)]
            impl Production {
                fn test_impl_method(&self) { value.disclose(); }
            }

            impl Production {
                #[cfg(test)]
                fn test_method(&self) { value.disclose(); }

                fn production_method(&self) { value.disclose(); }
            }
        "#;

        let rows = analyze_source("doradb-storage/src/sample.rs", source).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].function, "Production::production_method");
        assert_eq!(rows[0].disclose_calls, 1);
    }

    #[test]
    fn audit_ignores_test_only_scopes_inside_production_functions() {
        let source = r#"
            fn production_function() {
                production.disclose();

                #[cfg(test)]
                if test_condition() {
                    test_branch.disclose();
                }

                #[cfg(test)]
                let test_value = test_initializer.disclose();

                match value {
                    #[cfg(test)]
                    Test => test_arm.disclose(),
                    Production => production_arm.disclose(),
                }

                let value = Struct {
                    #[cfg(test)]
                    test_field: test_field.disclose(),
                    production_field: production_field.disclose(),
                };
            }
        "#;

        let rows = analyze_source("doradb-storage/src/sample.rs", source).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].function, "production_function");
        assert_eq!(rows[0].disclose_calls, 3);
    }

    #[test]
    fn directory_filter_excludes_tests_and_examples() {
        let repo_root = Path::new("/repo");
        assert!(is_excluded_directory(
            repo_root,
            Path::new("/repo/doradb-storage/tests")
        ));
        assert!(is_excluded_directory(
            repo_root,
            Path::new("/repo/doradb-storage/examples/nested")
        ));
        assert!(!is_excluded_directory(
            repo_root,
            Path::new("/repo/doradb-storage/src")
        ));
    }

    #[test]
    fn output_is_sorted_and_uses_stable_schemas() {
        let mut rows = vec![
            AuditRow {
                path: "doradb-storage/src/z.rs".to_string(),
                function: "zeta".to_string(),
                disclose_calls: 1,
            },
            AuditRow {
                path: "doradb-storage/src/a,quoted.rs".to_string(),
                function: "Worker::zeta".to_string(),
                disclose_calls: 2,
            },
            AuditRow {
                path: "doradb-storage/src/a,quoted.rs".to_string(),
                function: "Worker::alpha".to_string(),
                disclose_calls: 3,
            },
        ];
        sort_rows(&mut rows);

        assert_eq!(rows[0].function, "Worker::alpha");
        assert_eq!(rows[1].function, "Worker::zeta");
        assert_eq!(rows[2].path, "doradb-storage/src/z.rs");

        let csv = render_csv(&rows);
        assert!(csv.starts_with("file,function_or_method,disclose_calls\n"));
        assert!(csv.contains("\"doradb-storage/src/a,quoted.rs\",Worker::alpha,3"));

        let table = render_table("doradb-storage", &rows);
        assert!(table.contains("- Functions/methods: 3"));
        assert!(table.contains("- Disclose calls: 6"));
        let alpha = table.find("Worker::alpha").unwrap();
        let zeta = table.find("Worker::zeta").unwrap();
        let last_file = table.find("doradb-storage/src/z.rs").unwrap();
        assert!(alpha < zeta && zeta < last_file);
    }

    #[test]
    fn parse_args_uses_default_root_and_accepts_output() {
        let defaults = parse_args(Vec::<String>::new()).unwrap();
        assert_eq!(defaults.root, Path::new(DEFAULT_ROOT));
        assert!(defaults.write_path.is_none());

        let args = parse_args([
            "--root".to_string(),
            "doradb-storage/src".to_string(),
            "--write".to_string(),
            "target/error-audit.csv".to_string(),
        ])
        .unwrap();
        assert_eq!(args.root, Path::new("doradb-storage/src"));
        assert_eq!(
            args.write_path.as_deref(),
            Some(Path::new("target/error-audit.csv"))
        );
    }
}
