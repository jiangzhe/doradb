use super::model::{
    ExcludedRange, FilePolicy, LinePolicy, Position, Result, SourceIndex, digest, repo_path,
};
use proc_macro2::{Delimiter, Span, TokenStream, TokenTree};
use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Attribute, Expr, Lit, Meta, Token};

#[derive(Clone, Debug, Default)]
pub(super) struct Configuration {
    pub(super) predicates: BTreeSet<String>,
    pub(super) features: BTreeSet<String>,
    pub(super) declared_features: BTreeSet<String>,
}

impl Configuration {
    fn evaluate(&self, meta: &Meta, test: bool) -> Result<bool> {
        match meta {
            Meta::List(list) => {
                let values = list
                    .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                    .map_err(|e| format!("invalid cfg: {e}"))?;
                // Evaluate every operand, so an unknown predicate cannot hide behind short circuiting.
                let values = values
                    .iter()
                    .map(|m| self.evaluate(m, test))
                    .collect::<Result<Vec<_>>>()?;
                if list.path.is_ident("all") {
                    Ok(values.iter().all(|v| *v))
                } else if list.path.is_ident("any") {
                    Ok(values.iter().any(|v| *v))
                } else if list.path.is_ident("not") && values.len() == 1 {
                    Ok(!values[0])
                } else {
                    Err(format!(
                        "unsupported cfg operator: {}",
                        list.to_token_stream()
                    ))
                }
            }
            Meta::Path(path) if path.is_ident("test") => Ok(test),
            Meta::Path(path) => {
                let key = path.to_token_stream().to_string();
                if matches!(
                    key.as_str(),
                    "unix"
                        | "windows"
                        | "debug_assertions"
                        | "proc_macro"
                        | "coverage"
                        | "coverage_nightly"
                ) {
                    Ok(self.predicates.contains(&key))
                } else {
                    Err(format!(
                        "unmodeled cfg `{key}`; extend the coverage configuration before collecting"
                    ))
                }
            }
            Meta::NameValue(nv) => {
                let key = nv.path.to_token_stream().to_string();
                let Expr::Lit(value) = &nv.value else {
                    return Err("cfg values must be string literals".into());
                };
                let Lit::Str(value) = &value.lit else {
                    return Err("cfg values must be string literals".into());
                };
                let value = value.value();
                if key == "feature" {
                    if !self.declared_features.contains(&value) {
                        return Err(format!("unknown workspace feature `{value}`"));
                    }
                    Ok(self.features.contains(&value))
                } else if matches!(
                    key.as_str(),
                    "target_arch"
                        | "target_os"
                        | "target_env"
                        | "target_vendor"
                        | "target_family"
                        | "target_endian"
                        | "target_pointer_width"
                        | "target_feature"
                        | "target_has_atomic"
                        | "panic"
                ) {
                    Ok(self.predicates.contains(&format!("{key}={value:?}")))
                } else {
                    Err(format!("unmodeled cfg key `{key}`"))
                }
            }
        }
    }

    fn expand(&self, meta: &Meta, test: bool, output: &mut Vec<Meta>) -> Result<()> {
        if meta.path().is_ident("cfg_attr") {
            let Meta::List(list) = meta else {
                return Err("invalid cfg_attr".into());
            };
            let args = list
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .map_err(|e| e.to_string())?;
            if args.len() < 2 {
                return Err("cfg_attr needs a predicate and attributes".into());
            }
            if self.evaluate(&args[0], test)? {
                for attr in args.iter().skip(1) {
                    self.expand(attr, test, output)?;
                }
            }
        } else {
            output.push(meta.clone());
        }
        Ok(())
    }

    fn attributes(&self, attrs: &[Attribute], test: bool) -> Result<Vec<Meta>> {
        let mut result = Vec::new();
        for attr in attrs {
            self.expand(&attr.meta, test, &mut result)?;
        }
        Ok(result)
    }

    fn enabled(&self, attrs: &[Attribute], test: bool) -> Result<bool> {
        let mut enabled = true;
        for meta in self.attributes(attrs, test)? {
            if meta.path().is_ident("cfg") {
                let Meta::List(list) = meta else {
                    return Err("invalid cfg attribute".into());
                };
                enabled &= self.evaluate(&list.parse_args().map_err(|e| e.to_string())?, test)?;
            } else if meta.path().is_ident("test") || meta.path().is_ident("bench") {
                enabled &= test;
            }
        }
        Ok(enabled)
    }
}

#[derive(Clone)]
pub(super) struct Root {
    pub(super) path: PathBuf,
    pub(super) owner: String,
    pub(super) scope: Option<String>,
    pub(super) configuration: Configuration,
}

#[derive(Clone)]
struct Inclusion {
    root: Root,
    directory: PathBuf,
    attribute_directory: PathBuf,
    normal: bool,
    test: bool,
    ancestry: Vec<PathBuf>,
}

struct ParsedOwner {
    ranges: Vec<ExcludedRange>,
    ignored: Vec<ExcludedRange>,
}

#[derive(Default)]
struct IndexedFile {
    source: String,
    owners: Vec<ParsedOwner>,
    names: BTreeSet<String>,
}

trait NodeAttributes {
    fn attributes(&self) -> &[Attribute];
}

macro_rules! enum_attributes {
    ($ty:ident: $($variant:ident),+ $(,)?) => {
        impl NodeAttributes for syn::$ty {
            fn attributes(&self) -> &[Attribute] {
                match self {
                    $(Self::$variant(node) => &node.attrs,)+
                    _ => &[],
                }
            }
        }
    };
}

macro_rules! struct_attributes {
    ($($ty:ident),+ $(,)?) => {
        $(impl NodeAttributes for syn::$ty {
            fn attributes(&self) -> &[Attribute] { &self.attrs }
        })+
    };
}

enum_attributes!(Item: Const, Enum, ExternCrate, Fn, ForeignMod, Impl, Macro, Mod, Static, Struct, Trait, TraitAlias, Type, Union, Use);
enum_attributes!(ImplItem: Const, Fn, Type, Macro);
enum_attributes!(TraitItem: Const, Fn, Type, Macro);
enum_attributes!(ForeignItem: Fn, Static, Type, Macro);
enum_attributes!(Expr: Array, Assign, Async, Await, Binary, Block, Break, Call, Cast, Closure, Const, Continue, Field, ForLoop, Group, If, Index, Infer, Let, Lit, Loop, Macro, Match, MethodCall, Paren, Path, Range, RawAddr, Reference, Repeat, Return, Struct, Try, TryBlock, Tuple, Unary, Unsafe, While, Yield);
enum_attributes!(Pat: Const, Ident, Lit, Macro, Or, Paren, Path, Range, Reference, Rest, Slice, Struct, Tuple, TupleStruct, Type, Wild);
struct_attributes!(Local, Field, FieldValue, FieldPat, Arm, Variant);

impl NodeAttributes for syn::FnArg {
    fn attributes(&self) -> &[Attribute] {
        match self {
            Self::Receiver(node) => &node.attrs,
            Self::Typed(node) => &node.attrs,
        }
    }
}

impl NodeAttributes for syn::GenericParam {
    fn attributes(&self) -> &[Attribute] {
        match self {
            Self::Lifetime(node) => &node.attrs,
            Self::Type(node) => &node.attrs,
            Self::Const(node) => &node.attrs,
        }
    }
}

impl NodeAttributes for syn::Stmt {
    fn attributes(&self) -> &[Attribute] {
        match self {
            Self::Local(node) => &node.attrs,
            Self::Macro(node) => &node.attrs,
            Self::Item(node) => node.attributes(),
            Self::Expr(node, _) => node.attributes(),
        }
    }
}

macro_rules! visit_node {
    ($method:ident, $ty:ident, $comma:expr) => {
        fn $method(&mut self, node: &'ast syn::$ty) {
            let state = self.node(node, $comma);
            visit::$method(self, node);
            self.restore(state);
        }
    };
}

struct Classifier {
    inclusion: Inclusion,
    commas: BTreeMap<Position, Position>,
    ranges: Vec<ExcludedRange>,
    ignored: Vec<ExcludedRange>,
    pending: Vec<Inclusion>,
    error: Option<String>,
}

impl Classifier {
    fn enter(&mut self, attrs: &[Attribute], start: Position, end: Position) -> (bool, bool) {
        let previous = (self.inclusion.normal, self.inclusion.test);
        for attr in attrs {
            let (start, end) = positions(attr.span());
            self.ignored.push(ExcludedRange {
                start,
                end,
                reason: String::new(),
                owner: String::new(),
            });
        }
        let evaluate = || -> Result<(bool, bool)> {
            Ok((
                previous.0 && self.inclusion.root.configuration.enabled(attrs, false)?,
                previous.1 && self.inclusion.root.configuration.enabled(attrs, true)?,
            ))
        };
        match evaluate() {
            Ok((normal, test)) => {
                self.inclusion.normal = normal;
                self.inclusion.test = test;
                if !normal && (previous.0 || previous.1 != test) {
                    self.ranges.push(ExcludedRange {
                        start,
                        end,
                        reason: if !test {
                            "inactive cfg"
                        } else if attrs.iter().any(|a| a.path().is_ident("test")) {
                            "test attribute"
                        } else {
                            "test cfg"
                        }
                        .into(),
                        owner: self.inclusion.root.owner.clone(),
                    });
                }
            }
            Err(error) => self.error = Some(format!("{}:{}: {error}", start.line, start.column)),
        }
        previous
    }

    fn node<T: NodeAttributes + Spanned>(
        &mut self,
        node: &T,
        trailing_comma: bool,
    ) -> (bool, bool) {
        let attrs = node.attributes();
        let (start, mut end) = positions(node.span());
        if trailing_comma && let Some(comma_end) = self.commas.get(&end) {
            end = *comma_end;
        }
        self.enter(attrs, start, end)
    }

    fn restore(&mut self, state: (bool, bool)) {
        (self.inclusion.normal, self.inclusion.test) = state;
    }

    fn module(&mut self, module: &syn::ItemMod) -> Result<()> {
        let name = module.ident.to_string();
        let old_directory = self.inclusion.directory.clone();
        let old_attribute_directory = self.inclusion.attribute_directory.clone();
        let old_owner = self.inclusion.root.owner.clone();
        self.inclusion.root.owner.push_str(&format!("::{name}"));
        // cfg_attr can select different paths in normal and harness compilations.
        let mut paths: BTreeMap<(PathBuf, bool), (bool, bool)> = BTreeMap::new();
        for test in [false, true] {
            if if test {
                !self.inclusion.test
            } else {
                !self.inclusion.normal
            } {
                continue;
            }
            let mut explicit = None;
            for meta in self
                .inclusion
                .root
                .configuration
                .attributes(&module.attrs, test)?
            {
                if meta.path().is_ident("path") {
                    if let Meta::NameValue(nv) = meta
                        && let Expr::Lit(expr) = nv.value
                        && let Lit::Str(value) = expr.lit
                    {
                        explicit = Some(value.value());
                    }
                    if explicit.is_none() {
                        return Err("module #[path] must be a literal string".into());
                    }
                }
            }
            let has_path = explicit.is_some();
            let path = if let Some(path) = explicit {
                old_attribute_directory.join(path)
            } else if module.content.is_some() {
                old_directory.join(&name)
            } else {
                let plain = old_directory.join(format!("{name}.rs"));
                let nested = old_directory.join(&name).join("mod.rs");
                match (plain.exists(), nested.exists()) {
                    (true, false) => plain,
                    (false, true) => nested,
                    _ => {
                        return Err(format!(
                            "cannot resolve module {name} in {}",
                            old_directory.display()
                        ));
                    }
                }
            };
            let flags = paths.entry((path, has_path)).or_default();
            if test {
                flags.1 = true;
            } else {
                flags.0 = true;
            }
        }
        for ((path, explicit), (normal, test)) in paths {
            let state = (self.inclusion.normal, self.inclusion.test);
            self.inclusion.normal = normal;
            self.inclusion.test = test;
            if let Some((_, items)) = &module.content {
                self.inclusion.directory = path.clone();
                self.inclusion.attribute_directory = path;
                for item in items {
                    self.visit_item(item);
                }
            } else {
                let mut child = self.inclusion.clone();
                child.root.path = path.clone();
                child.attribute_directory = path.parent().unwrap_or(&old_directory).to_path_buf();
                child.directory = if explicit || path.file_name().is_some_and(|n| n == "mod.rs") {
                    path.parent().unwrap_or(&old_directory).to_path_buf()
                } else {
                    path.with_extension("")
                };
                self.pending.push(child);
            }
            self.restore(state);
        }
        self.inclusion.directory = old_directory;
        self.inclusion.attribute_directory = old_attribute_directory;
        self.inclusion.root.owner = old_owner;
        Ok(())
    }
}

impl<'ast> Visit<'ast> for Classifier {
    visit_node!(visit_item, Item, false);
    visit_node!(visit_impl_item, ImplItem, false);
    visit_node!(visit_trait_item, TraitItem, false);
    visit_node!(visit_foreign_item, ForeignItem, false);
    visit_node!(visit_local, Local, false);
    visit_node!(visit_expr, Expr, true);
    visit_node!(visit_pat, Pat, true);
    visit_node!(visit_field, Field, true);
    visit_node!(visit_field_value, FieldValue, true);
    visit_node!(visit_field_pat, FieldPat, true);
    visit_node!(visit_fn_arg, FnArg, true);
    visit_node!(visit_arm, Arm, true);
    visit_node!(visit_variant, Variant, true);
    visit_node!(visit_generic_param, GenericParam, true);

    fn visit_stmt(&mut self, node: &'ast syn::Stmt) {
        // The statement span includes the semicolon omitted by Expr::span().
        let state = self.node(node, false);
        visit::visit_stmt(self, node);
        self.restore(state);
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if let Err(error) = self.module(node) {
            self.error = Some(error);
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if !self.inclusion.normal || self.inclusion.root.scope.is_some() {
            return;
        }
        if node.path.is_ident("include") || opaque_gating(node.tokens.clone()) {
            self.error = Some(format!(
                "{}: opaque macro contains conditional ownership or source inclusion; expose this source as ordinary Rust modules/nodes",
                node.span().start().line
            ));
        }
    }
}

pub(super) fn index(
    root: &Path,
    sources: &BTreeMap<String, String>,
    roots: Vec<Root>,
) -> Result<SourceIndex> {
    let mut pending: Vec<_> = roots
        .into_iter()
        .map(|r| Inclusion {
            directory: r.path.parent().unwrap_or(root).to_path_buf(),
            attribute_directory: r.path.parent().unwrap_or(root).to_path_buf(),
            root: r,
            normal: true,
            test: true,
            ancestry: Vec::new(),
        })
        .collect();
    let mut files: BTreeMap<String, IndexedFile> = BTreeMap::new();
    while let Some(mut inclusion) = pending.pop() {
        let path = inclusion
            .root
            .path
            .canonicalize()
            .map_err(|e| format!("{}: {e}", inclusion.root.path.display()))?;
        if inclusion.ancestry.contains(&path) {
            return Err(format!("module cycle at {}", path.display()));
        }
        inclusion.ancestry.push(path.clone());
        let key = repo_path(root, &path)?;
        let source = sources
            .get(&key)
            .ok_or_else(|| format!("module missing from source snapshot: {key}"))?;
        let parsed = syn::parse_file(source).map_err(|e| format!("{key}: {e}"))?;
        let mut spans = Vec::new();
        token_spans(
            source.parse::<TokenStream>().map_err(|e| e.to_string())?,
            &mut spans,
        );
        let commas = spans
            .windows(2)
            .filter_map(|pair| pair[1].2.then_some((pair[0].1, pair[1].1)))
            .collect();
        let mut visitor = Classifier {
            inclusion,
            commas,
            ranges: Vec::new(),
            ignored: Vec::new(),
            pending: Vec::new(),
            error: None,
        };
        let whole = source_range(source);
        if let Some(reason) = &visitor.inclusion.root.scope {
            visitor.ranges.push(ExcludedRange {
                reason: reason.clone(),
                owner: visitor.inclusion.root.owner.clone(),
                ..whole.clone()
            });
        }
        if !visitor.inclusion.normal {
            visitor.ranges.push(ExcludedRange {
                reason: if visitor.inclusion.test {
                    "inherited test module"
                } else {
                    "inactive module"
                }
                .into(),
                owner: visitor.inclusion.root.owner.clone(),
                ..whole.clone()
            });
        }
        visitor.enter(&parsed.attrs, whole.start, whole.end);
        for item in &parsed.items {
            visitor.visit_item(item);
        }
        if let Some(error) = visitor.error {
            return Err(format!("{key}: {error}"));
        }
        pending.extend(visitor.pending);
        let file = files.entry(key).or_default();
        file.source = source.clone();
        file.names.insert(visitor.inclusion.root.owner);
        file.owners.push(ParsedOwner {
            ranges: visitor.ranges,
            ignored: visitor.ignored,
        });
    }
    files
        .into_iter()
        .map(|(path, file)| project(file).map(|policy| (path, policy)))
        .collect()
}

fn source_range(source: &str) -> ExcludedRange {
    let (line, column) = source.chars().fold((1, 0), |(line, col), c| {
        if c == '\n' {
            (line + 1, 0)
        } else {
            (line, col + 1)
        }
    });
    ExcludedRange {
        start: Position { line: 1, column: 0 },
        end: Position { line, column },
        reason: String::new(),
        owner: String::new(),
    }
}

fn positions(span: Span) -> (Position, Position) {
    let start = span.start();
    let end = span.end();
    (
        Position {
            line: start.line,
            column: start.column,
        },
        Position {
            line: end.line,
            column: end.column,
        },
    )
}

fn token_spans(tokens: TokenStream, spans: &mut Vec<(Position, Position, bool)>) {
    for token in tokens {
        if let TokenTree::Group(group) = token {
            if group.delimiter() != Delimiter::None {
                let (start, end) = positions(group.span_open());
                spans.push((start, end, false));
            }
            token_spans(group.stream(), spans);
            if group.delimiter() != Delimiter::None {
                let (start, end) = positions(group.span_close());
                spans.push((start, end, false));
            }
        } else {
            let (start, end) = positions(token.span());
            spans.push((
                start,
                end,
                matches!(token, TokenTree::Punct(p) if p.as_char() == ','),
            ));
        }
    }
}

fn project(file: IndexedFile) -> Result<FilePolicy> {
    let mut tokens = Vec::new();
    token_spans(
        file.source
            .parse::<TokenStream>()
            .map_err(|e| e.to_string())?,
        &mut tokens,
    );
    let mut line_flags: BTreeMap<u32, (bool, bool)> = BTreeMap::new();
    for (start, end, _) in tokens {
        let mut production = false;
        let mut excluded = false;
        for owner in &file.owners {
            if owner.ignored.iter().any(|r| r.contains(start, end)) {
                continue;
            }
            if owner.ranges.iter().any(|r| r.contains(start, end)) {
                excluded = true;
            } else {
                production = true;
            }
        }
        // Physical tokens with any production owner always remain production.
        let last = if end.column == 0 {
            end.line.saturating_sub(1)
        } else {
            end.line
        };
        for line in start.line..=last {
            let flags = line_flags.entry(line as u32).or_default();
            flags.0 |= production;
            flags.1 |= excluded && !production;
        }
    }
    let whole = source_range(&file.source);
    let all_excluded = file
        .owners
        .iter()
        .all(|o| o.ranges.iter().any(|r| r.contains(whole.start, whole.end)));
    let mut exclusions: Vec<_> = file.owners.iter().flat_map(|o| o.ranges.clone()).collect();
    exclusions.sort();
    exclusions.dedup();
    let line_count = file.source.lines().count() as u32;
    // LLVM may attribute blank/comment-only lines to an enclosing region.
    for line in 1..=line_count {
        if line_flags.contains_key(&line) {
            continue;
        }
        let point = Position {
            line: line as usize,
            column: 0,
        };
        let excluded = file
            .owners
            .iter()
            .all(|o| o.ranges.iter().any(|r| r.start <= point && point < r.end));
        if excluded {
            line_flags.insert(line, (false, true));
        }
    }
    Ok(FilePolicy {
        digest: digest(&file.source),
        line_count,
        owners: file.names,
        whole_file: all_excluded.then(|| {
            exclusions
                .iter()
                .find(|r| r.contains(whole.start, whole.end))
                .map(|r| r.reason.clone())
                .unwrap_or_default()
        }),
        exclusions,
        lines: line_flags
            .into_iter()
            .filter_map(|(line, (production, excluded))| {
                excluded.then_some((
                    line,
                    if production {
                        LinePolicy::Mixed
                    } else {
                        LinePolicy::Excluded
                    },
                ))
            })
            .collect(),
    })
}

fn opaque_gating(tokens: TokenStream) -> bool {
    let tokens: Vec<_> = tokens.into_iter().collect();
    for (i, token) in tokens.iter().enumerate() {
        if let TokenTree::Ident(ident) = token
            && ident == "include"
            && matches!(tokens.get(i + 1), Some(TokenTree::Punct(p)) if p.as_char() == '!')
        {
            return true;
        }
        if let TokenTree::Group(group) = token {
            if group.delimiter() == Delimiter::Bracket
                && i > 0
                && matches!(&tokens[i - 1], TokenTree::Punct(p) if p.as_char() == '#')
                && let Some(TokenTree::Ident(name)) = group.stream().into_iter().next()
                && matches!(
                    name.to_string().as_str(),
                    "cfg" | "cfg_attr" | "test" | "path"
                )
            {
                return true;
            }
            if opaque_gating(group.stream()) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn classify(source: &str) -> FilePolicy {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("lib.rs"), source).unwrap();
        let root = Root {
            path: dir.path().join("lib.rs"),
            owner: "lib".into(),
            scope: None,
            configuration: Configuration::default(),
        };
        index(
            dir.path(),
            &BTreeMap::from([("lib.rs".into(), source.into())]),
            vec![root],
        )
        .unwrap()
        .remove("lib.rs")
        .unwrap()
    }

    /// Purpose: Keep production after conditional fields, statements, arguments and patterns.
    /// Expected: Exact test-owned lines disappear while adjacent production remains eligible.
    #[test]
    fn embedded_nodes_have_bounded_ownership() {
        let source = "struct S {\n#[cfg(test)]\nhook: usize,\nvalue: usize,\n}\nfn f(\n#[cfg(test)] hook: usize,\nvalue: usize,\n) {\n#[cfg(test)]\nlet hook = 1;\nlet s = S {\n#[cfg(test)]\nhook: hook,\nvalue,\n};\nlet S {\n#[cfg(test)]\nhook,\nvalue,\n} = s;\n#[cfg(test)]\nf(1, 2);\nconsume(value);\n}\n";
        let policy = classify(source);
        for line in [3, 7, 11, 14, 19, 23] {
            assert_eq!(
                policy.lines.get(&line),
                Some(&LinePolicy::Excluded),
                "line {line}"
            );
        }
        for line in [4, 8, 12, 15, 16, 20, 21, 24, 25] {
            assert!(!policy.lines.contains_key(&line), "production line {line}");
        }
    }

    /// Purpose: Distinguish inclusion-changing attributes from harmless references to test.
    /// Expected: Nested cfg_attr and test attributes exclude tests; dead-code expectations retain helpers.
    #[test]
    fn conditional_attribute_semantics() {
        let policy = classify(
            "#[cfg_attr(not(test), expect(dead_code))]\nfn helper() {}\n#[cfg_attr(all(), cfg_attr(any(test), test))]\nfn conditional() {}\n#[test]\nfn standalone() {}\n#[cfg(all(test, not(windows)))]\nmod tests { fn helper() {} }\n",
        );
        assert!(!policy.lines.contains_key(&2));
        // cfg_attr(test, test) leaves the function available in production.
        assert!(!policy.lines.contains_key(&4));
        assert_eq!(policy.lines.get(&6), Some(&LinePolicy::Excluded));
        assert_eq!(policy.lines.get(&8), Some(&LinePolicy::Excluded));
        assert!(
            Configuration::default()
                .evaluate(&syn::parse_quote!(any(test, mystery)), true)
                .is_err()
        );
    }

    /// Purpose: Preserve scalar-column boundaries with Unicode, comments, CRLF and raw strings.
    /// Expected: Mixed executable lines are marked ambiguous and strings do not create false scopes.
    #[test]
    fn token_boundaries_and_mixed_lines() {
        let policy = classify(
            "fn f() { let café = r#\"} #[cfg(test)]\"#;\r\n#[cfg(test)]\r\nconsume(café); // }\r\nlet café = 0; #[cfg(test)] consume(café);\r\n}\r\n",
        );
        assert!(!policy.lines.contains_key(&1));
        assert_eq!(policy.lines.get(&3), Some(&LinePolicy::Excluded));
        assert_eq!(policy.lines.get(&4), Some(&LinePolicy::Mixed));
        assert!(
            policy
                .exclusions
                .iter()
                .any(|r| r.start.line == 4 && r.start.column == 14 && r.end.column == 41)
        );
    }

    /// Purpose: Resolve inherited external modules without losing shared production ownership.
    /// Expected: A shared file stays eligible and an exclusively test-owned descendant is excluded.
    #[test]
    fn external_and_shared_modules() {
        let dir = tempfile::tempdir().unwrap();
        let sources = BTreeMap::from([
            ("lib.rs".into(), "#[cfg(test)]\n#[path = \"shared.rs\"] mod tests;\n#[path = \"shared.rs\"] mod production;\n#[cfg(test)] mod only;\n".into()),
            ("shared.rs".into(), "pub fn shared() {}\n".into()),
            ("only.rs".into(), "pub fn helper() {}\n".into()),
        ]);
        for (name, source) in &sources {
            fs::write(dir.path().join(name), source).unwrap();
        }
        let root = Root {
            path: dir.path().join("lib.rs"),
            owner: "lib".into(),
            scope: None,
            configuration: Configuration::default(),
        };
        let index = index(dir.path(), &sources, vec![root]).unwrap();
        assert!(index["shared.rs"].lines.is_empty());
        assert_eq!(
            index["only.rs"].whole_file.as_deref(),
            Some("inherited test module")
        );
    }

    /// Purpose: Reject opaque conditional source while allowing ordinary macro attribution.
    /// Expected: Production cfg/include tokens fail inspection and test-owned macros remain excludable.
    #[test]
    fn opaque_macro_policy() {
        assert!(opaque_gating("{ #[cfg(test)] fn f() {} }".parse().unwrap()));
        assert!(opaque_gating("include!(\"file.rs\")".parse().unwrap()));
        assert!(!opaque_gating(
            "println!(\"#[cfg(test)]\")".parse().unwrap()
        ));
        let policy = classify(
            "macro_rules! ordinary { () => { fn f() {} } }\n#[cfg(test)]\nmod tests { macro_rules! hidden { () => { #[cfg(test)] fn f() {} } } }\n",
        );
        assert!(!policy.lines.contains_key(&1));
        assert_eq!(policy.lines.get(&3), Some(&LinePolicy::Excluded));
    }

    /// Purpose: Attribute separators to their conditional arguments even across comments and newlines.
    /// Expected: Test-only call arguments and their commas are excluded without hiding the following argument.
    #[test]
    fn argument_separators_and_inner_attributes() {
        let policy = classify(
            "fn f() {\ncall(\n#[cfg(test)]\nhook(),\n#[cfg(test)]\nother() // trivia\n,\nproduction(),\n);\n}\nmod tests {\n#![cfg(test)]\nfn helper() {}\n}\n",
        );
        for line in [4, 6, 7, 11, 13, 14] {
            assert_eq!(
                policy.lines.get(&line),
                Some(&LinePolicy::Excluded),
                "line {line}"
            );
        }
        for line in [1, 2, 8, 9, 10] {
            assert!(!policy.lines.contains_key(&line), "line {line}");
        }
        let file = classify("#![cfg_attr(not(test), cfg(any()))]\nfn helper() {}\n");
        assert_eq!(file.whole_file.as_deref(), Some("test cfg"));
    }

    /// Purpose: Resolve conditional path attributes and inherited ownership according to Rust module rules.
    /// Expected: Explicit-path descendants use their file directory, ordinary modules use their stem, and shared production survives.
    #[test]
    fn path_attribute_and_file_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let sources: BTreeMap<String, String> = [
            ("lib.rs", "mod regular;\n#[cfg(test)]\n#[path = \"support/renamed.rs\"] mod tests;\n#[cfg_attr(test, path = \"test.rs\")]\n#[cfg_attr(not(test), path = \"normal.rs\")]\nmod selected;\n"),
            ("regular.rs", "#[path = \"shared.rs\"] mod direct;\nmod inline { #[path = \"leaf.rs\"] mod child; }\n"),
            ("regular/inline/leaf.rs", "pub fn leaf() {}\n"),
            ("shared.rs", "pub fn shared() {}\n"),
            ("support/renamed.rs", "mod child;\n#[path = \"../shared.rs\"] mod shared;\n"),
            ("support/child.rs", "pub fn helper() {}\n"),
            ("test.rs", "pub fn harness() {}\n"),
            ("normal.rs", "pub fn production() {}\n"),
        ].into_iter().map(|(p, s)| (p.into(), s.into())).collect();
        for (name, source) in &sources {
            let path = dir.path().join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, source).unwrap();
        }
        let root = Root {
            path: dir.path().join("lib.rs"),
            owner: "lib".into(),
            scope: None,
            configuration: Configuration::default(),
        };
        let policies = index(dir.path(), &sources, vec![root]).unwrap();
        assert!(policies["shared.rs"].lines.is_empty());
        assert!(policies["regular/inline/leaf.rs"].lines.is_empty());
        assert!(policies["normal.rs"].lines.is_empty());
        assert!(policies["support/child.rs"].whole_file.is_some());
        assert!(policies["test.rs"].whole_file.is_some());
    }

    /// Purpose: Evaluate fixed-build feature and target predicates without guessing custom configuration.
    /// Expected: Active production configurations survive, test-only cfg_attr is excluded, and unknown predicates fail.
    #[test]
    fn feature_target_and_nested_cfg_semantics() {
        let config = Configuration {
            predicates: BTreeSet::from([
                "unix".into(),
                "target_arch=\"aarch64\"".into(),
                "coverage".into(),
            ]),
            features: BTreeSet::from(["iouring".into()]),
            declared_features: BTreeSet::from(["iouring".into(), "libaio".into()]),
        };
        assert!(
            config
                .evaluate(
                    &syn::parse_quote!(all(
                        feature = "iouring",
                        target_arch = "aarch64",
                        not(windows),
                        coverage
                    )),
                    false
                )
                .unwrap()
        );
        assert!(
            !config
                .evaluate(&syn::parse_quote!(any(feature = "libaio", windows)), true)
                .unwrap()
        );
        assert!(
            config
                .evaluate(&syn::parse_quote!(feature = "unknown"), false)
                .is_err()
        );
        let policy = classify(
            "#[cfg_attr(not(test), cfg(any()))]\nfn test_only() {}\n#[cfg(not(test))]\nfn production() {}\n",
        );
        assert_eq!(policy.lines.get(&2), Some(&LinePolicy::Excluded));
        assert!(!policy.lines.contains_key(&4));
    }
}
