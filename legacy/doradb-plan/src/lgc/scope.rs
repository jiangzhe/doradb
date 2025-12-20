use crate::lgc::alias::QueryAliases;
use doradb_expr::{ColIndex, ExprKind, GlobalID, QueryID};
use fnv::FnvHashSet;
use indexmap::IndexMap;
use semistr::SemiStr;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

// Scopes is stack-like environment for query blocks.
#[derive(Debug, Default)]
pub struct Scopes(Vec<Scope>);

impl Deref for Scopes {
    type Target = [Scope];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Scopes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Scopes {
    #[inline]
    pub fn curr_scope(&self) -> &Scope {
        self.0.last().unwrap()
    }

    #[inline]
    pub fn curr_scope_mut(&mut self) -> &mut Scope {
        self.0.last_mut().unwrap()
    }

    #[inline]
    pub fn push(&mut self, scope: Scope) {
        self.0.push(scope)
    }

    #[inline]
    pub fn pop(&mut self) -> Option<Scope> {
        self.0.pop()
    }
}

/// Scope represents the namespace of query block.
/// There are two alias tables:
/// 1. table aliases
/// 2. column aliases
#[derive(Debug, Clone, Default)]
pub struct Scope {
    /// CTE aliases will only contain row and subquery.
    pub cte_aliases: QueryAliases,
    /// Query aliases will only contain row and subqueries.
    /// The source is FROM clause.
    /// Row and derived table remain the same.
    /// Any table present in FROM clause, will be converted to
    /// a simple query automatically.
    /// The converted query has a Proj operator upon Table operator,
    /// and no other operator or query involved.
    pub query_aliases: QueryAliases,
    /// if set to true, unknown identifier can be passed to
    /// outer scope for search.
    pub transitive: bool,
    /// Correlated variables in current scope.
    pub cor_vars: FnvHashSet<(QueryID, ColIndex)>,
    /// Correlated columns in current scope.
    pub cor_cols: Vec<ExprKind>,
    /// intra column information.
    pub intra_cols: HashMap<GlobalID, ExprKind>,
}

impl Scope {
    #[inline]
    pub fn new(transitive: bool) -> Self {
        Scope {
            transitive,
            ..Default::default()
        }
    }

    #[inline]
    pub fn restrict_from_aliases(&self, aliases: &[SemiStr]) -> QueryAliases {
        let m: IndexMap<SemiStr, QueryID> = aliases
            .iter()
            .filter_map(|a| self.query_aliases.get(a).cloned().map(|p| (a.clone(), p)))
            .collect();
        QueryAliases::new(m)
    }
}
