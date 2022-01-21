//! This module defines the main algebraic structs used in query plan optimization.
//!
//! The source of the initial plan is an AST directly parsed from SQL, so each plan
//! node should be able to constructed like "raw" SQL format.
//! Identification/normalization is performed at first to convert textual identifiers
//! and expressions to typed/numbered structs for space/processing efficiency, also
//! with schema validated.
//!
//! Each table/column is lookuped from catalog and assigned a unique id.
use smallvec::{smallvec, SmallVec};
use smol_str::SmolStr;
use xngin_catalog::{SchemaID, TableID};
use xngin_expr::{Expr, Pred, QueryID, Setq};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    Proj,
    Filt,
    Aggr,
    Join,
    Sort,
    Limit,
    Row,
    Subquery,
    Table,
    Setop,
}

/// Op stands for logical operator.
/// This is the general enum containing all nodes of logical plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Projection node.
    Proj(Proj),
    /// Filter node.
    Filt(Filt),
    /// Aggregation node.
    Aggr(Box<Aggr>),
    /// Join node.
    Join(Box<Join>),
    /// Sort node.
    Sort(Sort),
    /// Limit node.
    Limit(Limit),
    /// Row represents a single select without source table. e.g. "SELECT 1"
    Row(Vec<(Expr, SmolStr)>),
    /// Subquery node represents a single row, a concrete table or
    /// a sub-tree containing one or more operators.
    Subquery(QueryID),
    /// Table node.
    Table(SchemaID, TableID),
    /// Set operations include union, except, intersect.
    Setop(Setop),
}

impl Op {
    #[inline]
    pub fn kind(&self) -> OpKind {
        match self {
            Op::Proj(_) => OpKind::Proj,
            Op::Filt(_) => OpKind::Filt,
            Op::Aggr(_) => OpKind::Aggr,
            Op::Join(_) => OpKind::Join,
            Op::Sort(_) => OpKind::Sort,
            Op::Limit(_) => OpKind::Limit,
            Op::Row(_) => OpKind::Row,
            Op::Subquery(_) => OpKind::Subquery,
            Op::Table(..) => OpKind::Table,
            Op::Setop(_) => OpKind::Setop,
        }
    }

    #[inline]
    pub fn proj(cols: Vec<(Expr, SmolStr)>, q: Setq, source: Op) -> Self {
        Op::Proj(Proj {
            cols,
            q,
            source: Box::new(source),
        })
    }

    #[inline]
    pub fn filt(pred: Expr, source: Op) -> Self {
        Op::Filt(Filt {
            pred,
            source: Box::new(source),
        })
    }

    #[inline]
    pub fn sort(items: Vec<SortItem>, source: Op) -> Self {
        Op::Sort(Sort {
            items,
            limit: None,
            source: Box::new(source),
        })
    }

    #[inline]
    pub fn join(join: Join) -> Self {
        Op::Join(Box::new(join))
    }

    #[inline]
    pub fn aggr(groups: Vec<Expr>, source: Op) -> Self {
        Op::Aggr(Box::new(Aggr {
            groups,
            filt: Expr::pred(Pred::True),
            proj: vec![],
            source,
        }))
    }

    #[inline]
    pub fn limit(start: u64, end: u64, source: Op) -> Self {
        Op::Limit(Limit {
            start,
            end,
            source: Box::new(source),
        })
    }

    #[inline]
    pub fn row(row: Vec<(Expr, SmolStr)>) -> Self {
        Op::Row(row)
    }

    #[inline]
    pub fn table(schema_id: SchemaID, table_id: TableID) -> Self {
        Op::Table(schema_id, table_id)
    }

    #[inline]
    pub fn subquery(query_id: QueryID) -> Self {
        Op::Subquery(query_id)
    }

    #[inline]
    pub fn setop(kind: SetopKind, q: Setq, sources: Vec<Op>) -> Self {
        Op::Setop(Setop { kind, q, sources })
    }

    #[inline]
    pub fn cross_join(tables: Vec<JoinOp>) -> Self {
        Op::Join(Box::new(Join::Cross(tables)))
    }

    /// Returns children under current operator until row/table/join/subquery
    #[inline]
    pub fn children(&self) -> SmallVec<[&Op; 2]> {
        match self {
            Op::Proj(proj) => smallvec![proj.source.as_ref()],
            Op::Filt(filt) => smallvec![filt.source.as_ref()],
            Op::Aggr(aggr) => smallvec![&aggr.source],
            Op::Sort(sort) => smallvec![sort.source.as_ref()],
            Op::Limit(limit) => smallvec![limit.source.as_ref()],
            Op::Join(join) => match join.as_ref() {
                Join::Cross(jos) => jos.iter().collect(),
                Join::Qualified(QualifiedJoin { left, right, .. })
                | Join::Dependent(DependentJoin { left, right, .. }) => smallvec![left, right],
            },
            Op::Setop(set) => set.sources.iter().collect(),
            Op::Subquery(_) | Op::Row(_) | Op::Table(..) => smallvec![],
        }
    }

    #[inline]
    pub fn children_mut(&mut self) -> SmallVec<[&mut Op; 2]> {
        match self {
            Op::Proj(proj) => smallvec![proj.source.as_mut()],
            Op::Filt(filt) => smallvec![filt.source.as_mut()],
            Op::Aggr(aggr) => smallvec![&mut aggr.source],
            Op::Sort(sort) => smallvec![sort.source.as_mut()],
            Op::Limit(limit) => smallvec![limit.source.as_mut()],
            Op::Join(join) => match join.as_mut() {
                Join::Cross(jos) => jos.iter_mut().collect(),
                Join::Qualified(QualifiedJoin { left, right, .. })
                | Join::Dependent(DependentJoin { left, right, .. }) => smallvec![left, right],
            },
            Op::Setop(set) => set.sources.iter_mut().collect(),
            Op::Subquery(_) | Op::Row(_) | Op::Table(..) => smallvec![],
        }
    }

    #[inline]
    pub fn exprs(&self) -> SmallVec<[&Expr; 2]> {
        match self {
            Op::Proj(proj) => proj.cols.iter().map(|(e, _)| e).collect(),
            Op::Filt(filt) => smallvec![&filt.pred],
            Op::Aggr(aggr) => aggr
                .groups
                .iter()
                .chain(std::iter::once(&aggr.filt))
                .chain(aggr.proj.iter().map(|(e, _)| e))
                .collect(),
            Op::Sort(sort) => sort.items.iter().map(|si| &si.expr).collect(),
            Op::Limit(_) | Op::Subquery(_) | Op::Table(..) | Op::Setop(_) => smallvec![],
            Op::Join(j) => match j.as_ref() {
                Join::Cross(_) => smallvec![],
                Join::Qualified(QualifiedJoin { cond, .. })
                | Join::Dependent(DependentJoin { cond, .. }) => smallvec![cond],
            },
            Op::Row(row) => row.iter().map(|(e, _)| e).collect(),
        }
    }

    #[inline]
    pub fn exprs_mut(&mut self) -> SmallVec<[&mut Expr; 2]> {
        match self {
            Op::Proj(proj) => proj.cols.iter_mut().map(|(e, _)| e).collect(),
            Op::Filt(filt) => smallvec![&mut filt.pred],
            Op::Aggr(aggr) => aggr
                .groups
                .iter_mut()
                .chain(std::iter::once(&mut aggr.filt))
                .chain(aggr.proj.iter_mut().map(|(e, _)| e))
                .collect(),
            Op::Sort(sort) => sort.items.iter_mut().map(|si| &mut si.expr).collect(),
            Op::Limit(_) | Op::Subquery(_) | Op::Table(..) | Op::Setop(_) => smallvec![],
            Op::Join(j) => match j.as_mut() {
                Join::Cross(_) => smallvec![],
                Join::Qualified(QualifiedJoin { cond, .. })
                | Join::Dependent(DependentJoin { cond, .. }) => smallvec![cond],
            },
            Op::Row(row) => row.iter_mut().map(|(e, _)| e).collect(),
        }
    }

    pub fn walk<V: OpVisitor>(&self, visitor: &mut V) -> bool {
        if !visitor.enter(self) {
            return false;
        }
        for c in self.children() {
            if !c.walk(visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }

    pub fn walk_mut<V: OpMutVisitor>(&mut self, visitor: &mut V) -> bool {
        if !visitor.enter(self) {
            return false;
        }
        for c in self.children_mut() {
            if !c.walk_mut(visitor) {
                return false;
            }
        }
        visitor.leave(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proj {
    pub cols: Vec<(Expr, SmolStr)>,
    pub q: Setq,
    pub source: Box<Op>,
}

/// Filter node.
///
/// Filter result with given predicates.
/// The normal filter won't include projection, but here we
/// add proj to allow combine them into one node.
/// Actually all Scan node will be converted to Filter
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filt {
    pub pred: Expr,
    pub source: Box<Op>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrKind {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggr {
    pub groups: Vec<Expr>,
    pub filt: Expr,
    pub proj: Vec<(Expr, SmolStr)>,
    pub source: Op,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrProjKind {
    Group,
    Func,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    AntiSemi,
    Mark,
    Single,
}

impl JoinKind {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            JoinKind::Inner => "inner",
            JoinKind::Left => "left",
            JoinKind::Right => "right",
            JoinKind::Full => "full",
            JoinKind::Semi => "semi",
            JoinKind::AntiSemi => "antisemi",
            JoinKind::Mark => "mark",
            JoinKind::Single => "single",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Join {
    Cross(Vec<JoinOp>),
    /// All natural join are converted to cross join or qualified join.
    Qualified(QualifiedJoin),
    Dependent(DependentJoin),
}

impl Join {
    #[inline]
    pub fn qualified(kind: JoinKind, left: JoinOp, right: JoinOp, cond: Expr) -> Self {
        Join::Qualified(QualifiedJoin {
            kind,
            left,
            right,
            cond,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NaturalJoin {
    pub left: JoinOp,
    pub right: JoinOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedJoin {
    pub kind: JoinKind,
    pub left: JoinOp,
    pub right: JoinOp,
    pub cond: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependentJoin {
    pub kind: JoinKind,
    pub left: JoinOp,
    pub right: JoinOp,
    pub cond: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Setop {
    pub kind: SetopKind,
    pub q: Setq,
    /// Sources of Setop are always subqueries.
    pub sources: Vec<Op>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetopKind {
    Union,
    Except,
    Intersect,
}

impl SetopKind {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            SetopKind::Union => "union",
            SetopKind::Except => "except",
            SetopKind::Intersect => "intersect",
        }
    }
}

/// JoinOp is subset of Op, which only includes
/// Subquery and Join as its variants.
pub type JoinOp = Op;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sort {
    pub items: Vec<SortItem>,
    pub limit: Option<u64>,
    pub source: Box<Op>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortItem {
    pub expr: Expr,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    pub start: u64,
    pub end: u64,
    pub source: Box<Op>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Col {
    pub expr: Expr,
    pub alias: Option<SmolStr>,
}

pub trait OpVisitor {
    /// Returns true if continue
    fn enter(&mut self, op: &Op) -> bool;

    /// Returns true if continue
    fn leave(&mut self, _op: &Op) -> bool {
        true
    }
}

pub trait OpMutVisitor {
    /// Returns true if continue
    fn enter(&mut self, op: &mut Op) -> bool;

    /// Returns true if continue
    fn leave(&mut self, op: &mut Op) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_size_of_logical_nodes() {
        println!("size of Op {}", std::mem::size_of::<Op>());
        println!("size of Proj {}", std::mem::size_of::<Proj>());
        println!("size of Filt {}", std::mem::size_of::<Filt>());
        println!("size of Pred {}", std::mem::size_of::<Pred>());
        println!("size of Join {}", std::mem::size_of::<Join>());
        println!("size of JoinKind {}", std::mem::size_of::<JoinKind>());
        println!("size of Aggr {}", std::mem::size_of::<Aggr>());
    }
}
