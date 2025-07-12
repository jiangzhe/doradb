//! This module defines the main algebraic structs used in query plan optimization.
//!
//! The source of the initial plan is an AST directly parsed from SQL, so each plan
//! node should be able to constructed like "raw" SQL format.
//! Identification/normalization is performed at first to convert textual identifiers
//! and expressions to typed/numbered structs for space/processing efficiency, also
//! with schema validated.
//!
//! Each table/column is lookuped from catalog and assigned a unique id.
use crate::error::Error;
use crate::join::{Join, JoinGraph, JoinKind, JoinOp, QualifiedJoin};
use crate::lgc::col::ProjCol;
use crate::lgc::setop::{Setop, SetopKind, SubqOp};
use doradb_catalog::{SchemaID, TableID};
use doradb_expr::controlflow::ControlFlow;
use doradb_expr::{Effect, ExprKind, GlobalID, QueryID, Setq, INVALID_GLOBAL_ID};
use semistr::SemiStr;
use smallvec::{smallvec, SmallVec};
use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct Op {
    pub id: GlobalID,
    pub kind: OpKind,
}

impl Op {
    #[inline]
    pub fn new(kind: OpKind) -> Self {
        Op {
            id: INVALID_GLOBAL_ID,
            kind,
        }
    }

    #[inline]
    pub fn empty() -> Self {
        Op {
            id: INVALID_GLOBAL_ID,
            kind: OpKind::Empty,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self.kind, OpKind::Empty)
    }

    #[inline]
    pub fn ty(&self) -> OpTy {
        self.kind.ty()
    }

    /// Returns output columns of current operator.
    ///
    /// If output fix is done, just returns output field.
    /// Otherwise, traverse down the operator tree and find
    /// the first operator that can output columns, such
    /// as Proj, Aggr, Row, etc.
    #[inline]
    pub fn out_cols(&self) -> Option<&[ProjCol]> {
        let mut op = self;
        loop {
            match &op.kind {
                OpKind::Aggr(aggr) => return Some(&aggr.proj),
                OpKind::Proj { cols, .. } => return Some(&cols[..]),
                OpKind::Row(row) => return row.as_ref().map(|cs| &cs[..]),
                OpKind::Sort { input, .. } => op = input.as_ref(),
                OpKind::Limit { input, .. } => op = input.as_ref(),
                OpKind::Filt { input, .. } => op = input.as_ref(),
                OpKind::Scan(scan) => return Some(&scan.cols),
                OpKind::Setop(setop) => return Some(&setop.cols),
                OpKind::Query(_)
                | OpKind::Join(_)
                | OpKind::JoinGraph(_)
                | OpKind::Attach(..)
                | OpKind::Empty => return None,
            }
        }
    }

    /// Returns mutable output columns of current operator.
    #[inline]
    pub fn out_cols_mut(&mut self) -> Option<&mut Vec<ProjCol>> {
        let mut op = self;
        loop {
            match &mut op.kind {
                OpKind::Aggr(aggr) => return Some(&mut aggr.proj),
                OpKind::Proj { cols, .. } => return Some(cols),
                OpKind::Row(row) => return row.as_mut(),
                OpKind::Sort { input, .. } => op = input.as_mut(),
                OpKind::Limit { input, .. } => op = input.as_mut(),
                OpKind::Filt { input, .. } => op = input.as_mut(),
                OpKind::Setop(setop) => return Some(&mut setop.cols),
                OpKind::Scan(..)
                | OpKind::Query(_)
                | OpKind::Join(_)
                | OpKind::JoinGraph(_)
                | OpKind::Attach(..)
                | OpKind::Empty => return None,
            }
        }
    }

    #[inline]
    pub fn collect_qry_ids(&self, qry_ids: &mut HashSet<QueryID>) {
        struct Collect<'a>(&'a mut HashSet<QueryID>);
        impl OpVisitor for Collect<'_> {
            type Cont = ();
            type Break = ();
            #[inline]
            fn enter(&mut self, op: &Op) -> ControlFlow<()> {
                if let OpKind::Query(qry_id) = op.kind {
                    self.0.insert(qry_id);
                }
                ControlFlow::Continue(())
            }
        }
        let mut c = Collect(qry_ids);
        let _ = self.walk(&mut c);
    }

    pub fn walk<V: OpVisitor>(&self, visitor: &mut V) -> ControlFlow<V::Break, V::Cont> {
        let mut eff = visitor.enter(self)?;
        for c in self.kind.inputs() {
            eff.merge(c.walk(visitor)?)
        }
        eff.merge(visitor.leave(self)?);
        ControlFlow::Continue(eff)
    }

    pub fn walk_mut<V: OpMutVisitor>(&mut self, visitor: &mut V) -> ControlFlow<V::Break, V::Cont> {
        let mut eff = visitor.enter(self)?;
        for c in self.kind.inputs_mut() {
            eff.merge(c.walk_mut(visitor)?)
        }
        eff.merge(visitor.leave(self)?);
        ControlFlow::Continue(eff)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpTy {
    Proj,
    Filt,
    Aggr,
    Join,
    JoinGraph,
    Sort,
    Limit,
    Attach,
    Row,
    Query,
    Scan,
    Setop,
    Empty,
}

/// Op stands for logical operator.
/// This is the general enum containing all nodes of logical plan.
#[derive(Debug, Clone, Default)]
pub enum OpKind {
    /// Project node.
    ///
    /// This node is used to build a logical plan.
    /// It's equivalent to "SELECT ..." clause in SQL statement.
    /// After plan optimization, this kind of node will be removed
    /// as we apply projection on each other nodes' output.
    Proj { cols: Vec<ProjCol>, input: Box<Op> },
    /// Filter node.
    ///
    /// It's equivalent to "WHERE ..." clause or "HAVING ..." clause in
    /// SQL statement.
    Filt { pred: Vec<ExprKind>, input: Box<Op> },
    /// Aggregation node.
    Aggr(Box<Aggr>),
    /// Join node.
    ///
    /// It's equivalent to "... JOIN ..." clause in SQL statement.
    Join(Box<Join>),
    /// This is a temporary node only existing in query optimization,
    /// especially for join reordering.
    /// Once it's done, it will be converted back to multiple Join nodes.
    JoinGraph(Box<JoinGraph>),
    /// Sort node.
    Sort {
        items: Vec<SortItem>,
        limit: Option<u64>,
        input: Box<Op>,
    },
    /// Limit node.
    Limit {
        start: u64,
        end: u64,
        input: Box<Op>,
    },
    /// Attach node.
    /// Attach a deferred scalar value into result set.
    /// This node is converted from a non-correlated scalar subquery.
    Attach(Box<Op>, QueryID),
    /// Row represents a single select without source table. e.g. "SELECT 1"
    Row(Option<Vec<ProjCol>>),
    /// Query node represents a single row, a concrete table or
    /// a sub-tree containing one or more operators.
    Query(QueryID),
    /// Table node.
    // Table(SchemaID, TableID),
    Scan(Box<TableScan>),
    /// Set operations include union, except, intersect.
    Setop(Box<Setop>),
    /// Empty represent a empty data set.
    /// It is used in place for special cases such as impossible predicate,
    /// limit 0 rows, etc.
    #[default]
    Empty,
}

impl OpKind {
    #[inline]
    pub fn ty(&self) -> OpTy {
        match self {
            OpKind::Proj { .. } => OpTy::Proj,
            OpKind::Filt { .. } => OpTy::Filt,
            OpKind::Aggr { .. } => OpTy::Aggr,
            OpKind::Join { .. } => OpTy::Join,
            OpKind::JoinGraph { .. } => OpTy::JoinGraph,
            OpKind::Sort { .. } => OpTy::Sort,
            OpKind::Limit { .. } => OpTy::Limit,
            OpKind::Attach { .. } => OpTy::Attach,
            OpKind::Row { .. } => OpTy::Row,
            OpKind::Query { .. } => OpTy::Query,
            OpKind::Scan { .. } => OpTy::Scan,
            OpKind::Setop { .. } => OpTy::Setop,
            OpKind::Empty => OpTy::Empty,
        }
    }

    #[inline]
    pub fn table(
        schema_id: SchemaID,
        schema: SemiStr,
        table_id: TableID,
        table: SemiStr,
        cols: Vec<ProjCol>,
    ) -> Self {
        OpKind::Scan(Box::new(TableScan {
            schema_id,
            schema,
            table_id,
            table,
            cols,
            filt: vec![],
        }))
    }

    #[inline]
    pub fn proj(cols: Vec<ProjCol>, input: Op) -> Self {
        OpKind::Proj {
            cols,
            input: Box::new(input),
        }
    }

    #[inline]
    pub fn filt(pred: Vec<ExprKind>, input: Op) -> Self {
        OpKind::Filt {
            pred,
            input: Box::new(input),
        }
    }

    #[inline]
    pub fn sort(items: Vec<SortItem>, input: Op) -> Self {
        OpKind::Sort {
            items,
            limit: None,
            input: Box::new(input),
        }
    }

    #[inline]
    pub fn join_graph(graph: JoinGraph) -> Self {
        OpKind::JoinGraph(Box::new(graph))
    }

    #[inline]
    pub fn aggr(groups: Vec<ExprKind>, input: Op) -> Self {
        OpKind::Aggr(Box::new(Aggr {
            groups,
            proj: vec![],
            input,
            filt: vec![],
        }))
    }

    #[inline]
    pub fn limit(start: u64, end: u64, input: Op) -> Self {
        OpKind::Limit {
            start,
            end,
            input: Box::new(input),
        }
    }

    #[inline]
    pub fn setop(
        kind: SetopKind,
        q: Setq,
        left: SubqOp,
        right: SubqOp,
        cols: Vec<ProjCol>,
    ) -> Self {
        OpKind::Setop(Box::new(Setop {
            kind,
            q,
            left,
            right,
            cols,
        }))
    }

    #[inline]
    pub fn cross_join(tables: Vec<JoinOp>) -> Self {
        OpKind::Join(Box::new(Join::Cross(tables)))
    }

    #[inline]
    pub fn qualified_join(
        kind: JoinKind,
        left: JoinOp,
        right: JoinOp,
        cond: Vec<ExprKind>,
        filt: Vec<ExprKind>,
    ) -> Self {
        OpKind::Join(Box::new(Join::Qualified(QualifiedJoin {
            kind,
            left,
            right,
            cond,
            filt,
        })))
    }

    /// Returns single source of the operator
    #[inline]
    pub fn input_mut(&mut self) -> Option<&mut Op> {
        match self {
            OpKind::Proj { input, .. } => Some(input),
            OpKind::Filt { input, .. } => Some(input),
            OpKind::Sort { input, .. } => Some(input),
            OpKind::Limit { input, .. } => Some(input),
            OpKind::Aggr(aggr) => Some(&mut aggr.input),
            OpKind::Join(_)
            | OpKind::JoinGraph(_)
            | OpKind::Setop(_)
            | OpKind::Attach(..)
            | OpKind::Row(_)
            | OpKind::Scan(..)
            | OpKind::Query(_)
            | OpKind::Empty => None,
        }
    }

    /// Returns children under current operator until row/table/join/subquery
    #[inline]
    pub fn inputs(&self) -> SmallVec<[&Op; 2]> {
        match self {
            OpKind::Proj { input, .. } => smallvec![input.as_ref()],
            OpKind::Filt { input, .. } => smallvec![input.as_ref()],
            OpKind::Aggr(aggr) => smallvec![&aggr.input],
            OpKind::Sort { input, .. } => smallvec![input.as_ref()],
            OpKind::Limit { input, .. } => smallvec![input.as_ref()],
            OpKind::Attach(c, _) => smallvec![c.as_ref()],
            OpKind::Join(join) => match join.as_ref() {
                Join::Cross(jos) => jos.iter().map(AsRef::as_ref).collect(),
                Join::Qualified(QualifiedJoin { left, right, .. }) => {
                    smallvec![left.as_ref(), right.as_ref()]
                }
            },
            OpKind::JoinGraph(graph) => graph.children(),
            OpKind::Setop(set) => {
                let Setop { left, right, .. } = set.as_ref();
                smallvec![left.as_ref(), right.as_ref()]
            }
            OpKind::Query(_) | OpKind::Row(_) | OpKind::Scan(..) | OpKind::Empty => smallvec![],
        }
    }

    #[inline]
    pub fn inputs_mut(&mut self) -> SmallVec<[&mut Op; 2]> {
        match self {
            OpKind::Proj { input, .. } => smallvec![input.as_mut()],
            OpKind::Filt { input, .. } => smallvec![input.as_mut()],
            OpKind::Aggr(aggr) => smallvec![&mut aggr.input],
            OpKind::Sort { input, .. } => smallvec![input.as_mut()],
            OpKind::Limit { input, .. } => smallvec![input.as_mut()],
            OpKind::Attach(c, _) => smallvec![c.as_mut()],
            OpKind::Join(join) => match join.as_mut() {
                Join::Cross(jos) => jos.iter_mut().map(AsMut::as_mut).collect(),
                Join::Qualified(QualifiedJoin { left, right, .. }) => {
                    smallvec![left.as_mut(), right.as_mut()]
                }
            },
            OpKind::JoinGraph(graph) => graph.children_mut(),
            OpKind::Setop(set) => {
                let Setop { left, right, .. } = set.as_mut();
                smallvec![left.as_mut(), right.as_mut()]
            }
            OpKind::Query(_) | OpKind::Row(_) | OpKind::Scan(..) | OpKind::Empty => smallvec![],
        }
    }

    #[inline]
    pub fn exprs(&self) -> SmallVec<[&ExprKind; 2]> {
        match self {
            OpKind::Proj { cols, .. } => cols.iter().map(|c| &c.expr).collect(),
            OpKind::Filt { pred, .. } => pred.iter().collect(),
            OpKind::Aggr(aggr) => aggr
                .groups
                .iter()
                .chain(aggr.proj.iter().map(|c| &c.expr))
                .chain(&aggr.filt)
                .collect(),
            OpKind::Sort { items, .. } => items.iter().map(|si| &si.expr).collect(),
            OpKind::Scan(scan) => scan
                .cols
                .iter()
                .map(|c| &c.expr)
                .chain(scan.filt.iter())
                .collect(),
            OpKind::Setop(setop) => setop.cols.iter().map(|c| &c.expr).collect(),
            OpKind::Limit { .. } | OpKind::Query(_) | OpKind::Empty | OpKind::Attach(..) => {
                smallvec![]
            }
            OpKind::Join(j) => match j.as_ref() {
                Join::Cross(_) => smallvec![],
                Join::Qualified(QualifiedJoin { cond, filt, .. }) => {
                    cond.iter().chain(filt.iter()).collect()
                }
            },
            OpKind::JoinGraph(graph) => graph.exprs().into_iter().collect(),
            OpKind::Row(row) => {
                if let Some(row) = row {
                    row.iter().map(|c| &c.expr).collect()
                } else {
                    smallvec![]
                }
            }
        }
    }

    #[inline]
    pub fn exprs_mut(&mut self) -> SmallVec<[&mut ExprKind; 2]> {
        match self {
            OpKind::Proj { cols, .. } => cols.iter_mut().map(|c| &mut c.expr).collect(),
            OpKind::Filt { pred, .. } => pred.iter_mut().collect(),
            OpKind::Aggr(aggr) => aggr
                .groups
                .iter_mut()
                .chain(aggr.proj.iter_mut().map(|c| &mut c.expr))
                .chain(&mut aggr.filt)
                .collect(),
            OpKind::Sort { items, .. } => items.iter_mut().map(|si| &mut si.expr).collect(),
            OpKind::Scan(scan) => scan
                .cols
                .iter_mut()
                .map(|c| &mut c.expr)
                .chain(scan.filt.iter_mut())
                .collect(),
            OpKind::Setop(setop) => setop.cols.iter_mut().map(|c| &mut c.expr).collect(),
            OpKind::Limit { .. } | OpKind::Query(_) | OpKind::Empty | OpKind::Attach(..) => {
                smallvec![]
            }
            OpKind::Join(j) => match j.as_mut() {
                Join::Cross(_) => smallvec![],
                Join::Qualified(QualifiedJoin { cond, filt, .. }) => {
                    cond.iter_mut().chain(filt.iter_mut()).collect()
                }
            },
            OpKind::JoinGraph(graph) => graph.exprs_mut().into_iter().collect(),
            OpKind::Row(row) => {
                if let Some(row) = row {
                    row.iter_mut().map(|c| &mut c.expr).collect()
                } else {
                    smallvec![]
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrKind {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

#[derive(Debug, Clone)]
pub struct Aggr {
    pub groups: Vec<ExprKind>,
    pub proj: Vec<ProjCol>,
    pub input: Op,
    // The filter applied after aggregation
    pub filt: Vec<ExprKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrProjKind {
    Group,
    Func,
}

#[derive(Debug, Clone)]
pub struct Apply {
    pub kind: ApplyKind,
    /// Free variables of subquery.
    /// If empty, subquery is non-correlated.
    /// Otherwise, correlated.
    pub vars: Vec<ExprKind>,
    /// Outer query.
    pub left: Op,
    /// inner subquery.
    /// The operator is always Op::Subquery.
    pub right: Op,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyKind {
    /// Right generate single value,
    /// Append it to left for each row.
    Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortItem {
    pub expr: ExprKind,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableScan {
    pub schema_id: SchemaID,
    pub schema: SemiStr,
    pub table_id: TableID,
    pub table: SemiStr,
    // pub qry: QueryID,
    pub cols: Vec<ProjCol>,
    // filter expression that can be pushed down to scan.
    pub filt: Vec<ExprKind>,
}

pub trait OpVisitor {
    type Cont: Effect;
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _op: &Op) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _op: &Op) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }
}

/// Helper function to generate a visitor to
/// traverse the operator tree in preorder.
pub fn preorder<F: FnMut(&Op)>(f: F) -> impl OpVisitor {
    struct Preorder<F>(F);
    impl<F: FnMut(&Op)> OpVisitor for Preorder<F> {
        type Cont = ();
        type Break = Error;
        fn enter(&mut self, op: &Op) -> ControlFlow<Error> {
            (self.0)(op);
            ControlFlow::Continue(())
        }
    }
    Preorder(f)
}

pub trait OpMutVisitor {
    type Cont: Effect;
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _op: &mut Op) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _op: &mut Op) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_logical_nodes() {
        use std::mem::size_of;
        println!("size of Op {}", size_of::<Op>());
        println!("size of Join {}", size_of::<Join>());
        println!("size of JoinKind {}", size_of::<JoinKind>());
        println!("size of Aggr {}", size_of::<Aggr>());
        println!("size of JoinGraph {}", size_of::<JoinGraph>());
        println!(
            "size of SmallVec<[QueryID;4]> is {}",
            size_of::<SmallVec<[QueryID; 4]>>()
        );
        println!(
            "size of SmallVec<[QueryID;5]> is {}",
            size_of::<SmallVec<[QueryID; 5]>>()
        );
    }
}
