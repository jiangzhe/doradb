use crate::controlflow::ControlFlow;
use crate::func::FuncKind;
use crate::id::{ColIndex, GlobalID, QueryID};
use crate::pred::{Pred, PredFuncKind};
use semistr::SemiStr;
use smallvec::{smallvec, SmallVec};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use xngin_catalog::TableID;
pub use xngin_datatype::{Const, ValidF64};
use xngin_datatype::{Date, Datetime, Decimal, Interval, PreciseType, Time, TimeUnit};

// #[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
// pub struct Expr {
//     pub kind: ExprKind,
//     pub ty: PreciseType,
// }

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExprKind {
    Const(Const),
    Col(Col),
    Aggf {
        kind: AggKind,
        q: Setq,
        arg: Box<ExprKind>,
    },
    Func {
        kind: FuncKind,
        args: Vec<ExprKind>,
    },
    Case {
        op: Option<Box<ExprKind>>,
        acts: Vec<(ExprKind, ExprKind)>,
        fallback: Option<Box<ExprKind>>,
    },
    Cast {
        arg: Box<ExprKind>,
        ty: PreciseType,
        implicit: bool,
    },
    Pred(Pred),
    Tuple(Vec<ExprKind>),
    /// Subquery that returns single value.
    Subq(SubqKind, QueryID),
    /// Attval represents single value returned by
    /// an attached plan.
    /// If multiple values returned, it throws runtime error.
    /// If no values returned, it use NULL by default.
    Attval(QueryID),
    /// Placeholder can represent any intermediate value
    /// generated in building phase. It should be
    /// resolved as a normal expression later.
    /// Placeholder is also used in optimization, such as
    /// predicate pushdown.
    Plhd(Plhd),
    /// Predefined function argument.
    Farg(Farg),
}

impl ExprKind {
    #[inline]
    pub fn const_null() -> Self {
        ExprKind::Const(Const::Null)
    }

    #[inline]
    pub fn const_bool(v: bool) -> Self {
        ExprKind::Const(Const::Bool(v))
    }

    #[inline]
    pub fn const_i64(i: i64) -> Self {
        ExprKind::Const(Const::I64(i))
    }

    #[inline]
    pub fn const_u64(u: u64) -> Self {
        ExprKind::Const(Const::U64(u))
    }

    /// Caller should ensure input is finite and is a number
    #[inline]
    pub fn const_f64(f: f64) -> Self {
        ExprKind::Const(Const::F64(ValidF64::new(f).unwrap()))
    }

    #[inline]
    pub fn const_decimal(d: Decimal) -> Self {
        ExprKind::Const(Const::Decimal(d))
    }

    #[inline]
    pub fn const_date(dt: Date) -> Self {
        ExprKind::Const(Const::Date(dt))
    }

    #[inline]
    pub fn const_time(tm: Time) -> Self {
        ExprKind::Const(Const::Time(tm))
    }

    #[inline]
    pub fn const_datetime(ts: Datetime) -> Self {
        ExprKind::Const(Const::Datetime(ts))
    }

    #[inline]
    pub fn const_interval(unit: TimeUnit, value: i32) -> Self {
        ExprKind::Const(Const::Interval(Interval { unit, value }))
    }

    #[inline]
    pub fn const_str(s: Arc<str>) -> Self {
        ExprKind::Const(Const::String(s))
    }

    #[inline]
    pub fn const_bytes(bs: Arc<[u8]>) -> Self {
        ExprKind::Const(Const::Bytes(bs))
    }

    #[inline]
    pub fn is_const(&self) -> bool {
        matches!(self, ExprKind::Const(_))
    }

    #[inline]
    pub fn is_col(&self, col: &Col) -> bool {
        match &self {
            ExprKind::Col(c) => c == col,
            _ => false,
        }
    }

    #[inline]
    pub fn count_asterisk() -> Self {
        ExprKind::Aggf {
            kind: AggKind::Count,
            q: Setq::All,
            arg: Box::new(ExprKind::const_i64(1)),
        }
    }

    #[inline]
    pub fn count(q: Setq, e: ExprKind) -> Self {
        ExprKind::Aggf {
            kind: AggKind::Count,
            q,
            arg: Box::new(e),
        }
    }

    #[inline]
    pub fn sum(q: Setq, e: ExprKind) -> Self {
        ExprKind::Aggf {
            kind: AggKind::Sum,
            q,
            arg: Box::new(e),
        }
    }

    #[inline]
    pub fn avg(q: Setq, e: ExprKind) -> Self {
        ExprKind::Aggf {
            kind: AggKind::Avg,
            q,
            arg: Box::new(e),
        }
    }

    #[inline]
    pub fn min(q: Setq, e: ExprKind) -> Self {
        ExprKind::Aggf {
            kind: AggKind::Min,
            q,
            arg: Box::new(e),
        }
    }

    #[inline]
    pub fn max(q: Setq, e: ExprKind) -> Self {
        ExprKind::Aggf {
            kind: AggKind::Max,
            q,
            arg: Box::new(e),
        }
    }

    #[inline]
    pub fn pred_not(e: ExprKind) -> Self {
        ExprKind::Pred(Pred::Not(Box::new(e)))
    }

    #[inline]
    pub fn pred_in_subq(lhs: ExprKind, subq: ExprKind) -> Self {
        ExprKind::Pred(Pred::InSubquery(Box::new(lhs), Box::new(subq)))
    }

    #[inline]
    pub fn pred_not_in_subq(lhs: ExprKind, subq: ExprKind) -> Self {
        ExprKind::Pred(Pred::NotInSubquery(Box::new(lhs), Box::new(subq)))
    }

    #[inline]
    pub fn pred_exists(subq: ExprKind) -> Self {
        ExprKind::Pred(Pred::Exists(Box::new(subq)))
    }

    #[inline]
    pub fn pred_not_exists(subq: ExprKind) -> Self {
        ExprKind::Pred(Pred::NotExists(Box::new(subq)))
    }

    #[inline]
    pub fn pred_func(kind: PredFuncKind, args: Vec<ExprKind>) -> Self {
        ExprKind::Pred(Pred::func(kind, args))
    }

    #[inline]
    pub fn pred_conj(mut exprs: Vec<ExprKind>) -> Self {
        assert!(!exprs.is_empty());
        if exprs.len() == 1 {
            exprs.pop().unwrap()
        } else {
            ExprKind::Pred(Pred::Conj(exprs))
        }
    }

    #[inline]
    pub fn into_conj(self) -> Vec<ExprKind> {
        match self {
            ExprKind::Pred(Pred::Conj(es)) => es,
            _ => vec![self],
        }
    }

    /// Construct a table column with table id, column index and precise type.
    /// This differs from other expressions, as the precise type is passed
    /// as input argument.
    #[inline]
    pub fn table_col(
        gid: GlobalID,
        table_id: TableID,
        idx: ColIndex,
        ty: PreciseType,
        col_name: SemiStr,
    ) -> Self {
        ExprKind::Col(Col {
            gid,
            kind: ColKind::Table(table_id, col_name, ty),
            idx,
        })
    }

    #[inline]
    pub fn query_col(gid: GlobalID, query_id: QueryID, idx: ColIndex) -> Self {
        ExprKind::Col(Col {
            gid,
            kind: ColKind::Query(query_id),
            idx,
        })
    }

    #[inline]
    pub fn correlated_col(gid: GlobalID, query_id: QueryID, idx: ColIndex) -> Self {
        ExprKind::Col(Col {
            gid,
            kind: ColKind::Correlated(query_id),
            idx,
        })
    }

    #[inline]
    pub fn intra_col(gid: GlobalID, child_idx: u8, col_idx: ColIndex) -> Self {
        ExprKind::Col(Col {
            gid,
            kind: ColKind::Intra(child_idx),
            idx: col_idx,
        })
    }

    #[inline]
    pub fn func(kind: FuncKind, args: Vec<ExprKind>) -> Self {
        debug_assert!({
            let (min, max) = kind.n_args();
            args.len() >= min && (max.is_none() || args.len() <= max.unwrap())
        });
        ExprKind::Func { kind, args }
    }

    #[inline]
    pub fn new_case(
        op: Option<Box<ExprKind>>,
        acts: Vec<(ExprKind, ExprKind)>,
        fallback: Option<Box<ExprKind>>,
    ) -> Self {
        ExprKind::Case { op, acts, fallback }
    }

    /// Cast a given expression to specific data type.
    #[inline]
    pub fn implicit_cast(arg: ExprKind, ty: PreciseType) -> Self {
        ExprKind::Cast {
            arg: Box::new(arg),
            implicit: true,
            ty,
        }
    }

    #[inline]
    pub fn ph_ident(uid: u32) -> Self {
        ExprKind::Plhd(Plhd::Ident(uid))
    }

    #[inline]
    pub fn ph_subquery(kind: SubqKind, uid: u32) -> Self {
        ExprKind::Plhd(Plhd::Subquery(kind, uid))
    }

    #[inline]
    pub fn farg_none() -> Self {
        ExprKind::Farg(Farg::None)
    }

    #[inline]
    pub fn farg(arg: Farg) -> Self {
        ExprKind::Farg(arg)
    }

    #[inline]
    pub fn tuple(exprs: Vec<ExprKind>) -> Self {
        ExprKind::Tuple(exprs)
    }

    #[inline]
    pub fn attval(qry_id: QueryID) -> Self {
        ExprKind::Attval(qry_id)
    }

    #[inline]
    pub fn subq(kind: SubqKind, qry_id: QueryID) -> Self {
        ExprKind::Subq(kind, qry_id)
    }

    #[inline]
    pub fn n_args(&self) -> usize {
        match &self {
            ExprKind::Const(_)
            | ExprKind::Col(..)
            | ExprKind::Plhd(_)
            | ExprKind::Subq(..)
            | ExprKind::Farg(_)
            | ExprKind::Attval(_) => 0,
            ExprKind::Aggf { .. } | ExprKind::Cast { .. } => 1,
            ExprKind::Func { args, .. } => args.len(),
            ExprKind::Case { acts, .. } => acts.len() + 2,
            ExprKind::Pred(p) => match p {
                Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => es.len(),
                Pred::Not(_) => 1,
                Pred::Func { args, .. } => args.len(),
                Pred::InSubquery(..) | Pred::NotInSubquery(..) => 2,
                Pred::Exists(_) | Pred::NotExists(_) => 1,
            },
            ExprKind::Tuple(es) => es.len(),
        }
    }

    /// Return arguments of current expression.
    /// Many expressions has two arguments so we use SmallVec<[&Expr; 2]>.
    #[inline]
    pub fn args(&self) -> SmallVec<[&ExprKind; 2]> {
        match &self {
            ExprKind::Const(_)
            | ExprKind::Col(..)
            | ExprKind::Plhd(_)
            | ExprKind::Subq(..)
            | ExprKind::Farg(_)
            | ExprKind::Attval(_) => {
                smallvec![]
            }
            ExprKind::Aggf { arg, .. } | ExprKind::Cast { arg, .. } => smallvec![arg.as_ref()],
            ExprKind::Func { args, .. } => args.iter().collect(),
            ExprKind::Case { op, acts, fallback } => {
                op.iter()
                    .map(|e| e.as_ref())
                    .chain(acts.iter().flat_map(|(when, then)| {
                        std::iter::once(when).chain(std::iter::once(then))
                    }))
                    .chain(fallback.iter().map(|e| e.as_ref()))
                    .collect()
            }
            ExprKind::Pred(p) => match p {
                Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => SmallVec::from_iter(es.iter()),
                Pred::Not(e) => smallvec![e.as_ref()],
                Pred::Func { args, .. } => args.iter().collect(),
                Pred::InSubquery(lhs, subq) | Pred::NotInSubquery(lhs, subq) => {
                    smallvec![lhs.as_ref(), subq.as_ref()]
                }
                Pred::Exists(subq) | Pred::NotExists(subq) => smallvec![subq.as_ref()],
            },
            ExprKind::Tuple(es) => SmallVec::from_iter(es.iter()),
        }
    }

    /// Returns mutable arguments of current expression.
    #[inline]
    pub fn args_mut(&mut self) -> SmallVec<[&mut ExprKind; 2]> {
        match self {
            ExprKind::Const(_)
            | ExprKind::Col(..)
            | ExprKind::Plhd(_)
            | ExprKind::Subq(..)
            | ExprKind::Farg(_)
            | ExprKind::Attval(_) => {
                smallvec![]
            }
            ExprKind::Aggf { arg, .. } | ExprKind::Cast { arg, .. } => smallvec![arg.as_mut()],
            ExprKind::Func { args, .. } => args.iter_mut().collect(),
            ExprKind::Case { op, acts, fallback } => {
                op.iter_mut()
                    .map(|e| e.as_mut())
                    .chain(acts.iter_mut().flat_map(|(when, then)| {
                        std::iter::once(when).chain(std::iter::once(then))
                    }))
                    .chain(fallback.iter_mut().map(|e| e.as_mut()))
                    .collect()
            }
            ExprKind::Pred(p) => match p {
                Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => {
                    SmallVec::from_iter(es.iter_mut())
                }
                Pred::Not(e) => smallvec![e.as_mut()],
                Pred::Func { args, .. } => args.iter_mut().collect(),
                Pred::InSubquery(lhs, subq) | Pred::NotInSubquery(lhs, subq) => {
                    smallvec![lhs.as_mut(), subq.as_mut()]
                }
                Pred::Exists(subq) | Pred::NotExists(subq) => smallvec![subq.as_mut()],
            },
            ExprKind::Tuple(es) => SmallVec::from_iter(es.iter_mut()),
        }
    }

    pub fn walk<'a, V: ExprVisitor<'a>>(
        &'a self,
        visitor: &mut V,
    ) -> ControlFlow<V::Break, V::Cont> {
        let mut eff = visitor.enter(self)?;
        for c in self.args() {
            eff.merge(c.walk(visitor)?)
        }
        eff.merge(visitor.leave(self)?);
        ControlFlow::Continue(eff)
    }

    pub fn walk_mut<V: ExprMutVisitor>(
        &mut self,
        visitor: &mut V,
    ) -> ControlFlow<V::Break, V::Cont> {
        let mut eff = visitor.enter(self)?;
        for c in self.args_mut() {
            eff.merge(c.walk_mut(visitor)?)
        }
        eff.merge(visitor.leave(self)?);
        ControlFlow::Continue(eff)
    }

    #[inline]
    pub fn collect_non_aggr_cols(&self) -> (Vec<Col>, bool) {
        let mut cols = vec![];
        let has_aggr = self.collect_non_aggr_cols_into(&mut cols);
        (cols, has_aggr)
    }

    /// Collect non-aggr columns and returns true if aggr function exists
    #[inline]
    pub fn collect_non_aggr_cols_into(&self, cols: &mut Vec<Col>) -> bool {
        struct Collect<'a> {
            aggr_lvl: usize,
            has_aggr: bool,
            cols: &'a mut Vec<Col>,
        }
        impl<'a> ExprVisitor<'a> for Collect<'_> {
            type Cont = ();
            type Break = ();
            #[inline]
            fn enter(&mut self, e: &ExprKind) -> ControlFlow<()> {
                match e {
                    ExprKind::Aggf { .. } => {
                        self.aggr_lvl += 1;
                        self.has_aggr = true
                    }
                    ExprKind::Col(col) => {
                        if self.aggr_lvl == 0 {
                            self.cols.push(col.clone())
                        }
                    }
                    _ => (),
                }
                ControlFlow::Continue(())
            }

            #[inline]
            fn leave(&mut self, e: &ExprKind) -> ControlFlow<()> {
                if let ExprKind::Aggf { .. } = e {
                    self.aggr_lvl -= 1
                }
                ControlFlow::Continue(())
            }
        }
        let mut c = Collect {
            aggr_lvl: 0,
            cols,
            has_aggr: false,
        };
        let _ = self.walk(&mut c);
        c.has_aggr
    }

    #[inline]
    pub fn contains_aggr_func(&self) -> bool {
        struct Contains(bool);
        impl<'a> ExprVisitor<'a> for Contains {
            type Cont = ();
            type Break = ();
            #[inline]
            fn enter(&mut self, e: &ExprKind) -> ControlFlow<()> {
                if let ExprKind::Aggf { .. } = e {
                    self.0 = true;
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            }
        }
        let mut c = Contains(false);
        let _ = self.walk(&mut c);
        c.0
    }

    #[inline]
    pub fn contains_non_aggr_cols(&self) -> bool {
        struct Contains {
            aggr_lvl: usize,
            has_non_aggr_cols: bool,
        }

        impl<'a> ExprVisitor<'a> for Contains {
            type Cont = ();
            type Break = ();
            #[inline]
            fn enter(&mut self, e: &ExprKind) -> ControlFlow<()> {
                match e {
                    ExprKind::Aggf { .. } => self.aggr_lvl += 1,
                    ExprKind::Col(_) => {
                        if self.aggr_lvl == 0 {
                            self.has_non_aggr_cols = true;
                            return ControlFlow::Break(());
                        }
                    }
                    _ => (),
                }
                ControlFlow::Continue(())
            }

            #[inline]
            fn leave(&mut self, e: &ExprKind) -> ControlFlow<()> {
                if let ExprKind::Aggf { .. } = e {
                    self.aggr_lvl -= 1
                }
                ControlFlow::Continue(())
            }
        }

        let mut c = Contains {
            aggr_lvl: 0,
            has_non_aggr_cols: false,
        };
        let _ = self.walk(&mut c);
        c.has_non_aggr_cols
    }

    #[inline]
    pub fn collect_qry_ids(&self, hs: &mut HashSet<QueryID>) {
        let mut c = CollectQryIDs(hs);
        let _ = self.walk(&mut c);
    }
}

impl Default for ExprKind {
    fn default() -> Self {
        ExprKind::Const(Const::Null)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Col {
    pub gid: GlobalID,
    pub kind: ColKind,
    pub idx: ColIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ColKind {
    // table id and column name
    Table(TableID, SemiStr, PreciseType),
    Query(QueryID),
    Correlated(QueryID),
    /// Intra column. Used to chain output of operator nodes.
    /// For example, we may have aggregation expression "sum(c1)+1"
    /// in SELECT list.
    /// We create two operators, one is aggr, the other is
    /// proj. Aggr output "sum(c1)"" and proj output "sum(c1)+1".
    /// IntraCol is generated to represent "sum(c1)" in the middle
    /// between aggr and proj.
    /// Join operator has two children, child index can be either
    /// 0(left) or 1(right).
    Intra(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Setq {
    All,
    Distinct,
}

impl Setq {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        match self {
            Setq::All => "all",
            Setq::Distinct => "distinct",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggKind {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubqKind {
    Scalar,
    // in and not in
    In,
    // exists and not exists
    Exists,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Plhd {
    Ident(u32),
    Subquery(SubqKind, u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Farg {
    /// Indicates there is no argument present in this position of the argument list.
    /// This is important for function with variable arguments.
    /// We fix the number of arguments and use None to be the placeholder.
    None,
    TimeUnit(TimeUnit),
}

pub trait Effect: Default {
    fn merge(&mut self, other: Self);
}

impl Effect for () {
    #[inline]
    fn merge(&mut self, _other: Self) {}
}

pub trait ExprVisitor<'a>: Sized {
    type Cont: Effect;
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _e: &'a ExprKind) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _e: &'a ExprKind) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }
}

pub trait ExprMutVisitor {
    type Cont: Effect;
    type Break;
    /// Returns true if continue
    #[inline]
    fn enter(&mut self, _e: &mut ExprKind) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }

    /// Returns true if continue
    #[inline]
    fn leave(&mut self, _e: &mut ExprKind) -> ControlFlow<Self::Break, Self::Cont> {
        ControlFlow::Continue(Self::Cont::default())
    }
}

pub struct CollectQryIDs<'a>(pub &'a mut HashSet<QueryID>);

impl<'a> ExprVisitor<'a> for CollectQryIDs<'_> {
    type Cont = ();
    type Break = ();
    #[inline]
    fn leave(&mut self, e: &ExprKind) -> ControlFlow<()> {
        if let ExprKind::Col(Col {
            kind: ColKind::Query(qry_id),
            ..
        }) = &e
        {
            self.0.insert(*qry_id);
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_of_smallvec_expr_ref() {
        use std::mem::size_of;
        // println!("size of Expr is {}", size_of::<Expr>());
        println!("size of ExprKind is {}", size_of::<ExprKind>());
        println!(
            "size of SmallVec<[&Expr; 2]> is {}",
            size_of::<SmallVec<[&ExprKind; 2]>>()
        );
        println!(
            "size of SmallVec<[&Expr; 3]> is {}",
            size_of::<SmallVec<[&ExprKind; 3]>>()
        );
    }
}
