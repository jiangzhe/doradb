use crate::op::{
    Aggr, DependentJoin, Filt, Join, Limit, Op, OpVisitor, Proj, QualifiedJoin, Setop, Sort,
    SortItem,
};
use crate::query::{QueryPlan, QuerySet};
use std::fmt::{self, Write};
use xngin_expr::{AggKind, Aggf, Col, Const, Expr, Func, Pred, PredFunc, Setq};

const INDENT: usize = 4;
const BRANCH_1: char = '└';
const BRANCH_N: char = '├';
const BRANCH_V: char = '│';
const LINE: char = '─';

/// Explain defines how to explain an expression, an operator
/// or a plan.
pub trait Explain {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result;
}

impl Explain for QueryPlan {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self.queries.get(&self.root) {
            Some(subq) => {
                let mut qe = QueryExplain {
                    queries: &self.queries,
                    f,
                    spans: vec![],
                    res: Ok(()),
                };
                subq.root.walk(&mut qe);
                qe.res
            }
            None => f.write_str("No plan found"),
        }
    }
}

/* Implements Explain for all operators */

impl Explain for Op {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self {
            Op::Proj(proj) => proj.explain(f),
            Op::Filt(filt) => filt.explain(f),
            Op::Aggr(aggr) => aggr.explain(f),
            Op::Sort(sort) => sort.explain(f),
            Op::Join(join) => join.explain(f),
            Op::Setop(setop) => setop.explain(f),
            Op::Limit(limit) => limit.explain(f),
            Op::Row(row) => {
                f.write_str("Row{")?;
                let (head, tail) = row.split_first().unwrap();
                head.0.explain(f)?;
                for (e, _) in tail {
                    f.write_str(", ")?;
                    e.explain(f)?
                }
                f.write_char('}')
            }
            Op::Table(_, table_id) => {
                write!(f, "Table{{{}}}", table_id.value())
            }
            Op::Subquery(_) => f.write_str("(subquery todo)"),
        }
    }
}

impl Explain for Proj {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Proj{q=")?;
        f.write_str(self.q.to_lower())?;
        f.write_str(", cols=[")?;
        let (head, tail) = self.cols.split_first().unwrap();
        head.0.explain(f)?;
        for (e, _) in tail {
            f.write_str(", ")?;
            e.explain(f)?
        }
        f.write_str("]}")
    }
}

impl Explain for Filt {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Filt{")?;
        self.pred.explain(f)?;
        f.write_char('}')
    }
}

impl Explain for Sort {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Sort{")?;
        let (head, tail) = self.items.split_first().unwrap();
        head.explain(f)?;
        for si in tail {
            f.write_str(", ")?;
            si.explain(f)?
        }
        f.write_char('}')
    }
}

impl Explain for SortItem {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        self.expr.explain(f)?;
        if self.desc {
            f.write_str(" desc")?
        }
        Ok(())
    }
}

impl Explain for Aggr {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Aggr{")?;
        f.write_str("proj=[")?;
        let (head, tail) = self.proj.split_first().unwrap();
        head.0.explain(f)?;
        for (e, _) in tail {
            f.write_str(", ")?;
            e.explain(f)?
        }
        f.write_str("], filt=")?;
        self.filt.explain(f)?;
        f.write_char('}')
    }
}

impl Explain for Join {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Join{")?;
        match self {
            Join::Cross(_) => f.write_str("type=cross")?,
            Join::Dependent(DependentJoin { kind, cond, .. }) => {
                f.write_str("type=dependent, kind=")?;
                f.write_str(kind.to_lower())?;
                f.write_str(", cond=")?;
                cond.explain(f)?
            }
            Join::Qualified(QualifiedJoin { kind, cond, .. }) => {
                f.write_str("type=qualified, kind=")?;
                f.write_str(kind.to_lower())?;
                f.write_str(", cond=")?;
                cond.explain(f)?
            }
        }
        f.write_char('}')
    }
}

impl Explain for Setop {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str("Setop{kind=")?;
        f.write_str(self.kind.to_lower())?;
        if self.q == Setq::All {
            f.write_str(" all")?
        }
        f.write_char('}')
    }
}

impl Explain for Limit {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        write!(f, "Limit{{start={}, end={}}}", self.start, self.end)
    }
}

/* Implements Explain for all expressions */

impl Explain for Expr {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self {
            Expr::Const(c) => c.explain(f),
            Expr::Col(c) => c.explain(f),
            Expr::Aggf(a) => a.explain(f),
            Expr::Func(v) => v.explain(f),
            Expr::Pred(p) => p.explain(f),
            Expr::Tuple(_) => write!(f, "(tuple todo)"),
            Expr::Subq(..) => write!(f, "(subquery todo)"),
            Expr::Plhd(_) => write!(f, "(placeholder todo)"),
            Expr::Farg(_) => write!(f, "(funcarg todo)"),
        }
    }
}

impl Explain for Const {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self {
            Const::I64(v) => write!(f, "{}", v),
            Const::U64(v) => write!(f, "{}", v),
            Const::F64(v) => write!(f, "{}", v.value()),
            Const::Decimal(v) => write!(f, "{}", v.to_string(-1)),
            Const::Date(v) => write!(f, "date'{}'", v),
            Const::Time(v) => write!(f, "time'{}'", v),
            Const::Datetime(v) => write!(f, "timestamp'{}'", v),
            Const::Interval(v) => write!(f, "interval'{}'{}", v.value, v.unit.to_lower()),
            Const::String(s) => write!(f, "'{}'", s),
            Const::Bytes(_) => write!(f, "(bytes todo)"),
            Const::Null => f.write_str("null"),
        }
    }
}

impl Explain for Col {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self {
            Col::TableCol(table_id, idx) => write!(f, "tc({},{})", table_id.value(), idx),
            Col::QueryCol(query_id, idx) => write!(f, "qc({},{})", **query_id, idx),
            Col::CorrelatedCol(query_id, idx) => write!(f, "cc({},{})", **query_id, idx),
        }
    }
}

impl Explain for Aggf {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self.kind {
            AggKind::Count => f.write_str("count(")?,
            AggKind::Sum => f.write_str("sum(")?,
            AggKind::Max => f.write_str("max(")?,
            AggKind::Min => f.write_str("min(")?,
            AggKind::Avg => f.write_str("avg(")?,
        }
        if self.q == Setq::Distinct {
            f.write_str("distinct ")?
        }
        self.arg.explain(f)?;
        f.write_char(')')
    }
}

impl Explain for Func {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str(self.kind.to_lower())?;
        f.write_char('(')?;
        if self.args.is_empty() {
            return f.write_char(')');
        }
        write_exprs(f, &self.args, ", ")?;
        f.write_char(')')
    }
}

impl Explain for Pred {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        match self {
            Pred::True => f.write_str("true"),
            Pred::False => f.write_str("false"),
            Pred::Conj(es) => write_exprs(f, es, " and "),
            Pred::Disj(es) => write_exprs(f, es, " or "),
            Pred::Xor(es) => write_exprs(f, es, " xor "),
            Pred::Func(pf) => pf.explain(f),
            Pred::Not(e) => {
                f.write_str("not ")?;
                e.explain(f)
            }
            Pred::InSubquery(..)
            | Pred::NotInSubquery(..)
            | Pred::Exists(_)
            | Pred::NotExists(_) => f.write_str("(pred subquery todo)"),
        }
    }
}

impl Explain for PredFunc {
    fn explain<F: Write>(&self, f: &mut F) -> fmt::Result {
        f.write_str(self.kind.to_lower())?;
        f.write_char('(')?;
        write_exprs(f, &self.args, ", ")?;
        f.write_char(')')
    }
}

fn write_exprs<F: Write>(f: &mut F, exprs: &[Expr], delimiter: &str) -> fmt::Result {
    let (head, tail) = exprs.split_first().unwrap();
    head.explain(f)?;
    for e in tail {
        f.write_str(delimiter)?;
        e.explain(f)?
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Span {
    Space(u16),
    Branch(u16, bool),
}

struct QueryExplain<'a, F> {
    queries: &'a QuerySet,
    f: &'a mut F,
    spans: Vec<Span>,
    res: fmt::Result,
}

impl<'a, F: Write> QueryExplain<'a, F> {
    // returns true if continue
    fn write_prefix(&mut self) -> bool {
        self.res = write_prefix(self.f, &self.spans);
        self.res.is_ok()
    }

    fn write_str(&mut self, s: &str) -> bool {
        self.res = self.f.write_str(s);
        self.res.is_ok()
    }

    fn set_res(&mut self, res: fmt::Result) -> bool {
        self.res = res;
        self.res.is_ok()
    }
}

impl<F: Write> OpVisitor for QueryExplain<'_, F> {
    #[inline]
    fn enter(&mut self, op: &Op) -> bool {
        // special handling Subquery
        if let Op::Subquery(query_id) = op {
            let res = if let Some(subq) = self.queries.get(query_id) {
                let mut qe = QueryExplain {
                    queries: self.queries,
                    f: self.f,
                    spans: self.spans.clone(),
                    res: Ok(()),
                };
                subq.root.walk(&mut qe);
                qe.res
            } else {
                Err(fmt::Error)
            };
            return self.set_res(res);
        }
        let child_cnt = op.children().len();
        if !self.write_prefix() {
            return false;
        }
        // process at parent level
        if let Some(span) = self.spans.pop() {
            match span {
                Span::Branch(1, _) => {
                    if let Some(Span::Space(n)) = self.spans.last_mut() {
                        *n += INDENT as u16
                    } else {
                        self.spans.push(Span::Space(INDENT as u16))
                    }
                }
                Span::Branch(n, _) => self.spans.push(Span::Branch(n - 1, true)),
                _ => self.spans.push(span),
            }
        }
        // process at current level
        if child_cnt > 0 {
            self.spans.push(Span::Branch(child_cnt as u16, false))
        }
        let res = op.explain(self.f);
        if !self.set_res(res) {
            return false;
        }
        self.write_str("\n")
    }

    #[inline]
    fn leave(&mut self, _op: &Op) -> bool {
        if let Some(span) = self.spans.last_mut() {
            match span {
                Span::Branch(1, _) => {
                    let _ = self.spans.pop();
                }
                Span::Branch(n, vertical) => {
                    *n -= 1;
                    *vertical = false;
                }
                _ => (),
            }
        }
        true
    }
}

fn write_prefix<F: Write>(f: &mut F, spans: &[Span]) -> fmt::Result {
    for &span in spans {
        match span {
            Span::Space(n) => {
                for _ in 0..n {
                    f.write_char(' ')?
                }
            }
            Span::Branch(1, false) => {
                f.write_char(BRANCH_1)?;
                for _ in 1..INDENT {
                    f.write_char(LINE)?
                }
            }
            Span::Branch(_, false) => {
                f.write_char(BRANCH_N)?;
                for _ in 1..INDENT {
                    f.write_char(LINE)?
                }
            }
            Span::Branch(_, true) => {
                f.write_char(BRANCH_V)?;
                for _ in 1..INDENT {
                    f.write_char(' ')?
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Explain;
    use crate::builder::tests::tpch_catalog;
    use crate::builder::PlanBuilder;
    use std::sync::Arc;
    use xngin_frontend::parser::dialect::MySQL;
    use xngin_frontend::parser::parse_query;

    #[test]
    fn test_explain_plan() {
        let cat = tpch_catalog();
        for sql in vec![
            "select 1",
            "with cte1 as (select 1), cte2 as (select 2) select * from cte1",
            "select l1.l_orderkey from lineitem l1, lineitem l2",
        ] {
            let builder = PlanBuilder::new(Arc::clone(&cat), "tpch").unwrap();
            let (_, qr) = parse_query(MySQL(sql)).unwrap();
            let plan = builder.build_plan(&qr).unwrap();
            let mut s = String::new();
            let _ = plan.explain(&mut s).unwrap();
            println!("Explain plan:\n{}", s)
        }
    }
}