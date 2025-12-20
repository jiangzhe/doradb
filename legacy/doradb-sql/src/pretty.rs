use crate::ast::*;
use crate::parser::expr::{
    bin_infix_binding_power, pred_infix_binding_power, prefix_binding_power, BindingPower,
};
use std::fmt::{self, Write};

pub struct PrettyConf {
    pub ident_delim: char,
    pub upper_kw: bool,
    pub indent: usize,
    pub newline: bool,
    pub tuple_newline: bool,
    pub enclose_multi_preds: bool,
}

impl Default for PrettyConf {
    fn default() -> Self {
        PrettyConf {
            ident_delim: '`',
            upper_kw: true,
            indent: 4,
            newline: true,
            tuple_newline: false,
            enclose_multi_preds: true,
        }
    }
}

/// All AST structs should implements PrettyDisplay to enable
/// pretty formatting.
pub trait PrettyFormat {
    /// Print in pretty format.
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result;

    /// Convenient method to get a string representation in pretty format.
    fn pretty_string(&self, conf: PrettyConf) -> Result<String, fmt::Error> {
        let mut s = String::new();
        self.pretty_fmt(&mut s, &conf, 0)?;
        Ok(s)
    }
}

impl PrettyFormat for Ident<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, _indent: usize) -> fmt::Result {
        match self.kind {
            IdentKind::Regular => f.write_str(self.as_ref()),
            IdentKind::Quoted | IdentKind::AutoAlias => {
                // here treat auto-alias same as delimited.
                // todo: should take care of escape characters.
                f.write_char(conf.ident_delim)?;
                f.write_str(self.as_ref())?;
                f.write_char(conf.ident_delim)
            }
        }
    }
}

impl PrettyFormat for TableName<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        if let Some(schema) = &self.schema {
            schema.pretty_fmt(f, conf, indent)?;
            f.write_char('.')?;
        }
        self.table.pretty_fmt(f, conf, indent)
    }
}

impl PrettyFormat for QualifiedAsterisk<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        for q in &self.0 {
            q.pretty_fmt(f, conf, indent)?;
            f.write_char(conf.ident_delim)?;
        }
        f.write_char('*')
    }
}

impl PrettyFormat for Literal<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        match self {
            Literal::Numeric(s) => f.write_str(s),
            Literal::CharStr(cs) => cs.pretty_fmt(f, conf, indent),
            Literal::BitStr(s) => {
                if conf.upper_kw {
                    f.write_str("B'")?;
                } else {
                    f.write_str("b'")?;
                }
                f.write_str(s)?;
                f.write_char('\'')
            }
            Literal::HexStr(s) => {
                if conf.upper_kw {
                    f.write_str("X'")?;
                } else {
                    f.write_str("x'")?;
                }
                f.write_str(s)?;
                f.write_char('\'')
            }
            Literal::Date(s) => {
                if conf.upper_kw {
                    f.write_str("DATE '")?;
                } else {
                    f.write_str("date '")?;
                }
                f.write_str(s)?;
                f.write_char('\'')
            }
            Literal::Time(s) => {
                if conf.upper_kw {
                    f.write_str("TIME '")?;
                } else {
                    f.write_str("time '")?;
                }
                f.write_str(s)?;
                f.write_char('\'')
            }
            Literal::Timestamp(s) => {
                if conf.upper_kw {
                    f.write_str("TIMESTAMP '")?;
                } else {
                    f.write_str("timestamp '")?;
                }
                f.write_str(s)?;
                f.write_char('\'')
            }
            Literal::Interval(i) => i.pretty_fmt(f, conf, indent),
            Literal::Bool(b) => match (conf.upper_kw, *b) {
                (true, true) => f.write_str("TRUE"),
                (true, false) => f.write_str("FALSE"),
                (false, true) => f.write_str("true"),
                (false, false) => f.write_str("false"),
            },
            Literal::Null => {
                if conf.upper_kw {
                    f.write_str("NULL")
                } else {
                    f.write_str("null")
                }
            }
        }
    }
}

impl PrettyFormat for CharStr<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, _conf: &PrettyConf, _indent: usize) -> fmt::Result {
        // todo: print charset
        f.write_char('\'')?;
        f.write_str(self.first)?;
        for r in &self.rest {
            f.write_str(r)?;
        }
        f.write_char('\'')
    }
}

impl PrettyFormat for Interval<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, _indent: usize) -> fmt::Result {
        if conf.upper_kw {
            f.write_str("INTERVAL ")?;
        } else {
            f.write_str("interval ")?;
        }
        f.write_char('\'')?;
        f.write_str(self.value)?;
        f.write_str("' ")?;
        f.write_str(self.unit.kw_str(conf.upper_kw))
    }
}

impl PrettyFormat for Expr<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        match self {
            Expr::Literal(lit) => lit.pretty_fmt(f, conf, indent),
            Expr::ColumnRef(cr) => {
                let (last, head) = cr.split_last().unwrap();
                for id in head {
                    id.pretty_fmt(f, conf, indent)?;
                    f.write_char('.')?;
                }
                last.pretty_fmt(f, conf, indent)
            }
            Expr::AggrFunc(af) => af.pretty_fmt(f, conf, indent),
            Expr::Unary(ue) => ue.pretty_fmt(f, conf, indent),
            Expr::Binary(be) => be.pretty_fmt(f, conf, indent),
            Expr::Predicate(pred) => pred.pretty_fmt(f, conf, indent),
            Expr::Func(func) => func.pretty_fmt(f, conf, indent),
            Expr::FuncArg(arg) => arg.pretty_fmt(f, conf, indent),
            Expr::CaseWhen(cw) => cw.pretty_fmt(f, conf, indent),
            Expr::ScalarSubquery(sq) => {
                f.write_char('(')?;
                let sub_indent = indent + conf.indent;
                write_indent(f, conf.newline, sub_indent)?;
                sq.pretty_fmt(f, conf, sub_indent)?;
                write_indent(f, conf.newline, indent)?;
                f.write_char(')')
            }
            Expr::Tuple(tp) => {
                if let Some((last, head)) = tp.split_last() {
                    let sub_indent = indent + conf.indent;
                    f.write_char('(')?;
                    write_indent(f, conf.tuple_newline, sub_indent)?;
                    for e in head {
                        e.pretty_fmt(f, conf, sub_indent)?;
                        f.write_char(',')?;
                        write_sp(f, conf.tuple_newline, sub_indent)?;
                    }
                    last.pretty_fmt(f, conf, sub_indent)?;
                    write_indent(f, conf.tuple_newline, indent)?;
                    f.write_char(')')
                } else {
                    unreachable!()
                }
            }
        }
    }
}

impl PrettyFormat for UnaryExpr<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        match self.op {
            UnaryOp::Neg => f.write_char('-')?,
            UnaryOp::BitInv => f.write_char('~')?,
            UnaryOp::LogicalNot => {
                if conf.upper_kw {
                    f.write_str("NOT ")?
                } else {
                    f.write_str("not ")?
                }
            }
        }
        self.arg.pretty_fmt(f, conf, indent)
    }
}

impl PrettyFormat for AggrFunc<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        f.write_str(self.kind.kw_str(conf.upper_kw))?;
        match self.kind {
            AggrFuncKind::CountAsterisk => Ok(()),
            _ => {
                f.write_char('(')?;
                if let SetQuantifier::Distinct = self.q {
                    if conf.upper_kw {
                        f.write_str("DISTINCT ")?;
                    } else {
                        f.write_str("distinct ")?;
                    }
                }
                self.expr.pretty_fmt(f, conf, indent)?;
                f.write_char(')')
            }
        }
    }
}

// Consideration of operator precedence must be taken for both Predicate and BinaryExpr
// for correct format the SQL.
impl PrettyFormat for Predicate<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        match self {
            Predicate::Cmp(cmp_op, lhs, rhs) => {
                lhs.pretty_fmt(f, conf, indent)?;
                f.write_char(' ')?;
                f.write_str(cmp_op.kw_str(conf.upper_kw))?;
                f.write_char(' ')?;
                rhs.pretty_fmt(f, conf, indent)
            }
            Predicate::QuantCmp(cmp_op, q, lhs, subquery) => {
                lhs.pretty_fmt(f, conf, indent)?;
                f.write_char(' ')?;
                f.write_str(cmp_op.kw_str(conf.upper_kw))?;
                f.write_char(' ')?;
                f.write_str(q.kw_str(conf.upper_kw))?;
                f.write_char(' ')?;
                pretty_fmt_subquery(subquery, f, conf, indent)
            }
            Predicate::Is(op, lhs) => {
                lhs.pretty_fmt(f, conf, indent)?;
                f.write_char(' ')?;
                f.write_str(op.kw_str(conf.upper_kw))
            }
            Predicate::Match(match_op, lhs, rhs) => {
                lhs.pretty_fmt(f, conf, indent)?;
                f.write_char(' ')?;
                f.write_str(match_op.kw_str(conf.upper_kw))?;
                f.write_char(' ')?;
                rhs.pretty_fmt(f, conf, indent)
            }
            Predicate::InValues(lhs, exprs) | Predicate::NotInValues(lhs, exprs) => {
                let pred_op = self.infix_op().unwrap(); // won't fail
                lhs.pretty_fmt(f, conf, indent)?;
                f.write_char(' ')?;
                f.write_str(pred_op.kw_str(conf.upper_kw))?;
                f.write_char(' ')?;
                f.write_char('(')?;
                let sub_indent = indent + conf.indent;
                write_indent(f, conf.tuple_newline, sub_indent)?;
                pretty_fmt_exprs(exprs, f, conf, conf.tuple_newline, sub_indent)?;
                write_indent(f, conf.tuple_newline, indent)?;
                f.write_char(')')
            }
            Predicate::InSubquery(lhs, query) | Predicate::NotInSubquery(lhs, query) => {
                let pred_op = self.infix_op().unwrap(); // won't fail
                lhs.pretty_fmt(f, conf, indent)?;
                f.write_char(' ')?;
                f.write_str(pred_op.kw_str(conf.upper_kw))?;
                f.write_char(' ')?;
                pretty_fmt_subquery(query, f, conf, indent)
            }
            Predicate::Between(lhs, mhs, rhs) | Predicate::NotBetween(lhs, mhs, rhs) => {
                let pred_op = self.infix_op().unwrap(); // won't fail
                lhs.pretty_fmt(f, conf, indent)?;
                f.write_char(' ')?;
                f.write_str(pred_op.kw_str(conf.upper_kw))?;
                f.write_char(' ')?;
                mhs.pretty_fmt(f, conf, indent)?;
                if conf.upper_kw {
                    f.write_str(" AND ")?
                } else {
                    f.write_str(" and ")?
                }
                rhs.pretty_fmt(f, conf, indent)
            }
            Predicate::Exists(query) => {
                if conf.upper_kw {
                    f.write_str("EXISTS ")?;
                } else {
                    f.write_str("exists ")?;
                }
                pretty_fmt_subquery(query, f, conf, indent)
            }
            Predicate::Conj(exprs) | Predicate::Disj(exprs) | Predicate::LogicalXor(exprs) => {
                let op = self.infix_op().unwrap(); // won't fail
                pretty_fmt_preds(op, exprs, f, conf, indent)
            }
        }
    }
}

#[inline]
fn enclose_left(lhs: &Expr<'_>, curr_bp: BindingPower, enclose_multi_preds: bool) -> bool {
    let prev_bp = match lhs {
        Expr::Binary(be) => {
            let (_, prev_bp) = bin_infix_binding_power(be.op);
            prev_bp
        }
        // todo: consider prefix op if LogicalNot added to predicate
        Expr::Predicate(pred) => {
            if enclose_multi_preds && is_multi_preds(pred) {
                return true;
            }
            if let Some(pred_op) = pred.infix_op() {
                let (_, prev_bp) = pred_infix_binding_power(pred_op);
                prev_bp
            } else {
                return false;
            }
        }
        Expr::Unary(ue) => prefix_binding_power(ue.op),
        _ => return false,
    };
    prev_bp < curr_bp
}

#[inline]
fn enclose_right(curr_bp: BindingPower, rhs: &Expr<'_>, enclose_multi_preds: bool) -> bool {
    let next_bp = match rhs {
        Expr::Binary(be) => {
            let (next_bp, _) = bin_infix_binding_power(be.op);
            next_bp
        }
        Expr::Predicate(pred) => {
            if enclose_multi_preds && is_multi_preds(pred) {
                return true;
            }
            if let Some(pred_op) = pred.infix_op() {
                let (next_bp, _) = pred_infix_binding_power(pred_op);
                next_bp
            } else {
                return false;
            }
        }
        _ => return false,
    };
    next_bp < curr_bp
}

#[inline]
fn is_multi_preds(pred: &Predicate<'_>) -> bool {
    matches!(
        pred,
        Predicate::Conj(_) | Predicate::Disj(_) | Predicate::LogicalXor(_)
    )
}

impl PrettyFormat for BinaryExpr<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        // we need to take care of operator precedence
        let (l_bp, r_bp) = bin_infix_binding_power(self.op);
        let enclose_left = enclose_left(&self.lhs, l_bp, false);

        // left expression
        if enclose_left {
            f.write_char('(')?
        }
        self.lhs.pretty_fmt(f, conf, indent)?;
        if enclose_left {
            f.write_char(')')?
        }

        // operator
        f.write_char(' ')?;
        f.write_str(self.op.kw_str(conf.upper_kw))?;
        f.write_char(' ')?;

        // right operator
        let enclose_right = enclose_right(r_bp, &self.rhs, false);
        if enclose_right {
            f.write_char('(')?
        }
        self.rhs.pretty_fmt(f, conf, indent)?;
        if enclose_right {
            f.write_char(')')?
        }
        Ok(())
    }
}

impl PrettyFormat for FuncExpr<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        f.write_str(self.ty.kw_str(conf.upper_kw))?;
        f.write_char('(')?;
        match self.ty {
            FuncType::Extract => {
                self.args[0].pretty_fmt(f, conf, indent)?;
                if conf.upper_kw {
                    f.write_str(" FROM ")?;
                } else {
                    f.write_str(" from ")?;
                }
                self.args[1].pretty_fmt(f, conf, indent)?;
            }
            FuncType::Substring => {
                self.args[0].pretty_fmt(f, conf, indent)?;
                f.write_str(", ")?;
                self.args[1].pretty_fmt(f, conf, indent)?;
                if self.args[2] != Expr::FuncArg(ConstArg::None) {
                    f.write_str(", ")?;
                    self.args[2].pretty_fmt(f, conf, indent)?;
                }
            }
        }
        f.write_char(')')
    }
}

impl PrettyFormat for ConstArg {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, _indent: usize) -> fmt::Result {
        match self {
            ConstArg::DatetimeUnit(unit) => f.write_str(unit.kw_str(conf.upper_kw)),
            // absent argument should not be output.
            ConstArg::None => unreachable!(),
        }
    }
}

impl PrettyFormat for Builtin<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        f.write_str(self.kw_str(conf.upper_kw))?;
        f.write_char('(')?;
        match self {
            Builtin::Extract(unit, value) => {
                unit.pretty_fmt(f, conf, indent)?;
                if conf.upper_kw {
                    f.write_str(" FROM ")?;
                } else {
                    f.write_str(" from ")?;
                }
                value.pretty_fmt(f, conf, indent)?;
            }
            Builtin::Substring(value, start, end) => {
                value.pretty_fmt(f, conf, indent)?;
                f.write_str(", ")?;
                start.pretty_fmt(f, conf, indent)?;
                if let Some(end) = end {
                    f.write_str(", ")?;
                    end.pretty_fmt(f, conf, indent)?;
                }
            }
        }
        f.write_char(')')
    }
}

impl PrettyFormat for CaseWhen<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        let (case_str, when_str, then_str, else_str, end_str) = if conf.upper_kw {
            ("CASE", "WHEN", "THEN", "ELSE", "END")
        } else {
            ("case", "when", "then", "else", "end")
        };
        f.write_str(case_str)?;
        if let Some(op) = &self.operand {
            f.write_char(' ')?;
            op.pretty_fmt(f, conf, indent)?;
        }

        for (when, then) in &self.branches {
            f.write_char(' ')?;
            f.write_str(when_str)?;
            f.write_char(' ')?;
            when.pretty_fmt(f, conf, indent)?;
            f.write_char(' ')?;
            f.write_str(then_str)?;
            f.write_char(' ')?;
            then.pretty_fmt(f, conf, indent)?;
        }

        if let Some(fb) = &self.fallback {
            f.write_char(' ')?;
            f.write_str(else_str)?;
            f.write_char(' ')?;
            fb.pretty_fmt(f, conf, indent)?;
        }

        f.write_char(' ')?;
        f.write_str(end_str)
    }
}

impl PrettyFormat for Statement<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        match self {
            Statement::Select(q) => q.pretty_fmt(f, conf, indent),
            _ => todo!(),
        }
    }
}

impl PrettyFormat for QueryExpr<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        if let Some(with) = &self.with {
            with.pretty_fmt(f, conf, indent)?;
        }
        self.query.pretty_fmt(f, conf, indent)
    }
}

impl PrettyFormat for With<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        let (with_str, rec_str) = if conf.upper_kw {
            ("WITH", "RECURSIVE")
        } else {
            ("with", "recursive")
        };

        f.write_str(with_str)?;
        if self.recursive {
            f.write_char(' ')?;
            f.write_str(rec_str)?;
        }
        write_sp(f, conf.newline, indent)?;
        let (last, head) = self.elements.split_last().unwrap();
        for elem in head {
            elem.pretty_fmt(f, conf, indent)?;
            f.write_char(',')?;
            write_indent(f, conf.newline, indent)?
        }
        last.pretty_fmt(f, conf, indent)?;
        write_indent(f, conf.newline, indent)
    }
}

impl PrettyFormat for WithElement<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        let sub_indent = indent + conf.indent;
        self.name.pretty_fmt(f, conf, indent)?;
        if !self.cols.is_empty() {
            f.write_str(" (")?;
            write_indent(f, conf.tuple_newline, sub_indent)?;
            pretty_fmt_idents(&self.cols, f, conf, conf.tuple_newline, sub_indent)?;
            write_indent(f, conf.tuple_newline, indent)?;
            f.write_char(')')?
        }
        f.write_char(' ')?;
        if conf.upper_kw {
            f.write_str("AS")?;
        } else {
            f.write_str("as")?;
        }
        f.write_char(' ')?;
        pretty_fmt_subquery(&self.query_expr, f, conf, indent)
    }
}

impl PrettyFormat for Query<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        match self {
            Query::Row(cols) => {
                if conf.upper_kw {
                    f.write_str("SELECT")?;
                } else {
                    f.write_str("select")?;
                }
                let sub_indent = indent + conf.indent;
                write_sp(f, conf.tuple_newline, sub_indent)?;
                pretty_fmt_cols(cols, f, conf, conf.tuple_newline, sub_indent)
            }
            Query::Table(table) => table.pretty_fmt(f, conf, indent),
            Query::Set(set) => set.pretty_fmt(f, conf, indent),
        }
    }
}

impl PrettyFormat for DerivedCol<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        match self {
            DerivedCol::Asterisk(qualifiers) => {
                for q in qualifiers {
                    q.pretty_fmt(f, conf, indent)?;
                    f.write_char('.')?;
                }
                f.write_char('*')
            }
            DerivedCol::Expr(expr, alias) => {
                expr.pretty_fmt(f, conf, indent)?;
                if alias.kind != IdentKind::AutoAlias {
                    if conf.upper_kw {
                        f.write_str(" AS ")?;
                    } else {
                        f.write_str(" as ")?;
                    }
                    alias.pretty_fmt(f, conf, indent)?;
                }
                Ok(())
            }
        }
    }
}

impl PrettyFormat for SelectTable<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        let (
            sel_str,
            from_str,
            filter_str,
            group_str,
            having_str,
            order_str,
            limit_str,
            offset_str,
        ) = if conf.upper_kw {
            (
                "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET",
            )
        } else {
            (
                "select", "from", "where", "group by", "having", "order by", "limit", "offset",
            )
        };
        let sub_indent = indent + conf.indent;
        f.write_str(sel_str)?;
        write_sp(f, conf.newline, sub_indent)?;
        pretty_fmt_cols(&self.cols, f, conf, conf.newline, sub_indent)?;
        write_sp(f, conf.newline, indent)?;
        f.write_str(from_str)?;
        write_sp(f, conf.newline, sub_indent)?;
        pretty_fmt_table_refs(&self.from, f, conf, sub_indent)?;
        if let Some(filter) = &self.filter {
            write_sp(f, conf.newline, indent)?;
            f.write_str(filter_str)?;
            write_sp(f, conf.newline, sub_indent)?;
            filter.pretty_fmt(f, conf, sub_indent)?;
        }
        if !self.group_by.is_empty() {
            write_sp(f, conf.newline, indent)?;
            f.write_str(group_str)?;
            write_sp(f, conf.newline, sub_indent)?;
            pretty_fmt_exprs(&self.group_by, f, conf, conf.newline, indent)?;
        }
        if let Some(having) = &self.having {
            write_sp(f, conf.newline, indent)?;
            f.write_str(having_str)?;
            write_sp(f, conf.newline, sub_indent)?;
            having.pretty_fmt(f, conf, sub_indent)?;
        }
        if let Some((last, head)) = self.order_by.split_last() {
            write_sp(f, conf.newline, indent)?;
            f.write_str(order_str)?;
            write_sp(f, conf.newline, sub_indent)?;
            for elem in head {
                elem.pretty_fmt(f, conf, sub_indent)?;
                write_delim(f, ',', conf.newline, sub_indent)?;
            }
            last.pretty_fmt(f, conf, sub_indent)?;
        }
        if let Some(limit) = &self.limit {
            write_sp(f, conf.newline, indent)?;
            f.write_str(limit_str)?;
            f.write_char(' ')?;
            write!(f, "{}", limit.limit)?;
            if let Some(offset) = limit.offset {
                f.write_char(' ')?;
                f.write_str(offset_str)?;
                f.write_char(' ')?;
                write!(f, "{offset}")?;
            }
        }
        Ok(())
    }
}

impl PrettyFormat for TableRef<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        match self {
            TableRef::Primitive(tp) => tp.pretty_fmt(f, conf, indent),
            TableRef::Joined(tj) => tj.pretty_fmt(f, conf, indent),
        }
    }
}

impl PrettyFormat for TablePrimitive<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        match self {
            TablePrimitive::Named(tn, alias) => {
                tn.pretty_fmt(f, conf, indent)?;
                if let Some(alias) = alias {
                    if conf.upper_kw {
                        f.write_str(" AS ")?;
                    } else {
                        f.write_str(" as ")?;
                    }
                    alias.pretty_fmt(f, conf, indent)?;
                }
                Ok(())
            }
            TablePrimitive::Derived(query, alias) => {
                let sub_indent = indent + conf.indent;
                f.write_char('(')?;
                write_indent(f, conf.newline, sub_indent)?;
                query.pretty_fmt(f, conf, sub_indent)?;
                write_indent(f, conf.newline, indent)?;
                f.write_char(')')?;
                if conf.upper_kw {
                    f.write_str(" AS ")?;
                } else {
                    f.write_str(" as ")?;
                }
                alias.pretty_fmt(f, conf, indent)
            }
        }
    }
}

impl PrettyFormat for TableJoin<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        let (cross_str, nat_str) = if conf.upper_kw {
            ("CROSS JOIN", "NATURAL")
        } else {
            ("cross join", "natural")
        };
        let sub_indent = indent + conf.indent;
        match self {
            TableJoin::Cross(cj) => {
                cj.left.pretty_fmt(f, conf, indent)?;
                write_sp(f, conf.newline, indent)?;
                f.write_str(cross_str)?;
                f.write_char(' ')?;
                cj.right.pretty_fmt(f, conf, sub_indent)
            }
            TableJoin::Natural(nj) => {
                nj.left.pretty_fmt(f, conf, indent)?;
                write_sp(f, conf.newline, indent)?;
                f.write_str(nat_str)?;
                f.write_char(' ')?;
                f.write_str(nj.ty.kw_str(conf.upper_kw))?;
                f.write_char(' ')?;
                nj.right.pretty_fmt(f, conf, sub_indent)
            }
            TableJoin::Qualified(qj) => {
                qj.left.pretty_fmt(f, conf, indent)?;
                write_sp(f, conf.newline, indent)?;
                f.write_str(qj.ty.kw_str(conf.upper_kw))?;
                f.write_char(' ')?;
                qj.right.pretty_fmt(f, conf, indent)?;
                if let Some(cond) = &qj.cond {
                    write_sp(f, conf.newline, indent)?;
                    cond.pretty_fmt(f, conf, sub_indent)?;
                }
                Ok(())
            }
        }
    }
}

impl PrettyFormat for JoinCondition<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        let sub_indent = indent + conf.indent;
        match self {
            JoinCondition::NamedCols(cols) => {
                if conf.upper_kw {
                    f.write_str("USING ")?;
                } else {
                    f.write_str("using ")?;
                }
                f.write_char('(')?;
                write_indent(f, conf.tuple_newline, sub_indent)?;
                pretty_fmt_idents(cols, f, conf, conf.tuple_newline, sub_indent)?;
                write_indent(f, conf.tuple_newline, indent)?;
                f.write_char(')')
            }
            JoinCondition::Conds(cond) => {
                if conf.upper_kw {
                    f.write_str("ON")?;
                } else {
                    f.write_str("on")?;
                }
                write_sp(f, conf.newline, indent)?;
                cond.pretty_fmt(f, conf, indent)
            }
        }
    }
}

impl PrettyFormat for OrderElement<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        self.expr.pretty_fmt(f, conf, indent)?;
        if self.desc {
            f.write_char(' ')?;
            if conf.upper_kw {
                f.write_str("DESC")?;
            } else {
                f.write_str("desc")?;
            }
        }
        Ok(())
    }
}

impl PrettyFormat for SelectSet<'_> {
    fn pretty_fmt<F: Write>(&self, f: &mut F, conf: &PrettyConf, indent: usize) -> fmt::Result {
        let all_str = if conf.upper_kw { "ALL" } else { "all" };
        self.left.pretty_fmt(f, conf, indent)?;
        write_sp(f, conf.newline, indent)?;
        f.write_str(self.op.kw_str(conf.upper_kw))?;
        if !self.distinct {
            f.write_char(' ')?;
            f.write_str(all_str)?;
        }
        write_sp(f, conf.newline, indent)?;
        self.right.pretty_fmt(f, conf, indent)
    }
}

#[inline]
fn pretty_fmt_subquery<F: Write>(
    subquery: &QueryExpr<'_>,
    f: &mut F,
    conf: &PrettyConf,
    indent: usize,
) -> fmt::Result {
    f.write_char('(')?;
    let sub_indent = indent + conf.indent;
    write_indent(f, conf.newline, sub_indent)?;
    subquery.pretty_fmt(f, conf, sub_indent)?;
    write_indent(f, conf.newline, indent)?;
    f.write_char(')')
}

#[inline]
fn pretty_fmt_idents<F: Write>(
    cols: &[Ident<'_>],
    f: &mut F,
    conf: &PrettyConf,
    newline: bool,
    indent: usize,
) -> fmt::Result {
    if let Some((last, head)) = cols.split_last() {
        for col in head {
            col.pretty_fmt(f, conf, indent)?;
            write_delim(f, ',', newline, indent)?;
        }
        last.pretty_fmt(f, conf, indent)
    } else {
        unreachable!()
    }
}

#[inline]
fn pretty_fmt_cols<F: Write>(
    cols: &[DerivedCol<'_>],
    f: &mut F,
    conf: &PrettyConf,
    newline: bool,
    indent: usize,
) -> fmt::Result {
    if let Some((last, head)) = cols.split_last() {
        for col in head {
            col.pretty_fmt(f, conf, indent)?;
            write_delim(f, ',', newline, indent)?;
        }
        last.pretty_fmt(f, conf, indent)
    } else {
        unreachable!()
    }
}

#[inline]
fn pretty_fmt_table_refs<F: Write>(
    table_refs: &[TableRef<'_>],
    f: &mut F,
    conf: &PrettyConf,
    indent: usize,
) -> fmt::Result {
    if let Some((last, head)) = table_refs.split_last() {
        for tb in head {
            tb.pretty_fmt(f, conf, indent)?;
            write_delim(f, ',', conf.newline, indent)?;
        }
        last.pretty_fmt(f, conf, indent)
    } else {
        unreachable!()
    }
}

#[inline]
fn pretty_fmt_exprs<F: Write>(
    exprs: &[Expr<'_>],
    f: &mut F,
    conf: &PrettyConf,
    newline: bool,
    indent: usize,
) -> fmt::Result {
    if let Some((last, head)) = exprs.split_last() {
        for tb in head {
            tb.pretty_fmt(f, conf, indent)?;
            write_delim(f, ',', newline, indent)?;
        }
        last.pretty_fmt(f, conf, indent)
    } else {
        unreachable!()
    }
}

#[inline]
fn pretty_fmt_preds<F: Write>(
    op: PredicateOp,
    preds: &[Expr<'_>],
    f: &mut F,
    conf: &PrettyConf,
    indent: usize,
) -> fmt::Result {
    let (l_bp, r_bp) = pred_infix_binding_power(op);
    // indent injected for AND/OR/XOR
    let sub_indent = indent + conf.indent;
    let (head, tail) = preds.split_first().unwrap(); // won't fail
    let enclose_left = enclose_left(head, l_bp, conf.enclose_multi_preds);
    if enclose_left {
        f.write_char('(')?;
        write_indent(f, conf.newline, sub_indent)?;
        head.pretty_fmt(f, conf, sub_indent)?;
        write_indent(f, conf.newline, indent)?;
        f.write_char(')')?
    } else {
        head.pretty_fmt(f, conf, indent)?
    }

    for e in tail {
        write_sp(f, conf.newline, indent)?;
        f.write_str(op.kw_str(conf.upper_kw))?;

        let enclose_right = enclose_right(r_bp, e, conf.enclose_multi_preds);
        if enclose_right {
            write_sp(f, conf.newline, indent)?;
            f.write_char('(')?;
            write_indent(f, conf.newline, sub_indent)?;
            e.pretty_fmt(f, conf, sub_indent)?;
            write_indent(f, conf.newline, indent)?;
            f.write_char(')')?
        } else {
            f.write_char(' ')?;
            e.pretty_fmt(f, conf, indent)?
        }
    }
    Ok(())
}

#[inline]
fn write_sp<F: Write>(f: &mut F, newline: bool, indent: usize) -> fmt::Result {
    if newline {
        f.write_char('\n')?;
        for _ in 0..indent {
            f.write_char(' ')?;
        }
        Ok(())
    } else {
        f.write_char(' ')
    }
}

#[inline]
fn write_delim<F: Write>(f: &mut F, delim: char, newline: bool, indent: usize) -> fmt::Result {
    f.write_char(delim)?;
    if newline {
        f.write_char('\n')?;
        for _ in 0..indent {
            f.write_char(' ')?;
        }
        Ok(())
    } else {
        f.write_char(' ')
    }
}

#[inline]
fn write_indent<F: Write>(f: &mut F, newline: bool, indent: usize) -> fmt::Result {
    if newline {
        f.write_char('\n')?;
        for _ in 0..indent {
            f.write_char(' ')?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::MySQL;
    use crate::parser::expr::expr_sp0;
    use nom::error::Error;

    #[test]
    fn test_pretty_expr() -> anyhow::Result<()> {
        for c in vec![
            // literal
            ("1", "1", "1"),
            ("1.5", "1.5", "1.5"),
            ("'hello'", "'hello'", "'hello'"),
            ("b'0010'", "B'0010'", "b'0010'"),
            ("x'f2d3'", "X'f2d3'", "x'f2d3'"),
            ("date'2000-01-01'", "DATE '2000-01-01'", "date '2000-01-01'"),
            ("time '23:10:31'", "TIME '23:10:31'", "time '23:10:31'"),
            (
                "timeSTAMP '2021-12-06 05:00:01'",
                "TIMESTAMP '2021-12-06 05:00:01'",
                "timestamp '2021-12-06 05:00:01'",
            ),
            ("interval '1' DAY", "INTERVAL '1' DAY", "interval '1' day"),
            ("true", "TRUE", "true"),
            ("FALse", "FALSE", "false"),
            ("null", "NULL", "null"),
            // column reference
            ("a", "a", "a"),
            ("`a`", "`a`", "`a`"),
            ("`a`.b", "`a`.b", "`a`.b"),
            // aggregate function
            ("count(*)", "COUNT(*)", "count(*)"),
            ("count(a)", "COUNT(a)", "count(a)"),
            (
                "count( distinct a)",
                "COUNT(DISTINCT a)",
                "count(distinct a)",
            ),
            ("sum(1)", "SUM(1)", "sum(1)"),
            ("AVG(a)", "AVG(a)", "avg(a)"),
            ("min(a)", "MIN(a)", "min(a)"),
            ("MAX(a)", "MAX(a)", "max(a)"),
            // unary expression
            ("-a", "-a", "-a"),
            ("~1", "~1", "~1"),
            ("Not a", "NOT a", "not a"),
            // binary expression
            ("1 + a", "1 + a", "1 + a"),
            ("b-1", "b - 1", "b - 1"),
            ("1*3", "1 * 3", "1 * 3"),
            ("2.0 / x", "2.0 / x", "2.0 / x"),
            ("1+2+3", "1 + 2 + 3", "1 + 2 + 3"),
            ("1 * 2 - a", "1 * 2 - a", "1 * 2 - a"),
            ("a-b/10", "a - b / 10", "a - b / 10"),
            ("a*(b+1)", "a * (b + 1)", "a * (b + 1)"),
            ("1 << 2", "1 << 2", "1 << 2"),
            ("1 << 2 + 1", "1 << 2 + 1", "1 << 2 + 1"),
            ("1 >>(2 - 1)", "1 >> 2 - 1", "1 >> 2 - 1"),
            ("(1 << 2)+1", "(1 << 2) + 1", "(1 << 2) + 1"),
            // comparison
            ("a=1", "a = 1", "a = 1"),
            ("a = b+1", "a = b + 1", "a = b + 1"),
            ("a <=> null", "a <=> NULL", "a <=> null"),
            ("a >=x", "a >= x", "a >= x"),
            ("a > (x+1)", "a > x + 1", "a > x + 1"),
            ("a-1<=b", "a - 1 <= b", "a - 1 <= b"),
            ("a*x < b+ 1", "a * x < b + 1", "a * x < b + 1"),
            ("a != sum(1)", "a <> SUM(1)", "a <> sum(1)"),
            ("1&0", "1 & 0", "1 & 0"),
            ("1 | a", "1 | a", "1 | a"),
            ("x ^ y", "x ^ y", "x ^ y"),
            // match
            ("a like '123%'", "a LIKE '123%'", "a like '123%'"),
            ("a not LIKE 1", "a NOT LIKE 1", "a not like 1"),
            (
                "'xyz' regexp '.yz'",
                "'xyz' REGEXP '.yz'",
                "'xyz' regexp '.yz'",
            ),
            (
                "'xyz' NOT regexp '.yz'",
                "'xyz' NOT REGEXP '.yz'",
                "'xyz' not regexp '.yz'",
            ),
            // range
            ("`a` in (1,2,3)", "`a` IN (1, 2, 3)", "`a` in (1, 2, 3)"),
            (
                "(1,2) in ((1,2),(1,3))",
                "(1, 2) IN ((1, 2), (1, 3))",
                "(1, 2) in ((1, 2), (1, 3))",
            ),
            (
                "a in (select 1)",
                "a IN (\n    SELECT 1\n)",
                "a in (\n    select 1\n)",
            ),
            (
                "a not in ('x','y','z')",
                "a NOT IN ('x', 'y', 'z')",
                "a not in ('x', 'y', 'z')",
            ),
            (
                "1 between a and 100",
                "1 BETWEEN a AND 100",
                "1 between a and 100",
            ),
            (
                "x not BETWEEN 10 and 20",
                "x NOT BETWEEN 10 AND 20",
                "x not between 10 and 20",
            ),
            // is
            ("a is null", "a IS NULL", "a is null"),
            ("a is NOT null", "a IS NOT NULL", "a is not null"),
            ("1 is true", "1 IS TRUE", "1 is true"),
            ("1 is not true", "1 IS NOT TRUE", "1 is not true"),
            ("1+1=2 is false", "1 + 1 = 2 IS FALSE", "1 + 1 = 2 is false"),
            (
                "1+1=2 is not false",
                "1 + 1 = 2 IS NOT FALSE",
                "1 + 1 = 2 is not false",
            ),
            // quantified comparison
            (
                "a = ANY (select 1)",
                "a = ANY (\n    SELECT 1\n)",
                "a = any (\n    select 1\n)",
            ),
            (
                "0 < ALL (select 1)",
                "0 < ALL (\n    SELECT 1\n)",
                "0 < all (\n    select 1\n)",
            ),
            // predicates
            ("1 and 2", "1\nAND 2", "1\nand 2"),
            ("1 and 2 And 3", "1\nAND 2\nAND 3", "1\nand 2\nand 3"),
            (
                "1 and 2 or 3",
                "(\n    1\n    AND 2\n)\nOR 3",
                "(\n    1\n    and 2\n)\nor 3",
            ),
            (
                "1 and (2 or 3)",
                "1\nAND\n(\n    2\n    OR 3\n)",
                "1\nand\n(\n    2\n    or 3\n)",
            ),
            (
                "(1 or 2) and (3 or 4)",
                "(\n    1\n    OR 2\n)\nAND\n(\n    3\n    OR 4\n)",
                "(\n    1\n    or 2\n)\nand\n(\n    3\n    or 4\n)",
            ),
            ("1 xor 2 xor 3", "1\nXOR 2\nXOR 3", "1\nxor 2\nxor 3"),
            ("not 1 or 2", "NOT 1\nOR 2", "not 1\nor 2"),
            ("1=1 AND 2", "1 = 1\nAND 2", "1 = 1\nand 2"),
            ("1 and 1 > 2", "1\nAND 1 > 2", "1\nand 1 > 2"),
            // force enclosed on multi preds, even if paren can be eliminated
            (
                "(1 and 2) or (3 and 4)",
                "(\n    1\n    AND 2\n)\nOR\n(\n    3\n    AND 4\n)",
                "(\n    1\n    and 2\n)\nor\n(\n    3\n    and 4\n)",
            ),
            // exists
            (
                "exists(select 1)",
                "EXISTS (\n    SELECT 1\n)",
                "exists (\n    select 1\n)",
            ),
            (
                "not exists(select * from t)",
                "NOT EXISTS (\n    SELECT\n        *\n    FROM\n        t\n)",
                "not exists (\n    select\n        *\n    from\n        t\n)",
            ),
            // builtin
            (
                "extract(year from '2021-01-01')",
                "EXTRACT(YEAR FROM '2021-01-01')",
                "extract(year from '2021-01-01')",
            ),
            (
                "extract(quarter from a)",
                "EXTRACT(QUARTER FROM a)",
                "extract(quarter from a)",
            ),
            (
                "extract(MONTH from a)",
                "EXTRACT(MONTH FROM a)",
                "extract(month from a)",
            ),
            (
                "extract(weeK from a)",
                "EXTRACT(WEEK FROM a)",
                "extract(week from a)",
            ),
            (
                "extract(day from a)",
                "EXTRACT(DAY FROM a)",
                "extract(day from a)",
            ),
            (
                "extract(hour from a)",
                "EXTRACT(HOUR FROM a)",
                "extract(hour from a)",
            ),
            (
                "extract(minute from a)",
                "EXTRACT(MINUTE FROM a)",
                "extract(minute from a)",
            ),
            (
                "extract(second from a)",
                "EXTRACT(SECOND FROM a)",
                "extract(second from a)",
            ),
            (
                "extract(microsecond from a)",
                "EXTRACT(MICROSECOND FROM a)",
                "extract(microsecond from a)",
            ),
            (
                "substring('abc', 1)",
                "SUBSTRING('abc', 1)",
                "substring('abc', 1)",
            ),
            (
                "substring('abc', 1, 3)",
                "SUBSTRING('abc', 1, 3)",
                "substring('abc', 1, 3)",
            ),
            (
                "substring('abc' from 1)",
                "SUBSTRING('abc', 1)",
                "substring('abc', 1)",
            ),
            (
                "substring('abc' from 1 for 2)",
                "SUBSTRING('abc', 1, 2)",
                "substring('abc', 1, 2)",
            ),
            // case when
            (
                "case when a then b else 0 end",
                "CASE WHEN a THEN b ELSE 0 END",
                "case when a then b else 0 end",
            ),
            (
                "case a when 1 then 10 when 2 then 20 end",
                "CASE a WHEN 1 THEN 10 WHEN 2 THEN 20 END",
                "case a when 1 then 10 when 2 then 20 end",
            ),
            // scalar subquery
            ("(select 1)", "(\n    SELECT 1\n)", "(\n    select 1\n)"),
            (
                "(select count(*) from t)",
                "(\n    SELECT\n        COUNT(*)\n    FROM\n        t\n)",
                "(\n    select\n        count(*)\n    from\n        t\n)",
            ),
        ] {
            let (_, expr) = expr_sp0::<'_, _, Error<_>>(MySQL(c.0))?;
            let res1 = expr.pretty_string(PrettyConf::default())?;
            let res2 = expr.pretty_string(PrettyConf {
                upper_kw: false,
                ..Default::default()
            })?;
            assert_eq!(c.1, &res1);
            assert_eq!(c.2, &res2);
        }
        Ok(())
    }
}
