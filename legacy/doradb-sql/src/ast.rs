//! This module defines Abstract Syntax Tree(AST) of X-Engine.
use semistr::SemiStr;
use std::ops::{Add, Div, Mul, Neg, Sub};

pub trait KeywordString {
    fn kw_str(&self, upper: bool) -> &'static str;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatementKind {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    UseDB,
    Explain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement<'a> {
    // DML statements
    Select(QueryExpr<'a>),
    Insert(InsertExpr<'a>),
    Update(UpdateExpr<'a>),
    Delete(DeleteExpr<'a>),
    // DDL statements
    Create(Create<'a>),
    Drop(Drop<'a>),
    // Utility statements
    UseDB(UseDB<'a>),
    Explain(Explain<'a>),
}

impl Statement<'_> {
    /// Returns the kind of statement.
    #[inline]
    pub fn kind(&self) -> StatementKind {
        match self {
            Statement::Select(_) => StatementKind::Select,
            Statement::Insert(_) => StatementKind::Insert,
            Statement::Update(_) => StatementKind::Update,
            Statement::Delete(_) => StatementKind::Delete,
            Statement::Create(_) => StatementKind::Create,
            Statement::Drop(_) => StatementKind::Drop,
            Statement::UseDB(_) => StatementKind::UseDB,
            Statement::Explain(_) => StatementKind::Explain,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertExpr<'a> {
    pub target: TableName<'a>,
    pub cols: Vec<Ident<'a>>,
    pub source: InsertSource<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertSource<'a> {
    Values(Vec<Expr<'a>>),
    Query(Box<QueryExpr<'a>>),
}

impl<'a> InsertSource<'a> {
    #[inline]
    pub fn values(vals: Vec<Expr<'a>>) -> Self {
        Self::Values(vals)
    }

    #[inline]
    pub fn query(q: QueryExpr<'a>) -> Self {
        Self::Query(Box::new(q))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteExpr<'a> {
    pub target: TableName<'a>,
    pub cond: Option<Expr<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateExpr<'a> {
    pub target: TableName<'a>,
    pub acts: Vec<(Ident<'a>, Expr<'a>)>,
    pub cond: Option<Expr<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ident<'a> {
    pub kind: IdentKind,
    pub s: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdentKind {
    Regular,
    Quoted,
    AutoAlias,
}

impl<'a> Ident<'a> {
    #[inline]
    pub fn regular<T: Into<&'a str>>(s: T) -> Self {
        Ident {
            kind: IdentKind::Regular,
            s: s.into(),
        }
    }

    #[inline]
    pub fn quoted<T: Into<&'a str>>(s: T) -> Self {
        Ident {
            kind: IdentKind::Quoted,
            s: s.into(),
        }
    }

    #[inline]
    pub fn auto_alias<T: Into<&'a str>>(s: T) -> Self {
        Ident {
            kind: IdentKind::AutoAlias,
            s: s.into(),
        }
    }

    #[inline]
    pub fn as_semi_str(&self) -> SemiStr {
        // todo: handle escaped characters in delimited format
        SemiStr::new(self.s)
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        self.s
    }

    #[inline]
    pub fn to_lower(&self) -> SemiStr {
        // todo: handle escaped characters in delimited format
        SemiStr::from_iter(self.s.chars().map(|c| c.to_ascii_lowercase()))
    }

    #[inline]
    pub fn to_upper(&self) -> SemiStr {
        // todo: handle escaped characters in delimited format
        SemiStr::from_iter(self.s.chars().map(|c| c.to_ascii_uppercase()))
    }
}

impl<'a> From<&'a str> for Ident<'a> {
    fn from(s: &'a str) -> Self {
        Ident {
            kind: IdentKind::Regular,
            s,
        }
    }
}

impl AsRef<str> for Ident<'_> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.s
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableName<'a> {
    pub schema: Option<Ident<'a>>,
    pub table: Ident<'a>,
}

impl<'a> TableName<'a> {
    #[inline]
    pub fn new(schema: Option<Ident<'a>>, table: Ident<'a>) -> Self {
        TableName { schema, table }
    }
}

impl<'a> From<&'a str> for TableName<'a> {
    fn from(src: &'a str) -> Self {
        TableName::new(None, src.into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetQuantifier {
    All,
    Distinct,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedAsterisk<'a>(pub Vec<Ident<'a>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal<'a> {
    Numeric(&'a str),
    CharStr(CharStr<'a>),
    BitStr(&'a str),
    HexStr(&'a str),
    Date(&'a str),      // format validation must be applied
    Time(&'a str),      // format validation must be applied
    Timestamp(&'a str), // format validation must be applied
    Interval(Interval<'a>),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CharStr<'a> {
    pub charset: Option<&'a str>,
    pub first: &'a str,
    pub rest: Vec<&'a str>,
}

impl<'a> From<&'a str> for CharStr<'a> {
    fn from(src: &'a str) -> Self {
        CharStr {
            charset: None,
            first: src,
            rest: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Comment<'a> {
    Simple(&'a str),
    Bracketed(&'a str),
}

impl<'a> Comment<'a> {
    pub fn simple<T: Into<&'a str>>(value: T) -> Self {
        Comment::Simple(value.into())
    }

    pub fn bracketed<T: Into<&'a str>>(value: T) -> Self {
        Comment::Bracketed(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interval<'a> {
    pub unit: DatetimeUnit,
    pub value: &'a str,
}

/// adapt MySQL interval types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatetimeUnit {
    Microsecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl DatetimeUnit {
    #[inline]
    fn upper_str(&self) -> &'static str {
        match self {
            DatetimeUnit::Microsecond => "MICROSECOND",
            DatetimeUnit::Second => "SECOND",
            DatetimeUnit::Minute => "MINUTE",
            DatetimeUnit::Hour => "HOUR",
            DatetimeUnit::Day => "DAY",
            DatetimeUnit::Week => "WEEK",
            DatetimeUnit::Month => "MONTH",
            DatetimeUnit::Quarter => "QUARTER",
            DatetimeUnit::Year => "YEAR",
        }
    }

    #[inline]
    fn lower_str(&self) -> &'static str {
        match self {
            DatetimeUnit::Microsecond => "microsecond",
            DatetimeUnit::Second => "second",
            DatetimeUnit::Minute => "minute",
            DatetimeUnit::Hour => "hour",
            DatetimeUnit::Day => "day",
            DatetimeUnit::Week => "week",
            DatetimeUnit::Month => "month",
            DatetimeUnit::Quarter => "quarter",
            DatetimeUnit::Year => "year",
        }
    }
}

impl_kw_str!(DatetimeUnit);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr<'a> {
    Literal(Literal<'a>),
    ColumnRef(Vec<Ident<'a>>),
    AggrFunc(AggrFunc<'a>),
    Unary(Box<UnaryExpr<'a>>),
    Binary(Box<BinaryExpr<'a>>),
    Predicate(Box<Predicate<'a>>), // all predicates will be evaluated to bool expression
    Func(FuncExpr<'a>),
    FuncArg(ConstArg),
    CaseWhen(CaseWhen<'a>),
    ScalarSubquery(Box<QueryExpr<'a>>),
    Tuple(Vec<Expr<'a>>), // MySQL allow tuple expression
}

impl<'a> Add for Expr<'a> {
    type Output = Expr<'a>;
    #[inline]
    fn add(self, rhs: Expr<'a>) -> Self {
        Expr::binary(BinaryOp::Add, self, rhs)
    }
}

impl<'a> Sub for Expr<'a> {
    type Output = Expr<'a>;
    #[inline]
    fn sub(self, rhs: Expr<'a>) -> Self {
        Self::binary(BinaryOp::Sub, self, rhs)
    }
}

impl<'a> Mul for Expr<'a> {
    type Output = Expr<'a>;
    #[inline]
    fn mul(self, rhs: Expr<'a>) -> Self {
        Self::binary(BinaryOp::Mul, self, rhs)
    }
}

impl<'a> Div for Expr<'a> {
    type Output = Expr<'a>;
    #[inline]
    fn div(self, rhs: Expr<'a>) -> Self {
        Self::binary(BinaryOp::Div, self, rhs)
    }
}

impl<'a> Neg for Expr<'a> {
    type Output = Expr<'a>;
    #[inline]
    fn neg(self: Expr<'a>) -> Self {
        Self::unary(UnaryOp::Neg, self)
    }
}

impl<'a> Expr<'a> {
    pub fn numeric_lit<T: Into<&'a str>>(value: T) -> Self {
        Expr::Literal(Literal::Numeric(value.into()))
    }

    pub fn charstr_lit(value: CharStr<'a>) -> Self {
        Expr::Literal(Literal::CharStr(value))
    }

    pub fn bitstr_lit<T: Into<&'a str>>(value: T) -> Self {
        Expr::Literal(Literal::BitStr(value.into()))
    }

    pub fn hexstr_lit<T: Into<&'a str>>(value: T) -> Self {
        Expr::Literal(Literal::HexStr(value.into()))
    }

    pub fn date_lit<T: Into<&'a str>>(value: T) -> Self {
        Expr::Literal(Literal::Date(value.into()))
    }

    pub fn time_lit<T: Into<&'a str>>(value: T) -> Self {
        Expr::Literal(Literal::Time(value.into()))
    }

    pub fn timestamp_lit<T: Into<&'a str>>(value: T) -> Self {
        Expr::Literal(Literal::Timestamp(value.into()))
    }

    pub fn interval_lit<T: Into<&'a str>>(value: T, kind: DatetimeUnit) -> Self {
        Expr::Literal(Literal::Interval(Interval {
            unit: kind,
            value: value.into(),
        }))
    }

    #[inline]
    pub fn bool_lit(value: bool) -> Self {
        Expr::Literal(Literal::Bool(value))
    }

    #[inline]
    pub fn null_lit() -> Self {
        Expr::Literal(Literal::Null)
    }

    #[inline]
    pub fn logical_not(value: Expr<'a>) -> Self {
        Self::unary(UnaryOp::LogicalNot, value)
    }

    #[inline]
    pub fn bit_inv(value: Expr<'a>) -> Self {
        Self::unary(UnaryOp::BitInv, value)
    }

    #[inline]
    pub fn unary(op: UnaryOp, arg: Expr<'a>) -> Self {
        Self::Unary(Box::new(UnaryExpr { op, arg }))
    }

    #[inline]
    pub fn cmp_eq(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_cmp(CompareOp::Equal, lhs, rhs)
    }

    #[inline]
    pub fn cmp_ne(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_cmp(CompareOp::NotEqual, lhs, rhs)
    }

    #[inline]
    pub fn cmp_ge(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_cmp(CompareOp::GreaterEqual, lhs, rhs)
    }

    #[inline]
    pub fn cmp_gt(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_cmp(CompareOp::Greater, lhs, rhs)
    }

    #[inline]
    pub fn cmp_le(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_cmp(CompareOp::LessEqual, lhs, rhs)
    }

    #[inline]
    pub fn cmp_lt(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_cmp(CompareOp::Less, lhs, rhs)
    }

    #[inline]
    pub fn pred_cmp(op: CompareOp, lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::predicate(Predicate::Cmp(op, lhs, rhs))
    }

    #[inline]
    pub fn pred_quant_cmp(
        op: CompareOp,
        q: CmpQuantifier,
        lhs: Expr<'a>,
        sq: QueryExpr<'a>,
    ) -> Self {
        Self::predicate(Predicate::QuantCmp(op, q, lhs, Box::new(sq)))
    }

    #[inline]
    pub fn pred_safeeq(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_match(MatchOp::SafeEqual, lhs, rhs)
    }

    #[inline]
    pub fn pred_like(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_match(MatchOp::Like, lhs, rhs)
    }

    #[inline]
    pub fn pred_nlike(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_match(MatchOp::NotLike, lhs, rhs)
    }

    #[inline]
    pub fn pred_regexp(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_match(MatchOp::Regexp, lhs, rhs)
    }

    #[inline]
    pub fn pred_nregexp(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::pred_match(MatchOp::NotRegexp, lhs, rhs)
    }

    #[inline]
    pub fn pred_match(op: MatchOp, lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::predicate(Predicate::Match(op, lhs, rhs))
    }

    #[inline]
    pub fn pred_btw(lhs: Expr<'a>, mhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::predicate(Predicate::Between(lhs, mhs, rhs))
    }

    #[inline]
    pub fn pred_nbtw(lhs: Expr<'a>, mhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::predicate(Predicate::NotBetween(lhs, mhs, rhs))
    }

    #[inline]
    pub fn pred_in_subquery(lhs: Expr<'a>, subq: QueryExpr<'a>) -> Self {
        Self::predicate(Predicate::InSubquery(lhs, Box::new(subq)))
    }

    #[inline]
    pub fn pred_nin_subquery(lhs: Expr<'a>, subq: QueryExpr<'a>) -> Self {
        Self::predicate(Predicate::NotInSubquery(lhs, Box::new(subq)))
    }

    #[inline]
    pub fn pred_in_values(lhs: Expr<'a>, values: Vec<Expr<'a>>) -> Self {
        Self::predicate(Predicate::InValues(lhs, values))
    }

    #[inline]
    pub fn pred_nin_values(lhs: Expr<'a>, values: Vec<Expr<'a>>) -> Self {
        Self::predicate(Predicate::NotInValues(lhs, values))
    }

    #[inline]
    pub fn logical_and(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::predicate(Predicate::Conj(vec![lhs, rhs]))
    }

    #[inline]
    pub fn pred_conj(exprs: Vec<Expr<'a>>) -> Self {
        Self::predicate(Predicate::Conj(exprs))
    }

    #[inline]
    pub fn pred_is_null(value: Expr<'a>) -> Self {
        Self::pred_is(IsOp::Null, value)
    }

    #[inline]
    pub fn pred_not_null(value: Expr<'a>) -> Self {
        Self::pred_is(IsOp::NotNull, value)
    }

    #[inline]
    pub fn pred_is_true(value: Expr<'a>) -> Self {
        Self::pred_is(IsOp::True, value)
    }

    #[inline]
    pub fn pred_not_true(value: Expr<'a>) -> Self {
        Self::pred_is(IsOp::NotTrue, value)
    }

    #[inline]
    pub fn pred_is_false(value: Expr<'a>) -> Self {
        Self::pred_is(IsOp::False, value)
    }

    #[inline]
    pub fn pred_not_false(value: Expr<'a>) -> Self {
        Self::pred_is(IsOp::NotFalse, value)
    }

    #[inline]
    pub fn pred_is(op: IsOp, value: Expr<'a>) -> Self {
        Self::predicate(Predicate::Is(op, value))
    }

    #[inline]
    pub fn logical_or(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::predicate(Predicate::Disj(vec![lhs, rhs]))
    }

    #[inline]
    pub fn pred_disj(exprs: Vec<Expr<'a>>) -> Self {
        Self::predicate(Predicate::Disj(exprs))
    }

    #[inline]
    pub fn logical_xor(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::predicate(Predicate::LogicalXor(vec![lhs, rhs]))
    }

    #[inline]
    pub fn pred_xor(exprs: Vec<Expr<'a>>) -> Self {
        Self::predicate(Predicate::LogicalXor(exprs))
    }

    #[inline]
    pub fn bit_and(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::binary(BinaryOp::BitAnd, lhs, rhs)
    }

    #[inline]
    pub fn bit_or(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::binary(BinaryOp::BitOr, lhs, rhs)
    }

    #[inline]
    pub fn bit_xor(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::binary(BinaryOp::BitXor, lhs, rhs)
    }

    #[inline]
    pub fn bit_shl(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::binary(BinaryOp::BitShl, lhs, rhs)
    }

    #[inline]
    pub fn bit_shr(lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Self::binary(BinaryOp::BitShr, lhs, rhs)
    }

    #[inline]
    pub fn binary(op: BinaryOp, lhs: Expr<'a>, rhs: Expr<'a>) -> Self {
        Expr::Binary(Box::new(BinaryExpr { op, lhs, rhs }))
    }

    #[inline]
    pub fn predicate(pred: Predicate<'a>) -> Self {
        Expr::Predicate(Box::new(pred))
    }

    #[inline]
    pub fn column_ref(id: Vec<Ident<'a>>) -> Self {
        Expr::ColumnRef(id)
    }

    #[inline]
    pub fn count_asterisk() -> Self {
        Expr::AggrFunc(AggrFunc::count_asterisk())
    }

    #[inline]
    pub fn count(expr: Expr<'a>) -> Self {
        Expr::AggrFunc(AggrFunc::count(SetQuantifier::All, expr))
    }

    #[inline]
    pub fn count_distinct(expr: Expr<'a>) -> Self {
        Expr::AggrFunc(AggrFunc::count(SetQuantifier::Distinct, expr))
    }

    #[inline]
    pub fn sum(expr: Expr<'a>) -> Self {
        Expr::AggrFunc(AggrFunc::sum(SetQuantifier::All, expr))
    }

    #[inline]
    pub fn sum_distinct(expr: Expr<'a>) -> Self {
        Expr::AggrFunc(AggrFunc::sum(SetQuantifier::Distinct, expr))
    }

    #[inline]
    pub fn avg(expr: Expr<'a>) -> Self {
        Expr::AggrFunc(AggrFunc::avg(SetQuantifier::All, expr))
    }

    #[inline]
    pub fn min(expr: Expr<'a>) -> Self {
        Expr::AggrFunc(AggrFunc::min(SetQuantifier::All, expr))
    }

    #[inline]
    pub fn max(expr: Expr<'a>) -> Self {
        Expr::AggrFunc(AggrFunc::max(SetQuantifier::All, expr))
    }

    #[inline]
    pub fn func(ty: FuncType, args: Vec<Expr<'a>>) -> Self {
        Expr::Func(FuncExpr {
            ty,
            args,
            fname: None,
        })
    }

    #[inline]
    pub fn scalar_subquery(query: QueryExpr<'a>) -> Self {
        Expr::ScalarSubquery(Box::new(query))
    }

    #[inline]
    pub fn exists(query: QueryExpr<'a>) -> Self {
        Expr::predicate(Predicate::Exists(Box::new(query)))
    }

    #[inline]
    pub fn case_when(
        operand: Option<Box<Expr<'a>>>,
        branches: Vec<(Expr<'a>, Expr<'a>)>,
        fallback: Option<Box<Expr<'a>>>,
    ) -> Self {
        Expr::CaseWhen(CaseWhen {
            operand,
            branches,
            fallback,
        })
    }

    #[inline]
    pub fn into_conj(self) -> Vec<Self> {
        match self {
            Expr::Predicate(pred) if pred.is_conj() => pred.conj().unwrap(),
            _ => vec![self],
        }
    }

    #[inline]
    pub fn into_tuple(self) -> Option<Vec<Self>> {
        match self {
            Expr::Tuple(tu) => Some(tu),
            _ => None,
        }
    }

    #[inline]
    pub fn func_arg(&self) -> Option<&ConstArg> {
        match self {
            Expr::FuncArg(arg) => Some(arg),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryExpr<'a> {
    pub op: BinaryOp,
    pub lhs: Expr<'a>,
    pub rhs: Expr<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    BitAnd,
    BitOr,
    BitXor,
    BitShl,
    BitShr,
}

impl KeywordString for BinaryOp {
    fn kw_str(&self, _upper: bool) -> &'static str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::BitAnd => "&",
            BinaryOp::BitOr => "|",
            BinaryOp::BitXor => "^",
            BinaryOp::BitShl => "<<",
            BinaryOp::BitShr => ">>",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate<'a> {
    Cmp(CompareOp, Expr<'a>, Expr<'a>),
    QuantCmp(CompareOp, CmpQuantifier, Expr<'a>, Box<QueryExpr<'a>>),
    Is(IsOp, Expr<'a>),
    Match(MatchOp, Expr<'a>, Expr<'a>),
    InValues(Expr<'a>, Vec<Expr<'a>>),
    InSubquery(Expr<'a>, Box<QueryExpr<'a>>),
    NotInValues(Expr<'a>, Vec<Expr<'a>>),
    NotInSubquery(Expr<'a>, Box<QueryExpr<'a>>),
    Between(Expr<'a>, Expr<'a>, Expr<'a>),
    NotBetween(Expr<'a>, Expr<'a>, Expr<'a>),
    Exists(Box<QueryExpr<'a>>),
    Conj(Vec<Expr<'a>>),
    Disj(Vec<Expr<'a>>),
    LogicalXor(Vec<Expr<'a>>),
}

impl<'a> Predicate<'a> {
    #[inline]
    pub fn infix_op(&self) -> Option<PredicateOp> {
        let res = match self {
            Predicate::Cmp(op, ..) => PredicateOp::Cmp(*op),
            Predicate::InValues(..) | Predicate::InSubquery(..) => PredicateOp::In,
            Predicate::NotInValues(..) | Predicate::NotInSubquery(..) => PredicateOp::NotIn,
            Predicate::Between(..) => PredicateOp::Between,
            Predicate::NotBetween(..) => PredicateOp::NotBetween,
            Predicate::Conj(..) => PredicateOp::LogicalAnd,
            Predicate::Disj(..) => PredicateOp::LogicalOr,
            Predicate::LogicalXor(..) => PredicateOp::LogicalXor,
            _ => return None,
        };
        Some(res)
    }

    #[inline]
    pub fn is_conj(&self) -> bool {
        matches!(self, Predicate::Conj(_))
    }

    #[inline]
    pub fn conj(self) -> Option<Vec<Expr<'a>>> {
        match self {
            Predicate::Conj(es) => Some(es),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    Cmp(CompareOp),
    Match(MatchOp),
    In,
    NotIn,
    Between,
    NotBetween,
    LogicalAnd,
    LogicalOr,
    LogicalXor,
}

impl PredicateOp {
    #[inline]
    fn upper_str(self) -> &'static str {
        match self {
            PredicateOp::Cmp(cmp) => cmp.kw_str(true),
            PredicateOp::Match(mth) => mth.upper_str(),
            PredicateOp::In => "IN",
            PredicateOp::NotIn => "NOT IN",
            PredicateOp::Between => "BETWEEN", // put between as binary op, and handle special "and" accordingly
            PredicateOp::NotBetween => "NOT BETWEEN",
            PredicateOp::LogicalAnd => "AND",
            PredicateOp::LogicalOr => "OR",
            PredicateOp::LogicalXor => "XOR",
        }
    }

    #[inline]
    fn lower_str(self) -> &'static str {
        match self {
            PredicateOp::Cmp(cmp) => cmp.kw_str(false),
            PredicateOp::Match(mth) => mth.lower_str(),
            PredicateOp::In => "in",
            PredicateOp::NotIn => "not in",
            PredicateOp::Between => "between", // put between as binary op, and handle special "and" accordingly
            PredicateOp::NotBetween => "not between",
            PredicateOp::LogicalAnd => "and",
            PredicateOp::LogicalOr => "or",
            PredicateOp::LogicalXor => "xor",
        }
    }
}

impl_kw_str!(PredicateOp);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Equal,
    GreaterEqual,
    Greater,
    LessEqual,
    Less,
    NotEqual,
}

impl KeywordString for CompareOp {
    fn kw_str(&self, _upper: bool) -> &'static str {
        match self {
            CompareOp::Equal => "=",
            CompareOp::GreaterEqual => ">=",
            CompareOp::Greater => ">",
            CompareOp::LessEqual => "<=",
            CompareOp::Less => "<",
            CompareOp::NotEqual => "<>",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpQuantifier {
    All,
    Any, // Some is alias of any
}

impl CmpQuantifier {
    #[inline]
    pub fn upper_str(self) -> &'static str {
        match self {
            CmpQuantifier::All => "ALL",
            CmpQuantifier::Any => "ANY",
        }
    }

    #[inline]
    pub fn lower_str(self) -> &'static str {
        match self {
            CmpQuantifier::All => "all",
            CmpQuantifier::Any => "any",
        }
    }
}

impl_kw_str!(CmpQuantifier);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    SafeEqual,
    Like,
    NotLike,
    Regexp,
    NotRegexp,
}

impl MatchOp {
    #[inline]
    fn upper_str(self) -> &'static str {
        match self {
            MatchOp::SafeEqual => "<=>",
            MatchOp::Like => "LIKE",
            MatchOp::NotLike => "NOT LIKE",
            MatchOp::Regexp => "REGEXP",
            MatchOp::NotRegexp => "NOT REGEXP",
        }
    }

    #[inline]
    fn lower_str(self) -> &'static str {
        match self {
            MatchOp::SafeEqual => "<=>",
            MatchOp::Like => "like",
            MatchOp::NotLike => "not like",
            MatchOp::Regexp => "regexp",
            MatchOp::NotRegexp => "not regexp",
        }
    }
}

impl_kw_str!(MatchOp);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsOp {
    Null,
    NotNull,
    True,
    NotTrue,
    False,
    NotFalse,
}

impl IsOp {
    #[inline]
    fn upper_str(self) -> &'static str {
        match self {
            IsOp::Null => "IS NULL",
            IsOp::NotNull => "IS NOT NULL",
            IsOp::True => "IS TRUE",
            IsOp::NotTrue => "IS NOT TRUE",
            IsOp::False => "IS FALSE",
            IsOp::NotFalse => "IS NOT FALSE",
        }
    }

    #[inline]
    fn lower_str(self) -> &'static str {
        match self {
            IsOp::Null => "is null",
            IsOp::NotNull => "is not null",
            IsOp::True => "is true",
            IsOp::NotTrue => "is not true",
            IsOp::False => "is false",
            IsOp::NotFalse => "is not false",
        }
    }
}

impl_kw_str!(IsOp);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuncExpr<'a> {
    pub ty: FuncType,
    pub args: Vec<Expr<'a>>,
    // optional function name input by user.
    // it can identify UDF at runtime.
    pub fname: Option<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FuncType {
    // extract(unit from datetime)
    Extract,
    // substring(src, start), substring(src FROM start)
    // substring(src, start, len), substring(src FROM start FOR len)
    Substring,
}

impl FuncType {
    #[inline]
    fn upper_str(&self) -> &'static str {
        match self {
            FuncType::Extract => "EXTRACT",
            FuncType::Substring => "SUBSTRING",
        }
    }

    #[inline]
    fn lower_str(&self) -> &'static str {
        match self {
            FuncType::Extract => "extract",
            FuncType::Substring => "substring",
        }
    }
}

impl_kw_str!(FuncType);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstArg {
    // placeholder for absent argument.
    None,
    DatetimeUnit(DatetimeUnit),
}

impl ConstArg {
    #[inline]
    pub fn datetime_unit(&self) -> Option<DatetimeUnit> {
        match self {
            ConstArg::DatetimeUnit(unit) => Some(*unit),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnaryExpr<'a> {
    pub op: UnaryOp,
    pub arg: Expr<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    BitInv,
    Neg,
    LogicalNot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Builtin<'a> {
    // extract(unit from datetime)
    Extract(Box<Expr<'a>>, Box<Expr<'a>>),
    // substring(src, start), substring(src FROM start)
    // substring(src, start, len), substring(src FROM start FOR len)
    Substring(Box<Expr<'a>>, Box<Expr<'a>>, Option<Box<Expr<'a>>>),
}

impl Builtin<'_> {
    #[inline]
    fn upper_str(&self) -> &'static str {
        match self {
            Builtin::Extract(..) => "EXTRACT",
            Builtin::Substring(..) => "SUBSTRING",
        }
    }

    #[inline]
    fn lower_str(&self) -> &'static str {
        match self {
            Builtin::Extract(..) => "extract",
            Builtin::Substring(..) => "substring",
        }
    }
}

impl_kw_str!(Builtin<'_>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivedCol<'a> {
    Asterisk(Vec<Ident<'a>>),
    // expression and alias.
    Expr(Expr<'a>, Ident<'a>),
}

impl<'a> DerivedCol<'a> {
    /// Create a new derived column with alias.
    #[inline]
    pub fn new(expr: Expr<'a>, alias: Ident<'a>) -> Self {
        DerivedCol::Expr(expr, alias)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggrFunc<'a> {
    pub kind: AggrFuncKind,
    pub q: SetQuantifier,
    pub expr: Box<Expr<'a>>,
}

impl<'a> AggrFunc<'a> {
    #[inline]
    pub fn count_asterisk() -> Self {
        AggrFunc {
            kind: AggrFuncKind::CountAsterisk,
            q: SetQuantifier::All,
            expr: Box::new(Expr::numeric_lit("1")),
        }
    }

    #[inline]
    pub fn count(q: SetQuantifier, expr: Expr<'a>) -> Self {
        AggrFunc {
            kind: AggrFuncKind::Count,
            q,
            expr: Box::new(expr),
        }
    }

    #[inline]
    pub fn sum(q: SetQuantifier, expr: Expr<'a>) -> Self {
        AggrFunc {
            kind: AggrFuncKind::Sum,
            q,
            expr: Box::new(expr),
        }
    }

    #[inline]
    pub fn avg(q: SetQuantifier, expr: Expr<'a>) -> Self {
        AggrFunc {
            kind: AggrFuncKind::Avg,
            q,
            expr: Box::new(expr),
        }
    }

    #[inline]
    pub fn min(q: SetQuantifier, expr: Expr<'a>) -> Self {
        AggrFunc {
            kind: AggrFuncKind::Min,
            q,
            expr: Box::new(expr),
        }
    }

    #[inline]
    pub fn max(q: SetQuantifier, expr: Expr<'a>) -> Self {
        AggrFunc {
            kind: AggrFuncKind::Max,
            q,
            expr: Box::new(expr),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggrFuncKind {
    CountAsterisk,
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

impl AggrFuncKind {
    #[inline]
    pub fn upper_str(self) -> &'static str {
        match self {
            AggrFuncKind::CountAsterisk => "COUNT(*)",
            AggrFuncKind::Count => "COUNT",
            AggrFuncKind::Sum => "SUM",
            AggrFuncKind::Avg => "AVG",
            AggrFuncKind::Max => "MAX",
            AggrFuncKind::Min => "MIN",
        }
    }

    #[inline]
    pub fn lower_str(self) -> &'static str {
        match self {
            AggrFuncKind::CountAsterisk => "count(*)",
            AggrFuncKind::Count => "count",
            AggrFuncKind::Sum => "sum",
            AggrFuncKind::Avg => "avg",
            AggrFuncKind::Max => "max",
            AggrFuncKind::Min => "min",
        }
    }
}

impl_kw_str!(AggrFuncKind);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseWhen<'a> {
    pub operand: Option<Box<Expr<'a>>>,
    pub branches: Vec<(Expr<'a>, Expr<'a>)>,
    pub fallback: Option<Box<Expr<'a>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryExpr<'a> {
    pub with: Option<With<'a>>,
    pub query: Query<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct With<'a> {
    pub recursive: bool,
    pub elements: Vec<WithElement<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithElement<'a> {
    pub name: Ident<'a>,
    pub cols: Vec<Ident<'a>>,
    pub query_expr: QueryExpr<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query<'a> {
    Set(SelectSet<'a>),
    Table(Box<SelectTable<'a>>),
    Row(Vec<DerivedCol<'a>>),
}

impl<'a> Query<'a> {
    #[inline]
    pub fn row(cols: Vec<DerivedCol<'a>>) -> Self {
        Query::Row(cols)
    }

    #[inline]
    pub fn table(table: SelectTable<'a>) -> Self {
        Query::Table(Box::new(table))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectSet<'a> {
    pub op: SetOp,
    pub left: Box<Query<'a>>,
    pub right: Box<Query<'a>>,
    pub distinct: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Except,
    Intersect,
}

impl SetOp {
    #[inline]
    fn upper_str(self) -> &'static str {
        match self {
            SetOp::Union => "UNION",
            SetOp::Except => "EXCEPT",
            SetOp::Intersect => "INTERSECT",
        }
    }

    #[inline]
    fn lower_str(self) -> &'static str {
        match self {
            SetOp::Union => "union",
            SetOp::Except => "except",
            SetOp::Intersect => "intersect",
        }
    }
}

impl_kw_str!(SetOp);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectTable<'a> {
    pub q: SetQuantifier,
    pub cols: Vec<DerivedCol<'a>>,
    pub from: Vec<TableRef<'a>>,
    pub filter: Option<Expr<'a>>,
    pub group_by: Vec<Expr<'a>>,
    pub having: Option<Expr<'a>>,
    // todo: window
    pub order_by: Vec<OrderElement<'a>>,
    pub limit: Option<Limit>,
}

// todo: specify null ordering (MySQL does not support currently)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderElement<'a> {
    pub expr: Expr<'a>,
    pub desc: bool,
}

impl<'a> OrderElement<'a> {
    #[inline]
    pub fn new(expr: Expr<'a>, desc: bool) -> Self {
        OrderElement { expr, desc }
    }

    #[inline]
    pub fn asc(expr: Expr<'a>) -> Self {
        OrderElement { expr, desc: false }
    }

    #[inline]
    pub fn desc(expr: Expr<'a>) -> Self {
        OrderElement { expr, desc: true }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    pub limit: u64,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    #[inline]
    fn upper_str(self) -> &'static str {
        match self {
            JoinType::Inner => "JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Full => "FULL JOIN",
        }
    }

    #[inline]
    fn lower_str(self) -> &'static str {
        match self {
            JoinType::Inner => "join",
            JoinType::Left => "left join",
            JoinType::Right => "right jion",
            JoinType::Full => "full join",
        }
    }
}

impl_kw_str!(JoinType);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableJoin<'a> {
    Cross(CrossJoin<'a>),
    Natural(NaturalJoin<'a>),
    Qualified(QualifiedJoin<'a>),
}

impl<'a> TableJoin<'a> {
    #[inline]
    pub fn cross(left: TableRef<'a>, right: TablePrimitive<'a>) -> Self {
        TableJoin::Cross(CrossJoin { left, right })
    }

    #[inline]
    pub fn natural(left: TableRef<'a>, right: TablePrimitive<'a>, ty: JoinType) -> Self {
        TableJoin::Natural(NaturalJoin { left, right, ty })
    }

    #[inline]
    pub fn qualified(
        left: TableRef<'a>,
        right: TablePrimitive<'a>,
        ty: JoinType,
        cond: Option<JoinCondition<'a>>,
    ) -> Self {
        TableJoin::Qualified(QualifiedJoin {
            left,
            right,
            ty,
            cond,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossJoin<'a> {
    pub left: TableRef<'a>,
    pub right: TablePrimitive<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NaturalJoin<'a> {
    pub left: TableRef<'a>,
    pub right: TablePrimitive<'a>,
    pub ty: JoinType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedJoin<'a> {
    pub left: TableRef<'a>,
    pub right: TablePrimitive<'a>,
    pub ty: JoinType,
    pub cond: Option<JoinCondition<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinCondition<'a> {
    NamedCols(Vec<Ident<'a>>),
    Conds(Expr<'a>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableRef<'a> {
    Primitive(Box<TablePrimitive<'a>>),
    Joined(Box<TableJoin<'a>>),
}

impl<'a> TableRef<'a> {
    #[inline]
    pub fn primitive(table: TablePrimitive<'a>) -> Self {
        TableRef::Primitive(Box::new(table))
    }

    #[inline]
    pub fn joined(join: TableJoin<'a>) -> Self {
        TableRef::Joined(Box::new(join))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TablePrimitive<'a> {
    Named(TableName<'a>, Option<Ident<'a>>),
    Derived(Box<QueryExpr<'a>>, Ident<'a>),
}

impl<'a> TablePrimitive<'a> {
    #[inline]
    pub fn derived(query: QueryExpr<'a>, alias: Ident<'a>) -> Self {
        Self::Derived(Box::new(query), alias)
    }

    #[inline]
    pub fn alias(&self) -> Ident<'a> {
        match self {
            TablePrimitive::Derived(_, alias) => *alias,
            TablePrimitive::Named(tn, alias) => {
                if let Some(alias) = alias.as_ref() {
                    *alias
                } else {
                    tn.table
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Create<'a> {
    Table {
        if_not_exists: bool,
        definition: TableDefinition<'a>,
    },
    Database {
        if_not_exists: bool,
        definition: DatabaseDefinition<'a>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseDefinition<'a> {
    pub name: Ident<'a>,
    pub charset: Option<Ident<'a>>,
    pub collate: Option<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDefinition<'a> {
    pub name: TableName<'a>,
    pub elems: Vec<TableElement<'a>>,
    // pub constraint_defs: Vec<TableConstraintDefinition<'a>>,
    // pub table_options: Vec<TableOption<'a>>,
    // pub partition_options: Vec<PartitionOption<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableElement<'a> {
    Column {
        name: Ident<'a>,
        ty: DataType,
        not_null: bool,
        default: Option<ColumnDefault<'a>>,
        auto_increment: bool,
        unique: bool,
        primary_key: bool,
        comment: Option<&'a str>,
        collate: Option<CollationType>,
    },
    Key {
        ty: TableKeyType,
        name: Option<Ident<'a>>,
        keys: Vec<KeyPart<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableKeyType {
    PrimaryKey,
    UniqueKey,
    Index,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDefault<'a> {
    Literal(Literal<'a>),
    Func(FuncExpr<'a>),
    User,
    CurrentUser,
    CurrentRole,
    SessionUser,
    SystemUser,
    CurrentPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPart<'a> {
    pub col_name: Ident<'a>,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Bigint(bool),
    Int(bool),
    Smallint(bool),
    Tinyint(bool),
    Varchar(u16),
    Char(u16),
    Decimal(Option<u8>, Option<u8>),
    Date,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollationType {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drop<'a> {
    Table {
        if_exists: bool,
        names: Vec<TableName<'a>>,
    },
    Database {
        if_exists: bool,
        name: Ident<'a>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseDB<'a> {
    pub name: Ident<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Explain<'a> {
    Select(QueryExpr<'a>),
    Insert(InsertExpr<'a>),
    Update(UpdateExpr<'a>),
    Delete(DeleteExpr<'a>),
}
