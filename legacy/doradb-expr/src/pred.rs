use crate::expr::ExprKind;

/// Predicate represents filter conditions in conjunctive form.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Pred {
    Conj(Vec<ExprKind>),
    Disj(Vec<ExprKind>),
    Xor(Vec<ExprKind>),
    // Wrapped expression must return bool.
    // It can be achieved by adding cast function.
    Func {
        kind: PredFuncKind,
        args: Vec<ExprKind>,
    },
    Not(Box<ExprKind>),
    InSubquery(Box<ExprKind>, Box<ExprKind>),
    NotInSubquery(Box<ExprKind>, Box<ExprKind>),
    Exists(Box<ExprKind>),
    NotExists(Box<ExprKind>),
}

impl Pred {
    #[inline]
    pub fn func(kind: PredFuncKind, args: Vec<ExprKind>) -> Self {
        debug_assert_eq!(kind.n_args(), args.len());
        Pred::Func { kind, args }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PredFuncKind {
    // comparison
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    NotEqual,
    // special comparison: is
    IsNull,
    IsNotNull,
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    // special comparison: match
    SafeEqual,
    Like,
    NotLike,
    Regexp,
    NotRegexp,
    // in values
    // layout of arguments: first arg is lhs, others are in list.
    InValues,
    NotInValues,
    Between,
    NotBetween,
}

impl PredFuncKind {
    #[inline]
    pub fn to_lower(&self) -> &'static str {
        use PredFuncKind::*;
        match self {
            Equal => "eq",
            Greater => "gt",
            GreaterEqual => "ge",
            Less => "lt",
            LessEqual => "le",
            NotEqual => "ne",
            IsNull => "isnull",
            IsNotNull => "isnotnull",
            IsTrue => "istrue",
            IsNotTrue => "isnottrue",
            IsFalse => "isfalse",
            IsNotFalse => "isnotfalse",
            SafeEqual => "safeeq",
            Like => "like",
            NotLike => "notlike",
            Regexp => "regexp",
            NotRegexp => "notregexp",
            InValues => "in",
            NotInValues => "notin",
            Between => "between",
            NotBetween => "notbetween",
        }
    }

    #[inline]
    pub fn n_args(&self) -> usize {
        use PredFuncKind::*;
        match self {
            Equal | Greater | GreaterEqual | Less | LessEqual | NotEqual | SafeEqual | Like
            | NotLike | Regexp | NotRegexp | InValues | NotInValues => 2,
            IsNull | IsNotNull | IsTrue | IsNotTrue | IsFalse | IsNotFalse => 1,
            Between | NotBetween => 3,
        }
    }

    // flip kind logically by NOT operator
    #[inline]
    pub fn logic_flip(&self) -> Option<Self> {
        use PredFuncKind::*;
        let res = match self {
            Equal => NotEqual,
            Greater => LessEqual,
            GreaterEqual => Less,
            Less => GreaterEqual,
            LessEqual => Greater,
            NotEqual => Equal,
            IsNull => IsNotNull,
            IsNotNull => IsNull,
            IsTrue => IsNotTrue,
            IsNotTrue => IsTrue,
            IsFalse => IsNotFalse,
            IsNotFalse => IsFalse,
            SafeEqual => return None,
            Like => NotLike,
            NotLike => Like,
            Regexp => NotRegexp,
            NotRegexp => Regexp,
            InValues => NotInValues,
            NotInValues => InValues,
            Between => NotBetween,
            NotBetween => Between,
        };
        Some(res)
    }

    // flip kind by swap operand by position
    #[inline]
    pub fn pos_flip(&self) -> Option<Self> {
        use PredFuncKind::*;
        let res = match self {
            Equal => Equal,
            Greater => Less,
            GreaterEqual => LessEqual,
            Less => Greater,
            LessEqual => GreaterEqual,
            NotEqual => NotEqual,
            _ => return None,
        };
        Some(res)
    }
}
