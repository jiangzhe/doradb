mod add;
mod cmp;
mod neg;
mod not;
mod sub;

use crate::error::Result;
use crate::{Const, Expr, ExprMutVisitor, Func, FuncKind, Pred, PredFunc, PredFuncKind};

pub use add::*;
pub use cmp::*;
pub use neg::*;
pub use not::*;
pub use sub::*;

/// General trait to wrap expressions to perform constant folding,
/// as well as checking whether an expression rejects null given
/// specific condition.
pub trait Fold: Sized {
    /// fold consumes self and returns any error if folding can
    /// be performed but fails.
    fn fold(self) -> Result<Expr> {
        self.replace_fold(|_| {})
    }

    fn replace_fold<F: Fn(&mut Expr)>(self, f: F) -> Result<Expr>;

    fn reject_null<F: Fn(&mut Expr)>(self, f: F) -> Result<bool> {
        self.replace_fold(f).map(|res| match res {
            Expr::Const(Const::Null) => true,
            Expr::Const(c) => c.is_zero().unwrap_or_default(),
            _ => false,
        })
    }
}

impl Fold for Expr {
    fn replace_fold<F: Fn(&mut Expr)>(mut self, f: F) -> Result<Expr> {
        let mut fe = FoldExpr(Ok(()), &f);
        let _ = self.walk_mut(&mut fe);
        fe.0.map(|_| self)
    }
}

struct FoldExpr<'a, F>(Result<()>, &'a F);

impl<'a, F> FoldExpr<'a, F> {
    fn update(&mut self, res: Result<Option<Const>>, e: &mut Expr) -> bool {
        match res {
            Err(e) => {
                self.0 = Err(e);
                false
            }
            Ok(Some(c)) => {
                *e = Expr::Const(c);
                true
            }
            Ok(None) => true,
        }
    }
}

impl<'a, F: Fn(&mut Expr)> ExprMutVisitor for FoldExpr<'a, F> {
    fn leave(&mut self, e: &mut Expr) -> bool {
        (self.1)(e);
        match e {
            Expr::Const(_) => true,
            Expr::Func(Func { kind, args }) => match kind {
                FuncKind::Neg => self.update(fold_neg(&args[0]), e),
                FuncKind::Add => self.update(fold_add(&args[0], &args[1]), e),
                FuncKind::Sub => self.update(fold_sub(&args[0], &args[1]), e),
                _ => true, // todo: fold more functions
            },
            Expr::Pred(Pred::Not(arg)) => self.update(fold_not(arg), e),
            Expr::Pred(Pred::Func(PredFunc { kind, args })) => match kind {
                PredFuncKind::Equal => self.update(fold_eq(&args[0], &args[1]), e),
                PredFuncKind::Greater => self.update(fold_gt(&args[0], &args[1]), e),
                PredFuncKind::GreaterEqual => self.update(fold_ge(&args[0], &args[1]), e),
                PredFuncKind::Less => self.update(fold_lt(&args[0], &args[1]), e),
                PredFuncKind::LessEqual => self.update(fold_le(&args[0], &args[1]), e),
                PredFuncKind::NotEqual => self.update(fold_ne(&args[0], &args[1]), e),
                PredFuncKind::SafeEqual => self.update(fold_safeeq(&args[0], &args[1]), e),
                PredFuncKind::IsNull => self.update(fold_isnull(&args[0]), e),
                PredFuncKind::IsNotNull => self.update(fold_isnotnull(&args[0]), e),
                PredFuncKind::IsTrue => self.update(fold_istrue(&args[0]), e),
                PredFuncKind::IsNotTrue => self.update(fold_isnottrue(&args[0]), e),
                PredFuncKind::IsFalse => self.update(fold_isfalse(&args[0]), e),
                PredFuncKind::IsNotFalse => self.update(fold_isnotfalse(&args[0]), e),
                _ => true,
            },
            _ => true, // todo: fold other expressions
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::Const::{self, *};
    use std::sync::Arc;

    pub(crate) fn new_f64(v: f64) -> Const {
        Const::new_f64(v).unwrap()
    }

    pub(crate) fn new_decimal(s: &str) -> Const {
        let d: xngin_datatype::Decimal = s.parse().unwrap();
        Decimal(d)
    }

    pub(crate) fn new_str(s: &str) -> Const {
        Const::String(Arc::from(s))
    }

    pub(crate) fn new_bytes(bs: &[u8]) -> Const {
        Const::Bytes(Arc::from(bs))
    }
}