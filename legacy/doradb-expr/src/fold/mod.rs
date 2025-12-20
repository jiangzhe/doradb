mod add;
mod cmp;
mod neg;
mod not;
mod sub;

use crate::controlflow::{ControlFlow, Unbranch};
use crate::error::{Error, Result};
use crate::expr_ext::ExprExt;
use crate::{Const, ExprKind, ExprMutVisitor, FuncKind, Pred, PredFuncKind};

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
    fn fold(self) -> Result<ExprKind> {
        self.replace_fold(|_| {})
    }

    fn replace_fold<F: Fn(&mut ExprKind)>(self, f: F) -> Result<ExprKind>;

    fn reject_null<F: Fn(&mut ExprKind)>(self, f: F) -> Result<bool> {
        self.replace_fold(f).map(|res| match &res {
            ExprKind::Const(Const::Null) => true,
            ExprKind::Const(c) => c.is_zero().unwrap_or_default(),
            _ => false,
        })
    }
}

impl Fold for ExprKind {
    fn replace_fold<F: Fn(&mut ExprKind)>(mut self, f: F) -> Result<ExprKind> {
        let mut fe = FoldExpr(&f);
        self.walk_mut(&mut fe).unbranch()?;
        Ok(self)
    }
}

struct FoldExpr<'a, F>(&'a F);

impl<F> FoldExpr<'_, F> {
    fn update(&mut self, res: Result<Option<Const>>, e: &mut ExprKind) -> ControlFlow<Error> {
        match res {
            Err(err) => ControlFlow::Break(err),
            Ok(Some(c)) => {
                *e = ExprKind::Const(c);
                ControlFlow::Continue(())
            }
            Ok(None) => ControlFlow::Continue(()),
        }
    }
}

impl<F: Fn(&mut ExprKind)> ExprMutVisitor for FoldExpr<'_, F> {
    type Cont = ();
    type Break = Error;
    fn leave(&mut self, e: &mut ExprKind) -> ControlFlow<Error> {
        (self.0)(e);
        match e {
            ExprKind::Const(_) => ControlFlow::Continue(()),
            ExprKind::Func { kind, args, .. } => match kind {
                FuncKind::Neg => self.update(fold_neg(&args[0]), e),
                FuncKind::Add => self.update(fold_add(&args[0], &args[1]), e),
                FuncKind::Sub => self.update(fold_sub(&args[0], &args[1]), e),
                _ => ControlFlow::Continue(()), // todo: fold more functions
            },
            ExprKind::Pred(Pred::Not(arg)) => self.update(fold_not(arg), e),
            ExprKind::Pred(Pred::Func { kind, args }) => match kind {
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
                _ => ControlFlow::Continue(()),
            },
            _ => ControlFlow::Continue(()), // todo: fold other expressions
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
        let d: doradb_datatype::Decimal = s.parse().unwrap();
        Decimal(d)
    }

    pub(crate) fn new_str(s: &str) -> Const {
        Const::String(Arc::from(s))
    }

    pub(crate) fn new_bytes(bs: &[u8]) -> Const {
        Const::Bytes(Arc::from(bs))
    }
}
