use crate::error::Result;
use crate::{Const, ExprKind};

#[inline]
pub fn fold_not(arg: &ExprKind) -> Result<Option<Const>> {
    match arg {
        ExprKind::Const(Const::Null) => Ok(Some(Const::Null)),
        ExprKind::Const(c) => fold_not_const(c),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_not_const(arg: &Const) -> Result<Option<Const>> {
    Ok(arg.is_zero().map(Const::Bool))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::tests::{new_decimal, new_f64};
    use crate::Const::*;

    #[test]
    fn test_fold_not() {
        assert_eq_fold_not(I64(1), Bool(false));
        assert_eq_fold_not(I64(0), Bool(true));
        assert_eq_fold_not(U64(1), Bool(false));
        assert_eq_fold_not(U64(0), Bool(true));
        assert_eq_fold_not(new_f64(1.0), Bool(false));
        assert_eq_fold_not(new_f64(0.0), Bool(true));
        assert_eq_fold_not(new_decimal("1.0"), Bool(false));
        assert_eq_fold_not(new_decimal("0.0"), Bool(true));
        assert_eq_fold_not(Bool(true), Bool(false));
        assert_eq_fold_not(Bool(false), Bool(true));
        assert_eq_fold_not(Null, Null);
    }

    fn assert_eq_fold_not(c1: Const, c2: Const) {
        let res = fold_not(&ExprKind::Const(c1)).unwrap().unwrap();
        assert_eq!(res, c2)
    }
}
