use crate::error::Result;
use crate::{Const, ExprKind};
use doradb_datatype::Decimal;

#[inline]
pub fn fold_neg(arg: &ExprKind) -> Result<Option<Const>> {
    match &arg {
        ExprKind::Const(Const::Null) => Ok(Some(Const::Null)),
        ExprKind::Const(c) => fold_neg_const(c),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_neg_const(arg: &Const) -> Result<Option<Const>> {
    let res = match arg {
        Const::I64(i) => {
            let mut d = Decimal::from(*i);
            if *i < 0 {
                d.set_pos()
            } else {
                d.set_neg()
            }
            match d.as_i64() {
                // in MySQL negating negative number returns decimal, here we
                // returns the i64 if possible
                Ok(new) => Const::I64(new),
                Err(_) => Const::Decimal(d),
            }
        }
        Const::U64(u) => {
            if *u == 0 {
                Const::U64(0)
            } else {
                let mut d = Decimal::from(*u);
                d.set_neg();
                match d.as_i64() {
                    Ok(new) => Const::I64(new),
                    Err(_) => Const::Decimal(d),
                }
            }
        }
        Const::F64(f) => Const::new_f64(-f.value()).unwrap(),
        Const::Decimal(d) => {
            if d.is_zero() {
                Const::Decimal(d.clone())
            } else {
                let mut res = d.clone();
                if d.is_neg() {
                    res.set_pos()
                } else {
                    res.set_neg()
                }
                Const::Decimal(res)
            }
        }
        Const::Bool(b) => {
            if *b {
                Const::I64(-1)
            } else {
                Const::I64(0)
            }
        }
        Const::Null => Const::Null,
        // todo: handle negating non-numeric values
        _ => return Ok(None),
    };
    Ok(Some(res))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::tests::{new_decimal, new_f64};
    use crate::Const::*;

    #[test]
    fn test_fold_neg() {
        assert_eq_fold_neg(I64(1), I64(-1));
        assert_eq_fold_neg(I64(-1), I64(1));
        assert_eq_fold_neg(
            I64(-9223372036854775808),
            new_decimal("9223372036854775808"),
        );
        assert_eq_fold_neg(U64(1), I64(-1));
        assert_eq_fold_neg(U64(9223372036854775808), I64(-9223372036854775808));
        assert_eq_fold_neg(
            U64(9223372036854775809),
            new_decimal("-9223372036854775809"),
        );
        assert_eq_fold_neg(new_f64(1.0), new_f64(-1.0));
        assert_eq_fold_neg(new_f64(-1.0), new_f64(1.0));
        assert_eq_fold_neg(new_decimal("1.0"), new_decimal("-1.0"));
        assert_eq_fold_neg(new_decimal("-1.0"), new_decimal("1.0"));
        assert_eq_fold_neg(Bool(true), I64(-1));
        assert_eq_fold_neg(Bool(false), I64(0));
        assert_eq_fold_neg(Null, Null);
    }

    fn assert_eq_fold_neg(c1: Const, c2: Const) {
        let res = fold_neg(&ExprKind::Const(c1)).unwrap().unwrap();
        assert_eq!(res, c2)
    }
}
