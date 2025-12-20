use crate::error::Result;
use crate::{Const, ExprKind};
use doradb_datatype::AlignPartialOrd;
use std::cmp::Ordering;

macro_rules! impl_fold_cmp {
    ( $fn:ident, $fnc:ident, $($e:pat),* ) => {
        #[inline]
        pub fn $fn(lhs: &ExprKind, rhs: &ExprKind) -> Result<Option<Const>> {
            match (lhs, rhs) {
                (ExprKind::Const(Const::Null), _) | (_, ExprKind::Const(Const::Null)) => Ok(Some(Const::Null)),
                (ExprKind::Const(c1), ExprKind::Const(c2)) => $fnc(c1, c2),
                _ => Ok(None),
            }
        }

        #[inline]
        pub fn $fnc(lhs: &Const, rhs: &Const) -> Result<Option<Const>> {
            let res = match lhs.align_partial_cmp(rhs) {
                None => return Ok(None),
                $(
                    Some($e) => Const::Bool(true),
                )*
                _ => Const::Bool(false),
            };
            Ok(Some(res))
        }
    }
}

impl_fold_cmp!(fold_eq, fold_eq_const, Ordering::Equal);
impl_fold_cmp!(fold_gt, fold_gt_const, Ordering::Greater);
impl_fold_cmp!(fold_ge, fold_ge_const, Ordering::Greater, Ordering::Equal);
impl_fold_cmp!(fold_lt, fold_lt_const, Ordering::Less);
impl_fold_cmp!(fold_le, fold_le_const, Ordering::Less, Ordering::Equal);
impl_fold_cmp!(fold_ne, fold_ne_const, Ordering::Less, Ordering::Greater);

#[inline]
pub fn fold_safeeq(lhs: &ExprKind, rhs: &ExprKind) -> Result<Option<Const>> {
    match (lhs, rhs) {
        (ExprKind::Const(c1), ExprKind::Const(c2)) => fold_safeeq_const(c1, c2),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_safeeq_const(lhs: &Const, rhs: &Const) -> Result<Option<Const>> {
    let res = match (lhs, rhs) {
        (Const::Null, Const::Null) => Const::Bool(true),
        (Const::Null, _) => Const::Bool(false),
        (_, Const::Null) => Const::Bool(false),
        (c1, c2) => match c1.align_partial_cmp(c2) {
            None => return Ok(None),
            Some(Ordering::Equal) => Const::Bool(true),
            _ => Const::Bool(false),
        },
    };
    Ok(Some(res))
}

#[inline]
pub fn fold_isnull(arg: &ExprKind) -> Result<Option<Const>> {
    match arg {
        ExprKind::Const(c) => fold_isnull_const(c),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_isnull_const(arg: &Const) -> Result<Option<Const>> {
    let res = match arg {
        Const::Null => Const::Bool(true),
        _ => Const::Bool(false),
    };
    Ok(Some(res))
}

#[inline]
pub fn fold_isnotnull(arg: &ExprKind) -> Result<Option<Const>> {
    match arg {
        ExprKind::Const(c) => fold_isnotnull_const(c),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_isnotnull_const(arg: &Const) -> Result<Option<Const>> {
    let res = match arg {
        Const::Null => Const::Bool(false),
        _ => Const::Bool(true),
    };
    Ok(Some(res))
}

#[inline]
pub fn fold_istrue(arg: &ExprKind) -> Result<Option<Const>> {
    match arg {
        ExprKind::Const(c) => fold_istrue_const(c),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_istrue_const(arg: &Const) -> Result<Option<Const>> {
    Ok(fold_is_bool_const(arg, false, false))
}

#[inline]
pub fn fold_isnottrue(arg: &ExprKind) -> Result<Option<Const>> {
    match arg {
        ExprKind::Const(c) => fold_isnottrue_const(c),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_isnottrue_const(arg: &Const) -> Result<Option<Const>> {
    Ok(fold_is_bool_const(arg, true, true))
}

#[inline]
pub fn fold_isfalse(arg: &ExprKind) -> Result<Option<Const>> {
    match arg {
        ExprKind::Const(c) => fold_isfalse_const(c),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_isfalse_const(arg: &Const) -> Result<Option<Const>> {
    Ok(fold_is_bool_const(arg, true, false))
}

#[inline]
pub fn fold_isnotfalse(arg: &ExprKind) -> Result<Option<Const>> {
    match arg {
        ExprKind::Const(c) => fold_isnotfalse_const(c),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_isnotfalse_const(arg: &Const) -> Result<Option<Const>> {
    Ok(fold_is_bool_const(arg, false, true))
}

#[inline]
fn fold_is_bool_const(arg: &Const, zero_as: bool, null_as: bool) -> Option<Const> {
    match arg {
        Const::Null => Some(Const::Bool(null_as)),
        c => c.is_zero().map(|zero| Const::Bool(zero == zero_as)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::tests::{new_bytes, new_decimal, new_f64, new_str};
    use crate::Const::*;
    use std::sync::Arc;

    macro_rules! assert_fold_cmp_null {
        ($e1:expr, $e2:expr) => {
            assert_eq_fold_eq($e1, $e2, Null);
            assert_eq_fold_gt($e1, $e2, Null);
            assert_eq_fold_ge($e1, $e2, Null);
            assert_eq_fold_lt($e1, $e2, Null);
            assert_eq_fold_le($e1, $e2, Null);
            assert_eq_fold_ne($e1, $e2, Null);
        };
    }

    #[test]
    fn test_fold_cmp_null() {
        assert_fold_cmp_null!(Null, I64(1));
        assert_fold_cmp_null!(Null, U64(1));
        assert_fold_cmp_null!(Null, new_f64(1.0));
        assert_fold_cmp_null!(Null, new_decimal("1.0"));
        assert_fold_cmp_null!(Null, Bool(true));
        assert_fold_cmp_null!(Null, String(Arc::from("abc")));
        assert_fold_cmp_null!(Null, Bytes(Arc::from("abc".as_bytes())));
        assert_fold_cmp_null!(I64(1), Null);
        assert_fold_cmp_null!(U64(1), Null);
        assert_fold_cmp_null!(new_f64(1.0), Null);
        assert_fold_cmp_null!(new_decimal("1.0"), Null);
        assert_fold_cmp_null!(Bool(true), Null);
        assert_fold_cmp_null!(new_str("abc"), Null);
        assert_fold_cmp_null!(new_bytes(b"abc"), Null);
        // todo: date, time, datetime
        assert_fold_cmp_null!(Null, Null);

        assert_eq_fold_se(Null, Null, Bool(true));
        assert_eq_fold_se(Null, I64(1), Bool(false));
        assert_eq_fold_se(I64(1), Null, Bool(false));
    }

    #[test]
    fn test_fold_cmp_non_null() {
        // i64
        assert_eq_fold_eq(I64(1), I64(1), Bool(true));
        assert_eq_fold_eq(I64(1), I64(-1), Bool(false));
        assert_eq_fold_gt(I64(1), I64(1), Bool(false));
        assert_eq_fold_gt(I64(1), I64(-1), Bool(true));
        assert_eq_fold_ge(I64(1), I64(1), Bool(true));
        assert_eq_fold_ge(I64(1), I64(-1), Bool(true));
        assert_eq_fold_ge(I64(-1), I64(1), Bool(false));
        assert_eq_fold_lt(I64(1), I64(1), Bool(false));
        assert_eq_fold_lt(I64(-1), I64(1), Bool(true));
        assert_eq_fold_le(I64(1), I64(1), Bool(true));
        assert_eq_fold_le(I64(-1), I64(1), Bool(true));
        assert_eq_fold_le(I64(1), I64(-1), Bool(false));
        assert_eq_fold_ne(I64(1), I64(1), Bool(false));
        assert_eq_fold_ne(I64(1), I64(-1), Bool(true));

        // u64
        assert_eq_fold_eq(U64(1), U64(1), Bool(true));
        assert_eq_fold_eq(U64(1), U64(0), Bool(false));
        assert_eq_fold_gt(U64(1), U64(1), Bool(false));
        assert_eq_fold_gt(U64(1), U64(0), Bool(true));
        assert_eq_fold_ge(U64(1), U64(1), Bool(true));
        assert_eq_fold_ge(U64(1), U64(0), Bool(true));
        assert_eq_fold_ge(U64(0), U64(1), Bool(false));
        assert_eq_fold_lt(U64(1), U64(1), Bool(false));
        assert_eq_fold_lt(U64(0), U64(1), Bool(true));
        assert_eq_fold_le(U64(1), U64(1), Bool(true));
        assert_eq_fold_le(U64(0), U64(1), Bool(true));
        assert_eq_fold_le(U64(1), U64(0), Bool(false));
        assert_eq_fold_ne(U64(1), U64(1), Bool(false));
        assert_eq_fold_ne(U64(1), U64(0), Bool(true));

        // f64
        assert_eq_fold_eq(new_f64(1.0), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), new_f64(0.0), Bool(false));
        assert_eq_fold_gt(new_f64(1.0), new_f64(1.0), Bool(false));
        assert_eq_fold_gt(new_f64(1.0), new_f64(0.0), Bool(true));
        assert_eq_fold_ge(new_f64(1.0), new_f64(1.0), Bool(true));
        assert_eq_fold_ge(new_f64(1.0), new_f64(0.0), Bool(true));
        assert_eq_fold_ge(new_f64(0.0), new_f64(1.0), Bool(false));
        assert_eq_fold_lt(new_f64(1.0), new_f64(1.0), Bool(false));
        assert_eq_fold_lt(new_f64(0.0), new_f64(1.0), Bool(true));
        assert_eq_fold_le(new_f64(1.0), new_f64(1.0), Bool(true));
        assert_eq_fold_le(new_f64(0.0), new_f64(1.0), Bool(true));
        assert_eq_fold_le(new_f64(1.0), new_f64(0.0), Bool(false));
        assert_eq_fold_ne(new_f64(1.0), new_f64(1.0), Bool(false));
        assert_eq_fold_ne(new_f64(1.0), new_f64(0.0), Bool(true));

        // decimal
        assert_eq_fold_eq(new_decimal("1.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), new_decimal("0.0"), Bool(false));
        assert_eq_fold_gt(new_decimal("1.0"), new_decimal("1.0"), Bool(false));
        assert_eq_fold_gt(new_decimal("1.0"), new_decimal("0.0"), Bool(true));
        assert_eq_fold_ge(new_decimal("1.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_ge(new_decimal("1.0"), new_decimal("0.0"), Bool(true));
        assert_eq_fold_ge(new_decimal("0.0"), new_decimal("1.0"), Bool(false));
        assert_eq_fold_lt(new_decimal("1.0"), new_decimal("1.0"), Bool(false));
        assert_eq_fold_lt(new_decimal("0.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_le(new_decimal("1.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_le(new_decimal("0.0"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_le(new_decimal("1.0"), new_decimal("0.0"), Bool(false));
        assert_eq_fold_ne(new_decimal("1.0"), new_decimal("1.0"), Bool(false));
        assert_eq_fold_ne(new_decimal("1.0"), new_decimal("0.0"), Bool(true));

        // bool
        assert_eq_fold_eq(Bool(true), Bool(true), Bool(true));
        assert_eq_fold_eq(Bool(true), Bool(false), Bool(false));
        assert_eq_fold_gt(Bool(true), Bool(true), Bool(false));
        assert_eq_fold_gt(Bool(true), Bool(false), Bool(true));
        assert_eq_fold_ge(Bool(true), Bool(true), Bool(true));
        assert_eq_fold_ge(Bool(true), Bool(false), Bool(true));
        assert_eq_fold_ge(Bool(false), Bool(true), Bool(false));
        assert_eq_fold_lt(Bool(true), Bool(true), Bool(false));
        assert_eq_fold_lt(Bool(false), Bool(true), Bool(true));
        assert_eq_fold_le(Bool(true), Bool(true), Bool(true));
        assert_eq_fold_le(Bool(false), Bool(true), Bool(true));
        assert_eq_fold_le(Bool(true), Bool(false), Bool(false));
        assert_eq_fold_ne(Bool(true), Bool(true), Bool(false));
        assert_eq_fold_ne(Bool(true), Bool(false), Bool(true));

        // string
        assert_eq_fold_eq(new_str("1"), new_str("1"), Bool(true));
        assert_eq_fold_eq(new_str("1"), new_str("0"), Bool(false));
        assert_eq_fold_gt(new_str("1"), new_str("1"), Bool(false));
        assert_eq_fold_gt(new_str("1"), new_str("0"), Bool(true));
        assert_eq_fold_ge(new_str("1"), new_str("1"), Bool(true));
        assert_eq_fold_ge(new_str("1"), new_str("0"), Bool(true));
        assert_eq_fold_ge(new_str("0"), new_str("1"), Bool(false));
        assert_eq_fold_lt(new_str("1"), new_str("1"), Bool(false));
        assert_eq_fold_lt(new_str("0"), new_str("1"), Bool(true));
        assert_eq_fold_le(new_str("1"), new_str("1"), Bool(true));
        assert_eq_fold_le(new_str("0"), new_str("1"), Bool(true));
        assert_eq_fold_le(new_str("1"), new_str("0"), Bool(false));
        assert_eq_fold_ne(new_str("1"), new_str("1"), Bool(false));
        assert_eq_fold_ne(new_str("1"), new_str("0"), Bool(true));

        // bytes
        assert_eq_fold_eq(new_bytes(b"1"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), new_bytes(b"0"), Bool(false));
        assert_eq_fold_gt(new_bytes(b"1"), new_bytes(b"1"), Bool(false));
        assert_eq_fold_gt(new_bytes(b"1"), new_bytes(b"0"), Bool(true));
        assert_eq_fold_ge(new_bytes(b"1"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_ge(new_bytes(b"1"), new_bytes(b"0"), Bool(true));
        assert_eq_fold_ge(new_bytes(b"0"), new_bytes(b"1"), Bool(false));
        assert_eq_fold_lt(new_bytes(b"1"), new_bytes(b"1"), Bool(false));
        assert_eq_fold_lt(new_bytes(b"0"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_le(new_bytes(b"1"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_le(new_bytes(b"0"), new_bytes(b"1"), Bool(true));
        assert_eq_fold_le(new_bytes(b"1"), new_bytes(b"0"), Bool(false));
        assert_eq_fold_ne(new_bytes(b"1"), new_bytes(b"1"), Bool(false));
        assert_eq_fold_ne(new_bytes(b"1"), new_bytes(b"0"), Bool(true));

        // todo: date, time, datetime
    }

    #[test]
    fn test_fold_cmp_align() {
        assert_eq_fold_eq(I64(1), U64(1), Bool(true));
        assert_eq_fold_eq(I64(1), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(I64(1), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(I64(1), Bool(true), Bool(true));
        assert_eq_fold_eq(I64(1), new_str("1"), Bool(true));
        assert_eq_fold_eq(I64(1), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(U64(1), I64(1), Bool(true));
        assert_eq_fold_eq(U64(1), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(U64(1), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(U64(1), Bool(true), Bool(true));
        assert_eq_fold_eq(U64(1), new_str("1"), Bool(true));
        assert_eq_fold_eq(U64(1), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(new_f64(1.0), I64(1), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), U64(1), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), Bool(true), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), new_str("1"), Bool(true));
        assert_eq_fold_eq(new_f64(1.0), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(new_decimal("1.0"), I64(1), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), U64(1), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), Bool(true), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), new_str("1"), Bool(true));
        assert_eq_fold_eq(new_decimal("1.0"), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(Bool(true), I64(1), Bool(true));
        assert_eq_fold_eq(Bool(true), U64(1), Bool(true));
        assert_eq_fold_eq(Bool(true), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(Bool(true), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(Bool(true), new_str("1"), Bool(true));
        assert_eq_fold_eq(Bool(true), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(new_str("1"), I64(1), Bool(true));
        assert_eq_fold_eq(new_str("1"), U64(1), Bool(true));
        assert_eq_fold_eq(new_str("1"), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(new_str("1"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(new_str("1"), Bool(true), Bool(true));
        assert_eq_fold_eq(new_str("1"), new_bytes(b"1"), Bool(true));

        assert_eq_fold_eq(new_bytes(b"1"), I64(1), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), U64(1), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), new_f64(1.0), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), new_decimal("1.0"), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), Bool(true), Bool(true));
        assert_eq_fold_eq(new_bytes(b"1"), new_str("1"), Bool(true));

        // todo: date, time, datetime
    }

    fn assert_eq_fold_eq(c1: Const, c2: Const, c3: Const) {
        let res = fold_eq(&ExprKind::Const(c1), &ExprKind::Const(c2))
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_gt(c1: Const, c2: Const, c3: Const) {
        let res = fold_gt(&ExprKind::Const(c1), &ExprKind::Const(c2))
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_ge(c1: Const, c2: Const, c3: Const) {
        let res = fold_ge(&ExprKind::Const(c1), &ExprKind::Const(c2))
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_lt(c1: Const, c2: Const, c3: Const) {
        let res = fold_lt(&ExprKind::Const(c1), &ExprKind::Const(c2))
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_le(c1: Const, c2: Const, c3: Const) {
        let res = fold_le(&ExprKind::Const(c1), &ExprKind::Const(c2))
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_ne(c1: Const, c2: Const, c3: Const) {
        let res = fold_ne(&ExprKind::Const(c1), &ExprKind::Const(c2))
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }

    fn assert_eq_fold_se(c1: Const, c2: Const, c3: Const) {
        let res = fold_safeeq(&ExprKind::Const(c1), &ExprKind::Const(c2))
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }
}
