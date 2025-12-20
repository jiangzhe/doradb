use crate::error::{Error, Result};
use crate::{Const, ExprKind};
use doradb_datatype::Decimal;

#[inline]
pub fn fold_add(lhs: &ExprKind, rhs: &ExprKind) -> Result<Option<Const>> {
    match (lhs, rhs) {
        (ExprKind::Const(Const::Null), _) | (_, ExprKind::Const(Const::Null)) => {
            Ok(Some(Const::Null))
        }
        (ExprKind::Const(lhs), ExprKind::Const(rhs)) => fold_add_const(lhs, rhs),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_add_const(lhs: &Const, rhs: &Const) -> Result<Option<Const>> {
    let res = match lhs {
        Const::I64(v0) => match rhs {
            Const::I64(v1) => v0
                .checked_add(*v1)
                .map(Const::I64)
                .ok_or(Error::ValueOutOfRange)?,
            Const::U64(v1) => i64_add_u64(*v0, *v1)?,
            Const::F64(v1) => {
                Const::new_f64(*v0 as f64 + v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let v0 = Decimal::from(*v0);
                let mut res = Decimal::zero();
                Decimal::add_to(&v0, v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            // differ from MySQL, can return i64 if possible
            Const::Bool(v1) => i64_add_u64(*v0, u64::from(*v1))?,
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::U64(v0) => match rhs {
            Const::I64(v1) => i64_add_u64(*v1, *v0)?,
            Const::U64(v1) => v0
                .checked_add(*v1)
                .map(Const::U64)
                .ok_or(Error::ValueOutOfRange)?,
            Const::F64(v1) => {
                Const::new_f64(*v0 as f64 + v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let v0 = Decimal::from(*v0);
                let mut res = Decimal::zero();
                Decimal::add_to(&v0, v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::Bool(v1) => v0
                .checked_add(u64::from(*v1))
                .map(Const::U64)
                .ok_or(Error::ValueOutOfRange)?,
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::F64(v0) => match rhs {
            Const::I64(v1) => {
                Const::new_f64(v0.value() + *v1 as f64).ok_or(Error::ValueOutOfRange)?
            }
            Const::U64(v1) => {
                Const::new_f64(v0.value() + *v1 as f64).ok_or(Error::ValueOutOfRange)?
            }
            Const::F64(v1) => {
                Const::new_f64(v0.value() + v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let v1: f64 = v1.to_string(-1).parse()?;
                Const::new_f64(v0.value() + v1).ok_or(Error::ValueOutOfRange)?
            }
            Const::Bool(v1) => Const::new_f64(v0.value() + if *v1 { 1.0 } else { 0.0 })
                .ok_or(Error::ValueOutOfRange)?,
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::Decimal(v0) => match rhs {
            Const::I64(v1) => {
                let v1 = Decimal::from(*v1);
                let mut res = Decimal::zero();
                Decimal::add_to(v0, &v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::U64(v1) => {
                let v1 = Decimal::from(*v1);
                let mut res = Decimal::zero();
                Decimal::add_to(v0, &v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::F64(v1) => {
                let v0: f64 = v0.to_string(-1).parse()?;
                Const::new_f64(v0 + v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let mut res = Decimal::zero();
                Decimal::add_to(v0, v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::Bool(v1) => {
                let v1 = if *v1 { Decimal::one() } else { Decimal::zero() };
                let mut res = Decimal::zero();
                Decimal::add_to(v0, &v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::Bool(v0) => match rhs {
            Const::I64(v1) => i64_add_u64(*v1, u64::from(*v0))?,
            Const::U64(v1) => v1
                .checked_add(u64::from(*v0))
                .map(Const::U64)
                .ok_or(Error::ValueOutOfRange)?,
            Const::F64(v1) => {
                let v0: f64 = if *v0 { 1.0 } else { 0.0 };
                Const::new_f64(v0 + v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let v0 = if *v0 { Decimal::one() } else { Decimal::zero() };
                let mut res = Decimal::zero();
                Decimal::add_to(&v0, v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            // coerce to u64
            Const::Bool(v1) => {
                let v0 = u64::from(*v0);
                let v1 = u64::from(*v1);
                Const::U64(v0 + v1)
            }
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::Null => Const::Null,
        _ => return Ok(None), // todo: handle non-nuemric value
    };
    Ok(Some(res))
}

/// i64 + u64 always returns u64, fail if not in range
fn i64_add_u64(v0: i64, v1: u64) -> Result<Const> {
    let v0 = Decimal::from(v0);
    let v1 = Decimal::from(v1);
    let mut res = Decimal::zero();
    Decimal::add_to(&v0, &v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
    res.as_u64()
        .map(Const::U64)
        .map_err(|_| Error::ValueOutOfRange)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::tests::{new_decimal, new_f64};
    use crate::Const::*;

    #[test]
    fn test_fold_add() {
        assert_eq_fold_add(I64(1), I64(1), I64(2));
        assert_eq_fold_add(I64(1), U64(1), U64(2));
        assert_eq_fold_add(I64(1), new_f64(1.0), new_f64(2.0));
        assert_eq_fold_add(I64(1), new_decimal("1.0"), new_decimal("2.0"));
        assert_eq_fold_add(I64(1), Bool(true), U64(2));
        assert_eq_fold_add(I64(1), Bool(false), U64(1));
        assert_eq_fold_add(I64(1), Null, Null);
        assert_eq_fold_add(U64(1), I64(1), U64(2));
        assert_eq_fold_add(U64(1), U64(1), U64(2));
        assert_eq_fold_add(U64(1), new_f64(1.0), new_f64(2.0));
        assert_eq_fold_add(U64(1), new_decimal("1.0"), new_decimal("2.0"));
        assert_eq_fold_add(U64(1), Bool(true), U64(2));
        assert_eq_fold_add(U64(1), Bool(false), U64(1));
        assert_eq_fold_add(U64(1), Null, Null);
        assert_eq_fold_add(new_f64(1.0), I64(1), new_f64(2.0));
        assert_eq_fold_add(new_f64(1.0), U64(1), new_f64(2.0));
        assert_eq_fold_add(new_f64(1.0), new_f64(1.0), new_f64(2.0));
        assert_eq_fold_add(new_f64(1.0), new_decimal("1.0"), new_f64(2.0));
        assert_eq_fold_add(new_f64(1.0), Bool(true), new_f64(2.0));
        assert_eq_fold_add(new_f64(1.0), Bool(false), new_f64(1.0));
        assert_eq_fold_add(new_f64(1.0), Null, Null);
        assert_eq_fold_add(new_decimal("1.0"), I64(1), new_decimal("2.0"));
        assert_eq_fold_add(new_decimal("1.0"), U64(1), new_decimal("2.0"));
        assert_eq_fold_add(new_decimal("1.0"), new_f64(1.0), new_f64(2.0));
        assert_eq_fold_add(new_decimal("1.0"), new_decimal("1.0"), new_decimal("2.0"));
        assert_eq_fold_add(new_decimal("1.0"), Bool(true), new_decimal("2.0"));
        assert_eq_fold_add(new_decimal("1.0"), Bool(false), new_decimal("1.0"));
        assert_eq_fold_add(new_decimal("1.0"), Null, Null);
        assert_eq_fold_add(Bool(true), I64(1), U64(2));
        assert_eq_fold_add(Bool(false), I64(1), U64(1));
        assert_eq_fold_add(Bool(true), U64(1), U64(2));
        assert_eq_fold_add(Bool(false), U64(1), U64(1));
        assert_eq_fold_add(Bool(true), new_f64(1.0), new_f64(2.0));
        assert_eq_fold_add(Bool(false), new_f64(1.0), new_f64(1.0));
        assert_eq_fold_add(Bool(true), new_decimal("1.0"), new_decimal("2.0"));
        assert_eq_fold_add(Bool(false), new_decimal("1.0"), new_decimal("1.0"));
        assert_eq_fold_add(Bool(true), Bool(true), U64(2));
        assert_eq_fold_add(Bool(false), Bool(true), U64(1));
        assert_eq_fold_add(Bool(true), Bool(false), U64(1));
        assert_eq_fold_add(Bool(false), Bool(false), U64(0));
        assert_eq_fold_add(Bool(true), Null, Null);
        assert_eq_fold_add(Bool(false), Null, Null);
        assert_eq_fold_add(Null, I64(1), Null);
        assert_eq_fold_add(Null, U64(1), Null);
        assert_eq_fold_add(Null, new_f64(1.0), Null);
        assert_eq_fold_add(Null, new_decimal("1.0"), Null);
        assert_eq_fold_add(Null, Bool(true), Null);
        assert_eq_fold_add(Null, Bool(false), Null);
        assert_eq_fold_add(Null, Null, Null);
    }

    fn assert_eq_fold_add(c1: Const, c2: Const, c3: Const) {
        let res = fold_add(&ExprKind::Const(c1), &ExprKind::Const(c2))
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }
}
