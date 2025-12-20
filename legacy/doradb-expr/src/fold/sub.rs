use crate::error::{Error, Result};
use crate::{Const, ExprKind};
use doradb_datatype::Decimal;

#[inline]
pub fn fold_sub(lhs: &ExprKind, rhs: &ExprKind) -> Result<Option<Const>> {
    match (lhs, rhs) {
        (ExprKind::Const(Const::Null), _) | (_, ExprKind::Const(Const::Null)) => {
            Ok(Some(Const::Null))
        }
        (ExprKind::Const(lhs), ExprKind::Const(rhs)) => fold_sub_const(lhs, rhs),
        _ => Ok(None),
    }
}

#[inline]
pub fn fold_sub_const(lhs: &Const, rhs: &Const) -> Result<Option<Const>> {
    let res = match lhs {
        Const::I64(v0) => match rhs {
            Const::I64(v1) => v0
                .checked_sub(*v1)
                .map(Const::I64)
                .ok_or(Error::ValueOutOfRange)?,
            Const::U64(v1) => i64_sub_u64(*v0, *v1)?,
            Const::F64(v1) => {
                Const::new_f64(*v0 as f64 - v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let v0 = Decimal::from(*v0);
                let mut res = Decimal::zero();
                Decimal::sub_to(&v0, v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            // differ from MySQL, can return i64 if possible
            Const::Bool(v1) => i64_sub_u64(*v0, u64::from(*v1))?,
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::U64(v0) => match rhs {
            Const::I64(v1) => u64_sub_i64(*v0, *v1)?,
            Const::U64(v1) => v0
                .checked_sub(*v1)
                .map(Const::U64)
                .ok_or(Error::ValueOutOfRange)?,
            Const::F64(v1) => {
                Const::new_f64(*v0 as f64 - v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let v0 = Decimal::from(*v0);
                let mut res = Decimal::zero();
                Decimal::sub_to(&v0, v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::Bool(v1) => v0
                .checked_sub(u64::from(*v1))
                .map(Const::U64)
                .ok_or(Error::ValueOutOfRange)?,
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::F64(v0) => match rhs {
            Const::I64(v1) => {
                Const::new_f64(v0.value() - *v1 as f64).ok_or(Error::ValueOutOfRange)?
            }
            Const::U64(v1) => {
                Const::new_f64(v0.value() - *v1 as f64).ok_or(Error::ValueOutOfRange)?
            }
            Const::F64(v1) => {
                Const::new_f64(v0.value() - v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let v1: f64 = v1.to_string(-1).parse()?;
                Const::new_f64(v0.value() - v1).ok_or(Error::ValueOutOfRange)?
            }
            Const::Bool(v1) => Const::new_f64(v0.value() - if *v1 { 1.0 } else { 0.0 })
                .ok_or(Error::ValueOutOfRange)?,
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::Decimal(v0) => match rhs {
            Const::I64(v1) => {
                let v1 = Decimal::from(*v1);
                let mut res = Decimal::zero();
                Decimal::sub_to(v0, &v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::U64(v1) => {
                let v1 = Decimal::from(*v1);
                let mut res = Decimal::zero();
                Decimal::sub_to(v0, &v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::F64(v1) => {
                let v0: f64 = v0.to_string(-1).parse()?;
                Const::new_f64(v0 - v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let mut res = Decimal::zero();
                Decimal::sub_to(v0, v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::Bool(v1) => {
                if *v1 {
                    let v1 = Decimal::one();
                    let mut res = Decimal::zero();
                    Decimal::sub_to(v0, &v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                    Const::Decimal(res)
                } else {
                    Const::Decimal(v0.clone())
                }
            }
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::Bool(v0) => match rhs {
            Const::I64(v1) => u64_sub_i64(u64::from(*v0), *v1)?,
            Const::U64(v1) => {
                let v0 = u64::from(*v0);
                v0.checked_sub(*v1)
                    .map(Const::U64)
                    .ok_or(Error::ValueOutOfRange)?
            }
            Const::F64(v1) => {
                let v0: f64 = if *v0 { 1.0 } else { 0.0 };
                Const::new_f64(v0 - v1.value()).ok_or(Error::ValueOutOfRange)?
            }
            Const::Decimal(v1) => {
                let v0 = if *v0 { Decimal::one() } else { Decimal::zero() };
                let mut res = Decimal::zero();
                Decimal::sub_to(&v0, v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
                Const::Decimal(res)
            }
            Const::Bool(v1) => {
                // coerce to u64
                let v0 = u64::from(*v0);
                let v1 = u64::from(*v1);
                v0.checked_sub(v1)
                    .map(Const::U64)
                    .ok_or(Error::ValueOutOfRange)?
            }
            Const::Null => Const::Null,
            _ => return Ok(None), // todo: handle non-nuemric value
        },
        Const::Null => Const::Null,
        _ => return Ok(None), // todo: handle non-nuemric value
    };
    Ok(Some(res))
}

fn i64_sub_u64(v0: i64, v1: u64) -> Result<Const> {
    let v0 = Decimal::from(v0);
    let v1 = Decimal::from(v1);
    let mut res = Decimal::zero();
    Decimal::sub_to(&v0, &v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
    res.as_u64()
        .map(Const::U64)
        .map_err(|_| Error::ValueOutOfRange)
}

fn u64_sub_i64(v0: u64, v1: i64) -> Result<Const> {
    let v0 = Decimal::from(v0);
    let v1 = Decimal::from(v1);
    let mut res = Decimal::zero();
    Decimal::sub_to(&v0, &v1, &mut res).map_err(|_| Error::ValueOutOfRange)?;
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
    fn test_fold_sub() {
        assert_eq_fold_sub(I64(3), I64(1), I64(2));
        assert_eq_fold_sub(I64(3), U64(1), U64(2));
        assert_eq_fold_sub(I64(3), new_f64(1.0), new_f64(2.0));
        assert_eq_fold_sub(I64(3), new_decimal("1.0"), new_decimal("2.0"));
        assert_eq_fold_sub(I64(3), Bool(true), U64(2));
        assert_eq_fold_sub(I64(3), Bool(false), U64(3));
        assert_eq_fold_sub(I64(3), Null, Null);
        assert_eq_fold_sub(U64(3), I64(1), U64(2));
        assert_eq_fold_sub(U64(3), U64(1), U64(2));
        assert_eq_fold_sub(U64(3), new_f64(1.0), new_f64(2.0));
        assert_eq_fold_sub(U64(3), new_decimal("1.0"), new_decimal("2.0"));
        assert_eq_fold_sub(U64(3), Bool(true), U64(2));
        assert_eq_fold_sub(U64(3), Bool(false), U64(3));
        assert_eq_fold_sub(U64(3), Null, Null);
        assert_eq_fold_sub(new_f64(3.0), I64(1), new_f64(2.0));
        assert_eq_fold_sub(new_f64(3.0), U64(1), new_f64(2.0));
        assert_eq_fold_sub(new_f64(3.0), new_f64(1.0), new_f64(2.0));
        assert_eq_fold_sub(new_f64(3.0), new_decimal("1.0"), new_f64(2.0));
        assert_eq_fold_sub(new_f64(3.0), Bool(true), new_f64(2.0));
        assert_eq_fold_sub(new_f64(3.0), Bool(false), new_f64(3.0));
        assert_eq_fold_sub(new_f64(3.0), Null, Null);
        assert_eq_fold_sub(new_decimal("3.0"), I64(1), new_decimal("2.0"));
        assert_eq_fold_sub(new_decimal("3.0"), U64(1), new_decimal("2.0"));
        assert_eq_fold_sub(new_decimal("3.0"), new_f64(1.0), new_f64(2.0));
        assert_eq_fold_sub(new_decimal("3.0"), new_decimal("1.0"), new_decimal("2.0"));
        assert_eq_fold_sub(new_decimal("3.0"), Bool(true), new_decimal("2.0"));
        assert_eq_fold_sub(new_decimal("3.0"), Bool(false), new_decimal("3.0"));
        assert_eq_fold_sub(new_decimal("3.0"), Null, Null);
        assert_eq_fold_sub(Bool(true), I64(1), U64(0));
        assert_eq_fold_sub(Bool(false), I64(0), U64(0));
        assert_eq_fold_sub(Bool(true), U64(1), U64(0));
        assert_eq_fold_sub(Bool(false), U64(0), U64(0));
        assert_eq_fold_sub(Bool(true), new_f64(1.0), new_f64(0.0));
        assert_eq_fold_sub(Bool(false), new_f64(1.0), new_f64(-1.0));
        assert_eq_fold_sub(Bool(true), new_decimal("1.0"), new_decimal("0.0"));
        assert_eq_fold_sub(Bool(false), new_decimal("1.0"), new_decimal("-1.0"));
        assert_eq_fold_sub(Bool(true), Bool(true), U64(0));
        // this will fail
        // assert_eq_fold_sub(Bool(false), Bool(true), I64(-1));
        assert_eq_fold_sub(Bool(true), Bool(false), U64(1));
        assert_eq_fold_sub(Bool(false), Bool(false), U64(0));
        assert_eq_fold_sub(Bool(true), Null, Null);
        assert_eq_fold_sub(Bool(false), Null, Null);
        assert_eq_fold_sub(Null, I64(1), Null);
        assert_eq_fold_sub(Null, U64(1), Null);
        assert_eq_fold_sub(Null, new_f64(1.0), Null);
        assert_eq_fold_sub(Null, new_decimal("1.0"), Null);
        assert_eq_fold_sub(Null, Bool(true), Null);
        assert_eq_fold_sub(Null, Bool(false), Null);
        assert_eq_fold_sub(Null, Null, Null);
    }

    fn assert_eq_fold_sub(c1: Const, c2: Const, c3: Const) {
        let res = fold_sub(&ExprKind::Const(c1), &ExprKind::Const(c2))
            .unwrap()
            .unwrap();
        assert_eq!(res, c3)
    }
}
