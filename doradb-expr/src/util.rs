use crate::error::{Error, Result};
use crate::{Col, ColKind, ExprKind, FuncKind, Pred, PredFuncKind};
use doradb_datatype::{Collation, PreciseType, Typed};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem;

pub trait TypeInferer {
    /// confirm the type of given expression.
    fn confirm(&mut self, e: &ExprKind) -> Option<PreciseType>;
}

pub trait TypeInfer {
    /// infer expression type. this only resolve one level expression.
    fn infer<I: TypeInferer>(&self, inferer: &mut I) -> Result<PreciseType>;
}

/// cache the infered expressions.
pub struct TypeCache<I> {
    inner: I,
    cache: HashMap<ExprKind, PreciseType>,
}

impl<I: TypeInferer> TypeInferer for TypeCache<I> {
    #[inline]
    fn confirm(&mut self, e: &ExprKind) -> Option<PreciseType> {
        if let Some(ty) = self.cache.get(e) {
            return Some(*ty);
        }
        if let Some(ty) = self.inner.confirm(e) {
            self.cache.insert(e.clone(), ty);
            return Some(ty);
        }
        None
    }
}

impl TypeInfer for ExprKind {
    fn infer<I: TypeInferer>(&self, inferer: &mut I) -> Result<PreciseType> {
        let ty = match self {
            ExprKind::Const(c) => c.pty(),
            // column type should be handled by infer fast.
            ExprKind::Col(Col { kind, .. }) => match kind {
                ColKind::Table(_, _, ty) => *ty,
                // any other column type should be handled by infer fast.
                _ => return inferer.confirm(self).ok_or(Error::UnknownColumnType),
            },
            ExprKind::Func { kind, args } => infer_func(*kind, args, inferer)?,
            ExprKind::Pred(_) => PreciseType::bool(),
            _ => todo!(),
        };
        Ok(ty)
    }
}

#[inline]
fn infer_func<I: TypeInferer>(
    kind: FuncKind,
    args: &[ExprKind],
    inferer: &mut I,
) -> Result<PreciseType> {
    use PreciseType as PT;
    match kind {
        FuncKind::Add | FuncKind::Sub => {
            let arg0ty = args[0].infer(inferer)?;
            let arg1ty = args[1].infer(inferer)?;
            let ty = match (arg0ty, arg1ty) {
                (PT::Unknown, _) | (_, PT::Unknown) => return Err(Error::UnknownArgumentType),
                (PT::Compound, _) | (_, PT::Compound) => todo!("compound type infer"),
                // float vs float
                (PT::Float(lhs_bytes), PT::Float(rhs_bytes)) => match lhs_bytes.cmp(&rhs_bytes) {
                    Ordering::Greater => PT::Float(lhs_bytes),
                    Ordering::Less => PT::Float(rhs_bytes),
                    Ordering::Equal => PT::Float(lhs_bytes),
                },
                // decimal vs decimal
                (PT::Decimal(lhs_prec, lhs_frac), PT::Decimal(rhs_prec, rhs_frac)) => {
                    let lhs_intg = lhs_prec - lhs_frac;
                    let rhs_intg = rhs_prec - rhs_frac;
                    match (lhs_intg.cmp(&rhs_intg), lhs_frac.cmp(&rhs_frac)) {
                        (Ordering::Equal, Ordering::Equal) => PT::Decimal(lhs_prec, lhs_frac),
                        (Ordering::Equal, Ordering::Greater)
                        | (Ordering::Greater, Ordering::Greater | Ordering::Equal) => {
                            PT::Decimal(lhs_prec, lhs_frac)
                        }
                        (Ordering::Equal, Ordering::Less)
                        | (Ordering::Less, Ordering::Less | Ordering::Equal) => {
                            PT::Decimal(rhs_prec, rhs_frac)
                        }
                        (Ordering::Greater, Ordering::Less) => {
                            PT::Decimal(lhs_intg + rhs_frac, rhs_frac)
                        }
                        (Ordering::Less, Ordering::Greater) => {
                            PT::Decimal(rhs_intg + lhs_frac, lhs_frac)
                        }
                    }
                }
                // int vs int
                // Align integer with same width, prioritize unsigned over signed.
                (PT::Int(lhs_bytes, lhs_unsigned), PT::Int(rhs_bytes, rhs_unsigned)) => {
                    match lhs_bytes.cmp(&rhs_bytes) {
                        Ordering::Greater => PT::int(lhs_bytes, lhs_unsigned),
                        Ordering::Less => PT::int(rhs_bytes, rhs_unsigned),
                        // coerce to unsigned if any unsigned.
                        Ordering::Equal => PT::int(lhs_bytes, lhs_unsigned || rhs_unsigned),
                    }
                }
                // bool vs bool
                (PT::Bool, PT::Bool) => PT::u64(),
                // int vs bool
                (PT::Int(..), PT::Bool) => PT::u64(),
                (PT::Bool, PT::Int(..)) => PT::u64(),
                // datetime/date/time vs interval
                (
                    lhs_ty @ PT::Datetime(_) | lhs_ty @ PT::Date | lhs_ty @ PT::Time(_),
                    PT::Interval,
                ) => lhs_ty,
                (
                    PT::Interval,
                    rhs_ty @ PT::Datetime(_) | rhs_ty @ PT::Date | rhs_ty @ PT::Time(_),
                ) => rhs_ty,
                // datetime/time vs int
                (PT::Datetime(lhs_frac) | PT::Time(lhs_frac), PT::Int(.., rhs_unsigned)) => {
                    if lhs_frac == 0 {
                        PT::int(8, rhs_unsigned)
                    } else {
                        PT::decimal(18, lhs_frac)
                    }
                }
                (PT::Int(.., lhs_unsigned), PT::Datetime(rhs_frac) | PT::Time(rhs_frac)) => {
                    if rhs_frac == 0 {
                        PT::int(8, lhs_unsigned)
                    } else {
                        PT::decimal(18, rhs_frac)
                    }
                }
                // datetime/time vs bool
                (PT::Datetime(frac) | PT::Time(frac), PT::Bool)
                | (PT::Bool, PT::Datetime(frac) | PT::Time(frac)) => {
                    if frac == 0 {
                        PT::u64()
                    } else {
                        PT::decimal(18, frac)
                    }
                }
                // datetime/time vs datetime/time
                (
                    PT::Datetime(lhs_frac) | PT::Time(lhs_frac),
                    PT::Datetime(rhs_frac) | PT::Time(rhs_frac),
                ) => {
                    let max_frac = lhs_frac.max(rhs_frac);
                    if max_frac == 0 {
                        PT::i64()
                    } else {
                        PT::decimal(18, max_frac)
                    }
                }
                // datetime/time vs date
                (PT::Datetime(frac) | PT::Time(frac), PT::Date)
                | (PT::Date, PT::Datetime(frac) | PT::Time(frac)) => {
                    if frac == 0 {
                        PT::i64()
                    } else {
                        PT::decimal(18, frac)
                    }
                }
                // date vs int
                (PT::Date, PT::Int(.., rhs_unsigned)) => PT::int(8, rhs_unsigned),
                (PT::Int(.., lhs_unsigned), PT::Date) => PT::int(8, lhs_unsigned),
                // date vs bool
                (PT::Date, PT::Bool) | (PT::Bool, PT::Date) => PT::u64(),
                // date vs date
                (PT::Date, PT::Date) => PT::i64(),
                // float vs any
                (lhs_ty @ PT::Float(_), _) => lhs_ty,
                (_, rhs_ty @ PT::Float(_)) => rhs_ty,
                // decimal vs any
                (lhs_ty @ PT::Decimal(..), _) => lhs_ty,
                (_, rhs_ty @ PT::Decimal(..)) => rhs_ty,
                (_, _) => PT::f64(),
            };
            Ok(ty)
        }
        _ => todo!(),
    }
}

pub trait TypeFix: TypeInfer {
    /// fix expression type.
    fn fix<I: TypeInferer>(&mut self, inferer: &mut I) -> Result<PreciseType>;
}

impl TypeFix for ExprKind {
    fn fix<I: TypeInferer>(&mut self, inferer: &mut I) -> Result<PreciseType> {
        let ty = match self {
            ExprKind::Const(c) => c.pty(),
            // column type should be handled by infer fast.
            ExprKind::Col(Col { kind, .. }) => match kind {
                ColKind::Table(_, _, ty) => *ty,
                // any other column type should be handled by infer fast.
                _ => return inferer.confirm(self).ok_or(Error::UnknownColumnType),
            },
            ExprKind::Func { kind, args } => fix_func(*kind, args.as_mut(), inferer)?,
            ExprKind::Pred(pred) => {
                fix_pred(pred, inferer)?;
                PreciseType::bool()
            }
            _ => todo!(),
        };
        Ok(ty)
    }
}

#[inline]
pub fn fix_bools<I: TypeInferer>(preds: &mut [ExprKind], inferer: &mut I) -> Result<()> {
    for e in preds {
        let ty = e.fix(inferer)?;
        if !ty.is_bool() {
            cast_arg(e, PreciseType::bool());
        }
    }
    Ok(())
}

#[inline]
fn fix_func<I: TypeInferer>(
    kind: FuncKind,
    args: &mut [ExprKind],
    inferer: &mut I,
) -> Result<PreciseType> {
    use PreciseType as PT;
    match kind {
        FuncKind::Add | FuncKind::Sub => {
            let arg0ty = args[0].fix(inferer)?;
            let arg1ty = args[1].fix(inferer)?;
            match (arg0ty, arg1ty) {
                (PT::Unknown, _) | (_, PT::Unknown) => Err(Error::UnknownArgumentType),
                (PT::Compound, _) | (_, PT::Compound) => todo!("compound type infer"),
                // float vs float
                (PT::Float(lhs_bytes), PT::Float(rhs_bytes)) => {
                    match lhs_bytes.cmp(&rhs_bytes) {
                        Ordering::Greater => {
                            // implicit cast rhs
                            let res_ty = PT::Float(lhs_bytes);
                            cast_arg(&mut args[1], res_ty);
                            Ok(res_ty)
                        }
                        Ordering::Less => {
                            // implicit cast lhs
                            let res_ty = PT::Float(rhs_bytes);
                            cast_arg(&mut args[0], res_ty);
                            Ok(res_ty)
                        }
                        Ordering::Equal => Ok(PT::Float(lhs_bytes)),
                    }
                }
                // decimal vs decimal
                (PT::Decimal(lhs_prec, lhs_frac), PT::Decimal(rhs_prec, rhs_frac)) => {
                    let lhs_intg = lhs_prec - lhs_frac;
                    let rhs_intg = rhs_prec - rhs_frac;
                    match (lhs_intg.cmp(&rhs_intg), lhs_frac.cmp(&rhs_frac)) {
                        (Ordering::Equal, Ordering::Equal) => Ok(PT::Decimal(lhs_prec, lhs_frac)),
                        (Ordering::Equal, Ordering::Greater)
                        | (Ordering::Greater, Ordering::Greater | Ordering::Equal) => {
                            let res_ty = PT::Decimal(lhs_prec, lhs_frac);
                            cast_arg(&mut args[1], res_ty);
                            Ok(res_ty)
                        }
                        (Ordering::Equal, Ordering::Less)
                        | (Ordering::Less, Ordering::Less | Ordering::Equal) => {
                            let res_ty = PT::Decimal(rhs_prec, rhs_frac);
                            cast_arg(&mut args[0], res_ty);
                            Ok(res_ty)
                        }
                        (Ordering::Greater, Ordering::Less) => {
                            let res_ty = PT::Decimal(lhs_intg + rhs_frac, rhs_frac);
                            cast_arg(&mut args[0], res_ty);
                            cast_arg(&mut args[1], res_ty);
                            Ok(res_ty)
                        }
                        (Ordering::Less, Ordering::Greater) => {
                            let res_ty = PT::Decimal(rhs_intg + lhs_frac, lhs_frac);
                            cast_arg(&mut args[0], res_ty);
                            cast_arg(&mut args[1], res_ty);
                            Ok(res_ty)
                        }
                    }
                }
                // int vs int
                // Align integer with same width, prioritize unsigned over signed.
                (PT::Int(lhs_bytes, lhs_unsigned), PT::Int(rhs_bytes, rhs_unsigned)) => {
                    match lhs_bytes.cmp(&rhs_bytes) {
                        Ordering::Greater => {
                            // implicit cast rhs
                            cast_arg(&mut args[1], PT::int(lhs_bytes, rhs_unsigned));
                            Ok(PT::int(lhs_bytes, lhs_unsigned))
                        }
                        Ordering::Less => {
                            // implicit cast lhs
                            cast_arg(&mut args[0], PT::int(rhs_bytes, lhs_unsigned));
                            Ok(PT::int(rhs_bytes, rhs_unsigned))
                        }
                        // coerce to unsigned if any unsigned.
                        Ordering::Equal => Ok(PT::int(lhs_bytes, lhs_unsigned || rhs_unsigned)),
                    }
                }
                // bool vs bool
                (PT::Bool, PT::Bool) => {
                    cast_arg(&mut args[0], PT::u64());
                    cast_arg(&mut args[1], PT::u64());
                    Ok(PT::u64())
                }
                // int vs bool
                (PT::Int(lhs_bytes, lhs_unsigned), PT::Bool) => {
                    if lhs_bytes != 8 {
                        cast_arg(&mut args[0], PT::int(8, lhs_unsigned));
                    }
                    cast_arg(&mut args[1], PT::u64());
                    Ok(PT::u64())
                }
                (PT::Bool, PT::Int(rhs_bytes, rhs_unsigned)) => {
                    if rhs_bytes != 8 {
                        cast_arg(&mut args[1], PT::int(8, rhs_unsigned));
                    }
                    cast_arg(&mut args[0], PT::u64());
                    Ok(PT::u64())
                }
                // datetime/date/time vs interval
                (
                    lhs_ty @ PT::Datetime(_) | lhs_ty @ PT::Date | lhs_ty @ PT::Time(_),
                    PT::Interval,
                ) => Ok(lhs_ty),
                (
                    PT::Interval,
                    rhs_ty @ PT::Datetime(_) | rhs_ty @ PT::Date | rhs_ty @ PT::Time(_),
                ) => Ok(rhs_ty),
                // datetime/time vs int
                (PT::Datetime(lhs_frac) | PT::Time(lhs_frac), PT::Int(rhs_bytes, rhs_unsigned)) => {
                    let res_ty = if lhs_frac == 0 {
                        PT::int(8, rhs_unsigned)
                    } else {
                        PT::decimal(18, lhs_frac)
                    };
                    if rhs_bytes != 8 {
                        cast_arg(&mut args[1], res_ty);
                    }
                    cast_arg(&mut args[0], res_ty);
                    Ok(res_ty)
                }
                (PT::Int(lhs_bytes, lhs_unsigned), PT::Datetime(rhs_frac) | PT::Time(rhs_frac)) => {
                    let res_ty = if rhs_frac == 0 {
                        PT::int(8, lhs_unsigned)
                    } else {
                        PT::decimal(18, rhs_frac)
                    };
                    if lhs_bytes != 8 {
                        cast_arg(&mut args[0], res_ty);
                    }
                    cast_arg(&mut args[1], res_ty);
                    Ok(res_ty)
                }
                // datetime/time vs bool
                (PT::Datetime(frac) | PT::Time(frac), PT::Bool)
                | (PT::Bool, PT::Datetime(frac) | PT::Time(frac)) => {
                    let res_ty = if frac == 0 {
                        PT::u64()
                    } else {
                        PT::decimal(18, frac)
                    };
                    cast_arg(&mut args[0], res_ty);
                    cast_arg(&mut args[1], res_ty);
                    Ok(res_ty)
                }
                // datetime/time vs datetime/time
                (
                    PT::Datetime(lhs_frac) | PT::Time(lhs_frac),
                    PT::Datetime(rhs_frac) | PT::Time(rhs_frac),
                ) => {
                    let max_frac = lhs_frac.max(rhs_frac);
                    let res_ty = if max_frac == 0 {
                        PT::i64()
                    } else {
                        PT::decimal(18, max_frac)
                    };
                    cast_arg(&mut args[0], res_ty);
                    cast_arg(&mut args[1], res_ty);
                    Ok(res_ty)
                }
                // datetime/time vs date
                (PT::Datetime(frac) | PT::Time(frac), PT::Date)
                | (PT::Date, PT::Datetime(frac) | PT::Time(frac)) => {
                    let res_ty = if frac == 0 {
                        PT::i64()
                    } else {
                        PT::decimal(18, frac)
                    };
                    cast_arg(&mut args[0], res_ty);
                    cast_arg(&mut args[1], res_ty);
                    Ok(res_ty)
                }
                // date vs int
                (PT::Date, PT::Int(rhs_bytes, rhs_unsigned)) => {
                    let res_ty = PT::int(8, rhs_unsigned);
                    if rhs_bytes != 8 {
                        cast_arg(&mut args[1], res_ty);
                    }
                    cast_arg(&mut args[0], res_ty);
                    Ok(res_ty)
                }
                (PT::Int(lhs_bytes, lhs_unsigned), PT::Date) => {
                    let res_ty = PT::int(8, lhs_unsigned);
                    if lhs_bytes != 8 {
                        cast_arg(&mut args[0], res_ty);
                    }
                    cast_arg(&mut args[1], res_ty);
                    Ok(res_ty)
                }
                // date vs bool
                (PT::Date, PT::Bool) | (PT::Bool, PT::Date) => {
                    cast_arg(&mut args[0], PT::u64());
                    cast_arg(&mut args[1], PT::u64());
                    Ok(PT::u64())
                }
                // date vs date
                (PT::Date, PT::Date) => {
                    cast_arg(&mut args[0], PT::i64());
                    cast_arg(&mut args[1], PT::i64());
                    Ok(PT::i64())
                }
                // float vs any
                (lhs_ty @ PT::Float(_), _) => {
                    cast_arg(&mut args[1], lhs_ty);
                    Ok(lhs_ty)
                }
                (_, rhs_ty @ PT::Float(_)) => {
                    cast_arg(&mut args[0], rhs_ty);
                    Ok(rhs_ty)
                }
                // decimal vs any
                (lhs_ty @ PT::Decimal(..), _) => {
                    cast_arg(&mut args[1], lhs_ty);
                    Ok(lhs_ty)
                }
                (_, rhs_ty @ PT::Decimal(..)) => {
                    cast_arg(&mut args[0], rhs_ty);
                    Ok(rhs_ty)
                }
                (_, _) => {
                    cast_arg(&mut args[0], PT::f64());
                    cast_arg(&mut args[1], PT::f64());
                    Ok(PT::f64())
                }
            }
        }
        _ => todo!(),
    }
}

#[inline]
fn fix_cmp<I: TypeInferer>(args: &mut [ExprKind], inferer: &mut I) -> Result<()> {
    use PreciseType as PT;
    let arg0ty = args[0].fix(inferer)?;
    let arg1ty = args[1].fix(inferer)?;
    match (arg0ty, arg1ty) {
        (PT::Unknown, _) | (_, PT::Unknown) => return Err(Error::UnknownArgumentType),
        (PT::Compound, _) | (_, PT::Compound) => todo!("compound type infer"),
        (PT::Interval, _) | (_, PT::Interval) => return Err(Error::InvalidTypeToCompare),
        (PT::Null, PT::Null) => unreachable!(),
        /* first, we handle comparision between identical types */
        // float vs float
        (PT::Float(lhs_bytes), PT::Float(rhs_bytes)) => match lhs_bytes.cmp(&rhs_bytes) {
            Ordering::Greater => cast_arg(&mut args[1], PT::Float(lhs_bytes)),
            Ordering::Less => cast_arg(&mut args[0], PT::Float(rhs_bytes)),
            Ordering::Equal => (),
        },
        // decimal vs decimal
        (PT::Decimal(lhs_prec, lhs_frac), PT::Decimal(rhs_prec, rhs_frac)) => {
            let lhs_intg = lhs_prec - lhs_frac;
            let rhs_intg = rhs_prec - rhs_frac;
            match (lhs_intg.cmp(&rhs_intg), lhs_frac.cmp(&rhs_frac)) {
                (Ordering::Equal, Ordering::Equal) => (),
                (Ordering::Equal, Ordering::Greater)
                | (Ordering::Greater, Ordering::Greater | Ordering::Equal) => {
                    let res_ty = PT::Decimal(lhs_prec, lhs_frac);
                    cast_arg(&mut args[1], res_ty);
                }
                (Ordering::Equal, Ordering::Less)
                | (Ordering::Less, Ordering::Less | Ordering::Equal) => {
                    let res_ty = PT::Decimal(rhs_prec, rhs_frac);
                    cast_arg(&mut args[0], res_ty);
                }
                (Ordering::Greater, Ordering::Less) => {
                    let res_ty = PT::Decimal(lhs_intg + rhs_frac, rhs_frac);
                    cast_arg(&mut args[0], res_ty);
                    cast_arg(&mut args[1], res_ty);
                }
                (Ordering::Less, Ordering::Greater) => {
                    let res_ty = PT::Decimal(rhs_intg + lhs_frac, lhs_frac);
                    cast_arg(&mut args[0], res_ty);
                    cast_arg(&mut args[1], res_ty);
                }
            }
        }
        // bool vs bool
        (PT::Bool, PT::Bool) => (),
        // int vs int
        (PT::Int(lhs_bytes, lhs_unsigned), PT::Int(rhs_bytes, rhs_unsigned)) => {
            match lhs_bytes.cmp(&rhs_bytes) {
                Ordering::Greater => cast_arg(&mut args[1], PT::Int(lhs_bytes, rhs_unsigned)),
                Ordering::Less => cast_arg(&mut args[0], PT::Int(rhs_bytes, lhs_unsigned)),
                Ordering::Equal => (),
            }
        }
        // datetime vs datetime
        (PT::Datetime(lhs_frac), PT::Datetime(rhs_frac)) => match lhs_frac.cmp(&rhs_frac) {
            Ordering::Greater => cast_arg(&mut args[1], PT::Datetime(lhs_frac)),
            Ordering::Less => cast_arg(&mut args[0], PT::Datetime(rhs_frac)),
            Ordering::Equal => (),
        },
        // date vs date
        (PT::Date, PT::Date) => (),
        // time vs time
        (PT::Time(lhs_frac), PT::Time(rhs_frac)) => match lhs_frac.cmp(&rhs_frac) {
            Ordering::Greater => cast_arg(&mut args[1], PT::Time(lhs_frac)),
            Ordering::Less => cast_arg(&mut args[0], PT::Time(rhs_frac)),
            Ordering::Equal => (),
        },
        // bytes vs bytes
        (PT::Varchar(_, Collation::Binary), PT::Varchar(_, Collation::Binary)) => (),
        // utf8 vs utf8
        (PT::Varchar(_, Collation::Utf8mb4), PT::Varchar(_, Collation::Utf8mb4)) => (),
        // ascii vs ascii
        (PT::Varchar(_, Collation::Ascii), PT::Varchar(_, Collation::Ascii)) => (),
        // fixed bytes vs fixed bytes
        (PT::Char(lhs_bytes, Collation::Binary), PT::Char(rhs_bytes, Collation::Binary)) => {
            match lhs_bytes.cmp(&rhs_bytes) {
                Ordering::Greater => cast_arg(&mut args[1], PT::Char(lhs_bytes, Collation::Binary)),
                Ordering::Less => cast_arg(&mut args[0], PT::Char(rhs_bytes, Collation::Binary)),
                Ordering::Equal => (),
            }
        }
        // fixed utf8 vs fixed utf8
        (PT::Char(lhs_bytes, Collation::Utf8mb4), PT::Char(rhs_bytes, Collation::Utf8mb4)) => {
            match lhs_bytes.cmp(&rhs_bytes) {
                Ordering::Greater => {
                    cast_arg(&mut args[1], PT::Char(lhs_bytes, Collation::Utf8mb4))
                }
                Ordering::Less => cast_arg(&mut args[0], PT::Char(rhs_bytes, Collation::Utf8mb4)),
                Ordering::Equal => (),
            }
        }
        // fixed ascii vs fixed ascii
        (PT::Char(lhs_bytes, Collation::Ascii), PT::Char(rhs_bytes, Collation::Ascii)) => {
            match lhs_bytes.cmp(&rhs_bytes) {
                Ordering::Greater => cast_arg(&mut args[1], PT::Char(lhs_bytes, Collation::Ascii)),
                Ordering::Less => cast_arg(&mut args[0], PT::Char(rhs_bytes, Collation::Ascii)),
                Ordering::Equal => (),
            }
        }
        /* then, we handle special types vs special types */
        // datetime/date/time vs int
        (PT::Datetime(_) | PT::Date | PT::Time(_), PT::Int(bytes, unsigned)) => {
            if bytes != 8 {
                cast_arg(&mut args[1], PT::Int(8, unsigned));
            }
            cast_arg(&mut args[0], PT::Int(8, unsigned))
        }
        (PT::Int(bytes, unsigned), PT::Datetime(_) | PT::Date | PT::Time(_)) => {
            if bytes != 8 {
                cast_arg(&mut args[0], PT::Int(8, unsigned));
            }
            cast_arg(&mut args[1], PT::Int(8, unsigned))
        }
        // datetime/date/time vs bool
        (PT::Datetime(_) | PT::Date | PT::Time(_), PT::Bool)
        | (PT::Bool, PT::Datetime(_) | PT::Date | PT::Time(_)) => {
            cast_arg(&mut args[0], PT::u64());
            cast_arg(&mut args[1], PT::u64())
        }
        /* finally, we handle prioritized fallbacks */
        // float vs any
        (lhs_ty @ PT::Float(_), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Float(_)) => cast_arg(&mut args[0], rhs_ty),
        // decimal vs any
        (lhs_ty @ PT::Decimal(..), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Decimal(..)) => cast_arg(&mut args[0], rhs_ty),
        // bool vs any
        (PT::Bool, _) => cast_arg(&mut args[1], PT::Bool),
        (_, PT::Bool) => cast_arg(&mut args[0], PT::Bool),
        // datetime vs any
        (PT::Datetime(frac), _) => {
            if frac < 6 {
                cast_arg(&mut args[0], PT::Datetime(6))
            }
            cast_arg(&mut args[1], PT::Datetime(6))
        }
        (_, PT::Datetime(frac)) => {
            if frac < 6 {
                cast_arg(&mut args[1], PT::Datetime(6))
            }
            cast_arg(&mut args[0], PT::Datetime(6))
        }
        // date vs any, time vs any
        (PT::Date, _) | (_, PT::Date) | (PT::Time(_), _) | (_, PT::Time(_)) => {
            cast_arg(&mut args[0], PT::Datetime(6));
            cast_arg(&mut args[1], PT::Datetime(6))
        }
        // int vs any
        (PT::Int(..), _) | (_, PT::Int(..)) => {
            cast_arg(&mut args[0], PT::f64());
            cast_arg(&mut args[1], PT::f64());
        }
        // bytes vs any
        (PT::Varchar(bytes, Collation::Binary), _) => {
            cast_arg(&mut args[1], PT::Varchar(bytes, Collation::Binary))
        }
        (_, PT::Varchar(bytes, Collation::Binary)) => {
            cast_arg(&mut args[0], PT::Varchar(bytes, Collation::Binary))
        }
        // fixed bytes vs any
        (lhs_ty @ PT::Char(_, Collation::Binary), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Char(_, Collation::Binary)) => cast_arg(&mut args[0], rhs_ty),
        // utf8 vs any
        (PT::Varchar(bytes, Collation::Utf8mb4), _) => {
            cast_arg(&mut args[1], PT::Varchar(bytes, Collation::Utf8mb4))
        }
        (_, PT::Varchar(bytes, Collation::Utf8mb4)) => {
            cast_arg(&mut args[0], PT::Varchar(bytes, Collation::Utf8mb4))
        }
        // fixed utf8 vs any
        (lhs_ty @ PT::Char(_, Collation::Utf8mb4), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Char(_, Collation::Utf8mb4)) => cast_arg(&mut args[0], rhs_ty),
        // asci vs any
        (PT::Varchar(bytes, Collation::Ascii), _) => {
            cast_arg(&mut args[1], PT::Varchar(bytes, Collation::Ascii))
        }
        (_, PT::Varchar(bytes, Collation::Ascii)) => {
            cast_arg(&mut args[0], PT::Varchar(bytes, Collation::Ascii))
        }
        // fixed ascii vs any
        (lhs_ty @ PT::Char(_, Collation::Ascii), _) => cast_arg(&mut args[1], lhs_ty),
        (_, rhs_ty @ PT::Char(_, Collation::Ascii)) => cast_arg(&mut args[0], rhs_ty),
        // todo: specialize Char(1)
    }
    Ok(())
}

#[inline]
fn fix_pred<I: TypeInferer>(pred: &mut Pred, inferer: &mut I) -> Result<()> {
    match pred {
        Pred::Conj(es) | Pred::Disj(es) | Pred::Xor(es) => fix_bools(es, inferer),
        Pred::InSubquery(..) | Pred::NotInSubquery(..) | Pred::Exists(_) | Pred::NotExists(_) => {
            Err(Error::InferSubqueryNotSupport)
        }
        Pred::Not(e) => {
            let ty = e.fix(inferer)?;
            if !ty.is_bool() {
                cast_arg(e.as_mut(), PreciseType::bool());
            }
            Ok(())
        }
        Pred::Func { kind, args } => match *kind {
            PredFuncKind::Equal
            | PredFuncKind::NotEqual
            | PredFuncKind::Greater
            | PredFuncKind::GreaterEqual
            | PredFuncKind::Less
            | PredFuncKind::LessEqual
            | PredFuncKind::SafeEqual => fix_cmp(args.as_mut(), inferer),
            _ => todo!(),
        },
    }
}

#[inline]
fn cast_arg(arg: &mut ExprKind, new_ty: PreciseType) {
    let old = mem::take(arg);
    *arg = ExprKind::implicit_cast(old, new_ty)
}
