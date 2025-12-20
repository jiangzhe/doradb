use crate::error::{Error, Result};
use crate::{BinaryEval, UnaryEval};
use doradb_datatype::PreciseType;
use doradb_storage::col::attr::Attr;
use doradb_storage::col::bitmap::Bitmap;
use doradb_storage::col::codec::{Codec, Single};
use doradb_storage::col::sel::Sel;

/// Kinds of arithmetic expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogicKind {
    And,
    Or,
    Xor,
    Not,
    /// AndThen gathers all undetermined values after first condition in AND has been evaluated.
    /// For example, "a > 0 and b < 0", after "a > 0" evaluated, all values of true and null,
    /// has to be passed to evaluator to continue with the evaluation of "b < 0".
    AndThen,
    // OrElse gathers all undetermined values after first condition in OR has been evaluated.
    OrElse,
}

impl LogicKind {
    #[inline]
    pub fn eval_unary(&self, lhs: &Attr, sel: Option<&Sel>) -> Result<Attr> {
        match self {
            LogicKind::AndThen => Impl(AndThen).unary_eval(PreciseType::bool(), lhs, sel),
            _ => Err(Error::UnsupportedEval),
        }
    }

    #[inline]
    pub fn eval_binary(&self, lhs: &Attr, rhs: &Attr, sel: Option<&Sel>) -> Result<Attr> {
        match self {
            LogicKind::And => Impl(And).binary_eval(PreciseType::bool(), lhs, rhs, sel),
            LogicKind::Or => Impl(Or).binary_eval(PreciseType::bool(), lhs, rhs, sel),
            LogicKind::AndThen => Impl(AndThen).binary_eval(PreciseType::bool(), lhs, rhs, sel),
            _ => Err(Error::UnsupportedEval),
        }
    }
}

pub trait LogicBinary {
    fn apply_val(&self, l_val: bool, l_valid: bool, r_val: bool, r_valid: bool) -> (bool, bool);

    fn single_single(&self, l_val: bool, l_vmap: &Sel, r_val: bool, r_vmap: &Sel) -> Result<Attr>;

    fn bitmap_single(&self, l_bm: &Bitmap, l_vmap: &Sel, r_val: bool, r_vmap: &Sel)
        -> Result<Attr>;

    fn single_bitmap(&self, l_val: bool, l_vmap: &Sel, r_bm: &Bitmap, r_vmap: &Sel)
        -> Result<Attr>;

    fn bitmap_bitmap(
        &self,
        l_bm: &Bitmap,
        l_vmap: &Sel,
        r_bm: &Bitmap,
        r_vmap: &Sel,
    ) -> Result<Attr>;
}

pub trait LogicUnary {
    fn single(&self, val: bool, validity: &Sel, sel: Option<&Sel>) -> Result<Attr>;

    fn bitmap(&self, bm: &Bitmap, validity: &Sel, sel: Option<&Sel>) -> Result<Attr>;
}

struct Impl<T>(T);

impl<T: LogicBinary> BinaryEval for Impl<T> {
    #[inline]
    fn binary_eval(
        &self,
        res_ty: PreciseType,
        lhs: &Attr,
        rhs: &Attr,
        sel: Option<&Sel>,
    ) -> Result<Attr> {
        debug_assert_eq!(PreciseType::bool(), res_ty);
        let n_records = lhs.n_records();
        if n_records != rhs.n_records() {
            return Err(Error::RowNumberMismatch);
        }
        if let Some(sel) = sel {
            match sel {
                Sel::None(_) => return Ok(Attr::new_null(PreciseType::bool(), n_records as u16)),
                Sel::Index { count, indexes, .. } => {
                    let mut valids = [0u16; 6];
                    let mut valid_count = 0;
                    let mut vals = [false; 6];
                    let mut all_falses = true;
                    let mut all_trues = true;
                    for (i, idx) in indexes[..*count as usize].iter().enumerate() {
                        let idx = *idx as usize;
                        let (l_valid, l_val) = lhs.bool_at(idx)?;
                        let (r_valid, r_val) = rhs.bool_at(idx)?;
                        let (valid, val) = self.0.apply_val(l_val, l_valid, r_val, r_valid);
                        vals[i] = val;
                        if valid {
                            valids[valid_count] = idx as u16;
                            valid_count += 1;
                            if val {
                                all_falses = false;
                            } else {
                                all_trues = false;
                            }
                        }
                    }
                    if valid_count == 0 {
                        return Ok(Attr::new_null(PreciseType::bool(), n_records as u16));
                    }
                    if all_falses {
                        return Ok(Attr::new_single(
                            PreciseType::bool(),
                            Single::new_bool(false, n_records as u16),
                            Sel::Index {
                                count: valid_count as u8,
                                len: n_records as u16,
                                indexes: valids,
                            },
                        ));
                    }
                    if all_trues {
                        return Ok(Attr::new_single(
                            PreciseType::bool(),
                            Single::new_bool(true, n_records as u16),
                            Sel::Index {
                                count: valid_count as u8,
                                len: n_records as u16,
                                indexes: valids,
                            },
                        ));
                    }
                    // use bitmap to store values
                    let mut bm = Bitmap::zeroes(n_records);
                    for (idx, val) in indexes[..*count as usize].iter().zip(vals) {
                        bm.set(*idx as usize, val)?;
                    }
                    return Ok(Attr::new_bitmap(
                        bm,
                        Sel::Index {
                            count: valid_count as u8,
                            len: n_records as u16,
                            indexes: valids,
                        },
                    ));
                }
                _ => (),
            }
        }
        match (&lhs.codec, &rhs.codec) {
            (Codec::Single(l), Codec::Single(r)) => {
                let l_val = if lhs.validity.is_none() {
                    false
                } else {
                    l.view_bool()
                };
                let r_val = if rhs.validity.is_none() {
                    false
                } else {
                    r.view_bool()
                };
                self.0
                    .single_single(l_val, &lhs.validity, r_val, &rhs.validity)
            }
            (Codec::Bitmap(l), Codec::Single(r)) => {
                let r_val = if rhs.validity.is_none() {
                    false
                } else {
                    r.view_bool()
                };
                self.0.bitmap_single(l, &lhs.validity, r_val, &rhs.validity)
            }
            (Codec::Single(l), Codec::Bitmap(r)) => {
                let l_val = if lhs.validity.is_none() {
                    false
                } else {
                    l.view_bool()
                };
                self.0.single_bitmap(l_val, &lhs.validity, r, &rhs.validity)
            }
            (Codec::Bitmap(l), Codec::Bitmap(r)) => {
                self.0.bitmap_bitmap(l, &lhs.validity, r, &rhs.validity)
            }
            (Codec::Empty, _) | (_, Codec::Empty) => Ok(Attr::empty(res_ty)),
            // Logic expression does not support array codec.
            (Codec::Array(_), _) | (_, Codec::Array(_)) => Err(Error::UnsupportedEval),
        }
    }
}

impl<T: LogicUnary> UnaryEval for Impl<T> {
    #[inline]
    fn unary_eval(&self, res_ty: PreciseType, lhs: &Attr, sel: Option<&Sel>) -> Result<Attr> {
        debug_assert_eq!(PreciseType::bool(), res_ty);
        match &lhs.codec {
            Codec::Single(s) => {
                let val = if lhs.validity.is_none() {
                    false
                } else {
                    s.view_bool()
                };
                self.0.single(val, &lhs.validity, sel)
            }
            Codec::Bitmap(bm) => self.0.bitmap(bm, &lhs.validity, sel),
            _ => Err(Error::UnsupportedEval),
        }
    }
}

struct And;

impl LogicBinary for And {
    #[inline]
    fn apply_val(&self, l_val: bool, l_valid: bool, r_val: bool, r_valid: bool) -> (bool, bool) {
        // both not null
        if l_valid && r_valid {
            return (true, l_val && r_val);
        }
        // left not null
        if l_valid {
            // left=false => (true, false)
            if !l_val {
                return (true, false);
            }
            // left=true => (r_valid, r_val)
            return (r_valid, r_val);
        }
        // left null, right not null
        if r_valid {
            // right=true => (false, false)
            // even right is null, we still can not determine final value
            // because left is null(unknown).
            if r_val {
                return (false, false);
            }
            // right=false => (true, false)
            return (true, false);
        }
        // both null
        (false, false)
    }

    #[inline]
    fn single_single(&self, l_val: bool, l_vmap: &Sel, r_val: bool, r_vmap: &Sel) -> Result<Attr> {
        let n_records = l_vmap.n_records() as u16;
        match (l_val, r_val) {
            (false, false) => {
                // only keep (null, null) as null, any other will be false.
                let validity = l_vmap.union(r_vmap)?;
                if validity.is_none() {
                    Ok(Attr::new_null(PreciseType::bool(), n_records))
                } else {
                    Ok(Attr::new_single(
                        PreciseType::bool(),
                        Single::new_bool(false, n_records),
                        validity,
                    ))
                }
            }
            (false, true) => {
                // if left is not null, final result is false.
                // if left is null, final result is null.
                // So, result is always equal to left.
                if l_vmap.is_none() {
                    Ok(Attr::new_null(PreciseType::bool(), n_records))
                } else {
                    Ok(Attr::new_single(
                        PreciseType::bool(),
                        Single::new_bool(false, n_records),
                        l_vmap.clone_to_owned(),
                    ))
                }
            }
            (true, false) => {
                // similar to above case, result is always equal to right.
                if r_vmap.is_none() {
                    Ok(Attr::new_null(PreciseType::bool(), n_records))
                } else {
                    Ok(Attr::new_single(
                        PreciseType::bool(),
                        Single::new_bool(false, n_records),
                        r_vmap.clone_to_owned(),
                    ))
                }
            }
            (true, true) => {
                // only keep (true, ture) as true, any other will be null.
                let validity = l_vmap.intersect(r_vmap)?;
                if validity.is_none() {
                    Ok(Attr::new_null(PreciseType::bool(), n_records))
                } else {
                    Ok(Attr::new_single(
                        PreciseType::bool(),
                        Single::new_bool(true, n_records),
                        validity,
                    ))
                }
            }
        }
    }

    #[inline]
    fn bitmap_single(
        &self,
        l_bm: &Bitmap,
        l_vmap: &Sel,
        r_val: bool,
        r_vmap: &Sel,
    ) -> Result<Attr> {
        let n_records = l_bm.len() as u16;
        if r_val {
            // (null,  null) => null
            // (false, null) => false
            // (true,  null) => null   <- update on left side required
            // (null,  true) => null
            // (false, true) => false
            // (true,  true) => true
            let mut changes = l_bm.to_owned();
            changes.intersect_sel(l_vmap)?; // only pick valid true values.
            changes.intersect_sel(&r_vmap.inverse())?; // intersect left trues with right nulls
            changes.inverse(); // create mask to left validity
            let validity = l_vmap.intersect_bm(&changes)?; // apply validity
            if validity.is_none() {
                Ok(Attr::new_null(PreciseType::bool(), n_records))
            } else {
                Ok(Attr::new_bitmap(l_bm.to_owned(), validity))
            }
        } else {
            // (null,  null ) => null
            // (false, null ) => false  <- update on right side required.
            // (true,  null ) => null
            // (null,  false) => false
            // (false, false) => false
            // (true,  false) => false
            let mut changes = l_bm.to_owned();
            changes.inverse(); // convert false to true, true to false.
            changes.intersect_sel(l_vmap)?; // intersect validity to get all false values as bitmap.
            let validity = r_vmap.union_bm(&changes)?;
            if validity.is_none() {
                Ok(Attr::new_null(PreciseType::bool(), n_records))
            } else {
                Ok(Attr::new_single(
                    PreciseType::bool(),
                    Single::new_bool(false, n_records),
                    validity,
                ))
            }
        }
    }

    #[inline]
    fn single_bitmap(
        &self,
        l_val: bool,
        l_vmap: &Sel,
        r_bm: &Bitmap,
        r_vmap: &Sel,
    ) -> Result<Attr> {
        // commutative law
        self.bitmap_single(r_bm, r_vmap, l_val, l_vmap)
    }

    #[inline]
    fn bitmap_bitmap(
        &self,
        l_bm: &Bitmap,
        l_vmap: &Sel,
        r_bm: &Bitmap,
        r_vmap: &Sel,
    ) -> Result<Attr> {
        // if any side is false, reserve it.
        let ff = {
            let mut falses = l_bm.to_owned();
            falses.inverse();
            falses.intersect_sel(l_vmap)?; // left valid falses
            let mut r_falses = r_bm.to_owned();
            r_falses.inverse();
            r_falses.intersect_sel(r_vmap)?; // right valid falses
            falses.union(&r_falses)?; // either left is false or right is false
            falses
        };
        let validity = l_vmap.intersect(r_vmap)?;
        let validity = validity.union_bm(&ff)?;
        let n_records = l_bm.len() as u16;
        if validity.is_none() {
            Ok(Attr::new_null(PreciseType::bool(), n_records))
        } else {
            let mut bm = l_bm.to_owned();
            bm.intersect(r_bm)?;
            Ok(Attr::new_bitmap(bm, validity))
        }
    }
}

struct Or;

impl LogicBinary for Or {
    #[inline]
    fn apply_val(&self, l_val: bool, l_valid: bool, r_val: bool, r_valid: bool) -> (bool, bool) {
        // both not null
        if l_valid && r_valid {
            return (true, l_val || r_val);
        }
        // left not null
        if l_valid {
            // left=true => (true, true)
            if l_val {
                return (true, true);
            }
            // left=false => (r_valid, r_val)
            return (r_valid, r_val);
        }
        // left null, right not null
        if r_valid {
            // right=false => (false, false)
            if !r_val {
                return (false, false);
            }
            // right=true => (true, true)
            return (true, true);
        }
        // both null
        (false, false)
    }

    #[inline]
    fn single_single(&self, l_val: bool, l_vmap: &Sel, r_val: bool, r_vmap: &Sel) -> Result<Attr> {
        let n_records = l_vmap.n_records() as u16;
        match (l_val, r_val) {
            (false, false) => {
                // only keep (false, false) as false, any other will be null.
                let validity = l_vmap.intersect(r_vmap)?;
                if validity.is_none() {
                    Ok(Attr::new_null(PreciseType::bool(), n_records))
                } else {
                    Ok(Attr::new_single(
                        PreciseType::bool(),
                        Single::new_bool(false, n_records),
                        validity,
                    ))
                }
            }
            (false, true) => {
                // if right is not null, final result is true.
                // if right is null, final result is null.
                // So, result is always equal to right.
                if r_vmap.is_none() {
                    Ok(Attr::new_null(PreciseType::bool(), n_records))
                } else {
                    Ok(Attr::new_single(
                        PreciseType::bool(),
                        Single::new_bool(true, n_records),
                        r_vmap.clone_to_owned(),
                    ))
                }
            }
            (true, false) => {
                // similar to above case, result is always equal to left.
                if l_vmap.is_none() {
                    Ok(Attr::new_null(PreciseType::bool(), n_records))
                } else {
                    Ok(Attr::new_single(
                        PreciseType::bool(),
                        Single::new_bool(true, n_records),
                        l_vmap.clone_to_owned(),
                    ))
                }
            }
            (true, true) => {
                // only keep (null, null) as null, any other will be true.
                let validity = l_vmap.union(r_vmap)?;
                if validity.is_none() {
                    Ok(Attr::new_null(PreciseType::bool(), n_records))
                } else {
                    Ok(Attr::new_single(
                        PreciseType::bool(),
                        Single::new_bool(true, n_records),
                        validity,
                    ))
                }
            }
        }
    }

    #[inline]
    fn bitmap_single(
        &self,
        l_bm: &Bitmap,
        l_vmap: &Sel,
        r_val: bool,
        r_vmap: &Sel,
    ) -> Result<Attr> {
        let n_records = l_bm.len() as u16;
        if r_val {
            // (null,  null) => null
            // (false, null) => null
            // (true,  null) => true    <- update on right side required.
            // (null,  true) => true
            // (false, true) => true
            // (true,  true) => true
            let mut changes = l_bm.to_owned();
            changes.intersect_sel(l_vmap)?; // only pick valid true values.
            let validity = r_vmap.union_bm(&changes)?;
            if validity.is_none() {
                Ok(Attr::new_null(PreciseType::bool(), n_records))
            } else {
                Ok(Attr::new_single(
                    PreciseType::bool(),
                    Single::new_bool(true, n_records),
                    validity,
                ))
            }
        } else {
            // (null,  null ) => null
            // (false, null ) => null  <- update on left side required.
            // (true,  null ) => true
            // (null,  false) => null
            // (false, false) => false
            // (true,  false) => true
            let mut changes = l_bm.to_owned();
            changes.inverse(); // pick false
            changes.intersect_sel(l_vmap)?; // pick valid false.
            changes.intersect_sel(&r_vmap.inverse())?; // intersect left false with right null.
            changes.inverse(); // create mask
            let validity = l_vmap.intersect_bm(&changes)?;
            if validity.is_none() {
                Ok(Attr::new_null(PreciseType::bool(), n_records))
            } else {
                Ok(Attr::new_bitmap(l_bm.to_owned(), validity))
            }
        }
    }

    #[inline]
    fn single_bitmap(
        &self,
        l_val: bool,
        l_vmap: &Sel,
        r_bm: &Bitmap,
        r_vmap: &Sel,
    ) -> Result<Attr> {
        // commutative law
        self.bitmap_single(r_bm, r_vmap, l_val, l_vmap)
    }

    #[inline]
    fn bitmap_bitmap(
        &self,
        l_bm: &Bitmap,
        l_vmap: &Sel,
        r_bm: &Bitmap,
        r_vmap: &Sel,
    ) -> Result<Attr> {
        // if any side is true, reserve it.
        let tt = {
            let mut trues = l_bm.to_owned();
            trues.intersect_sel(l_vmap)?; // left valid trues
            let mut r_trues = r_bm.to_owned();
            r_trues.intersect_sel(r_vmap)?; // right valid trues
            trues.union(&r_trues)?; // either left is true or right is true
            trues
        };
        let validity = l_vmap.intersect(r_vmap)?;
        let validity = validity.union_bm(&tt)?;
        let n_records = l_bm.len() as u16;
        if validity.is_none() {
            Ok(Attr::new_null(PreciseType::bool(), n_records))
        } else {
            let mut bm = l_bm.to_owned();
            bm.union(r_bm)?;
            Ok(Attr::new_bitmap(bm, validity))
        }
    }
}

struct Not;

/// AndThen defines the selection for further evaluation in CNF context.
/// The rules are as below:
/// null  => true
/// true  => true
/// false => false
///
/// There might be base selection with only true/false values.
/// Current selection must be based on the base selection if present.
struct AndThen;

impl LogicBinary for AndThen {
    #[inline]
    fn apply_val(&self, l_val: bool, l_valid: bool, r_val: bool, r_valid: bool) -> (bool, bool) {
        debug_assert!(l_valid);
        (true, l_val && !r_valid || r_val)
    }

    #[inline]
    fn single_single(&self, l_val: bool, l_vmap: &Sel, r_val: bool, r_vmap: &Sel) -> Result<Attr> {
        debug_assert!(l_vmap.is_all());
        let n_records = l_vmap.n_records() as u16;
        if l_val {
            if r_val {
                // all values are true or null.
                Ok(Attr::new_single(
                    PreciseType::bool(),
                    Single::new_bool(true, n_records),
                    Sel::All(n_records),
                ))
            } else {
                // convert nulls to true
                let nulls = r_vmap.inverse();
                Ok(Attr::from(nulls))
            }
        } else {
            // no base values, so returns empty selection.
            Ok(Attr::new_single(
                PreciseType::bool(),
                Single::new_bool(false, n_records),
                Sel::All(n_records),
            ))
        }
    }

    #[inline]
    fn bitmap_single(
        &self,
        l_bm: &Bitmap,
        l_vmap: &Sel,
        r_val: bool,
        r_vmap: &Sel,
    ) -> Result<Attr> {
        debug_assert!(l_vmap.is_all());
        let n_records = l_bm.len() as u16;
        if r_val {
            // all values are true or null, so returns base selection.
            Ok(Attr::new_bitmap(l_bm.to_owned(), Sel::All(n_records)))
        } else {
            // intersect nulls
            let mut bm = l_bm.to_owned();
            bm.intersect_sel(&r_vmap.inverse())?;
            Ok(Attr::new_bitmap(bm, Sel::All(n_records)))
        }
    }

    #[inline]
    fn single_bitmap(
        &self,
        l_val: bool,
        l_vmap: &Sel,
        r_bm: &Bitmap,
        r_vmap: &Sel,
    ) -> Result<Attr> {
        debug_assert!(l_vmap.is_all());
        let n_records = l_vmap.n_records() as u16;
        if l_val {
            // base selection is all.
            let nulls = r_vmap.inverse();
            let mut bm = r_bm.to_owned();
            bm.union_sel(&nulls)?; // convert null to true
            Ok(Attr::new_bitmap(bm, Sel::All(n_records)))
        } else {
            // base selection is empty.
            Ok(Attr::new_single(
                PreciseType::bool(),
                Single::new_bool(false, n_records),
                Sel::All(n_records),
            ))
        }
    }

    #[inline]
    fn bitmap_bitmap(
        &self,
        l_bm: &Bitmap,
        l_vmap: &Sel,
        r_bm: &Bitmap,
        r_vmap: &Sel,
    ) -> Result<Attr> {
        debug_assert!(l_vmap.is_all());
        let n_records = l_vmap.n_records() as u16;
        let nulls = r_vmap.inverse();
        let mut bm = r_bm.to_owned();
        bm.union_sel(&nulls)?; // convert null to true
        bm.intersect(l_bm)?; // intersect base
        Ok(Attr::new_bitmap(bm, Sel::All(n_records)))
    }
}

impl LogicUnary for AndThen {
    #[inline]
    fn single(&self, val: bool, validity: &Sel, sel: Option<&Sel>) -> Result<Attr> {
        let res = if val {
            // all values are true or null
            let out = sel
                .map(Sel::clone_to_owned)
                .unwrap_or_else(|| Sel::All(validity.n_records() as u16));
            Attr::from(out)
        } else {
            // only null values should be output
            let nulls = validity.inverse();
            let out = nulls.intersect(sel.unwrap_or(&Sel::All(validity.n_records() as u16)))?;
            Attr::from(out)
        };
        Ok(res)
    }

    #[inline]
    fn bitmap(&self, bm: &Bitmap, validity: &Sel, sel: Option<&Sel>) -> Result<Attr> {
        let trues = Sel::from(bm.to_owned());
        let nulls = validity.inverse();
        // union true and nulls
        let tns = trues.union(&nulls)?;
        let out = tns.intersect(sel.unwrap_or(&Sel::All(bm.len() as u16)))?;
        Ok(Attr::from(out))
    }
}

struct OrElse;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logic_and() {
        let a1 = Attr::new_null(PreciseType::bool(), 64);
        let a2 = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(true, 64),
            Sel::All(64),
        );
        let a3 = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(false, 64),
            Sel::All(64),
        );
        // 10 true, 54 false.
        let mut bm1 = Bitmap::zeroes(64);
        for i in 0..10 {
            bm1.set(i, true).unwrap();
        }
        let a4 = Attr::new_bitmap(bm1.to_owned(), Sel::All(64));
        let a5 = Attr::new_bitmap(bm1.to_owned(), Sel::new_indexes(64, vec![0, 1, 2, 20, 21]));
        // 54 true, 10 false.
        let mut bm2 = Bitmap::ones(64);
        for i in 5..15 {
            bm2.set(i, false).unwrap();
        }
        let a6 = Attr::new_bitmap(bm2.to_owned(), Sel::All(64));
        let a7 = Attr::new_bitmap(bm2.to_owned(), Sel::new_indexes(64, vec![10, 20, 30]));

        let and = LogicKind::And;
        // a1
        let res = and.eval_binary(&a1, &a1, Some(&Sel::All(64))).unwrap();
        assert_all_null(&res);
        let res = and.eval_binary(&a1, &a2, Some(&Sel::All(64))).unwrap();
        assert_all_null(&res);
        let res = and.eval_binary(&a1, &a3, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a1, &a4, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 54);
        let res = and.eval_binary(&a1, &a5, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 2);
        // a2
        let res = and.eval_binary(&a2, &a1, Some(&Sel::All(64))).unwrap();
        assert_all_null(&res);
        let res = and.eval_binary(&a2, &a2, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = and.eval_binary(&a2, &a3, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a2, &a4, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 10);
        let res = and.eval_binary(&a2, &a5, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 3);
        let res = and.eval_binary(&a2, &a6, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 64 - 10);
        let res = and.eval_binary(&a2, &a7, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 2);
        // a3
        let res = and.eval_binary(&a3, &a1, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a3, &a2, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a3, &a3, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a3, &a4, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a3, &a5, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a3, &a6, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a3, &a7, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        // a4
        let res = and.eval_binary(&a4, &a1, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 54);
        let res = and.eval_binary(&a4, &a2, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 54);
        let res = and.eval_binary(&a4, &a3, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a4, &a4, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 54);
        let res = and.eval_binary(&a4, &a5, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 54);
        let res = and.eval_binary(&a4, &a6, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 59);
        let res = and.eval_binary(&a4, &a7, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 54);
        // a6
        let res = and.eval_binary(&a6, &a1, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 10);
        let res = and.eval_binary(&a6, &a2, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 10);
        let res = and.eval_binary(&a6, &a3, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = and.eval_binary(&a6, &a4, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 59);
        let res = and.eval_binary(&a6, &a5, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 12);
        let res = and.eval_binary(&a6, &a6, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 10);
        let res = and.eval_binary(&a6, &a7, Some(&Sel::All(64))).unwrap();
        assert_false_count(&res, 10);
    }

    #[test]
    fn test_logic_or() {
        let a1 = Attr::new_null(PreciseType::bool(), 64);
        let a2 = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(true, 64),
            Sel::All(64),
        );
        let a3 = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(false, 64),
            Sel::All(64),
        );
        // 10 true, 54 false.
        let mut bm1 = Bitmap::zeroes(64);
        for i in 0..10 {
            bm1.set(i, true).unwrap();
        }
        let a4 = Attr::new_bitmap(bm1.to_owned(), Sel::All(64));
        let a5 = Attr::new_bitmap(bm1.to_owned(), Sel::new_indexes(64, vec![0, 1, 2, 20, 21]));
        // 54 true, 10 false.
        let mut bm2 = Bitmap::ones(64);
        for i in 5..15 {
            bm2.set(i, false).unwrap();
        }
        let a6 = Attr::new_bitmap(bm2.to_owned(), Sel::All(64));
        let a7 = Attr::new_bitmap(bm2.to_owned(), Sel::new_indexes(64, vec![10, 20, 30]));

        let or = LogicKind::Or;
        // a1
        let res = or.eval_binary(&a1, &a1, Some(&Sel::All(64))).unwrap();
        assert_all_null(&res);
        let res = or.eval_binary(&a1, &a2, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a1, &a3, Some(&Sel::All(64))).unwrap();
        assert_all_null(&res);
        let res = or.eval_binary(&a1, &a4, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 10);
        let res = or.eval_binary(&a1, &a5, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 3);
        // a2
        let res = or.eval_binary(&a2, &a1, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a2, &a2, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a2, &a3, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a2, &a4, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a2, &a5, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a2, &a6, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a2, &a7, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        // a3
        let res = or.eval_binary(&a3, &a1, Some(&Sel::All(64))).unwrap();
        assert_all_null(&res);
        let res = or.eval_binary(&a3, &a2, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a3, &a3, Some(&Sel::All(64))).unwrap();
        assert_all_false(&res);
        let res = or.eval_binary(&a3, &a4, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 10);
        let res = or.eval_binary(&a3, &a5, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 3);
        let res = or.eval_binary(&a3, &a6, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 54);
        let res = or.eval_binary(&a3, &a7, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 2);
        // a4
        let res = or.eval_binary(&a4, &a1, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 10);
        let res = or.eval_binary(&a4, &a2, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a4, &a3, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 10);
        let res = or.eval_binary(&a4, &a4, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 10);
        let res = or.eval_binary(&a4, &a5, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 10);
        let res = or.eval_binary(&a4, &a6, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 59);
        let res = or.eval_binary(&a4, &a7, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 12);
        // a6
        let res = or.eval_binary(&a6, &a1, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 54);
        let res = or.eval_binary(&a6, &a2, Some(&Sel::All(64))).unwrap();
        assert_all_true(&res);
        let res = or.eval_binary(&a6, &a3, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 54);
        let res = or.eval_binary(&a6, &a4, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 59);
        let res = or.eval_binary(&a6, &a5, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 54);
        let res = or.eval_binary(&a6, &a6, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 54);
        let res = or.eval_binary(&a6, &a7, Some(&Sel::All(64))).unwrap();
        assert_true_count(&res, 54);
    }

    #[test]
    fn test_logic_and_then() {
        let a1 = Attr::new_null(PreciseType::bool(), 64);
        let a2 = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(true, 64),
            Sel::All(64),
        );
        let a3 = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(false, 64),
            Sel::All(64),
        );
        // 10 true, 54 false.
        let mut bm1 = Bitmap::zeroes(64);
        for i in 0..10 {
            bm1.set(i, true).unwrap();
        }
        let a4 = Attr::new_bitmap(bm1.to_owned(), Sel::All(64));
        let a5 = Attr::new_bitmap(bm1.to_owned(), Sel::new_indexes(64, vec![0, 1, 2, 20, 21]));
        // 54 true, 10 false.
        let mut bm2 = Bitmap::ones(64);
        for i in 5..15 {
            bm2.set(i, false).unwrap();
        }
        let a6 = Attr::new_bitmap(bm2.to_owned(), Sel::All(64));
        let a7 = Attr::new_bitmap(bm2.to_owned(), Sel::new_indexes(64, vec![10, 20, 30]));

        let and_then = LogicKind::AndThen;
        // unary
        let res = and_then.eval_unary(&a1, None).unwrap();
        assert_true_count(&res, 64);
        let res = and_then.eval_unary(&a2, None).unwrap();
        assert_true_count(&res, 64);
        let res = and_then.eval_unary(&a3, None).unwrap();
        assert_false_count(&res, 64);
        let res = and_then.eval_unary(&a4, None).unwrap();
        assert_true_count(&res, 10);
        let res = and_then.eval_unary(&a5, None).unwrap();
        assert_true_count(&res, 62);
        let res = and_then.eval_unary(&a6, None).unwrap();
        assert_true_count(&res, 54);
        let res = and_then.eval_unary(&a7, None).unwrap();
        assert_true_count(&res, 63);
        // binary
        let res = and_then.eval_binary(&a2, &a3, None).unwrap();
        assert_false_count(&res, 64);
        let res = and_then.eval_binary(&a2, &a4, None).unwrap();
        assert_true_count(&res, 10);
        let res = and_then.eval_binary(&a2, &a5, None).unwrap();
        assert_true_count(&res, 62);
        let res = and_then.eval_binary(&a2, &a6, None).unwrap();
        assert_true_count(&res, 54);
        let res = and_then.eval_binary(&a2, &a7, None).unwrap();
        assert_true_count(&res, 63);
        let res = and_then.eval_binary(&a4, &a6, None).unwrap();
        assert_true_count(&res, 5);
        let res = and_then.eval_binary(&a4, &a7, None).unwrap();
        assert_true_count(&res, 10);
    }

    #[test]
    fn test_logic_sel() {
        let a1 = Attr::new_null(PreciseType::bool(), 64);
        let a2 = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(true, 64),
            Sel::All(64),
        );
        let a3 = Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(false, 64),
            Sel::All(64),
        );
        // 10 true, 54 false.
        let mut bm1 = Bitmap::zeroes(64);
        for i in 0..10 {
            bm1.set(i, true).unwrap();
        }
        let a4 = Attr::new_bitmap(bm1.to_owned(), Sel::All(64));
        let a5 = Attr::new_bitmap(bm1.to_owned(), Sel::new_indexes(64, vec![0, 1, 2, 20, 21]));
        // 54 true, 10 false.
        let mut bm2 = Bitmap::ones(64);
        for i in 5..15 {
            bm2.set(i, false).unwrap();
        }
        let a6 = Attr::new_bitmap(bm2.to_owned(), Sel::All(64));
        let a7 = Attr::new_bitmap(bm2.to_owned(), Sel::new_indexes(64, vec![10, 20, 30]));

        let and = LogicKind::And;
        // a1
        let res = and.eval_binary(&a1, &a1, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and.eval_binary(&a1, &a2, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and.eval_binary(&a1, &a3, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and.eval_binary(&a1, &a4, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and.eval_binary(&a1, &a5, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        // a6
        let res = and.eval_binary(&a6, &a1, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and.eval_binary(&a6, &a2, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and
            .eval_binary(&a6, &a2, Some(&Sel::new_indexes(64, vec![11, 42])))
            .unwrap();
        let (valid, val) = res.bool_at(11).unwrap();
        assert!(valid && !val);
        let (valid, val) = res.bool_at(42).unwrap();
        assert!(valid && val);
        let res = and.eval_binary(&a6, &a3, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and.eval_binary(&a6, &a4, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and
            .eval_binary(&a6, &a4, Some(&Sel::new_indexes(64, vec![1])))
            .unwrap();
        let (valid, val) = res.bool_at(1).unwrap();
        assert!(valid && val);
        let res = and
            .eval_binary(&a6, &a4, Some(&Sel::new_indexes(64, vec![1, 30])))
            .unwrap();
        let (valid, val) = res.bool_at(30).unwrap();
        assert!(valid && !val);
        let res = and.eval_binary(&a6, &a5, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and.eval_binary(&a6, &a6, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        let res = and.eval_binary(&a6, &a7, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        // a7
        let res = and
            .eval_binary(&a1, &a7, Some(&Sel::new_indexes(64, vec![10, 11, 30])))
            .unwrap();
        let (valid, val) = res.bool_at(10).unwrap();
        assert!(valid && !val);
        let (valid, _) = res.bool_at(11).unwrap();
        assert!(!valid);
        let (valid, _) = res.bool_at(30).unwrap();
        assert!(!valid);
    }

    fn assert_all_null(attr: &Attr) {
        for i in 0..attr.n_records() {
            assert!(!attr.is_valid(i).unwrap())
        }
    }

    fn assert_all_valid(attr: &Attr) {
        for i in 0..attr.n_records() {
            assert!(attr.is_valid(i).unwrap())
        }
    }

    fn assert_all_true(attr: &Attr) {
        assert_all_valid(attr);
        if let Some(bm) = attr.codec.as_bitmap() {
            assert_eq!(bm.true_count(), bm.len());
        } else if let Some(s) = attr.codec.as_single() {
            assert!(s.view_bool())
        } else {
            unreachable!()
        }
    }

    fn assert_all_false(attr: &Attr) {
        assert_all_valid(attr);
        if let Some(bm) = attr.codec.as_bitmap() {
            assert_eq!(bm.true_count(), 0);
        } else if let Some(s) = attr.codec.as_single() {
            assert!(!s.view_bool())
        } else {
            unreachable!()
        }
    }

    fn assert_true_count(attr: &Attr, count: usize) {
        if let Some(bm) = attr.codec.as_bitmap() {
            let sel = Sel::from(bm.to_owned());
            let trues = sel.intersect(&attr.validity).unwrap();
            assert_eq!(count, trues.n_filtered())
        } else if let Some(s) = attr.codec.as_single() {
            if s.view_bool() {
                assert_eq!(count, attr.validity.n_filtered())
            } else {
                assert_eq!(count, 0)
            }
        } else {
            unreachable!()
        }
    }

    fn assert_false_count(attr: &Attr, count: usize) {
        if let Some(bm) = attr.codec.as_bitmap() {
            let mut falses = bm.to_owned();
            falses.inverse();
            falses.intersect_sel(&attr.validity).unwrap();
            assert_eq!(count, falses.true_count());
        } else if let Some(s) = attr.codec.as_single() {
            if s.view_bool() {
                assert_eq!(count, 0)
            } else {
                assert_eq!(count, attr.validity.n_filtered())
            }
        } else {
            unreachable!()
        }
    }

    fn assert_valid_count(attr: &Attr, count: usize) {
        assert_eq!(attr.validity.n_filtered(), count)
    }
}
