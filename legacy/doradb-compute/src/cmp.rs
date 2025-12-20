use crate::error::{Error, Result};
use crate::BinaryEval;
use doradb_datatype::PreciseType;
use doradb_expr::PredFuncKind;
use doradb_storage::col::attr::Attr;
use doradb_storage::col::bitmap::Bitmap;
use doradb_storage::col::codec::{Codec, Single};
use doradb_storage::col::repr::ByteRepr;
use doradb_storage::col::sel::Sel;

/// Kinds of comparison expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmpKind {
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    NotEqual,
}

impl CmpKind {
    #[inline]
    pub fn from_pred(kind: PredFuncKind) -> Option<Self> {
        let res = match kind {
            PredFuncKind::Equal => CmpKind::Equal,
            PredFuncKind::Greater => CmpKind::Greater,
            PredFuncKind::GreaterEqual => CmpKind::GreaterEqual,
            PredFuncKind::Less => CmpKind::Less,
            PredFuncKind::LessEqual => CmpKind::LessEqual,
            PredFuncKind::NotEqual => CmpKind::NotEqual,
            _ => return None,
        };
        Some(res)
    }

    /// Evaluate comparison expression with pair of attributes.
    #[inline]
    pub fn eval(&self, lhs: &Attr, rhs: &Attr, sel: Option<&Sel>) -> Result<Attr> {
        match (self, lhs.ty, rhs.ty) {
            (CmpKind::Equal, PreciseType::Int(4, false), PreciseType::Int(4, false)) => {
                Impl(EqualI32).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::Equal, PreciseType::Int(8, false), PreciseType::Int(8, false)) => {
                Impl(EqualI64).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::Greater, PreciseType::Int(4, false), PreciseType::Int(4, false)) => {
                Impl(GreaterI32).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::Greater, PreciseType::Int(8, false), PreciseType::Int(8, false)) => {
                Impl(GreaterI64).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::GreaterEqual, PreciseType::Int(4, false), PreciseType::Int(4, false)) => {
                Impl(GreaterEqualI32).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::GreaterEqual, PreciseType::Int(8, false), PreciseType::Int(8, false)) => {
                Impl(GreaterEqualI64).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::Less, PreciseType::Int(4, false), PreciseType::Int(4, false)) => {
                Impl(LessI32).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::Less, PreciseType::Int(8, false), PreciseType::Int(8, false)) => {
                Impl(LessI64).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::LessEqual, PreciseType::Int(4, false), PreciseType::Int(4, false)) => {
                Impl(LessEqualI32).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::LessEqual, PreciseType::Int(8, false), PreciseType::Int(8, false)) => {
                Impl(LessEqualI64).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::NotEqual, PreciseType::Int(4, false), PreciseType::Int(4, false)) => {
                Impl(NotEqualI32).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            (CmpKind::NotEqual, PreciseType::Int(8, false), PreciseType::Int(8, false)) => {
                Impl(NotEqualI64).binary_eval(PreciseType::bool(), lhs, rhs, sel)
            }
            _ => Err(Error::UnsupportedEval),
        }
    }
}

pub trait CmpEval {
    type L: ByteRepr + Copy;
    type R: ByteRepr + Copy;

    /// Apply comparison between pair of element.
    fn apply_bool(&self, lhs: Self::L, rhs: Self::R) -> bool;
}

/// When selection is specified, all other values must be set to valid + false.
/// This is in order to achieve CNF and DNF evaluation.
struct Impl<T>(T);

impl<T: CmpEval> Impl<T> {
    /// Apply comparison between left chunk and right singleton.
    #[inline]
    fn apply_left_chunk(&self, lhs: &[T::L], rhs: T::R) -> u64 {
        let mut res = 0u64;
        let mut mask = 1;
        for l_val in lhs {
            let flag = self.0.apply_bool(*l_val, rhs);
            res |= if flag { mask } else { 0 };
            mask <<= 1;
        }
        res
    }

    /// Apply comparison between left singleton and right chunk.
    #[inline]
    fn apply_right_chunk(&self, lhs: T::L, rhs: &[T::R]) -> u64 {
        let mut res = 0u64;
        let mut mask = 1;
        for r_val in rhs {
            let flag = self.0.apply_bool(lhs, *r_val);
            res |= if flag { mask } else { 0 };
            mask <<= 1;
        }
        res
    }

    /// Apply comparison between chunk and chunk, returns packed u64.
    #[inline]
    fn apply_chunk(&self, lhs: &[T::L], rhs: &[T::R]) -> u64 {
        let mut res = 0u64;
        let mut mask = 1;
        for (l_val, r_val) in lhs.iter().zip(rhs) {
            let flag = self.0.apply_bool(*l_val, *r_val);
            res |= if flag { mask } else { 0 };
            mask <<= 1;
        }
        res
    }

    /// Apply calculation of single and single.
    #[inline]
    fn single_single(&self, l_val: T::L, r_val: T::R, validity: Sel) -> Result<Attr> {
        let flag = self.0.apply_bool(l_val, r_val);
        Ok(Attr::new_single(
            PreciseType::bool(),
            Single::new_bool(flag, validity.n_records() as u16),
            validity,
        ))
    }

    /// Apply calculation of array and single.
    #[inline]
    fn array_single(
        &self,
        l_vals: &[T::L],
        r_val: T::R,
        validity: Sel,
        sel: Option<&Sel>,
    ) -> Result<Attr> {
        if let Some(sel) = sel {
            match sel {
                Sel::None { .. } => {
                    // no value should return, we can fake with single null
                    return Ok(Attr::new_null(PreciseType::bool(), l_vals.len() as u16));
                }
                Sel::Index { count, indexes, .. } => {
                    return handle_sel_index(
                        &indexes[..*count as usize],
                        &validity,
                        l_vals.len(),
                        |idx| self.0.apply_bool(l_vals[idx], r_val),
                    )
                }
                _ => (),
            }
        }
        let mut res = Bitmap::zeroes(l_vals.len());
        let (res_u64s, _) = res.u64s_mut();
        for (lhs, res) in l_vals.chunks(64).zip(res_u64s) {
            *res = self.apply_left_chunk(lhs, r_val);
        }
        Ok(Attr::new_bitmap(res, validity))
    }

    /// Apply calculation of single and array
    #[inline]
    fn single_array(
        &self,
        l_val: T::L,
        r_vals: &[T::R],
        validity: Sel,
        sel: Option<&Sel>,
    ) -> Result<Attr> {
        if let Some(sel) = sel {
            match sel {
                Sel::None { .. } => {
                    // no value should return, we can fake with single null
                    return Ok(Attr::new_null(PreciseType::bool(), r_vals.len() as u16));
                }
                Sel::Index { count, indexes, .. } => {
                    return handle_sel_index(
                        &indexes[..*count as usize],
                        &validity,
                        r_vals.len(),
                        |idx| self.0.apply_bool(l_val, r_vals[idx]),
                    )
                }
                // Both Sel::All and Sel::Bitmap will result in evaluation on all values.
                _ => (),
            }
        }
        let mut res = Bitmap::zeroes(r_vals.len());
        let (res_u64s, _) = res.u64s_mut();
        for (rhs, res) in r_vals.chunks(64).zip(res_u64s) {
            *res = self.apply_right_chunk(l_val, rhs);
        }
        Ok(Attr::new_bitmap(res, validity))
    }

    /// Apply calculation of array and array.
    #[inline]
    fn array_array(
        &self,
        l_vals: &[T::L],
        r_vals: &[T::R],
        validity: Sel,
        sel: Option<&Sel>,
    ) -> Result<Attr> {
        assert!(l_vals.len() == r_vals.len());
        if let Some(sel) = sel {
            match sel {
                Sel::None { .. } => {
                    // no value should return, we can fake with single null
                    return Ok(Attr::new_null(PreciseType::bool(), l_vals.len() as u16));
                }
                Sel::Index { count, indexes, .. } => {
                    return handle_sel_index(
                        &indexes[..*count as usize],
                        &validity,
                        l_vals.len(),
                        |idx| self.0.apply_bool(l_vals[idx], r_vals[idx]),
                    )
                }
                _ => (),
            }
        }
        let mut res = Bitmap::zeroes(r_vals.len());
        let (res_u64s, _) = res.u64s_mut();
        for ((lhs, rhs), res) in l_vals.chunks(64).zip(r_vals.chunks(64)).zip(res_u64s) {
            *res = self.apply_chunk(lhs, rhs);
        }
        Ok(Attr::new_bitmap(res, validity))
    }
}

#[inline]
fn handle_sel_index<F: Fn(usize) -> bool>(
    sel: &[u16],
    validity: &Sel,
    len: usize,
    f: F,
) -> Result<Attr> {
    let mut res = Bitmap::zeroes(len);
    let mut valids = [0u16; 6];
    let mut valid_count = 0;
    for idx in sel {
        let i = *idx as usize;
        if validity.selected(i)? {
            let flag = f(i);
            res.set(i, flag)?;
            valids[valid_count] = *idx;
            valid_count += 1;
        }
    }
    let res = if valid_count == 0 {
        Attr::new_null(PreciseType::bool(), len as u16)
    } else {
        let validity = Sel::Index {
            count: valid_count as u8,
            len: len as u16,
            indexes: valids,
        };
        Attr::new_bitmap(res, validity)
    };
    Ok(res)
}

impl<T: CmpEval> BinaryEval for Impl<T> {
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
        let validity = lhs.validity.intersect(&rhs.validity)?;
        if validity.is_none() {
            return Ok(Attr::new_null(res_ty, n_records as u16));
        }
        match (&lhs.codec, &rhs.codec) {
            (Codec::Single(l), Codec::Single(r)) => {
                self.single_single(l.view(), r.view(), validity)
            }
            (Codec::Array(l), Codec::Single(r)) => {
                self.array_single(l.cast_slice(), r.view(), validity, sel)
            }
            (Codec::Single(l), Codec::Array(r)) => {
                self.single_array(l.view(), r.cast_slice(), validity, sel)
            }
            (Codec::Array(l), Codec::Array(r)) => {
                self.array_array(l.cast_slice(), r.cast_slice(), validity, sel)
            }
            (Codec::Empty, _) | (_, Codec::Empty) => Ok(Attr::empty(res_ty)),
            // comparison does not support bitmap codec
            (Codec::Bitmap(_), _) | (_, Codec::Bitmap(_)) => Err(Error::UnsupportedEval),
        }
    }
}

macro_rules! impl_cmp_eval_for_iso_num {
    ($id:ident, $ty:ty, $op:tt) => {
        pub struct $id;
        impl CmpEval for $id {
            type L = $ty;
            type R = $ty;

            #[inline]
            fn apply_bool(&self, lhs: Self::L, rhs: Self::R) -> bool {
                lhs $op rhs
            }
        }
    }
}

impl_cmp_eval_for_iso_num!(EqualI32, i32, ==);
impl_cmp_eval_for_iso_num!(GreaterI32, i32, >);
impl_cmp_eval_for_iso_num!(GreaterEqualI32, i32, >=);
impl_cmp_eval_for_iso_num!(LessI32, i32, <);
impl_cmp_eval_for_iso_num!(LessEqualI32, i32, <=);
impl_cmp_eval_for_iso_num!(NotEqualI32, i32, !=);

impl_cmp_eval_for_iso_num!(EqualI64, i64, ==);
impl_cmp_eval_for_iso_num!(GreaterI64, i64, >);
impl_cmp_eval_for_iso_num!(GreaterEqualI64, i64, >=);
impl_cmp_eval_for_iso_num!(LessI64, i64, <);
impl_cmp_eval_for_iso_num!(LessEqualI64, i64, <=);
impl_cmp_eval_for_iso_num!(NotEqualI64, i64, !=);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmp_eval_eq() {
        let size = 10i32;
        let c1 = Attr::from((0..size).into_iter());
        let c2 = Attr::from((0..size).into_iter());
        let c3 = Attr::new_single(
            PreciseType::i32(),
            Single::new(0i32, size as u16),
            Sel::All(size as u16),
        );
        let c4 = Attr::new_single(
            PreciseType::i32(),
            Single::new(0i32, size as u16),
            Sel::All(size as u16),
        );
        let c5 = Attr::new_null(PreciseType::i32(), size as u16);
        let eq = CmpKind::Equal;
        // array vs array
        let res = eq.eval(&c1, &c2, None).unwrap();
        let res = res.codec.as_bitmap().unwrap();
        assert!(res.bools().all(|b| b));
        // single vs single
        let res = eq.eval(&c3, &c4, None).unwrap();
        assert!(res.validity.is_all());
        let res = res.codec.as_single().unwrap();
        let value = res.view_bool();
        assert!(value);
        // array vs single
        let res = eq.eval(&c1, &c3, None).unwrap();
        let res = res.codec.as_bitmap().unwrap();
        assert!(res.get(0).unwrap());
        assert!(res.bools().skip(1).all(|b| !b));
        // single vs array
        let res = eq.eval(&c4, &c2, None).unwrap();
        let res = res.codec.as_bitmap().unwrap();
        assert!(res.get(0).unwrap());
        assert!(res.bools().skip(1).all(|b| !b));
        // array vs null
        let res = eq.eval(&c1, &c5, None).unwrap();
        assert!(res.validity.is_none());
        // null vs array
        let res = eq.eval(&c5, &c1, None).unwrap();
        assert!(res.validity.is_none());
        // single vs null
        let res = eq.eval(&c3, &c5, None).unwrap();
        assert!(res.validity.is_none());
    }

    #[test]
    fn test_cmp_eval_gt() {
        let c1 = Attr::from((0..64i32).into_iter());
        let c2 = Attr::from((0..64i32).into_iter().rev());
        let c3 = Attr::new_single(PreciseType::i32(), Single::new(5i32, 64), Sel::All(64));
        let c4 = Attr::new_single(PreciseType::i32(), Single::new(15i32, 64), Sel::All(64));
        let gt = CmpKind::Greater;
        // array vs array
        let res = gt
            .eval(&c1, &c2, Some(&Sel::new_indexes(64, vec![5, 50])))
            .unwrap();
        let (valid, _) = res.bool_at(0).unwrap();
        assert!(!valid);
        let (valid, value) = res.bool_at(5).unwrap();
        assert!(valid && !value);
        let (valid, value) = res.bool_at(50).unwrap();
        assert!(valid && value);
        let res = gt.eval(&c1, &c2, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        // array vs single
        let res = gt
            .eval(&c1, &c3, Some(&Sel::new_indexes(64, vec![1, 50])))
            .unwrap();
        let (valid, value) = res.bool_at(1).unwrap();
        assert!(valid && !value);
        let (valid, value) = res.bool_at(50).unwrap();
        assert!(valid && value);
        let res = gt.eval(&c1, &c3, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
        // single vs array
        let res = gt
            .eval(&c4, &c1, Some(&Sel::new_indexes(64, vec![1, 50])))
            .unwrap();
        let (valid, value) = res.bool_at(1).unwrap();
        assert!(valid && value);
        let (valid, value) = res.bool_at(50).unwrap();
        assert!(valid && !value);
        let res = gt.eval(&c4, &c1, Some(&Sel::None(64))).unwrap();
        assert_eq!(PreciseType::bool(), res.ty);
    }
}
