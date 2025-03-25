use crate::error::{Error, Result};
use crate::BinaryEval;
use doradb_datatype::PreciseType;
use doradb_storage::col::array::Array;
use doradb_storage::col::attr::Attr;
use doradb_storage::col::codec::{Codec, Single};
use doradb_storage::col::repr::ByteRepr;
use doradb_storage::col::sel::Sel;

/// Kinds of arithmetic expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithKind {
    Add,
    Sub,
    Mul,
    Div,
}

impl ArithKind {
    #[inline]
    pub fn eval(&self, lhs: &Attr, rhs: &Attr, sel: Option<&Sel>) -> Result<Attr> {
        match (self, lhs.ty, rhs.ty) {
            (ArithKind::Add, PreciseType::Int(4, l_unsigned), PreciseType::Int(4, r_unsigned)) => {
                Impl(AddI32).binary_eval(
                    PreciseType::Int(4, l_unsigned | r_unsigned),
                    lhs,
                    rhs,
                    sel,
                )
            }
            (ArithKind::Add, PreciseType::Int(8, l_unsigned), PreciseType::Int(8, r_unsigned)) => {
                Impl(AddI64).binary_eval(
                    PreciseType::Int(8, l_unsigned | r_unsigned),
                    lhs,
                    rhs,
                    sel,
                )
            }
            _ => Err(Error::UnsupportedEval),
        }
    }
}

/// Template trait for arithmetic expression evaluation
pub trait ArithEval {
    type L: ByteRepr + Copy;
    type R: ByteRepr + Copy;
    type O: ByteRepr + Copy;

    fn apply(&self, lhs: Self::L, rhs: Self::R) -> Self::O;
}

pub struct Impl<T>(pub T);

impl<T: ArithEval> Impl<T> {
    /// Apply calculation of single and single.
    #[inline]
    fn single_single(
        &self,
        res_ty: PreciseType,
        l_val: T::L,
        r_val: T::R,
        validity: Sel,
    ) -> Result<Attr> {
        let res = self.0.apply(l_val, r_val);
        Ok(Attr::new_single(
            res_ty,
            Single::new(res, validity.n_records() as u16),
            validity,
        ))
    }

    /// Apply calculation of array and single.
    #[inline]
    fn array_single(
        &self,
        res_ty: PreciseType,
        l_vals: &[T::L],
        r_val: T::R,
        validity: Sel,
        sel: Option<&Sel>,
    ) -> Result<Attr> {
        if let Some(sel) = sel {
            match sel {
                Sel::None { .. } => {
                    // no value should return, we can fake with single null
                    return Ok(Attr::new_null(res_ty, l_vals.len() as u16));
                }
                Sel::Index { count, indexes, .. } => {
                    return handle_sel_index::<T::O, _>(
                        &indexes[..*count as usize],
                        res_ty,
                        &validity,
                        l_vals.len(),
                        |idx| self.0.apply(l_vals[idx], r_val),
                    )
                }
                // Both Sel::All and Sel::Bitmap will result in evaluation on all values.
                _ => (),
            }
        }
        let mut arr = Array::new_owned::<T::O>(l_vals.len());
        let res_vals = arr.cast_slice_mut(l_vals.len()).unwrap();
        for (lhs, res) in l_vals.iter().zip(res_vals) {
            *res = self.0.apply(*lhs, r_val);
        }
        unsafe { arr.set_len(l_vals.len()) };
        Ok(Attr::new_array(res_ty, arr, validity, None))
    }

    /// Apply calculation of single and array.
    #[inline]
    fn single_array(
        &self,
        res_ty: PreciseType,
        l_val: T::L,
        r_vals: &[T::R],
        validity: Sel,
        sel: Option<&Sel>,
    ) -> Result<Attr> {
        if let Some(sel) = sel {
            match sel {
                Sel::None { .. } => {
                    // no value should return, we can fake with single null
                    return Ok(Attr::new_null(res_ty, r_vals.len() as u16));
                }
                Sel::Index { count, indexes, .. } => {
                    return handle_sel_index::<T::O, _>(
                        &indexes[..*count as usize],
                        res_ty,
                        &validity,
                        r_vals.len(),
                        |idx| self.0.apply(l_val, r_vals[idx]),
                    )
                }
                // Both Sel::All and Sel::Bitmap will result in evaluation on all values.
                _ => (),
            }
        }
        let mut arr = Array::new_owned::<T::O>(r_vals.len());
        let res_vals = arr.cast_slice_mut(r_vals.len()).unwrap();
        for (rhs, res) in r_vals.iter().zip(res_vals) {
            *res = self.0.apply(l_val, *rhs);
        }
        unsafe { arr.set_len(r_vals.len()) };
        Ok(Attr::new_array(res_ty, arr, validity, None))
    }

    /// Apply calculation of array and array.
    #[inline]
    fn array_array(
        &self,
        res_ty: PreciseType,
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
                    return Ok(Attr::new_null(res_ty, l_vals.len() as u16));
                }
                Sel::Index { count, indexes, .. } => {
                    return handle_sel_index::<T::O, _>(
                        &indexes[..*count as usize],
                        res_ty,
                        &validity,
                        l_vals.len(),
                        |idx| self.0.apply(l_vals[idx], r_vals[idx]),
                    )
                }
                // Both Sel::All and Sel::Bitmap will result in evaluation on all values.
                _ => (),
            }
        }
        let mut arr = Array::new_owned::<T::O>(l_vals.len());
        let res_vals = arr.cast_slice_mut(l_vals.len()).unwrap();
        for ((lhs, rhs), res) in l_vals.iter().zip(r_vals).zip(res_vals) {
            *res = self.0.apply(*lhs, *rhs);
        }
        Ok(Attr::new_array(res_ty, arr, validity, None))
    }
}

impl<T: ArithEval> BinaryEval for Impl<T> {
    #[inline]
    fn binary_eval(
        &self,
        res_ty: PreciseType,
        lhs: &Attr,
        rhs: &Attr,
        sel: Option<&Sel>,
    ) -> Result<Attr> {
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
                self.single_single(res_ty, l.view(), r.view(), validity)
            }
            (Codec::Array(l), Codec::Single(r)) => {
                self.array_single(res_ty, l.cast_slice(), r.view(), validity, sel)
            }
            (Codec::Single(l), Codec::Array(r)) => {
                self.single_array(res_ty, l.view(), r.cast_slice(), validity, sel)
            }
            (Codec::Array(l), Codec::Array(r)) => {
                self.array_array(res_ty, l.cast_slice(), r.cast_slice(), validity, sel)
            }
            (Codec::Empty, _) | (_, Codec::Empty) => Ok(Attr::empty(res_ty)),
            // Arithmetic expression does not support bitmap codec.
            (Codec::Bitmap(_), _) | (_, Codec::Bitmap(_)) => Err(Error::UnsupportedEval),
        }
    }
}

#[inline]
fn handle_sel_index<T: ByteRepr, F: Fn(usize) -> T>(
    sel: &[u16],
    res_ty: PreciseType,
    validity: &Sel,
    len: usize,
    f: F,
) -> Result<Attr> {
    let mut arr = Array::new_owned::<T>(len);
    let res_vals = arr.cast_slice_mut::<T>(len).unwrap();
    let mut valids = [0u16; 6];
    let mut valid_count = 0;
    for idx in sel {
        let idx = *idx as usize;
        if validity.selected(idx)? {
            res_vals[idx] = f(idx);
            valids[valid_count] = idx as u16;
            valid_count += 1;
        }
    }
    unsafe { arr.set_len(len) };
    let res = if valid_count == 0 {
        Attr::new_null(res_ty, len as u16)
    } else {
        let validity = Sel::Index {
            count: valid_count as u8,
            len: len as u16,
            indexes: valids,
        };
        Attr::new_array(res_ty, arr, validity, None)
    };
    Ok(res)
}

macro_rules! impl_arith_eval_for_num {
    ($id:ident, $l:ty, $r:ty, $o:ty, $op:tt) => {
        pub struct $id;
        impl ArithEval for $id {
            type L = $l;
            type R = $r;
            type O = $o;
            #[inline]
            fn apply(&self, lhs: Self::L, rhs: Self::R) -> Self::O {
                lhs $op rhs
            }
        }
    }
}

impl_arith_eval_for_num!(AddI32, i32, i32, i32, +);
impl_arith_eval_for_num!(AddI64, i64, i64, i64, +);
// impl_arith_eval!(SubI32, i32, i32, i32, -);
// impl_arith_eval!(SubI64, i64, i64, i64, -);

#[cfg(test)]
mod tests {
    use super::*;
    use doradb_storage::col::codec::Single;

    #[test]
    fn test_vec_eval_4096() {
        const SIZE: i32 = 4096;
        let c1 = Attr::from((0..SIZE as i32).into_iter());
        let c2 = Attr::from((0..SIZE as i32).into_iter());
        let add = Impl(AddI32);
        let res = add.binary_eval(PreciseType::i32(), &c1, &c2, None).unwrap();
        assert_eq!(4096, res.n_records());
    }

    #[test]
    fn test_arith_eval_add_i32() {
        let size = 10i32;
        let c1 = Attr::from((0..size).into_iter());
        let c2 = Attr::from((0..size).into_iter());
        let c3 = Attr::new_single(
            PreciseType::i32(),
            Single::new(1i32, size as u16),
            Sel::All(size as u16),
        );
        let c4 = Attr::new_single(
            PreciseType::i32(),
            Single::new(1i32, size as u16),
            Sel::All(size as u16),
        );
        let c5 = Attr::new_null(PreciseType::i32(), size as u16);
        let add = Impl(AddI32);
        // array vs array
        let res = add.binary_eval(PreciseType::i32(), &c1, &c2, None).unwrap();
        let array = res.codec.as_array().unwrap();
        let i32s = array.cast_slice::<i32>();
        let expected: Vec<_> = (0..size).map(|i| i + i).collect();
        assert_eq!(&expected, i32s);
        // single vs single
        let res = add.binary_eval(PreciseType::i32(), &c3, &c4, None).unwrap();
        assert!(res.validity.is_all());
        let single = res.codec.as_single().unwrap();
        let value = single.view::<i32>();
        assert!(value == 2);
        // array vs single
        let res = add.binary_eval(PreciseType::i32(), &c1, &c3, None).unwrap();
        let array = res.codec.as_array().unwrap();
        let i32s = array.cast_slice::<i32>();
        let expected: Vec<_> = (0..size).map(|i| i + 1).collect();
        assert_eq!(&expected, i32s);
        // single vs array
        let res = add.binary_eval(PreciseType::i32(), &c4, &c2, None).unwrap();
        let array = res.codec.as_array().unwrap();
        let i32s = array.cast_slice::<i32>();
        let expected: Vec<_> = (0..size).map(|i| i + 1).collect();
        assert_eq!(&expected, i32s);
        // array vs null
        let res = add.binary_eval(PreciseType::i32(), &c1, &c5, None).unwrap();
        assert!(res.validity.is_none());
        // null vs array
        let res = add.binary_eval(PreciseType::i32(), &c5, &c1, None).unwrap();
        assert!(res.validity.is_none());
        // single vs null
        let res = add.binary_eval(PreciseType::i32(), &c3, &c5, None).unwrap();
        assert!(res.validity.is_none());
    }

    #[test]
    fn test_arith_eval_sel() {
        let validity = Sel::new_indexes(1024, vec![0, 1, 2, 3, 4]);
        let c1 = Attr::new_single(
            PreciseType::i64(),
            Single::new(1i64, 1024),
            validity.clone_to_owned(),
        );
        let c2 = Attr::from((0..1024i32).map(|i| i as i64));
        let array = Array::from((0..1024i32).map(|i| i as i64));
        let c3 = Attr::new_array(PreciseType::i64(), array, validity, None);
        let sel = Sel::new_indexes(1024, vec![0, 1, 2, 3, 4, 5]);
        let add = Impl(AddI64);
        // single + single
        let res = add
            .binary_eval(PreciseType::i64(), &c1, &c1, Some(&sel))
            .unwrap();
        assert!(!res.is_valid(5).unwrap());
        let res = res.codec.as_single().unwrap();
        assert_eq!(2i64, res.view::<i64>());
        // single + array
        let res = add
            .binary_eval(PreciseType::i64(), &c1, &c2, Some(&sel))
            .unwrap();
        assert!(!res.is_valid(5).unwrap());
        let res = res.codec.as_array().unwrap();
        assert_eq!(&[1i64, 2, 3, 4, 5], &res.cast_slice::<i64>()[..5]);
        // array + single
        let res = add
            .binary_eval(PreciseType::i64(), &c2, &c1, Some(&sel))
            .unwrap();
        assert!(!res.is_valid(5).unwrap());
        let res = res.codec.as_array().unwrap();
        assert_eq!(&[1i64, 2, 3, 4, 5], &res.cast_slice::<i64>()[..5]);
        // array + array
        let res = add
            .binary_eval(PreciseType::i64(), &c2, &c3, Some(&sel))
            .unwrap();
        assert!(!res.is_valid(5).unwrap());
        let res = res.codec.as_array().unwrap();
        assert_eq!(&[0i64, 2, 4, 6, 8], &res.cast_slice::<i64>()[..5]);
    }
}
