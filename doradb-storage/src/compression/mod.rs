//! Compression algorithms.
//!
//! This module includes compression algorithms used in storage.
//! Currently lightweight columnar compression is supposed to be
//! enough, e.g. FOR+bitpacking, dict, FSST.

pub mod bitpacking;
pub mod lwc_page;

pub use bitpacking::*;

use crate::error::{Error, Result};
use crate::serde::{Deser, ForBitpackingDeser, ForBitpackingSer, Ser, SerdeCtx};
use crate::value::{Val, ValKind};
use std::mem;

/// Lightweight compressed data.
pub enum LwcData<'a> {
    Primitive(LwcPrimitive<'a>),
    // todo: bytes
}

impl<'a> LwcData<'a> {
    /// Parse input and create a reader to read compressed data.
    /// The input format should be as below:
    /// First byte must be compression code which indicates the
    /// compression type of the data.
    #[inline]
    pub fn from_bytes(kind: ValKind, input: &'a [u8]) -> Result<Self> {
        let c = LwcCode::try_from(input[0])?;
        let input = &input[1..];
        let res = match c {
            LwcCode::Flat => {
                let (len, input) = read_le_u64(input)?;
                match kind {
                    ValKind::I8 => {
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatI8(FlatI8(input)))
                    }
                    ValKind::U8 => {
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatU8(FlatU8(input)))
                    }
                    ValKind::U64 => {
                        let input = bytemuck::cast_slice::<u8, [u8; 8]>(input);
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatU64(FlatU64(input)))
                    }
                    ValKind::VarByte => unreachable!("unexpected value type"),
                    _ => todo!(),
                }
            }
            LwcCode::ForBitpacking => {
                // see `ForBitpackingDeser` for reference.
                let (n_bits, input) = read_u8(input)?;
                let (len, input) = read_le_u64(input)?;
                let len = len as usize;
                let n_bits = n_bits as usize;
                let p = match kind {
                    ValKind::U64 => {
                        let (min, input) = read_le_u64(input)?;
                        debug_assert!(input.len() == (len * n_bits).div_ceil(8));
                        match n_bits {
                            1 => LwcPrimitive::ForBp1U64(ForBitpacking1 {
                                len,
                                min,
                                data: input,
                            }),
                            2 => LwcPrimitive::ForBp2U64(ForBitpacking2 {
                                len,
                                min,
                                data: input,
                            }),
                            4 => LwcPrimitive::ForBp4U64(ForBitpacking4 {
                                len,
                                min,
                                data: input,
                            }),
                            8 => LwcPrimitive::ForBp8U64(ForBitpacking8 { min, data: input }),
                            16 => {
                                let data = bytemuck::cast_slice::<u8, [u8; 2]>(input);
                                LwcPrimitive::ForBp16U64(ForBitpacking16 { min, data })
                            }
                            32 => {
                                let data = bytemuck::cast_slice::<u8, [u8; 4]>(input);
                                LwcPrimitive::ForBp32U64(ForBitpacking32 { min, data })
                            }
                            _ => {
                                // any other number of bits are not supported.
                                return Err(Error::NotSupported("unexpected packing bits"));
                            }
                        }
                    }
                    _ => todo!(),
                };
                LwcData::Primitive(p)
            }
        };
        Ok(res)
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            LwcData::Primitive(p) => match p {
                LwcPrimitive::FlatI8(f) => f.len(),
                LwcPrimitive::FlatU8(f) => f.len(),
                LwcPrimitive::FlatU64(f) => f.len(),
                LwcPrimitive::ForBp1U64(f) => f.len(),
                LwcPrimitive::ForBp2U64(f) => f.len(),
                LwcPrimitive::ForBp4U64(f) => f.len(),
                LwcPrimitive::ForBp8U64(f) => f.len(),
                LwcPrimitive::ForBp16U64(f) => f.len(),
                LwcPrimitive::ForBp32U64(f) => f.len(),
            },
        }
    }

    #[inline]
    pub fn value(&self, col_idx: usize) -> Option<Val> {
        match self {
            LwcData::Primitive(p) => p.value(col_idx),
        }
    }
}

/// Abstract of primitive data which are lightweight compressed.
/// Provides methods for random access and batch access.
pub trait LwcPrimitiveData {
    type Value;
    type Iter: Iterator<Item = Self::Value>;

    /// Returns total number of values.
    fn len(&self) -> usize;

    /// Returns value at given position.
    /// None if index is out of range.
    fn value(&self, idx: usize) -> Option<Self::Value>;

    /// Extend all values to target collection.
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E);

    /// Returns the iterator over all values.
    fn iter(&self) -> Self::Iter;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LwcCode {
    Flat = 1,
    // Bitpacking = 2,
    /// See `ForBitpackingSer` for serialization details.
    ForBitpacking = 3,
    // todo: dict, fsst.
}

impl TryFrom<u8> for LwcCode {
    type Error = Error;
    #[inline]
    fn try_from(value: u8) -> Result<Self> {
        let res = match value {
            1 => LwcCode::Flat,
            3 => LwcCode::ForBitpacking,
            _ => return Err(Error::InvalidCompressedData),
        };
        Ok(res)
    }
}

pub enum LwcPrimitive<'a> {
    FlatI8(FlatI8<'a>),
    FlatU8(FlatU8<'a>),
    FlatU64(FlatU64<'a>),
    ForBp1U64(ForBitpacking1<'a, u64>),
    ForBp2U64(ForBitpacking2<'a, u64>),
    ForBp4U64(ForBitpacking4<'a, u64>),
    ForBp8U64(ForBitpacking8<'a, u64>),
    ForBp16U64(ForBitpacking16<'a, u64>),
    ForBp32U64(ForBitpacking32<'a, u64>),
}

impl LwcPrimitive<'_> {
    #[inline]
    pub fn value(&self, col_idx: usize) -> Option<Val> {
        match self {
            LwcPrimitive::FlatI8(f) => f.value(col_idx).map(|v| Val::Byte1(v as u8)),
            LwcPrimitive::FlatU8(f) => f.value(col_idx).map(|v| Val::Byte1(v)),
            LwcPrimitive::FlatU64(f) => f.value(col_idx).map(Val::Byte8),
            LwcPrimitive::ForBp1U64(f) => f.value(col_idx).map(Val::Byte8),
            LwcPrimitive::ForBp2U64(f) => f.value(col_idx).map(Val::Byte8),
            LwcPrimitive::ForBp4U64(f) => f.value(col_idx).map(Val::Byte8),
            LwcPrimitive::ForBp8U64(f) => f.value(col_idx).map(Val::Byte8),
            LwcPrimitive::ForBp16U64(f) => f.value(col_idx).map(Val::Byte8),
            LwcPrimitive::ForBp32U64(f) => f.value(col_idx).map(Val::Byte8),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LwcPrimitiveSer<'a> {
    FlatI8(&'a [i8]),
    FlatU8(&'a [u8]),
    FlatU64(&'a [u64]),
    ForBpU64(ForBitpackingSer<'a, u64>),
}

impl<'a> LwcPrimitiveSer<'a> {
    // todo: support fbp.
    #[inline]
    pub fn new_i8(input: &'a [i8]) -> Self {
        LwcPrimitiveSer::FlatI8(input)
    }

    // todo: support fbp.
    #[inline]
    pub fn new_u8(input: &'a [u8]) -> Self {
        LwcPrimitiveSer::FlatU8(input)
    }

    #[inline]
    pub fn new_u64(input: &'a [u64]) -> Self {
        if input.is_empty() {
            return LwcPrimitiveSer::FlatU64(input);
        }
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpU64(fbp),
            None => LwcPrimitiveSer::FlatU64(input),
        }
    }

    #[inline]
    pub fn code(&self) -> LwcCode {
        match self {
            LwcPrimitiveSer::FlatI8(_)
            | LwcPrimitiveSer::FlatU8(_)
            | LwcPrimitiveSer::FlatU64(_) => LwcCode::Flat,
            LwcPrimitiveSer::ForBpU64(_) => LwcCode::ForBitpacking,
        }
    }
}

impl<'a> Ser<'a> for LwcPrimitiveSer<'a> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        mem::size_of::<u8>()
            + match self {
                LwcPrimitiveSer::FlatI8(f) => f.ser_len(ctx),
                LwcPrimitiveSer::FlatU8(f) => f.ser_len(ctx),
                LwcPrimitiveSer::FlatU64(f) => f.ser_len(ctx),
                LwcPrimitiveSer::ForBpU64(b) => b.ser_len(ctx),
            }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = ctx.ser_u8(out, start_idx, self.code() as u8);
        match self {
            LwcPrimitiveSer::FlatI8(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatU8(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatU64(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::ForBpU64(b) => b.ser(ctx, out, idx),
        }
    }
}

pub struct LwcPrimitiveDeser<T>(pub Vec<T>);

impl<T: Deser + BitPackable> Deser for LwcPrimitiveDeser<T> {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, code) = ctx.deser_u8(input, start_idx)?;
        match LwcCode::try_from(code)? {
            LwcCode::Flat => {
                let (idx, data) = Vec::<T>::deser(ctx, input, idx)?;
                Ok((idx, LwcPrimitiveDeser(data)))
            }
            LwcCode::ForBitpacking => {
                let (idx, data) = ForBitpackingDeser::<T>::deser(ctx, input, idx)?;
                Ok((idx, LwcPrimitiveDeser(data.0)))
            }
        }
    }
}

pub struct FlatI8<'a>(&'a [u8]);

impl<'a> LwcPrimitiveData for FlatI8<'a> {
    type Value = i8;
    type Iter = FlatI8Iter<'a>;

    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn value(&self, idx: usize) -> Option<Self::Value> {
        if idx < self.0.len() {
            let v = self.0[idx] as i8;
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E) {
        target.extend(self.iter())
    }

    #[inline]
    fn iter(&self) -> FlatI8Iter<'a> {
        FlatI8Iter(self.0.iter())
    }
}

pub struct FlatI8Iter<'a>(std::slice::Iter<'a, u8>);

impl<'a> Iterator for FlatI8Iter<'a> {
    type Item = i8;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|v| *v as i8)
    }
}

pub struct FlatU8<'a>(&'a [u8]);

impl<'a> LwcPrimitiveData for FlatU8<'a> {
    type Value = u8;
    type Iter = FlatU8Iter<'a>;

    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn value(&self, idx: usize) -> Option<Self::Value> {
        if idx < self.0.len() {
            let v = self.0[idx];
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E) {
        target.extend(self.iter())
    }

    #[inline]
    fn iter(&self) -> FlatU8Iter<'a> {
        FlatU8Iter(self.0.iter())
    }
}

pub struct FlatU8Iter<'a>(std::slice::Iter<'a, u8>);

impl<'a> Iterator for FlatU8Iter<'a> {
    type Item = u8;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().cloned()
    }
}

macro_rules! impl_lwc_flat {
    ($t:ident, $v:ty, $unit:ty, $it:ident) => {
        pub struct $t<'a>(&'a [$unit]);

        impl<'a> LwcPrimitiveData for $t<'a> {
            type Value = $v;
            type Iter = $it<'a>;

            #[inline]
            fn len(&self) -> usize {
                self.0.len()
            }

            #[inline]
            fn value(&self, idx: usize) -> Option<Self::Value> {
                if idx < self.0.len() {
                    let u = self.0[idx];
                    let v = <$v>::from_le_bytes(u);
                    Some(v)
                } else {
                    None
                }
            }

            #[inline]
            fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E) {
                target.extend(self.iter())
            }

            #[inline]
            fn iter(&self) -> Self::Iter {
                $it(self.0.iter())
            }
        }

        pub struct $it<'a>(std::slice::Iter<'a, $unit>);

        impl<'a> Iterator for $it<'a> {
            type Item = $v;
            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                self.0.next().map(|v| <$v>::from_le_bytes(*v))
            }
        }
    };
}

impl_lwc_flat!(FlatI16, i16, [u8; 2], FlatI16Iter);
impl_lwc_flat!(FlatU16, u16, [u8; 2], FlatU16Iter);
impl_lwc_flat!(FlatI32, i32, [u8; 4], FlatI32Iter);
impl_lwc_flat!(FlatU32, u32, [u8; 4], FlatU32Iter);
impl_lwc_flat!(FlatI64, i64, [u8; 8], FlatI64Iter);
impl_lwc_flat!(FlatU64, u64, [u8; 8], FlatU64Iter);
impl_lwc_flat!(FlatF32, f32, [u8; 4], FlatF32Iter);
impl_lwc_flat!(FlatF64, f64, [u8; 8], FlatF64Iter);

pub trait SortedPosition {
    type Value;
    /// Search the given value and return its position if found.
    /// The packed array should be sorted, otherwise the result
    /// is meaningless.
    /// Note single we don't store maximum value, there might be
    /// overflow and lead to wrong result. So if we use this
    /// function, we must make sure the given value is within
    /// range.
    fn sorted_position(&self, value: Self::Value) -> Option<usize>;
}

macro_rules! impl_sorted_position {
    ($t:ident, $v:ty) => {
        impl $t<'_> {
            #[inline]
            pub fn sorted_position(&self, val: $v) -> Option<usize> {
                self.0
                    .binary_search_by_key(&val, |&u| <$v>::from_le_bytes(u))
                    .ok()
            }
        }
    };
}

impl_sorted_position!(FlatI16, i16);
impl_sorted_position!(FlatU16, u16);
impl_sorted_position!(FlatI32, i32);
impl_sorted_position!(FlatU32, u32);
impl_sorted_position!(FlatI64, i64);
impl_sorted_position!(FlatU64, u64);

#[inline]
fn read_le_u64(input: &[u8]) -> Result<(u64, &[u8])> {
    if input.len() < 8 {
        return Err(Error::InvalidCompressedData);
    }
    let u: [u8; 8] = input[..8].try_into().unwrap();
    let v = u64::from_le_bytes(u);
    Ok((v, &input[8..]))
}

#[inline]
fn read_u8(input: &[u8]) -> Result<(u8, &[u8])> {
    if input.len() < 1 {
        return Err(Error::InvalidCompressedData);
    }
    let v = input[0];
    Ok((v, &input[1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lwc_primitive_serde() {
        let ctx = SerdeCtx::default();
        for input in vec![
            vec![],
            vec![1u64],
            vec![1, 1 << 1],
            vec![1, 1 << 1, 1 << 2],
            vec![1, 1 << 1, 1 << 2, 1 << 4],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16, 1 << 32],
        ] {
            let lwc_ser = LwcPrimitiveSer::new_u64(&input);
            let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
            let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
            assert_eq!(ser_idx, res.len());
            let lwc_data = LwcData::from_bytes(ValKind::U64, &res).unwrap();
            let mut output = vec![];
            for i in 0..lwc_data.len() {
                output.push(lwc_data.value(i).unwrap().as_u64().unwrap());
            }
            assert_eq!(input, output);
        }
    }

    #[test]
    fn test_lwc_primitive_flat() {
        let ctx = SerdeCtx::default();

        // i8
        let input = vec![1i8, 2, 4, 16, -100, 100];
        let lwc_ser = LwcPrimitiveSer::new_i8(&input);
        let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
        let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
        assert_eq!(ser_idx, res.len());
        let lwc_data = LwcData::from_bytes(ValKind::I8, &res).unwrap();
        let mut output = vec![];
        for i in 0..lwc_data.len() {
            output.push(lwc_data.value(i).unwrap().as_i8().unwrap());
        }
        assert_eq!(input, output);

        // u8
        let input = vec![1u8, 2, 4, 16, 100, 200, 250];
        let lwc_ser = LwcPrimitiveSer::new_u8(&input);
        let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
        let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
        assert_eq!(ser_idx, res.len());
        let lwc_data = LwcData::from_bytes(ValKind::U8, &res).unwrap();
        let mut output = vec![];
        for i in 0..lwc_data.len() {
            output.push(lwc_data.value(i).unwrap().as_u8().unwrap());
        }
        assert_eq!(input, output);

        // u64
        let input = vec![1u64, 2, 4, 16, 100, 200, 250, 100000000, 99999999999];
        let lwc_ser = LwcPrimitiveSer::new_u64(&input);
        let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
        let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
        assert_eq!(ser_idx, res.len());
        let lwc_data = LwcData::from_bytes(ValKind::U64, &res).unwrap();
        let mut output = vec![];
        for i in 0..lwc_data.len() {
            output.push(lwc_data.value(i).unwrap().as_u64().unwrap());
        }
        assert_eq!(input, output);
    }
}
