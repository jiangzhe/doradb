//! This module contains definition and functions of LWC(Lightweight Compression) Block.

pub mod page;

pub use page::*;

use crate::compression::*;
use crate::error::{Error, Result};
use crate::serde::{Deser, ForBitpackingDeser, ForBitpackingSer, Ser, SerdeCtx};
use crate::value::{MemVar, Val, ValKind};
use std::mem;

/// Lightweight compressed data.
pub enum LwcData<'a> {
    Primitive(LwcPrimitive<'a>),
    Bytes(LwcBytes<'a>),
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
                    ValKind::I16 => {
                        let input = bytemuck::cast_slice::<u8, [u8; 2]>(input);
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatI16(FlatI16(input)))
                    }
                    ValKind::U16 => {
                        let input = bytemuck::cast_slice::<u8, [u8; 2]>(input);
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatU16(FlatU16(input)))
                    }
                    ValKind::I32 => {
                        let input = bytemuck::cast_slice::<u8, [u8; 4]>(input);
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatI32(FlatI32(input)))
                    }
                    ValKind::U32 => {
                        let input = bytemuck::cast_slice::<u8, [u8; 4]>(input);
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatU32(FlatU32(input)))
                    }
                    ValKind::F32 => {
                        let input = bytemuck::cast_slice::<u8, [u8; 4]>(input);
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatF32(FlatF32(input)))
                    }
                    ValKind::I64 => {
                        let input = bytemuck::cast_slice::<u8, [u8; 8]>(input);
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatI64(FlatI64(input)))
                    }
                    ValKind::U64 => {
                        let input = bytemuck::cast_slice::<u8, [u8; 8]>(input);
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatU64(FlatU64(input)))
                    }
                    ValKind::F64 => {
                        let input = bytemuck::cast_slice::<u8, [u8; 8]>(input);
                        debug_assert_eq!(input.len(), len as usize);
                        LwcData::Primitive(LwcPrimitive::FlatF64(FlatF64(input)))
                    }
                    ValKind::VarByte => {
                        let len = len as usize;
                        if input.len() < (len + 1) * mem::size_of::<u32>() {
                            return Err(Error::InvalidCompressedData);
                        }
                        let (offset_bytes, data) =
                            input.split_at((len + 1) * mem::size_of::<u32>());
                        let offsets = bytemuck::cast_slice::<u8, [u8; 4]>(offset_bytes);
                        LwcData::Bytes(LwcBytes { offsets, data })
                    }
                }
            }
            LwcCode::ForBitpacking => {
                // see `ForBitpackingDeser` for reference.
                let (n_bits, input) = read_u8(input)?;
                let (len, input) = read_le_u64(input)?;
                let len = len as usize;
                let n_bits = n_bits as usize;
                let p = match kind {
                    ValKind::I8 => {
                        let (min, data) = read_i8(input)?;
                        debug_assert!(data.len() == (len * n_bits).div_ceil(8));
                        match n_bits {
                            1 => LwcPrimitive::ForBp1I8(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2I8(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4I8(ForBitpacking4 { len, min, data }),
                            _ => return Err(Error::NotSupported("unexpected packing bits")),
                        }
                    }
                    ValKind::U8 => {
                        let (min, data) = read_u8(input)?;
                        debug_assert!(data.len() == (len * n_bits).div_ceil(8));
                        match n_bits {
                            1 => LwcPrimitive::ForBp1U8(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2U8(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4U8(ForBitpacking4 { len, min, data }),
                            _ => return Err(Error::NotSupported("unexpected packing bits")),
                        }
                    }
                    ValKind::I16 => {
                        let (min, data) = read_le_i16(input)?;
                        debug_assert!(data.len() == (len * n_bits).div_ceil(8));
                        match n_bits {
                            1 => LwcPrimitive::ForBp1I16(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2I16(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4I16(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8I16(ForBitpacking8 { min, data }),
                            _ => return Err(Error::NotSupported("unexpected packing bits")),
                        }
                    }
                    ValKind::U16 => {
                        let (min, data) = read_le_u16(input)?;
                        debug_assert!(data.len() == (len * n_bits).div_ceil(8));
                        match n_bits {
                            1 => LwcPrimitive::ForBp1U16(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2U16(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4U16(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8U16(ForBitpacking8 { min, data }),
                            _ => return Err(Error::NotSupported("unexpected packing bits")),
                        }
                    }
                    ValKind::I32 => {
                        let (min, data) = read_le_i32(input)?;
                        debug_assert!(data.len() == (len * n_bits).div_ceil(8));
                        match n_bits {
                            1 => LwcPrimitive::ForBp1I32(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2I32(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4I32(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8I32(ForBitpacking8 { min, data }),
                            16 => {
                                let data = bytemuck::cast_slice::<u8, [u8; 2]>(data);
                                LwcPrimitive::ForBp16I32(ForBitpacking16 { min, data })
                            }
                            _ => return Err(Error::NotSupported("unexpected packing bits")),
                        }
                    }
                    ValKind::U32 => {
                        let (min, data) = read_le_u32(input)?;
                        debug_assert!(data.len() == (len * n_bits).div_ceil(8));
                        match n_bits {
                            1 => LwcPrimitive::ForBp1U32(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2U32(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4U32(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8U32(ForBitpacking8 { min, data }),
                            16 => {
                                let data = bytemuck::cast_slice::<u8, [u8; 2]>(data);
                                LwcPrimitive::ForBp16U32(ForBitpacking16 { min, data })
                            }
                            _ => return Err(Error::NotSupported("unexpected packing bits")),
                        }
                    }
                    ValKind::I64 => {
                        let (min, data) = read_le_i64(input)?;
                        debug_assert!(data.len() == (len * n_bits).div_ceil(8));
                        match n_bits {
                            1 => LwcPrimitive::ForBp1I64(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2I64(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4I64(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8I64(ForBitpacking8 { min, data }),
                            16 => {
                                let data = bytemuck::cast_slice::<u8, [u8; 2]>(data);
                                LwcPrimitive::ForBp16I64(ForBitpacking16 { min, data })
                            }
                            32 => {
                                let data = bytemuck::cast_slice::<u8, [u8; 4]>(data);
                                LwcPrimitive::ForBp32I64(ForBitpacking32 { min, data })
                            }
                            _ => return Err(Error::NotSupported("unexpected packing bits")),
                        }
                    }
                    ValKind::U64 => {
                        let (min, data) = read_le_u64(input)?;
                        debug_assert!(data.len() == (len * n_bits).div_ceil(8));
                        match n_bits {
                            1 => LwcPrimitive::ForBp1U64(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2U64(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4U64(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8U64(ForBitpacking8 { min, data }),
                            16 => {
                                let data = bytemuck::cast_slice::<u8, [u8; 2]>(data);
                                LwcPrimitive::ForBp16U64(ForBitpacking16 { min, data })
                            }
                            32 => {
                                let data = bytemuck::cast_slice::<u8, [u8; 4]>(data);
                                LwcPrimitive::ForBp32U64(ForBitpacking32 { min, data })
                            }
                            _ => {
                                // any other number of bits are not supported.
                                return Err(Error::NotSupported("unexpected packing bits"));
                            }
                        }
                    }
                    ValKind::F32 | ValKind::F64 | ValKind::VarByte => {
                        return Err(Error::NotSupported("unexpected type"));
                    }
                };
                LwcData::Primitive(p)
            }
        };
        Ok(res)
    }

    /// Returns the length of Lwc Data.
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            LwcData::Primitive(p) => match p {
                LwcPrimitive::FlatI8(f) => f.len(),
                LwcPrimitive::FlatU8(f) => f.len(),
                LwcPrimitive::FlatI16(f) => f.len(),
                LwcPrimitive::FlatU16(f) => f.len(),
                LwcPrimitive::FlatI32(f) => f.len(),
                LwcPrimitive::FlatU32(f) => f.len(),
                LwcPrimitive::FlatF32(f) => f.len(),
                LwcPrimitive::FlatI64(f) => f.len(),
                LwcPrimitive::FlatU64(f) => f.len(),
                LwcPrimitive::FlatF64(f) => f.len(),
                LwcPrimitive::ForBp1I8(f) => f.len(),
                LwcPrimitive::ForBp2I8(f) => f.len(),
                LwcPrimitive::ForBp4I8(f) => f.len(),
                LwcPrimitive::ForBp1U8(f) => f.len(),
                LwcPrimitive::ForBp2U8(f) => f.len(),
                LwcPrimitive::ForBp4U8(f) => f.len(),
                LwcPrimitive::ForBp1I16(f) => f.len(),
                LwcPrimitive::ForBp2I16(f) => f.len(),
                LwcPrimitive::ForBp4I16(f) => f.len(),
                LwcPrimitive::ForBp8I16(f) => f.len(),
                LwcPrimitive::ForBp1U16(f) => f.len(),
                LwcPrimitive::ForBp2U16(f) => f.len(),
                LwcPrimitive::ForBp4U16(f) => f.len(),
                LwcPrimitive::ForBp8U16(f) => f.len(),
                LwcPrimitive::ForBp1I32(f) => f.len(),
                LwcPrimitive::ForBp2I32(f) => f.len(),
                LwcPrimitive::ForBp4I32(f) => f.len(),
                LwcPrimitive::ForBp8I32(f) => f.len(),
                LwcPrimitive::ForBp16I32(f) => f.len(),
                LwcPrimitive::ForBp1U32(f) => f.len(),
                LwcPrimitive::ForBp2U32(f) => f.len(),
                LwcPrimitive::ForBp4U32(f) => f.len(),
                LwcPrimitive::ForBp8U32(f) => f.len(),
                LwcPrimitive::ForBp16U32(f) => f.len(),
                LwcPrimitive::ForBp1I64(f) => f.len(),
                LwcPrimitive::ForBp2I64(f) => f.len(),
                LwcPrimitive::ForBp4I64(f) => f.len(),
                LwcPrimitive::ForBp8I64(f) => f.len(),
                LwcPrimitive::ForBp16I64(f) => f.len(),
                LwcPrimitive::ForBp32I64(f) => f.len(),
                LwcPrimitive::ForBp1U64(f) => f.len(),
                LwcPrimitive::ForBp2U64(f) => f.len(),
                LwcPrimitive::ForBp4U64(f) => f.len(),
                LwcPrimitive::ForBp8U64(f) => f.len(),
                LwcPrimitive::ForBp16U64(f) => f.len(),
                LwcPrimitive::ForBp32U64(f) => f.len(),
                LwcPrimitive::Bytes(b) => b.len(),
            },
            LwcData::Bytes(b) => b.len(),
        }
    }

    /// Returns value at given position.
    #[inline]
    pub fn value(&self, idx: usize) -> Option<Val> {
        match self {
            LwcData::Primitive(p) => p.value(idx),
            LwcData::Bytes(b) => b.value(idx).map(Val::VarByte),
        }
    }
}

/// Abstract of primitive data which are lightweight compressed.
/// Provides methods for random access and batch access.
pub trait LwcPrimitiveData {
    type Value;
    type Iter: Iterator<Item = Self::Value>;

    /// Returns total number of values.
    #[allow(clippy::len_without_is_empty)]
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
    FlatI16(FlatI16<'a>),
    FlatU16(FlatU16<'a>),
    FlatI32(FlatI32<'a>),
    FlatU32(FlatU32<'a>),
    FlatF32(FlatF32<'a>),
    FlatI64(FlatI64<'a>),
    FlatU64(FlatU64<'a>),
    FlatF64(FlatF64<'a>),
    ForBp1I8(ForBitpacking1<'a, i8>),
    ForBp2I8(ForBitpacking2<'a, i8>),
    ForBp4I8(ForBitpacking4<'a, i8>),
    ForBp1U8(ForBitpacking1<'a, u8>),
    ForBp2U8(ForBitpacking2<'a, u8>),
    ForBp4U8(ForBitpacking4<'a, u8>),
    ForBp1I16(ForBitpacking1<'a, i16>),
    ForBp2I16(ForBitpacking2<'a, i16>),
    ForBp4I16(ForBitpacking4<'a, i16>),
    ForBp8I16(ForBitpacking8<'a, i16>),
    ForBp1U16(ForBitpacking1<'a, u16>),
    ForBp2U16(ForBitpacking2<'a, u16>),
    ForBp4U16(ForBitpacking4<'a, u16>),
    ForBp8U16(ForBitpacking8<'a, u16>),
    ForBp1I32(ForBitpacking1<'a, i32>),
    ForBp2I32(ForBitpacking2<'a, i32>),
    ForBp4I32(ForBitpacking4<'a, i32>),
    ForBp8I32(ForBitpacking8<'a, i32>),
    ForBp16I32(ForBitpacking16<'a, i32>),
    ForBp1U32(ForBitpacking1<'a, u32>),
    ForBp2U32(ForBitpacking2<'a, u32>),
    ForBp4U32(ForBitpacking4<'a, u32>),
    ForBp8U32(ForBitpacking8<'a, u32>),
    ForBp16U32(ForBitpacking16<'a, u32>),
    ForBp1I64(ForBitpacking1<'a, i64>),
    ForBp2I64(ForBitpacking2<'a, i64>),
    ForBp4I64(ForBitpacking4<'a, i64>),
    ForBp8I64(ForBitpacking8<'a, i64>),
    ForBp16I64(ForBitpacking16<'a, i64>),
    ForBp32I64(ForBitpacking32<'a, i64>),
    ForBp1U64(ForBitpacking1<'a, u64>),
    ForBp2U64(ForBitpacking2<'a, u64>),
    ForBp4U64(ForBitpacking4<'a, u64>),
    ForBp8U64(ForBitpacking8<'a, u64>),
    ForBp16U64(ForBitpacking16<'a, u64>),
    ForBp32U64(ForBitpacking32<'a, u64>),
    Bytes(LwcBytes<'a>),
}

impl LwcPrimitive<'_> {
    #[inline]
    pub fn value(&self, idx: usize) -> Option<Val> {
        match self {
            LwcPrimitive::FlatI8(f) => f.value(idx).map(Val::I8),
            LwcPrimitive::FlatU8(f) => f.value(idx).map(Val::U8),
            LwcPrimitive::FlatI16(f) => f.value(idx).map(Val::I16),
            LwcPrimitive::FlatU16(f) => f.value(idx).map(Val::U16),
            LwcPrimitive::FlatI32(f) => f.value(idx).map(Val::I32),
            LwcPrimitive::FlatU32(f) => f.value(idx).map(Val::U32),
            LwcPrimitive::FlatF32(f) => f.value(idx).map(|v| Val::F32(v.into())),
            LwcPrimitive::FlatI64(f) => f.value(idx).map(Val::I64),
            LwcPrimitive::FlatU64(f) => f.value(idx).map(Val::U64),
            LwcPrimitive::FlatF64(f) => f.value(idx).map(|v| Val::F64(v.into())),
            LwcPrimitive::ForBp1I8(f) => f.value(idx).map(Val::I8),
            LwcPrimitive::ForBp2I8(f) => f.value(idx).map(Val::I8),
            LwcPrimitive::ForBp4I8(f) => f.value(idx).map(Val::I8),
            LwcPrimitive::ForBp1U8(f) => f.value(idx).map(Val::U8),
            LwcPrimitive::ForBp2U8(f) => f.value(idx).map(Val::U8),
            LwcPrimitive::ForBp4U8(f) => f.value(idx).map(Val::U8),
            LwcPrimitive::ForBp1I16(f) => f.value(idx).map(Val::I16),
            LwcPrimitive::ForBp2I16(f) => f.value(idx).map(Val::I16),
            LwcPrimitive::ForBp4I16(f) => f.value(idx).map(Val::I16),
            LwcPrimitive::ForBp8I16(f) => f.value(idx).map(Val::I16),
            LwcPrimitive::ForBp1U16(f) => f.value(idx).map(Val::U16),
            LwcPrimitive::ForBp2U16(f) => f.value(idx).map(Val::U16),
            LwcPrimitive::ForBp4U16(f) => f.value(idx).map(Val::U16),
            LwcPrimitive::ForBp8U16(f) => f.value(idx).map(Val::U16),
            LwcPrimitive::ForBp1I32(f) => f.value(idx).map(Val::I32),
            LwcPrimitive::ForBp2I32(f) => f.value(idx).map(Val::I32),
            LwcPrimitive::ForBp4I32(f) => f.value(idx).map(Val::I32),
            LwcPrimitive::ForBp8I32(f) => f.value(idx).map(Val::I32),
            LwcPrimitive::ForBp16I32(f) => f.value(idx).map(Val::I32),
            LwcPrimitive::ForBp1U32(f) => f.value(idx).map(Val::U32),
            LwcPrimitive::ForBp2U32(f) => f.value(idx).map(Val::U32),
            LwcPrimitive::ForBp4U32(f) => f.value(idx).map(Val::U32),
            LwcPrimitive::ForBp8U32(f) => f.value(idx).map(Val::U32),
            LwcPrimitive::ForBp16U32(f) => f.value(idx).map(Val::U32),
            LwcPrimitive::ForBp1I64(f) => f.value(idx).map(Val::I64),
            LwcPrimitive::ForBp2I64(f) => f.value(idx).map(Val::I64),
            LwcPrimitive::ForBp4I64(f) => f.value(idx).map(Val::I64),
            LwcPrimitive::ForBp8I64(f) => f.value(idx).map(Val::I64),
            LwcPrimitive::ForBp16I64(f) => f.value(idx).map(Val::I64),
            LwcPrimitive::ForBp32I64(f) => f.value(idx).map(Val::I64),
            LwcPrimitive::ForBp1U64(f) => f.value(idx).map(Val::U64),
            LwcPrimitive::ForBp2U64(f) => f.value(idx).map(Val::U64),
            LwcPrimitive::ForBp4U64(f) => f.value(idx).map(Val::U64),
            LwcPrimitive::ForBp8U64(f) => f.value(idx).map(Val::U64),
            LwcPrimitive::ForBp16U64(f) => f.value(idx).map(Val::U64),
            LwcPrimitive::ForBp32U64(f) => f.value(idx).map(Val::U64),
            LwcPrimitive::Bytes(b) => b.value(idx).map(Val::VarByte),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LwcPrimitiveSer<'a> {
    FlatI8(&'a [i8]),
    FlatU8(&'a [u8]),
    FlatI16(&'a [[u8; 2]]),
    FlatU16(&'a [[u8; 2]]),
    FlatI32(&'a [[u8; 4]]),
    FlatU32(&'a [[u8; 4]]),
    FlatF32(&'a [[u8; 4]]),
    FlatI64(&'a [[u8; 8]]),
    FlatU64(&'a [[u8; 8]]),
    FlatF64(&'a [[u8; 8]]),
    ForBpI8(ForBitpackingSer<'a, i8>),
    ForBpU8(ForBitpackingSer<'a, u8>),
    ForBpI16(ForBitpackingSer<'a, i16>),
    ForBpU16(ForBitpackingSer<'a, u16>),
    ForBpI32(ForBitpackingSer<'a, i32>),
    ForBpU32(ForBitpackingSer<'a, u32>),
    ForBpI64(ForBitpackingSer<'a, i64>),
    ForBpU64(ForBitpackingSer<'a, u64>),
    Bytes(LwcBytesSer),
}

impl<'a> LwcPrimitiveSer<'a> {
    #[inline]
    pub fn new_i8(input: &'a [i8]) -> Self {
        if input.is_empty() {
            return LwcPrimitiveSer::FlatI8(input);
        }
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpI8(fbp),
            None => LwcPrimitiveSer::FlatI8(input),
        }
    }

    #[inline]
    pub fn new_u8(input: &'a [u8]) -> Self {
        if input.is_empty() {
            return LwcPrimitiveSer::FlatU8(input);
        }
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpU8(fbp),
            None => LwcPrimitiveSer::FlatU8(input),
        }
    }

    #[inline]
    pub fn new_u64(input: &'a [u64]) -> Self {
        if input.is_empty() {
            return LwcPrimitiveSer::FlatU64(bytemuck::cast_slice(input));
        }
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpU64(fbp),
            None => LwcPrimitiveSer::FlatU64(bytemuck::cast_slice(input)),
        }
    }

    #[inline]
    pub fn new_i16(input: &'a [i16]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpI16(fbp),
            None => LwcPrimitiveSer::FlatI16(bytemuck::cast_slice(input)),
        }
    }

    #[inline]
    pub fn new_u16(input: &'a [u16]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpU16(fbp),
            None => LwcPrimitiveSer::FlatU16(bytemuck::cast_slice(input)),
        }
    }

    #[inline]
    pub fn new_i32(input: &'a [i32]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpI32(fbp),
            None => LwcPrimitiveSer::FlatI32(bytemuck::cast_slice(input)),
        }
    }

    #[inline]
    pub fn new_u32(input: &'a [u32]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpU32(fbp),
            None => LwcPrimitiveSer::FlatU32(bytemuck::cast_slice(input)),
        }
    }

    #[inline]
    pub fn new_i64(input: &'a [i64]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpI64(fbp),
            None => LwcPrimitiveSer::FlatI64(bytemuck::cast_slice(input)),
        }
    }

    #[inline]
    pub fn new_f32(input: &'a [f32]) -> Self {
        LwcPrimitiveSer::FlatF32(bytemuck::cast_slice(input))
    }

    #[inline]
    pub fn new_f64(input: &'a [f64]) -> Self {
        LwcPrimitiveSer::FlatF64(bytemuck::cast_slice(input))
    }

    #[inline]
    pub fn new_bytes(offsets: &[u32], data: &[u8]) -> Result<Self> {
        if offsets.is_empty() || offsets[0] != 0 {
            return Err(Error::InvalidCompressedData);
        }
        Ok(LwcPrimitiveSer::Bytes(LwcBytesSer {
            offsets: offsets.to_vec(),
            data: data.to_vec(),
        }))
    }

    #[inline]
    pub fn new_bytes_owned(offsets: Vec<u32>, data: Vec<u8>) -> Result<Self> {
        if offsets.is_empty() || offsets[0] != 0 {
            return Err(Error::InvalidCompressedData);
        }
        Ok(LwcPrimitiveSer::Bytes(LwcBytesSer { offsets, data }))
    }

    #[inline]
    pub fn code(&self) -> LwcCode {
        match self {
            LwcPrimitiveSer::FlatI8(_)
            | LwcPrimitiveSer::FlatU8(_)
            | LwcPrimitiveSer::FlatI16(_)
            | LwcPrimitiveSer::FlatU16(_)
            | LwcPrimitiveSer::FlatI32(_)
            | LwcPrimitiveSer::FlatU32(_)
            | LwcPrimitiveSer::FlatF32(_)
            | LwcPrimitiveSer::FlatI64(_)
            | LwcPrimitiveSer::FlatU64(_)
            | LwcPrimitiveSer::FlatF64(_)
            | LwcPrimitiveSer::Bytes(_) => LwcCode::Flat,
            LwcPrimitiveSer::ForBpI8(_)
            | LwcPrimitiveSer::ForBpU8(_)
            | LwcPrimitiveSer::ForBpI16(_)
            | LwcPrimitiveSer::ForBpU16(_)
            | LwcPrimitiveSer::ForBpI32(_)
            | LwcPrimitiveSer::ForBpU32(_)
            | LwcPrimitiveSer::ForBpI64(_)
            | LwcPrimitiveSer::ForBpU64(_) => LwcCode::ForBitpacking,
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
                LwcPrimitiveSer::FlatI16(f) => f.ser_len(ctx),
                LwcPrimitiveSer::FlatU16(f) => f.ser_len(ctx),
                LwcPrimitiveSer::FlatI32(f) => f.ser_len(ctx),
                LwcPrimitiveSer::FlatU32(f) => f.ser_len(ctx),
                LwcPrimitiveSer::FlatF32(f) => f.ser_len(ctx),
                LwcPrimitiveSer::FlatI64(f) => f.ser_len(ctx),
                LwcPrimitiveSer::FlatU64(f) => f.ser_len(ctx),
                LwcPrimitiveSer::FlatF64(f) => f.ser_len(ctx),
                LwcPrimitiveSer::ForBpI8(b) => b.ser_len(ctx),
                LwcPrimitiveSer::ForBpU8(b) => b.ser_len(ctx),
                LwcPrimitiveSer::ForBpI16(b) => b.ser_len(ctx),
                LwcPrimitiveSer::ForBpU16(b) => b.ser_len(ctx),
                LwcPrimitiveSer::ForBpI32(b) => b.ser_len(ctx),
                LwcPrimitiveSer::ForBpU32(b) => b.ser_len(ctx),
                LwcPrimitiveSer::ForBpI64(b) => b.ser_len(ctx),
                LwcPrimitiveSer::ForBpU64(b) => b.ser_len(ctx),
                LwcPrimitiveSer::Bytes(b) => b.ser_len(ctx),
            }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = ctx.ser_u8(out, start_idx, self.code() as u8);
        match self {
            LwcPrimitiveSer::FlatI8(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatU8(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatI16(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatU16(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatI32(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatU32(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatF32(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatI64(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatU64(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::FlatF64(f) => f.ser(ctx, out, idx),
            LwcPrimitiveSer::ForBpI8(b) => b.ser(ctx, out, idx),
            LwcPrimitiveSer::ForBpU8(b) => b.ser(ctx, out, idx),
            LwcPrimitiveSer::ForBpI16(b) => b.ser(ctx, out, idx),
            LwcPrimitiveSer::ForBpU16(b) => b.ser(ctx, out, idx),
            LwcPrimitiveSer::ForBpI32(b) => b.ser(ctx, out, idx),
            LwcPrimitiveSer::ForBpU32(b) => b.ser(ctx, out, idx),
            LwcPrimitiveSer::ForBpI64(b) => b.ser(ctx, out, idx),
            LwcPrimitiveSer::ForBpU64(b) => b.ser(ctx, out, idx),
            LwcPrimitiveSer::Bytes(b) => b.ser(ctx, out, idx),
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

pub struct LwcBytes<'a> {
    offsets: &'a [[u8; 4]],
    data: &'a [u8],
}

impl<'a> LwcBytes<'a> {
    #[inline]
    fn slice(&self, idx: usize) -> Option<&'a [u8]> {
        if idx + 1 >= self.offsets.len() {
            return None;
        }
        let start = u32::from_le_bytes(self.offsets[idx]) as usize;
        let end = u32::from_le_bytes(self.offsets[idx + 1]) as usize;
        if start > end || end > self.data.len() {
            return None;
        }
        Some(&self.data[start..end])
    }

    #[inline]
    pub fn value_at(&self, idx: usize) -> Option<MemVar> {
        self.slice(idx).map(MemVar::from)
    }
}

pub struct LwcBytesIter<'a> {
    offsets: &'a [[u8; 4]],
    data: &'a [u8],
    idx: usize,
}

impl<'a> Iterator for LwcBytesIter<'a> {
    type Item = MemVar;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let res = LwcBytes {
            offsets: self.offsets,
            data: self.data,
        }
        .value_at(self.idx);
        self.idx += 1;
        res
    }
}

impl<'a> LwcPrimitiveData for LwcBytes<'a> {
    type Value = MemVar;
    type Iter = LwcBytesIter<'a>;

    #[inline]
    fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    #[inline]
    fn value(&self, idx: usize) -> Option<Self::Value> {
        self.value_at(idx)
    }

    #[inline]
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E) {
        target.extend(self.iter());
    }

    #[inline]
    fn iter(&self) -> Self::Iter {
        LwcBytesIter {
            offsets: self.offsets,
            data: self.data,
            idx: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LwcBytesSer {
    offsets: Vec<u32>,
    data: Vec<u8>,
}

impl Ser<'_> for LwcBytesSer {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u64>() + self.offsets.len() * mem::size_of::<u32>() + self.data.len()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        debug_assert!(!self.offsets.is_empty());
        let mut idx = ctx.ser_u64(out, start_idx, (self.offsets.len() - 1) as u64);
        for off in self.offsets.iter().copied() {
            idx = ctx.ser_u32(out, idx, off);
        }
        let end_idx = idx + self.data.len();
        out[idx..end_idx].copy_from_slice(self.data.as_slice());
        end_idx
    }
}

pub struct LwcNullBitmap<'a> {
    bytes: &'a [u8],
}

impl<'a> LwcNullBitmap<'a> {
    #[inline]
    pub fn from_bytes(input: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let (len, input) = read_le_u16(input)?;
        let len = len as usize;
        if input.len() < len {
            return Err(Error::InvalidCompressedData);
        }
        let (bytes, rest) = input.split_at(len);
        Ok((LwcNullBitmap { bytes }, rest))
    }

    #[inline]
    pub fn is_null(&self, row_idx: usize) -> bool {
        let byte_idx = row_idx / 8;
        if byte_idx >= self.bytes.len() {
            return false;
        }
        let bit_mask = 1 << (row_idx % 8);
        self.bytes[byte_idx] & bit_mask != 0
    }

    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

pub struct LwcNullBitmapSer<'a> {
    bytes: &'a [u8],
}

impl<'a> LwcNullBitmapSer<'a> {
    #[inline]
    pub fn new(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() > u16::MAX as usize {
            return Err(Error::InvalidCompressedData);
        }
        Ok(LwcNullBitmapSer { bytes })
    }
}

impl Ser<'_> for LwcNullBitmapSer<'_> {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u16>() + self.bytes.len()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = ctx.ser_u16(out, start_idx, self.bytes.len() as u16);
        let end_idx = idx + self.bytes.len();
        out[idx..end_idx].copy_from_slice(self.bytes);
        end_idx
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
fn read_le_u32(input: &[u8]) -> Result<(u32, &[u8])> {
    if input.len() < 4 {
        return Err(Error::InvalidCompressedData);
    }
    let u: [u8; 4] = input[..4].try_into().unwrap();
    Ok((u32::from_le_bytes(u), &input[4..]))
}

#[inline]
fn read_le_i32(input: &[u8]) -> Result<(i32, &[u8])> {
    if input.len() < 4 {
        return Err(Error::InvalidCompressedData);
    }
    let u: [u8; 4] = input[..4].try_into().unwrap();
    Ok((i32::from_le_bytes(u), &input[4..]))
}

#[inline]
fn read_le_u16(input: &[u8]) -> Result<(u16, &[u8])> {
    if input.len() < 2 {
        return Err(Error::InvalidCompressedData);
    }
    let u: [u8; 2] = input[..2].try_into().unwrap();
    Ok((u16::from_le_bytes(u), &input[2..]))
}

#[inline]
fn read_le_i16(input: &[u8]) -> Result<(i16, &[u8])> {
    if input.len() < 2 {
        return Err(Error::InvalidCompressedData);
    }
    let u: [u8; 2] = input[..2].try_into().unwrap();
    Ok((i16::from_le_bytes(u), &input[2..]))
}

#[inline]
fn read_le_i64(input: &[u8]) -> Result<(i64, &[u8])> {
    if input.len() < 8 {
        return Err(Error::InvalidCompressedData);
    }
    let u: [u8; 8] = input[..8].try_into().unwrap();
    Ok((i64::from_le_bytes(u), &input[8..]))
}

#[inline]
fn read_u8(input: &[u8]) -> Result<(u8, &[u8])> {
    if input.is_empty() {
        return Err(Error::InvalidCompressedData);
    }
    let v = input[0];
    Ok((v, &input[1..]))
}

#[inline]
fn read_i8(input: &[u8]) -> Result<(i8, &[u8])> {
    if input.is_empty() {
        return Err(Error::InvalidCompressedData);
    }
    let v = input[0] as i8;
    Ok((v, &input[1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lwc_primitive_serde() {
        let ctx = SerdeCtx::default();

        // i8
        for input in [
            vec![1i8],
            vec![1i8, 0, 1, 0, 1, 0],
            vec![1i8, 2],
            vec![1i8, 2, 4],
            vec![1i8, 2, 4, 8],
            vec![1i8, 2, 4, 8, 16],
            vec![1i8, 2, 4, 8, 16, 32],
            vec![1i8, 2, 4, 16, -100, 100],
        ] {
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
        }

        // u8
        for input in [
            vec![1u8],
            vec![1u8, 2],
            vec![1u8, 2, 4],
            vec![1u8, 2, 4, 8],
            vec![1u8, 2, 4, 8, 16],
            vec![1u8, 2, 4, 8, 16, 32],
            vec![1u8, 2, 4, 16, 100, 200, 250],
        ] {
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
        }

        // i16
        for input in [
            vec![-1i16, 0],
            vec![-1i16, 0, 1],
            vec![-1i16, 0, 1, 4],
            vec![-1i16, 0, 1, 4, 8],
            vec![-1i16, 2, 3, 4, -8, 16],
            vec![-1i16, 2, 3, 4, -8, 1 << 14],
        ] {
            let lwc_ser = LwcPrimitiveSer::new_i16(&input);
            let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
            let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
            assert_eq!(ser_idx, res.len());
            let lwc_data = LwcData::from_bytes(ValKind::I16, &res).unwrap();
            let mut output = vec![];
            for i in 0..lwc_data.len() {
                output.push(lwc_data.value(i).unwrap().as_i16().unwrap());
            }
            assert_eq!(input, output);
        }

        // u16
        for input in [
            vec![1u16, 2],
            vec![1u16, 2, 3, 4],
            vec![1u16, 2, 3, 4, 8],
            vec![1u16, 2, 3, 4, 8, 16],
            vec![1u16, 2, 3, 4, 8, 16, 32],
            vec![1u16, 2, 3, 4, 8, 16, 1 << 14],
        ] {
            let lwc_ser = LwcPrimitiveSer::new_u16(&input);
            let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
            let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
            assert_eq!(ser_idx, res.len());
            let lwc_data = LwcData::from_bytes(ValKind::U16, &res).unwrap();
            let mut output = vec![];
            for i in 0..lwc_data.len() {
                output.push(lwc_data.value(i).unwrap().as_u16().unwrap());
            }
            assert_eq!(input, output);
        }

        // i32
        for input in [
            vec![1i32, 2],
            vec![1i32, 2, 3, 4],
            vec![1i32, 2, 3, 4, 8],
            vec![1i32, 2, 3, 4, 8, 16],
            vec![1i32, 2, 3, 4, 8, 16, 32],
            vec![1i32, 2, 3, 4, 8, 16, 32, 128],
            vec![-2i32, -1, 0, 1, 2, 4, 8, 128, 1024],
            vec![-2i32, -1, 0, 1, 2, 4, 8, 128, 1 << 30],
        ] {
            let lwc_ser = LwcPrimitiveSer::new_i32(&input);
            let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
            let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
            assert_eq!(ser_idx, res.len());
            let lwc_data = LwcData::from_bytes(ValKind::I32, &res).unwrap();
            let mut output = vec![];
            for i in 0..lwc_data.len() {
                output.push(lwc_data.value(i).unwrap().as_i32().unwrap());
            }
            assert_eq!(input, output);
        }

        // u32
        for input in [
            vec![1u32, 2],
            vec![1u32, 2, 3, 4],
            vec![1u32, 2, 3, 4, 8],
            vec![1u32, 2, 3, 4, 8, 16],
            vec![1u32, 2, 3, 4, 8, 16, 32],
            vec![1u32, 3, 7, 15, 31, 63, 127],
            vec![1u32, 3, 7, 15, 31, 63, 127, 1 << 14],
            vec![1u32, 3, 7, 15, 31, 63, 1 << 30],
        ] {
            let lwc_ser = LwcPrimitiveSer::new_u32(&input);
            let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
            let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
            assert_eq!(ser_idx, res.len());
            let lwc_data = LwcData::from_bytes(ValKind::U32, &res).unwrap();
            let mut output = vec![];
            for i in 0..lwc_data.len() {
                output.push(lwc_data.value(i).unwrap().as_u32().unwrap());
            }
            assert_eq!(input, output);
        }

        // f32
        let input = vec![1.5f32, 2.25, -3.5, 0.0];
        let lwc_ser = LwcPrimitiveSer::new_f32(&input);
        let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
        let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
        assert_eq!(ser_idx, res.len());
        let lwc_data = LwcData::from_bytes(ValKind::F32, &res).unwrap();
        let mut output = vec![];
        for i in 0..lwc_data.len() {
            output.push(lwc_data.value(i).unwrap().as_f32().unwrap());
        }
        assert_eq!(input, output);

        // i64
        for input in [
            vec![1i64, 2],
            vec![1i64, 2, 3, 4],
            vec![1i64, 2, 3, 4, 8],
            vec![1i64, 2, 3, 4, 8, 16],
            vec![1i64, 2, 3, 4, 8, 16, 32],
            vec![1i64, 3, 7, 15, 31, 63, 127],
            vec![1i64, 3, 7, 15, 31, 63, 127, 1 << 14],
            vec![-2i64, -1, 0, 1, 2, 4, 8, 1024, 65536],
            vec![-2i64, -1, 0, 1, 2, 4, 8, 1024, 1 << 60],
        ] {
            let lwc_ser = LwcPrimitiveSer::new_i64(&input);
            let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
            let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
            assert_eq!(ser_idx, res.len());
            let lwc_data = LwcData::from_bytes(ValKind::I64, &res).unwrap();
            let mut output = vec![];
            for i in 0..lwc_data.len() {
                output.push(lwc_data.value(i).unwrap().as_i64().unwrap());
            }
            assert_eq!(input, output);
        }

        // u64
        for input in [
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

        // f64
        let input = vec![1.5f64, 2.25, -3.5, 0.0, 128.5];
        let lwc_ser = LwcPrimitiveSer::new_f64(&input);
        let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
        let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
        assert_eq!(ser_idx, res.len());
        let lwc_data = LwcData::from_bytes(ValKind::F64, &res).unwrap();
        let mut output = vec![];
        for i in 0..lwc_data.len() {
            output.push(lwc_data.value(i).unwrap().as_f64().unwrap());
        }
        assert_eq!(input, output);

        // bytes
        let offsets = vec![0u32, 3, 3, 7];
        let bytes = b"abc"
            .iter()
            .chain(b"".iter())
            .chain(b"defg".iter())
            .copied()
            .collect::<Vec<u8>>();
        let lwc_ser = LwcPrimitiveSer::new_bytes(&offsets, &bytes).unwrap();
        let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
        let ser_idx = lwc_ser.ser(&ctx, &mut res, 0);
        assert_eq!(ser_idx, res.len());
        let lwc_data = LwcData::from_bytes(ValKind::VarByte, &res).unwrap();
        if let LwcData::Bytes(b) = lwc_data {
            let out: Vec<Vec<u8>> = b.iter().map(|v| v.as_bytes().to_vec()).collect();
            assert_eq!(out, vec![b"abc".to_vec(), b"".to_vec(), b"defg".to_vec()]);
        } else {
            panic!("expected bytes variant");
        }
    }

    #[test]
    fn test_lwc_bytes_invalid() {
        let ctx = SerdeCtx::default();

        let err = LwcPrimitiveSer::new_bytes(&[], &[]);
        assert!(matches!(err, Err(Error::InvalidCompressedData)));

        let err = LwcPrimitiveSer::new_bytes(&[1, 2], &[0u8]);
        assert!(matches!(err, Err(Error::InvalidCompressedData)));

        let offsets = vec![0u32, 0];
        let bytes = vec![];
        let lwc_ser = LwcPrimitiveSer::new_bytes(&offsets, &bytes).unwrap();
        let mut res = vec![0u8; lwc_ser.ser_len(&ctx)];
        lwc_ser.ser(&ctx, &mut res, 0);
        let err = LwcData::from_bytes(ValKind::VarByte, &res[..res.len() - 1]);
        assert!(matches!(err, Err(Error::InvalidCompressedData)));
    }

    #[test]
    fn test_lwc_bytes_out_of_range() {
        // offsets point past the end of the backing data to exercise the
        // defensive bounds checks inside `LwcBytes::slice` and `value_at`.
        let offsets: &[[u8; 4]] = &[[0, 0, 0, 0], [5, 0, 0, 0]];
        let data: &[u8] = &[1, 2, 3];
        let bytes = LwcBytes { offsets, data };

        assert_eq!(bytes.len(), 1);
        assert!(bytes.value_at(0).is_none());
        assert!(bytes.value_at(1).is_none());
        assert_eq!(bytes.iter().count(), 0);
        assert!(bytes.iter().next().is_none());
    }

    #[test]
    fn test_for_bitpacking_invalid_bits() {
        // Build a payload with an unsupported bit width for u8 (n_bits = 3)
        // so `LwcData::from_bytes` returns NotSupported.
        let mut payload = vec![LwcCode::ForBitpacking as u8];
        payload.push(3); // n_bits
        payload.extend_from_slice(&1u64.to_le_bytes()); // len
        payload.push(0); // min value for u8
        payload.push(0); // data byte (div_ceil(1 * 3, 8) == 1)

        let err = LwcData::from_bytes(ValKind::U8, &payload);
        assert!(matches!(err, Err(Error::NotSupported(_))));
    }

    #[test]
    fn test_lwc_null_bitmap_serde() {
        let ctx = SerdeCtx::default();
        let bytes = [0b0000_1010u8, 0b0000_0001];
        let ser = LwcNullBitmapSer::new(&bytes).unwrap();
        let mut out = vec![0u8; ser.ser_len(&ctx)];
        let idx = ser.ser(&ctx, &mut out, 0);
        assert_eq!(idx, out.len());

        let (bitmap, rest) = LwcNullBitmap::from_bytes(&out).unwrap();
        assert!(rest.is_empty());
        assert_eq!(bitmap.as_bytes(), bytes);
        assert!(bitmap.is_null(1));
        assert!(bitmap.is_null(3));
        assert!(bitmap.is_null(8));
        assert!(!bitmap.is_null(0));
        assert!(!bitmap.is_null(2));
    }
}
