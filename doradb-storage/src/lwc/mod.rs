//! This module contains definition and functions of LWC(Lightweight Compression) Block.

pub mod page;

pub use page::*;

use crate::bitmap::Bitmap;
use crate::catalog::TableMetadata;
use crate::compression::*;
use crate::error::{Error, Result};
use crate::io::DirectBuf;
use crate::row::vector_scan::{ScanBuffer, ScanColumnValues};
use crate::row::{RowID, RowPage};
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

const LWC_PAGE_SIZE: usize = 64 * 1024;
const LWC_PAGE_HEADER_SIZE: usize = 24;
const LWC_PAGE_FOOTER_SIZE: usize = 32;

struct LwcColumnStats {
    min_i64: i64,
    max_i64: i64,
    min_u64: u64,
    max_u64: u64,
    initialized: bool,
}

impl LwcColumnStats {
    fn new() -> Self {
        LwcColumnStats {
            min_i64: 0,
            max_i64: 0,
            min_u64: 0,
            max_u64: 0,
            initialized: false,
        }
    }

    fn update_i64(&mut self, value: i64) {
        if !self.initialized {
            self.min_i64 = value;
            self.max_i64 = value;
            self.initialized = true;
        } else {
            self.min_i64 = self.min_i64.min(value);
            self.max_i64 = self.max_i64.max(value);
        }
    }

    fn update_u64(&mut self, value: u64) {
        if !self.initialized {
            self.min_u64 = value;
            self.max_u64 = value;
            self.initialized = true;
        } else {
            self.min_u64 = self.min_u64.min(value);
            self.max_u64 = self.max_u64.max(value);
        }
    }
}

#[derive(Clone, Copy)]
struct LwcColumnSnapshot {
    initialized: bool,
    min_i64: i64,
    max_i64: i64,
    min_u64: u64,
    max_u64: u64,
}

impl LwcColumnStats {
    fn snapshot(&self) -> LwcColumnSnapshot {
        LwcColumnSnapshot {
            initialized: self.initialized,
            min_i64: self.min_i64,
            max_i64: self.max_i64,
            min_u64: self.min_u64,
            max_u64: self.max_u64,
        }
    }

    fn restore(&mut self, snapshot: LwcColumnSnapshot) {
        self.initialized = snapshot.initialized;
        self.min_i64 = snapshot.min_i64;
        self.max_i64 = snapshot.max_i64;
        self.min_u64 = snapshot.min_u64;
        self.max_u64 = snapshot.max_u64;
    }
}

struct LwcSnapshot {
    row_count: usize,
    row_ids_len: usize,
    stats: Vec<LwcColumnSnapshot>,
}

pub struct LwcBuilder<'a> {
    metadata: &'a TableMetadata,
    buffer: ScanBuffer,
    row_ids: Vec<RowID>,
    stats: Vec<LwcColumnStats>,
    ctx: SerdeCtx,
}

impl<'a> LwcBuilder<'a> {
    pub fn new(metadata: &'a TableMetadata) -> Self {
        let scan_set: Vec<_> = (0..metadata.col_count()).collect();
        let stats = (0..metadata.col_count())
            .map(|_| LwcColumnStats::new())
            .collect();
        let buffer = ScanBuffer::new(metadata, &scan_set);
        LwcBuilder {
            metadata,
            buffer,
            row_ids: Vec::new(),
            stats,
            ctx: SerdeCtx::default(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    pub fn row_count(&self) -> usize {
        self.buffer.len()
    }

    pub fn append_row_page(&mut self, page: &RowPage) -> Result<bool> {
        let snapshot = self.snapshot_state();
        let view = page.vector_view(self.metadata);
        let mut new_row_ids = Vec::with_capacity(view.rows_non_deleted());
        for (start_idx, end_idx) in view.range_non_deleted() {
            for idx in start_idx..end_idx {
                new_row_ids.push(page.row_id(idx));
            }
        }
        self.scan_page_stats(&view, &new_row_ids)?;
        self.buffer.scan(view)?;
        self.row_ids.extend(new_row_ids);
        if self.estimate_size()? > LWC_PAGE_SIZE {
            self.rollback(snapshot);
            return Ok(false);
        }
        Ok(true)
    }

    pub fn build(&self) -> Result<DirectBuf> {
        if self.buffer.is_empty() {
            return Err(Error::InvalidState);
        }
        let row_count = self.buffer.len();
        if row_count > u16::MAX as usize {
            return Err(Error::InvalidArgument);
        }
        let row_id_ser = LwcPrimitiveSer::new_u64(&self.row_ids);
        let row_id_len = row_id_ser.ser_len(&self.ctx);
        let mut column_payloads = Vec::with_capacity(self.metadata.col_count());
        let mut col_offsets = Vec::with_capacity(self.metadata.col_count());
        let mut offset = mem::size_of::<u16>() * self.metadata.col_count() + row_id_len;

        for col_idx in 0..self.metadata.col_count() {
            let column = self
                .buffer
                .column(col_idx)
                .ok_or(Error::InvalidColumnScan)?;
            let mut data = Vec::new();
            if let Some(bitmap) = column.null_bitmap {
                let bytes = bitmap_to_bytes(bitmap, row_count);
                let bitmap_ser = LwcNullBitmapSer::new(&bytes)?;
                let mut tmp = vec![0u8; bitmap_ser.ser_len(&self.ctx)];
                bitmap_ser.ser(&self.ctx, &mut tmp, 0);
                data.extend_from_slice(&tmp);
            }
            let payload = match column.values {
                ScanColumnValues::I8(vals) => {
                    let ser = LwcPrimitiveSer::new_i8(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::U8(vals) => {
                    let ser = LwcPrimitiveSer::new_u8(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::I16(vals) => {
                    let ser = LwcPrimitiveSer::new_i16(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::U16(vals) => {
                    let ser = LwcPrimitiveSer::new_u16(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::I32(vals) => {
                    let ser = LwcPrimitiveSer::new_i32(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::U32(vals) => {
                    let ser = LwcPrimitiveSer::new_u32(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::F32(vals) => {
                    let ser = LwcPrimitiveSer::new_f32(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::I64(vals) => {
                    let ser = LwcPrimitiveSer::new_i64(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::U64(vals) => {
                    let ser = LwcPrimitiveSer::new_u64(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::F64(vals) => {
                    let ser = LwcPrimitiveSer::new_f64(vals);
                    serialize_primitive(&ser, &self.ctx)
                }
                ScanColumnValues::VarByte {
                    offsets,
                    data: bytes,
                } => {
                    let mut lwc_offsets = Vec::with_capacity(offsets.len() + 1);
                    lwc_offsets.push(0u32);
                    for (_, end) in offsets.iter().copied() {
                        lwc_offsets.push(end as u32);
                    }
                    let ser = LwcPrimitiveSer::new_bytes_owned(lwc_offsets, bytes.to_vec())?;
                    serialize_primitive(&ser, &self.ctx)
                }
            };
            data.extend_from_slice(&payload);
            offset += data.len();
            col_offsets.push(offset as u16);
            column_payloads.push(data);
        }

        let first_row_id = *self.row_ids.first().unwrap_or(&0);
        let last_row_id = *self.row_ids.last().unwrap_or(&0);
        let first_col_offset =
            (mem::size_of::<u16>() * self.metadata.col_count() + row_id_len) as u16;
        let header = LwcPageHeader::new(
            first_row_id,
            last_row_id,
            row_count as u16,
            self.metadata.col_count() as u16,
            first_col_offset,
        );

        let mut buf = DirectBuf::zeroed(LWC_PAGE_SIZE);
        buf.truncate(0);
        buf.extend_ser(&header, &self.ctx);
        for offset in col_offsets {
            buf.extend_ser(&offset, &self.ctx);
        }
        buf.extend_ser(&row_id_ser, &self.ctx);
        for payload in column_payloads {
            buf.extend_from_slice(&payload);
        }
        Ok(buf)
    }

    fn snapshot_state(&self) -> LwcSnapshot {
        LwcSnapshot {
            row_count: self.buffer.len(),
            row_ids_len: self.row_ids.len(),
            stats: self.stats.iter().map(|s| s.snapshot()).collect(),
        }
    }

    fn rollback(&mut self, snapshot: LwcSnapshot) {
        self.buffer.truncate(snapshot.row_count);
        self.row_ids.truncate(snapshot.row_ids_len);
        for (stat, snap) in self.stats.iter_mut().zip(snapshot.stats.into_iter()) {
            stat.restore(snap);
        }
    }

    fn scan_page_stats(
        &mut self,
        view: &crate::row::vector_scan::PageVectorView<'_, '_>,
        row_ids: &[RowID],
    ) -> Result<()> {
        if row_ids.is_empty() {
            return Ok(());
        }
        for col_idx in 0..self.metadata.col_count() {
            let (null_bitmap, values) = view.col(col_idx);
            match values {
                crate::row::vector_scan::ValArrayRef::I8(vals) => {
                    self.update_stats_i64(col_idx, null_bitmap, vals, view.range_non_deleted());
                }
                crate::row::vector_scan::ValArrayRef::U8(vals) => {
                    self.update_stats_u64(col_idx, null_bitmap, vals, view.range_non_deleted());
                }
                crate::row::vector_scan::ValArrayRef::I16(vals) => {
                    self.update_stats_i64(col_idx, null_bitmap, vals, view.range_non_deleted());
                }
                crate::row::vector_scan::ValArrayRef::U16(vals) => {
                    self.update_stats_u64(col_idx, null_bitmap, vals, view.range_non_deleted());
                }
                crate::row::vector_scan::ValArrayRef::I32(vals) => {
                    self.update_stats_i64(col_idx, null_bitmap, vals, view.range_non_deleted());
                }
                crate::row::vector_scan::ValArrayRef::U32(vals) => {
                    self.update_stats_u64(col_idx, null_bitmap, vals, view.range_non_deleted());
                }
                crate::row::vector_scan::ValArrayRef::I64(vals) => {
                    self.update_stats_i64(col_idx, null_bitmap, vals, view.range_non_deleted());
                }
                crate::row::vector_scan::ValArrayRef::U64(vals) => {
                    self.update_stats_u64(col_idx, null_bitmap, vals, view.range_non_deleted());
                }
                crate::row::vector_scan::ValArrayRef::F32(_)
                | crate::row::vector_scan::ValArrayRef::F64(_)
                | crate::row::vector_scan::ValArrayRef::VarByte(_, _) => {}
            }
        }
        Ok(())
    }

    fn update_stats_i64<T: Copy + Into<i64>, R: Iterator<Item = (usize, usize)>>(
        &mut self,
        col_idx: usize,
        null_bitmap: Option<&[u64]>,
        values: &[T],
        ranges: R,
    ) {
        for (start_idx, end_idx) in ranges {
            for idx in start_idx..end_idx {
                if null_bitmap.map(|bm| bm.bitmap_get(idx)).unwrap_or(false) {
                    continue;
                }
                if let Some(value) = values.get(idx) {
                    self.stats[col_idx].update_i64((*value).into());
                }
            }
        }
    }

    fn update_stats_u64<T: Copy + Into<u64>, R: Iterator<Item = (usize, usize)>>(
        &mut self,
        col_idx: usize,
        null_bitmap: Option<&[u64]>,
        values: &[T],
        ranges: R,
    ) {
        for (start_idx, end_idx) in ranges {
            for idx in start_idx..end_idx {
                if null_bitmap.map(|bm| bm.bitmap_get(idx)).unwrap_or(false) {
                    continue;
                }
                if let Some(value) = values.get(idx) {
                    self.stats[col_idx].update_u64((*value).into());
                }
            }
        }
    }

    fn estimate_size(&self) -> Result<usize> {
        let row_count = self.buffer.len();
        let mut total = LWC_PAGE_HEADER_SIZE;
        total += mem::size_of::<u16>() * self.metadata.col_count();
        total += estimate_row_ids_size(&self.row_ids);
        total += estimate_columns_size(self.metadata, &self.buffer, &self.stats, row_count)?;
        total += LWC_PAGE_FOOTER_SIZE;
        Ok(total)
    }
}

fn serialize_primitive<'a>(ser: &LwcPrimitiveSer<'a>, ctx: &SerdeCtx) -> Vec<u8> {
    let mut bytes = vec![0u8; ser.ser_len(ctx)];
    ser.ser(ctx, &mut bytes, 0);
    bytes
}

fn bitmap_to_bytes(bitmap: &[u64], len: usize) -> Vec<u8> {
    let units = len.div_ceil(64);
    let mut bytes = vec![0u8; len.div_ceil(8)];
    for (unit_idx, &unit) in bitmap.iter().take(units).enumerate() {
        let start = unit_idx * 8;
        let end = (start + 8).min(bytes.len());
        bytes[start..end].copy_from_slice(&unit.to_le_bytes()[..end - start]);
    }
    if let Some(last) = bytes.last_mut() {
        let rem = len % 8;
        if rem != 0 {
            let mask = (1u8 << rem) - 1;
            *last &= mask;
        }
    }
    bytes
}

fn estimate_row_ids_size(row_ids: &[RowID]) -> usize {
    if row_ids.is_empty() {
        return mem::size_of::<u8>();
    }
    match ForBitpackingSer::new(row_ids) {
        Some(fbp) => {
            let packed = mem::size_of::<u8>() + fbp.ser_len(&SerdeCtx::default());
            let flat = mem::size_of::<u8>()
                + mem::size_of::<u64>()
                + row_ids.len() * mem::size_of::<u64>();
            if packed < flat { packed } else { flat }
        }
        None => {
            mem::size_of::<u8>() + mem::size_of::<u64>() + row_ids.len() * mem::size_of::<u64>()
        }
    }
}

fn estimate_columns_size(
    metadata: &TableMetadata,
    buffer: &ScanBuffer,
    stats: &[LwcColumnStats],
    row_count: usize,
) -> Result<usize> {
    let mut total = 0usize;
    for col_idx in 0..metadata.col_count() {
        let column = buffer.column(col_idx).ok_or(Error::InvalidColumnScan)?;
        if column.null_bitmap.is_some() {
            total += mem::size_of::<u16>() + row_count.div_ceil(8);
        }
        total += estimate_column_payload(
            metadata.val_kind(col_idx),
            &column.values,
            &stats[col_idx],
            row_count,
        )?;
    }
    Ok(total)
}

fn estimate_column_payload(
    kind: ValKind,
    values: &ScanColumnValues<'_>,
    stats: &LwcColumnStats,
    row_count: usize,
) -> Result<usize> {
    let size = match (kind, values) {
        (ValKind::I8, ScanColumnValues::I8(_)) => {
            estimate_i64_payload(stats, row_count, mem::size_of::<i8>())
        }
        (ValKind::U8, ScanColumnValues::U8(_)) => {
            estimate_u64_payload(stats, row_count, mem::size_of::<u8>())
        }
        (ValKind::I16, ScanColumnValues::I16(_)) => {
            estimate_i64_payload(stats, row_count, mem::size_of::<i16>())
        }
        (ValKind::U16, ScanColumnValues::U16(_)) => {
            estimate_u64_payload(stats, row_count, mem::size_of::<u16>())
        }
        (ValKind::I32, ScanColumnValues::I32(_)) => {
            estimate_i64_payload(stats, row_count, mem::size_of::<i32>())
        }
        (ValKind::U32, ScanColumnValues::U32(_)) => {
            estimate_u64_payload(stats, row_count, mem::size_of::<u32>())
        }
        (ValKind::I64, ScanColumnValues::I64(_)) => {
            estimate_i64_payload(stats, row_count, mem::size_of::<i64>())
        }
        (ValKind::U64, ScanColumnValues::U64(_)) => {
            estimate_u64_payload(stats, row_count, mem::size_of::<u64>())
        }
        (ValKind::F32, ScanColumnValues::F32(_)) => {
            mem::size_of::<u8>() + mem::size_of::<u64>() + row_count * mem::size_of::<f32>()
        }
        (ValKind::F64, ScanColumnValues::F64(_)) => {
            mem::size_of::<u8>() + mem::size_of::<u64>() + row_count * mem::size_of::<f64>()
        }
        (ValKind::VarByte, ScanColumnValues::VarByte { offsets, data }) => {
            let count = offsets.len();
            mem::size_of::<u8>()
                + mem::size_of::<u64>()
                + (count + 1) * mem::size_of::<u32>()
                + data.len()
        }
        _ => return Err(Error::InvalidColumnScan),
    };
    Ok(size)
}

fn estimate_i64_payload(stats: &LwcColumnStats, row_count: usize, unit: usize) -> usize {
    let flat = mem::size_of::<u8>() + mem::size_of::<u64>() + row_count * unit;
    if !stats.initialized {
        return flat;
    }
    let range = stats.max_i64.wrapping_sub(stats.min_i64) as u64;
    estimate_bitpacked_size(range, row_count, unit, flat).min(flat)
}

fn estimate_u64_payload(stats: &LwcColumnStats, row_count: usize, unit: usize) -> usize {
    let flat = mem::size_of::<u8>() + mem::size_of::<u64>() + row_count * unit;
    if !stats.initialized {
        return flat;
    }
    let range = stats.max_u64.wrapping_sub(stats.min_u64);
    estimate_bitpacked_size(range, row_count, unit, flat).min(flat)
}

fn estimate_bitpacked_size(range: u64, row_count: usize, unit: usize, flat: usize) -> usize {
    let bits = if range < (1 << 1) {
        1
    } else if range < (1 << 2) {
        2
    } else if range < (1 << 4) {
        4
    } else if range < (1 << 8) {
        if unit <= 1 {
            return flat;
        }
        8
    } else if range < (1 << 16) {
        if unit <= 2 {
            return flat;
        }
        16
    } else if range < (1 << 32) {
        if unit <= 4 {
            return flat;
        }
        32
    } else {
        return flat;
    };
    mem::size_of::<u8>()
        + mem::size_of::<u8>()
        + mem::size_of::<u64>()
        + unit
        + (row_count * bits).div_ceil(8)
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

#[derive(Debug)]
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
    use crate::catalog::{ColumnAttributes, ColumnSpec};
    use crate::row::{Delete, InsertRow, RowPage};
    use crate::value::Val;
    use std::mem::MaybeUninit;

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

    #[test]
    fn test_lwc_builder_from_row_page() {
        let metadata = TableMetadata::new(
            vec![
                ColumnSpec::new("c0", ValKind::U8, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::I16, ColumnAttributes::NULLABLE),
            ],
            vec![],
        );
        let mut page: RowPage = unsafe { MaybeUninit::zeroed().assume_init() };
        page.init(100, 20, &metadata);

        let mut expected_rows = Vec::new();
        for offset in 0..10u64 {
            let row_id = 100 + offset;
            let c0 = Val::U8(offset as u8);
            let c1 = if offset % 2 == 0 {
                Val::Null
            } else {
                Val::I16(offset as i16)
            };
            let res = page.insert(&metadata, &[c0, c1]);
            assert!(matches!(res, InsertRow::Ok(_)));
            expected_rows.push((
                row_id,
                offset as u8,
                if offset % 2 == 0 {
                    None
                } else {
                    Some(offset as i16)
                },
            ));
        }

        assert!(matches!(page.delete(102), Delete::Ok));
        assert!(matches!(page.delete(105), Delete::Ok));
        expected_rows.retain(|(row_id, _, _)| *row_id != 102 && *row_id != 105);

        let mut builder = LwcBuilder::new(&metadata);
        let appended = builder.append_row_page(&page).unwrap();
        assert!(appended);
        let buf = builder.build().unwrap();

        let mut bytes = [0u8; 65536];
        bytes[..buf.data().len()].copy_from_slice(buf.data());
        let lwc_page = unsafe { std::mem::transmute::<&[u8; 65536], &LwcPage>(&bytes) };
        assert_eq!(lwc_page.header.row_count() as usize, expected_rows.len());
        assert_eq!(
            lwc_page.header.first_row_id(),
            expected_rows.first().unwrap().0
        );
        assert_eq!(
            lwc_page.header.last_row_id(),
            expected_rows.last().unwrap().0
        );

        let column0 = lwc_page.column(&metadata, 0).unwrap();
        let column1 = lwc_page.column(&metadata, 1).unwrap();
        let data0 = column0.data().unwrap();
        let data1 = column1.data().unwrap();
        for (idx, (_row_id, c0, c1)) in expected_rows.iter().enumerate() {
            assert_eq!(data0.value(idx).unwrap().as_u8().unwrap(), *c0);
            if let Some(value) = c1 {
                assert!(!column1.is_null(idx));
                assert_eq!(data1.value(idx).unwrap().as_i16().unwrap(), *value);
            } else {
                assert!(column1.is_null(idx));
            }
        }
    }
}
