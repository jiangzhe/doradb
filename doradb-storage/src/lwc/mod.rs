//! This module contains definition and functions of LWC(Lightweight Compression) Block.

pub(crate) mod block;

pub(crate) use block::*;

use crate::bitmap::Bitmap;
use crate::catalog::TableColumnLayout;
use crate::compression::*;
use crate::error::{DataIntegrityError, DataIntegrityResult, InternalError, InternalResult};
use crate::file::block_integrity::{LWC_BLOCK_SPEC, write_block_checksum, write_block_header};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::id::RowID;
use crate::io::DirectBuf;
use crate::layout;
use crate::row::vector_scan::{PageVectorView, ScanBuffer, ScanColumnValues, ValArrayRef};
use crate::serde::{ForBitpackingSer, Ser, Serde};
use crate::value::{MemVar, Val, ValKind};
use error_stack::{Report, ResultExt};
use std::borrow::Cow;
use std::mem;
use std::slice::Iter;
use zerocopy::{Immutable, IntoBytes};

const LWC_BLOCK_HEADER_SIZE: usize = 24;

/// Lightweight compressed data.
pub(crate) enum LwcData<'a> {
    Primitive(LwcPrimitive<'a>),
    Bytes(LwcBytes<'a>),
}

impl<'a> LwcData<'a> {
    /// Parse input and create a reader to read compressed data.
    /// The input format should be as below:
    /// First byte must be compression code which indicates the
    /// compression type of the data.
    #[inline]
    pub(crate) fn from_bytes(kind: ValKind, input: &'a [u8]) -> DataIntegrityResult<Self> {
        let (code, input) = read_u8(input)?;
        let c = LwcCode::decode(code)?;
        let res = match c {
            LwcCode::Flat => {
                let (len, input) = read_le_u64(input)?;
                let len = lwc_payload_len_from_u64(len, "LWC flat payload")?;
                match kind {
                    ValKind::I8 => {
                        let input = flat_lwc_payload(input, len, 1, "LWC flat i8 payload")?;
                        LwcData::Primitive(LwcPrimitive::FlatI8(FlatI8(input)))
                    }
                    ValKind::U8 => {
                        let input = flat_lwc_payload(input, len, 1, "LWC flat u8 payload")?;
                        LwcData::Primitive(LwcPrimitive::FlatU8(FlatU8(input)))
                    }
                    ValKind::I16 => {
                        let input = flat_lwc_payload(input, len, 2, "LWC flat i16 payload")?;
                        let input = persisted_lwc_layout(
                            layout::try_slice_from_bytes::<[u8; 2]>(input),
                            "flat_i16_payload",
                        )?;
                        LwcData::Primitive(LwcPrimitive::FlatI16(FlatI16(input)))
                    }
                    ValKind::U16 => {
                        let input = flat_lwc_payload(input, len, 2, "LWC flat u16 payload")?;
                        let input = persisted_lwc_layout(
                            layout::try_slice_from_bytes::<[u8; 2]>(input),
                            "flat_u16_payload",
                        )?;
                        LwcData::Primitive(LwcPrimitive::FlatU16(FlatU16(input)))
                    }
                    ValKind::I32 => {
                        let input = flat_lwc_payload(input, len, 4, "LWC flat i32 payload")?;
                        let input = persisted_lwc_layout(
                            layout::try_slice_from_bytes::<[u8; 4]>(input),
                            "flat_i32_payload",
                        )?;
                        LwcData::Primitive(LwcPrimitive::FlatI32(FlatI32(input)))
                    }
                    ValKind::U32 => {
                        let input = flat_lwc_payload(input, len, 4, "LWC flat u32 payload")?;
                        let input = persisted_lwc_layout(
                            layout::try_slice_from_bytes::<[u8; 4]>(input),
                            "flat_u32_payload",
                        )?;
                        LwcData::Primitive(LwcPrimitive::FlatU32(FlatU32(input)))
                    }
                    ValKind::F32 => {
                        let input = flat_lwc_payload(input, len, 4, "LWC flat f32 payload")?;
                        let input = persisted_lwc_layout(
                            layout::try_slice_from_bytes::<[u8; 4]>(input),
                            "flat_f32_payload",
                        )?;
                        LwcData::Primitive(LwcPrimitive::FlatF32(FlatF32(input)))
                    }
                    ValKind::I64 => {
                        let input = flat_lwc_payload(input, len, 8, "LWC flat i64 payload")?;
                        let input = persisted_lwc_layout(
                            layout::try_slice_from_bytes::<[u8; 8]>(input),
                            "flat_i64_payload",
                        )?;
                        LwcData::Primitive(LwcPrimitive::FlatI64(FlatI64(input)))
                    }
                    ValKind::U64 => {
                        let input = flat_lwc_payload(input, len, 8, "LWC flat u64 payload")?;
                        let input = persisted_lwc_layout(
                            layout::try_slice_from_bytes::<[u8; 8]>(input),
                            "flat_u64_payload",
                        )?;
                        LwcData::Primitive(LwcPrimitive::FlatU64(FlatU64(input)))
                    }
                    ValKind::F64 => {
                        let input = flat_lwc_payload(input, len, 8, "LWC flat f64 payload")?;
                        let input = persisted_lwc_layout(
                            layout::try_slice_from_bytes::<[u8; 8]>(input),
                            "flat_f64_payload",
                        )?;
                        LwcData::Primitive(LwcPrimitive::FlatF64(FlatF64(input)))
                    }
                    ValKind::VarByte => {
                        let offset_count = len.checked_add(1).ok_or_else(|| {
                            invalid_compressed_payload_report(
                                "LWC varbyte offset table length overflows usize",
                            )
                        })?;
                        let offset_bytes_len = checked_lwc_payload_len(
                            offset_count,
                            mem::size_of::<u32>(),
                            "LWC varbyte offset table",
                        )?;
                        if input.len() < offset_bytes_len {
                            return Err(invalid_compressed_payload_report(
                                "LWC varbyte payload is shorter than its offset table",
                            ));
                        }
                        let (offset_bytes, data) = input.split_at(offset_bytes_len);
                        let offsets = persisted_lwc_layout(
                            layout::try_slice_from_bytes::<[u8; 4]>(offset_bytes),
                            "varbyte_offsets",
                        )?;
                        LwcData::Bytes(LwcBytes { offsets, data })
                    }
                }
            }
            LwcCode::ForBitpacking => {
                // see `ForBitpackingDeser` for reference.
                let (n_bits, input) = read_u8(input)?;
                let (len, input) = read_le_u64(input)?;
                let len = lwc_payload_len_from_u64(len, "LWC FOR bitpacking payload")?;
                let n_bits = n_bits as usize;
                let p = match kind {
                    ValKind::I8 => {
                        let (min, data) = read_i8(input)?;
                        let data = for_bitpacking_lwc_payload(data, len, n_bits)?;
                        match n_bits {
                            1 => LwcPrimitive::ForBp1I8(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2I8(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4I8(ForBitpacking4 { len, min, data }),
                            _ => {
                                return Err(invalid_compressed_payload_report(format!(
                                    "unexpected packing bits: val_kind={kind:?}, n_bits={n_bits}"
                                )));
                            }
                        }
                    }
                    ValKind::U8 => {
                        let (min, data) = read_u8(input)?;
                        let data = for_bitpacking_lwc_payload(data, len, n_bits)?;
                        match n_bits {
                            1 => LwcPrimitive::ForBp1U8(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2U8(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4U8(ForBitpacking4 { len, min, data }),
                            _ => {
                                return Err(invalid_compressed_payload_report(format!(
                                    "unexpected packing bits: val_kind={kind:?}, n_bits={n_bits}"
                                )));
                            }
                        }
                    }
                    ValKind::I16 => {
                        let (min, data) = read_le_i16(input)?;
                        let data = for_bitpacking_lwc_payload(data, len, n_bits)?;
                        match n_bits {
                            1 => LwcPrimitive::ForBp1I16(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2I16(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4I16(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8I16(ForBitpacking8 { min, data }),
                            _ => {
                                return Err(invalid_compressed_payload_report(format!(
                                    "unexpected packing bits: val_kind={kind:?}, n_bits={n_bits}"
                                )));
                            }
                        }
                    }
                    ValKind::U16 => {
                        let (min, data) = read_le_u16(input)?;
                        let data = for_bitpacking_lwc_payload(data, len, n_bits)?;
                        match n_bits {
                            1 => LwcPrimitive::ForBp1U16(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2U16(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4U16(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8U16(ForBitpacking8 { min, data }),
                            _ => {
                                return Err(invalid_compressed_payload_report(format!(
                                    "unexpected packing bits: val_kind={kind:?}, n_bits={n_bits}"
                                )));
                            }
                        }
                    }
                    ValKind::I32 => {
                        let (min, data) = read_le_i32(input)?;
                        let data = for_bitpacking_lwc_payload(data, len, n_bits)?;
                        match n_bits {
                            1 => LwcPrimitive::ForBp1I32(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2I32(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4I32(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8I32(ForBitpacking8 { min, data }),
                            16 => {
                                let data = persisted_lwc_layout(
                                    layout::try_slice_from_bytes::<[u8; 2]>(data),
                                    "for_bitpacking_i32_u16_units",
                                )?;
                                LwcPrimitive::ForBp16I32(ForBitpacking16 { min, data })
                            }
                            _ => {
                                return Err(invalid_compressed_payload_report(format!(
                                    "unexpected packing bits: val_kind={kind:?}, n_bits={n_bits}"
                                )));
                            }
                        }
                    }
                    ValKind::U32 => {
                        let (min, data) = read_le_u32(input)?;
                        let data = for_bitpacking_lwc_payload(data, len, n_bits)?;
                        match n_bits {
                            1 => LwcPrimitive::ForBp1U32(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2U32(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4U32(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8U32(ForBitpacking8 { min, data }),
                            16 => {
                                let data = persisted_lwc_layout(
                                    layout::try_slice_from_bytes::<[u8; 2]>(data),
                                    "for_bitpacking_u32_u16_units",
                                )?;
                                LwcPrimitive::ForBp16U32(ForBitpacking16 { min, data })
                            }
                            _ => {
                                return Err(invalid_compressed_payload_report(format!(
                                    "unexpected packing bits: val_kind={kind:?}, n_bits={n_bits}"
                                )));
                            }
                        }
                    }
                    ValKind::I64 => {
                        let (min, data) = read_le_i64(input)?;
                        let data = for_bitpacking_lwc_payload(data, len, n_bits)?;
                        match n_bits {
                            1 => LwcPrimitive::ForBp1I64(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2I64(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4I64(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8I64(ForBitpacking8 { min, data }),
                            16 => {
                                let data = persisted_lwc_layout(
                                    layout::try_slice_from_bytes::<[u8; 2]>(data),
                                    "for_bitpacking_i64_u16_units",
                                )?;
                                LwcPrimitive::ForBp16I64(ForBitpacking16 { min, data })
                            }
                            32 => {
                                let data = persisted_lwc_layout(
                                    layout::try_slice_from_bytes::<[u8; 4]>(data),
                                    "for_bitpacking_i64_u32_units",
                                )?;
                                LwcPrimitive::ForBp32I64(ForBitpacking32 { min, data })
                            }
                            _ => {
                                return Err(invalid_compressed_payload_report(format!(
                                    "unexpected packing bits: val_kind={kind:?}, n_bits={n_bits}"
                                )));
                            }
                        }
                    }
                    ValKind::U64 => {
                        let (min, data) = read_le_u64(input)?;
                        let data = for_bitpacking_lwc_payload(data, len, n_bits)?;
                        match n_bits {
                            1 => LwcPrimitive::ForBp1U64(ForBitpacking1 { len, min, data }),
                            2 => LwcPrimitive::ForBp2U64(ForBitpacking2 { len, min, data }),
                            4 => LwcPrimitive::ForBp4U64(ForBitpacking4 { len, min, data }),
                            8 => LwcPrimitive::ForBp8U64(ForBitpacking8 { min, data }),
                            16 => {
                                let data = persisted_lwc_layout(
                                    layout::try_slice_from_bytes::<[u8; 2]>(data),
                                    "for_bitpacking_u64_u16_units",
                                )?;
                                LwcPrimitive::ForBp16U64(ForBitpacking16 { min, data })
                            }
                            32 => {
                                let data = persisted_lwc_layout(
                                    layout::try_slice_from_bytes::<[u8; 4]>(data),
                                    "for_bitpacking_u64_u32_units",
                                )?;
                                LwcPrimitive::ForBp32U64(ForBitpacking32 { min, data })
                            }
                            _ => {
                                // any other number of bits are not supported.
                                return Err(invalid_compressed_payload_report(format!(
                                    "unexpected packing bits: val_kind={kind:?}, n_bits={n_bits}"
                                )));
                            }
                        }
                    }
                    ValKind::F32 | ValKind::F64 | ValKind::VarByte => {
                        return Err(invalid_compressed_payload_report(format!(
                            "unexpected type: val_kind={kind:?}"
                        )));
                    }
                };
                LwcData::Primitive(p)
            }
        };
        Ok(res)
    }

    /// Returns the length of Lwc Data.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved len"))]
    pub(crate) fn len(&self) -> usize {
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
    pub(crate) fn value(&self, idx: usize) -> Option<Val> {
        match self {
            LwcData::Primitive(p) => p.value(idx),
            LwcData::Bytes(b) => b.value(idx).map(Val::VarByte),
        }
    }
}

/// Abstract of primitive data which are lightweight compressed.
/// Provides methods for random access and batch access.
pub(crate) trait LwcPrimitiveData {
    type Value;
    type Iter: Iterator<Item = Self::Value>;

    /// Returns total number of values.
    fn len(&self) -> usize;

    /// Returns whether this data is empty.
    #[inline]
    #[expect(dead_code, reason = "reserved is_empty")]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns value at given position.
    /// None if index is out of range.
    fn value(&self, idx: usize) -> Option<Self::Value>;

    /// Extend all values to target collection.
    #[expect(dead_code, reason = "reserved extend_to")]
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E);

    /// Returns the iterator over all values.
    fn iter(&self) -> Self::Iter;
}

/// Serialized LWC compression code stored at the start of each column payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum LwcCode {
    Flat = 1,
    // Bitpacking = 2,
    /// See `ForBitpackingSer` for serialization details.
    ForBitpacking = 3,
    // todo: dict, fsst.
}

impl LwcCode {
    /// Decodes one persisted compression tag without converting to the crate error.
    #[inline]
    fn decode(value: u8) -> DataIntegrityResult<Self> {
        match value {
            1 => Ok(LwcCode::Flat),
            3 => Ok(LwcCode::ForBitpacking),
            _ => Err(invalid_compressed_payload_report(format!(
                "invalid LWC code {value}"
            ))),
        }
    }
}

/// Borrowed primitive LWC payload reader.
pub(crate) enum LwcPrimitive<'a> {
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
    #[expect(dead_code, reason = "reserved Bytes")]
    Bytes(LwcBytes<'a>),
}

impl LwcPrimitive<'_> {
    /// Returns the decoded value at `idx`.
    #[inline]
    pub(crate) fn value(&self, idx: usize) -> Option<Val> {
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

/// Serializer for primitive LWC column payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LwcPrimitiveSer<'a> {
    FlatI8(&'a [i8]),
    FlatU8(&'a [u8]),
    FlatI16(Cow<'a, [[u8; 2]]>),
    FlatU16(Cow<'a, [[u8; 2]]>),
    FlatI32(Cow<'a, [[u8; 4]]>),
    FlatU32(Cow<'a, [[u8; 4]]>),
    FlatF32(Cow<'a, [[u8; 4]]>),
    FlatI64(Cow<'a, [[u8; 8]]>),
    FlatU64(Cow<'a, [[u8; 8]]>),
    FlatF64(Cow<'a, [[u8; 8]]>),
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
    /// Creates an i8 serializer, choosing bitpacking when it is smaller.
    #[inline]
    pub(crate) fn new_i8(input: &'a [i8]) -> Self {
        if input.is_empty() {
            return LwcPrimitiveSer::FlatI8(input);
        }
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpI8(fbp),
            None => LwcPrimitiveSer::FlatI8(input),
        }
    }

    /// Creates a u8 serializer, choosing bitpacking when it is smaller.
    #[inline]
    pub(crate) fn new_u8(input: &'a [u8]) -> Self {
        if input.is_empty() {
            return LwcPrimitiveSer::FlatU8(input);
        }
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpU8(fbp),
            None => LwcPrimitiveSer::FlatU8(input),
        }
    }

    /// Creates a u64 serializer, choosing bitpacking when it is smaller.
    #[inline]
    pub(crate) fn new_u64(input: &'a [u64]) -> Self {
        if input.is_empty() {
            return LwcPrimitiveSer::FlatU64(flat_u64_bytes(input));
        }
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpU64(fbp),
            None => LwcPrimitiveSer::FlatU64(flat_u64_bytes(input)),
        }
    }

    /// Creates an i16 serializer, choosing bitpacking when it is smaller.
    #[inline]
    pub(crate) fn new_i16(input: &'a [i16]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpI16(fbp),
            None => LwcPrimitiveSer::FlatI16(flat_i16_bytes(input)),
        }
    }

    /// Creates a u16 serializer, choosing bitpacking when it is smaller.
    #[inline]
    pub(crate) fn new_u16(input: &'a [u16]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpU16(fbp),
            None => LwcPrimitiveSer::FlatU16(flat_u16_bytes(input)),
        }
    }

    /// Creates an i32 serializer, choosing bitpacking when it is smaller.
    #[inline]
    pub(crate) fn new_i32(input: &'a [i32]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpI32(fbp),
            None => LwcPrimitiveSer::FlatI32(flat_i32_bytes(input)),
        }
    }

    /// Creates a u32 serializer, choosing bitpacking when it is smaller.
    #[inline]
    pub(crate) fn new_u32(input: &'a [u32]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpU32(fbp),
            None => LwcPrimitiveSer::FlatU32(flat_u32_bytes(input)),
        }
    }

    /// Creates an i64 serializer, choosing bitpacking when it is smaller.
    #[inline]
    pub(crate) fn new_i64(input: &'a [i64]) -> Self {
        match ForBitpackingSer::new(input) {
            Some(fbp) => LwcPrimitiveSer::ForBpI64(fbp),
            None => LwcPrimitiveSer::FlatI64(flat_i64_bytes(input)),
        }
    }

    /// Creates an f32 flat serializer.
    #[inline]
    pub(crate) fn new_f32(input: &'a [f32]) -> Self {
        LwcPrimitiveSer::FlatF32(flat_f32_bytes(input))
    }

    /// Creates an f64 flat serializer.
    #[inline]
    pub(crate) fn new_f64(input: &'a [f64]) -> Self {
        LwcPrimitiveSer::FlatF64(flat_f64_bytes(input))
    }

    /// Creates a borrowed varbyte flat serializer.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved new_bytes"))]
    pub(crate) fn new_bytes(offsets: &[u32], data: &[u8]) -> InternalResult<Self> {
        if offsets.is_empty() || offsets[0] != 0 {
            return Err(lwc_builder_misuse(
                "LWC bytes offsets must be non-empty and start at zero",
            ));
        }
        Ok(LwcPrimitiveSer::Bytes(LwcBytesSer {
            offsets: offsets.to_vec(),
            data: data.to_vec(),
        }))
    }

    /// Creates an owned varbyte flat serializer.
    #[inline]
    pub(crate) fn new_bytes_owned(offsets: Vec<u32>, data: Vec<u8>) -> InternalResult<Self> {
        if offsets.is_empty() || offsets[0] != 0 {
            return Err(lwc_builder_misuse(
                "LWC bytes offsets must be non-empty and start at zero",
            ));
        }
        Ok(LwcPrimitiveSer::Bytes(LwcBytesSer { offsets, data }))
    }

    /// Returns the wire compression code for this serializer.
    #[inline]
    pub(crate) fn code(&self) -> LwcCode {
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
    fn ser_len(&self) -> usize {
        mem::size_of::<u8>()
            + match self {
                LwcPrimitiveSer::FlatI8(f) => f.ser_len(),
                LwcPrimitiveSer::FlatU8(f) => f.ser_len(),
                LwcPrimitiveSer::FlatI16(f) => f.ser_len(),
                LwcPrimitiveSer::FlatU16(f) => f.ser_len(),
                LwcPrimitiveSer::FlatI32(f) => f.ser_len(),
                LwcPrimitiveSer::FlatU32(f) => f.ser_len(),
                LwcPrimitiveSer::FlatF32(f) => f.ser_len(),
                LwcPrimitiveSer::FlatI64(f) => f.ser_len(),
                LwcPrimitiveSer::FlatU64(f) => f.ser_len(),
                LwcPrimitiveSer::FlatF64(f) => f.ser_len(),
                LwcPrimitiveSer::ForBpI8(b) => b.ser_len(),
                LwcPrimitiveSer::ForBpU8(b) => b.ser_len(),
                LwcPrimitiveSer::ForBpI16(b) => b.ser_len(),
                LwcPrimitiveSer::ForBpU16(b) => b.ser_len(),
                LwcPrimitiveSer::ForBpI32(b) => b.ser_len(),
                LwcPrimitiveSer::ForBpU32(b) => b.ser_len(),
                LwcPrimitiveSer::ForBpI64(b) => b.ser_len(),
                LwcPrimitiveSer::ForBpU64(b) => b.ser_len(),
                LwcPrimitiveSer::Bytes(b) => b.ser_len(),
            }
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u8(start_idx, self.code() as u8);
        match self {
            LwcPrimitiveSer::FlatI8(f) => f.ser(out, idx),
            LwcPrimitiveSer::FlatU8(f) => f.ser(out, idx),
            LwcPrimitiveSer::FlatI16(f) => f.ser(out, idx),
            LwcPrimitiveSer::FlatU16(f) => f.ser(out, idx),
            LwcPrimitiveSer::FlatI32(f) => f.ser(out, idx),
            LwcPrimitiveSer::FlatU32(f) => f.ser(out, idx),
            LwcPrimitiveSer::FlatF32(f) => f.ser(out, idx),
            LwcPrimitiveSer::FlatI64(f) => f.ser(out, idx),
            LwcPrimitiveSer::FlatU64(f) => f.ser(out, idx),
            LwcPrimitiveSer::FlatF64(f) => f.ser(out, idx),
            LwcPrimitiveSer::ForBpI8(b) => b.ser(out, idx),
            LwcPrimitiveSer::ForBpU8(b) => b.ser(out, idx),
            LwcPrimitiveSer::ForBpI16(b) => b.ser(out, idx),
            LwcPrimitiveSer::ForBpU16(b) => b.ser(out, idx),
            LwcPrimitiveSer::ForBpI32(b) => b.ser(out, idx),
            LwcPrimitiveSer::ForBpU32(b) => b.ser(out, idx),
            LwcPrimitiveSer::ForBpI64(b) => b.ser(out, idx),
            LwcPrimitiveSer::ForBpU64(b) => b.ser(out, idx),
            LwcPrimitiveSer::Bytes(b) => b.ser(out, idx),
        }
    }
}

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

#[derive(Clone, Copy)]
struct LwcColumnSnapshot {
    initialized: bool,
    min_i64: i64,
    max_i64: i64,
    min_u64: u64,
    max_u64: u64,
}

struct LwcSnapshot {
    row_count: usize,
    row_ids_len: usize,
    stats: Vec<LwcColumnSnapshot>,
}

/// Builder that accumulates row-page data into one persisted LWC block image.
pub(crate) struct LwcBuilder<'a> {
    col_layout: &'a TableColumnLayout,
    buffer: ScanBuffer,
    row_ids: Vec<RowID>,
    stats: Vec<LwcColumnStats>,
}

impl<'a> LwcBuilder<'a> {
    /// Creates an empty LWC builder for the provided column layout.
    pub(crate) fn new(col_layout: &'a TableColumnLayout) -> Self {
        let scan_set: Vec<_> = (0..col_layout.col_count()).collect();
        let stats = (0..col_layout.col_count())
            .map(|_| LwcColumnStats::new())
            .collect();
        let buffer = ScanBuffer::new(col_layout, &scan_set);
        LwcBuilder {
            col_layout,
            buffer,
            row_ids: Vec::new(),
            stats,
        }
    }

    /// Returns whether no rows have been appended.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the number of rows currently buffered.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved row_count"))]
    pub(crate) fn row_count(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the row ids represented by the buffered rows.
    #[inline]
    pub(crate) fn row_ids(&self) -> &[RowID] {
        &self.row_ids
    }

    /// Appends one validated decoded row if the block still fits.
    ///
    /// Returns `false` and restores the previous builder state when the row
    /// would exceed the block payload capacity.
    ///
    /// # Panics
    ///
    /// Panics when the row was not validated against the layout supplied to
    /// [`Self::new`].
    pub(crate) fn append_row_values(&mut self, row_id: RowID, vals: &[Val]) -> bool {
        let snapshot = self.snapshot_state();
        if self.append_row_values_inner(row_id, vals) {
            true
        } else {
            self.rollback(snapshot);
            false
        }
    }

    /// Appends rows described by `view` if the block still fits.
    ///
    /// Returns `false` and restores the previous builder state when the view
    /// would exceed the block payload capacity.
    ///
    /// # Panics
    ///
    /// Panics when the page view was not constructed from the layout supplied
    /// to [`Self::new`].
    pub(crate) fn append_view(
        &mut self,
        view: PageVectorView<'_, '_>,
        start_row_id: RowID,
    ) -> bool {
        let snapshot = self.snapshot_state();
        if self.append_view_inner(view, start_row_id) {
            true
        } else {
            self.rollback(snapshot);
            false
        }
    }

    fn append_view_inner(&mut self, view: PageVectorView<'_, '_>, start_row_id: RowID) -> bool {
        let mut new_row_ids = Vec::with_capacity(view.rows_non_deleted());
        for (start_idx, end_idx) in view.range_non_deleted() {
            for idx in start_idx..end_idx {
                new_row_ids.push(start_row_id + idx as u64);
            }
        }
        self.scan_page_stats(&view, &new_row_ids);
        self.buffer.scan(view);
        self.row_ids.extend(new_row_ids);
        self.estimate_size() <= LWC_BLOCK_PAYLOAD_SIZE
    }

    fn append_row_values_inner(&mut self, row_id: RowID, vals: &[Val]) -> bool {
        self.scan_row_value_stats(vals);
        self.buffer.append_row_values(self.col_layout, vals);
        self.row_ids.push(row_id);
        self.estimate_size() <= LWC_BLOCK_PAYLOAD_SIZE
    }

    /// Builds a persisted LWC block with the supplied row-shape fingerprint.
    pub(crate) fn build(&self, row_shape_fingerprint: u128) -> InternalResult<DirectBuf> {
        if self.buffer.is_empty() {
            return Err(Report::new(InternalError::LwcBuilderMisuse)
                .attach("cannot build an empty LWC block"));
        }
        let row_count = self.buffer.len();
        if row_count > u16::MAX as usize {
            return Err(lwc_block_encoding_contract().attach(format!(
                "field=row_count, actual={row_count}, maximum={}",
                u16::MAX
            )));
        }
        let mut column_payloads = Vec::with_capacity(self.col_layout.col_count());
        let mut col_offsets = Vec::with_capacity(self.col_layout.col_count());
        let mut offset = mem::size_of::<u16>() * self.col_layout.col_count();

        for col_idx in 0..self.col_layout.col_count() {
            let column = self.buffer.column(col_idx).unwrap_or_else(|| {
                panic!(
                    "LWC builder scan buffer must contain column {col_idx} from its table layout"
                )
            });
            let mut data = Vec::new();
            if let Some(bitmap) = column.null_bitmap {
                let bytes = bitmap_to_bytes(bitmap, row_count);
                let bitmap_ser = LwcNullBitmapSer::new(&bytes)?;
                let mut tmp = vec![0u8; bitmap_ser.ser_len()];
                bitmap_ser.ser(&mut tmp[..], 0);
                data.extend_from_slice(&tmp);
            }
            let payload = match column.values {
                ScanColumnValues::I8(vals) => {
                    let ser = LwcPrimitiveSer::new_i8(vals);
                    serialize_primitive(&ser)
                }
                ScanColumnValues::U8(vals) => {
                    let ser = LwcPrimitiveSer::new_u8(vals);
                    serialize_primitive(&ser)
                }
                ScanColumnValues::I16(vals) => {
                    let ser = LwcPrimitiveSer::new_i16(vals);
                    serialize_primitive(&ser)
                }
                ScanColumnValues::U16(vals) => {
                    let ser = LwcPrimitiveSer::new_u16(vals);
                    serialize_primitive(&ser)
                }
                ScanColumnValues::I32(vals) => {
                    let ser = LwcPrimitiveSer::new_i32(vals);
                    serialize_primitive(&ser)
                }
                ScanColumnValues::U32(vals) => {
                    let ser = LwcPrimitiveSer::new_u32(vals);
                    serialize_primitive(&ser)
                }
                ScanColumnValues::F32(vals) => {
                    let ser = LwcPrimitiveSer::new_f32(vals);
                    serialize_primitive(&ser)
                }
                ScanColumnValues::I64(vals) => {
                    let ser = LwcPrimitiveSer::new_i64(vals);
                    serialize_primitive(&ser)
                }
                ScanColumnValues::U64(vals) => {
                    let ser = LwcPrimitiveSer::new_u64(vals);
                    serialize_primitive(&ser)
                }
                ScanColumnValues::F64(vals) => {
                    let ser = LwcPrimitiveSer::new_f64(vals);
                    serialize_primitive(&ser)
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
                    serialize_primitive(&ser)
                }
            };
            data.extend_from_slice(&payload);
            offset += data.len();
            if offset > u16::MAX as usize {
                return Err(lwc_block_encoding_contract().attach(format!(
                    "field=column_end_offset, column_no={col_idx}, actual={offset}, maximum={}",
                    u16::MAX
                )));
            }
            col_offsets.push(offset as u16);
            column_payloads.push(data);
        }

        let header = LwcBlockHeader::new(
            row_shape_fingerprint,
            row_count as u16,
            self.col_layout.col_count() as u16,
            0,
        );

        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let payload_start = write_block_header(buf.data_mut(), LWC_BLOCK_SPEC);
        let payload_end = payload_start + LWC_BLOCK_PAYLOAD_SIZE;
        let page = LwcBlock::from_bytes_mut(&mut buf.data_mut()[payload_start..payload_end]);
        page.header = header;
        let mut body_idx = 0;
        for offset in col_offsets {
            body_idx = offset.ser(&mut page.body[..], body_idx);
        }
        for payload in column_payloads {
            let end = body_idx + payload.len();
            page.body[body_idx..end].copy_from_slice(&payload);
            body_idx = end;
        }
        write_block_checksum(buf.data_mut());
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
        for (stat, snap) in self.stats.iter_mut().zip(snapshot.stats) {
            stat.restore(snap);
        }
    }

    fn scan_page_stats(&mut self, view: &PageVectorView<'_, '_>, row_ids: &[RowID]) {
        assert_eq!(
            row_ids.len(),
            view.rows_non_deleted(),
            "LWC page statistics row ids must match the page view's visible rows"
        );
        if row_ids.is_empty() {
            return;
        }
        for col_idx in 0..self.col_layout.col_count() {
            let (null_bitmap, values) = view.col(col_idx);
            let null_bitmap = null_bitmap.as_deref();
            match values {
                ValArrayRef::I8(vals) => {
                    self.update_stats_i64(
                        col_idx,
                        null_bitmap,
                        vals,
                        view.range_non_deleted(),
                        |v| v as i64,
                    );
                }
                ValArrayRef::U8(vals) => {
                    self.update_stats_u64(
                        col_idx,
                        null_bitmap,
                        vals,
                        view.range_non_deleted(),
                        |v| v as u64,
                    );
                }
                ValArrayRef::I16(vals) => {
                    self.update_stats_i64(
                        col_idx,
                        null_bitmap,
                        vals,
                        view.range_non_deleted(),
                        |v| v.get() as i64,
                    );
                }
                ValArrayRef::U16(vals) => {
                    self.update_stats_u64(
                        col_idx,
                        null_bitmap,
                        vals,
                        view.range_non_deleted(),
                        |v| v.get() as u64,
                    );
                }
                ValArrayRef::I32(vals) => {
                    self.update_stats_i64(
                        col_idx,
                        null_bitmap,
                        vals,
                        view.range_non_deleted(),
                        |v| v.get() as i64,
                    );
                }
                ValArrayRef::U32(vals) => {
                    self.update_stats_u64(
                        col_idx,
                        null_bitmap,
                        vals,
                        view.range_non_deleted(),
                        |v| v.get() as u64,
                    );
                }
                ValArrayRef::I64(vals) => {
                    self.update_stats_i64(
                        col_idx,
                        null_bitmap,
                        vals,
                        view.range_non_deleted(),
                        |v| v.get(),
                    );
                }
                ValArrayRef::U64(vals) => {
                    self.update_stats_u64(
                        col_idx,
                        null_bitmap,
                        vals,
                        view.range_non_deleted(),
                        |v| v.get(),
                    );
                }
                ValArrayRef::F32(_) | ValArrayRef::F64(_) | ValArrayRef::VarByte(_, _) => {}
            }
        }
    }

    fn scan_row_value_stats(&mut self, vals: &[Val]) {
        assert_eq!(
            vals.len(),
            self.col_layout.col_count(),
            "LWC statistics require a complete row validated against the builder layout"
        );
        for (col_idx, val) in vals.iter().enumerate() {
            if val.is_null() {
                continue;
            }
            match (self.col_layout.val_kind(col_idx), val) {
                (ValKind::I8, Val::I8(value)) => self.stats[col_idx].update_i64(*value as i64),
                (ValKind::U8, Val::U8(value)) => self.stats[col_idx].update_u64(*value as u64),
                (ValKind::I16, Val::I16(value)) => self.stats[col_idx].update_i64(*value as i64),
                (ValKind::U16, Val::U16(value)) => self.stats[col_idx].update_u64(*value as u64),
                (ValKind::I32, Val::I32(value)) => self.stats[col_idx].update_i64(*value as i64),
                (ValKind::U32, Val::U32(value)) => self.stats[col_idx].update_u64(*value as u64),
                (ValKind::I64, Val::I64(value)) => self.stats[col_idx].update_i64(*value),
                (ValKind::U64, Val::U64(value)) => self.stats[col_idx].update_u64(*value),
                (ValKind::F32, Val::F32(_))
                | (ValKind::F64, Val::F64(_))
                | (ValKind::VarByte, Val::VarByte(_)) => {}
                _ => unreachable!(
                    "LWC statistics value kind for column {col_idx} must match the validated row layout"
                ),
            }
        }
    }

    fn update_stats_i64<T: Copy, R: Iterator<Item = (usize, usize)>>(
        &mut self,
        col_idx: usize,
        null_bitmap: Option<&[u64]>,
        values: &[T],
        ranges: R,
        decode: impl Fn(T) -> i64,
    ) {
        for (start_idx, end_idx) in ranges {
            for idx in start_idx..end_idx {
                if null_bitmap.map(|bm| bm.bitmap_get(idx)).unwrap_or(false) {
                    continue;
                }
                let value = values.get(idx).unwrap_or_else(|| {
                    panic!(
                        "LWC page statistics column {col_idx} is missing visible row index {idx}"
                    )
                });
                self.stats[col_idx].update_i64(decode(*value));
            }
        }
    }

    fn update_stats_u64<T: Copy, R: Iterator<Item = (usize, usize)>>(
        &mut self,
        col_idx: usize,
        null_bitmap: Option<&[u64]>,
        values: &[T],
        ranges: R,
        decode: impl Fn(T) -> u64,
    ) {
        for (start_idx, end_idx) in ranges {
            for idx in start_idx..end_idx {
                if null_bitmap.map(|bm| bm.bitmap_get(idx)).unwrap_or(false) {
                    continue;
                }
                let value = values.get(idx).unwrap_or_else(|| {
                    panic!(
                        "LWC page statistics column {col_idx} is missing visible row index {idx}"
                    )
                });
                self.stats[col_idx].update_u64(decode(*value));
            }
        }
    }

    fn estimate_size(&self) -> usize {
        let row_count = self.buffer.len();
        let mut total = LWC_BLOCK_HEADER_SIZE;
        total += mem::size_of::<u16>() * self.col_layout.col_count();
        total += estimate_columns_size(self.col_layout, &self.buffer, &self.stats, row_count);
        total
    }
}

/// Flat borrowed i8 payload.
pub(crate) struct FlatI8<'a>(&'a [u8]);

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

/// Iterator over a flat borrowed i8 payload.
pub(crate) struct FlatI8Iter<'a>(Iter<'a, u8>);

impl<'a> Iterator for FlatI8Iter<'a> {
    type Item = i8;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|v| *v as i8)
    }
}

/// Flat borrowed u8 payload.
pub(crate) struct FlatU8<'a>(&'a [u8]);

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

/// Iterator over a flat borrowed u8 payload.
pub(crate) struct FlatU8Iter<'a>(Iter<'a, u8>);

impl<'a> Iterator for FlatU8Iter<'a> {
    type Item = u8;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().copied()
    }
}

/// Borrowed flat varbyte payload.
pub(crate) struct LwcBytes<'a> {
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

    /// Returns the varbyte value at `idx`.
    #[inline]
    pub(crate) fn value_at(&self, idx: usize) -> Option<MemVar> {
        self.slice(idx).map(MemVar::from)
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

/// Iterator over borrowed flat varbyte values.
pub(crate) struct LwcBytesIter<'a> {
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

/// Owned serializer for flat varbyte payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LwcBytesSer {
    offsets: Vec<u32>,
    data: Vec<u8>,
}

impl Ser<'_> for LwcBytesSer {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>() + self.offsets.len() * mem::size_of::<u32>() + self.data.len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        debug_assert!(!self.offsets.is_empty());
        let mut idx = out.ser_u64(start_idx, (self.offsets.len() - 1) as u64);
        for off in self.offsets.iter().copied() {
            idx = out.ser_u32(idx, off);
        }
        out.ser_byte_slice(idx, &self.data)
    }
}

/// Borrowed null bitmap payload.
#[derive(Debug)]
pub(crate) struct LwcNullBitmap<'a> {
    bytes: &'a [u8],
}

impl<'a> LwcNullBitmap<'a> {
    /// Parses a length-prefixed null bitmap and returns the remaining payload.
    #[inline]
    pub(crate) fn from_bytes(input: &'a [u8]) -> DataIntegrityResult<(Self, &'a [u8])> {
        let (len, input) = read_le_u16(input)?;
        let len = len as usize;
        if input.len() < len {
            return Err(invalid_compressed_payload_report(
                "LWC null bitmap payload is shorter than declared length",
            ));
        }
        let (bytes, rest) = input.split_at(len);
        Ok((LwcNullBitmap { bytes }, rest))
    }

    /// Returns whether `row_idx` is marked null.
    #[inline]
    pub(crate) fn is_null(&self, row_idx: usize) -> bool {
        let byte_idx = row_idx / 8;
        if byte_idx >= self.bytes.len() {
            return false;
        }
        let bit_mask = 1 << (row_idx % 8);
        self.bytes[byte_idx] & bit_mask != 0
    }

    /// Returns the bitmap length in bytes.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns the raw bitmap bytes.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

/// Serializer for length-prefixed null bitmap payloads.
pub(crate) struct LwcNullBitmapSer<'a> {
    bytes: &'a [u8],
}

impl<'a> LwcNullBitmapSer<'a> {
    /// Creates a null bitmap serializer after validating the length prefix fits.
    #[inline]
    pub(crate) fn new(bytes: &'a [u8]) -> InternalResult<Self> {
        if bytes.len() > u16::MAX as usize {
            return Err(lwc_block_encoding_contract().attach(format!(
                "field=null_bitmap_byte_length, actual={}, maximum={}",
                bytes.len(),
                u16::MAX
            )));
        }
        Ok(LwcNullBitmapSer { bytes })
    }
}

impl Ser<'_> for LwcNullBitmapSer<'_> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u16>() + self.bytes.len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u16(start_idx, self.bytes.len() as u16);
        out.ser_byte_slice(idx, self.bytes)
    }
}

macro_rules! impl_lwc_flat {
    ($t:ident, $v:ty, $unit:ty, $it:ident) => {
        pub(crate) struct $t<'a>(&'a [$unit]);

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

        pub(crate) struct $it<'a>(std::slice::Iter<'a, $unit>);

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

#[inline]
fn invalid_compressed_payload_report(message: impl Into<String>) -> Report<DataIntegrityError> {
    Report::new(DataIntegrityError::InvalidPayload).attach(message.into())
}

#[inline]
fn persisted_lwc_layout<T>(
    result: layout::LayoutResult<T>,
    field: &'static str,
) -> DataIntegrityResult<T> {
    result
        .change_context(DataIntegrityError::InvalidPayload)
        .attach_with(|| format!("format=lwc_column_payload, field={field}"))
}

#[inline]
fn lwc_block_encoding_contract() -> Report<InternalError> {
    Report::new(InternalError::LwcBlockEncodingContract)
}

#[inline]
fn lwc_builder_misuse(message: impl Into<String>) -> Report<InternalError> {
    Report::new(InternalError::LwcBuilderMisuse).attach(message.into())
}

#[inline]
fn lwc_payload_len_from_u64(len: u64, payload: &str) -> DataIntegrityResult<usize> {
    usize::try_from(len).map_err(|_| {
        invalid_compressed_payload_report(format!("{payload} length exceeds usize::MAX"))
    })
}

#[inline]
fn checked_lwc_payload_len(
    len: usize,
    unit_len: usize,
    payload: &str,
) -> DataIntegrityResult<usize> {
    len.checked_mul(unit_len).ok_or_else(|| {
        invalid_compressed_payload_report(format!("{payload} length overflows usize"))
    })
}

#[inline]
fn expect_exact_lwc_payload_len(
    actual_len: usize,
    expected_len: usize,
    payload: &str,
) -> DataIntegrityResult<()> {
    if actual_len != expected_len {
        return Err(invalid_compressed_payload_report(format!(
            "{payload} length mismatch: expected {expected_len}, actual {actual_len}"
        )));
    }
    Ok(())
}

#[inline]
fn flat_lwc_payload<'a>(
    input: &'a [u8],
    len: usize,
    unit_len: usize,
    payload: &str,
) -> DataIntegrityResult<&'a [u8]> {
    let expected_len = checked_lwc_payload_len(len, unit_len, payload)?;
    expect_exact_lwc_payload_len(input.len(), expected_len, payload)?;
    Ok(input)
}

#[inline]
fn for_bitpacking_lwc_payload(
    data: &[u8],
    len: usize,
    n_bits: usize,
) -> DataIntegrityResult<&[u8]> {
    let expected_bits = checked_lwc_payload_len(len, n_bits, "LWC FOR bitpacking payload")?;
    let expected_len = expected_bits.div_ceil(8);
    expect_exact_lwc_payload_len(data.len(), expected_len, "LWC FOR bitpacking payload")?;
    Ok(data)
}

#[inline]
fn flat_bytes_borrowed<'a, T, const N: usize>(input: &'a [T]) -> Cow<'a, [[u8; N]]>
where
    [T]: IntoBytes + Immutable,
{
    Cow::Borrowed(layout::slice_from_bytes::<[u8; N]>(input.as_bytes()))
}

#[cfg(target_endian = "little")]
#[inline]
fn flat_i16_bytes(input: &[i16]) -> Cow<'_, [[u8; 2]]> {
    flat_bytes_borrowed(input)
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn flat_i16_bytes(input: &[i16]) -> Cow<'_, [[u8; 2]]> {
    Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
}

#[cfg(target_endian = "little")]
#[inline]
fn flat_u16_bytes(input: &[u16]) -> Cow<'_, [[u8; 2]]> {
    flat_bytes_borrowed(input)
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn flat_u16_bytes(input: &[u16]) -> Cow<'_, [[u8; 2]]> {
    Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
}

#[cfg(target_endian = "little")]
#[inline]
fn flat_i32_bytes(input: &[i32]) -> Cow<'_, [[u8; 4]]> {
    flat_bytes_borrowed(input)
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn flat_i32_bytes(input: &[i32]) -> Cow<'_, [[u8; 4]]> {
    Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
}

#[cfg(target_endian = "little")]
#[inline]
fn flat_u32_bytes(input: &[u32]) -> Cow<'_, [[u8; 4]]> {
    flat_bytes_borrowed(input)
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn flat_u32_bytes(input: &[u32]) -> Cow<'_, [[u8; 4]]> {
    Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
}

#[cfg(target_endian = "little")]
#[inline]
fn flat_f32_bytes(input: &[f32]) -> Cow<'_, [[u8; 4]]> {
    flat_bytes_borrowed(input)
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn flat_f32_bytes(input: &[f32]) -> Cow<'_, [[u8; 4]]> {
    Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
}

#[cfg(target_endian = "little")]
#[inline]
fn flat_i64_bytes(input: &[i64]) -> Cow<'_, [[u8; 8]]> {
    flat_bytes_borrowed(input)
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn flat_i64_bytes(input: &[i64]) -> Cow<'_, [[u8; 8]]> {
    Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
}

#[cfg(target_endian = "little")]
#[inline]
fn flat_u64_bytes(input: &[u64]) -> Cow<'_, [[u8; 8]]> {
    flat_bytes_borrowed(input)
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn flat_u64_bytes(input: &[u64]) -> Cow<'_, [[u8; 8]]> {
    Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
}

#[cfg(target_endian = "little")]
#[inline]
fn flat_f64_bytes(input: &[f64]) -> Cow<'_, [[u8; 8]]> {
    flat_bytes_borrowed(input)
}

#[cfg(not(target_endian = "little"))]
#[inline]
fn flat_f64_bytes(input: &[f64]) -> Cow<'_, [[u8; 8]]> {
    Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
}

#[inline]
fn serialize_primitive<'a>(ser: &LwcPrimitiveSer<'a>) -> Vec<u8> {
    let mut bytes = vec![0u8; ser.ser_len()];
    ser.ser(&mut bytes[..], 0);
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

fn estimate_columns_size(
    col_layout: &TableColumnLayout,
    buffer: &ScanBuffer,
    stats: &[LwcColumnStats],
    row_count: usize,
) -> usize {
    let mut total = 0usize;
    assert_eq!(
        stats.len(),
        col_layout.col_count(),
        "LWC statistics columns must match the builder table layout"
    );
    for (col_idx, st) in stats.iter().enumerate() {
        let column = buffer
            .column(col_idx)
            .unwrap_or_else(|| {
                panic!(
                    "LWC size estimation scan buffer must contain column {col_idx} from its table layout"
                )
            });
        if column.null_bitmap.is_some() {
            total += mem::size_of::<u16>() + row_count.div_ceil(8);
        }
        total += estimate_column_payload(
            col_layout.val_kind(col_idx),
            &column.values,
            st,
            row_count,
            col_idx,
        );
    }
    total
}

fn estimate_column_payload(
    kind: ValKind,
    values: &ScanColumnValues<'_>,
    stats: &LwcColumnStats,
    row_count: usize,
    col_idx: usize,
) -> usize {
    match (kind, values) {
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
        _ => unreachable!(
            "LWC size estimation value kind for column {col_idx} must match its table layout"
        ),
    }
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

#[inline]
fn read_le_u64(input: &[u8]) -> DataIntegrityResult<(u64, &[u8])> {
    if input.len() < 8 {
        return Err(invalid_compressed_payload_report(
            "expected LWC little-endian u64",
        ));
    }
    let u: [u8; 8] = input[..8].try_into().unwrap();
    let v = u64::from_le_bytes(u);
    Ok((v, &input[8..]))
}

#[inline]
fn read_le_u32(input: &[u8]) -> DataIntegrityResult<(u32, &[u8])> {
    if input.len() < 4 {
        return Err(invalid_compressed_payload_report(
            "expected LWC little-endian u32",
        ));
    }
    let u: [u8; 4] = input[..4].try_into().unwrap();
    Ok((u32::from_le_bytes(u), &input[4..]))
}

#[inline]
fn read_le_i32(input: &[u8]) -> DataIntegrityResult<(i32, &[u8])> {
    if input.len() < 4 {
        return Err(invalid_compressed_payload_report(
            "expected LWC little-endian i32",
        ));
    }
    let u: [u8; 4] = input[..4].try_into().unwrap();
    Ok((i32::from_le_bytes(u), &input[4..]))
}

#[inline]
fn read_le_u16(input: &[u8]) -> DataIntegrityResult<(u16, &[u8])> {
    if input.len() < 2 {
        return Err(invalid_compressed_payload_report(
            "expected LWC little-endian u16",
        ));
    }
    let u: [u8; 2] = input[..2].try_into().unwrap();
    Ok((u16::from_le_bytes(u), &input[2..]))
}

#[inline]
fn read_le_i16(input: &[u8]) -> DataIntegrityResult<(i16, &[u8])> {
    if input.len() < 2 {
        return Err(invalid_compressed_payload_report(
            "expected LWC little-endian i16",
        ));
    }
    let u: [u8; 2] = input[..2].try_into().unwrap();
    Ok((i16::from_le_bytes(u), &input[2..]))
}

#[inline]
fn read_le_i64(input: &[u8]) -> DataIntegrityResult<(i64, &[u8])> {
    if input.len() < 8 {
        return Err(invalid_compressed_payload_report(
            "expected LWC little-endian i64",
        ));
    }
    let u: [u8; 8] = input[..8].try_into().unwrap();
    Ok((i64::from_le_bytes(u), &input[8..]))
}

#[inline]
fn read_u8(input: &[u8]) -> DataIntegrityResult<(u8, &[u8])> {
    if input.is_empty() {
        return Err(invalid_compressed_payload_report("expected LWC u8"));
    }
    let v = input[0];
    Ok((v, &input[1..]))
}

#[inline]
fn read_i8(input: &[u8]) -> DataIntegrityResult<(i8, &[u8])> {
    if input.is_empty() {
        return Err(invalid_compressed_payload_report("expected LWC i8"));
    }
    let v = input[0] as i8;
    Ok((v, &input[1..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::error::{DataIntegrityError, DataIntegrityResult, FileKind, InternalError};
    use crate::file::test_block_id;
    use crate::id::RowID;
    use crate::index::ColumnBlockEntryShape;
    use crate::io::IOBuf;
    use crate::row::{Delete, InsertRow, RowPage};
    use crate::value::{MemVar, Val};
    use ordered_float::OrderedFloat;

    fn row_shape_fingerprint_for(row_ids: &[RowID], start_row_id: u64, end_row_id: u64) -> u128 {
        ColumnBlockEntryShape::new(
            RowID::new(start_row_id),
            RowID::new(end_row_id),
            row_ids.to_vec(),
            Vec::new(),
        )
        .unwrap()
        .row_shape_fingerprint()
    }

    #[test]
    fn test_lwc_primitive_serde() {
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
            let mut res = vec![0u8; lwc_ser.ser_len()];
            let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
            let mut res = vec![0u8; lwc_ser.ser_len()];
            let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
            let mut res = vec![0u8; lwc_ser.ser_len()];
            let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
            let mut res = vec![0u8; lwc_ser.ser_len()];
            let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
            let mut res = vec![0u8; lwc_ser.ser_len()];
            let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
            let mut res = vec![0u8; lwc_ser.ser_len()];
            let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
        let mut res = vec![0u8; lwc_ser.ser_len()];
        let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
            let mut res = vec![0u8; lwc_ser.ser_len()];
            let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
            let mut res = vec![0u8; lwc_ser.ser_len()];
            let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
        let mut res = vec![0u8; lwc_ser.ser_len()];
        let ser_idx = lwc_ser.ser(&mut res[..], 0);
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
        let mut res = vec![0u8; lwc_ser.ser_len()];
        let ser_idx = lwc_ser.ser(&mut res[..], 0);
        assert_eq!(ser_idx, res.len());
        let lwc_data = LwcData::from_bytes(ValKind::VarByte, &res).unwrap();
        if let LwcData::Bytes(b) = lwc_data {
            let out: Vec<Vec<u8>> = b.iter().map(|v| v.as_bytes().to_vec()).collect();
            assert_eq!(out, vec![b"abc".to_vec(), b"".to_vec(), b"defg".to_vec()]);
        } else {
            panic!("expected bytes variant");
        }
    }

    fn assert_invalid_payload<T>(result: DataIntegrityResult<T>) {
        assert!(
            result
                .as_ref()
                .is_err_and(|err| *err.current_context() == DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_lwc_data_from_bytes_rejects_empty_payload() {
        assert_invalid_payload(LwcData::from_bytes(ValKind::U8, &[]));
    }

    #[test]
    fn test_lwc_data_from_bytes_rejects_flat_payload_length_mismatch() {
        let mut short_payload = vec![LwcCode::Flat as u8];
        short_payload.extend_from_slice(&2u64.to_le_bytes());
        short_payload.push(7);
        assert_invalid_payload(LwcData::from_bytes(ValKind::U8, &short_payload));

        let mut long_payload = vec![LwcCode::Flat as u8];
        long_payload.extend_from_slice(&1u64.to_le_bytes());
        long_payload.extend_from_slice(&[7, 8]);
        assert_invalid_payload(LwcData::from_bytes(ValKind::U8, &long_payload));
    }

    #[test]
    fn test_lwc_data_from_bytes_rejects_flat_oversized_len() {
        let mut payload = vec![LwcCode::Flat as u8];
        payload.extend_from_slice(&(1u64 << 32).to_le_bytes());

        assert_invalid_payload(LwcData::from_bytes(ValKind::U8, &payload));
    }

    #[test]
    fn test_lwc_data_from_bytes_rejects_flat_typed_partial_element() {
        let mut payload = vec![LwcCode::Flat as u8];
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.push(7);

        assert_invalid_payload(LwcData::from_bytes(ValKind::I16, &payload));
    }

    #[test]
    fn test_lwc_data_from_bytes_rejects_for_bitpacking_payload_length_mismatch() {
        let mut payload = vec![LwcCode::ForBitpacking as u8];
        payload.push(1);
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.push(0);

        assert_invalid_payload(LwcData::from_bytes(ValKind::U8, &payload));
    }

    #[test]
    fn test_lwc_data_from_bytes_rejects_for_bitpacking_oversized_len() {
        let mut payload = vec![LwcCode::ForBitpacking as u8];
        payload.push(1);
        payload.extend_from_slice(&(1u64 << 32).to_le_bytes());
        payload.push(0);

        assert_invalid_payload(LwcData::from_bytes(ValKind::U8, &payload));
    }

    #[test]
    fn test_lwc_bytes_invalid() {
        let err = LwcPrimitiveSer::new_bytes(&[], &[]);
        assert!(
            err.as_ref()
                .is_err_and(|err| err.current_context() == &InternalError::LwcBuilderMisuse)
        );

        let err = LwcPrimitiveSer::new_bytes(&[1, 2], &[0u8]);
        assert!(
            err.as_ref()
                .is_err_and(|err| err.current_context() == &InternalError::LwcBuilderMisuse)
        );

        let offsets = vec![0u32, 0];
        let bytes = vec![];
        let lwc_ser = LwcPrimitiveSer::new_bytes(&offsets, &bytes).unwrap();
        let mut res = vec![0u8; lwc_ser.ser_len()];
        lwc_ser.ser(&mut res[..], 0);
        let err = LwcData::from_bytes(ValKind::VarByte, &res[..res.len() - 1]);
        assert!(
            err.as_ref()
                .is_err_and(|err| *err.current_context() == DataIntegrityError::InvalidPayload)
        );
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
        // so persisted decoding reports an invalid payload.
        let mut payload = vec![LwcCode::ForBitpacking as u8];
        payload.push(3); // n_bits
        payload.extend_from_slice(&1u64.to_le_bytes()); // len
        payload.push(0); // min value for u8
        payload.push(0); // data byte (div_ceil(1 * 3, 8) == 1)

        let err = LwcData::from_bytes(ValKind::U8, &payload);
        assert!(
            err.as_ref()
                .is_err_and(|err| *err.current_context() == DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_lwc_null_bitmap_serde() {
        let bytes = [0b0000_1010u8, 0b0000_0001];
        let ser = LwcNullBitmapSer::new(&bytes).unwrap();
        let mut out = vec![0u8; ser.ser_len()];
        let idx = ser.ser(&mut out[..], 0);
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
        let metadata = TableMetadata::try_new(
            vec![
                ColumnSpec::new("c0", ValKind::U8, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::I16, ColumnAttributes::NULLABLE),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(100), 20, metadata.col.as_ref());

        let mut expected_rows = Vec::new();
        for offset in 0..10u64 {
            let row_id = 100 + offset;
            let c0 = Val::U8(offset as u8);
            let c1 = if offset % 2 == 0 {
                Val::Null
            } else {
                Val::I16(offset as i16)
            };
            let res = page.insert(metadata.col.as_ref(), &[c0, c1]);
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

        assert!(matches!(page.delete(RowID::new(102)), Delete::Ok));
        assert!(matches!(page.delete(RowID::new(105)), Delete::Ok));
        expected_rows.retain(|(row_id, _, _)| *row_id != 102 && *row_id != 105);

        let mut builder = LwcBuilder::new(metadata.col.as_ref());
        assert!(builder.is_empty());
        assert!(builder.row_count() == 0);
        let view = page.vector_view(metadata.col.as_ref());
        let appended = builder.append_view(view, page.header.start_row_id);
        assert!(appended);
        let expected_fingerprint = row_shape_fingerprint_for(builder.row_ids(), 100, 110);
        let buf = builder.build(expected_fingerprint).unwrap();

        let lwc_block = LwcBlock::try_from_persisted_bytes(
            buf.as_bytes(),
            FileKind::TableFile,
            test_block_id(1),
        )
        .unwrap();
        assert_eq!(lwc_block.header.row_count() as usize, expected_rows.len());
        assert_eq!(
            lwc_block.header.row_shape_fingerprint(),
            expected_fingerprint
        );

        let column0 = lwc_block.column(metadata.col.as_ref(), 0).unwrap();
        let column1 = lwc_block.column(metadata.col.as_ref(), 1).unwrap();
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

    #[test]
    fn test_lwc_builder_append_row_values_matches_row_page() {
        let metadata = TableMetadata::try_new(
            vec![
                ColumnSpec::new("c_i16", ValKind::I16, ColumnAttributes::NULLABLE),
                ColumnSpec::new("c_u64", ValKind::U64, ColumnAttributes::empty()),
                ColumnSpec::new("c_bytes", ValKind::VarByte, ColumnAttributes::NULLABLE),
                ColumnSpec::new("c_f32", ValKind::F32, ColumnAttributes::empty()),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let rows = vec![
            vec![
                Val::Null,
                Val::U64(10),
                Val::from(Vec::from(&b"alpha"[..])),
                Val::F32(OrderedFloat(1.25)),
            ],
            vec![
                Val::I16(-5),
                Val::U64(11),
                Val::Null,
                Val::F32(OrderedFloat(2.5)),
            ],
            vec![
                Val::I16(9),
                Val::U64(12),
                Val::from(Vec::from(&b"beta"[..])),
                Val::F32(OrderedFloat(3.75)),
            ],
        ];
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(10), rows.len(), metadata.col.as_ref());
        for vals in &rows {
            assert!(matches!(
                page.insert(metadata.col.as_ref(), vals),
                InsertRow::Ok(_)
            ));
        }

        let mut page_builder = LwcBuilder::new(metadata.col.as_ref());
        let view = page.vector_view(metadata.col.as_ref());
        assert!(page_builder.append_view(view, page.header.start_row_id));
        let mut direct_builder = LwcBuilder::new(metadata.col.as_ref());
        for (offset, vals) in rows.iter().enumerate() {
            assert!(direct_builder.append_row_values(RowID::new(10 + offset as u64), vals));
        }

        assert_eq!(direct_builder.row_ids(), page_builder.row_ids());
        let fingerprint = row_shape_fingerprint_for(direct_builder.row_ids(), 10, 13);
        let page_buf = page_builder.build(fingerprint).unwrap();
        let direct_buf = direct_builder.build(fingerprint).unwrap();
        assert_eq!(direct_buf.as_bytes(), page_buf.as_bytes());
    }

    #[test]
    fn test_lwc_builder_append_row_values_rolls_back_on_capacity() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "bytes",
                ValKind::VarByte,
                ColumnAttributes::empty(),
            )],
            vec![],
        )
        .expect("valid table metadata");
        let mut builder = LwcBuilder::new(metadata.col.as_ref());
        assert!(builder.append_row_values(RowID::new(1), &[Val::from(Vec::from(&b"ok"[..]))]));

        let oversized = Val::from(vec![0u8; LWC_BLOCK_PAYLOAD_SIZE]);
        assert!(!builder.append_row_values(RowID::new(2), &[oversized]));

        assert_eq!(builder.row_count(), 1);
        assert_eq!(builder.row_ids(), &[RowID::new(1)]);
        assert!(builder.build(0).is_ok());
    }

    #[test]
    fn test_lwc_builder_reports_reachable_row_count_encoding_contract() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "compressible",
                ValKind::U8,
                ColumnAttributes::empty(),
            )],
            vec![],
        )
        .expect("valid table metadata");
        let mut builder = LwcBuilder::new(metadata.col.as_ref());

        for row_no in 0..=u16::MAX {
            assert!(builder.append_row_values(RowID::new(u64::from(row_no)), &[Val::U8(0)]));
        }
        assert_eq!(builder.row_count(), u16::MAX as usize + 1);

        let err = match builder.build(0) {
            Ok(_) => panic!("row count above u16::MAX must fail encoding"),
            Err(err) => err,
        };
        assert_eq!(
            err.current_context(),
            &InternalError::LwcBlockEncodingContract
        );
        let report = format!("{err:?}");
        assert!(report.contains("field=row_count"), "{report}");
        assert!(report.contains("maximum=65535"), "{report}");
    }

    #[test]
    fn test_lwc_builder_nullable_integer_stats_skip_nulls_and_deleted_rows() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "nullable_i16",
                ValKind::I16,
                ColumnAttributes::NULLABLE,
            )],
            vec![],
        )
        .expect("valid table metadata");
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(0), 8, metadata.col.as_ref());

        for (row_id, value) in [
            Val::Null,
            Val::I16(5),
            Val::I16(-10),
            Val::Null,
            Val::I16(1000),
            Val::I16(8),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(matches!(
                page.insert(metadata.col.as_ref(), &[value]),
                InsertRow::Ok(inserted) if inserted == RowID::new(row_id as u64)
            ));
        }
        assert!(matches!(page.delete(RowID::new(4)), Delete::Ok));

        let mut builder = LwcBuilder::new(metadata.col.as_ref());
        let view = page.vector_view(metadata.col.as_ref());
        assert!(builder.append_view(view, page.header.start_row_id));

        let stats = builder.stats[0].snapshot();
        assert!(stats.initialized);
        assert_eq!(stats.min_i64, -10);
        assert_eq!(stats.max_i64, 8);
    }

    #[test]
    fn test_lwc_builder_rollback() {
        let metadata = TableMetadata::try_new(
            vec![
                ColumnSpec::new("c0", ValKind::U8, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::I16, ColumnAttributes::empty()),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(1), 4, metadata.col.as_ref());

        for offset in 0..2u64 {
            let c0 = Val::U8((10 + offset) as u8);
            let c1 = Val::I16((20 + offset) as i16);
            assert!(matches!(
                page.insert(metadata.col.as_ref(), &[c0, c1]),
                InsertRow::Ok(_)
            ));
        }

        let mut builder = LwcBuilder::new(metadata.col.as_ref());
        let view = page.vector_view(metadata.col.as_ref());
        assert!(builder.append_view(view, page.header.start_row_id));

        let snapshot = builder.snapshot_state();
        let expected_row_count = builder.row_count();
        let expected_row_ids_len = builder.row_ids.len();
        let expected_stats: Vec<_> = builder
            .stats
            .iter()
            .map(|stat| {
                let snap = stat.snapshot();
                (
                    snap.initialized,
                    snap.min_i64,
                    snap.max_i64,
                    snap.min_u64,
                    snap.max_u64,
                )
            })
            .collect();

        for offset in 0..2u64 {
            let c0 = Val::U8((30 + offset) as u8);
            let c1 = Val::I16((40 + offset) as i16);
            assert!(matches!(
                page.insert(metadata.col.as_ref(), &[c0, c1]),
                InsertRow::Ok(_)
            ));
        }
        let view = page.vector_view(metadata.col.as_ref());
        assert!(builder.append_view(view, page.header.start_row_id));

        builder.rollback(snapshot);

        assert_eq!(builder.row_count(), expected_row_count);
        assert_eq!(builder.row_ids.len(), expected_row_ids_len);
        let restored_stats: Vec<_> = builder
            .stats
            .iter()
            .map(|stat| {
                let snap = stat.snapshot();
                (
                    snap.initialized,
                    snap.min_i64,
                    snap.max_i64,
                    snap.min_u64,
                    snap.max_u64,
                )
            })
            .collect();
        assert_eq!(restored_stats, expected_stats);
    }

    #[test]
    fn test_lwc_builder_all_column_types() {
        let metadata = TableMetadata::try_new(
            vec![
                ColumnSpec::new("c_i8", ValKind::I8, ColumnAttributes::empty()),
                ColumnSpec::new("c_u8", ValKind::U8, ColumnAttributes::empty()),
                ColumnSpec::new("c_i16", ValKind::I16, ColumnAttributes::empty()),
                ColumnSpec::new("c_u16", ValKind::U16, ColumnAttributes::empty()),
                ColumnSpec::new("c_i32", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("c_u32", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c_i64", ValKind::I64, ColumnAttributes::empty()),
                ColumnSpec::new("c_u64", ValKind::U64, ColumnAttributes::empty()),
                ColumnSpec::new("c_f32", ValKind::F32, ColumnAttributes::empty()),
                ColumnSpec::new("c_f64", ValKind::F64, ColumnAttributes::empty()),
                ColumnSpec::new("c_bytes", ValKind::VarByte, ColumnAttributes::empty()),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(10), 6, metadata.col.as_ref());

        let rows = vec![
            vec![
                Val::I8(-1),
                Val::U8(1),
                Val::I16(-10),
                Val::U16(10),
                Val::I32(-100),
                Val::U32(100),
                Val::I64(-1000),
                Val::U64(1000),
                Val::F32(OrderedFloat(1.25)),
                Val::F64(OrderedFloat(2.5)),
                Val::VarByte(MemVar::inline(b"alpha")),
            ],
            vec![
                Val::I8(2),
                Val::U8(3),
                Val::I16(20),
                Val::U16(30),
                Val::I32(200),
                Val::U32(300),
                Val::I64(2000),
                Val::U64(3000),
                Val::F32(OrderedFloat(-4.5)),
                Val::F64(OrderedFloat(6.75)),
                Val::VarByte(MemVar::inline(b"beta")),
            ],
            vec![
                Val::I8(4),
                Val::U8(5),
                Val::I16(40),
                Val::U16(50),
                Val::I32(400),
                Val::U32(500),
                Val::I64(4000),
                Val::U64(5000),
                Val::F32(OrderedFloat(7.75)),
                Val::F64(OrderedFloat(-8.125)),
                Val::VarByte(MemVar::inline(b"gamma")),
            ],
        ];

        for row in &rows {
            assert!(matches!(
                page.insert(metadata.col.as_ref(), row),
                InsertRow::Ok(_)
            ));
        }

        let mut builder = LwcBuilder::new(metadata.col.as_ref());
        let view = page.vector_view(metadata.col.as_ref());
        assert!(builder.append_view(view, page.header.start_row_id));
        let buf = builder
            .build(row_shape_fingerprint_for(builder.row_ids(), 10, 13))
            .unwrap();

        let lwc_block = LwcBlock::try_from_persisted_bytes(
            buf.as_bytes(),
            FileKind::TableFile,
            test_block_id(1),
        )
        .unwrap();
        assert_eq!(lwc_block.header.row_count() as usize, rows.len());

        for (col_idx, expected_kind) in [
            ValKind::I8,
            ValKind::U8,
            ValKind::I16,
            ValKind::U16,
            ValKind::I32,
            ValKind::U32,
            ValKind::I64,
            ValKind::U64,
            ValKind::F32,
            ValKind::F64,
            ValKind::VarByte,
        ]
        .iter()
        .enumerate()
        {
            let column = lwc_block.column(metadata.col.as_ref(), col_idx).unwrap();
            let data = column.data().unwrap();
            assert_eq!(data.len(), rows.len());
            for (row_idx, expected_row) in rows.iter().enumerate() {
                let value = data.value(row_idx).unwrap();
                assert_eq!(value.kind(), Some(*expected_kind));
                assert_eq!(value, expected_row[col_idx]);
            }
        }
    }
}
