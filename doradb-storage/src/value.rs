// Public TryFrom adapters disclose crate errors: ValKind forwards DataIntegrity,
// while owned byte conversions forward Operation. Internal construction and
// decoding retain their native report domains.
use crate::error::{
    DataIntegrityError, DataIntegrityResult, DiscloseError, DiscloseResultExt, Error,
    OperationError, Result,
};
use crate::memcmp::{
    BytesExtendable, MIN_VAR_MCF_LEN, MIN_VAR_NMCF_LEN, MemCmpFormat, Null, NullableMemCmpFormat,
    SegmentedBytes,
};
use crate::serde::{Deser, DeserResult, MinBytesHint, Ser, Serde, min_bytes_hint};
use error_stack::Report;
use ordered_float::OrderedFloat;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::ptr::{copy_nonoverlapping, slice_from_raw_parts_mut};
use std::result::Result as StdResult;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::str::{Utf8Error, from_utf8};
use std::sync::atomic::*;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Number of bytes used by a page varlen field.
pub(crate) const PAGE_VAR_HEADER: usize = 8;
/// Maximum inline payload bytes for page varlen values.
pub(crate) const PAGE_VAR_LEN_INLINE: usize = 6;
/// Prefix bytes stored for outlined page varlen values.
pub(crate) const PAGE_VAR_LEN_PREFIX: usize = 4;
const _: () = assert!(mem::size_of::<PageVar>() == 8);

/// Number of bytes used by an in-memory varlen field.
#[cfg_attr(not(test), expect(dead_code, reason = "reserved MEM_VAR_HEADER"))]
pub(crate) const MEM_VAR_HEADER: usize = 16;
/// Maximum inline payload bytes for in-memory varlen values.
pub(crate) const MEM_VAR_LEN_INLINE: usize = 14;
/// Prefix bytes stored for outlined in-memory varlen values.
pub(crate) const MEM_VAR_LEN_PREFIX: usize = 6;
const _: () = assert!(mem::size_of::<MemVar>() == 16);
const _: () = assert!(mem::size_of::<ValKind>() == mem::size_of::<u32>());

/// Logical value type and nullability metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValType {
    /// Non-null physical value kind.
    pub kind: ValKind,
    /// Whether the value may be null.
    pub nullable: bool,
}

impl ValType {
    /// Creates value type metadata.
    #[inline]
    pub fn new(kind: ValKind, nullable: bool) -> Self {
        ValType { kind, nullable }
    }

    /// Returns the inline byte length for this value kind.
    #[inline]
    pub fn inline_len(self) -> usize {
        self.kind.inline_len()
    }

    /// Returns fixed-width memcmp encoding length, or `None` for varlen kinds.
    #[inline]
    pub fn memcmp_encoded_len(self) -> Option<usize> {
        if self.kind.is_fixed() {
            let len = self.kind.inline_len() + if self.nullable { 1 } else { 0 };
            return Some(len);
        }
        None
    }

    /// Returns the minimum memcmp encoding length, including nullable flag bytes.
    #[inline]
    pub fn memcmp_encoded_len_maybe_var(self) -> usize {
        if self.kind.is_fixed() {
            return self.kind.inline_len() + if self.nullable { 1 } else { 0 };
        }
        if self.nullable {
            MIN_VAR_NMCF_LEN
        } else {
            MIN_VAR_MCF_LEN
        }
    }
}

impl Ser<'_> for ValType {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u32>() + mem::size_of::<u8>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = out.ser_u32(idx, self.kind as u32);
        out.ser_u8(idx, self.nullable as u8)
    }
}

impl Deser for ValType {
    const MIN_BYTES_HINT: MinBytesHint =
        min_bytes_hint(mem::size_of::<u32>() + mem::size_of::<u8>());

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let idx = start_idx;
        let (idx, kind) = input.deser_u32(idx)?;
        let kind = ValKind::decode(kind)?;
        let (idx, nullable) = input.deser_u8(idx)?;
        Ok((
            idx,
            ValType {
                kind,
                nullable: nullable != 0,
            },
        ))
    }
}
/// Physical value kind code used in row serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ValKind {
    I8 = 1,
    U8 = 2,
    I16 = 3,
    U16 = 4,
    I32 = 5,
    U32 = 6,
    F32 = 7,
    I64 = 8,
    U64 = 9,
    F64 = 10,
    VarByte = 11,
}
impl ValKind {
    /// Returns the inline byte length for this kind.
    #[inline]
    pub const fn inline_len(self) -> usize {
        match self {
            ValKind::I8 | ValKind::U8 => 1,
            ValKind::I16 | ValKind::U16 => 2,
            ValKind::I32 | ValKind::U32 | ValKind::F32 => 4,
            ValKind::I64 | ValKind::U64 | ValKind::F64 => 8,
            // 2-byte len, 2-byte offset, 4-byte prefix
            // or inline version, 2-byte len, at most 6 inline bytes
            ValKind::VarByte => 8,
        }
    }

    /// Returns true when values of this kind have fixed inline length.
    #[inline]
    pub fn is_fixed(self) -> bool {
        !matches!(self, ValKind::VarByte)
    }

    /// Decodes one persisted value-kind code before a public error boundary is chosen.
    #[inline]
    pub(crate) fn decode(value: u32) -> DataIntegrityResult<Self> {
        let res = match value {
            1 => ValKind::I8,
            2 => ValKind::U8,
            3 => ValKind::I16,
            4 => ValKind::U16,
            5 => ValKind::I32,
            6 => ValKind::U32,
            7 => ValKind::F32,
            8 => ValKind::I64,
            9 => ValKind::U64,
            10 => ValKind::F64,
            11 => ValKind::VarByte,
            _ => {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!("invalid value kind code {value}")));
            }
        };
        Ok(res)
    }

    /// Create a value type with nullable setting.
    #[inline]
    pub fn nullable(self, nullable: bool) -> ValType {
        ValType {
            kind: self,
            nullable,
        }
    }
}

impl TryFrom<u32> for ValKind {
    type Error = Error;
    #[inline]
    fn try_from(value: u32) -> Result<Self> {
        Self::decode(value).disclose()
    }
}

/// Val is value representation of row-store.
/// The variable-length data may require new allocation
/// because we cannot rely on page data.
#[derive(Clone, Default, Eq)]
pub enum Val {
    #[default]
    Null,
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    F32(OrderedFloat<f32>),
    I64(i64),
    U64(u64),
    F64(OrderedFloat<f64>),
    VarByte(MemVar),
}

// SAFETY: `Val` contains plain scalar values or `MemVar`, whose owned heap
// storage is self-contained and not thread-affine.
unsafe impl Send for Val {}
// SAFETY: shared references to `Val` only expose immutable access to those
// scalar fields or immutable byte storage.
unsafe impl Sync for Val {}

impl PartialEq for Val {
    #[inline]
    fn eq(&self, rhs: &Self) -> bool {
        match (self, rhs) {
            (Val::Null, Val::Null) => true,
            (Val::I8(l), Val::I8(r)) => l == r,
            (Val::U8(l), Val::U8(r)) => l == r,
            (Val::I16(l), Val::I16(r)) => l == r,
            (Val::U16(l), Val::U16(r)) => l == r,
            (Val::I32(l), Val::I32(r)) => l == r,
            (Val::U32(l), Val::U32(r)) => l == r,
            (Val::F32(l), Val::F32(r)) => l == r,
            (Val::I64(l), Val::I64(r)) => l == r,
            (Val::U64(l), Val::U64(r)) => l == r,
            (Val::F64(l), Val::F64(r)) => l == r,
            (Val::VarByte(l), Val::VarByte(r)) => l.as_bytes() == r.as_bytes(),
            _ => false,
        }
    }
}

impl Hash for Val {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Val::Null => state.write_u64(0),
            Val::I8(v) => state.write_i8(*v),
            Val::U8(v) => state.write_u8(*v),
            Val::I16(v) => state.write_i16(*v),
            Val::U16(v) => state.write_u16(*v),
            Val::I32(v) => state.write_i32(*v),
            Val::U32(v) => state.write_u32(*v),
            Val::F32(v) => state.write_u32(v.to_bits()),
            Val::I64(v) => state.write_i64(*v),
            Val::U64(v) => state.write_u64(*v),
            Val::F64(v) => state.write_u64(v.to_bits()),
            Val::VarByte(var) => state.write(var.as_bytes()),
        }
    }
}

impl Val {
    /// Copies at most [`MemVar::MAX_LEN`] bytes, returning `InvalidDmlInput` if oversized.
    /// Representable values may still exceed a row page's capacity.
    #[inline]
    pub fn try_from_bytes(data: &[u8]) -> Result<Self> {
        // Keep report construction outside the successful constructor's inline body.
        #[cold]
        fn invalid_length(len: usize) -> Error {
            MemVar::invalid_length(len)
                .attach("Val::try_from_bytes")
                .disclose()
        }

        if checked_var_len(data.len()).is_none() {
            return Err(invalid_length(data.len()));
        }
        Ok(Self::VarByte(MemVar::from_bytes(data)))
    }

    /// Consumes a byte vector, returning `InvalidDmlInput` if it exceeds [`MemVar::MAX_LEN`].
    /// The vector is consumed on error too; allocation failure is not recoverable.
    #[inline]
    pub fn try_from_vec(data: Vec<u8>) -> Result<Self> {
        // Keep report construction outside the successful constructor's inline body.
        #[cold]
        fn invalid_length(data: Vec<u8>) -> Error {
            MemVar::invalid_length(data.len())
                .attach("Val::try_from_vec")
                .disclose()
        }

        if checked_var_len(data.len()).is_none() {
            return Err(invalid_length(data));
        }
        Ok(Self::VarByte(MemVar::from_vec(data)))
    }

    /// Borrows this value without allocating or copying its byte payload.
    #[inline]
    pub(crate) fn view(&self) -> ValRef<'_> {
        match self {
            Val::Null => ValRef::Null,
            Val::I8(value) => ValRef::I8(*value),
            Val::U8(value) => ValRef::U8(*value),
            Val::I16(value) => ValRef::I16(*value),
            Val::U16(value) => ValRef::U16(*value),
            Val::I32(value) => ValRef::I32(*value),
            Val::U32(value) => ValRef::U32(*value),
            Val::F32(value) => ValRef::F32(*value),
            Val::I64(value) => ValRef::I64(*value),
            Val::U64(value) => ValRef::U64(*value),
            Val::F64(value) => ValRef::F64(*value),
            Val::VarByte(value) => ValRef::VarByte(value.as_bytes()),
        }
    }

    /// Returns true when this value is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        self.view().is_null()
    }

    /// Returns this value as `u8`, when it has that kind.
    #[inline]
    pub fn as_u8(&self) -> Option<u8> {
        match self {
            Val::U8(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns this value as `i8`, when it has that kind.
    #[inline]
    pub fn as_i8(&self) -> Option<i8> {
        match self {
            Val::I8(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns this value as `u16`, when it has that kind.
    #[inline]
    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Val::U16(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns this value as `i16`, when it has that kind.
    #[inline]
    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Val::I16(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns this value as `i32`, when it has that kind.
    #[inline]
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Val::I32(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns this value as `u32`, when it has that kind.
    #[inline]
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Val::U32(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns this value as `i64`, when it has that kind.
    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Val::I64(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns this value as `u64`, when it has that kind.
    #[inline]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Val::U64(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns this value as `f32`, when it has that kind.
    #[inline]
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Val::F32(v) => Some(v.0),
            _ => None,
        }
    }

    /// Returns this value as `f64`, when it has that kind.
    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Val::F64(v) => Some(v.0),
            _ => None,
        }
    }

    /// Returns this value as bytes, when it is varlen bytes.
    #[inline]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Val::VarByte(v) => Some(v.as_bytes()),
            _ => None,
        }
    }

    /// Returns UTF-8 text, or `None` for invalid UTF-8 and non-byte values.
    /// Use [`Self::try_as_str`] to distinguish invalid UTF-8 from other value kinds.
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        self.try_as_str().ok().flatten()
    }

    /// Checks byte values as UTF-8, returning `Ok(None)` for non-byte values and null.
    #[inline]
    pub fn try_as_str(&self) -> StdResult<Option<&str>, Utf8Error> {
        match self {
            Val::VarByte(v) => v.try_as_str().map(Some),
            _ => Ok(None),
        }
    }

    /// Encodes this value into memcmp format for `ty`.
    #[inline]
    pub fn encode_memcmp(&self, ty: ValType, buf: &mut Vec<u8>) {
        if ty.nullable {
            self.encode_nmcf(ty.kind, buf);
        } else {
            self.encode_mcf(ty.kind, buf);
        }
    }

    /// Returns the non-null kind for this value.
    #[inline]
    pub fn kind(&self) -> Option<ValKind> {
        self.view().kind()
    }

    /// Returns true when this value is null or has the requested kind.
    #[inline]
    pub fn matches_kind(&self, kind: ValKind) -> bool {
        self.view().matches_kind(kind)
    }

    #[inline]
    fn encode_mcf<T: BytesExtendable>(&self, kind: ValKind, buf: &mut T) {
        match kind {
            ValKind::I8 => self.as_i8().unwrap().extend_mcf_to(buf),
            ValKind::U8 => self.as_u8().unwrap().extend_mcf_to(buf),
            ValKind::I16 => self.as_i16().unwrap().extend_mcf_to(buf),
            ValKind::U16 => self.as_u16().unwrap().extend_mcf_to(buf),
            ValKind::I32 => self.as_i32().unwrap().extend_mcf_to(buf),
            ValKind::U32 => self.as_u32().unwrap().extend_mcf_to(buf),
            ValKind::I64 => self.as_i64().unwrap().extend_mcf_to(buf),
            ValKind::U64 => self.as_u64().unwrap().extend_mcf_to(buf),
            ValKind::F32 => self.as_f32().unwrap().extend_mcf_to(buf),
            ValKind::F64 => self.as_f64().unwrap().extend_mcf_to(buf),
            ValKind::VarByte => SegmentedBytes(self.as_bytes().unwrap()).extend_mcf_to(buf),
        }
    }

    #[inline]
    fn encode_nmcf<T: BytesExtendable>(&self, kind: ValKind, buf: &mut T) {
        if self.is_null() {
            Null.extend_nmcf_to(buf);
            return;
        }
        match kind {
            ValKind::I8 => self.as_i8().unwrap().extend_nmcf_to(buf),
            ValKind::U8 => self.as_u8().unwrap().extend_nmcf_to(buf),
            ValKind::I16 => self.as_i16().unwrap().extend_nmcf_to(buf),
            ValKind::U16 => self.as_u16().unwrap().extend_nmcf_to(buf),
            ValKind::I32 => self.as_i32().unwrap().extend_nmcf_to(buf),
            ValKind::U32 => self.as_u32().unwrap().extend_nmcf_to(buf),
            ValKind::I64 => self.as_i64().unwrap().extend_nmcf_to(buf),
            ValKind::U64 => self.as_u64().unwrap().extend_nmcf_to(buf),
            ValKind::F32 => self.as_f32().unwrap().extend_nmcf_to(buf),
            ValKind::F64 => self.as_f64().unwrap().extend_nmcf_to(buf),
            ValKind::VarByte => SegmentedBytes(self.as_bytes().unwrap()).extend_nmcf_to(buf),
        }
    }
}

impl fmt::Debug for Val {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.view().fmt(f)
    }
}

impl From<u8> for Val {
    #[inline]
    fn from(value: u8) -> Self {
        Val::U8(value)
    }
}

impl From<i8> for Val {
    #[inline]
    fn from(value: i8) -> Self {
        Val::I8(value)
    }
}

impl From<u16> for Val {
    #[inline]
    fn from(value: u16) -> Self {
        Val::U16(value)
    }
}

impl From<i16> for Val {
    #[inline]
    fn from(value: i16) -> Self {
        Val::I16(value)
    }
}

impl From<u32> for Val {
    #[inline]
    fn from(value: u32) -> Self {
        Val::U32(value)
    }
}

impl From<i32> for Val {
    #[inline]
    fn from(value: i32) -> Self {
        Val::I32(value)
    }
}

impl From<f32> for Val {
    #[inline]
    fn from(value: f32) -> Self {
        Val::F32(OrderedFloat(value))
    }
}

impl From<u64> for Val {
    #[inline]
    fn from(value: u64) -> Self {
        Val::U64(value)
    }
}

impl From<i64> for Val {
    #[inline]
    fn from(value: i64) -> Self {
        Val::I64(value)
    }
}

impl From<f64> for Val {
    #[inline]
    fn from(value: f64) -> Self {
        Val::F64(OrderedFloat(value))
    }
}

impl TryFrom<&[u8]> for Val {
    type Error = Error;

    #[inline]
    fn try_from(value: &[u8]) -> Result<Self> {
        Self::try_from_bytes(value)
    }
}

impl<const LEN: usize> TryFrom<&[u8; LEN]> for Val {
    type Error = Error;

    #[inline]
    fn try_from(value: &[u8; LEN]) -> Result<Self> {
        Self::try_from_bytes(value)
    }
}

impl TryFrom<&str> for Val {
    type Error = Error;

    #[inline]
    fn try_from(value: &str) -> Result<Self> {
        Self::try_from_bytes(value.as_bytes())
    }
}

impl TryFrom<Vec<u8>> for Val {
    type Error = Error;

    #[inline]
    fn try_from(value: Vec<u8>) -> Result<Self> {
        Self::try_from_vec(value)
    }
}

impl Ser<'_> for Val {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u32>()
            + match self {
                Val::Null => 0, // null is encoded with code only.
                Val::I8(_) | Val::U8(_) => 1,
                Val::I16(_) | Val::U16(_) => 2,
                Val::I32(_) | Val::U32(_) | Val::F32(_) => 4,
                Val::I64(_) | Val::U64(_) | Val::F64(_) => 8,
                Val::VarByte(v) => mem::size_of::<u16>() + v.len(),
            }
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        debug_assert!(start_idx + self.ser_len() <= out.size());
        let code = self.kind().map(|k| k as u32).unwrap_or(0);
        let idx = out.ser_u32(start_idx, code);
        match self {
            Val::Null => idx,
            Val::I8(v) => out.ser_i8(idx, *v),
            Val::U8(v) => out.ser_u8(idx, *v),
            Val::I16(v) => out.ser_i16(idx, *v),
            Val::U16(v) => out.ser_u16(idx, *v),
            Val::I32(v) => out.ser_i32(idx, *v),
            Val::U32(v) => out.ser_u32(idx, *v),
            Val::F32(v) => out.ser_f32(idx, v.0),
            Val::I64(v) => out.ser_i64(idx, *v),
            Val::U64(v) => out.ser_u64(idx, *v),
            Val::F64(v) => out.ser_f64(idx, v.0),
            Val::VarByte(v) => {
                let idx = out.ser_u16(idx, v.len() as u16);
                out.ser_byte_slice(idx, v.as_bytes())
            }
        }
    }
}

impl Deser for Val {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(mem::size_of::<u32>());

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (idx, value) = ValRef::deser(input, start_idx)?;
        let value = match value {
            ValRef::Null => Val::Null,
            ValRef::I8(v) => Val::I8(v),
            ValRef::U8(v) => Val::U8(v),
            ValRef::I16(v) => Val::I16(v),
            ValRef::U16(v) => Val::U16(v),
            ValRef::I32(v) => Val::I32(v),
            ValRef::U32(v) => Val::U32(v),
            ValRef::F32(v) => Val::F32(v),
            ValRef::I64(v) => Val::I64(v),
            ValRef::U64(v) => Val::U64(v),
            ValRef::F64(v) => Val::F64(v),
            // The wire decoder reads a u16 byte length.
            ValRef::VarByte(v) => Val::VarByte(MemVar::from_bytes(v)),
        };
        Ok((idx, value))
    }
}

/// A short-lived value view borrowing bytes from its input owner.
/// Scalar payloads retain their exact bits, including floating-point values.
/// Byte slices need not be bounded; validators and writers enforce [`MemVar::MAX_LEN`].
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValRef<'a> {
    Null,
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    F32(OrderedFloat<f32>),
    I64(i64),
    U64(u64),
    F64(OrderedFloat<f64>),
    VarByte(&'a [u8]),
}

impl<'a> ValRef<'a> {
    /// Decode one checked wire value, borrowing variable bytes from the input.
    #[inline]
    pub(crate) fn deser<S: Serde + ?Sized>(
        input: &'a S,
        start_idx: usize,
    ) -> DeserResult<(usize, Self)> {
        let (idx, c) = input.deser_u32(start_idx)?;
        if c == 0 {
            return Ok((idx, ValRef::Null));
        }
        let kind = ValKind::decode(c)?;
        match kind {
            ValKind::I8 => {
                let (idx, v) = input.deser_i8(idx)?;
                Ok((idx, ValRef::I8(v)))
            }
            ValKind::U8 => {
                let (idx, v) = input.deser_u8(idx)?;
                Ok((idx, ValRef::U8(v)))
            }
            ValKind::I16 => {
                let (idx, v) = input.deser_i16(idx)?;
                Ok((idx, ValRef::I16(v)))
            }
            ValKind::U16 => {
                let (idx, v) = input.deser_u16(idx)?;
                Ok((idx, ValRef::U16(v)))
            }
            ValKind::I32 => {
                let (idx, v) = input.deser_i32(idx)?;
                Ok((idx, ValRef::I32(v)))
            }
            ValKind::U32 => {
                let (idx, v) = input.deser_u32(idx)?;
                Ok((idx, ValRef::U32(v)))
            }
            ValKind::F32 => {
                let (idx, v) = input.deser_f32(idx)?;
                Ok((idx, ValRef::F32(OrderedFloat(v))))
            }
            ValKind::I64 => {
                let (idx, v) = input.deser_i64(idx)?;
                Ok((idx, ValRef::I64(v)))
            }
            ValKind::U64 => {
                let (idx, v) = input.deser_u64(idx)?;
                Ok((idx, ValRef::U64(v)))
            }
            ValKind::F64 => {
                let (idx, v) = input.deser_f64(idx)?;
                Ok((idx, ValRef::F64(OrderedFloat(v))))
            }
            ValKind::VarByte => {
                let (idx, len) = input.deser_u16(idx)?;
                let (idx, s) = input.deser(idx, len as usize)?;
                Ok((idx, ValRef::VarByte(s)))
            }
        }
    }

    /// Returns true when this value is null.
    #[inline]
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, ValRef::Null)
    }

    /// Returns the non-null kind for this value.
    #[inline]
    pub(crate) fn kind(&self) -> Option<ValKind> {
        let kind = match self {
            ValRef::Null => return None,
            ValRef::I8(_) => ValKind::I8,
            ValRef::U8(_) => ValKind::U8,
            ValRef::I16(_) => ValKind::I16,
            ValRef::U16(_) => ValKind::U16,
            ValRef::I32(_) => ValKind::I32,
            ValRef::U32(_) => ValKind::U32,
            ValRef::F32(_) => ValKind::F32,
            ValRef::I64(_) => ValKind::I64,
            ValRef::U64(_) => ValKind::U64,
            ValRef::F64(_) => ValKind::F64,
            ValRef::VarByte(_) => ValKind::VarByte,
        };
        Some(kind)
    }

    /// Returns true when this value is null or has the requested kind.
    #[inline]
    pub(crate) fn matches_kind(&self, kind: ValKind) -> bool {
        // null matches all types.
        self.kind().map(|k| k == kind).unwrap_or(true)
    }
}

impl fmt::Debug for ValRef<'_> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValRef::Null => f.pad("Null"),
            ValRef::I8(v) => {
                f.pad("i8(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::U8(v) => {
                f.pad("u8(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::I16(v) => {
                f.pad("i16(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::U16(v) => {
                f.pad("u16(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::I32(v) => {
                f.pad("i32(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::U32(v) => {
                f.pad("u32(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::F32(v) => {
                f.pad("f32(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::I64(v) => {
                f.pad("i64(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::U64(v) => {
                f.pad("u64(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::F64(v) => {
                f.pad("f64(")?;
                write!(f, "{}", v)?;
                f.pad(")")
            }
            ValRef::VarByte(v) => {
                f.pad("bytes(")?;
                if let Ok(s) = str::from_utf8(v) {
                    f.write_str(s)?;
                } else {
                    write!(f, "{:?}", v)?;
                }
                f.pad(")")
            }
        }
    }
}

/// Value is a marker trait to represent
/// fixed-length column value in row page.
pub(crate) trait Value: Sized {
    /// Store self value into target position in atomic way.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a properly aligned page slot whose inline layout
    /// matches `Self`.
    unsafe fn atomic_store(&self, ptr: *const u8);

    /// Store self value into target position.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a properly aligned writable page slot whose inline
    /// layout matches `Self`.
    unsafe fn store(&self, ptr: *mut u8);
}

macro_rules! impl_value_fixed_size {
    ($t:ty, $at:ident, $size:expr) => {
        impl Value for $t {
            #[inline]
            unsafe fn atomic_store(&self, ptr: *const u8) {
                debug_assert!((ptr as usize).is_multiple_of($size));
                // SAFETY: the trait contract guarantees `ptr` is a properly
                // aligned slot for `$t`, so the atomic view aliases exactly that
                // storage.
                unsafe {
                    let atom = $at::from_ptr(ptr as *mut u8 as *mut $t);
                    atom.store(self.to_le(), Ordering::Release);
                }
            }

            #[inline]
            unsafe fn store(&self, ptr: *mut u8) {
                // SAFETY: the trait contract guarantees `ptr` points to a
                // writable slot laid out as `$t`.
                unsafe {
                    copy_nonoverlapping(self.to_le_bytes().as_ptr(), ptr, $size);
                }
            }
        }
    };
}

impl_value_fixed_size!(i8, AtomicI8, 1);
impl_value_fixed_size!(u8, AtomicU8, 1);
impl_value_fixed_size!(i16, AtomicI16, 2);
impl_value_fixed_size!(u16, AtomicU16, 2);
impl_value_fixed_size!(i32, AtomicI32, 4);
impl_value_fixed_size!(u32, AtomicU32, 4);
impl_value_fixed_size!(i64, AtomicI64, 8);
impl_value_fixed_size!(u64, AtomicU64, 8);

impl Value for f32 {
    #[inline]
    unsafe fn atomic_store(&self, ptr: *const u8) {
        debug_assert!((ptr as usize).is_multiple_of(4));
        // SAFETY: the trait contract guarantees `ptr` is a properly aligned
        // 4-byte slot for storing the raw bits of this `f32`.
        unsafe {
            let atom = AtomicU32::from_ptr(ptr as *mut u8 as *mut u32);
            atom.store(self.to_bits().to_le(), Ordering::Release);
        }
    }

    #[inline]
    unsafe fn store(&self, ptr: *mut u8) {
        // SAFETY: the trait contract guarantees `ptr` points to a writable
        // 4-byte slot for storing the raw bits of this `f32`.
        unsafe {
            copy_nonoverlapping(self.to_bits().to_le_bytes().as_ptr(), ptr, 4);
        }
    }
}

impl Value for f64 {
    #[inline]
    unsafe fn atomic_store(&self, ptr: *const u8) {
        debug_assert!((ptr as usize).is_multiple_of(8));
        // SAFETY: the trait contract guarantees `ptr` is a properly aligned
        // 8-byte slot for storing the raw bits of this `f64`.
        unsafe {
            let atom = AtomicU64::from_ptr(ptr as *mut u8 as *mut u64);
            atom.store(self.to_bits().to_le(), Ordering::Release);
        }
    }

    #[inline]
    unsafe fn store(&self, ptr: *mut u8) {
        // SAFETY: the trait contract guarantees `ptr` points to a writable
        // 8-byte slot for storing the raw bits of this `f64`.
        unsafe {
            copy_nonoverlapping(self.to_bits().to_le_bytes().as_ptr(), ptr, 8);
        }
    }
}

/// ValBuffer represents a temporary buffer to hold
/// continuous values.
/// Null bitmap is separated from this buffer.
pub(crate) enum ValBuffer {
    I8(Vec<i8>),
    U8(Vec<u8>),
    I16(Vec<i16>),
    U16(Vec<u16>),
    I32(Vec<i32>),
    U32(Vec<u32>),
    F32(Vec<f32>),
    I64(Vec<i64>),
    U64(Vec<u64>),
    F64(Vec<f64>),
    VarByte {
        offsets: Vec<(usize, usize)>,
        data: Vec<u8>,
    },
}

impl ValBuffer {
    /// Create a new value buffer.
    #[inline]
    pub(crate) fn new(kind: ValKind) -> Self {
        match kind {
            ValKind::I8 => ValBuffer::I8(vec![]),
            ValKind::U8 => ValBuffer::U8(vec![]),
            ValKind::I16 => ValBuffer::I16(vec![]),
            ValKind::U16 => ValBuffer::U16(vec![]),
            ValKind::I32 => ValBuffer::I32(vec![]),
            ValKind::U32 => ValBuffer::U32(vec![]),
            ValKind::F32 => ValBuffer::F32(vec![]),
            ValKind::I64 => ValBuffer::I64(vec![]),
            ValKind::U64 => ValBuffer::U64(vec![]),
            ValKind::F64 => ValBuffer::F64(vec![]),
            ValKind::VarByte => ValBuffer::VarByte {
                offsets: vec![],
                data: vec![],
            },
        }
    }

    /// Clears all buffered values while retaining allocation capacity.
    #[inline]
    pub(crate) fn clear(&mut self) {
        match self {
            ValBuffer::I8(vec) => vec.clear(),
            ValBuffer::U8(vec) => vec.clear(),
            ValBuffer::I16(vec) => vec.clear(),
            ValBuffer::U16(vec) => vec.clear(),
            ValBuffer::I32(vec) => vec.clear(),
            ValBuffer::U32(vec) => vec.clear(),
            ValBuffer::F32(vec) => vec.clear(),
            ValBuffer::I64(vec) => vec.clear(),
            ValBuffer::U64(vec) => vec.clear(),
            ValBuffer::F64(vec) => vec.clear(),
            ValBuffer::VarByte { offsets, data } => {
                offsets.clear();
                data.clear();
            }
        }
    }
}

/// PageVar represents var-len value in page.
/// It has two kinds: inline and outline.
/// Inline means the bytes are inlined in the fixed field.
/// Outline means the fixed field only store length,
/// offset and prfix. Entire value is located at
/// tail of page.
#[derive(Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, align(8))]
pub(crate) struct PageVar {
    b: [u8; 8],
}

impl PageVar {
    /// Create a new PageVar with inline data.
    /// The data length must be no more than 6 bytes.
    #[inline]
    pub(crate) fn inline(data: &[u8]) -> Self {
        assert!(
            data.len() <= PAGE_VAR_LEN_INLINE,
            "PageVar::inline length out of range: len={}, max={PAGE_VAR_LEN_INLINE}",
            data.len()
        );
        let mut b = [0u8; PAGE_VAR_HEADER];
        b[..2].copy_from_slice(&(data.len() as u16).to_le_bytes());
        b[2..2 + data.len()].copy_from_slice(data);
        PageVar { b }
    }

    /// Create a new PageVar with pointer info.
    /// The prefix length must be 4 bytes.
    #[inline]
    pub(crate) fn outline(len: u16, offset: u16, prefix: &[u8]) -> Self {
        assert!(
            usize::from(len) > PAGE_VAR_LEN_INLINE,
            "PageVar::outline requires outlined length: len={len}, offset={offset}"
        );
        assert!(
            prefix.len() == PAGE_VAR_LEN_PREFIX,
            "PageVar::outline prefix length mismatch: len={}, expected={PAGE_VAR_LEN_PREFIX}",
            prefix.len()
        );
        let mut b = [0u8; PAGE_VAR_HEADER];
        b[..2].copy_from_slice(&len.to_le_bytes());
        b[2..4].copy_from_slice(&offset.to_le_bytes());
        b[4..].copy_from_slice(prefix);
        PageVar { b }
    }

    /// Returns length of the value.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        u16::from_le_bytes([self.b[0], self.b[1]]) as usize
    }

    /// Returns offset if outlined.
    #[inline]
    pub(crate) fn offset(&self) -> Option<usize> {
        if self.is_inlined() {
            return None;
        }
        Some(u16::from_le_bytes([self.b[2], self.b[3]]) as usize)
    }

    /// Returns whether the value is inlined.
    #[inline]
    pub(crate) fn is_inlined(&self) -> bool {
        self.len() <= PAGE_VAR_LEN_INLINE
    }

    /// Returns inpage length of given value.
    /// If the value can be inlined, returns 0.
    #[inline]
    pub(crate) fn outline_len(data: &[u8]) -> usize {
        if data.len() > PAGE_VAR_LEN_INLINE {
            data.len()
        } else {
            0
        }
    }

    /// Returns bytes.
    ///
    /// # Safety
    ///
    /// If this value is outlined, `ptr` must point at the start of the page
    /// backing this `PageVar`, and the outlined payload range
    /// `ptr.add(self.offset().unwrap())..ptr.add(self.offset().unwrap() + self.len())`
    /// must be valid for reads.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved as_bytes_unchecked"))]
    pub(crate) unsafe fn as_bytes_unchecked(&self, ptr: *const u8) -> &[u8] {
        // SAFETY: the function contract guarantees `ptr` covers the outlined
        // payload when needed, and the inline/outline tag is derived from `len`.
        unsafe {
            let len = self.len();
            if len <= PAGE_VAR_LEN_INLINE {
                &self.b[2..2 + len]
            } else {
                let offset = u16::from_le_bytes([self.b[2], self.b[3]]) as usize;
                let data = ptr.add(offset);
                from_raw_parts(data, len)
            }
        }
    }

    /// Returns bytes on given page.
    #[inline]
    pub(crate) fn as_bytes<'a>(&'a self, page_data: &'a [u8]) -> &'a [u8] {
        let len = self.len();
        if len <= PAGE_VAR_LEN_INLINE {
            &self.b[2..2 + len]
        } else {
            let offset = u16::from_le_bytes([self.b[2], self.b[3]]) as usize;
            &page_data[offset..offset + len]
        }
    }

    /// In-place update with given value.
    /// Caller must ensure no extra space is required.
    ///
    /// # Safety
    ///
    /// If this value is currently outlined, `ptr` must point at the start of
    /// the page backing this `PageVar`, and the existing outlined payload range
    /// must be valid for writes and disjoint from `val`. Representational length
    /// and fit are checked before modifying the descriptor or payload.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved update_in_place"))]
    pub(crate) unsafe fn update_in_place(&mut self, ptr: *mut u8, val: &[u8]) {
        assert!(
            checked_var_len(val.len()).is_some()
                && (val.len() <= PAGE_VAR_LEN_INLINE || val.len() <= self.len()),
            "PageVar::update_in_place length does not fit: len={}, current_len={}, max={}",
            val.len(),
            self.len(),
            MemVar::MAX_LEN
        );
        if val.len() > PAGE_VAR_LEN_INLINE {
            let offset = u16::from_le_bytes([self.b[2], self.b[3]]) as usize;
            // SAFETY: the caller supplies live, disjoint storage for the old
            // outlined payload. The checked new length fits that allocation;
            // inline old values cannot reach this branch after the fit check.
            let target = unsafe { from_raw_parts_mut(ptr.add(offset), val.len()) };
            target.copy_from_slice(val);
            self.b[..2].copy_from_slice(&(val.len() as u16).to_le_bytes());
            self.b[4..].copy_from_slice(&val[..PAGE_VAR_LEN_PREFIX]);
        } else {
            *self = Self::inline(val);
        }
    }

    /// Creates a page varlen field from its raw little-endian representation.
    #[expect(dead_code, reason = "reserved from_u64")]
    #[inline]
    pub(crate) fn from_u64(raw: u64) -> Self {
        PageVar {
            b: raw.to_le_bytes(),
        }
    }

    /// Returns the raw little-endian representation.
    #[inline]
    pub(crate) fn into_u64(self) -> u64 {
        u64::from_le_bytes(self.b)
    }

    /// Creates a page varlen field from stored bytes.
    #[inline]
    pub(crate) fn from_le_bytes(b: [u8; PAGE_VAR_HEADER]) -> Self {
        PageVar { b }
    }
}

/// Owned binary bytes, stored inline up to 14 bytes and outlined otherwise.
///
/// Values contain at most [`Self::MAX_LEN`] bytes. Slice, array, and vector
/// `TryFrom` conversions return `InvalidDmlInput` for oversized inputs, as do
/// [`Self::try_from_bytes`] and [`Self::try_from_vec`]. Bytes need not be UTF-8.
#[repr(C)]
pub union MemVar {
    i: MemVarInline,
    o: ManuallyDrop<MemVarOutline>,
}

impl MemVar {
    /// Maximum byte length representable by owned values and page descriptors.
    /// A value within this limit may still exceed a row page's available capacity.
    pub const MAX_LEN: usize = u16::MAX as usize;

    /// Copies bytes, returning `InvalidDmlInput` if they exceed [`Self::MAX_LEN`].
    /// This validates length but does not make allocation failure recoverable.
    #[inline]
    pub fn try_from_bytes(data: &[u8]) -> Result<Self> {
        // Keep report construction outside the successful constructor's inline body.
        #[cold]
        fn invalid_length(len: usize) -> Error {
            MemVar::invalid_length(len)
                .attach("MemVar::try_from_bytes")
                .disclose()
        }

        if checked_var_len(data.len()).is_none() {
            return Err(invalid_length(data.len()));
        }
        Ok(Self::from_bytes(data))
    }

    /// Consumes a byte vector, returning `InvalidDmlInput` if it exceeds [`Self::MAX_LEN`].
    /// The vector is consumed on error too; allocation failure is not recoverable.
    #[inline]
    pub fn try_from_vec(data: Vec<u8>) -> Result<Self> {
        // Keep report construction outside the successful constructor's inline body.
        #[cold]
        fn invalid_length(data: Vec<u8>) -> Error {
            MemVar::invalid_length(data.len())
                .attach("MemVar::try_from_vec")
                .disclose()
        }

        if checked_var_len(data.len()).is_none() {
            return Err(invalid_length(data));
        }
        Ok(Self::from_vec(data))
    }

    /// Copies bytes whose length is established by an internal storage or catalog contract.
    /// Panics in release builds if that invariant is violated. Runtime inputs must
    /// use the public fallible constructors instead.
    #[inline]
    pub(crate) fn from_bytes(data: &[u8]) -> Self {
        if data.len() <= MEM_VAR_LEN_INLINE {
            Self::inline(data)
        } else {
            assert!(
                checked_var_len(data.len()).is_some(),
                "MemVar::from_bytes length exceeds limit: len={}, max={}",
                data.len(),
                Self::MAX_LEN
            );
            Self::outline(data)
        }
    }

    /// Consumes bytes whose length is established by an internal catalog contract.
    /// Panics in release builds if that invariant is violated, before ownership transfer.
    #[inline]
    pub(crate) fn from_vec(data: Vec<u8>) -> Self {
        assert!(
            checked_var_len(data.len()).is_some(),
            "MemVar::from_vec length exceeds limit: len={}, max={}",
            data.len(),
            Self::MAX_LEN
        );
        if data.len() <= MEM_VAR_LEN_INLINE {
            Self::inline(&data)
        } else {
            Self::outline_boxed_slice(data.into_boxed_slice())
        }
    }

    #[cold]
    fn invalid_length(len: usize) -> Report<OperationError> {
        Report::new(OperationError::InvalidDmlInput).attach(format!(
            "variable byte length exceeds limit: len={len}, max={}",
            Self::MAX_LEN
        ))
    }

    /// Creates an inline value without allocating.
    ///
    /// # Panics
    /// Panics if `data` exceeds 14 bytes. Use [`Self::try_from_bytes`] for
    /// runtime input that should select its storage mode automatically.
    #[inline]
    pub fn inline(data: &[u8]) -> Self {
        assert!(
            data.len() <= MEM_VAR_LEN_INLINE,
            "MemVar::inline length out of range: len={}, max={MEM_VAR_LEN_INLINE}",
            data.len()
        );
        let mut bytes = [MaybeUninit::uninit(); MEM_VAR_LEN_INLINE];
        bytes[..data.len()].write_copy_of_slice(data);
        Self {
            i: MemVarInline {
                len: data.len() as u16,
                data: bytes,
            },
        }
    }

    /// Copies bytes into an outlined allocation.
    ///
    /// # Panics
    /// Panics unless the length is in `15..=Self::MAX_LEN`.
    #[inline]
    pub fn outline(data: &[u8]) -> Self {
        assert!(
            data.len() > MEM_VAR_LEN_INLINE && checked_var_len(data.len()).is_some(),
            "MemVar::outline length out of range: len={}, min={}, max={}",
            data.len(),
            MEM_VAR_LEN_INLINE + 1,
            Self::MAX_LEN
        );
        Self::outline_boxed_slice(Box::from(data))
    }

    /// Takes ownership of an outlined boxed slice.
    ///
    /// # Panics
    /// Panics unless the length is in `15..=Self::MAX_LEN`.
    #[inline]
    pub fn outline_boxed_slice(data: Box<[u8]>) -> Self {
        assert!(
            data.len() > MEM_VAR_LEN_INLINE && checked_var_len(data.len()).is_some(),
            "MemVar::outline_boxed_slice length out of range: len={}, min={}, max={}",
            data.len(),
            MEM_VAR_LEN_INLINE + 1,
            Self::MAX_LEN
        );
        let len = data.len() as u16;
        let mut prefix = [0; MEM_VAR_LEN_PREFIX];
        prefix.copy_from_slice(&data[..MEM_VAR_LEN_PREFIX]);
        // All checks and prefix preparation precede ownership transfer. The
        // unchanged length is retained to reconstruct this exact box in Drop.
        let ptr = Box::into_raw(data).cast::<u8>();
        Self {
            o: ManuallyDrop::new(MemVarOutline { len, prefix, ptr }),
        }
    }

    /// Returns length of the value.
    #[expect(clippy::len_without_is_empty, reason = "code style")]
    #[inline]
    pub fn len(&self) -> usize {
        // SAFETY: the C-layout union and both C-layout members share the same
        // initialized leading u16 length, regardless of the active member.
        unsafe { self.i.len as usize }
    }

    /// Returns whether the value is inlined.
    #[inline]
    pub fn is_inlined(&self) -> bool {
        self.len() <= MEM_VAR_LEN_INLINE
    }

    /// Returns inpage length of given value.
    /// If the value can be inlined, returns 0.
    #[inline]
    pub fn outline_len(data: &[u8]) -> usize {
        if data.len() > MEM_VAR_LEN_INLINE {
            data.len()
        } else {
            0
        }
    }

    /// Borrows only the initialized payload bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        let len = self.len();
        if len <= MEM_VAR_LEN_INLINE {
            // SAFETY: len selects the inline member. Construction initializes
            // exactly this prefix; MaybeUninit tail slots are never exposed.
            // The slice borrows this member for no longer than self.
            unsafe { from_raw_parts(self.i.data.as_ptr().cast::<u8>(), len) }
        } else {
            // SAFETY: len selects the outlined member, which uniquely owns a
            // live Box<[u8]> of exactly len initialized bytes for self's lifetime.
            unsafe { from_raw_parts(self.o.ptr, len) }
        }
    }

    /// Checks the payload as UTF-8 without allocating or changing its bytes.
    #[inline]
    pub fn try_as_str(&self) -> StdResult<&str, Utf8Error> {
        from_utf8(self.as_bytes())
    }

    /// Borrows the payload as UTF-8 text.
    ///
    /// # Panics
    /// Panics on invalid UTF-8. Use [`Self::try_as_str`] for arbitrary bytes.
    #[inline]
    pub fn as_str(&self) -> &str {
        self.try_as_str()
            .unwrap_or_else(|err| panic!("MemVar::as_str requires valid UTF-8: {err}"))
    }
}

impl Hash for MemVar {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl PartialEq for MemVar {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for MemVar {}

impl Clone for MemVar {
    #[inline]
    fn clone(&self) -> Self {
        if self.is_inlined() {
            // SAFETY: len selects the inline member. Copying MaybeUninit slots
            // preserves the initialized prefix without reading the unused tail.
            unsafe { Self { i: self.i } }
        } else {
            Self::outline(self.as_bytes())
        }
    }
}

impl Drop for MemVar {
    #[inline]
    fn drop(&mut self) {
        let len = self.len();
        if len > MEM_VAR_LEN_INLINE {
            // SAFETY: len selects the outlined member and is unchanged since
            // Box::into_raw. This value uniquely owns that allocation; rebuilding
            // its original slice metadata drops it exactly once with its layout.
            unsafe {
                drop(Box::from_raw(slice_from_raw_parts_mut(self.o.ptr, len)));
            }
        }
    }
}

impl TryFrom<&[u8]> for MemVar {
    type Error = Error;

    #[inline]
    fn try_from(value: &[u8]) -> Result<Self> {
        Self::try_from_bytes(value)
    }
}

impl<const LEN: usize> TryFrom<&[u8; LEN]> for MemVar {
    type Error = Error;

    #[inline]
    fn try_from(value: &[u8; LEN]) -> Result<Self> {
        Self::try_from_bytes(value)
    }
}

impl TryFrom<Vec<u8>> for MemVar {
    type Error = Error;

    #[inline]
    fn try_from(value: Vec<u8>) -> Result<Self> {
        Self::try_from_vec(value)
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct MemVarInline {
    len: u16,
    data: [MaybeUninit<u8>; MEM_VAR_LEN_INLINE],
}

#[repr(C, align(8))]
struct MemVarOutline {
    len: u16,
    prefix: [u8; MEM_VAR_LEN_PREFIX],
    ptr: *mut u8,
}

/// Checks whether a variable byte length fits the owned and stored representations.
#[inline]
pub(crate) fn checked_var_len(len: usize) -> Option<u16> {
    u16::try_from(len).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use std::array::from_fn;
    use std::f64::consts::PI as PI_F64;
    use std::hash::DefaultHasher;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    fn assert_contract_panic<T>(operation: impl FnOnce() -> T, diagnostic: &str) {
        let result = catch_unwind(AssertUnwindSafe(operation));
        let Err(panic) = result else {
            panic!("expected contract rejection: {diagnostic}");
        };
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap();
        assert!(message.contains(diagnostic), "{message}");
    }

    fn check_binary_construction<const N: usize>() {
        let bytes: [u8; N] = from_fn(|idx| if idx % 2 == 0 { 0xff } else { 0 });
        let direct = if N <= MEM_VAR_LEN_INLINE {
            MemVar::inline(&bytes)
        } else {
            MemVar::outline(&bytes)
        };
        // Typed results also exercise the blanket TryInto adapters for every byte source.
        let array_var: Result<MemVar> = (&bytes).try_into();
        let slice_var: Result<MemVar> = bytes.as_slice().try_into();
        let vec_var: Result<MemVar> = bytes.to_vec().try_into();
        let array_val: Result<Val> = (&bytes).try_into();
        let slice_val: Result<Val> = bytes.as_slice().try_into();
        let vec_val: Result<Val> = bytes.to_vec().try_into();
        let values = [
            Val::VarByte(direct),
            Val::VarByte(array_var.unwrap()),
            Val::VarByte(slice_var.unwrap()),
            Val::VarByte(vec_var.unwrap()),
            Val::VarByte(MemVar::try_from_bytes(&bytes).unwrap()),
            Val::VarByte(MemVar::try_from_vec(bytes.to_vec()).unwrap()),
            Val::try_from_bytes(&bytes).unwrap(),
            Val::try_from_vec(bytes.to_vec()).unwrap(),
            array_val.unwrap(),
            slice_val.unwrap(),
            vec_val.unwrap(),
        ];
        for value in values {
            let clone = value.clone();
            assert_eq!(value.as_bytes(), Some(bytes.as_slice()), "len={N}");
            assert_eq!(clone, value, "len={N}");
            assert_ne!(clone, Val::Null);
            let Val::VarByte(var) = &value else {
                unreachable!()
            };
            let Val::VarByte(cloned_var) = &clone else {
                unreachable!()
            };
            assert_eq!(var.len(), N);
            assert_eq!(var.is_inlined(), N <= MEM_VAR_LEN_INLINE);
            assert!(var == cloned_var);
            let hash = |v: &MemVar| {
                let mut h = DefaultHasher::new();
                v.hash(&mut h);
                h.finish()
            };
            assert_eq!(hash(var), hash(cloned_var));
            if N > MEM_VAR_LEN_INLINE {
                assert_ne!(var.as_bytes().as_ptr(), cloned_var.as_bytes().as_ptr());
            }
            assert_eq!(
                format!("{value:?}"),
                if N == 0 {
                    "bytes()".to_owned()
                } else {
                    format!("bytes({bytes:?})")
                }
            );
            let mut encoded = vec![0; value.ser_len()];
            assert_eq!(value.ser(&mut encoded[..], 0), encoded.len());
            assert_eq!(&encoded[..4], &11u32.to_le_bytes());
            assert_eq!(&encoded[4..6], &(N as u16).to_le_bytes());
            assert_eq!(&encoded[6..], bytes.as_slice());
            let (end, decoded) = Val::deser(encoded.as_slice(), 0).unwrap();
            assert_eq!(end, encoded.len());
            assert_eq!(decoded, value);
            drop(value);
            assert_eq!(clone.as_bytes(), Some(bytes.as_slice()));
        }
        if N > MEM_VAR_LEN_INLINE {
            let boxed = MemVar::outline_boxed_slice(bytes.to_vec().into_boxed_slice());
            assert_eq!(boxed.as_bytes(), bytes);
        }
        let text = "x".repeat(N);
        let text_val: Result<Val> = text.as_str().try_into();
        assert_eq!(text_val.unwrap().as_str(), Some(text.as_str()));
    }

    fn check_oversized_construction<const N: usize>() {
        let bytes = [b'x'; N];
        for (constructor, err) in [
            (
                "MemVar::try_from_bytes",
                MemVar::try_from_bytes(&bytes).err().unwrap(),
            ),
            (
                "MemVar::try_from_vec",
                MemVar::try_from_vec(bytes.to_vec()).err().unwrap(),
            ),
            (
                "Val::try_from_bytes",
                Val::try_from_bytes(&bytes).unwrap_err(),
            ),
            (
                "Val::try_from_vec",
                Val::try_from_vec(bytes.to_vec()).unwrap_err(),
            ),
            (
                "MemVar::try_from_bytes",
                MemVar::try_from(bytes.as_slice()).err().unwrap(),
            ),
            (
                "MemVar::try_from_bytes",
                MemVar::try_from(&bytes).err().unwrap(),
            ),
            (
                "MemVar::try_from_vec",
                MemVar::try_from(bytes.to_vec()).err().unwrap(),
            ),
            (
                "Val::try_from_bytes",
                Val::try_from(bytes.as_slice()).unwrap_err(),
            ),
            ("Val::try_from_bytes", Val::try_from(&bytes).unwrap_err()),
            (
                "Val::try_from_vec",
                Val::try_from(bytes.to_vec()).unwrap_err(),
            ),
            (
                "Val::try_from_bytes",
                Val::try_from("x".repeat(N).as_str()).unwrap_err(),
            ),
        ] {
            assert_eq!(err.kind(), ErrorKind::Operation);
            assert_eq!(
                err.report().downcast_ref::<OperationError>(),
                Some(&OperationError::InvalidDmlInput)
            );
            let report = format!("{err:?}");
            for fact in [
                constructor.to_owned(),
                format!("len={N}"),
                "max=65535".to_owned(),
            ] {
                assert!(report.contains(&fact), "{report}");
            }
        }
        assert_contract_panic(|| MemVar::from_bytes(&bytes), "MemVar::from_bytes length");
        assert_contract_panic(
            || MemVar::from_vec(bytes.to_vec()),
            "MemVar::from_vec length",
        );
        assert_contract_panic(|| MemVar::outline(&bytes), "MemVar::outline length");
        assert_contract_panic(
            || MemVar::outline_boxed_slice(bytes.to_vec().into_boxed_slice()),
            "MemVar::outline_boxed_slice length",
        );
    }

    /// Purpose: Protect owned binary values across storage and representational length boundaries.
    /// Expected: Every conversion preserves exact bytes, independent clones, logical comparisons, and the wire format.
    #[test]
    fn test_owned_binary_length_boundaries() {
        check_binary_construction::<0>();
        check_binary_construction::<1>();
        check_binary_construction::<6>();
        check_binary_construction::<7>();
        check_binary_construction::<13>();
        check_binary_construction::<14>();
        check_binary_construction::<15>();
        check_binary_construction::<65535>();
        assert_eq!(mem::align_of::<MemVar>(), 8);
    }

    /// Purpose: Reject lengths that would truncate the owned discriminator or violate an explicit storage mode.
    /// Expected: Public conversions preserve typed length diagnostics; explicit modes and internal invariant constructors reject misuse.
    #[test]
    fn test_owned_constructor_rejections() {
        check_oversized_construction::<65536>();
        check_oversized_construction::<65551>();
        assert_contract_panic(|| MemVar::inline(&[0; 15]), "MemVar::inline length");
        for len in [0, 1, 6, 7, 14] {
            let bytes = vec![0; len];
            assert_contract_panic(|| MemVar::outline(&bytes), "MemVar::outline length");
            assert_contract_panic(
                || MemVar::outline_boxed_slice(bytes.into_boxed_slice()),
                "MemVar::outline_boxed_slice length",
            );
        }
    }

    /// Purpose: Protect safe text access to arbitrary inline and outlined binary payloads.
    /// Expected: Valid UTF-8 borrows unchanged text, invalid bytes retain native UTF-8 errors, and only the legacy MemVar accessor panics.
    #[test]
    fn test_checked_utf8_access() {
        for text in ["", "a\0b", "你好", "你好，世界！hello"] {
            let var = MemVar::try_from(text.as_bytes()).unwrap();
            assert_eq!(var.try_as_str(), Ok(text));
            assert_eq!(var.as_str(), text);
            assert_eq!(var.try_as_str().unwrap().as_ptr(), var.as_bytes().as_ptr());
            let val = Val::try_from(text).unwrap();
            assert_eq!(val.try_as_str(), Ok(Some(text)));
            assert_eq!(val.as_str(), Some(text));
        }
        for bytes in [
            vec![0xff, 0],
            vec![0xe4, 0xbd],
            vec![0xff; 15],
            [b"long binary bytes".as_slice(), &[0xe4]].concat(),
        ] {
            let expected = from_utf8(&bytes).unwrap_err();
            let var = MemVar::try_from(bytes.as_slice()).unwrap();
            assert_eq!(var.try_as_str(), Err(expected));
            assert_contract_panic(|| var.as_str(), "MemVar::as_str requires valid UTF-8");
            let val = Val::try_from(bytes.as_slice()).unwrap();
            assert_eq!(val.try_as_str(), Err(expected));
            assert_eq!(val.as_str(), None);
            assert_eq!(val.as_bytes(), Some(bytes.as_slice()));
        }
        for val in [Val::Null, Val::I32(3)] {
            assert_eq!(val.try_as_str(), Ok(None));
            assert_eq!(val.as_str(), None);
        }
    }

    /// Purpose: Protect page descriptor modes and in-place update bounds before mutation.
    /// Expected: Invalid modes, prefixes, and growth leave bytes untouched, while accepted outlined updates refresh their prefix.
    #[test]
    fn test_page_var_contracts() {
        assert_contract_panic(|| PageVar::inline(&[0; 7]), "PageVar::inline length");
        assert_contract_panic(
            || PageVar::outline(6, 0, b"abcd"),
            "requires outlined length",
        );
        for prefix in [b"abc".as_slice(), b"abcde"] {
            assert_contract_panic(|| PageVar::outline(7, 0, prefix), "prefix length mismatch");
        }
        for initial in [b"inline".as_slice(), b"outlined initial"] {
            let mut page = vec![0xa5; MemVar::MAX_LEN + 1];
            page[..initial.len()].copy_from_slice(initial);
            let mut var = if initial.len() <= PAGE_VAR_LEN_INLINE {
                PageVar::inline(initial)
            } else {
                PageVar::outline(initial.len() as u16, 0, &initial[..4])
            };
            for len in [initial.len() + 1, MemVar::MAX_LEN + 1] {
                let bytes = vec![0; len];
                let before = page.clone();
                let descriptor = var.into_u64();
                assert_contract_panic(
                    || {
                        // SAFETY: page backs the existing range and is disjoint from
                        // bytes; the method must reject a new length that cannot fit.
                        unsafe { var.update_in_place(page.as_mut_ptr(), &bytes) };
                    },
                    "PageVar::update_in_place length does not fit",
                );
                assert_eq!(var.into_u64(), descriptor);
                assert_eq!(page, before);
            }
        }
        let mut page = [0; 16];
        let mut var = PageVar::outline(16, 0, b"old!");
        // SAFETY: page holds the complete existing outlined range and does not alias input.
        unsafe { var.update_in_place(page.as_mut_ptr(), b"new data") };
        assert_eq!(&var.b[4..], b"new ");
        assert_eq!(var.as_bytes(&page), b"new data");
    }

    /// Purpose: Preserve checked borrowed decoding at the maximum byte length and malformed wire boundaries.
    /// Expected: Valid bytes borrow the wire payload exactly; truncation, invalid tags, and offset overflow return typed integrity errors.
    #[test]
    fn test_borrowed_decode_length_boundaries() {
        let mut encoded = Vec::from(11u32.to_le_bytes());
        encoded.extend(u16::MAX.to_le_bytes());
        encoded.extend(vec![0xff; MemVar::MAX_LEN]);
        let (end, val) = ValRef::deser(encoded.as_slice(), 0).unwrap();
        assert_eq!(end, encoded.len());
        let ValRef::VarByte(bytes) = val else {
            panic!("expected borrowed bytes")
        };
        assert_eq!(bytes, &encoded[6..]);
        assert_eq!(bytes.as_ptr(), encoded[6..].as_ptr());
        for input in [
            &encoded[..3],
            &encoded[..5],
            &encoded[..encoded.len() - 1],
            &[0xff; 4],
        ] {
            let err = ValRef::deser(input, 0).unwrap_err();
            assert_eq!(err.current_context(), &DataIntegrityError::InvalidPayload);
        }
        let err = ValRef::deser(encoded.as_slice(), usize::MAX).unwrap_err();
        assert_eq!(err.current_context(), &DataIntegrityError::InvalidPayload);
    }

    /// Purpose: Protect public disclosure of an invalid value-kind tag.
    /// Expected: The error retains its data-integrity classification and native invalid-payload cause.
    #[test]
    fn test_val_kind_try_from_preserves_data_integrity_source() {
        let err = ValKind::try_from(257u32).expect_err("invalid value-kind tag must fail");

        assert_eq!(err.kind(), ErrorKind::DataIntegrity);
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    /// Purpose: Protect the compact variable-value header layouts.
    /// Expected: Memory and page representations retain their specified header sizes.
    #[test]
    fn test_var_len() {
        assert!(mem::size_of::<MemVar>() == MEM_VAR_HEADER);
        assert!(mem::size_of::<PageVar>() == PAGE_VAR_HEADER);
    }

    /// Purpose: Protect memory-value access across inline and outlined storage.
    /// Expected: Bytes, text, length, and outlined-storage requirements match the input.
    #[test]
    fn test_mem_var() {
        let var1 = MemVar::try_from(&b"hello"[..]).unwrap();
        assert!(var1.is_inlined());
        assert!(var1.len() == 5);
        assert!(var1.as_bytes() == b"hello");
        assert!(var1.as_str() == "hello");
        assert!(MemVar::outline_len(b"hello") == 0);

        let var2 = MemVar::try_from(&b"a long value stored outline"[..]).unwrap();
        assert!(!var2.is_inlined());
        assert!(var2.len() == 27);
        assert!(var2.as_bytes() == b"a long value stored outline");
        assert!(var2.as_str() == "a long value stored outline");
        assert!(MemVar::outline_len(b"a long value stored outline") == 27);
    }

    /// Purpose: Protect the persisted representation of all value variants.
    /// Expected: Type tags and payload bytes match the specified format and decode to the original values.
    #[test]
    fn test_val_serde() {
        // serialize and deserialize null
        let val = Val::Null;
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        assert!(buf == b"\x00\x00\x00\x00");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::Null);

        // serialize and deserialize u8
        let val = Val::from(42u8);
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        assert!(buf == b"\x02\x00\x00\x00\x2a");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(42u8));

        // serialize and deserialize i8
        let val = Val::from(-42i8);
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        // i8 code is 1, value -42 is 0xd6 in two's complement
        assert!(buf == b"\x01\x00\x00\x00\xd6");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(-42i8));

        // serialize and deserialize u16
        let val = Val::from(1200u16);
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        assert!(buf == b"\x04\x00\x00\x00\xb0\x04");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(1200u16));

        // serialize and deserialize i16
        let val = Val::from(-1200i16);
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        // i16 code is 3, value -1200 is 0xfb50 in two's complement (little-endian)
        assert!(buf == b"\x03\x00\x00\x00\x50\xfb");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(-1200i16));

        // serialize and deserialize u32
        let val = Val::from(0xdefcab12u32);
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        assert!(buf == b"\x06\x00\x00\x00\x12\xab\xfc\xde");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(0xdefcab12u32));

        // serialize and deserialize i32
        let val = Val::from(-0x12345678i32);
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        // i32 code is 5, value -0x12345678 is 0xedcba988 in two's complement (little-endian)
        // -0x12345678 = 0xedcba988
        assert!(buf == b"\x05\x00\x00\x00\x88\xa9\xcb\xed");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(-0x12345678i32));

        // serialize and deserialize u64
        let val = Val::from(0x1234567890abcdefu64);
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        assert!(buf == b"\x09\x00\x00\x00\xef\xcd\xab\x90\x78\x56\x34\x12");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(0x1234567890abcdefu64));

        // serialize and deserialize i64
        let val = Val::from(-0x1234567890abcdefi64);
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        // i64 code is 8, value -0x1234567890abcdef is 0xedcba9876f543211 in two's complement (little-endian)
        // -0x1234567890abcdef = 0xedcba9876f543211
        assert!(buf == b"\x08\x00\x00\x00\x11\x32\x54\x6f\x87\xa9\xcb\xed");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(-0x1234567890abcdefi64));

        // serialize and deserialize f32
        let val = Val::from(f32::from_bits(0x4048_f5c3));
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        // f32 code is 7, value 3.14f32 bits: 0x4048f5c3
        assert!(buf == b"\x07\x00\x00\x00\xc3\xf5\x48\x40");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(f32::from_bits(0x4048_f5c3)));

        // serialize and deserialize f64
        let val = Val::from(PI_F64);
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        // f64 code is 10, value 3.141592653589793 bits: 0x400921fb54442d18
        assert!(buf == b"\x0a\x00\x00\x00\x18\x2d\x44\x54\xfb\x21\x09\x40");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::from(PI_F64));

        // serialize and deserialize bytes
        let val = Val::try_from(&b"hello"[..]).unwrap();
        let mut buf = vec![0; val.ser_len()];
        val.ser(&mut buf[..], 0);
        assert!(buf == b"\x0b\x00\x00\x00\x05\x00\x68\x65\x6c\x6c\x6f");

        let (_, val) = Val::deser(&buf[..], 0).unwrap();
        assert!(val == Val::try_from(&b"hello"[..]).unwrap());
    }

    /// Purpose: Protect value-type metadata serialization including nonzero offsets.
    /// Expected: Kind and nullability survive encoding with correct field placement and consumption.
    #[test]
    fn test_valtype_serde() {
        // 测试用例1：非空的固定长度类型
        let val_type = ValType {
            kind: ValKind::I32,
            nullable: false,
        };
        let mut buf = vec![0; val_type.ser_len()];
        val_type.ser(&mut buf[..], 0);

        // 验证序列化结果
        assert_eq!(buf.len(), 5);
        assert_eq!(&buf[..4], &(ValKind::I32 as u32).to_le_bytes());
        assert_eq!(buf[4], 0); // false

        // 验证反序列化结果
        let (_, deserialized) = ValType::deser(&buf[..], 0).unwrap();
        assert_eq!(deserialized.kind, ValKind::I32);
        assert!(!deserialized.nullable);

        // 测试用例2：可空的变长类型
        let val_type = ValType {
            kind: ValKind::VarByte,
            nullable: true,
        };
        let mut buf = vec![0; val_type.ser_len()];
        val_type.ser(&mut buf[..], 0);

        // 验证序列化结果
        assert_eq!(buf.len(), 5);
        assert_eq!(&buf[..4], &(ValKind::VarByte as u32).to_le_bytes());
        assert_eq!(buf[4], 1); // true

        // 验证反序列化结果
        let (_, deserialized) = ValType::deser(&buf[..], 0).unwrap();
        assert_eq!(deserialized.kind, ValKind::VarByte);
        assert!(deserialized.nullable);

        // 测试用例3：测试所有ValKind类型
        let kinds = vec![
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
        ];

        for kind in kinds {
            let val_type = ValType {
                kind,
                nullable: true,
            };
            let mut buf = vec![0; val_type.ser_len()];
            val_type.ser(&mut buf[..], 0);

            let (_, deserialized) = ValType::deser(&buf[..], 0).unwrap();
            assert_eq!(deserialized.kind, kind);
            assert!(deserialized.nullable);
        }

        // 测试用例4：测试序列化位置偏移
        let val_type = ValType {
            kind: ValKind::I64,
            nullable: true,
        };
        let mut buf = vec![0; 4 + val_type.ser_len()]; // 添加4字节前缀
        val_type.ser(&mut buf[..], 4); // 从位置4开始序列化

        // 验证序列化结果
        assert_eq!(&buf[4..8], &(ValKind::I64 as u32).to_le_bytes());
        assert_eq!(buf[8], 1); // true

        // 验证反序列化结果
        let (next_pos, deserialized) = ValType::deser(&buf[..], 4).unwrap();
        assert_eq!(next_pos, 9); // 应该前进5个字节
        assert_eq!(deserialized.kind, ValKind::I64);
        assert!(deserialized.nullable);
    }

    /// Purpose: Protect type and byte-length distinctions in value hashing.
    /// Expected: The sampled distinct scalar types and trailing-byte payloads yield different hashes.
    #[test]
    fn test_val_hash() {
        use std::hash::DefaultHasher;
        let hash1 = {
            let v1 = Val::U8(1);
            let mut h = DefaultHasher::new();
            v1.hash(&mut h);
            h.finish()
        };
        let hash2 = {
            let v1 = Val::U16(1);
            let mut h = DefaultHasher::new();
            v1.hash(&mut h);
            h.finish()
        };
        let hash3 = {
            let v1 = Val::U32(1);
            let mut h = DefaultHasher::new();
            v1.hash(&mut h);
            h.finish()
        };
        let hash4 = {
            let v1 = Val::U64(1);
            let mut h = DefaultHasher::new();
            v1.hash(&mut h);
            h.finish()
        };
        assert!(hash1 != hash2 && hash1 != hash3 && hash1 != hash4);

        let hash5 = {
            let v1 = Val::VarByte(MemVar::inline(b"hello"));
            let mut h = DefaultHasher::new();
            v1.hash(&mut h);
            h.finish()
        };
        let hash6 = {
            let v1 = Val::VarByte(MemVar::inline(b"hello  "));
            let mut h = DefaultHasher::new();
            v1.hash(&mut h);
            h.finish()
        };
        assert!(hash5 != hash6);
    }

    /// Purpose: Protect inline page-value metadata and access.
    /// Expected: Inline bytes retain their length and require no external offset.
    #[test]
    fn test_page_var_inline() {
        let data = b"hello";
        let var = PageVar::inline(data);
        assert!(var.is_inlined());
        assert_eq!(var.len(), data.len());
        // SAFETY: inline `PageVar` values ignore the page pointer argument.
        assert_eq!(unsafe { var.as_bytes_unchecked(std::ptr::null()) }, data);
        assert_eq!(var.offset(), None);
    }

    /// Purpose: Protect page-relative access to an outlined value.
    /// Expected: The recorded offset and length locate the original bytes in the backing page.
    #[test]
    fn test_page_var_outline() {
        let data = b"a long string that needs outline storage";
        let mut page_data = [0u8; 100];
        let offset = 10;
        let prefix = &data[..PAGE_VAR_LEN_PREFIX];

        // Store data in page
        page_data[offset..offset + data.len()].copy_from_slice(data);

        let var = PageVar::outline(data.len() as u16, offset as u16, prefix);
        assert!(!var.is_inlined());
        assert_eq!(var.len(), data.len());
        assert_eq!(var.offset(), Some(offset));
        // SAFETY: `page_data` contains the outlined bytes at the recorded
        // offset for this test value.
        assert_eq!(unsafe { var.as_bytes_unchecked(page_data.as_ptr()) }, data);
    }

    /// Purpose: Protect page-value metadata at the inline storage boundary.
    /// Expected: Length, storage mode, and external space requirements agree with each named payload.
    #[test]
    fn test_page_var_storage_boundaries() {
        for (name, data, inline) in [
            ("empty", b"".as_slice(), true),
            ("short", b"short".as_slice(), true),
            ("inline_limit", b"inline".as_slice(), true),
            ("first_outline", b"outline".as_slice(), false),
            (
                "long",
                b"a long string that exceeds inline limit".as_slice(),
                false,
            ),
            (
                "outline_space",
                b"a long string that needs outline storage".as_slice(),
                false,
            ),
            (
                "outline_mode",
                b"this will be stored outline".as_slice(),
                false,
            ),
        ] {
            let value = if inline {
                PageVar::inline(data)
            } else {
                PageVar::outline(data.len() as u16, 0, &data[..PAGE_VAR_LEN_PREFIX])
            };
            assert_eq!(value.len(), data.len(), "{name}");
            assert_eq!(value.is_inlined(), inline, "{name}");
            assert_eq!(
                PageVar::outline_len(data),
                if inline { 0 } else { data.len() },
                "{name}"
            );
        }
    }

    /// Purpose: Protect shrinking a page value across the inline storage boundary.
    /// Expected: Updates preserve the new bytes and switch storage form when the payload fits inline.
    #[test]
    fn test_page_var_update_in_place() {
        let mut page_data = vec![0u8; 100];
        let offset = 30;
        let original_data = b"original data";
        let updated_data = b"updated";
        let short_data = b"short";

        // Store original data
        page_data[offset..offset + original_data.len()].copy_from_slice(original_data);

        let mut var = PageVar::outline(
            original_data.len() as u16,
            offset as u16,
            &original_data[..PAGE_VAR_LEN_PREFIX],
        );

        // Update with shorter data (should not switch to inline)
        // SAFETY: `page_data` backs the outlined payload referenced by `var`.
        unsafe { var.update_in_place(page_data.as_mut_ptr(), updated_data) };
        assert!(!var.is_inlined());
        assert_eq!(var.len(), updated_data.len());
        assert_eq!(
            // SAFETY: `page_data` still backs the outlined payload after the
            // in-place update above.
            unsafe { var.as_bytes_unchecked(page_data.as_ptr()) },
            updated_data
        );
        // Shrinking below the inline limit should switch storage form.
        // SAFETY: `page_data` backs the outlined payload referenced by `var`.
        unsafe { var.update_in_place(page_data.as_mut_ptr(), short_data) };
        assert!(var.is_inlined());
        assert_eq!(var.len(), short_data.len());
        assert_eq!(
            // SAFETY: after shrinking to inline storage, the page pointer is
            // unused.
            unsafe { var.as_bytes_unchecked(std::ptr::null()) },
            short_data
        );
    }

    /// Purpose: Protect cloning an outlined owned value.
    /// Expected: The clone owns independent bytes that remain valid after the original is dropped.
    #[test]
    fn test_val_clone() {
        let v1 = Val::try_from("000000000000000").unwrap();
        let v2 = v1.clone();
        println!("v1={:?}", v1.as_bytes());
        println!("v2={:?}", v2.as_bytes());
        assert!(v1 == v2);
        assert_ne!(
            v1.as_bytes().unwrap().as_ptr(),
            v2.as_bytes().unwrap().as_ptr()
        );
        drop(v1);
        assert_eq!(v2.as_bytes().unwrap(), b"000000000000000");
    }

    /// Purpose: Protect diagnostic formatting across value variants.
    /// Expected: Output identifies scalar types and distinguishes text from non-UTF-8 byte payloads.
    #[test]
    fn test_val_debug_fmt() {
        assert!(format!("{:?}", Val::Null) == "Null");
        assert!(format!("{:?}", Val::I8(-8)) == "i8(-8)");
        assert!(format!("{:?}", Val::U8(8)) == "u8(8)");
        assert!(format!("{:?}", Val::I16(-16)) == "i16(-16)");
        assert!(format!("{:?}", Val::U16(16)) == "u16(16)");
        assert!(format!("{:?}", Val::I32(-32)) == "i32(-32)");
        assert!(format!("{:?}", Val::U32(32)) == "u32(32)");
        assert!(format!("{:?}", Val::F32(OrderedFloat(1.5))) == "f32(1.5)");
        assert!(format!("{:?}", Val::I64(-64)) == "i64(-64)");
        assert!(format!("{:?}", Val::U64(64)) == "u64(64)");
        assert!(format!("{:?}", Val::F64(OrderedFloat(2.5))) == "f64(2.5)");

        let utf8 = Val::VarByte(MemVar::try_from(&b"hi"[..]).unwrap());
        assert!(format!("{:?}", utf8) == "bytes(hi)");
        let non_utf8 = Val::VarByte(MemVar::try_from(&[0xffu8][..]).unwrap());
        assert!(format!("{:?}", non_utf8) == "bytes([255])");
    }

    /// Purpose: Protect borrowed scalar views at numeric boundaries.
    /// Expected: Views preserve variants, integer values, and exact floating-point bits.
    #[test]
    fn test_value_views_preserve_variants_and_scalar_bits() {
        let cases = [
            (Val::Null, ValRef::Null),
            (Val::I8(i8::MIN), ValRef::I8(i8::MIN)),
            (Val::I8(i8::MAX), ValRef::I8(i8::MAX)),
            (Val::U8(u8::MIN), ValRef::U8(u8::MIN)),
            (Val::U8(u8::MAX), ValRef::U8(u8::MAX)),
            (Val::I16(i16::MIN), ValRef::I16(i16::MIN)),
            (Val::I16(i16::MAX), ValRef::I16(i16::MAX)),
            (Val::U16(u16::MIN), ValRef::U16(u16::MIN)),
            (Val::U16(u16::MAX), ValRef::U16(u16::MAX)),
            (Val::I32(i32::MIN), ValRef::I32(i32::MIN)),
            (Val::I32(i32::MAX), ValRef::I32(i32::MAX)),
            (Val::U32(u32::MIN), ValRef::U32(u32::MIN)),
            (Val::U32(u32::MAX), ValRef::U32(u32::MAX)),
            (Val::I64(i64::MIN), ValRef::I64(i64::MIN)),
            (Val::I64(i64::MAX), ValRef::I64(i64::MAX)),
            (Val::U64(u64::MIN), ValRef::U64(u64::MIN)),
            (Val::U64(u64::MAX), ValRef::U64(u64::MAX)),
        ];
        for (owned, expected) in cases {
            assert_eq!(owned.view(), expected);
            assert_eq!(owned.kind(), expected.kind());
            assert_eq!(owned.is_null(), expected.is_null());
            for kind in [ValKind::I8, ValKind::U64, ValKind::VarByte] {
                let matches = expected.is_null() || expected.kind() == Some(kind);
                assert_eq!(owned.matches_kind(kind), matches);
                assert_eq!(expected.matches_kind(kind), matches);
            }
            assert_eq!(format!("{owned:?}"), format!("{expected:?}"));
        }
        for value in [
            0.0f32,
            -0.0,
            1.5,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::from_bits(0x7fc01234),
        ] {
            let owned = Val::from(value);
            let ValRef::F32(view) = owned.view() else {
                panic!("expected f32")
            };
            assert_eq!(view.to_bits(), value.to_bits());
            assert_eq!(owned.kind(), Some(ValKind::F32));
            assert_eq!(format!("{:?}", owned.view()), format!("f32({value})"));
        }
        for value in [
            0.0f64,
            -0.0,
            2.5,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::from_bits(0x7ff8000000001234),
        ] {
            let owned = Val::from(value);
            let ValRef::F64(view) = owned.view() else {
                panic!("expected f64")
            };
            assert_eq!(view.to_bits(), value.to_bits());
            assert_eq!(owned.kind(), Some(ValKind::F64));
            assert_eq!(format!("{:?}", owned.view()), format!("f64({value})"));
        }
    }

    /// Purpose: Protect borrowed byte views across storage boundaries.
    /// Expected: Views borrow the original bytes without copying and preserve byte-specific diagnostics.
    #[test]
    fn test_value_views_borrow_inline_and_outlined_bytes() {
        for len in [0, 6, 7, 14, 15, 128] {
            let input = vec![b'x'; len];
            let owned = Val::try_from(input.as_slice()).unwrap();
            let ValRef::VarByte(bytes) = owned.view() else {
                panic!("expected bytes")
            };
            assert_eq!(bytes, input);
            assert_eq!(bytes.as_ptr(), owned.as_bytes().unwrap().as_ptr());
            assert_eq!(bytes.len(), len);
            assert!(!owned.view().is_null());
            assert_eq!(owned.view().kind(), Some(ValKind::VarByte));
            assert_eq!(
                format!("{:?}", owned.view()),
                format!("bytes({})", "x".repeat(len))
            );
        }
        assert_ne!(ValRef::Null, ValRef::VarByte(b""));
        for (bytes, expected) in [
            (b"hello".as_slice(), "bytes(hello)"),
            (&[0xff, 0, 0x80], "bytes([255, 0, 128])"),
        ] {
            assert_eq!(format!("{:?}", ValRef::VarByte(bytes)), expected);
            assert_eq!(format!("{:?}", Val::try_from(bytes).unwrap()), expected);
        }
    }
}
