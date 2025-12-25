use crate::error::{Error, Result};
use crate::memcmp::{
    BytesExtendable, MIN_VAR_MCF_LEN, MIN_VAR_NMCF_LEN, MemCmpFormat, Null, NullableMemCmpFormat,
    SegmentedBytes,
};
use crate::serde::{Deser, Ser, SerdeCtx};
use ordered_float::OrderedFloat;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};
use std::alloc::{Layout as AllocLayout, alloc, dealloc};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::result::Result as StdResult;
use std::sync::atomic::*;

pub const PAGE_VAR_HEADER: usize = 8;
pub const PAGE_VAR_LEN_INLINE: usize = 6;
pub const PAGE_VAR_LEN_PREFIX: usize = 4;
const _: () = assert!(mem::size_of::<PageVar>() == 8);

pub const MEM_VAR_HEADER: usize = 16;
pub const MEM_VAR_LEN_INLINE: usize = 14;
pub const MEM_VAR_LEN_PREFIX: usize = 6;
const _: () = assert!(mem::size_of::<MemVar>() == 16);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValType {
    pub kind: ValKind,
    pub nullable: bool,
}

impl ValType {
    #[inline]
    pub fn new(kind: ValKind, nullable: bool) -> Self {
        ValType { kind, nullable }
    }

    #[inline]
    pub fn inline_len(self) -> usize {
        self.kind.inline_len()
    }

    // todo: replace with MemCmpKey support.
    #[inline]
    pub fn memcmp_encoded_len(self) -> Option<usize> {
        if self.kind.is_fixed() {
            let len = self.kind.inline_len() + if self.nullable { 1 } else { 0 };
            return Some(len);
        }
        None
    }

    // todo: replace with MemCmpKey support.
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
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u8>() + mem::size_of::<u8>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = ctx.ser_u8(out, idx, self.kind as u8);
        ctx.ser_u8(out, idx, self.nullable as u8)
    }
}

impl Deser for ValType {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, data: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let idx = start_idx;
        let (idx, kind) = ctx.deser_u8(data, idx)?;
        let kind = ValKind::try_from(kind)?;
        let (idx, nullable) = ctx.deser_u8(data, idx)?;
        Ok((
            idx,
            ValType {
                kind,
                nullable: nullable != 0,
            },
        ))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
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

    #[inline]
    pub fn is_fixed(self) -> bool {
        !matches!(self, ValKind::VarByte)
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

impl TryFrom<u8> for ValKind {
    type Error = Error;
    #[inline]
    fn try_from(value: u8) -> Result<Self> {
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
            _ => return Err(Error::InvalidFormat),
        };
        Ok(res)
    }
}

/// Val is value representation of row-store.
/// The variable-length data may require new allocation
/// because we cannot rely on page data.
#[derive(Clone, Serialize, Default, Deserialize, Eq)]
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

unsafe impl Send for Val {}
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
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Val::Null)
    }

    #[inline]
    pub fn as_u8(&self) -> Option<u8> {
        match self {
            Val::U8(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i8(&self) -> Option<i8> {
        match self {
            Val::I8(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Val::U16(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Val::I16(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Val::I32(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Val::U32(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Val::I64(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Val::U64(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_f32(&self) -> Option<OrderedFloat<f32>> {
        match self {
            Val::F32(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_f64(&self) -> Option<OrderedFloat<f64>> {
        match self {
            Val::F64(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Val::VarByte(v) => Some(v.as_bytes()),
            _ => None,
        }
    }

    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Val::VarByte(v) => Some(v.as_str()),
            _ => None,
        }
    }

    #[inline]
    pub fn encode_memcmp(&self, ty: ValType, buf: &mut Vec<u8>) {
        if ty.nullable {
            self.encode_nmcf(ty.kind, buf);
        } else {
            self.encode_mcf(ty.kind, buf);
        }
    }

    #[inline]
    pub fn kind(&self) -> Option<ValKind> {
        let kind = match self {
            Val::Null => return None,
            Val::I8(_) => ValKind::I8,
            Val::U8(_) => ValKind::U8,
            Val::I16(_) => ValKind::I16,
            Val::U16(_) => ValKind::U16,
            Val::I32(_) => ValKind::I32,
            Val::U32(_) => ValKind::U32,
            Val::F32(_) => ValKind::F32,
            Val::I64(_) => ValKind::I64,
            Val::U64(_) => ValKind::U64,
            Val::F64(_) => ValKind::F64,
            Val::VarByte(_) => ValKind::VarByte,
        };
        Some(kind)
    }

    #[inline]
    pub fn matches_kind(&self, kind: ValKind) -> bool {
        // null matches all types.
        self.kind().map(|k| k == kind).unwrap_or(true)
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
        f.debug_struct("Val").finish()
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

impl From<&[u8]> for Val {
    #[inline]
    fn from(value: &[u8]) -> Self {
        Val::VarByte(MemVar::from(value))
    }
}

impl<const LEN: usize> From<&[u8; LEN]> for Val {
    #[inline]
    fn from(value: &[u8; LEN]) -> Self {
        Val::VarByte(MemVar::from(value))
    }
}

impl From<&str> for Val {
    #[inline]
    fn from(value: &str) -> Self {
        Val::VarByte(MemVar::from(value.as_bytes()))
    }
}

impl From<Vec<u8>> for Val {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Val::VarByte(MemVar::from(value))
    }
}

impl Ser<'_> for Val {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u8>()
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
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        debug_assert!(start_idx + self.ser_len(ctx) <= out.len());
        let code = self.kind().map(|k| k as u8).unwrap_or(0);
        let idx = ctx.ser_u8(out, start_idx, code);
        match self {
            Val::Null => idx,
            Val::I8(v) => ctx.ser_i8(out, idx, *v),
            Val::U8(v) => ctx.ser_u8(out, idx, *v),
            Val::I16(v) => ctx.ser_i16(out, idx, *v),
            Val::U16(v) => ctx.ser_u16(out, idx, *v),
            Val::I32(v) => ctx.ser_i32(out, idx, *v),
            Val::U32(v) => ctx.ser_u32(out, idx, *v),
            Val::F32(v) => ctx.ser_f32(out, idx, v.0),
            Val::I64(v) => ctx.ser_i64(out, idx, *v),
            Val::U64(v) => ctx.ser_u64(out, idx, *v),
            Val::F64(v) => ctx.ser_f64(out, idx, v.0),
            Val::VarByte(v) => {
                let idx = ctx.ser_u16(out, idx, v.len() as u16);
                out[idx..idx + v.len()].copy_from_slice(v.as_bytes());
                idx + v.len()
            }
        }
    }
}

impl Deser for Val {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let c = input[start_idx];
        let idx = start_idx + 1;
        if c == 0 {
            return Ok((idx, Val::Null));
        }
        let kind = ValKind::try_from(c)?;
        match kind {
            ValKind::I8 => {
                let (idx, v) = ctx.deser_i8(input, idx)?;
                Ok((idx, Val::I8(v)))
            }
            ValKind::U8 => {
                let (idx, v) = ctx.deser_u8(input, idx)?;
                Ok((idx, Val::U8(v)))
            }
            ValKind::I16 => {
                let (idx, v) = ctx.deser_i16(input, idx)?;
                Ok((idx, Val::I16(v)))
            }
            ValKind::U16 => {
                let (idx, v) = ctx.deser_u16(input, idx)?;
                Ok((idx, Val::U16(v)))
            }
            ValKind::I32 => {
                let (idx, v) = ctx.deser_i32(input, idx)?;
                Ok((idx, Val::I32(v)))
            }
            ValKind::U32 => {
                let (idx, v) = ctx.deser_u32(input, idx)?;
                Ok((idx, Val::U32(v)))
            }
            ValKind::F32 => {
                let (idx, v) = ctx.deser_f32(input, idx)?;
                Ok((idx, Val::F32(OrderedFloat(v))))
            }
            ValKind::I64 => {
                let (idx, v) = ctx.deser_i64(input, idx)?;
                Ok((idx, Val::I64(v)))
            }
            ValKind::U64 => {
                let (idx, v) = ctx.deser_u64(input, idx)?;
                Ok((idx, Val::U64(v)))
            }
            ValKind::F64 => {
                let (idx, v) = ctx.deser_f64(input, idx)?;
                Ok((idx, Val::F64(OrderedFloat(v))))
            }
            ValKind::VarByte => {
                let len = u16::from_le_bytes(input[idx..idx + 2].try_into()?);
                let v = MemVar::from(&input[idx + 2..idx + 2 + len as usize]);
                Ok((idx + 2 + len as usize, Val::VarByte(v)))
            }
        }
    }
}

/// Value is a marker trait to represent
/// fixed-length column value in row page.
pub(crate) trait Value: Sized {
    /// Store self value into target position in atomic way.
    ///
    /// # Safety: This method is only used for atomic update on page.
    unsafe fn atomic_store(&self, ptr: *const u8);

    /// Store self value into target position.
    ///
    /// # Safety: This method is only used for atomic update on page.
    unsafe fn store(&self, ptr: *mut u8);
}

macro_rules! impl_value_fixed_size {
    ($t:ty, $at:ident, $size:expr) => {
        impl Value for $t {
            #[inline]
            unsafe fn atomic_store(&self, ptr: *const u8) {
                debug_assert!((ptr as usize).is_multiple_of($size));
                unsafe {
                    let atom = $at::from_ptr(ptr as *mut u8 as *mut $t);
                    atom.store(*self, Ordering::Release);
                }
            }

            #[inline]
            unsafe fn store(&self, ptr: *mut u8) {
                unsafe {
                    *(ptr as *mut $t) = *self;
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
        unsafe {
            let atom = AtomicU32::from_ptr(ptr as *mut u8 as *mut u32);
            atom.store(self.to_bits(), Ordering::Release);
        }
    }

    #[inline]
    unsafe fn store(&self, ptr: *mut u8) {
        unsafe {
            *(ptr as *mut u32) = self.to_bits();
        }
    }
}

impl Value for f64 {
    #[inline]
    unsafe fn atomic_store(&self, ptr: *const u8) {
        debug_assert!((ptr as usize).is_multiple_of(8));
        unsafe {
            let atom = AtomicU64::from_ptr(ptr as *mut u8 as *mut u64);
            atom.store(self.to_bits(), Ordering::Release);
        }
    }

    #[inline]
    unsafe fn store(&self, ptr: *mut u8) {
        unsafe {
            *(ptr as *mut u64) = self.to_bits();
        }
    }
}

/// PageVar represents var-len value in page.
/// It has two kinds: inline and outline.
/// Inline means the bytes are inlined in the fixed field.
/// Outline means the fixed field only store length,
/// offset and prfix. Entire value is located at
/// tail of page.
#[derive(Clone, Copy)]
#[repr(align(8))]
pub union PageVar {
    i: PageVarInline,
    o: PageVarOutline,
}

impl PageVar {
    /// Create a new PageVar with inline data.
    /// The data length must be no more than 6 bytes.
    #[inline]
    pub fn inline(data: &[u8]) -> Self {
        debug_assert!(data.len() <= PAGE_VAR_LEN_INLINE);
        let mut i = PageVarInline {
            len: data.len() as u16,
            data: [0u8; PAGE_VAR_LEN_INLINE],
        };
        i.data[..data.len()].copy_from_slice(data);
        PageVar { i }
    }

    /// Create a new PageVar with pointer info.
    /// The prefix length must be 4 bytes.
    #[inline]
    pub fn outline(len: u16, offset: u16, prefix: &[u8]) -> Self {
        debug_assert!(prefix.len() == PAGE_VAR_LEN_PREFIX);
        let mut o = PageVarOutline {
            len,
            offset,
            prefix: [0u8; PAGE_VAR_LEN_PREFIX],
        };
        o.prefix.copy_from_slice(prefix);
        PageVar { o }
    }

    /// Returns length of the value.
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { self.i.len as usize }
    }

    /// Returns offset if outlined.
    #[inline]
    pub fn offset(&self) -> Option<usize> {
        if self.is_inlined() {
            return None;
        }
        unsafe { Some(self.o.offset as usize) }
    }

    /// Returns whether the value is inlined.
    #[inline]
    pub fn is_inlined(&self) -> bool {
        self.len() <= PAGE_VAR_LEN_INLINE
    }

    /// Returns inpage length of given value.
    /// If the value can be inlined, returns 0.
    #[inline]
    pub fn outline_len(data: &[u8]) -> usize {
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
    /// Caller should make sure ptr is valid.
    #[inline]
    pub unsafe fn as_bytes(&self, ptr: *const u8) -> &[u8] {
        unsafe {
            let len = self.len();
            if len <= PAGE_VAR_LEN_INLINE {
                &self.i.data[..len]
            } else {
                let data = ptr.add(self.o.offset as usize);
                std::slice::from_raw_parts(data, len)
            }
        }
    }

    /// Returns mutable bytes.
    ///
    /// # Safety
    ///
    /// Caller should make sure ptr is valid.
    #[inline]
    pub unsafe fn as_bytes_mut(&mut self, ptr: *mut u8) -> &mut [u8] {
        unsafe {
            let len = self.len();
            if len <= PAGE_VAR_LEN_INLINE {
                &mut self.i.data[..len]
            } else {
                let data = ptr.add(self.o.offset as usize);
                std::slice::from_raw_parts_mut(data, len)
            }
        }
    }

    /// Returns string.
    ///
    /// # Safety
    ///
    /// Caller should make sure ptr is valid.
    #[inline]
    pub unsafe fn as_str(&self, ptr: *const u8) -> &str {
        unsafe {
            let len = self.len();
            if len <= PAGE_VAR_LEN_INLINE {
                std::str::from_utf8_unchecked(&self.i.data[..len])
            } else {
                let data = ptr.add(self.o.offset as usize);
                let bytes = std::slice::from_raw_parts(data, len);
                std::str::from_utf8_unchecked(bytes)
            }
        }
    }

    /// Returns mutable string.
    ///
    /// # Safety
    ///
    /// Caller should make sure ptr is valid.
    #[inline]
    pub unsafe fn as_str_mut(&mut self, ptr: *mut u8) -> &mut str {
        unsafe {
            let len = self.len();
            if len <= PAGE_VAR_LEN_INLINE {
                std::str::from_utf8_unchecked_mut(&mut self.i.data[..len])
            } else {
                let data = ptr.add(self.o.offset as usize);
                let bytes = std::slice::from_raw_parts_mut(data, len);
                std::str::from_utf8_unchecked_mut(bytes)
            }
        }
    }

    /// In-place update with given value.
    /// Caller must ensure no extra space is required.
    ///
    /// # Safety
    ///
    /// Caller should make sure ptr is valid.
    #[inline]
    pub unsafe fn update_in_place(&mut self, ptr: *mut u8, val: &[u8]) {
        unsafe {
            debug_assert!(val.len() <= PAGE_VAR_LEN_INLINE || val.len() <= self.len());

            if val.len() > PAGE_VAR_LEN_INLINE {
                // all not inline, but original is longer or equal to input value.
                debug_assert!(self.len() > PAGE_VAR_LEN_INLINE);
                self.o.len = val.len() as u16;
                let target =
                    std::slice::from_raw_parts_mut(ptr.add(self.o.offset as usize), val.len());
                target.copy_from_slice(val);
            } else {
                // input is inlined.
                // better to reuse release page data.
                self.i.len = val.len() as u16;
                self.i.data[..val.len()].copy_from_slice(val);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
struct PageVarInline {
    len: u16,
    data: [u8; PAGE_VAR_LEN_INLINE],
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
struct PageVarOutline {
    len: u16,
    offset: u16,
    prefix: [u8; PAGE_VAR_LEN_PREFIX],
}

/// VarBytes is similar to PageVar, but more general to use.
/// It does not depend on page data.
pub union MemVar {
    i: MemVarInline,
    o: ManuallyDrop<MemVarOutline>,
}

impl MemVar {
    /// Create a new MemVar with inline data.
    /// The data length must be no more than 14 bytes.
    #[inline]
    pub fn inline(data: &[u8]) -> Self {
        debug_assert!(data.len() <= MEM_VAR_LEN_INLINE);
        unsafe {
            let mut var = MaybeUninit::<MemVar>::uninit();
            let i = &mut var.assume_init_mut().i;
            i.len = data.len() as u16;
            i.data[..data.len()].copy_from_slice(data);
            var.assume_init()
        }
    }

    /// Create a new outlined PageVar.
    #[inline]
    pub fn outline(data: &[u8]) -> Self {
        debug_assert!(data.len() > MEM_VAR_LEN_INLINE && data.len() <= 0xffff); // must be in range of u16
        unsafe {
            let mut var = MaybeUninit::<MemVar>::uninit();
            let o = &mut var.assume_init_mut().o;
            o.len = data.len() as u16;
            o.prefix.copy_from_slice(&data[..MEM_VAR_LEN_PREFIX]);
            let layout = AllocLayout::from_size_align_unchecked(data.len(), 1);
            o.ptr = alloc(layout);
            o.ptr.copy_from_nonoverlapping(data.as_ptr(), data.len());
            var.assume_init()
        }
    }

    #[inline]
    pub fn outline_boxed_slice(data: Box<[u8]>) -> Self {
        debug_assert!(data.len() > MEM_VAR_LEN_INLINE && data.len() <= 0xffff);
        unsafe {
            let mut var = MaybeUninit::<MemVar>::uninit();
            let o = &mut var.assume_init_mut().o;
            o.len = data.len() as u16;
            o.prefix.copy_from_slice(&data[..MEM_VAR_LEN_PREFIX]);
            let ptr = Box::leak(data);
            o.ptr = ptr as *mut [u8] as *mut u8;
            var.assume_init()
        }
    }

    /// Returns length of the value.
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
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

    /// Returns bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        let len = self.len();
        if len <= MEM_VAR_LEN_INLINE {
            unsafe { &self.i.data[..len] }
        } else {
            unsafe { std::slice::from_raw_parts(self.o.ptr, len) }
        }
    }

    /// Returns string.
    #[inline]
    pub fn as_str(&self) -> &str {
        let len = self.len();
        if len <= MEM_VAR_LEN_INLINE {
            unsafe { std::str::from_utf8_unchecked(&self.i.data[..len]) }
        } else {
            unsafe {
                let bytes = std::slice::from_raw_parts(self.o.ptr, len);
                std::str::from_utf8_unchecked(bytes)
            }
        }
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
        unsafe {
            if self.len() > MEM_VAR_LEN_INLINE {
                MemVar {
                    o: ManuallyDrop::new((*self.o).clone()),
                }
            } else {
                MemVar { i: self.i }
            }
        }
    }
}

impl Drop for MemVar {
    #[inline]
    fn drop(&mut self) {
        let len = self.len();
        if len > MEM_VAR_LEN_INLINE {
            unsafe {
                let layout = AllocLayout::from_size_align_unchecked(len, 1);
                dealloc(self.o.ptr, layout);
            }
        }
    }
}

impl Serialize for MemVar {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self.as_bytes())
    }
}

impl<'de> Deserialize<'de> for MemVar {
    #[inline]
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(MemVarVisitor)
    }
}

impl From<&[u8]> for MemVar {
    #[inline]
    fn from(value: &[u8]) -> Self {
        debug_assert!(value.len() <= 0xffff);
        if value.len() <= MEM_VAR_LEN_INLINE {
            Self::inline(value)
        } else {
            Self::outline(value)
        }
    }
}

impl<const LEN: usize> From<&[u8; LEN]> for MemVar {
    #[inline]
    fn from(value: &[u8; LEN]) -> Self {
        debug_assert!(LEN <= 0xffff);
        if LEN <= MEM_VAR_LEN_INLINE {
            Self::inline(value)
        } else {
            Self::outline(value)
        }
    }
}

impl From<Vec<u8>> for MemVar {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        debug_assert!(value.len() <= 0xffff);
        if value.len() <= MEM_VAR_LEN_INLINE {
            Self::inline(&value[..])
        } else {
            Self::outline_boxed_slice(value.into_boxed_slice())
        }
    }
}

struct MemVarVisitor;

impl Visitor<'_> for MemVarVisitor {
    type Value = MemVar;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("byte array")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> StdResult<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() >= 0xffff {
            return fail_long_bytes();
        }
        Ok(MemVar::from(v))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> StdResult<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() >= 0xffff {
            return fail_long_bytes();
        }
        Ok(MemVar::from(v))
    }

    fn visit_str<E>(self, v: &str) -> StdResult<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() >= 0xffff {
            return fail_long_bytes();
        }
        Ok(MemVar::from(v.as_bytes()))
    }

    fn visit_string<E>(self, v: String) -> StdResult<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() >= 0xffff {
            return fail_long_bytes();
        }
        Ok(MemVar::from(v.as_bytes()))
    }
}

#[inline]
fn fail_long_bytes<T, E: serde::de::Error>() -> StdResult<T, E> {
    Err(serde::de::Error::custom(
        "MemVar does not support bytes longer than u16:MAX",
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C, align(8))]
struct MemVarInline {
    len: u16,
    data: [u8; MEM_VAR_LEN_INLINE],
}

#[derive(PartialEq, Eq)]
#[repr(C, align(8))]
struct MemVarOutline {
    len: u16,
    prefix: [u8; MEM_VAR_LEN_PREFIX],
    ptr: *mut u8,
}

impl Clone for MemVarOutline {
    #[inline]
    fn clone(&self) -> Self {
        unsafe {
            let layout = AllocLayout::from_size_align_unchecked(self.len as usize, 1);
            let ptr = alloc(layout);
            std::ptr::copy_nonoverlapping(self.ptr, ptr, self.len as usize);
            MemVarOutline {
                len: self.len,
                prefix: self.prefix,
                ptr,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_var_len() {
        assert!(mem::size_of::<MemVar>() == MEM_VAR_HEADER);
        assert!(mem::size_of::<PageVar>() == PAGE_VAR_HEADER);
    }

    #[test]
    fn test_page_var() {
        let var1 = PageVar::inline(b"hello");
        assert!(var1.is_inlined());
        assert!(var1.len() == 5);
        assert!(unsafe { var1.as_bytes(std::ptr::null()) } == b"hello");
    }

    #[test]
    fn test_mem_var() {
        let var1 = MemVar::from(&b"hello"[..]);
        assert!(var1.is_inlined());
        assert!(var1.len() == 5);
        assert!(var1.as_bytes() == b"hello");
        assert!(var1.as_str() == "hello");
        assert!(MemVar::outline_len(b"hello") == 0);

        let var2 = MemVar::from(&b"a long value stored outline"[..]);
        assert!(!var2.is_inlined());
        assert!(var2.len() == 27);
        assert!(var2.as_bytes() == b"a long value stored outline");
        assert!(var2.as_str() == "a long value stored outline");
        assert!(MemVar::outline_len(b"a long value stored outline") == 27);
    }

    #[test]
    fn test_val_serde() {
        // serialize and deserialize null
        let ctx = &mut SerdeCtx::default();
        let val = Val::Null;
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x00");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::Null);

        // serialize and deserialize u8
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(42u8);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x02\x2a");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(42u8));

        // serialize and deserialize i8
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(-42i8);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        // i8 code is 1, value -42 is 0xd6 in two's complement
        assert!(buf == b"\x01\xd6");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(-42i8));

        // serialize and deserialize u16
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(1200u16);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x04\xb0\x04");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(1200u16));

        // serialize and deserialize i16
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(-1200i16);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        // i16 code is 3, value -1200 is 0xfb50 in two's complement (little-endian)
        assert!(buf == b"\x03\x50\xfb");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(-1200i16));

        // serialize and deserialize u32
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(0xdefcab12u32);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x06\x12\xab\xfc\xde");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(0xdefcab12u32));

        // serialize and deserialize i32
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(-0x12345678i32);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        // i32 code is 5, value -0x12345678 is 0xedcba988 in two's complement (little-endian)
        // -0x12345678 = 0xedcba988
        assert!(buf == b"\x05\x88\xa9\xcb\xed");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(-0x12345678i32));

        // serialize and deserialize u64
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(0x1234567890abcdefu64);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x09\xef\xcd\xab\x90\x78\x56\x34\x12");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(0x1234567890abcdefu64));

        // serialize and deserialize i64
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(-0x1234567890abcdefi64);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        // i64 code is 8, value -0x1234567890abcdef is 0xedcba9876f543211 in two's complement (little-endian)
        // -0x1234567890abcdef = 0xedcba9876f543211
        assert!(buf == b"\x08\x11\x32\x54\x6f\x87\xa9\xcb\xed");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(-0x1234567890abcdefi64));

        // serialize and deserialize f32
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(3.14f32);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        // f32 code is 7, value 3.14f32 bits: 0x4048f5c3
        assert!(buf == b"\x07\xc3\xf5\x48\x40");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(3.14f32));

        // serialize and deserialize f64
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(3.141592653589793f64);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        // f64 code is 10, value 3.141592653589793 bits: 0x400921fb54442d18
        assert!(buf == b"\x0a\x18\x2d\x44\x54\xfb\x21\x09\x40");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(3.141592653589793f64));

        // serialize and deserialize bytes
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(&b"hello"[..]);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x0b\x05\x00\x68\x65\x6c\x6c\x6f");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(&b"hello"[..]));
    }

    #[test]
    fn test_valtype_serde() {
        let mut ctx = SerdeCtx::default();

        // 测试用例1：非空的固定长度类型
        let val_type = ValType {
            kind: ValKind::I32,
            nullable: false,
        };
        let mut buf = vec![0; val_type.ser_len(&ctx)];
        val_type.ser(&ctx, &mut buf, 0);

        // 验证序列化结果
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], ValKind::I32 as u8);
        assert_eq!(buf[1], 0); // false

        // 验证反序列化结果
        let (_, deserialized) = ValType::deser(&mut ctx, &buf, 0).unwrap();
        assert_eq!(deserialized.kind, ValKind::I32);
        assert_eq!(deserialized.nullable, false);

        // 测试用例2：可空的变长类型
        let val_type = ValType {
            kind: ValKind::VarByte,
            nullable: true,
        };
        let mut buf = vec![0; val_type.ser_len(&ctx)];
        val_type.ser(&ctx, &mut buf, 0);

        // 验证序列化结果
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], ValKind::VarByte as u8);
        assert_eq!(buf[1], 1); // true

        // 验证反序列化结果
        let (_, deserialized) = ValType::deser(&mut ctx, &buf, 0).unwrap();
        assert_eq!(deserialized.kind, ValKind::VarByte);
        assert_eq!(deserialized.nullable, true);

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
            let mut buf = vec![0; val_type.ser_len(&ctx)];
            val_type.ser(&ctx, &mut buf, 0);

            let (_, deserialized) = ValType::deser(&mut ctx, &buf, 0).unwrap();
            assert_eq!(deserialized.kind, kind);
            assert_eq!(deserialized.nullable, true);
        }

        // 测试用例4：测试序列化位置偏移
        let val_type = ValType {
            kind: ValKind::I64,
            nullable: true,
        };
        let mut buf = vec![0; 4 + val_type.ser_len(&ctx)]; // 添加4字节前缀
        val_type.ser(&ctx, &mut buf, 4); // 从位置4开始序列化

        // 验证序列化结果
        assert_eq!(buf[4], ValKind::I64 as u8);
        assert_eq!(buf[5], 1); // true

        // 验证反序列化结果
        let (next_pos, deserialized) = ValType::deser(&mut ctx, &buf, 4).unwrap();
        assert_eq!(next_pos, 6); // 应该前进2个字节
        assert_eq!(deserialized.kind, ValKind::I64);
        assert_eq!(deserialized.nullable, true);
    }

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

    #[test]
    fn test_page_var_inline() {
        let data = b"hello";
        let var = PageVar::inline(data);
        assert!(var.is_inlined());
        assert_eq!(var.len(), data.len());
        assert_eq!(unsafe { var.as_bytes(std::ptr::null()) }, data);
        assert_eq!(var.offset(), None);
    }

    #[test]
    fn test_page_var_outline() {
        let data = b"a long string that needs outline storage";
        let mut page_data = vec![0u8; 100];
        let offset = 10;
        let prefix = &data[..PAGE_VAR_LEN_PREFIX];

        // Store data in page
        page_data[offset..offset + data.len()].copy_from_slice(data);

        let var = PageVar::outline(data.len() as u16, offset as u16, prefix);
        assert!(!var.is_inlined());
        assert_eq!(var.len(), data.len());
        assert_eq!(var.offset(), Some(offset));
        assert_eq!(unsafe { var.as_bytes(page_data.as_ptr()) }, data);
    }

    #[test]
    fn test_page_var_len() {
        let short_data = b"short";
        let long_data = b"a long string that exceeds inline limit";

        let short_var = PageVar::inline(short_data);
        let long_var =
            PageVar::outline(long_data.len() as u16, 0, &long_data[..PAGE_VAR_LEN_PREFIX]);

        assert_eq!(short_var.len(), short_data.len());
        assert_eq!(long_var.len(), long_data.len());
    }

    #[test]
    fn test_page_var_is_inlined() {
        let inline_data = b"inline";
        let outline_data = b"this will be stored outline";

        let inline_var = PageVar::inline(inline_data);
        let outline_var = PageVar::outline(
            outline_data.len() as u16,
            0,
            &outline_data[..PAGE_VAR_LEN_PREFIX],
        );

        assert!(inline_var.is_inlined());
        assert!(!outline_var.is_inlined());
    }

    #[test]
    fn test_page_var_outline_len() {
        let inline_data = b"short";
        let outline_data = b"a long string that needs outline storage";

        assert_eq!(PageVar::outline_len(inline_data), 0);
        assert_eq!(PageVar::outline_len(outline_data), outline_data.len());
    }

    #[test]
    fn test_page_var_as_str() {
        let inline_str = "hello";
        let outline_str = "a longer string that requires outline storage";
        let mut page_data = vec![0u8; 100];
        let offset = 20;

        // Store outline data
        page_data[offset..offset + outline_str.len()].copy_from_slice(outline_str.as_bytes());

        let inline_var = PageVar::inline(inline_str.as_bytes());
        let outline_var = PageVar::outline(
            outline_str.len() as u16,
            offset as u16,
            &outline_str.as_bytes()[..PAGE_VAR_LEN_PREFIX],
        );

        assert_eq!(unsafe { inline_var.as_str(std::ptr::null()) }, inline_str);
        assert_eq!(
            unsafe { outline_var.as_str(page_data.as_ptr()) },
            outline_str
        );
    }

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
        unsafe { var.update_in_place(page_data.as_mut_ptr(), updated_data) };
        assert!(!var.is_inlined());
        assert_eq!(var.len(), updated_data.len());
        assert_eq!(unsafe { var.as_bytes(page_data.as_ptr()) }, updated_data);
        // Update with shorter data (should not switch to inline)
        unsafe { var.update_in_place(page_data.as_mut_ptr(), short_data) };
        assert!(var.is_inlined());
        assert_eq!(var.len(), short_data.len());
        assert_eq!(unsafe { var.as_bytes(std::ptr::null()) }, short_data);
    }

    #[test]
    fn test_val_clone() {
        let v1 = Val::from("000000000000000");
        let v2 = v1.clone();
        println!("v1={:?}", v1.as_bytes());
        println!("v2={:?}", v2.as_bytes());
        assert!(v1 == v2);
    }
}
