use crate::error::Result;
use crate::serde::{Deser, Ser, SerdeCtx};
use doradb_datatype::konst::{ValidF32, ValidF64};
use doradb_datatype::memcmp::{
    attach_null, MemCmpFormat, NullableMemCmpFormat, MIN_VAR_MCF_LEN, MIN_VAR_NMCF_LEN,
};
use doradb_datatype::PreciseType;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};
use std::alloc::{alloc, dealloc, Layout as AllocLayout};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering};

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
    pub fn inline_len(self) -> usize {
        self.kind.inline_len()
    }

    #[inline]
    pub fn memcmp_encoded_len(self) -> Option<usize> {
        if self.kind.is_fixed() {
            let len = self.kind.inline_len() + if self.nullable { 1 } else { 0 };
            return Some(len);
        }
        None
    }

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
        let (idx, nullable) = ctx.deser_u8(data, idx)?;
        Ok((
            idx,
            ValType {
                kind: ValKind::from(kind),
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
    I64 = 7,
    U64 = 8,
    F32 = 9,
    F64 = 10,
    VarByte = 11,
}

impl ValKind {
    #[inline]
    pub fn layout(self) -> Layout {
        match self {
            ValKind::I8 | ValKind::U8 => Layout::Byte1,
            ValKind::I16 | ValKind::U16 => Layout::Byte2,
            ValKind::I32 | ValKind::U32 | ValKind::F32 => Layout::Byte4,
            ValKind::I64 | ValKind::U64 | ValKind::F64 => Layout::Byte8,
            ValKind::VarByte => Layout::VarByte,
        }
    }

    #[inline]
    pub fn inline_len(self) -> usize {
        self.layout().inline_len()
    }

    #[inline]
    pub fn is_fixed(self) -> bool {
        self.layout().is_fixed()
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

impl From<u8> for ValKind {
    #[inline]
    fn from(value: u8) -> Self {
        unsafe { mem::transmute(value) }
    }
}

impl From<PreciseType> for ValKind {
    #[inline]
    fn from(value: PreciseType) -> Self {
        match value {
            PreciseType::Int(1, false) => ValKind::U8,
            PreciseType::Int(1, true) => ValKind::I8,
            PreciseType::Int(2, false) => ValKind::U16,
            PreciseType::Int(2, true) => ValKind::I16,
            PreciseType::Int(4, false) => ValKind::U32,
            PreciseType::Int(4, true) => ValKind::I32,
            PreciseType::Int(8, false) => ValKind::U64,
            PreciseType::Int(8, true) => ValKind::I64,
            PreciseType::Int(_, _) => unreachable!(),
            PreciseType::Float(4) => ValKind::F32,
            PreciseType::Float(8) => ValKind::F64,
            PreciseType::Bool => ValKind::U8,
            PreciseType::Decimal(_, _)
            | PreciseType::Date
            | PreciseType::Time(..)
            | PreciseType::Datetime(..)
            | PreciseType::Interval => todo!(),
            PreciseType::Char(_, _) | PreciseType::Varchar(_, _) => ValKind::VarByte,
            PreciseType::Compound => todo!(),
            _ => todo!(),
        }
    }
}

/// Layout defines the memory layout of columns
/// stored in row page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Layout {
    Byte1, // i8, u8, bool, char(1) with ascii/latin charset
    Byte2, // i16, u16, decimal(1-2)
    Byte4, // i32, u32, f32, decimal(3-8)
    Byte8, // i64, u64, f64, decimal(9-18)
    // Byte16,  // decimal(19-38)
    VarByte, // bytes, string, no more than 60k.
}

impl Layout {
    #[inline]
    pub const fn inline_len(&self) -> usize {
        match self {
            Layout::Byte1 => 1,
            Layout::Byte2 => 2,
            Layout::Byte4 => 4,
            Layout::Byte8 => 8,
            // Layout::Byte16 => 16,
            // 2-byte len, 2-byte offset, 4-byte prefix
            // or inline version, 2-byte len, at most 6 inline bytes
            Layout::VarByte => 8,
        }
    }

    #[inline]
    pub fn is_fixed(&self) -> bool {
        match self {
            Layout::VarByte => false,
            _ => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValCode {
    Null = 0,
    Byte1 = 1,
    Byte2 = 2,
    Byte4 = 3,
    Byte8 = 4,
    VarByte = 5,
}

impl From<u8> for ValCode {
    #[inline]
    fn from(value: u8) -> Self {
        unsafe { mem::transmute(value) }
    }
}

/// Val is value representation of row-store.
/// The variable-length data may require new allocation
/// because we cannot rely on page data.
#[derive(Clone, Serialize, Default, Deserialize, Eq, Hash)]
pub enum Val {
    #[default]
    Null,
    Byte1(Byte1Val),
    Byte2(Byte2Val),
    Byte4(Byte4Val),
    Byte8(Byte8Val),
    VarByte(MemVar),
}

unsafe impl Send for Val {}
unsafe impl Sync for Val {}

impl PartialEq for Val {
    #[inline]
    fn eq(&self, rhs: &Self) -> bool {
        match (self, rhs) {
            (Val::Null, Val::Null) => true,
            (Val::Byte1(l), Val::Byte1(r)) => l == r,
            (Val::Byte2(l), Val::Byte2(r)) => l == r,
            (Val::Byte4(l), Val::Byte4(r)) => l == r,
            (Val::Byte8(l), Val::Byte8(r)) => l == r,
            (Val::VarByte(l), Val::VarByte(r)) => l.as_bytes() == r.as_bytes(),
            _ => false,
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
            Val::Byte1(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i8(&self) -> Option<i8> {
        match self {
            Val::Byte1(v) => Some(*v as i8),
            _ => None,
        }
    }

    #[inline]
    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Val::Byte2(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Val::Byte2(v) => Some(*v as i16),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Val::Byte4(v) => Some(*v as i32),
            _ => None,
        }
    }

    #[inline]
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Val::Byte4(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Val::Byte8(v) => Some(*v as i64),
            _ => None,
        }
    }

    #[inline]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Val::Byte8(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_f32(&self) -> Option<ValidF32> {
        match self {
            Val::Byte4(v) => ValidF32::new(f32::from_bits(*v)),
            _ => None,
        }
    }

    #[inline]
    pub fn as_f64(&self) -> Option<ValidF64> {
        match self {
            Val::Byte8(v) => ValidF64::new(f64::from_bits(*v)),
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
    fn encode_mcf(&self, kind: ValKind, buf: &mut Vec<u8>) {
        match kind {
            ValKind::I8 => MemCmpFormat::attach_mcf(&self.as_i8().unwrap(), buf),
            ValKind::U8 => MemCmpFormat::attach_mcf(&self.as_u8().unwrap(), buf),
            ValKind::I16 => MemCmpFormat::attach_mcf(&self.as_i16().unwrap(), buf),
            ValKind::U16 => MemCmpFormat::attach_mcf(&self.as_u16().unwrap(), buf),
            ValKind::I32 => MemCmpFormat::attach_mcf(&self.as_i32().unwrap(), buf),
            ValKind::U32 => MemCmpFormat::attach_mcf(&self.as_u32().unwrap(), buf),
            ValKind::I64 => MemCmpFormat::attach_mcf(&self.as_i64().unwrap(), buf),
            ValKind::U64 => MemCmpFormat::attach_mcf(&self.as_u64().unwrap(), buf),
            ValKind::F32 => MemCmpFormat::attach_mcf(&self.as_f32().unwrap(), buf),
            ValKind::F64 => MemCmpFormat::attach_mcf(&self.as_f64().unwrap(), buf),
            ValKind::VarByte => MemCmpFormat::attach_mcf(self.as_bytes().unwrap(), buf),
        }
    }

    #[inline]
    fn encode_nmcf(&self, kind: ValKind, buf: &mut Vec<u8>) {
        if self.is_null() {
            attach_null(buf);
            return;
        }
        match kind {
            ValKind::I8 => NullableMemCmpFormat::attach_nmcf(&self.as_i8().unwrap(), buf),
            ValKind::U8 => NullableMemCmpFormat::attach_nmcf(&self.as_u8().unwrap(), buf),
            ValKind::I16 => NullableMemCmpFormat::attach_nmcf(&self.as_i16().unwrap(), buf),
            ValKind::U16 => NullableMemCmpFormat::attach_nmcf(&self.as_u16().unwrap(), buf),
            ValKind::I32 => NullableMemCmpFormat::attach_nmcf(&self.as_i32().unwrap(), buf),
            ValKind::U32 => NullableMemCmpFormat::attach_nmcf(&self.as_u32().unwrap(), buf),
            ValKind::I64 => NullableMemCmpFormat::attach_nmcf(&self.as_i64().unwrap(), buf),
            ValKind::U64 => NullableMemCmpFormat::attach_nmcf(&self.as_u64().unwrap(), buf),
            ValKind::F32 => NullableMemCmpFormat::attach_nmcf(&self.as_f32().unwrap(), buf),
            ValKind::F64 => NullableMemCmpFormat::attach_nmcf(&self.as_f64().unwrap(), buf),
            ValKind::VarByte => NullableMemCmpFormat::attach_nmcf(self.as_bytes().unwrap(), buf),
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
        Val::Byte1(value)
    }
}

impl From<i8> for Val {
    #[inline]
    fn from(value: i8) -> Self {
        Val::Byte1(value as u8)
    }
}

impl From<u16> for Val {
    #[inline]
    fn from(value: u16) -> Self {
        Val::Byte2(value)
    }
}

impl From<i16> for Val {
    #[inline]
    fn from(value: i16) -> Self {
        Val::Byte2(value as u16)
    }
}

impl From<u32> for Val {
    #[inline]
    fn from(value: u32) -> Self {
        Val::Byte4(value)
    }
}

impl From<i32> for Val {
    #[inline]
    fn from(value: i32) -> Self {
        Val::Byte4(value as u32)
    }
}

impl From<u64> for Val {
    #[inline]
    fn from(value: u64) -> Self {
        Val::Byte8(value)
    }
}

impl From<i64> for Val {
    #[inline]
    fn from(value: i64) -> Self {
        Val::Byte8(value as u64)
    }
}

impl From<&[u8]> for Val {
    #[inline]
    fn from(value: &[u8]) -> Self {
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
        mem::size_of::<ValCode>()
            + match self {
                Val::Null => 0, // null is encoded with code only.
                Val::Byte1(_) => 1,
                Val::Byte2(_) => 2,
                Val::Byte4(_) => 4,
                Val::Byte8(_) => 8,
                Val::VarByte(v) => mem::size_of::<u16>() + v.len(),
            }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        debug_assert!(start_idx + self.ser_len(ctx) <= out.len());
        let mut idx = start_idx;
        match self {
            Val::Null => {
                out[idx] = ValCode::Null as u8;
                idx += 1;
            }
            Val::Byte1(v) => {
                out[idx] = ValCode::Byte1 as u8;
                out[idx + 1] = *v;
                idx += 2;
            }
            Val::Byte2(v) => {
                out[idx] = ValCode::Byte2 as u8;
                out[idx + 1..idx + 3].copy_from_slice(&v.to_le_bytes());
                idx += 3;
            }
            Val::Byte4(v) => {
                out[idx] = ValCode::Byte4 as u8;
                out[idx + 1..idx + 5].copy_from_slice(&v.to_le_bytes());
                idx += 5;
            }
            Val::Byte8(v) => {
                out[idx] = ValCode::Byte8 as u8;
                out[idx + 1..idx + 9].copy_from_slice(&v.to_le_bytes());
                idx += 9;
            }
            Val::VarByte(v) => {
                out[idx] = ValCode::VarByte as u8;
                out[idx + 1..idx + 3].copy_from_slice(&(v.len() as u16).to_le_bytes());
                out[idx + 3..idx + 3 + v.len()].copy_from_slice(v.as_bytes());
                idx += 3 + v.len();
            }
        }
        idx
    }
}

impl Deser for Val {
    #[inline]
    fn deser<'a>(_ctx: &mut SerdeCtx, input: &'a [u8], start_idx: usize) -> Result<(usize, Self)> {
        let mut idx = start_idx;
        let code = ValCode::from(input[idx]);
        idx += 1;
        match code {
            ValCode::Null => Ok((idx, Val::Null)),
            ValCode::Byte1 => {
                let v = Byte1Val::from(input[idx]);
                Ok((idx + 1, Val::Byte1(v)))
            }
            ValCode::Byte2 => {
                let v = Byte2Val::from_le_bytes(input[idx..idx + 2].try_into()?);
                Ok((idx + 2, Val::Byte2(v)))
            }
            ValCode::Byte4 => {
                let v = Byte4Val::from_le_bytes(input[idx..idx + 4].try_into()?);
                Ok((idx + 4, Val::Byte4(v)))
            }
            ValCode::Byte8 => {
                let v = Byte8Val::from_le_bytes(input[idx..idx + 8].try_into()?);
                Ok((idx + 8, Val::Byte8(v)))
            }
            ValCode::VarByte => {
                let len = u16::from_le_bytes(input[idx..idx + 2].try_into()?);
                let v = MemVar::from(&input[idx + 2..idx + 2 + len as usize]);
                Ok((idx + 2 + len as usize, Val::VarByte(v)))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ValRef<'a> {
    Byte1(Byte1Val),
    Byte2(Byte2Val),
    Byte4(Byte4Val),
    Byte8(Byte8Val),
    VarByte(&'a [u8]),
}

impl<'a> From<u8> for ValRef<'a> {
    #[inline]
    fn from(value: u8) -> Self {
        ValRef::Byte1(value)
    }
}

impl<'a> From<i8> for ValRef<'a> {
    #[inline]
    fn from(value: i8) -> Self {
        ValRef::Byte1(value as u8)
    }
}

impl<'a> From<u16> for ValRef<'a> {
    #[inline]
    fn from(value: u16) -> Self {
        ValRef::Byte2(value)
    }
}

impl<'a> From<i16> for ValRef<'a> {
    #[inline]
    fn from(value: i16) -> Self {
        ValRef::Byte2(value as u16)
    }
}

impl<'a> From<u32> for ValRef<'a> {
    #[inline]
    fn from(value: u32) -> Self {
        ValRef::Byte4(value)
    }
}

impl<'a> From<i32> for ValRef<'a> {
    #[inline]
    fn from(value: i32) -> Self {
        ValRef::Byte4(value as u32)
    }
}

impl<'a> From<f32> for ValRef<'a> {
    #[inline]
    fn from(value: f32) -> Self {
        ValRef::Byte4(u32::from_ne_bytes(value.to_ne_bytes()))
    }
}

impl<'a> From<u64> for ValRef<'a> {
    #[inline]
    fn from(value: u64) -> Self {
        ValRef::Byte8(value)
    }
}

impl<'a> From<i64> for ValRef<'a> {
    #[inline]
    fn from(value: i64) -> Self {
        ValRef::Byte8(value as u64)
    }
}

impl<'a> From<f64> for ValRef<'a> {
    #[inline]
    fn from(value: f64) -> Self {
        ValRef::Byte8(u64::from_ne_bytes(value.to_ne_bytes()))
    }
}

impl<'a> From<&'a [u8]> for ValRef<'a> {
    #[inline]
    fn from(value: &'a [u8]) -> Self {
        ValRef::VarByte(value)
    }
}

impl<'a> From<&'a str> for ValRef<'a> {
    #[inline]
    fn from(value: &'a str) -> Self {
        ValRef::VarByte(value.as_bytes())
    }
}

/// Value is a marker trait to represent
/// fixed-length column value in row page.
pub trait Value: Sized {
    const LAYOUT: Layout;

    unsafe fn atomic_store(&self, ptr: *mut u8);

    unsafe fn atomic_load(ptr: *mut u8) -> Self;
}

pub trait ToValue {
    type Target: Value;

    fn to_val(&self) -> Self::Target;
}

pub type Byte1Val = u8;
pub trait Byte1ValSlice {
    fn as_i8s(&self) -> &[i8];

    fn as_i8s_mut(&mut self) -> &mut [i8];
}

impl Value for Byte1Val {
    const LAYOUT: Layout = Layout::Byte1;
    #[inline]
    unsafe fn atomic_store(&self, ptr: *mut u8) {
        let atom = AtomicU8::from_ptr(ptr);
        atom.store(*self, Ordering::Relaxed);
    }

    #[inline]
    unsafe fn atomic_load(ptr: *mut u8) -> Self {
        let atom = AtomicU8::from_ptr(ptr);
        atom.load(Ordering::Relaxed)
    }
}

impl Byte1ValSlice for [Byte1Val] {
    #[inline]
    fn as_i8s(&self) -> &[i8] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i8s_mut(&mut self) -> &mut [i8] {
        unsafe { mem::transmute(self) }
    }
}

impl ToValue for u8 {
    type Target = Byte1Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self
    }
}

impl ToValue for i8 {
    type Target = Byte1Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self as u8
    }
}

pub type Byte2Val = u16;
pub trait Byte2ValSlice {
    fn as_i16s(&self) -> &[i16];

    fn as_i16s_mut(&mut self) -> &mut [i16];
}
impl Value for Byte2Val {
    const LAYOUT: Layout = Layout::Byte2;
    #[inline]
    unsafe fn atomic_store(&self, ptr: *mut u8) {
        debug_assert!(ptr as usize % 2 == 0);
        let atom = AtomicU16::from_ptr(ptr as *mut _);
        atom.store(*self, Ordering::Relaxed);
    }

    #[inline]
    unsafe fn atomic_load(ptr: *mut u8) -> Self {
        debug_assert!(ptr as usize % 2 == 0);
        let atom = AtomicU16::from_ptr(ptr as *mut _);
        atom.load(Ordering::Relaxed)
    }
}

impl ToValue for u16 {
    type Target = Byte2Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self
    }
}

impl ToValue for i16 {
    type Target = Byte2Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self as u16
    }
}

impl Byte2ValSlice for [Byte2Val] {
    #[inline]
    fn as_i16s(&self) -> &[i16] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i16s_mut(&mut self) -> &mut [i16] {
        unsafe { mem::transmute(self) }
    }
}

pub type Byte4Val = u32;
pub trait Byte4ValSlice {
    fn as_i32s(&self) -> &[i32];

    fn as_i32s_mut(&mut self) -> &mut [i32];

    fn as_f32s(&self) -> &[f32];

    fn as_f32s_mut(&mut self) -> &mut [f32];
}

impl Value for Byte4Val {
    const LAYOUT: Layout = Layout::Byte4;
    #[inline]
    unsafe fn atomic_store(&self, ptr: *mut u8) {
        debug_assert!(ptr as usize % 4 == 0);
        let atom = AtomicU32::from_ptr(ptr as *mut _);
        atom.store(*self, Ordering::Relaxed);
    }

    #[inline]
    unsafe fn atomic_load(ptr: *mut u8) -> Self {
        debug_assert!(ptr as usize % 4 == 0);
        let atom = AtomicU32::from_ptr(ptr as *mut _);
        atom.load(Ordering::Relaxed)
    }
}

impl Byte4ValSlice for [Byte4Val] {
    #[inline]
    fn as_i32s(&self) -> &[i32] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i32s_mut(&mut self) -> &mut [i32] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_f32s(&self) -> &[f32] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_f32s_mut(&mut self) -> &mut [f32] {
        unsafe { mem::transmute(self) }
    }
}

impl ToValue for u32 {
    type Target = Byte4Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self
    }
}

impl ToValue for i32 {
    type Target = Byte4Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self as u32
    }
}

pub type Byte8Val = u64;
pub trait Byte8ValSlice {
    fn as_i64s(&self) -> &[i64];

    fn as_i64s_mut(&mut self) -> &mut [i64];

    fn as_f64s(&self) -> &[f64];

    fn as_f64s_mut(&mut self) -> &mut [f64];
}

impl Value for Byte8Val {
    const LAYOUT: Layout = Layout::Byte8;
    #[inline]
    unsafe fn atomic_store(&self, ptr: *mut u8) {
        debug_assert!(ptr as usize % 8 == 0);
        let atom = AtomicU64::from_ptr(ptr as *mut _);
        atom.store(*self, Ordering::Relaxed);
    }

    #[inline]
    unsafe fn atomic_load(ptr: *mut u8) -> Self {
        debug_assert!(ptr as usize % 8 == 0);
        let atom = AtomicU64::from_ptr(ptr as *mut _);
        atom.load(Ordering::Relaxed)
    }
}

impl ToValue for u64 {
    type Target = Byte8Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self
    }
}

impl ToValue for i64 {
    type Target = Byte8Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self as u64
    }
}

impl Byte8ValSlice for [Byte8Val] {
    #[inline]
    fn as_i64s(&self) -> &[i64] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i64s_mut(&mut self) -> &mut [i64] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_f64s(&self) -> &[f64] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_f64s_mut(&mut self) -> &mut [f64] {
        unsafe { mem::transmute(self) }
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
        let mut inline = MaybeUninit::<PageVarInline>::uninit();
        unsafe {
            let i = inline.assume_init_mut();
            i.len = data.len() as u16;
            i.data[..data.len()].copy_from_slice(data);
            PageVar {
                i: inline.assume_init(),
            }
        }
    }

    /// Create a new PageVar with pointer info.
    /// The prefix length must be 4 bytes.
    #[inline]
    pub fn outline(len: u16, offset: u16, prefix: &[u8]) -> Self {
        debug_assert!(prefix.len() == PAGE_VAR_LEN_PREFIX);
        let mut outline = MaybeUninit::<PageVarOutline>::uninit();
        unsafe {
            let p = outline.assume_init_mut();
            p.len = len;
            p.offset = offset;
            p.prefix.copy_from_slice(prefix);
            PageVar {
                o: outline.assume_init(),
            }
        }
    }

    /// Returns length of the value.
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
    #[inline]
    pub fn as_bytes(&self, ptr: *const u8) -> &[u8] {
        let len = self.len();
        if len <= PAGE_VAR_LEN_INLINE {
            unsafe { &self.i.data[..len] }
        } else {
            unsafe {
                let data = ptr.add(self.o.offset as usize);
                std::slice::from_raw_parts(data, len)
            }
        }
    }

    /// Returns mutable bytes.
    #[inline]
    pub fn as_bytes_mut(&mut self, ptr: *mut u8) -> &mut [u8] {
        let len = self.len();
        if len <= PAGE_VAR_LEN_INLINE {
            unsafe { &mut self.i.data[..len] }
        } else {
            unsafe {
                let data = ptr.add(self.o.offset as usize);
                std::slice::from_raw_parts_mut(data, len)
            }
        }
    }

    /// Returns string.
    #[inline]
    pub fn as_str(&self, ptr: *const u8) -> &str {
        let len = self.len();
        if len <= PAGE_VAR_LEN_INLINE {
            unsafe { std::str::from_utf8_unchecked(&self.i.data[..len]) }
        } else {
            unsafe {
                let data = ptr.add(self.o.offset as usize);
                let bytes = std::slice::from_raw_parts(data, len);
                std::str::from_utf8_unchecked(bytes)
            }
        }
    }

    /// Returns mutable string.
    #[inline]
    pub fn as_str_mut(&mut self, ptr: *mut u8) -> &mut str {
        let len = self.len();
        if len <= PAGE_VAR_LEN_INLINE {
            unsafe { std::str::from_utf8_unchecked_mut(&mut self.i.data[..len]) }
        } else {
            unsafe {
                let data = ptr.add(self.o.offset as usize);
                let bytes = std::slice::from_raw_parts_mut(data, len);
                std::str::from_utf8_unchecked_mut(bytes)
            }
        }
    }

    /// In-place update with given value.
    /// Caller must ensure no extra space is required.
    #[inline]
    pub fn update_in_place(&mut self, ptr: *mut u8, val: &[u8]) {
        debug_assert!(val.len() <= PAGE_VAR_LEN_INLINE || val.len() <= self.len());
        unsafe {
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
                MemVar { o: self.o.clone() }
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

impl<'de> Visitor<'de> for MemVarVisitor {
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
        assert!(var1.as_bytes(std::ptr::null()) == b"hello");
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
        assert!(buf == b"\x01\x2a");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(42u8));

        // serialize and deserialize u16
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(1200u16);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x02\xb0\x04");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(1200u16));

        // serialize and deserialize u32
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(0xdefcab12u32);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x03\x12\xab\xfc\xde");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(0xdefcab12u32));

        // serialize and deserialize u64
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(0x1234567890abcdefu64);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x04\xef\xcd\xab\x90\x78\x56\x34\x12");

        let ctx = &mut SerdeCtx::default();
        let (_, val) = Val::deser(ctx, &buf, 0).unwrap();
        assert!(val == Val::from(0x1234567890abcdefu64));

        // serialize and deserialize bytes
        let ctx = &mut SerdeCtx::default();
        let val = Val::from(&b"hello"[..]);
        let mut buf = vec![0; val.ser_len(ctx)];
        val.ser(ctx, &mut buf, 0);
        assert!(buf == b"\x05\x05\x00\x68\x65\x6c\x6c\x6f");

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
}
