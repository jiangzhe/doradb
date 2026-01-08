use crate::catalog::{IndexAttributes, IndexKey, IndexOrder, IndexSpec};
use crate::compression::bitpacking::*;
use crate::error::{Error, Result};
use semistr::SemiStr;
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Default)]
pub struct SerdeCtx {
    /// Whether to validate checksum when deserializing
    /// an object when checksum is present.
    validate_checksum: bool,
}

impl SerdeCtx {
    /// Set whether to validate checksum.
    #[inline]
    pub fn validate_checksum(mut self, validate_checksum: bool) -> Self {
        self.validate_checksum = validate_checksum;
        self
    }

    /// Serialize a u64 value to a byte slice.
    #[inline]
    pub fn ser_u64(&self, out: &mut [u8], idx: usize, val: u64) -> usize {
        debug_assert!(idx + mem::size_of::<u64>() <= out.len());
        out[idx..idx + mem::size_of::<u64>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<u64>()
    }

    /// Serialize a i64 value to a byte slice.
    #[inline]
    pub fn ser_i64(&self, out: &mut [u8], idx: usize, val: i64) -> usize {
        debug_assert!(idx + mem::size_of::<i64>() <= out.len());
        out[idx..idx + mem::size_of::<i64>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<i64>()
    }

    /// Serialize a f64 value to a byte slice.
    #[inline]
    pub fn ser_f64(&self, out: &mut [u8], idx: usize, val: f64) -> usize {
        debug_assert!(idx + mem::size_of::<f64>() <= out.len());
        out[idx..idx + mem::size_of::<f64>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<f64>()
    }

    /// Serialize a u32 value to a byte slice.
    #[inline]
    pub fn ser_u32(&self, out: &mut [u8], idx: usize, val: u32) -> usize {
        debug_assert!(idx + mem::size_of::<u32>() <= out.len());
        out[idx..idx + mem::size_of::<u32>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<u32>()
    }

    /// Serialize a f32 value to a byte slice.
    #[inline]
    pub fn ser_f32(&self, out: &mut [u8], idx: usize, val: f32) -> usize {
        debug_assert!(idx + mem::size_of::<f32>() <= out.len());
        out[idx..idx + mem::size_of::<f32>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<f32>()
    }

    /// Serialize a i32 value to a byte slice.
    #[inline]
    pub fn ser_i32(&self, out: &mut [u8], idx: usize, val: i32) -> usize {
        debug_assert!(idx + mem::size_of::<i32>() <= out.len());
        out[idx..idx + mem::size_of::<i32>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<i32>()
    }

    /// Serialize a u16 value to a byte slice.
    #[inline]
    pub fn ser_u16(&self, out: &mut [u8], idx: usize, val: u16) -> usize {
        debug_assert!(idx + mem::size_of::<u16>() <= out.len());
        out[idx..idx + mem::size_of::<u16>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<u16>()
    }

    /// Serialize a i16 value to a byte slice.
    #[inline]
    pub fn ser_i16(&self, out: &mut [u8], idx: usize, val: i16) -> usize {
        debug_assert!(idx + mem::size_of::<i16>() <= out.len());
        out[idx..idx + mem::size_of::<i16>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<i16>()
    }

    /// Serialize a u8 value to a byte slice.
    #[inline]
    pub fn ser_u8(&self, out: &mut [u8], idx: usize, val: u8) -> usize {
        debug_assert!(idx + mem::size_of::<u8>() <= out.len());
        out[idx] = val;
        idx + mem::size_of::<u8>()
    }

    /// Serialize a i8 value to a byte slice.
    #[inline]
    pub fn ser_i8(&self, out: &mut [u8], idx: usize, val: i8) -> usize {
        debug_assert!(idx + mem::size_of::<i8>() <= out.len());
        out[idx] = val as u8;
        idx + mem::size_of::<i8>()
    }

    #[inline]
    pub fn ser_slice<'a, T: Ser<'a>>(&self, out: &mut [u8], idx: usize, slice: &[T]) -> usize {
        debug_assert!(idx + slice.ser_len(self) <= out.len());
        slice.ser(self, out, idx)
    }

    #[inline]
    pub fn ser_byte_array<const N: usize>(
        &self,
        out: &mut [u8],
        idx: usize,
        val: &[u8; N],
    ) -> usize {
        debug_assert!(idx + N <= out.len());
        out[idx..idx + N].copy_from_slice(val);
        idx + N
    }

    /// Deserialize a u64 value from a byte slice.
    #[inline]
    pub fn deser_u64(&self, input: &[u8], idx: usize) -> Result<(usize, u64)> {
        debug_assert!(idx + mem::size_of::<u64>() <= input.len());
        let val = u64::from_le_bytes(input[idx..idx + mem::size_of::<u64>()].try_into()?);
        Ok((idx + mem::size_of::<u64>(), val))
    }

    /// Deserialize a i64 value from a byte slice.
    #[inline]
    pub fn deser_i64(&self, input: &[u8], idx: usize) -> Result<(usize, i64)> {
        debug_assert!(idx + mem::size_of::<i64>() <= input.len());
        let val = i64::from_le_bytes(input[idx..idx + mem::size_of::<i64>()].try_into()?);
        Ok((idx + mem::size_of::<i64>(), val))
    }

    /// Deserialize a f64 value from a byte slice.
    #[inline]
    pub fn deser_f64(&self, input: &[u8], idx: usize) -> Result<(usize, f64)> {
        debug_assert!(idx + mem::size_of::<f64>() <= input.len());
        let val = f64::from_le_bytes(input[idx..idx + mem::size_of::<f64>()].try_into()?);
        Ok((idx + mem::size_of::<f64>(), val))
    }

    /// Deserialize a u32 value from a byte slice.
    #[inline]
    pub fn deser_u32(&self, input: &[u8], idx: usize) -> Result<(usize, u32)> {
        debug_assert!(idx + mem::size_of::<u32>() <= input.len());
        let val = u32::from_le_bytes(input[idx..idx + mem::size_of::<u32>()].try_into()?);
        Ok((idx + mem::size_of::<u32>(), val))
    }

    /// Deserialize a i32 value from a byte slice.
    #[inline]
    pub fn deser_i32(&self, input: &[u8], idx: usize) -> Result<(usize, i32)> {
        debug_assert!(idx + mem::size_of::<i32>() <= input.len());
        let val = i32::from_le_bytes(input[idx..idx + mem::size_of::<i32>()].try_into()?);
        Ok((idx + mem::size_of::<i32>(), val))
    }

    /// Deserialize a f32 value from a byte slice.
    #[inline]
    pub fn deser_f32(&self, input: &[u8], idx: usize) -> Result<(usize, f32)> {
        debug_assert!(idx + mem::size_of::<f32>() <= input.len());
        let val = f32::from_le_bytes(input[idx..idx + mem::size_of::<f32>()].try_into()?);
        Ok((idx + mem::size_of::<f32>(), val))
    }

    /// Deserialize a u16 value from a byte slice.
    #[inline]
    pub fn deser_u16(&self, input: &[u8], idx: usize) -> Result<(usize, u16)> {
        debug_assert!(idx + mem::size_of::<u16>() <= input.len());
        let val = u16::from_le_bytes(input[idx..idx + mem::size_of::<u16>()].try_into()?);
        Ok((idx + mem::size_of::<u16>(), val))
    }

    /// Deserialize a i16 value from a byte slice.
    #[inline]
    pub fn deser_i16(&self, input: &[u8], idx: usize) -> Result<(usize, i16)> {
        debug_assert!(idx + mem::size_of::<i16>() <= input.len());
        let val = i16::from_le_bytes(input[idx..idx + mem::size_of::<i16>()].try_into()?);
        Ok((idx + mem::size_of::<i16>(), val))
    }

    /// Deserialize a u8 value from a byte slice.
    #[inline]
    pub fn deser_u8(&self, input: &[u8], idx: usize) -> Result<(usize, u8)> {
        debug_assert!(idx + mem::size_of::<u8>() <= input.len());
        Ok((idx + mem::size_of::<u8>(), input[idx]))
    }

    /// Deserialize a i8 value from a byte slice.
    #[inline]
    pub fn deser_i8(&self, input: &[u8], idx: usize) -> Result<(usize, i8)> {
        debug_assert!(idx + mem::size_of::<i8>() <= input.len());
        Ok((idx + mem::size_of::<i8>(), input[idx] as i8))
    }

    #[inline]
    pub fn deser_byte_array<const N: usize>(
        &self,
        input: &[u8],
        idx: usize,
    ) -> Result<(usize, [u8; N])> {
        debug_assert!(idx + N <= input.len());
        let mut res = [0u8; N];
        res.copy_from_slice(&input[idx..idx + N]);
        Ok((idx + N, res))
    }
}

/// Defines how to serialize self to bytes.
///
/// This trait is designed to write a serialized object with a known
/// size to a fixed-sized buffer.
pub trait Ser<'a> {
    /// length of serialized bytes.
    fn ser_len(&self, ctx: &SerdeCtx) -> usize;

    /// Serialize object into fix-sized byte slice.
    /// The buffer is guaranteed to be big enough.
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize;
}

/// Defines how to deserialize objects from bytes.
///
/// This trait is designed to read a serialized object from a byte slice,
/// and the result is owned by the caller so that it can be passed to
/// different threads.
pub trait Deser: Sized {
    /// Deserialize objects from input.
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)>;
}

impl Ser<'_> for u64 {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u64>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_u64(out, start_idx, *self)
    }
}

impl Deser for u64 {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_u64(input, start_idx)
    }
}

impl Ser<'_> for i32 {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<i32>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_i32(out, start_idx, *self)
    }
}

impl Deser for i32 {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_i32(input, start_idx)
    }
}

impl Ser<'_> for i64 {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<i64>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_i64(out, start_idx, *self)
    }
}

impl Deser for i64 {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_i64(input, start_idx)
    }
}

impl Ser<'_> for u16 {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u16>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_u16(out, start_idx, *self)
    }
}

impl Deser for u16 {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_u16(input, start_idx)
    }
}

impl Ser<'_> for i16 {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<i16>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_i16(out, start_idx, *self)
    }
}

impl Deser for i16 {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_i16(input, start_idx)
    }
}

impl Ser<'_> for u32 {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u32>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_u32(out, start_idx, *self)
    }
}

impl Deser for u32 {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_u32(input, start_idx)
    }
}

impl Ser<'_> for u8 {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u8>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_u8(out, start_idx, *self)
    }
}

impl Deser for u8 {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_u8(input, start_idx)
    }
}

impl Ser<'_> for i8 {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<i8>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_i8(out, start_idx, *self)
    }
}

impl Deser for i8 {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_i8(input, start_idx)
    }
}

impl Ser<'_> for bool {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u8>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_u8(out, start_idx, if *self { 1u8 } else { 0u8 })
    }
}

impl Deser for bool {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_u8(input, start_idx).map(|(idx, v)| (idx, v != 0))
    }
}

impl<const N: usize> Ser<'_> for [u8; N] {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        N
    }

    #[inline]
    fn ser(&self, _ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        debug_assert!(out.len() >= start_idx + N);
        out[start_idx..start_idx + N].copy_from_slice(&self[..]);
        start_idx + N
    }
}

impl<const N: usize> Deser for [u8; N] {
    #[inline]
    fn deser(_ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let mut res = [0u8; N];
        res.copy_from_slice(&input[start_idx..start_idx + N]);
        Ok((start_idx + N, res))
    }
}

impl<'a, T: Ser<'a>> Ser<'a> for [T] {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        // 8-byte vector length + data
        mem::size_of::<u64>() + self.iter().map(|v| v.ser_len(ctx)).sum::<usize>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        let len = self.len();
        idx = ctx.ser_u64(out, idx, len as u64);
        for v in self.iter() {
            idx = v.ser(ctx, out, idx);
        }
        idx
    }
}

impl<T: Deser> Deser for Vec<T> {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (mut idx, len) = ctx.deser_u64(input, start_idx)?;
        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let (idx0, val) = T::deser(ctx, input, idx)?;
            idx = idx0;
            vec.push(val);
        }
        Ok((idx, vec))
    }
}

impl<'a, T: Ser<'a>> Ser<'a> for Option<T> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        // 1-byte bool + data
        match self.as_ref() {
            Some(v) => mem::size_of::<u8>() + v.ser_len(ctx),
            None => mem::size_of::<u8>(),
        }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        match self.as_ref() {
            Some(v) => {
                idx = true.ser(ctx, out, idx);
                idx = v.ser(ctx, out, idx);
            }
            None => {
                idx = false.ser(ctx, out, idx);
            }
        }
        idx
    }
}

impl<T: Deser> Deser for Option<T> {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, flag) = bool::deser(ctx, input, start_idx)?;
        if flag {
            let (idx, v) = T::deser(ctx, input, idx)?;
            Ok((idx, Some(v)))
        } else {
            Ok((idx, None))
        }
    }
}

impl<'a, T: Ser<'a>> Ser<'a> for Box<T> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        self.as_ref().ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        self.as_ref().ser(ctx, out, start_idx)
    }
}

impl<T: Deser> Deser for Box<T> {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        T::deser(ctx, input, start_idx).map(|(idx, v)| (idx, Box::new(v)))
    }
}

impl<'a, K: Ser<'a>, V: Ser<'a>> Ser<'a> for BTreeMap<K, V> {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        mem::size_of::<u64>()
            + self
                .iter()
                .map(|(k, v)| k.ser_len(ctx) + v.ser_len(ctx))
                .sum::<usize>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = ctx.ser_u64(out, idx, self.len() as u64);
        for (k, v) in self.iter() {
            idx = k.ser(ctx, out, idx);
            idx = v.ser(ctx, out, idx);
        }
        idx
    }
}

impl<K: Ord + Deser, V: Deser> Deser for BTreeMap<K, V> {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (mut idx, len) = ctx.deser_u64(input, start_idx)?;
        let mut map = BTreeMap::new();
        for _ in 0..len {
            let (idx0, k) = K::deser(ctx, input, idx)?;
            idx = idx0;
            let (idx0, v) = V::deser(ctx, input, idx)?;
            idx = idx0;
            map.insert(k, v);
        }
        Ok((idx, map))
    }
}

impl Ser<'_> for SemiStr {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u32>() + self.as_bytes().len()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = ctx.ser_u32(out, start_idx, self.len() as u32);
        debug_assert!(idx + self.len() <= out.len());
        let end_idx = idx + self.len();
        out[idx..end_idx].copy_from_slice(self.as_bytes());
        end_idx
    }
}

impl Deser for SemiStr {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, len) = ctx.deser_u32(input, start_idx)?;
        let end_idx = idx + len as usize;
        // here we always validate utf-8 encoding.
        let s = str::from_utf8(&input[idx..end_idx])?;
        Ok((end_idx, SemiStr::new(s)))
    }
}

impl Ser<'_> for IndexOrder {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u8>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_u8(out, start_idx, *self as u8)
    }
}

impl Deser for IndexOrder {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, v) = ctx.deser_u8(input, start_idx)?;
        Ok((idx, IndexOrder::from(v)))
    }
}

impl Ser<'_> for IndexKey {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u16>() + mem::size_of::<u8>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let start_idx = ctx.ser_u16(out, start_idx, self.col_no);
        self.order.ser(ctx, out, start_idx)
    }
}

impl Deser for IndexKey {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, col_no) = ctx.deser_u16(input, start_idx)?;
        let (idx, order) = IndexOrder::deser(ctx, input, idx)?;
        Ok((idx, IndexKey { col_no, order }))
    }
}

impl Ser<'_> for IndexAttributes {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u32>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        ctx.ser_u32(out, start_idx, self.bits())
    }
}

impl Deser for IndexAttributes {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, v) = ctx.deser_u32(input, start_idx)?;
        Ok((idx, IndexAttributes::from_bits_truncate(v)))
    }
}

impl Ser<'_> for IndexSpec {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        self.index_name.ser_len(ctx)
            + self.index_cols.ser_len(ctx)
            + self.index_attributes.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = self.index_name.ser(ctx, out, start_idx);
        let idx = self.index_cols.ser(ctx, out, idx);
        self.index_attributes.ser(ctx, out, idx)
    }
}

impl Deser for IndexSpec {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, index_name) = SemiStr::deser(ctx, input, start_idx)?;
        let (idx, index_cols) = <Vec<IndexKey>>::deser(ctx, input, idx)?;
        let (idx, index_attributes) = IndexAttributes::deser(ctx, input, idx)?;
        Ok((
            idx,
            IndexSpec {
                index_name,
                index_cols,
                index_attributes,
            },
        ))
    }
}

/// A struct that serializes a length-prefixed object.
///
/// This struct is used to serialize a length-prefixed object.
/// The length is serialized as a u64 value.
/// The data is serialized using the Ser trait.
pub struct LenPrefixSerView<'a, H, P> {
    data_len: u32,
    checksum: AtomicU32,
    header: &'a H,
    payload: &'a P,
    _marker: PhantomData<(&'a H, &'a P)>,
}

impl<'a, H: Ser<'a>, P: Ser<'a>> LenPrefixSerView<'a, H, P> {
    /// Create a new LenPrefixStruct.
    #[inline]
    pub fn new(header: &'a H, payload: &'a P, ctx: &SerdeCtx) -> Self {
        let header_len = header.ser_len(ctx);
        let payload_len = payload.ser_len(ctx);
        let data_len = header_len + payload_len;
        assert!(data_len <= u32::MAX as usize);
        Self {
            data_len: data_len as u32,
            checksum: AtomicU32::new(0),
            header,
            payload,
            _marker: PhantomData,
        }
    }
}

impl<H, P> LenPrefixSerView<'_, H, P> {
    /// Get the length of the data.
    #[inline]
    pub fn data_len(&self) -> usize {
        self.data_len as usize
    }

    #[inline]
    pub fn header(&self) -> &H {
        self.header
    }

    /// Get the data.
    #[inline]
    pub fn payload(&self) -> &P {
        self.payload
    }
}

impl<'a, H: Ser<'a>, P: Ser<'a>> Ser<'a> for LenPrefixSerView<'a, H, P> {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u32>() + mem::size_of::<u32>() + self.data_len as usize
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        debug_assert!(
            self.data_len as usize == self.header.ser_len(ctx) + self.payload.ser_len(ctx)
        );
        debug_assert!(start_idx + self.ser_len(ctx) <= out.len());
        let mut idx = start_idx;
        idx = ctx.ser_u32(out, idx, self.data_len);
        // Leave space for checksum
        let checksum_start_idx = idx;
        let checksum_end_idx = idx + mem::size_of::<u32>();
        idx = self.header.ser(ctx, out, checksum_end_idx);
        idx = self.payload.ser(ctx, out, idx);
        // Calculate and store checksum
        let checksum = crc32fast::hash(&out[checksum_end_idx..idx]);
        self.checksum.store(checksum, Ordering::Relaxed);
        let c_idx = ctx.ser_u32(out, checksum_start_idx, checksum);
        debug_assert!(c_idx == checksum_end_idx);
        idx
    }
}

#[inline]
pub fn len_prefix_pod_size(data: &[u8]) -> usize {
    debug_assert!(data.len() >= mem::size_of::<u32>() * 2);
    let len = u32::from_le_bytes(data[..mem::size_of::<u32>()].try_into().unwrap());
    len as usize + mem::size_of::<u32>() * 2
}

pub struct LenPrefixPod<H, P> {
    len: u32,
    checksum: u32,
    pub header: H,
    pub payload: P,
}

impl<H: fmt::Debug, P: fmt::Debug> fmt::Debug for LenPrefixPod<H, P> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LenPrefixPod")
            .field("header", &self.header)
            .field("payload", &self.payload)
            .finish()
    }
}

impl<'a, H: Ser<'a>, P: Ser<'a>> LenPrefixPod<H, P> {
    /// Create a new LenPrefixStruct.
    #[inline]
    pub fn new(header: H, payload: P, ctx: &SerdeCtx) -> Self {
        let header_len = header.ser_len(ctx);
        let payload_len = payload.ser_len(ctx);
        let len = header_len + payload_len;
        assert!(len <= u32::MAX as usize);
        Self {
            len: len as u32,
            checksum: 0,
            header,
            payload,
        }
    }
}

impl<'a, H: Ser<'a>, P: Ser<'a>> Ser<'a> for LenPrefixPod<H, P> {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        self.len as usize + mem::size_of::<u32>() * 2
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        debug_assert!(self.len as usize == self.header.ser_len(ctx) + self.payload.ser_len(ctx));
        debug_assert!(start_idx + self.ser_len(ctx) <= out.len());
        let mut idx = start_idx;
        idx = ctx.ser_u32(out, idx, self.len);
        // Leave space for checksum
        let checksum_start_idx = idx;
        let checksum_end_idx = idx + mem::size_of::<u32>();
        idx = self.header.ser(ctx, out, checksum_end_idx);
        idx = self.payload.ser(ctx, out, idx);
        // Calculate and store checksum
        let checksum = crc32fast::hash(&out[checksum_end_idx..idx]);
        let c_idx = ctx.ser_u32(out, checksum_start_idx, checksum);
        debug_assert!(c_idx == checksum_end_idx);
        idx
    }
}

impl<H: Deser, P: Deser> Deser for LenPrefixPod<H, P> {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, len) = ctx.deser_u32(input, start_idx)?;
        let (idx, checksum) = ctx.deser_u32(input, idx)?;
        let (idx, header, payload) = if ctx.validate_checksum {
            let calculated_checksum = crc32fast::hash(&input[idx..idx + len as usize]);
            if calculated_checksum != checksum {
                return Err(Error::ChecksumMismatch);
            }
            let (idx, header) = H::deser(ctx, input, idx)?;
            let (idx, payload) = P::deser(ctx, input, idx)?;
            (idx, header, payload)
        } else {
            let (idx, header) = H::deser(ctx, input, idx)?;
            let (idx, payload) = P::deser(ctx, input, idx)?;
            (idx, header, payload)
        };
        Ok((
            idx,
            LenPrefixPod {
                len,
                checksum,
                header,
                payload,
            },
        ))
    }
}

impl Ser<'_> for () {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        0
    }

    #[inline]
    fn ser(&self, _ctx: &SerdeCtx, _out: &mut [u8], start_idx: usize) -> usize {
        start_idx
    }
}

impl Deser for () {
    #[inline]
    fn deser(_ctx: &mut SerdeCtx, _input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        Ok((start_idx, ()))
    }
}

/// Serialization of FOR+Bitpacking.
///
/// The format is as below:
///
/// ```text
/// |----------|----------------|
/// | field    | length(B)      |
/// |----------|----------------|
/// | n_bits   | 1              |
/// | len      | 8              |
/// | min      | sizeof(T)      |
/// | packed   | (n_bits*len)/8 |
/// |----------|----------------|
/// ```
///
/// Special case handling:
/// 1. input is empty, n_bits will be set to 0 and no more output.
/// 2. input should not be packed, as no space can be saved,
///    no serializer willbe returned. User should use other method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForBitpackingSer<'a, T> {
    data: &'a [T],
    info: (usize, T),
}

impl<'a, T: BitPackable + Ord> ForBitpackingSer<'a, T> {
    #[inline]
    pub fn new(data: &'a [T]) -> Option<Self> {
        prepare_for_bitpacking(data).map(|info| ForBitpackingSer { data, info })
    }
}

impl<'a, T: BitPackable + Ord + Ser<'a>> Ser<'a> for ForBitpackingSer<'a, T> {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u8>() // number of bits: 0 means empty; 0xFF means no packing.
        + if self.data.is_empty() {
                        0
                    } else {
                        let (n_bits, _) = self.info;
                        mem::size_of::<u64>() // total number of elements
                            + mem::size_of::<T>() // minimum value of all elements
                            + (n_bits * self.data.len()).div_ceil(8) // packed bytes
                    }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = start_idx;
        if self.data.is_empty() {
            ctx.ser_u8(out, idx, 0)
        } else {
            let (n_bits, min) = self.info;
            let idx = ctx.ser_u8(out, idx, n_bits as u8);
            let idx = ctx.ser_u64(out, idx, self.data.len() as u64);
            let idx = min.ser(ctx, out, idx);
            let packed_len = (n_bits * self.data.len()).div_ceil(8);
            match n_bits {
                1 => for_b1_pack(self.data, min, &mut out[idx..idx + packed_len]),
                2 => for_b2_pack(self.data, min, &mut out[idx..idx + packed_len]),
                4 => for_b4_pack(self.data, min, &mut out[idx..idx + packed_len]),
                8 => for_b8_pack(self.data, min, &mut out[idx..idx + packed_len]),
                16 => for_b16_pack(self.data, min, &mut out[idx..idx + packed_len]),
                32 => for_b32_pack(self.data, min, &mut out[idx..idx + packed_len]),
                _ => unreachable!("unexpected number bits of FOR bitpacking"),
            }
            idx + packed_len
        }
    }
}

/// FOR+bitpacking decompression.
pub struct ForBitpackingDeser<T>(pub Vec<T>);

impl<T: BitPackable + Deser> Deser for ForBitpackingDeser<T> {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, n_bits) = ctx.deser_u8(input, start_idx)?;
        let (idx, n_elems) = ctx.deser_u64(input, idx)?;
        let (idx, min) = T::deser(ctx, input, idx)?;
        let n_bytes = (n_elems as usize * n_bits as usize).div_ceil(8);
        if idx + n_bytes > input.len() {
            return Err(Error::InvalidCompressedData);
        }
        let mut data = vec![T::ZERO; n_elems as usize];
        match n_bits {
            1 => for_b1_unpack(&input[idx..idx + n_bytes], min, &mut data),
            2 => for_b2_unpack(&input[idx..idx + n_bytes], min, &mut data),
            4 => for_b4_unpack(&input[idx..idx + n_bytes], min, &mut data),
            8 => for_b8_unpack(&input[idx..idx + n_bytes], min, &mut data),
            16 => for_b16_unpack(&input[idx..idx + n_bytes], min, &mut data),
            32 => for_b32_unpack(&input[idx..idx + n_bytes], min, &mut data),
            _ => return Err(Error::InvalidCompressedData),
        };
        Ok((idx + n_bytes, ForBitpackingDeser(data)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_len_prefix_struct_serde() {
        let mut ctx = SerdeCtx::default().validate_checksum(true);
        let test_struct = TestStruct {
            a: 1,
            b: 2,
            c: 3,
            d: 4,
        };
        let len_prefix_struct = LenPrefixSerView::new(&(), &test_struct, &ctx);
        let mut out = vec![0; len_prefix_struct.ser_len(&ctx)];
        len_prefix_struct.ser(&ctx, &mut out, 0);
        println!("{:?}", out);
        let (idx, pod) = LenPrefixPod::<(), TestStruct>::deser(&mut ctx, &out, 0).unwrap();
        assert_eq!(idx, out.len());
        assert_eq!(pod.payload, test_struct);

        // overwrite checksum so that checksum mismatch
        out[4..8].fill(0);
        let res = LenPrefixPod::<(), TestStruct>::deser(&mut ctx, &out, 0);
        if let Err(Error::ChecksumMismatch) = res {
            println!("expected checksum mismatch");
        } else {
            panic!("unexpected checksum match");
        }
    }

    #[test]
    fn test_vec_serde() {
        let mut ctx = SerdeCtx::default();
        let vec = vec![TestStruct {
            a: 1,
            b: 2,
            c: 3,
            d: 4,
        }];
        let mut out = vec![0; vec.ser_len(&ctx)];
        vec.ser(&ctx, &mut out, 0);
        let (idx, val) = Vec::<TestStruct>::deser(&mut ctx, &out, 0).unwrap();
        assert_eq!(idx, out.len());
        assert_eq!(
            val,
            vec![TestStruct {
                a: 1,
                b: 2,
                c: 3,
                d: 4
            }]
        );
    }

    #[test]
    fn test_btree_map_serde() {
        let mut ctx = SerdeCtx::default();
        let map = BTreeMap::from([(
            1,
            TestStruct {
                a: 1,
                b: 2,
                c: 3,
                d: 4,
            },
        )]);
        let mut out = vec![0; map.ser_len(&ctx)];
        map.ser(&ctx, &mut out, 0);
        let (idx, val) = BTreeMap::<u64, TestStruct>::deser(&mut ctx, &out, 0).unwrap();
        assert_eq!(idx, out.len());
        assert_eq!(val, map);
    }

    #[test]
    fn test_index_spec_serde() {
        let mut ctx = SerdeCtx::default();
        let index_name = SemiStr::new("index1");
        println!("index_name ser_len={}", index_name.ser_len(&ctx));
        let index_cols = vec![
            IndexKey::new(0),
            IndexKey {
                col_no: 1,
                order: IndexOrder::Desc,
            },
        ];
        println!("index_cols ser_len={}", index_cols.ser_len(&ctx));
        let index_attributes = IndexAttributes::PK;
        println!(
            "index_attributes ser_len={}",
            index_attributes.ser_len(&ctx)
        );
        let spec = IndexSpec {
            index_name,
            index_cols,
            index_attributes,
        };
        let len = spec.ser_len(&ctx);
        println!("index_spec ser_len={}", len);
        let mut vec = vec![0u8; len];
        let idx = spec.ser(&ctx, &mut vec, 0);
        assert_eq!(idx, len);
        let (idx, parsed) = IndexSpec::deser(&mut ctx, &vec, 0).unwrap();
        assert_eq!(idx, len);
        assert_eq!(parsed, spec);
    }

    #[test]
    fn test_array_serde() {
        let array = [0u8, 1, 2, 3, 4];
        let mut ctx = SerdeCtx::default();
        let len = array.ser_len(&ctx);
        assert_eq!(len, 5);
        let mut vec = vec![0u8; len];
        let idx = array.ser(&ctx, &mut vec, 0);
        assert_eq!(idx, len);
        let (idx, res) = <[u8; 5]>::deser(&mut ctx, &vec, 0).unwrap();
        assert_eq!(idx, 5);
        assert_eq!(res, array);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct TestStruct {
        a: u64,
        b: u32,
        c: u16,
        d: u8,
    }

    impl Ser<'_> for TestStruct {
        fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
            mem::size_of::<u64>()
                + mem::size_of::<u32>()
                + mem::size_of::<u16>()
                + mem::size_of::<u8>()
        }

        fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
            let mut idx = start_idx;
            idx = ctx.ser_u64(out, idx, self.a);
            idx = ctx.ser_u32(out, idx, self.b);
            idx = ctx.ser_u16(out, idx, self.c);
            ctx.ser_u8(out, idx, self.d)
        }
    }

    impl Deser for TestStruct {
        fn deser<'a>(
            ctx: &mut SerdeCtx,
            input: &'a [u8],
            start_idx: usize,
        ) -> Result<(usize, Self)> {
            let idx = start_idx;
            let (idx, a) = ctx.deser_u64(input, idx)?;
            let (idx, b) = ctx.deser_u32(input, idx)?;
            let (idx, c) = ctx.deser_u16(input, idx)?;
            let (idx, d) = ctx.deser_u8(input, idx)?;
            let res = TestStruct { a, b, c, d };
            Ok((idx, res))
        }
    }

    #[test]
    fn test_for_bitpacking_serde() {
        let mut ctx = SerdeCtx::default();
        for input in vec![
            vec![1u64],
            vec![1, 1 << 1],
            vec![1, 1 << 1, 1 << 2],
            vec![1, 1 << 1, 1 << 2, 1 << 4],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16, 1 << 32],
        ] {
            let bp = ForBitpackingSer::new(&input).unwrap();
            let mut res = vec![0u8; bp.ser_len(&ctx)];
            let ser_idx = bp.ser(&ctx, &mut res, 0);
            assert_eq!(ser_idx, res.len());
            let (de_idx, decompressed) =
                ForBitpackingDeser::<u64>::deser(&mut ctx, &res, 0).unwrap();
            assert_eq!(de_idx, res.len());
            assert_eq!(decompressed.0, input);
        }
        let input = vec![
            1u64,
            1 << 1,
            1 << 2,
            1 << 4,
            1 << 8,
            1 << 16,
            1 << 32,
            1 << 50,
        ];
        let bp = ForBitpackingSer::new(&input);
        assert!(bp.is_none());
    }

    #[test]
    fn test_for_bitpacking_serde_signed_ints() {
        let mut ctx = SerdeCtx::default();

        let input = vec![-10i16, -3, 0, 1, 5, 8];
        let bp = ForBitpackingSer::new(&input).unwrap();
        let mut res = vec![0u8; bp.ser_len(&ctx)];
        let ser_idx = bp.ser(&ctx, &mut res, 0);
        assert_eq!(ser_idx, res.len());
        let (de_idx, decompressed) = ForBitpackingDeser::<i16>::deser(&mut ctx, &res, 0).unwrap();
        assert_eq!(de_idx, res.len());
        assert_eq!(decompressed.0, input);

        let input = vec![-2i32, -1, 0, 1, 2, 4, 8, 128, 1024];
        let bp = ForBitpackingSer::new(&input).unwrap();
        let mut res = vec![0u8; bp.ser_len(&ctx)];
        let ser_idx = bp.ser(&ctx, &mut res, 0);
        assert_eq!(ser_idx, res.len());
        let (de_idx, decompressed) = ForBitpackingDeser::<i32>::deser(&mut ctx, &res, 0).unwrap();
        assert_eq!(de_idx, res.len());
        assert_eq!(decompressed.0, input);
    }

    #[test]
    fn test_scalar_signed_ser_de() {
        let mut ctx = SerdeCtx::default();

        let values = vec![-1024i16, -1, 0, 1, 2048];
        let mut out = vec![0u8; values.ser_len(&ctx)];
        let idx = values.ser(&ctx, &mut out, 0);
        assert_eq!(idx, out.len());
        let (idx, desered) = Vec::<i16>::deser(&mut ctx, &out, 0).unwrap();
        assert_eq!(idx, out.len());
        assert_eq!(desered, values);

        let values = vec![-1i32, 0, 1024];
        let mut out = vec![0u8; values.ser_len(&ctx)];
        let idx = values.ser(&ctx, &mut out, 0);
        assert_eq!(idx, out.len());
        let (idx, desered) = Vec::<i32>::deser(&mut ctx, &out, 0).unwrap();
        assert_eq!(idx, out.len());
        assert_eq!(desered, values);
    }
}
