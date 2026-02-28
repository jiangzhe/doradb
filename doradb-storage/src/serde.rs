use crate::catalog::{IndexAttributes, IndexKey, IndexOrder, IndexSpec};
use crate::compression::bitpacking::*;
use crate::error::{Error, Result};
use semistr::SemiStr;
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::mem;

pub trait Serde {
    /// Serialize a u64 value to a byte slice.
    fn ser_u64(&mut self, idx: usize, val: u64) -> usize;

    /// Serialize a i64 value to a byte slice.
    fn ser_i64(&mut self, idx: usize, val: i64) -> usize;

    /// Serialize a f64 value to a byte slice.
    fn ser_f64(&mut self, idx: usize, val: f64) -> usize;

    /// Serialize a u32 value to a byte slice.
    fn ser_u32(&mut self, idx: usize, val: u32) -> usize;

    /// Serialize a f32 value to a byte slice.
    fn ser_f32(&mut self, idx: usize, val: f32) -> usize;

    /// Serialize a i32 value to a byte slice.
    fn ser_i32(&mut self, idx: usize, val: i32) -> usize;

    /// Serialize a u16 value to a byte slice.
    fn ser_u16(&mut self, idx: usize, val: u16) -> usize;

    /// Serialize a i16 value to a byte slice.
    fn ser_i16(&mut self, idx: usize, val: i16) -> usize;

    /// Serialize a u8 value to a byte slice.
    fn ser_u8(&mut self, idx: usize, val: u8) -> usize;

    /// Serialize a i8 value to a byte slice.
    fn ser_i8(&mut self, idx: usize, val: i8) -> usize;

    /// Serialize bool value.
    #[inline]
    fn ser_bool(&mut self, idx: usize, val: bool) -> usize {
        self.ser_u8(idx, if val { 1 } else { 0 })
    }

    /// Serialize byte slice.
    fn ser_byte_slice(&mut self, idx: usize, val: &[u8]) -> usize;

    /// Serialize fixed size byte array.
    fn ser_byte_array<const N: usize>(&mut self, idx: usize, val: &[u8; N]) -> usize;

    /// Returns a mutable byte slice to caller for serialization.
    fn ser_mut(&mut self, idx: usize, len: usize) -> (usize, &mut [u8]);

    /// Returns size (number of bytes).
    fn size(&self) -> usize;

    /// Deserialize a u64 value from a byte slice.
    fn deser_u64(&self, idx: usize) -> Result<(usize, u64)>;

    /// Deserialize a i64 value from a byte slice.
    fn deser_i64(&self, idx: usize) -> Result<(usize, i64)>;

    /// Deserialize a f64 value from a byte slice.
    fn deser_f64(&self, idx: usize) -> Result<(usize, f64)>;

    /// Deserialize a u32 value from a byte slice.
    fn deser_u32(&self, idx: usize) -> Result<(usize, u32)>;

    /// Deserialize a i32 value from a byte slice.
    fn deser_i32(&self, idx: usize) -> Result<(usize, i32)>;

    /// Deserialize a f32 value from a byte slice.
    fn deser_f32(&self, idx: usize) -> Result<(usize, f32)>;

    /// Deserialize a u16 value from a byte slice.
    fn deser_u16(&self, idx: usize) -> Result<(usize, u16)>;

    /// Deserialize a i16 value from a byte slice.
    fn deser_i16(&self, idx: usize) -> Result<(usize, i16)>;

    /// Deserialize a u8 value from a byte slice.
    fn deser_u8(&self, idx: usize) -> Result<(usize, u8)>;

    /// Deserialize a i8 value from a byte slice.
    fn deser_i8(&self, idx: usize) -> Result<(usize, i8)>;

    /// Deserialize bool value.
    #[inline]
    fn deser_bool(&self, idx: usize) -> Result<(usize, bool)> {
        self.deser_u8(idx).map(|(i, r)| (i, r != 0))
    }

    /// Deserialize byte slice.
    fn deser_byte_slice(&self, idx: usize, len: usize) -> Result<(usize, &[u8])>;

    /// Deserialize fixed size byte array.
    fn deser_byte_array<const N: usize>(&self, idx: usize) -> Result<(usize, [u8; N])>;

    /// Returns a byte slice to caller for deserialization.
    fn deser(&self, idx: usize, len: usize) -> Result<(usize, &[u8])>;
}

impl Serde for [u8] {
    #[inline]
    fn ser_u64(&mut self, idx: usize, val: u64) -> usize {
        debug_assert!(idx + mem::size_of::<u64>() <= self.len());
        self[idx..idx + mem::size_of::<u64>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<u64>()
    }

    #[inline]
    fn ser_i64(&mut self, idx: usize, val: i64) -> usize {
        debug_assert!(idx + mem::size_of::<i64>() <= self.len());
        self[idx..idx + mem::size_of::<i64>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<i64>()
    }

    #[inline]
    fn ser_f64(&mut self, idx: usize, val: f64) -> usize {
        debug_assert!(idx + mem::size_of::<f64>() <= self.len());
        self[idx..idx + mem::size_of::<f64>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<f64>()
    }

    #[inline]
    fn ser_u32(&mut self, idx: usize, val: u32) -> usize {
        debug_assert!(idx + mem::size_of::<u32>() <= self.len());
        self[idx..idx + mem::size_of::<u32>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<u32>()
    }

    #[inline]
    fn ser_f32(&mut self, idx: usize, val: f32) -> usize {
        debug_assert!(idx + mem::size_of::<f32>() <= self.len());
        self[idx..idx + mem::size_of::<f32>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<f32>()
    }

    #[inline]
    fn ser_i32(&mut self, idx: usize, val: i32) -> usize {
        debug_assert!(idx + mem::size_of::<i32>() <= self.len());
        self[idx..idx + mem::size_of::<i32>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<i32>()
    }

    #[inline]
    fn ser_u16(&mut self, idx: usize, val: u16) -> usize {
        debug_assert!(idx + mem::size_of::<u16>() <= self.len());
        self[idx..idx + mem::size_of::<u16>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<u16>()
    }

    #[inline]
    fn ser_i16(&mut self, idx: usize, val: i16) -> usize {
        debug_assert!(idx + mem::size_of::<i16>() <= self.len());
        self[idx..idx + mem::size_of::<i16>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<i16>()
    }

    #[inline]
    fn ser_u8(&mut self, idx: usize, val: u8) -> usize {
        debug_assert!(idx + mem::size_of::<u8>() <= self.len());
        self[idx] = val;
        idx + mem::size_of::<u8>()
    }

    #[inline]
    fn ser_i8(&mut self, idx: usize, val: i8) -> usize {
        debug_assert!(idx + mem::size_of::<i8>() <= self.len());
        self[idx] = val as u8;
        idx + mem::size_of::<i8>()
    }

    #[inline]
    fn ser_byte_slice(&mut self, idx: usize, val: &[u8]) -> usize {
        debug_assert!(idx + val.len() <= self.len());
        self[idx..idx + val.len()].copy_from_slice(val);
        idx + val.len()
    }

    #[inline]
    fn ser_byte_array<const N: usize>(&mut self, idx: usize, val: &[u8; N]) -> usize {
        debug_assert!(idx + N <= self.len());
        self[idx..idx + N].copy_from_slice(val);
        idx + N
    }

    #[inline]
    fn ser_mut(&mut self, idx: usize, len: usize) -> (usize, &mut [u8]) {
        debug_assert!(idx + len <= self.len());
        (idx + len, &mut self[idx..idx + len])
    }

    #[inline]
    fn size(&self) -> usize {
        self.len()
    }

    #[inline]
    fn deser_u64(&self, idx: usize) -> Result<(usize, u64)> {
        debug_assert!(idx + mem::size_of::<u64>() <= self.len());
        let val = u64::from_le_bytes(self[idx..idx + mem::size_of::<u64>()].try_into()?);
        Ok((idx + mem::size_of::<u64>(), val))
    }

    #[inline]
    fn deser_i64(&self, idx: usize) -> Result<(usize, i64)> {
        debug_assert!(idx + mem::size_of::<i64>() <= self.len());
        let val = i64::from_le_bytes(self[idx..idx + mem::size_of::<i64>()].try_into()?);
        Ok((idx + mem::size_of::<i64>(), val))
    }

    #[inline]
    fn deser_f64(&self, idx: usize) -> Result<(usize, f64)> {
        debug_assert!(idx + mem::size_of::<f64>() <= self.len());
        let val = f64::from_le_bytes(self[idx..idx + mem::size_of::<f64>()].try_into()?);
        Ok((idx + mem::size_of::<f64>(), val))
    }

    #[inline]
    fn deser_u32(&self, idx: usize) -> Result<(usize, u32)> {
        debug_assert!(idx + mem::size_of::<u32>() <= self.len());
        let val = u32::from_le_bytes(self[idx..idx + mem::size_of::<u32>()].try_into()?);
        Ok((idx + mem::size_of::<u32>(), val))
    }

    #[inline]
    fn deser_i32(&self, idx: usize) -> Result<(usize, i32)> {
        debug_assert!(idx + mem::size_of::<i32>() <= self.len());
        let val = i32::from_le_bytes(self[idx..idx + mem::size_of::<i32>()].try_into()?);
        Ok((idx + mem::size_of::<i32>(), val))
    }

    #[inline]
    fn deser_f32(&self, idx: usize) -> Result<(usize, f32)> {
        debug_assert!(idx + mem::size_of::<f32>() <= self.len());
        let val = f32::from_le_bytes(self[idx..idx + mem::size_of::<f32>()].try_into()?);
        Ok((idx + mem::size_of::<f32>(), val))
    }

    #[inline]
    fn deser_u16(&self, idx: usize) -> Result<(usize, u16)> {
        debug_assert!(idx + mem::size_of::<u16>() <= self.len());
        let val = u16::from_le_bytes(self[idx..idx + mem::size_of::<u16>()].try_into()?);
        Ok((idx + mem::size_of::<u16>(), val))
    }

    #[inline]
    fn deser_i16(&self, idx: usize) -> Result<(usize, i16)> {
        debug_assert!(idx + mem::size_of::<i16>() <= self.len());
        let val = i16::from_le_bytes(self[idx..idx + mem::size_of::<i16>()].try_into()?);
        Ok((idx + mem::size_of::<i16>(), val))
    }

    #[inline]
    fn deser_u8(&self, idx: usize) -> Result<(usize, u8)> {
        debug_assert!(idx + mem::size_of::<u8>() <= self.len());
        Ok((idx + mem::size_of::<u8>(), self[idx]))
    }

    #[inline]
    fn deser_i8(&self, idx: usize) -> Result<(usize, i8)> {
        debug_assert!(idx + mem::size_of::<i8>() <= self.len());
        Ok((idx + mem::size_of::<i8>(), self[idx] as i8))
    }

    #[inline]
    fn deser_byte_slice(&self, idx: usize, len: usize) -> Result<(usize, &[u8])> {
        debug_assert!(idx + len <= self.len());
        let res = &self[idx..idx + len];
        Ok((idx + len, res))
    }

    #[inline]
    fn deser_byte_array<const N: usize>(&self, idx: usize) -> Result<(usize, [u8; N])> {
        debug_assert!(idx + N <= self.len());
        let mut res = [0u8; N];
        res.copy_from_slice(&self[idx..idx + N]);
        Ok((idx + N, res))
    }

    #[inline]
    fn deser(&self, idx: usize, len: usize) -> Result<(usize, &[u8])> {
        debug_assert!(idx + len <= self.len());
        Ok((idx + len, &self[idx..idx + len]))
    }
}

/// Defines how to serialize self to bytes.
///
/// This trait is designed to write a serialized object with a known
/// size to a fixed-sized buffer.
pub trait Ser<'a> {
    /// length of serialized bytes.
    fn ser_len(&self) -> usize;

    /// Serialize object into fix-sized byte slice.
    /// The buffer is guaranteed to be big enough.
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize;
}

/// Defines how to deserialize objects from bytes.
///
/// This trait is designed to read a serialized object from a byte slice,
/// and the result is owned by the caller so that it can be passed to
/// different threads.
pub trait Deser: Sized {
    /// Deserialize objects from input.
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)>;
}

impl Ser<'_> for u64 {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u64(start_idx, *self)
    }
}

impl Deser for u64 {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_u64(start_idx)
    }
}

impl Ser<'_> for i32 {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<i32>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_i32(start_idx, *self)
    }
}

impl Deser for i32 {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_i32(start_idx)
    }
}

impl Ser<'_> for i64 {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<i64>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_i64(start_idx, *self)
    }
}

impl Deser for i64 {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_i64(start_idx)
    }
}

impl Ser<'_> for u16 {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u16>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u16(start_idx, *self)
    }
}

impl Deser for u16 {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_u16(start_idx)
    }
}

impl Ser<'_> for i16 {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<i16>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_i16(start_idx, *self)
    }
}

impl Deser for i16 {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_i16(start_idx)
    }
}

impl Ser<'_> for u32 {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u32>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u32(start_idx, *self)
    }
}

impl Deser for u32 {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_u32(start_idx)
    }
}

impl Ser<'_> for u8 {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u8>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u8(start_idx, *self)
    }
}

impl Deser for u8 {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_u8(start_idx)
    }
}

impl Ser<'_> for i8 {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<i8>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_i8(start_idx, *self)
    }
}

impl Deser for i8 {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_i8(start_idx)
    }
}

impl Ser<'_> for bool {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u8>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_bool(start_idx, *self)
    }
}

impl Deser for bool {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_bool(start_idx)
    }
}

impl<const N: usize> Ser<'_> for [u8; N] {
    #[inline]
    fn ser_len(&self) -> usize {
        N
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_byte_array(start_idx, self)
    }
}

impl<const N: usize> Deser for [u8; N] {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        input.deser_byte_array(start_idx)
    }
}

impl<'a, T: Ser<'a>> Ser<'a> for [T] {
    #[inline]
    fn ser_len(&self) -> usize {
        // 8-byte vector length + data
        mem::size_of::<u64>() + self.iter().map(|v| v.ser_len()).sum::<usize>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let mut idx = start_idx;
        let len = self.len();
        idx = out.ser_u64(idx, len as u64);
        for v in self.iter() {
            idx = v.ser(out, idx);
        }
        idx
    }
}

impl<T: Deser> Deser for Vec<T> {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (mut idx, len) = input.deser_u64(start_idx)?;
        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let (idx0, val) = T::deser(input, idx)?;
            idx = idx0;
            vec.push(val);
        }
        Ok((idx, vec))
    }
}

impl<'a, T: Ser<'a>> Ser<'a> for Option<T> {
    #[inline]
    fn ser_len(&self) -> usize {
        // 1-byte bool + data
        match self.as_ref() {
            Some(v) => mem::size_of::<u8>() + v.ser_len(),
            None => mem::size_of::<u8>(),
        }
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let mut idx = start_idx;
        match self.as_ref() {
            Some(v) => {
                idx = true.ser(out, idx);
                idx = v.ser(out, idx);
            }
            None => {
                idx = false.ser(out, idx);
            }
        }
        idx
    }
}

impl<T: Deser> Deser for Option<T> {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, flag) = input.deser_bool(start_idx)?;
        if flag {
            let (idx, v) = T::deser(input, idx)?;
            Ok((idx, Some(v)))
        } else {
            Ok((idx, None))
        }
    }
}

impl<'a, T: Ser<'a>> Ser<'a> for Box<T> {
    #[inline]
    fn ser_len(&self) -> usize {
        self.as_ref().ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        self.as_ref().ser(out, start_idx)
    }
}

impl<T: Deser> Deser for Box<T> {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        T::deser(input, start_idx).map(|(idx, v)| (idx, Box::new(v)))
    }
}

impl<'a, K: Ser<'a>, V: Ser<'a>> Ser<'a> for BTreeMap<K, V> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>()
            + self
                .iter()
                .map(|(k, v)| k.ser_len() + v.ser_len())
                .sum::<usize>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = out.ser_u64(idx, self.len() as u64);
        for (k, v) in self.iter() {
            idx = k.ser(out, idx);
            idx = v.ser(out, idx);
        }
        idx
    }
}

impl<K: Ord + Deser, V: Deser> Deser for BTreeMap<K, V> {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (mut idx, len) = input.deser_u64(start_idx)?;
        let mut map = BTreeMap::new();
        for _ in 0..len {
            let (idx0, k) = K::deser(input, idx)?;
            idx = idx0;
            let (idx0, v) = V::deser(input, idx)?;
            idx = idx0;
            map.insert(k, v);
        }
        Ok((idx, map))
    }
}

impl Ser<'_> for SemiStr {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u32>() + self.as_bytes().len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u32(start_idx, self.len() as u32);
        out.ser_byte_slice(idx, self.as_bytes())
    }
}

impl Deser for SemiStr {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, len) = input.deser_u32(start_idx)?;
        let (idx, s) = input.deser_byte_slice(idx, len as usize)?;
        // here we always validate utf-8 encoding.
        let s = str::from_utf8(s)?;
        Ok((idx, SemiStr::new(s)))
    }
}

impl Ser<'_> for IndexOrder {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u8>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u8(start_idx, *self as u8)
    }
}

impl Deser for IndexOrder {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, v) = input.deser_u8(start_idx)?;
        Ok((idx, IndexOrder::from(v)))
    }
}

impl Ser<'_> for IndexKey {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u16>() + mem::size_of::<u8>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let start_idx = out.ser_u16(start_idx, self.col_no);
        self.order.ser(out, start_idx)
    }
}

impl Deser for IndexKey {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, col_no) = input.deser_u16(start_idx)?;
        let (idx, order) = IndexOrder::deser(input, idx)?;
        Ok((idx, IndexKey { col_no, order }))
    }
}

impl Ser<'_> for IndexAttributes {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u32>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u32(start_idx, self.bits())
    }
}

impl Deser for IndexAttributes {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, v) = input.deser_u32(start_idx)?;
        Ok((idx, IndexAttributes::from_bits_truncate(v)))
    }
}

impl Ser<'_> for IndexSpec {
    #[inline]
    fn ser_len(&self) -> usize {
        self.index_name.ser_len() + self.index_cols.ser_len() + self.index_attributes.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = self.index_name.ser(out, start_idx);
        let idx = self.index_cols.ser(out, idx);
        self.index_attributes.ser(out, idx)
    }
}

impl Deser for IndexSpec {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, index_name) = SemiStr::deser(input, start_idx)?;
        let (idx, index_cols) = <Vec<IndexKey>>::deser(input, idx)?;
        let (idx, index_attributes) = IndexAttributes::deser(input, idx)?;
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
    data_len: usize,
    header: &'a H,
    payload: &'a P,
    _marker: PhantomData<(&'a H, &'a P)>,
}

impl<'a, H: Ser<'a>, P: Ser<'a>> LenPrefixSerView<'a, H, P> {
    /// Create a new LenPrefixStruct.
    #[inline]
    pub fn new(header: &'a H, payload: &'a P) -> Self {
        let header_len = header.ser_len();
        let payload_len = payload.ser_len();
        let data_len = header_len + payload_len;
        Self {
            data_len,
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
        self.data_len
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
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>() + self.data_len
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        debug_assert!(self.data_len == self.header.ser_len() + self.payload.ser_len());
        let mut idx = start_idx;
        idx = out.ser_u64(idx, self.data_len as u64);
        idx = self.header.ser(out, idx);
        self.payload.ser(out, idx)
    }
}

// #[inline]
// pub fn len_prefix_pod_size(data: &[u8]) -> usize {
//     debug_assert!(data.len() >= mem::size_of::<u32>() * 2);
//     let len = u32::from_le_bytes(data[..mem::size_of::<u32>()].try_into().unwrap());
//     len as usize + mem::size_of::<u32>() * 2
// }

pub struct LenPrefixPod<H, P> {
    data_len: usize,
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
    pub fn new(header: H, payload: P) -> Self {
        let header_len = header.ser_len();
        let payload_len = payload.ser_len();
        let data_len = header_len + payload_len;
        Self {
            data_len,
            header,
            payload,
        }
    }
}

impl<'a, H: Ser<'a>, P: Ser<'a>> Ser<'a> for LenPrefixPod<H, P> {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u64>() + self.data_len
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        debug_assert!(self.data_len == self.header.ser_len() + self.payload.ser_len());
        let mut idx = start_idx;
        idx = out.ser_u64(idx, self.data_len as u64);
        idx = self.header.ser(out, idx);
        self.payload.ser(out, idx)
    }
}

impl<H: Deser, P: Deser> Deser for LenPrefixPod<H, P> {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, data_len) = input.deser_u64(start_idx)?;
        let (idx, header) = H::deser(input, idx)?;
        let (idx, payload) = P::deser(input, idx)?;
        Ok((
            idx,
            LenPrefixPod {
                data_len: data_len as usize,
                header,
                payload,
            },
        ))
    }
}

impl Ser<'_> for () {
    #[inline]
    fn ser_len(&self) -> usize {
        0
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, _out: &mut S, start_idx: usize) -> usize {
        start_idx
    }
}

impl Deser for () {
    #[inline]
    fn deser<S: Serde + ?Sized>(_input: &S, start_idx: usize) -> Result<(usize, Self)> {
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
    fn ser_len(&self) -> usize {
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
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = start_idx;
        if self.data.is_empty() {
            out.ser_u8(idx, 0)
        } else {
            let (n_bits, min) = self.info;
            let idx = out.ser_u8(idx, n_bits as u8);
            let idx = out.ser_u64(idx, self.data.len() as u64);
            let idx = min.ser(out, idx);
            let packed_len = (n_bits * self.data.len()).div_ceil(8);
            let (idx, to_pack) = out.ser_mut(idx, packed_len);
            match n_bits {
                1 => for_b1_pack(self.data, min, to_pack),
                2 => for_b2_pack(self.data, min, to_pack),
                4 => for_b4_pack(self.data, min, to_pack),
                8 => for_b8_pack(self.data, min, to_pack),
                16 => for_b16_pack(self.data, min, to_pack),
                32 => for_b32_pack(self.data, min, to_pack),
                _ => unreachable!("unexpected number bits of FOR bitpacking"),
            }
            idx
        }
    }
}

/// FOR+bitpacking decompression.
pub struct ForBitpackingDeser<T>(pub Vec<T>);

impl<T: BitPackable + Deser> Deser for ForBitpackingDeser<T> {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, n_bits) = input.deser_u8(start_idx)?;
        let (idx, n_elems) = input.deser_u64(idx)?;
        let (idx, min) = T::deser(input, idx)?;
        let n_bytes = (n_elems as usize * n_bits as usize).div_ceil(8);
        if idx + n_bytes > input.size() {
            return Err(Error::InvalidCompressedData);
        }
        let (idx, packed) = input.deser(idx, n_bytes)?;
        let mut data = vec![T::ZERO; n_elems as usize];
        match n_bits {
            1 => for_b1_unpack(packed, min, &mut data),
            2 => for_b2_unpack(packed, min, &mut data),
            4 => for_b4_unpack(packed, min, &mut data),
            8 => for_b8_unpack(packed, min, &mut data),
            16 => for_b16_unpack(packed, min, &mut data),
            32 => for_b32_unpack(packed, min, &mut data),
            _ => return Err(Error::InvalidCompressedData),
        };
        Ok((idx, ForBitpackingDeser(data)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_len_prefix_struct_serde() {
        let test_struct = TestStruct {
            a: 1,
            b: 2,
            c: 3,
            d: 4,
        };
        let len_prefix_struct = LenPrefixSerView::new(&(), &test_struct);
        let mut out = vec![0; len_prefix_struct.ser_len()];
        len_prefix_struct.ser(&mut out[..], 0);
        println!("{:?}", out);
        let (idx, pod) = LenPrefixPod::<(), TestStruct>::deser(&out[..], 0).unwrap();
        assert_eq!(idx, out.len());
        assert_eq!(pod.payload, test_struct);
    }

    #[test]
    fn test_vec_serde() {
        let vec = [TestStruct {
            a: 1,
            b: 2,
            c: 3,
            d: 4,
        }];
        let mut out = vec![0; vec.ser_len()];
        vec.ser(&mut out[..], 0);
        let (idx, val) = Vec::<TestStruct>::deser(&out[..], 0).unwrap();
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
        let map = BTreeMap::from([(
            1,
            TestStruct {
                a: 1,
                b: 2,
                c: 3,
                d: 4,
            },
        )]);
        let mut out = vec![0; map.ser_len()];
        map.ser(&mut out[..], 0);
        let (idx, val) = BTreeMap::<u64, TestStruct>::deser(&out[..], 0).unwrap();
        assert_eq!(idx, out.len());
        assert_eq!(val, map);
    }

    #[test]
    fn test_index_spec_serde() {
        let index_name = SemiStr::new("index1");
        println!("index_name ser_len={}", index_name.ser_len());
        let index_cols = vec![
            IndexKey::new(0),
            IndexKey {
                col_no: 1,
                order: IndexOrder::Desc,
            },
        ];
        println!("index_cols ser_len={}", index_cols.ser_len());
        let index_attributes = IndexAttributes::PK;
        println!("index_attributes ser_len={}", index_attributes.ser_len());
        let spec = IndexSpec {
            index_name,
            index_cols,
            index_attributes,
        };
        let len = spec.ser_len();
        println!("index_spec ser_len={}", len);
        let mut vec = vec![0u8; len];
        let idx = spec.ser(&mut vec[..], 0);
        assert_eq!(idx, len);
        let (idx, parsed) = IndexSpec::deser(&vec[..], 0).unwrap();
        assert_eq!(idx, len);
        assert_eq!(parsed, spec);
    }

    #[test]
    fn test_array_serde() {
        let array = [0u8, 1, 2, 3, 4];
        let len = array.ser_len();
        assert_eq!(len, 5);
        let mut vec = vec![0u8; len];
        let idx = array.ser(&mut vec[..], 0);
        assert_eq!(idx, len);
        let (idx, res) = <[u8; 5]>::deser(&vec[..], 0).unwrap();
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
        fn ser_len(&self) -> usize {
            mem::size_of::<u64>()
                + mem::size_of::<u32>()
                + mem::size_of::<u16>()
                + mem::size_of::<u8>()
        }

        fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
            let mut idx = start_idx;
            idx = out.ser_u64(idx, self.a);
            idx = out.ser_u32(idx, self.b);
            idx = out.ser_u16(idx, self.c);
            out.ser_u8(idx, self.d)
        }
    }

    impl Deser for TestStruct {
        fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
            let idx = start_idx;
            let (idx, a) = input.deser_u64(idx)?;
            let (idx, b) = input.deser_u32(idx)?;
            let (idx, c) = input.deser_u16(idx)?;
            let (idx, d) = input.deser_u8(idx)?;
            let res = TestStruct { a, b, c, d };
            Ok((idx, res))
        }
    }

    #[test]
    fn test_for_bitpacking_serde() {
        for input in [
            vec![1u64],
            vec![1, 1 << 1],
            vec![1, 1 << 1, 1 << 2],
            vec![1, 1 << 1, 1 << 2, 1 << 4],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16, 1 << 32],
        ] {
            let bp = ForBitpackingSer::new(&input).unwrap();
            let mut res = vec![0u8; bp.ser_len()];
            let ser_idx = bp.ser(&mut res[..], 0);
            assert_eq!(ser_idx, res.len());
            let (de_idx, decompressed) = ForBitpackingDeser::<u64>::deser(&res[..], 0).unwrap();
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
        let input = vec![-10i16, -3, 0, 1, 5, 8];
        let bp = ForBitpackingSer::new(&input).unwrap();
        let mut res = vec![0u8; bp.ser_len()];
        let ser_idx = bp.ser(&mut res[..], 0);
        assert_eq!(ser_idx, res.len());
        let (de_idx, decompressed) = ForBitpackingDeser::<i16>::deser(&res[..], 0).unwrap();
        assert_eq!(de_idx, res.len());
        assert_eq!(decompressed.0, input);

        let input = vec![-2i32, -1, 0, 1, 2, 4, 8, 128, 1024];
        let bp = ForBitpackingSer::new(&input).unwrap();
        let mut res = vec![0u8; bp.ser_len()];
        let ser_idx = bp.ser(&mut res[..], 0);
        assert_eq!(ser_idx, res.len());
        let (de_idx, decompressed) = ForBitpackingDeser::<i32>::deser(&res[..], 0).unwrap();
        assert_eq!(de_idx, res.len());
        assert_eq!(decompressed.0, input);
    }

    #[test]
    fn test_scalar_signed_ser_de() {
        let values = vec![-1024i16, -1, 0, 1, 2048];
        let mut out = vec![0u8; values.ser_len()];
        let idx = values.ser(&mut out[..], 0);
        assert_eq!(idx, out.len());
        let (idx, desered) = Vec::<i16>::deser(&out[..], 0).unwrap();
        assert_eq!(idx, out.len());
        assert_eq!(desered, values);

        let values = vec![-1i32, 0, 1024];
        let mut out = vec![0u8; values.ser_len()];
        let idx = values.ser(&mut out[..], 0);
        assert_eq!(idx, out.len());
        let (idx, desered) = Vec::<i32>::deser(&out[..], 0).unwrap();
        assert_eq!(idx, out.len());
        assert_eq!(desered, values);
    }
}
