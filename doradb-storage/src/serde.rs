use crate::error::{Error, Result};
use doradb_catalog::{IndexKey, IndexOrder};
use std::collections::BTreeMap;
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

    /// Serialize a u32 value to a byte slice.
    #[inline]
    pub fn ser_u32(&self, out: &mut [u8], idx: usize, val: u32) -> usize {
        debug_assert!(idx + mem::size_of::<u32>() <= out.len());
        out[idx..idx + mem::size_of::<u32>()].copy_from_slice(&val.to_le_bytes());
        idx + mem::size_of::<u32>()
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
    fn deser<'a>(ctx: &mut SerdeCtx, input: &'a [u8], start_idx: usize) -> Result<(usize, Self)>;
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
    fn deser<'a>(ctx: &mut SerdeCtx, input: &'a [u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_u64(input, start_idx)
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
    fn deser<'a>(ctx: &mut SerdeCtx, input: &'a [u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_i64(input, start_idx)
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
    fn deser<'a>(ctx: &mut SerdeCtx, input: &'a [u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_u32(input, start_idx)
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
    fn deser<'a>(ctx: &mut SerdeCtx, input: &'a [u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_u16(input, start_idx)
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
    fn deser<'a>(ctx: &mut SerdeCtx, input: &'a [u8], start_idx: usize) -> Result<(usize, Self)> {
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
    fn deser<'a>(ctx: &mut SerdeCtx, input: &'a [u8], start_idx: usize) -> Result<(usize, Self)> {
        ctx.deser_i8(input, start_idx)
    }
}

impl<'a, T: Ser<'a>> Ser<'a> for [T] {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
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
    fn deser<'a>(ctx: &mut SerdeCtx, input: &'a [u8], start_idx: usize) -> Result<(usize, Self)> {
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

impl<'a, H, P> LenPrefixSerView<'a, H, P> {
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

pub struct LenPrefixPod<H, P> {
    data_len: u32,
    pub header: H,
    pub payload: P,
}

impl<'a, H: Ser<'a>, P: Ser<'a>> LenPrefixPod<H, P> {
    /// Create a new LenPrefixStruct.
    #[inline]
    pub fn new(header: H, payload: P, ctx: &SerdeCtx) -> Self {
        let header_len = header.ser_len(ctx);
        let payload_len = payload.ser_len(ctx);
        let data_len = header_len + payload_len;
        assert!(data_len <= u32::MAX as usize);
        Self {
            data_len: data_len as u32,
            header,
            payload,
        }
    }
}

impl<'a, H: Ser<'a>, P: Ser<'a>> Ser<'a> for LenPrefixPod<H, P> {
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
        let c_idx = ctx.ser_u32(out, checksum_start_idx, checksum);
        debug_assert!(c_idx == checksum_end_idx);
        idx
    }
}
impl<H: Deser, P: Deser> Deser for LenPrefixPod<H, P> {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, data_len) = ctx.deser_u32(input, start_idx)?;
        let (idx, checksum) = ctx.deser_u32(input, idx)?;
        let (idx, header, payload) = if ctx.validate_checksum {
            let calculated_checksum = crc32fast::hash(&input[idx..idx + data_len as usize]);
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
                data_len,
                header,
                payload,
            },
        ))
    }
}

impl Ser<'_> for IndexKey {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u16>() + mem::size_of::<u8>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = ctx.ser_u16(out, idx, self.col_no);
        ctx.ser_u8(out, idx, self.order as u8)
    }
}

impl Deser for IndexKey {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, col_no) = ctx.deser_u16(input, start_idx)?;
        let (idx, order) = ctx.deser_u8(input, idx)?;
        Ok((
            idx,
            IndexKey {
                col_no,
                order: IndexOrder::from(order),
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
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        start_idx
    }
}

impl Deser for () {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        Ok((start_idx, ()))
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
}
