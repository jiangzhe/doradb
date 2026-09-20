//! Bitpacking compression
//!
//! Current implementation only support bits=1, 2, 4, 8, 16, 32.

use crate::layout;
use crate::lwc::LwcPrimitiveData;
use std::iter::once;
use std::mem;
use zerocopy::IntoBytes;

/// Data type that supports bitpacking.
/// constant ZERO is used to unify both bitpacking and FOR+bitpacking.
pub(crate) trait BitPackable: Copy {
    /// Zero value for this primitive type.
    const ZERO: Self;

    /// Returns `self - min` as a `u64` delta using wrapping arithmetic.
    fn sub_to_u64(self, min: Self) -> u64;

    /// Returns `self - min` as a `u32` delta using wrapping arithmetic.
    fn sub_to_u32(self, min: Self) -> u32;

    /// Adds a `u32` delta to this value using wrapping arithmetic.
    fn add_from_u32(self, delta: u32) -> Self;

    /// Returns `self - min` as a `u16` delta using wrapping arithmetic.
    fn sub_to_u16(self, min: Self) -> u16;

    /// Adds a `u16` delta to this value using wrapping arithmetic.
    fn add_from_u16(self, delta: u16) -> Self;

    /// Returns `self - min` as a `u8` delta using wrapping arithmetic.
    fn sub_to_u8(self, min: Self) -> u8;

    /// Adds a `u8` delta to this value using wrapping arithmetic.
    fn add_from_u8(self, delta: u8) -> Self;
}

macro_rules! impl_bit_packable {
    ($($t:ty),*) => {
        $(
            impl BitPackable for $t {
                const ZERO: Self = 0;

                #[inline(always)]
                fn sub_to_u64(self, min: Self) -> u64 {
                    self.wrapping_sub(min) as u64
                }

                #[inline(always)]
                fn sub_to_u32(self, min: Self) -> u32 {
                    self.wrapping_sub(min) as u32
                }

                #[inline(always)]
                fn add_from_u32(self, delta: u32) -> Self {
                    self.wrapping_add(delta as Self)
                }

                #[inline(always)]
                fn sub_to_u16(self, min: Self) -> u16 {
                    self.wrapping_sub(min) as u16
                }

                #[inline(always)]
                fn add_from_u16(self, delta: u16) -> Self {
                    self.wrapping_add(delta as Self)
                }

                #[inline(always)]
                fn sub_to_u8(self, min: Self) -> u8 {
                    self.wrapping_sub(min) as u8
                }

                #[inline(always)]
                fn add_from_u8(self, delta: u8) -> Self {
                    self.wrapping_add(delta as Self)
                }
            }
        )*
    }
}

impl_bit_packable!(i8, u8, i16, u16, i32, u32, i64, u64, isize, usize);

macro_rules! impl_lwc_bitpackable_data {
    ($t:ident, $nbits:literal, $extendf:ident, $it:ident) => {
        /// Frame-of-reference bitpacked data with sub-byte deltas.
        pub(crate) struct $t<'a, T> {
            /// Number of logical values represented by the packed bytes.
            pub(crate) len: usize,
            /// Minimum value used as the frame of reference.
            pub(crate) min: T,
            /// Packed delta bytes.
            pub(crate) data: &'a [u8],
        }

        impl<'a, T: BitPackable> LwcPrimitiveData for $t<'a, T> {
            type Value = T;
            type Iter = $it<'a, T>;

            #[inline]
            fn len(&self) -> usize {
                self.len
            }

            #[inline]
            fn value(&self, idx: usize) -> Option<T> {
                if idx < self.len {
                    let byte_idx = (idx * $nbits) / 8;
                    let bit_idx = (idx * $nbits) % 8;
                    let delta = (self.data[byte_idx] >> bit_idx) & ((1 << $nbits) - 1);
                    let v = self.min.add_from_u8(delta);
                    Some(v)
                } else {
                    None
                }
            }

            #[inline]
            fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E) {
                $extendf(self.data, self.len, self.min, target);
            }

            #[inline]
            fn iter(&self) -> Self::Iter {
                $it {
                    len: self.len,
                    min: self.min,
                    data: self.data,
                    idx: 0,
                }
            }
        }

        /// Iterator over frame-of-reference bitpacked sub-byte values.
        pub(crate) struct $it<'a, T> {
            len: usize,
            min: T,
            data: &'a [u8],
            idx: usize,
        }

        impl<T: BitPackable> Iterator for $it<'_, T> {
            type Item = T;

            #[inline]
            fn next(&mut self) -> Option<T> {
                if self.idx < self.len {
                    let v = value_fbp::<T, $nbits>(self.data, self.min, self.idx);
                    self.idx += 1;
                    Some(v)
                } else {
                    None
                }
            }
        }
    };
}

impl_lwc_bitpackable_data!(ForBitpacking1, 1, for_b1_unpack_extend, ForBitpacking1Iter);
impl_lwc_bitpackable_data!(ForBitpacking2, 2, for_b2_unpack_extend, ForBitpacking2Iter);
impl_lwc_bitpackable_data!(ForBitpacking4, 4, for_b4_unpack_extend, ForBitpacking4Iter);

/// Frame-of-reference bitpacked data with one-byte deltas.
pub(crate) struct ForBitpacking8<'a, T> {
    /// Minimum value used as the frame of reference.
    pub(crate) min: T,
    /// One-byte delta storage.
    pub(crate) data: &'a [u8],
}

impl<'a, T: BitPackable> LwcPrimitiveData for ForBitpacking8<'a, T> {
    type Value = T;
    type Iter = ForBitpacking8Iter<'a, T>;

    #[inline]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn value(&self, idx: usize) -> Option<T> {
        if idx < self.data.len() {
            let v = self.min.add_from_u8(self.data[idx]);
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E) {
        for_b8_unpack_extend(self.data, self.min, target);
    }

    #[inline]
    fn iter(&self) -> Self::Iter {
        ForBitpacking8Iter {
            min: self.min,
            data: self.data,
            idx: 0,
        }
    }
}

/// Iterator over frame-of-reference bitpacked values with one-byte deltas.
pub(crate) struct ForBitpacking8Iter<'a, T> {
    min: T,
    data: &'a [u8],
    idx: usize,
}

impl<T: BitPackable> Iterator for ForBitpacking8Iter<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        if self.idx < self.data.len() {
            let delta = self.data[self.idx];
            self.idx += 1;
            Some(self.min.add_from_u8(delta))
        } else {
            None
        }
    }
}

/// Frame-of-reference bitpacked data with two-byte deltas.
pub(crate) struct ForBitpacking16<'a, T> {
    /// Minimum value used as the frame of reference.
    pub(crate) min: T,
    /// Two-byte little-endian delta storage.
    pub(crate) data: &'a [[u8; 2]],
}

impl<'a, T: BitPackable> LwcPrimitiveData for ForBitpacking16<'a, T> {
    type Value = T;
    type Iter = ForBitpacking16Iter<'a, T>;

    #[inline]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn value(&self, idx: usize) -> Option<T> {
        if idx < self.data.len() {
            let u = self.data[idx];
            let v = self.min.add_from_u16(u16::from_le_bytes(u));
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E) {
        let input = self.data.as_bytes();
        for_b16_unpack_extend(input, self.min, target);
    }

    #[inline]
    fn iter(&self) -> Self::Iter {
        ForBitpacking16Iter {
            min: self.min,
            data: self.data,
            idx: 0,
        }
    }
}

/// Iterator over frame-of-reference bitpacked values with two-byte deltas.
pub(crate) struct ForBitpacking16Iter<'a, T> {
    min: T,
    data: &'a [[u8; 2]],
    idx: usize,
}

impl<T: BitPackable> Iterator for ForBitpacking16Iter<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        if self.idx < self.data.len() {
            let delta = u16::from_le_bytes(self.data[self.idx]);
            self.idx += 1;
            Some(self.min.add_from_u16(delta))
        } else {
            None
        }
    }
}

/// Frame-of-reference bitpacked data with four-byte deltas.
pub(crate) struct ForBitpacking32<'a, T> {
    /// Minimum value used as the frame of reference.
    pub(crate) min: T,
    /// Four-byte little-endian delta storage.
    pub(crate) data: &'a [[u8; 4]],
}

impl<'a, T: BitPackable> LwcPrimitiveData for ForBitpacking32<'a, T> {
    type Value = T;
    type Iter = ForBitpacking32Iter<'a, T>;

    #[inline]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    fn value(&self, idx: usize) -> Option<T> {
        if idx < self.data.len() {
            let u = self.data[idx];
            let v = self.min.add_from_u32(u32::from_le_bytes(u));
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E) {
        let input = self.data.as_bytes();
        for_b32_unpack_extend(input, self.min, target);
    }

    #[inline]
    fn iter(&self) -> Self::Iter {
        ForBitpacking32Iter {
            min: self.min,
            data: self.data,
            idx: 0,
        }
    }
}

/// Iterator over frame-of-reference bitpacked values with four-byte deltas.
pub(crate) struct ForBitpacking32Iter<'a, T> {
    min: T,
    data: &'a [[u8; 4]],
    idx: usize,
}

impl<T: BitPackable> Iterator for ForBitpacking32Iter<'_, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        if self.idx < self.data.len() {
            let delta = u32::from_le_bytes(self.data[self.idx]);
            self.idx += 1;
            Some(self.min.add_from_u32(delta))
        } else {
            None
        }
    }
}

/// Returns number of bits and minimum value on input data.
/// Returns None if not available.
#[inline]
pub(crate) fn prepare_for_bitpacking<T: BitPackable + Ord>(input: &[T]) -> Option<(usize, T)> {
    if input.is_empty() {
        return None;
    }
    let mut min = input[0];
    let mut max = input[0];
    input.iter().for_each(|v| {
        min = min.min(*v);
        max = max.max(*v);
    });
    let delta = max.sub_to_u64(min);
    let n_bits = if delta < (1 << 1) {
        1
    } else if delta < (1 << 2) {
        2
    } else if delta < (1 << 4) {
        4
    } else if delta < (1 << 8) {
        if mem::size_of::<T>() <= 1 {
            // compression is meaningless.
            return None;
        }
        8
    } else if delta < (1 << 16) {
        if mem::size_of::<T>() <= 2 {
            return None;
        }
        16
    } else if delta < (1 << 32) {
        if mem::size_of::<T>() <= 4 {
            return None;
        }
        32
    } else {
        return None;
    };
    Some((n_bits, min))
}

/// Pack (bits=1).
/// User has to guarantee all values are not out of range.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b1_pack"))]
pub(crate) fn b1_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b1_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=1).
/// Improve performance with Superword-Level Parallelism.
#[expect(clippy::needless_range_loop, reason = "code style")]
#[inline]
pub(crate) fn for_b1_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(input.len() <= res.len() * 8);
    // layer 1: batch 64
    let (chunks, remainder) = input.as_chunks::<64>();

    let (out_chunks, _) = res.as_chunks_mut::<8>();
    let mut out_idx = chunks.len() * 8;

    for (src, tgt) in chunks.iter().zip(out_chunks) {
        let mut packed: u64 = 0;
        for i in 0..64 {
            let bit = (src[i].sub_to_u64(min)) & 1;
            packed |= bit << i;
        }
        tgt.copy_from_slice(&packed.to_le_bytes());
    }

    // layer 2: batch 8
    let (rem_chunks, rem_final) = remainder.as_chunks::<8>();
    for src in rem_chunks {
        let mut packed_byte: u8 = 0;
        for i in 0..8 {
            let bit = src[i].sub_to_u8(min) & 1;
            packed_byte |= bit << i;
        }
        if let Some(b) = res.get_mut(out_idx) {
            *b = packed_byte;
        }
        out_idx += 1;
    }

    // layer 3: scalar (last 0..7 elements)
    if !rem_final.is_empty() {
        let mut packed_byte = 0u8;
        for (i, v) in rem_final.iter().enumerate() {
            if (v.sub_to_u8(min) & 1) != 0 {
                packed_byte |= 1 << i;
            }
        }
        if let Some(b) = res.get_mut(out_idx) {
            *b = packed_byte;
        }
    }
}

/// Unpack (bits=1).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b1_unpack"))]
pub(crate) fn b1_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b1_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=1).
/// Compressed element count is supposed to be greater or equal to result count.
#[expect(clippy::needless_range_loop, reason = "code style")]
#[inline]
pub(crate) fn for_b1_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() * 8 >= res.len());
    // layer 1: batch 64
    let (chunks, _) = input.as_chunks::<8>();
    let (out_chunks, _) = res.as_chunks_mut::<64>();
    // determine input index by output index.
    let mut input_idx = out_chunks.len() * 8;
    let mut out_idx = out_chunks.len() * 64;
    for (src, tgt) in chunks.iter().zip(out_chunks) {
        let packed = u64::from_le_bytes(*src);

        for i in 0..64 {
            let delta = ((packed >> i) & 1) as u8;
            tgt[i] = min.add_from_u8(delta);
        }
    }

    // layer 2: batch 8
    let (rem_chunks, _) = res[out_idx..].as_chunks_mut::<8>();
    out_idx += rem_chunks.len() * 8;
    for tgt in rem_chunks {
        let packed = input[input_idx];
        input_idx += 1;
        for i in 0..8 {
            let delta = (packed >> i) & 1;
            tgt[i] = min.add_from_u8(delta);
        }
    }

    // layer 3: scalar (last 0..7 elements)
    if out_idx < res.len() {
        let packed = input[input_idx];
        for (i, tgt) in res[out_idx..].iter_mut().enumerate() {
            let delta = (packed >> i) & 1;
            *tgt = min.add_from_u8(delta);
        }
    }
}

/// Extends `res` with `len` FOR-unpacked values from 1-bit deltas.
#[expect(clippy::needless_range_loop, reason = "code style")]
#[inline]
pub(crate) fn for_b1_unpack_extend<T: BitPackable, E: Extend<T>>(
    input: &[u8],
    len: usize,
    min: T,
    res: &mut E,
) {
    debug_assert!(len.div_ceil(8) == input.len());
    // layer 1: batch 64 first
    let mut tmp = [T::ZERO; 64];
    let chunks = input[..len / 64 * 8].as_chunks::<8>().0;
    let input_idx = chunks.len() * 8;
    for chunk in chunks {
        let packed = u64::from_le_bytes(*chunk);
        for i in 0..64 {
            let delta = ((packed >> i) & 1) as u8;
            tmp[i] = min.add_from_u8(delta);
        }
        res.extend(tmp);
    }

    // layer 2: batch 8
    if input_idx < len / 8 {
        for &packed in &input[input_idx..len / 8] {
            for i in 0..8 {
                let delta = (packed >> i) & 1;
                tmp[i] = min.add_from_u8(delta);
            }
            res.extend(tmp[..8].iter().copied());
        }
    }

    // layer 3: 1~7 elements.
    let rem = len % 8;
    if rem > 0 {
        let packed = input[len / 8];
        for i in 0..rem {
            let delta = (packed >> i) & 1;
            let v = min.add_from_u8(delta);
            res.extend(Some(v));
        }
    }
}

/// Pack (bits=2).
/// User has to guarantee all values are not out of range.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b2_pack"))]
pub(crate) fn b2_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b2_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=2).
/// Improve performance with Superword-Level Parallelism.
#[expect(clippy::needless_range_loop, reason = "code style")]
#[inline]
pub(crate) fn for_b2_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(input.len() <= res.len() * 4);
    // layer 1: batch 32
    let (chunks, remainder) = input.as_chunks::<32>();

    let (out_chunks, _) = res.as_chunks_mut::<8>();
    let mut out_idx = chunks.len() * 8;

    for (src, tgt) in chunks.iter().zip(out_chunks) {
        let mut packed: u64 = 0;
        for i in 0..32 {
            let val = src[i].sub_to_u64(min) & 3;
            packed |= val << (i * 2);
        }
        tgt.copy_from_slice(&packed.to_le_bytes());
    }

    // layer 2: batch 4
    let (rem_chunks, rem_final) = remainder.as_chunks::<4>();
    for src in rem_chunks {
        let mut packed_byte = 0u8;
        for i in 0..4 {
            let val = src[i].sub_to_u8(min) & 3;
            packed_byte |= val << (i * 2);
        }
        if let Some(b) = res.get_mut(out_idx) {
            *b = packed_byte;
        }
        out_idx += 1;
    }

    // layer 3: scalar
    if !rem_final.is_empty() {
        let mut packed_byte = 0u8;
        for (i, v) in rem_final.iter().enumerate() {
            let val = v.sub_to_u8(min) & 3;
            packed_byte |= val << (i * 2);
        }
        if let Some(b) = res.get_mut(out_idx) {
            *b = packed_byte;
        }
    }
}

/// Unpack (bits=2).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b2_unpack"))]
pub(crate) fn b2_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b2_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=2).
/// Compressed element count is supposed to be greater or equal to result count.
#[expect(clippy::needless_range_loop, reason = "code style")]
#[inline]
pub(crate) fn for_b2_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() * 4 >= res.len());
    // layer 1: batch 32
    let (chunks, _) = input.as_chunks::<8>();
    let (out_chunks, _) = res.as_chunks_mut::<32>();
    // determine input index by output index.
    let mut input_idx = out_chunks.len() * 8;
    let mut out_idx = out_chunks.len() * 32;
    for (src, tgt) in chunks.iter().zip(out_chunks) {
        let packed = u64::from_le_bytes(*src);

        for i in 0..32 {
            let delta = ((packed >> (i * 2)) & 3) as u8;
            tgt[i] = min.add_from_u8(delta);
        }
    }

    // layer 2: batch 4
    let (rem_chunks, _) = res[out_idx..].as_chunks_mut::<4>();
    out_idx += rem_chunks.len() * 4;
    for tgt in rem_chunks {
        let packed = input[input_idx];
        input_idx += 1;
        for i in 0..4 {
            let delta = (packed >> (i * 2)) & 3;
            tgt[i] = min.add_from_u8(delta);
        }
    }

    // layer 3: scalar (last 0..3 elements)
    if out_idx < res.len() {
        let packed = input[input_idx];
        for (i, tgt) in res[out_idx..].iter_mut().enumerate() {
            let delta = (packed >> (i * 2)) & 3;
            *tgt = min.add_from_u8(delta);
        }
    }
}

/// Extends `res` with `len` FOR-unpacked values from 2-bit deltas.
#[expect(clippy::needless_range_loop, reason = "code style")]
#[inline]
pub(crate) fn for_b2_unpack_extend<T: BitPackable, E: Extend<T>>(
    input: &[u8],
    len: usize,
    min: T,
    res: &mut E,
) {
    // 2 bits per item, so 1 byte holds 4 items.
    debug_assert!(len.div_ceil(4) == input.len());

    // Layer 1: Batch 32
    let mut tmp = [T::ZERO; 32];
    let chunks = input[..len / 32 * 4].as_chunks::<8>().0;
    let input_idx = chunks.len() * 8;

    for chunk in chunks {
        let packed = u64::from_le_bytes(*chunk);
        for i in 0..32 {
            let delta = ((packed >> (i * 2)) & 3) as u8;
            tmp[i] = min.add_from_u8(delta);
        }
        res.extend(tmp);
    }

    // Layer 2: Batch 4
    if input_idx < len / 4 {
        for &packed in &input[input_idx..len / 4] {
            for i in 0..4 {
                let delta = (packed >> (i * 2)) & 3;
                tmp[i] = min.add_from_u8(delta);
            }
            res.extend(tmp[..4].iter().copied());
        }
    }

    // Layer 3: Scalar (1~3 elements)
    let rem = len % 4;
    if rem > 0 {
        let packed = input[len / 4];
        for i in 0..rem {
            let delta = (packed >> (i * 2)) & 3;
            let v = min.add_from_u8(delta);
            res.extend(Some(v));
        }
    }
}

/// Pack (bits=4).
/// User has to guarantee all values are not out of range.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b4_pack"))]
pub(crate) fn b4_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b4_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=4).
/// Improve performance with Superword-Level Parallelism.
#[expect(clippy::needless_range_loop, reason = "code style")]
#[inline]
pub(crate) fn for_b4_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(input.len() <= res.len() * 2);
    // layer 1: batch 16
    let (chunks, remainder) = input.as_chunks::<16>();

    let (out_chunks, _) = res.as_chunks_mut::<8>();
    let mut out_idx = chunks.len() * 8;

    for (src, tgt) in chunks.iter().zip(out_chunks) {
        let mut packed: u64 = 0;
        for i in 0..16 {
            let val = src[i].sub_to_u64(min) & 15;
            packed |= val << (i * 4);
        }
        tgt.copy_from_slice(&packed.to_le_bytes());
    }

    // layer 2: remainder (0..15 elements)
    if !remainder.is_empty() {
        let mut packed_byte = 0u8;
        let mut shift = 0;
        for v in remainder {
            let val = v.sub_to_u8(min) & 15;
            packed_byte |= val << shift;
            shift += 4;
            if shift == 8 {
                if let Some(b) = res.get_mut(out_idx) {
                    *b = packed_byte;
                }
                out_idx += 1;
                packed_byte = 0;
                shift = 0;
            }
        }
        if shift > 0
            && let Some(b) = res.get_mut(out_idx)
        {
            *b = packed_byte;
        }
    }
}

/// Unpack (bits=4).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b4_unpack"))]
pub(crate) fn b4_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b4_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=4).
/// Compressed element count is supposed to be greater or equal to result count.
#[expect(clippy::needless_range_loop, reason = "code style")]
#[inline]
pub(crate) fn for_b4_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() * 2 >= res.len());
    // layer 1: batch 16
    let (chunks, _) = input.as_chunks::<8>();
    let (out_chunks, _) = res.as_chunks_mut::<16>();
    // determine input index by output index.
    let mut input_idx = out_chunks.len() * 8;
    let mut out_idx = out_chunks.len() * 16;

    for (src, tgt) in chunks.iter().zip(out_chunks) {
        let packed = u64::from_le_bytes(*src);

        for i in 0..16 {
            let delta = ((packed >> (i * 4)) & 15) as u8;
            tgt[i] = min.add_from_u8(delta);
        }
    }

    // Layer 2: Batch 2
    let (rem_chunks, _) = res[out_idx..].as_chunks_mut::<2>();
    out_idx += rem_chunks.len() * 2;

    for tgt in rem_chunks {
        let packed = input[input_idx];
        input_idx += 1;
        tgt[0] = min.add_from_u8(packed & 15);
        tgt[1] = min.add_from_u8((packed >> 4) & 15);
    }

    // Layer 3: Scalar (last 0 or 1 element)
    if out_idx < res.len() {
        let packed = input[input_idx];
        let delta = packed & 15;
        res[out_idx] = min.add_from_u8(delta);
    }
}

/// Extends `res` with `len` FOR-unpacked values from 4-bit deltas.
#[expect(clippy::needless_range_loop, reason = "code style")]
#[inline]
pub(crate) fn for_b4_unpack_extend<T: BitPackable, E: Extend<T>>(
    input: &[u8],
    len: usize,
    min: T,
    res: &mut E,
) {
    debug_assert!(len.div_ceil(2) == input.len());

    // Layer 1: Batch 16
    let mut tmp = [T::ZERO; 16];
    let chunks = input[..len / 16 * 2].as_chunks::<8>().0;
    let input_idx = chunks.len() * 8;

    for chunk in chunks {
        let packed = u64::from_le_bytes(*chunk);
        for i in 0..16 {
            let delta = ((packed >> (i * 4)) & 15) as u8;
            tmp[i] = min.add_from_u8(delta);
        }
        res.extend(tmp);
    }

    // Layer 2: Batch 2 (Full Bytes)
    if input_idx < len / 2 {
        for &packed in &input[input_idx..len / 2] {
            // Low nibble (bits 0-3)
            tmp[0] = min.add_from_u8(packed & 15);
            // High nibble (bits 4-7)
            tmp[1] = min.add_from_u8((packed >> 4) & 15);
            res.extend(tmp[..2].iter().copied());
        }
    }

    // Layer 3: Scalar (Remainder)
    let rem = len % 2;
    if rem > 0 {
        let packed = input[len / 2];
        let delta = packed & 15;
        let v = min.add_from_u8(delta);
        res.extend(once(v));
    }
}

/// Pack (bits=8).
/// User has to guarantee all values are not out of range.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b8_pack"))]
pub(crate) fn b8_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b8_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=8).
#[inline]
pub(crate) fn for_b8_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(res.len() >= input.len());
    input.iter().zip(res).for_each(|(src, tgt)| {
        *tgt = src.sub_to_u8(min);
    });
}

/// Unpack (bits=8).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b8_unpack"))]
pub(crate) fn b8_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b8_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=8).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
pub(crate) fn for_b8_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() >= res.len());
    input.iter().zip(res).for_each(|(src, tgt)| {
        *tgt = min.add_from_u8(*src);
    })
}

/// Extends `res` with FOR-unpacked values from 8-bit deltas.
#[inline]
pub(crate) fn for_b8_unpack_extend<T: BitPackable, E: Extend<T>>(
    input: &[u8],
    min: T,
    res: &mut E,
) {
    res.extend(input.iter().map(|&delta| min.add_from_u8(delta)));
}

/// Pack (bits=16).
/// User has to guarantee all values are not out of range.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b16_pack"))]
pub(crate) fn b16_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b16_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=16).
/// u16::to_le_bytes() to enable more efficient SIMD instruction.
#[inline]
pub(crate) fn for_b16_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(res.len() >= input.len() * 2);
    // convert slice of u8 to slice of u16(unaligned) for better auto-vectorization.
    let res = layout::slice_from_bytes_mut::<[u8; 2]>(&mut res[..input.len() * 2]);
    input.iter().zip(res).for_each(|(src, tgt)| {
        let val = src.sub_to_u16(min);
        *tgt = val.to_le_bytes();
    });
}

/// Unpack (bits=16).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b16_unpack"))]
pub(crate) fn b16_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b16_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=16).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
pub(crate) fn for_b16_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() >= res.len() * 2);
    let input = layout::slice_from_bytes::<[u8; 2]>(&input[..res.len() * 2]);
    input.iter().zip(res).for_each(|(src, tgt)| {
        let delta = u16::from_le_bytes(*src);
        *tgt = min.add_from_u16(delta);
    });
}

/// Extends `res` with FOR-unpacked values from 16-bit deltas.
#[inline]
pub(crate) fn for_b16_unpack_extend<T: BitPackable, E: Extend<T>>(
    input: &[u8],
    min: T,
    res: &mut E,
) {
    debug_assert!(input.len().is_multiple_of(2));
    let input = layout::slice_from_bytes::<[u8; 2]>(input);
    res.extend(input.iter().map(|src| {
        let delta = u16::from_le_bytes(*src);
        min.add_from_u16(delta)
    }))
}

/// Pack (bits=32).
/// User has to guarantee all values are not out of range.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b32_pack"))]
pub(crate) fn b32_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b32_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=32).
/// u32::to_le_bytes() to enable more efficient SIMD instruction.
#[inline]
pub(crate) fn for_b32_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(res.len() >= input.len() * 4);
    let res = layout::slice_from_bytes_mut::<[u8; 4]>(&mut res[..input.len() * 4]);
    input.iter().zip(res).for_each(|(src, tgt)| {
        let val = src.sub_to_u32(min);
        *tgt = val.to_le_bytes();
    });
}

/// Unpack (bits=32).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
#[cfg_attr(not(test), expect(dead_code, reason = "reserved b32_unpack"))]
pub(crate) fn b32_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b32_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=32).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
pub(crate) fn for_b32_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() >= res.len() * 4);
    let input = layout::slice_from_bytes::<[u8; 4]>(&input[..res.len() * 4]);
    input.iter().zip(res).for_each(|(src, tgt)| {
        let delta = u32::from_le_bytes(*src);
        *tgt = min.add_from_u32(delta);
    });
}

/// Extends `res` with FOR-unpacked values from 32-bit deltas.
#[inline]
pub(crate) fn for_b32_unpack_extend<T: BitPackable, E: Extend<T>>(
    input: &[u8],
    min: T,
    res: &mut E,
) {
    debug_assert!(input.len().is_multiple_of(4));
    let input = layout::slice_from_bytes::<[u8; 4]>(input);
    res.extend(input.iter().map(|src| {
        let delta = u32::from_le_bytes(*src);
        min.add_from_u32(delta)
    }))
}

#[inline]
fn value_fbp<T: BitPackable, const BITS: usize>(input: &[u8], min: T, idx: usize) -> T {
    let byte_idx = idx * BITS / 8;
    let bit_idx = idx * BITS % 8;
    let delta = (input[byte_idx] >> bit_idx) & ((1 << BITS) - 1);
    min.add_from_u8(delta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngExt, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::any::type_name;
    use std::fmt::Debug;

    const LEN: usize = 1000;
    const SEED: u64 = 312;
    const BOUNDARY_LENGTHS: &[usize] = &[
        0,
        1,
        2,
        3,
        7,
        8,
        9,
        15,
        16,
        31,
        32,
        63,
        64,
        65,
        127,
        128,
        LEN - 1,
        LEN,
    ];

    trait FromU64: Sized {
        fn from_u64(val: u64) -> Self;
    }
    macro_rules! impl_from_u64_ex {
        ($($t:ty),*) => {
            $(
                impl FromU64 for $t {
                    #[inline]
                    fn from_u64(val: u64) -> Self {
                        val as Self
                    }
                }
            )*
        }
    }
    impl_from_u64_ex!(i8, u8, i16, u16, i32, u32, i64, u64, isize, usize);

    fn random_bitpack_input<T: FromU64>(input_size: usize, n_bits: usize, seed: u64) -> Vec<T> {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let max = 1u64 << n_bits;
        (0..input_size)
            .map(|_| {
                let val = rng.random_range(0..max);
                T::from_u64(val)
            })
            .collect()
    }

    fn assert_extension<T: Copy + Debug + PartialEq>(
        input: &[T],
        prefix: T,
        context: &str,
        extend: impl Fn(&mut Vec<T>),
    ) {
        for mut result in [Vec::new(), vec![prefix]] {
            let prefix_len = result.len();
            let mut expected = result.clone();
            expected.extend_from_slice(input);
            extend(&mut result);
            assert_eq!(
                result, expected,
                "extend: {context}, prefix_len={prefix_len}"
            );
        }
    }

    fn assert_bitpack_round_trip<T: BitPackable + Debug + PartialEq>(
        input: &[T],
        n_bits: usize,
        case: &str,
    ) {
        type Pack<T> = fn(&[T], &mut [u8]);
        type Unpack<T> = fn(&[u8], &mut [T]);
        type Extend<T> = fn(&[u8], usize, T, &mut Vec<T>);
        let (pack, unpack, extend): (Pack<T>, Unpack<T>, Extend<T>) = match n_bits {
            1 => (b1_pack, b1_unpack, for_b1_unpack_extend),
            2 => (b2_pack, b2_unpack, for_b2_unpack_extend),
            4 => (b4_pack, b4_unpack, for_b4_unpack_extend),
            8 => (b8_pack, b8_unpack, |src, _, min, dst| {
                for_b8_unpack_extend(src, min, dst)
            }),
            16 => (b16_pack, b16_unpack, |src, _, min, dst| {
                for_b16_unpack_extend(src, min, dst)
            }),
            32 => (b32_pack, b32_unpack, |src, _, min, dst| {
                for_b32_unpack_extend(src, min, dst)
            }),
            _ => panic!("unsupported width: {n_bits}"),
        };
        let len = input.len();
        let context = format!(
            "type={}, width={n_bits}, len={len}, {case}",
            type_name::<T>()
        );
        let mut compressed = vec![0; (n_bits * len).div_ceil(8)];
        let mut decompressed = vec![T::ZERO; len];
        pack(input, &mut compressed);
        unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed, "unpack: {context}");
        assert_extension(input, T::ZERO, &context, |result| {
            extend(&compressed, len, T::ZERO, result);
        });
    }

    fn assert_bitpack_widths<T: FromU64 + BitPackable + Debug + PartialEq>(widths: &[usize]) {
        for &n_bits in widths {
            for &len in BOUNDARY_LENGTHS {
                let input = random_bitpack_input::<T>(len, n_bits, SEED);
                assert_bitpack_round_trip(&input, n_bits, &format!("random, seed={SEED}"));
            }
            let max = T::from_u64((1u64 << n_bits) - 1);
            assert_bitpack_round_trip(&[T::ZERO, max, max, T::ZERO, max], n_bits, "zero_max");
        }
    }

    fn assert_for_b1_extension<T: BitPackable + Debug + PartialEq>(
        compressed: &[u8],
        input: &[T],
        min: T,
        case: &str,
    ) {
        let context = format!(
            "type={}, len={}, min={min:?}, {case}",
            type_name::<T>(),
            input.len()
        );
        assert_extension(input, min, &context, |result| {
            for_b1_unpack_extend(compressed, input.len(), min, result);
        });
    }

    fn assert_for_b1_round_trip<T: BitPackable + Debug + PartialEq>(
        input: &[T],
        min: T,
        case: &str,
    ) {
        let mut compressed = vec![0; input.len().div_ceil(8)];
        for_b1_pack(input, min, &mut compressed);
        assert_for_b1_extension(&compressed, input, min, case);
    }

    /// Purpose: Protect i8 bitpacking across compressed widths and length boundaries.
    /// Expected: Unpacking and extension recover the original values.
    #[test]
    fn test_bitpack_i8() {
        assert_bitpack_widths::<i8>(&[1, 2, 4]);
    }

    /// Purpose: Protect u8 bitpacking across compressed widths and length boundaries.
    /// Expected: Unpacking and extension recover the original values.
    #[test]
    fn test_bitpack_u8() {
        assert_bitpack_widths::<u8>(&[1, 2, 4]);
    }

    /// Purpose: Protect i16 bitpacking across compressed widths and length boundaries.
    /// Expected: Unpacking and extension recover the original values.
    #[test]
    fn test_bitpack_i16() {
        assert_bitpack_widths::<i16>(&[1, 2, 4, 8]);
    }

    /// Purpose: Protect u16 bitpacking across compressed widths and length boundaries.
    /// Expected: Unpacking and extension recover the original values.
    #[test]
    fn test_bitpack_u16() {
        assert_bitpack_widths::<u16>(&[1, 2, 4, 8]);
    }

    /// Purpose: Protect i32 bitpacking across widths, length boundaries, and increasing inputs.
    /// Expected: Unpacking and extension preserve values without exposing padding.
    #[test]
    fn test_bitpack_i32() {
        assert_bitpack_widths::<i32>(&[1, 2, 4, 8, 16]);
        for (n_bits, end) in [(2, 4), (4, 16)] {
            for i in 2..end {
                let input: Vec<i32> = (1..i).collect();
                assert_bitpack_round_trip(&input, n_bits, "increasing");
            }
        }
    }

    /// Purpose: Protect u32 bitpacking across compressed widths and byte/chunk boundaries.
    /// Expected: Unpacking restores values; extension appends them while preserving the prefix.
    #[test]
    fn test_bitpack_u32() {
        assert_bitpack_widths::<u32>(&[1, 2, 4, 8, 16]);
    }

    /// Purpose: Protect i64 bitpacking across compressed widths and length boundaries.
    /// Expected: Unpacking and extension recover the original values.
    #[test]
    fn test_bitpack_i64() {
        assert_bitpack_widths::<i64>(&[1, 2, 4, 8, 16, 32]);
    }

    /// Purpose: Protect u64 bitpacking across compressed widths and length boundaries.
    /// Expected: Unpacking and extension recover the original values.
    #[test]
    fn test_bitpack_u64() {
        assert_bitpack_widths::<u64>(&[1, 2, 4, 8, 16, 32]);
    }

    /// Purpose: Protect frame-of-reference bitpacking with varied inputs and bases.
    /// Expected: Unpacking and extension restore the original values from their deltas.
    #[test]
    fn test_for_bitpack() {
        for seed in 0..100 {
            let input = random_bitpack_input::<u64>(LEN, 32, seed);
            let mut compressed = vec![0; LEN * 4];
            let mut decompressed = vec![0; LEN];
            let min = input.iter().min().copied().unwrap();
            for_b32_pack(&input, min, &mut compressed);
            for_b32_unpack(&compressed, min, &mut decompressed);
            let context = format!("type=u64, width=32, len={LEN}, seed={seed}, min={min}");
            assert_eq!(input, decompressed, "unpack: {context}");
            assert_extension(&input, u64::MAX, &context, |result| {
                for_b32_unpack_extend(&compressed, min, result);
            });
        }
    }

    /// Purpose: Protect forward progress of the frame-of-reference delta iterator.
    /// Expected: Iteration yields each rebased value in order and stops at exhaustion.
    #[test]
    fn test_for_bitpacking32_iter_advances() {
        let data = [1u32.to_le_bytes(), 5u32.to_le_bytes()];
        let mut iter = ForBitpacking32Iter {
            min: 10u64,
            data: &data,
            idx: 0,
        };

        assert_eq!(iter.next(), Some(11));
        assert_eq!(iter.next(), Some(15));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    /// Purpose: Protect one-bit frame-of-reference extension with nonzero bases.
    /// Expected: Extension restores the original values at each tested length boundary.
    #[test]
    fn test_for_b1_unpack_extend_with_min() {
        let min_values = [5, 10, 100, 1000];
        for &len in BOUNDARY_LENGTHS {
            let deltas = random_bitpack_input::<u32>(len, 1, SEED);
            for &min_val in &min_values {
                let input: Vec<_> = deltas.iter().map(|delta| min_val + delta).collect();
                assert_for_b1_round_trip(&input, min_val, &format!("seed={SEED}"));
            }
        }
    }

    /// Purpose: Protect one-bit extension near the limits of every supported integer type.
    /// Expected: Signed, unsigned, and pointer-sized values survive rebasing without loss.
    #[test]
    fn test_for_b1_unpack_extend_all_types() {
        let len: usize = 100;
        macro_rules! test_type {
            ($t:ty) => {
                for (case, min) in [("minimum", <$t>::MIN), ("maximum", <$t>::MAX - 1)] {
                    let input: Vec<$t> = [0, 1, 1, 0, 1, 0, 0, 1]
                        .into_iter()
                        .cycle()
                        .take(len)
                        .map(|delta| min + delta)
                        .collect();
                    assert_for_b1_round_trip(&input, min, case);
                }
            };
        }

        test_type!(i8);
        test_type!(u8);
        test_type!(i16);
        test_type!(u16);
        test_type!(i32);
        test_type!(u32);
        test_type!(i64);
        test_type!(u64);
        test_type!(isize);
        test_type!(usize);
    }

    /// Purpose: Protect one-bit wire layout and extension at partial-byte boundaries.
    /// Expected: Packing matches fixed wire bytes; extension preserves values and ignores padding.
    #[test]
    fn test_for_b1_unpack_extend_edge_cases() {
        let len = 67;
        let cases = [
            ("zeros", vec![0u32; len], [0; 9]),
            (
                "ones",
                vec![1; len],
                [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x07],
            ),
            (
                "alternating",
                (0..len).map(|i| (i % 2) as u32).collect(),
                [0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0x02],
            ),
            (
                "asymmetric",
                [1, 0, 1, 1, 0, 0, 0, 1]
                    .into_iter()
                    .cycle()
                    .take(len)
                    .collect(),
                [0x8d, 0x8d, 0x8d, 0x8d, 0x8d, 0x8d, 0x8d, 0x8d, 0x05],
            ),
            (
                "chunk_byte_order",
                (0..len).map(|i| u32::from(i % 3 == 0)).collect(),
                [0x49, 0x92, 0x24, 0x49, 0x92, 0x24, 0x49, 0x92, 0x04],
            ),
        ];
        for (case, input, mut wire) in cases {
            let mut compressed = vec![0; len.div_ceil(8)];
            for_b1_pack(&input, 0, &mut compressed);
            assert_eq!(compressed, wire, "wire layout: {case}");
            assert_for_b1_extension(&wire, &input, 0, case);

            // Unused high bits in the last byte must not become logical values.
            wire[8] |= 0b1111_1000;
            assert_for_b1_extension(&wire, &input, 0, &format!("{case}, nonzero padding"));
        }
    }
}
