//! Bitpacking compression
//!
//! Current implementation only support bits=1, 2, 4, 8, 16, 32.

use crate::lwc::{LwcPrimitiveData, SortedPosition};
use std::mem;

/// Data type that supports bitpacking.
/// constant ZERO is used to unify both bitpacking and FOR+bitpacking.
pub trait BitPackable: Copy {
    const ZERO: Self;

    fn sub_to_u64(self, min: Self) -> u64;

    fn sub_to_u32(self, min: Self) -> u32;

    fn add_from_u32(self, delta: u32) -> Self;

    fn sub_to_u16(self, min: Self) -> u16;

    fn add_from_u16(self, delta: u16) -> Self;

    fn sub_to_u8(self, min: Self) -> u8;

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

/// Returns number of bits and minimum value on input data.
/// Returns None if not available.
#[inline]
pub fn prepare_for_bitpacking<T: BitPackable + Ord>(input: &[T]) -> Option<(usize, T)> {
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
pub fn b1_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b1_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=1).
/// Improve performance with Superword-Level Parallelism.
#[allow(clippy::needless_range_loop)]
#[inline]
pub fn for_b1_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(input.len() <= res.len() * 8);
    // layer 1: batch 64
    let chunks = input.chunks_exact(64);
    let remainder = chunks.remainder();

    let out_chunks = res.chunks_exact_mut(8);
    let mut out_idx = chunks.len() * 8;

    for (src, tgt) in chunks.zip(out_chunks) {
        let src: &[T; 64] = src.try_into().unwrap();
        let mut packed: u64 = 0;
        for i in 0..64 {
            let bit = (src[i].sub_to_u64(min)) & 1;
            packed |= bit << i;
        }
        tgt.copy_from_slice(&packed.to_le_bytes());
    }

    // layer 2: batch 8
    let rem_chunks = remainder.chunks_exact(8);
    let rem_final = rem_chunks.remainder();
    for src in rem_chunks {
        let src: &[T; 8] = src.try_into().unwrap();
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
pub fn b1_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b1_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=1).
/// Compressed element count is supposed to be greater or equal to result count.
#[allow(clippy::needless_range_loop)]
#[inline]
pub fn for_b1_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() * 8 >= res.len());
    // layer 1: batch 64
    let chunks = input.chunks_exact(8);
    let out_chunks = res.chunks_exact_mut(64);
    // determine input index by output index.
    let mut input_idx = out_chunks.len() * 8;
    let mut out_idx = out_chunks.len() * 64;
    for (src, tgt) in chunks.zip(out_chunks) {
        let src: &[u8; 8] = src.try_into().unwrap();
        let packed = u64::from_le_bytes(*src);

        for i in 0..64 {
            let delta = ((packed >> i) & 1) as u8;
            tgt[i] = min.add_from_u8(delta);
        }
    }

    // layer 2: batch 8
    let rem_chunks = res[out_idx..].chunks_exact_mut(8);
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

#[allow(clippy::needless_range_loop)]
#[inline]
pub fn for_b1_unpack_extend<T: BitPackable, E: Extend<T>>(
    input: &[u8],
    len: usize,
    min: T,
    res: &mut E,
) {
    debug_assert!(len.div_ceil(8) == input.len());
    // layer 1: batch 64 first
    let mut tmp = [T::ZERO; 64];
    let chunks = input[..len / 64 * 8].chunks_exact(8);
    let input_idx = chunks.len() * 8;
    for chunk in chunks {
        let src: [u8; 8] = chunk.try_into().unwrap();
        let packed = u64::from_le_bytes(src);
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
            res.extend(tmp[..8].iter().cloned());
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
pub fn b2_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b2_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=2).
/// Improve performance with Superword-Level Parallelism.
#[allow(clippy::needless_range_loop)]
#[inline]
pub fn for_b2_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(input.len() <= res.len() * 4);
    // layer 1: batch 32
    let chunks = input.chunks_exact(32);
    let remainder = chunks.remainder();

    let out_chunks = res.chunks_exact_mut(8);
    let mut out_idx = chunks.len() * 8;

    for (src, tgt) in chunks.zip(out_chunks) {
        let src: &[T; 32] = src.try_into().unwrap();
        let mut packed: u64 = 0;
        for i in 0..32 {
            let val = src[i].sub_to_u64(min) & 3;
            packed |= val << (i * 2);
        }
        tgt.copy_from_slice(&packed.to_le_bytes());
    }

    // layer 2: batch 4
    let rem_chunks = remainder.chunks_exact(4);
    let rem_final = rem_chunks.remainder();
    for src in rem_chunks {
        let src: &[T; 4] = src.try_into().unwrap();
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
pub fn b2_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b2_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=2).
/// Compressed element count is supposed to be greater or equal to result count.
#[allow(clippy::needless_range_loop)]
#[inline]
pub fn for_b2_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() * 4 >= res.len());
    // layer 1: batch 32
    let chunks = input.chunks_exact(8);
    let out_chunks = res.chunks_exact_mut(32);
    // determine input index by output index.
    let mut input_idx = out_chunks.len() * 8;
    let mut out_idx = out_chunks.len() * 32;
    for (src, tgt) in chunks.zip(out_chunks) {
        let src: &[u8; 8] = src.try_into().unwrap();
        let packed = u64::from_le_bytes(*src);

        for i in 0..32 {
            let delta = ((packed >> (i * 2)) & 3) as u8;
            tgt[i] = min.add_from_u8(delta);
        }
    }

    // layer 2: batch 4
    let rem_chunks = res[out_idx..].chunks_exact_mut(4);
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

#[allow(clippy::needless_range_loop)]
#[inline]
pub fn for_b2_unpack_extend<T: BitPackable, E: Extend<T>>(
    input: &[u8],
    len: usize,
    min: T,
    res: &mut E,
) {
    // 2 bits per item, so 1 byte holds 4 items.
    debug_assert!(len.div_ceil(4) == input.len());

    // Layer 1: Batch 32
    let mut tmp = [T::ZERO; 32];
    let chunks = input[..len / 32 * 4].chunks_exact(8);
    let input_idx = chunks.len() * 8;

    for chunk in chunks {
        let src: [u8; 8] = chunk.try_into().unwrap();
        let packed = u64::from_le_bytes(src);
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
            res.extend(tmp[..4].iter().cloned());
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
pub fn b4_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b4_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=4).
/// Improve performance with Superword-Level Parallelism.
#[allow(clippy::needless_range_loop)]
#[inline]
pub fn for_b4_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(input.len() <= res.len() * 2);
    // layer 1: batch 16
    let chunks = input.chunks_exact(16);
    let remainder = chunks.remainder();

    let out_chunks = res.chunks_exact_mut(8);
    let mut out_idx = chunks.len() * 8;

    for (src, tgt) in chunks.zip(out_chunks) {
        let src: &[T; 16] = src.try_into().unwrap();
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
pub fn b4_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b4_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=4).
/// Compressed element count is supposed to be greater or equal to result count.
#[allow(clippy::needless_range_loop)]
#[inline]
pub fn for_b4_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() * 2 >= res.len());
    // layer 1: batch 16
    let chunks = input.chunks_exact(8);
    let out_chunks = res.chunks_exact_mut(16);
    // determine input index by output index.
    let mut input_idx = out_chunks.len() * 8;
    let mut out_idx = out_chunks.len() * 16;

    for (src, tgt) in chunks.zip(out_chunks) {
        let src: &[u8; 8] = src.try_into().unwrap();
        let packed = u64::from_le_bytes(*src);

        for i in 0..16 {
            let delta = ((packed >> (i * 4)) & 15) as u8;
            tgt[i] = min.add_from_u8(delta);
        }
    }

    // Layer 2: Batch 2
    let rem_chunks = res[out_idx..].chunks_exact_mut(2);
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

#[allow(clippy::needless_range_loop)]
#[inline]
pub fn for_b4_unpack_extend<T: BitPackable, E: Extend<T>>(
    input: &[u8],
    len: usize,
    min: T,
    res: &mut E,
) {
    debug_assert!(len.div_ceil(2) == input.len());

    // Layer 1: Batch 16
    let mut tmp = [T::ZERO; 16];
    let chunks = input[..len / 16 * 2].chunks_exact(8);
    let input_idx = chunks.len() * 8;

    for chunk in chunks {
        let src: [u8; 8] = chunk.try_into().unwrap();
        let packed = u64::from_le_bytes(src);
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
            res.extend(tmp[..2].iter().cloned());
        }
    }

    // Layer 3: Scalar (Remainder)
    let rem = len % 2;
    if rem > 0 {
        let packed = input[len / 2];
        let delta = packed & 15;
        let v = min.add_from_u8(delta);
        res.extend(std::iter::once(v));
    }
}

/// Pack (bits=8).
/// User has to guarantee all values are not out of range.
#[inline]
pub fn b8_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b8_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=8).
#[inline]
pub fn for_b8_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(res.len() >= input.len());
    input.iter().zip(res).for_each(|(src, tgt)| {
        *tgt = src.sub_to_u8(min);
    });
}

/// Unpack (bits=8).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
pub fn b8_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b8_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=8).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
pub fn for_b8_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() >= res.len());
    input.iter().zip(res).for_each(|(src, tgt)| {
        *tgt = min.add_from_u8(*src);
    })
}

#[inline]
pub fn for_b8_unpack_extend<T: BitPackable, E: Extend<T>>(input: &[u8], min: T, res: &mut E) {
    res.extend(input.iter().map(|&delta| min.add_from_u8(delta)));
}

/// Pack (bits=16).
/// User has to guarantee all values are not out of range.
#[inline]
pub fn b16_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b16_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=16).
/// u16::to_le_bytes() to enable more efficient SIMD instruction.
#[inline]
pub fn for_b16_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(res.len() >= input.len() * 2);
    // convert slice of u8 to slice of u16(unaligned) for better auto-vectorization.
    let res = bytemuck::cast_slice_mut::<u8, [u8; 2]>(&mut res[..input.len() * 2]);
    input.iter().zip(res).for_each(|(src, tgt)| {
        let val = src.sub_to_u16(min);
        *tgt = val.to_le_bytes();
    });
}

/// Unpack (bits=16).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
pub fn b16_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b16_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=16).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
pub fn for_b16_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() >= res.len() * 2);
    let input = bytemuck::cast_slice::<u8, [u8; 2]>(&input[..res.len() * 2]);
    input.iter().zip(res).for_each(|(src, tgt)| {
        let delta = u16::from_le_bytes(*src);
        *tgt = min.add_from_u16(delta);
    });
}

#[inline]
pub fn for_b16_unpack_extend<T: BitPackable, E: Extend<T>>(input: &[u8], min: T, res: &mut E) {
    debug_assert!(input.len().is_multiple_of(2));
    let input = bytemuck::cast_slice::<u8, [u8; 2]>(input);
    res.extend(input.iter().map(|src| {
        let delta = u16::from_le_bytes(*src);
        min.add_from_u16(delta)
    }))
}

/// Pack (bits=32).
/// User has to guarantee all values are not out of range.
#[inline]
pub fn b32_pack<T: BitPackable>(input: &[T], res: &mut [u8]) {
    for_b32_pack(input, T::ZERO, res)
}

/// Pack with FrameOfReference (bits=32).
/// u32::to_le_bytes() to enable more efficient SIMD instruction.
#[inline]
pub fn for_b32_pack<T: BitPackable>(input: &[T], min: T, res: &mut [u8]) {
    debug_assert!(res.len() >= input.len() * 4);
    let res = bytemuck::cast_slice_mut::<u8, [u8; 4]>(&mut res[..input.len() * 4]);
    input.iter().zip(res).for_each(|(src, tgt)| {
        let val = src.sub_to_u32(min);
        *tgt = val.to_le_bytes();
    });
}

/// Unpack (bits=32).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
pub fn b32_unpack<T: BitPackable>(input: &[u8], res: &mut [T]) {
    for_b32_unpack(input, T::ZERO, res)
}

/// FOR unpack (bits=32).
/// Compressed element count is supposed to be greater or equal to result count.
#[inline]
pub fn for_b32_unpack<T: BitPackable>(input: &[u8], min: T, res: &mut [T]) {
    debug_assert!(input.len() >= res.len() * 4);
    let input = bytemuck::cast_slice::<u8, [u8; 4]>(&input[..res.len() * 4]);
    input.iter().zip(res).for_each(|(src, tgt)| {
        let delta = u32::from_le_bytes(*src);
        *tgt = min.add_from_u32(delta);
    });
}

#[inline]
pub fn for_b32_unpack_extend<T: BitPackable, E: Extend<T>>(input: &[u8], min: T, res: &mut E) {
    debug_assert!(input.len().is_multiple_of(4));
    let input = bytemuck::cast_slice::<u8, [u8; 4]>(input);
    res.extend(input.iter().map(|src| {
        let delta = u32::from_le_bytes(*src);
        min.add_from_u32(delta)
    }))
}

macro_rules! impl_lwc_bitpackable_data {
    ($t:ident, $nbits:literal, $extendf:ident, $it:ident) => {
        pub struct $t<'a, T> {
            pub(crate) len: usize,
            pub(crate) min: T,
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

        pub struct $it<'a, T> {
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

#[inline]
fn value_fbp<T: BitPackable, const BITS: usize>(input: &[u8], min: T, idx: usize) -> T {
    let byte_idx = idx * BITS / 8;
    let bit_idx = idx * BITS % 8;
    let delta = (input[byte_idx] >> bit_idx) & ((1 << BITS) - 1);
    min.add_from_u8(delta)
}

impl_lwc_bitpackable_data!(ForBitpacking1, 1, for_b1_unpack_extend, ForBitpacking1Iter);
impl_lwc_bitpackable_data!(ForBitpacking2, 2, for_b2_unpack_extend, ForBitpacking2Iter);
impl_lwc_bitpackable_data!(ForBitpacking4, 4, for_b4_unpack_extend, ForBitpacking4Iter);

pub struct ForBitpacking8<'a, T> {
    pub(crate) min: T,
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

impl<T: BitPackable + Ord> SortedPosition for ForBitpacking8<'_, T> {
    type Value = T;

    #[inline]
    fn sorted_position(&self, value: T) -> Option<usize> {
        if value < self.min {
            return None;
        }
        let delta = value.sub_to_u8(self.min);
        self.data.binary_search(&delta).ok()
    }
}

pub struct ForBitpacking8Iter<'a, T> {
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

pub struct ForBitpacking16<'a, T> {
    pub(crate) min: T,
    pub(crate) data: &'a [[u8; 2]],
}

impl<T: BitPackable + Ord> SortedPosition for ForBitpacking16<'_, T> {
    type Value = T;

    #[inline]
    fn sorted_position(&self, value: T) -> Option<usize> {
        if value < self.min {
            return None;
        }
        let delta = value.sub_to_u16(self.min);
        self.data
            .binary_search_by_key(&delta, |&v| u16::from_le_bytes(v))
            .ok()
    }
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
        let input = bytemuck::cast_slice::<[u8; 2], u8>(self.data);
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

pub struct ForBitpacking16Iter<'a, T> {
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

pub struct ForBitpacking32<'a, T> {
    pub(crate) min: T,
    pub(crate) data: &'a [[u8; 4]],
}

impl<T: BitPackable + Ord> SortedPosition for ForBitpacking32<'_, T> {
    type Value = T;

    #[inline]
    fn sorted_position(&self, value: T) -> Option<usize> {
        if value < self.min {
            return None;
        }
        let delta = value.sub_to_u32(self.min);
        self.data
            .binary_search_by_key(&delta, |&v| u32::from_le_bytes(v))
            .ok()
    }
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
        let input = bytemuck::cast_slice::<[u8; 4], u8>(self.data);
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

pub struct ForBitpacking32Iter<'a, T> {
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
            Some(self.min.add_from_u32(delta))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn setup<T: FromU64 + BitPackable>(
        input_size: usize,
        n_bits: usize,
    ) -> (Vec<T>, Vec<u8>, Vec<T>) {
        let max = 1 << n_bits;
        let output_size = (n_bits * input_size).div_ceil(8);
        let input = (0..input_size)
            .map(|_| {
                let val = rand::random_range(0..max);
                T::from_u64(val)
            })
            .collect();
        // compressed size is at most half of original size.
        let compressed = vec![0u8; output_size];
        let decompressed = vec![T::ZERO; input_size];
        (input, compressed, decompressed)
    }

    const LEN: usize = 1000;

    #[test]
    fn test_bitpack_i8() {
        let (input, mut compressed, mut decompressed) = setup::<i8>(LEN, 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b1_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i8>(LEN, 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b2_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i8>(LEN, 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b4_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);
    }

    #[test]
    fn test_bitpack_u8() {
        let (input, mut compressed, mut decompressed) = setup::<u8>(LEN, 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b1_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u8>(LEN, 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b2_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);
        for i in 2i32..4 {
            let input: Vec<i32> = (1..i).collect();
            let mut compressed = vec![0u8; (input.len() * 2).div_ceil(8)];
            let mut decompressed = vec![0i32; input.len()];
            b2_pack(&input, &mut compressed);
            b2_unpack(&compressed, &mut decompressed);
            assert_eq!(input, decompressed);
            let mut res = vec![];
            for_b2_unpack_extend(&compressed, input.len(), 0, &mut res);
            assert_eq!(input, res);
        }

        let (input, mut compressed, mut decompressed) = setup::<u8>(LEN, 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b4_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);
        for i in 2i32..16 {
            let input: Vec<i32> = (1..i).collect();
            let mut compressed = vec![0u8; (input.len() * 4).div_ceil(8)];
            let mut decompressed = vec![0i32; input.len()];
            b4_pack(&input, &mut compressed);
            b4_unpack(&compressed, &mut decompressed);
            assert_eq!(input, decompressed);
            let mut res = vec![];
            for_b4_unpack_extend(&compressed, input.len(), 0, &mut res);
            assert_eq!(input, res);
        }
    }

    #[test]
    fn test_bitpack_i16() {
        let (input, mut compressed, mut decompressed) = setup::<i16>(LEN, 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b1_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i16>(LEN, 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b2_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i16>(LEN, 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b4_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i16>(LEN, 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b8_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);
    }

    #[test]
    fn test_bitpack_u16() {
        let (input, mut compressed, mut decompressed) = setup::<u16>(LEN, 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b1_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u16>(LEN, 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b2_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u16>(LEN, 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b4_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u16>(LEN, 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b8_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);
    }

    #[test]
    fn test_bitpack_i32() {
        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b1_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b2_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b4_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b8_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, 16);
        b16_pack(&input, &mut compressed);
        b16_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b16_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);
    }

    #[test]
    fn test_bitpack_u32() {
        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b1_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b2_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b4_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b8_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, 16);
        b16_pack(&input, &mut compressed);
        b16_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b16_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);
    }

    #[test]
    fn test_bitpack_i64() {
        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b1_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b2_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b4_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b8_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, 16);
        b16_pack(&input, &mut compressed);
        b16_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b16_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, 32);
        b32_pack(&input, &mut compressed);
        b32_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b32_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);
    }

    #[test]
    fn test_bitpack_u64() {
        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b1_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b2_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b4_unpack_extend(&compressed, input.len(), 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b8_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, 16);
        b16_pack(&input, &mut compressed);
        b16_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b16_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);

        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, 32);
        b32_pack(&input, &mut compressed);
        b32_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let mut res = vec![];
        for_b32_unpack_extend(&compressed, 0, &mut res);
        assert_eq!(input, res);
    }

    #[test]
    fn test_for_bitpack() {
        for _ in 0..100 {
            let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, 32);
            let min = input.iter().min().cloned().unwrap();
            for_b32_pack(&input, min, &mut compressed);
            for_b32_unpack(&compressed, min, &mut decompressed);
            assert_eq!(input, decompressed);
        }
    }

    #[test]
    fn test_for_b1_unpack_extend_basic() {
        // Test with different lengths
        let lengths: [usize; 14] = [0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 65, 127, 128, 1000];

        for &len in &lengths {
            // Generate random data with values 0 or 1 (since bits=1)
            let input: Vec<u32> = (0..len).map(|_| rand::random_range(0..2)).collect();

            // Calculate compressed size
            let compressed_len = len.div_ceil(8);
            let mut compressed = vec![0u8; compressed_len];

            // Pack the data
            for_b1_pack(&input, 0, &mut compressed);

            // Unpack using extend
            let mut result = Vec::new();
            for_b1_unpack_extend(&compressed, len, 0, &mut result);

            // Verify
            assert_eq!(input, result, "Failed for length {}", len);
        }
    }

    #[test]
    fn test_for_b1_unpack_extend_with_min() {
        // Test with non-zero min value (Frame of Reference)
        let lengths: [usize; 11] = [0, 1, 7, 8, 15, 16, 63, 64, 65, 127, 128];
        let min_values = [5, 10, 100, 1000];

        for &len in &lengths {
            for &min_val in &min_values {
                // Generate random data with values in [min_val, min_val+1]
                let input: Vec<u32> = (0..len)
                    .map(|_| min_val + rand::random_range(0..2))
                    .collect();

                let compressed_len = len.div_ceil(8);
                let mut compressed = vec![0u8; compressed_len];

                // Pack with FOR
                for_b1_pack(&input, min_val, &mut compressed);

                // Unpack using extend
                let mut result = Vec::new();
                for_b1_unpack_extend(&compressed, len, min_val, &mut result);

                assert_eq!(
                    input, result,
                    "Failed for length {} with min {}",
                    len, min_val
                );
            }
        }
    }

    #[test]
    fn test_for_b1_unpack_extend_all_types() {
        // Test all BitPackable types
        let len: usize = 100;

        // Helper macro to test a specific type
        macro_rules! test_type {
            ($t:ty, $max:expr) => {
                let input: Vec<$t> = (0..len)
                    .map(|_| {
                        // For isize/usize, we need to handle differently as they don't implement SampleUniform
                        if stringify!($t) == "isize" || stringify!($t) == "usize" {
                            // Generate random u64 and convert
                            let val: u64 = rand::random_range(0..$max);
                            val as $t
                        } else {
                            rand::random_range(0..$max) as $t
                        }
                    })
                    .collect();

                let compressed_len = len.div_ceil(8);
                let mut compressed = vec![0u8; compressed_len];

                for_b1_pack(&input, <$t>::ZERO, &mut compressed);

                let mut result = Vec::new();
                for_b1_unpack_extend(&compressed, len, <$t>::ZERO, &mut result);

                assert_eq!(input, result, "Failed for type {}", stringify!($t));
            };
        }

        test_type!(i8, 2);
        test_type!(u8, 2);
        test_type!(i16, 2);
        test_type!(u16, 2);
        test_type!(i32, 2);
        test_type!(u32, 2);
        test_type!(i64, 2);
        test_type!(u64, 2);
        // Skip isize and usize for now as they have platform-dependent size
        // and random_range doesn't support them directly
    }

    #[test]
    fn test_for_b1_unpack_extend_edge_cases() {
        // Test specific edge cases

        // Case 1: All zeros
        let len = 67;
        let input = vec![0u32; len];
        let compressed_len = len.div_ceil(8);
        let mut compressed = vec![0u8; compressed_len];

        for_b1_pack(&input, 0, &mut compressed);

        let mut result = Vec::new();
        for_b1_unpack_extend(&compressed, len, 0, &mut result);

        assert_eq!(input, result);

        // Case 2: All ones
        let input = vec![1u32; len];
        let mut compressed = vec![0u8; compressed_len];

        for_b1_pack(&input, 0, &mut compressed);

        let mut result = Vec::new();
        for_b1_unpack_extend(&compressed, len, 0, &mut result);

        assert_eq!(input, result);

        // Case 3: Alternating pattern
        let input: Vec<u32> = (0..len).map(|i| (i % 2) as u32).collect();
        let mut compressed = vec![0u8; compressed_len];

        for_b1_pack(&input, 0, &mut compressed);

        let mut result = Vec::new();
        for_b1_unpack_extend(&compressed, len, 0, &mut result);

        assert_eq!(input, result);
    }
}
