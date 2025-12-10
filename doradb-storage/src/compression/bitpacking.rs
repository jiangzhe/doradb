//! Bitpacking compression
//!
//! Current implementation only support bits=1, 2, 4, 8, 16, 32.

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
    let out_idx = out_chunks.len() * 16;
    for (src, tgt) in chunks.zip(out_chunks) {
        let src: &[u8; 8] = src.try_into().unwrap();
        let packed = u64::from_le_bytes(*src);

        for i in 0..16 {
            let delta = ((packed >> (i * 4)) & 15) as u8;
            tgt[i] = min.add_from_u8(delta);
        }
    }

    // layer 2: scalar (0..15 elements)
    if out_idx < res.len() {
        let mut shift = 0usize;
        for tgt in &mut res[out_idx..] {
            let delta = (input[input_idx] >> shift) & 15;
            *tgt = min.add_from_u8(delta);
            shift += 4;
            if shift == 8 {
                input_idx += 1;
                shift = 0;
            }
        }
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
        let val = u16::from_le_bytes(*src);
        *tgt = min.add_from_u16(val);
    });
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
        output_size: usize,
        max: u64,
    ) -> (Vec<T>, Vec<u8>, Vec<T>) {
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
        let (input, mut compressed, mut decompressed) = setup::<i8>(LEN, LEN, 1 << 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i8>(LEN, LEN, 1 << 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i8>(LEN, LEN, 1 << 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_bitpack_u8() {
        let (input, mut compressed, mut decompressed) = setup::<u8>(LEN, LEN, 1 << 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u8>(LEN, LEN, 1 << 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u8>(LEN, LEN, 1 << 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_bitpack_i16() {
        let (input, mut compressed, mut decompressed) = setup::<i16>(LEN, LEN, 1 << 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i16>(LEN, LEN, 1 << 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i16>(LEN, LEN, 1 << 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i16>(LEN, LEN, 1 << 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_bitpack_u16() {
        let (input, mut compressed, mut decompressed) = setup::<u16>(LEN, LEN, 1 << 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u16>(LEN, LEN, 1 << 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u16>(LEN, LEN, 1 << 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u16>(LEN, LEN, 1 << 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_bitpack_i32() {
        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, LEN, 1 << 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, LEN, 1 << 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, LEN, 1 << 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, LEN, 1 << 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i32>(LEN, LEN * 2, 1 << 16);
        b16_pack(&input, &mut compressed);
        b16_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_bitpack_u32() {
        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, LEN, 1 << 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, LEN, 1 << 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, LEN, 1 << 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, LEN, 1 << 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u32>(LEN, LEN * 2, 1 << 16);
        b16_pack(&input, &mut compressed);
        b16_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_bitpack_i64() {
        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, LEN, 1 << 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, LEN, 1 << 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, LEN, 1 << 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, LEN, 1 << 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, LEN * 2, 1 << 16);
        b16_pack(&input, &mut compressed);
        b16_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<i64>(LEN, LEN * 4, 1 << 32);
        b32_pack(&input, &mut compressed);
        b32_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_bitpack_u64() {
        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, LEN, 1 << 1);
        b1_pack(&input, &mut compressed);
        b1_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, LEN, 1 << 2);
        b2_pack(&input, &mut compressed);
        b2_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, LEN, 1 << 4);
        b4_pack(&input, &mut compressed);
        b4_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, LEN, 1 << 8);
        b8_pack(&input, &mut compressed);
        b8_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, LEN * 2, 1 << 16);
        b16_pack(&input, &mut compressed);
        b16_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
        let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, LEN * 4, 1 << 32);
        b32_pack(&input, &mut compressed);
        b32_unpack(&compressed, &mut decompressed);
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_for_bitpack() {
        for _ in 0..100 {
            let (input, mut compressed, mut decompressed) = setup::<u64>(LEN, LEN * 4, 1 << 32);
            let min = input.iter().min().cloned().unwrap();
            for_b32_pack(&input, min, &mut compressed);
            for_b32_unpack(&compressed, min, &mut decompressed);
            assert_eq!(input, decompressed);
        }
    }
}
