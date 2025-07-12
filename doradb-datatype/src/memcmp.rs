use crate::konst::{ValidF32, ValidF64, F32_ZERO, F64_ZERO};
use std::mem::size_of;

/// Memory comparable format ensure sort result of encoded value is
/// identical to original value.
pub trait MemCmpFormat {
    /// Returns estimated length of the type.
    /// This may return None if the length can only be determined by the runtime value.
    fn est_mcf_len() -> Option<usize>;

    /// Returns exact encoded length of this value.
    fn enc_mcf_len(value: &Self) -> usize;

    /// Attach to end of the buffer with the memory comparable format.
    fn attach_mcf(value: &Self, buf: &mut Vec<u8>);

    /// Write to buffer with memory comparable format.
    /// Client must make sure the buffer length matches the format.
    fn write_mcf(value: &Self, buf: &mut [u8]);
}

const NULL_FLAG: u8 = 0x01;
const NON_NULL_FLAG: u8 = 0x02;
const FIX_SEG_FLAG: u8 = 0xff;
const SEG_LEN: usize = 15;
pub const MIN_VAR_MCF_LEN: usize = SEG_LEN + 1;
pub const MIN_VAR_NMCF_LEN: usize = MIN_VAR_MCF_LEN + 1;

/// Nullable memory comparable format.
pub trait NullableMemCmpFormat: MemCmpFormat {
    /// Returns estimated length of the type.
    /// Should prepend 1-byte nullable flag, followed by
    /// memory comparable format.
    #[inline]
    fn est_nmcf_len() -> Option<usize> {
        <Self as MemCmpFormat>::est_mcf_len().map(|n| n + 1)
    }

    /// Returns exact encoded length of this value.
    #[inline]
    fn enc_nmcf_len(value: &Self) -> usize {
        <Self as MemCmpFormat>::enc_mcf_len(value) + 1
    }

    /// Returns exact encoded length of null.
    #[inline]
    fn enc_null_len() -> usize {
        1
    }

    /// Attach value to end of the buffer with the memory comparable format.
    #[inline]
    fn attach_nmcf(value: &Self, buf: &mut Vec<u8>) {
        buf.push(NON_NULL_FLAG);
        <Self as MemCmpFormat>::attach_mcf(value, buf);
    }
}

#[inline]
pub fn attach_null(buf: &mut Vec<u8>) {
    buf.push(NULL_FLAG);
}

macro_rules! impl_mcf_for_int {
    ($t1:ty, $flip:literal) => {
        impl MemCmpFormat for $t1 {
            #[inline]
            fn est_mcf_len() -> Option<usize> {
                Some(size_of::<$t1>())
            }

            #[inline]
            fn enc_mcf_len(_value: &Self) -> usize {
                size_of::<$t1>()
            }

            #[inline]
            fn attach_mcf(value: &Self, buf: &mut Vec<u8>) {
                let mut bs = value.to_be_bytes();
                if $flip {
                    bs[0] ^= 0x80;
                }
                buf.extend(bs);
            }

            #[inline]
            fn write_mcf(value: &Self, buf: &mut [u8]) {
                let mut bs = value.to_be_bytes();
                if $flip {
                    bs[0] ^= 0x80;
                }
                buf.copy_from_slice(&bs);
            }
        }

        impl NullableMemCmpFormat for $t1 {}
    };
}

impl_mcf_for_int!(u8, false);
impl_mcf_for_int!(u16, false);
impl_mcf_for_int!(u32, false);
impl_mcf_for_int!(u64, false);
impl_mcf_for_int!(i8, true);
impl_mcf_for_int!(i16, true);
impl_mcf_for_int!(i32, true);
impl_mcf_for_int!(i64, true);

macro_rules! impl_mcf_for_float {
    ($t1:ty, $zero:ident) => {
        impl MemCmpFormat for $t1 {
            #[inline]
            fn est_mcf_len() -> Option<usize> {
                Some(size_of::<$t1>())
            }

            #[inline]
            fn enc_mcf_len(_value: &Self) -> usize {
                size_of::<$t1>()
            }

            #[inline]
            fn attach_mcf(value: &Self, buf: &mut Vec<u8>) {
                let mut bs = value.to_be_bytes();
                if *value >= $zero {
                    bs[0] ^= 0x80;
                } else {
                    bs.iter_mut().for_each(|b| *b = !*b);
                }
                buf.extend(bs);
            }

            #[inline]
            fn write_mcf(value: &Self, buf: &mut [u8]) {
                let mut bs = value.to_be_bytes();
                if *value >= $zero {
                    bs[0] ^= 0x80;
                } else {
                    bs.iter_mut().for_each(|b| *b = !*b);
                }
                buf.copy_from_slice(&bs);
            }
        }

        impl NullableMemCmpFormat for $t1 {}
    };
}

impl_mcf_for_float!(ValidF64, F64_ZERO);
impl_mcf_for_float!(ValidF32, F32_ZERO);

macro_rules! impl_mcf_for_varlen {
    ($t1:ty) => {
        impl MemCmpFormat for $t1 {
            #[inline]
            fn est_mcf_len() -> Option<usize> {
                None
            }

            #[allow(clippy::manual_div_ceil)]
            #[inline]
            fn enc_mcf_len(value: &Self) -> usize {
                let n_segs = (value.len().max(1) + SEG_LEN - 1) / SEG_LEN;
                n_segs * (SEG_LEN + 1)
            }

            /// Attach to end of the buffer with the memory comparable format.
            fn attach_mcf(value: &Self, buf: &mut Vec<u8>) {
                attach_bytes(value.as_ref(), buf)
            }

            #[inline]
            fn write_mcf(value: &Self, buf: &mut [u8]) {
                write_bytes(value.as_ref(), buf)
            }
        }

        impl NullableMemCmpFormat for $t1 {}
    };
}

impl_mcf_for_varlen!(str);
impl_mcf_for_varlen!([u8]);

#[inline]
fn attach_bytes(bs: &[u8], buf: &mut Vec<u8>) {
    if bs.is_empty() {
        buf.extend([0; SEG_LEN + 1]); // last byte is zero
        return;
    }
    let mut chunks = bs.chunks_exact(SEG_LEN);
    if chunks.remainder().is_empty() {
        for c in chunks {
            buf.extend_from_slice(c);
            buf.push(FIX_SEG_FLAG);
        }
        // update last byte as segment length
        *buf.last_mut().unwrap() = SEG_LEN as u8;
    } else {
        for c in chunks.by_ref() {
            buf.extend_from_slice(c);
            buf.push(FIX_SEG_FLAG);
        }
        buf.extend_from_slice(chunks.remainder());
        buf.extend(std::iter::repeat_n(
            0x00,
            SEG_LEN - chunks.remainder().len(),
        ));
        buf.push(chunks.remainder().len() as u8);
    }
}

#[inline]
fn write_bytes(bs: &[u8], mut buf: &mut [u8]) {
    if bs.is_empty() {
        buf[..SEG_LEN + 1].iter_mut().for_each(|b| *b = 0);
        return;
    }
    let mut chunks = bs.chunks_exact(SEG_LEN);
    if chunks.remainder().is_empty() {
        let mut offset = 0;
        for c in chunks {
            buf[offset..offset + SEG_LEN].copy_from_slice(c);
            buf[offset + SEG_LEN] = FIX_SEG_FLAG;
            offset += SEG_LEN + 1;
        }
        // update last byte as segment length
        buf[offset - 1] = SEG_LEN as u8;
    } else {
        for c in chunks.by_ref() {
            buf[..c.len()].copy_from_slice(c);
            buf[c.len()] = FIX_SEG_FLAG;
            buf = &mut buf[c.len()..];
        }
        let rem = chunks.remainder();
        buf[..rem.len()].copy_from_slice(rem);
        buf[rem.len()..SEG_LEN].iter_mut().for_each(|b| *b = 0);
        buf[SEG_LEN] = chunks.remainder().len() as u8;
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand_distr::{Distribution, StandardUniform};

    use super::*;

    #[test]
    fn test_mcf_sized() {
        // int
        run_test_mcf::<u8>();
        run_test_mcf::<u16>();
        run_test_mcf::<u32>();
        run_test_mcf::<u64>();

        run_test_mcf::<i8>();
        run_test_mcf::<i16>();
        run_test_mcf::<i32>();
        run_test_mcf::<i64>();

        // float
        run_test_mcf::<ValidF64>();

        // int + int
        run_test_mcf2::<i32, i32>();
        run_test_mcf2::<u32, u32>();
        run_test_mcf2::<i64, i64>();
        run_test_mcf2::<u64, u64>();

        // int + float
        run_test_mcf2::<i32, ValidF64>();
        run_test_mcf2::<ValidF64, u32>();
        run_test_mcf2::<u64, ValidF64>();
        run_test_mcf2::<ValidF64, i64>();
    }

    #[test]
    fn test_mcf_varlen() {
        run_test_mcf_varlen(gen_rand_str);
        run_test_mcf_varlen(gen_rand_bytes);
    }

    #[test]
    fn test_mcf_float() {
        let f0 = ValidF64::new(-1.0).unwrap();
        let mut buf0 = vec![];
        MemCmpFormat::attach_mcf(&f0, &mut buf0);
        assert_eq!(buf0.len(), 8);
        assert!(buf0[0] & 0x80 == 0);

        let f1 = ValidF64::new(-1.0).unwrap();
        let mut buf1 = vec![];
        NullableMemCmpFormat::attach_nmcf(&f1, &mut buf1);
        assert_eq!(buf1.len(), 9);
        assert_eq!(buf1[0], NON_NULL_FLAG);
        assert!(buf1[1] & 0x80 == 0);
    }

    fn run_test_mcf<T>()
    where
        T: NullableMemCmpFormat + Ord,
        StandardUniform: Distribution<T>,
    {
        let mut r = rand::rng();
        let mut input1 = gen_input::<T>(&mut r);

        check_mcf_length(&input1[0]);

        let mut input2 = encode_mcf_input(&input1);

        sort_and_check_mcf(&mut input1, &mut input2);

        let mut input3 = gen_input::<T>(&mut r);

        check_nmcf_length(&input3[0]);

        let mut input4 = encode_nmcf_input(&input3);

        sort_and_check_nmcf(&mut input3, &mut input4);
    }

    fn run_test_mcf2<T, U>()
    where
        T: NullableMemCmpFormat + Ord,
        U: NullableMemCmpFormat + Ord,
        StandardUniform: Distribution<T> + Distribution<U>,
    {
        let mut r = rand::rng();

        // mcf
        let mut input1 = Vec::<(T, U)>::with_capacity(1024);
        for _ in 0..1024 {
            input1.push(r.sample(StandardUniform));
        }
        let mut input2 = Vec::with_capacity(1024);
        for (t, u) in &input1 {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(t) + U::enc_mcf_len(u));
            T::attach_mcf(t, &mut buf);
            U::attach_mcf(u, &mut buf);
            input2.push(buf);
        }
        input1.sort();
        input2.sort();

        for ((t, u), a) in input1.iter().zip(input2) {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(t) + U::enc_mcf_len(u));
            T::attach_mcf(t, &mut buf);
            U::attach_mcf(u, &mut buf);
            assert_eq!(buf, a);
        }

        // nmcf
        let mut input3 = Vec::<(T, U)>::with_capacity(1024);
        for _ in 0..1024 {
            input3.push(r.sample(StandardUniform));
        }
        let mut input4 = Vec::with_capacity(1024);
        for (t, v) in &input3 {
            let mut buf = Vec::with_capacity(T::enc_nmcf_len(t) + U::enc_nmcf_len(v));
            T::attach_nmcf(t, &mut buf);
            U::attach_nmcf(v, &mut buf);
            input4.push(buf);
        }
        input3.sort();
        input4.sort();

        for ((t, v), a) in input3.iter().zip(input4) {
            let mut buf = Vec::with_capacity(T::enc_nmcf_len(t) + U::enc_nmcf_len(v));
            T::attach_nmcf(t, &mut buf);
            U::attach_nmcf(v, &mut buf);
            assert_eq!(buf, a);
        }
    }

    fn run_test_mcf_varlen<T, U, F>(f: F)
    where
        T: NullableMemCmpFormat + Ord + ?Sized,
        U: std::ops::Deref<Target = T>,
        F: Fn(&mut ThreadRng) -> U,
        F: Copy,
    {
        let mut r = rand::rng();

        let mut input1 = gen_varlen_input(&mut r, f);

        check_mcf_length::<T>(&input1[0]);

        let mut input2 = encode_varlen_mcf_input(&input1);

        sort_and_check_varlen_mcf(&mut input1, &mut input2);

        let mut input3 = gen_varlen_input(&mut r, f);

        check_nmcf_length::<T>(&input3[0]);

        let mut input4 = encode_varlen_nmcf_input(&input3);

        sort_and_check_varlen_nmcf(&mut input3, &mut input4);
    }

    fn check_mcf_length<T>(value: &T)
    where
        T: MemCmpFormat + Ord + ?Sized,
    {
        if let Some(el) = T::est_mcf_len() {
            assert_eq!(el, T::enc_mcf_len(value));
        }
        let mut buf = vec![];
        T::attach_mcf(value, &mut buf);
        assert_eq!(buf.len(), T::enc_mcf_len(value));
    }

    fn check_nmcf_length<T>(value: &T)
    where
        T: NullableMemCmpFormat + Ord + ?Sized,
    {
        if let Some(el) = T::est_nmcf_len() {
            assert_eq!(el, NullableMemCmpFormat::enc_nmcf_len(value));
        }
        let mut buf = vec![];
        NullableMemCmpFormat::attach_nmcf(value, &mut buf);
        assert_eq!(buf.len(), NullableMemCmpFormat::enc_nmcf_len(value));
    }

    fn gen_input<T>(r: &mut ThreadRng) -> Vec<T>
    where
        StandardUniform: Distribution<T>,
    {
        let mut input = Vec::with_capacity(1024);
        for _ in 0..1024 {
            input.push(r.sample(StandardUniform));
        }
        input
    }

    fn gen_varlen_input<U, F>(r: &mut ThreadRng, f: F) -> Vec<U>
    where
        F: Fn(&mut ThreadRng) -> U,
    {
        let mut input = Vec::with_capacity(1024);
        for _ in 0..1024 {
            input.push(f(r));
        }
        input
    }

    fn encode_mcf_input<T>(input: &[T]) -> Vec<Vec<u8>>
    where
        T: MemCmpFormat + Ord,
    {
        let mut input2 = Vec::with_capacity(1024);
        for i in input {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(i));
            T::attach_mcf(i, &mut buf);
            // identical with write_mcf
            let mut buf2 = vec![0u8; T::enc_mcf_len(i)];
            T::write_mcf(i, &mut buf2);
            assert_eq!(buf, buf2);
            input2.push(buf);
        }
        input2
    }

    fn encode_varlen_mcf_input<T, U>(input: &[U]) -> Vec<Vec<u8>>
    where
        T: MemCmpFormat + Ord + ?Sized,
        U: std::ops::Deref<Target = T>,
    {
        let mut input2 = Vec::with_capacity(1024);
        for i in input {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(i));
            T::attach_mcf(i, &mut buf);
            input2.push(buf);
        }
        input2
    }

    fn encode_nmcf_input<T>(input: &[T]) -> Vec<Vec<u8>>
    where
        T: NullableMemCmpFormat + Ord,
    {
        let mut input2 = Vec::with_capacity(1024);
        for i in input {
            let mut buf = Vec::with_capacity(T::enc_nmcf_len(i));
            T::attach_nmcf(i, &mut buf);
            input2.push(buf);
        }
        input2
    }

    fn encode_varlen_nmcf_input<T, U>(input: &[U]) -> Vec<Vec<u8>>
    where
        T: NullableMemCmpFormat + Ord + ?Sized,
        U: std::ops::Deref<Target = T>,
    {
        let mut input2 = Vec::with_capacity(1024);
        for i in input {
            let mut buf = Vec::with_capacity(T::enc_nmcf_len(i));
            T::attach_nmcf(i, &mut buf);
            input2.push(buf);
        }
        input2
    }

    fn sort_and_check_mcf<T>(in1: &mut [T], in2: &mut [Vec<u8>])
    where
        T: MemCmpFormat + Ord,
    {
        in1.sort_by(|a, b| a.cmp(b));
        in2.sort();

        for (e, a) in in1.iter().zip(in2.iter()) {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(e));
            T::attach_mcf(e, &mut buf);
            assert_eq!(&buf, a);
        }
    }

    fn sort_and_check_varlen_mcf<T, U>(in1: &mut [U], in2: &mut [Vec<u8>])
    where
        T: MemCmpFormat + Ord + ?Sized,
        U: std::ops::Deref<Target = T>,
    {
        in1.sort_by(|a, b| a.cmp(b));
        in2.sort();

        for (e, a) in in1.iter().zip(in2.iter()) {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(e));
            T::attach_mcf(e, &mut buf);
            assert_eq!(&buf, a);
        }
    }

    fn sort_and_check_nmcf<T>(in1: &mut [T], in2: &mut [Vec<u8>])
    where
        T: NullableMemCmpFormat + Ord,
    {
        in1.sort_by(|a, b| a.cmp(b));
        in2.sort();

        for (e, a) in in1.iter().zip(in2.iter()) {
            let mut buf = Vec::with_capacity(NullableMemCmpFormat::enc_nmcf_len(e));
            NullableMemCmpFormat::attach_nmcf(e, &mut buf);
            assert_eq!(&buf, a);
        }
    }

    fn sort_and_check_varlen_nmcf<T, U>(in1: &mut [U], in2: &mut [Vec<u8>])
    where
        T: NullableMemCmpFormat + Ord + ?Sized,
        U: std::ops::Deref<Target = T>,
    {
        in1.sort_by(|a, b| a.cmp(b));
        in2.sort();

        for (e, a) in in1.iter().zip(in2.iter()) {
            let mut buf = Vec::with_capacity(T::enc_nmcf_len(e));
            T::attach_nmcf(e, &mut buf);
            assert_eq!(&buf, a);
        }
    }

    fn gen_rand_str(r: &mut ThreadRng) -> String {
        let len: u8 = r.sample(StandardUniform);
        let mut s = String::with_capacity(len as usize);
        for _ in 0..len {
            let ascii: u8 = r.sample(StandardUniform);
            s.push((ascii & 0x7f) as char);
        }
        s
    }

    fn gen_rand_bytes(r: &mut ThreadRng) -> Vec<u8> {
        let len: u8 = r.sample(StandardUniform);
        let mut s = Vec::with_capacity(len as usize);
        for _ in 0..len {
            s.push(r.sample(StandardUniform));
        }
        s
    }

    impl Distribution<ValidF64> for StandardUniform {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> ValidF64 {
            ValidF64::new(rng.sample(StandardUniform)).unwrap()
        }
    }
}
