use smallvec::{SmallVec, smallvec};
use std::io;

pub trait ByteRepr: Default + Copy {
    /// Convert value to byte vector.
    fn to_bytes(&self) -> SmallVec<[u8; 16]>;

    /// Convert bytes to value.
    fn from_bytes(bs: &[u8]) -> Self;

    /// Write value into byte format.
    fn write_bytes(&self, buf: &mut [u8]);

    /// Write out value slice in byte format.
    fn write_all<W: io::Write>(writer: &mut W, src: &[Self]) -> io::Result<usize>;
}

macro_rules! impl_num_for_byte_repr {
    ($ty:ty) => {
        impl ByteRepr for $ty {
            #[inline]
            fn to_bytes(&self) -> SmallVec<[u8; 16]> {
                self.to_ne_bytes().into_iter().collect()
            }

            #[inline]
            fn from_bytes(bs: &[u8]) -> Self {
                <$ty>::from_ne_bytes(bs.try_into().unwrap())
            }

            #[inline]
            fn write_bytes(&self, buf: &mut [u8]) {
                let bs = self.to_ne_bytes();
                buf.copy_from_slice(&bs);
            }

            #[inline]
            fn write_all<W: io::Write>(writer: &mut W, src: &[Self]) -> io::Result<usize> {
                use std::mem::size_of;
                let n_bytes = src.len() * size_of::<$ty>();
                let src = unsafe { std::slice::from_raw_parts(src.as_ptr() as *const u8, n_bytes) };
                writer.write_all(src)?;
                Ok(n_bytes)
            }
        }
    };
}

impl_num_for_byte_repr!(i16);
impl_num_for_byte_repr!(u16);
impl_num_for_byte_repr!(i32);
impl_num_for_byte_repr!(u32);
impl_num_for_byte_repr!(i64);
impl_num_for_byte_repr!(u64);

impl ByteRepr for u8 {
    #[inline]
    fn to_bytes(&self) -> SmallVec<[u8; 16]> {
        smallvec![*self]
    }

    #[inline]
    fn from_bytes(bs: &[u8]) -> Self {
        bs[0]
    }

    #[inline]
    fn write_bytes(&self, buf: &mut [u8]) {
        buf[0] = *self;
    }

    #[inline]
    fn write_all<W: io::Write>(writer: &mut W, src: &[u8]) -> io::Result<usize> {
        writer.write_all(src)?;
        Ok(src.len())
    }
}

pub trait SMARepr {
    /// Wrap subtract.
    fn wrap_sub(self, rhs: Self) -> Self;

    /// Cast value to u8.
    fn to_u8(self) -> u8;

    /// Cast value to u16.
    fn to_u16(self) -> u16;

    /// Cast value to u32.
    fn to_u32(self) -> u32;

    /// Cast value to u64.
    fn to_u64(self) -> u64;
}

macro_rules! impl_num_for_sma_repr {
    ($ty:ty) => {
        impl SMARepr for $ty {
            #[inline]
            fn wrap_sub(self, rhs: Self) -> Self {
                self.wrapping_sub(rhs)
            }

            #[inline]
            fn to_u8(self) -> u8 {
                self as u8
            }

            #[inline]
            fn to_u16(self) -> u16 {
                self as u16
            }

            #[inline]
            fn to_u32(self) -> u32 {
                self as u32
            }

            #[inline]
            fn to_u64(self) -> u64 {
                self as u64
            }
        }
    };
}

impl_num_for_sma_repr!(i16);
impl_num_for_sma_repr!(u16);
impl_num_for_sma_repr!(i32);
impl_num_for_sma_repr!(u32);
impl_num_for_sma_repr!(i64);
impl_num_for_sma_repr!(u64);

pub trait LeadingNonZeroByte {
    /// Returns leading non zero byte and its index.
    fn leading_non_zero_byte(self) -> (usize, u8);
}

macro_rules! impl_num_for_lnzb {
    ($ty:ty) => {
        impl LeadingNonZeroByte for $ty {
            #[inline]
            fn leading_non_zero_byte(self) -> (usize, u8) {
                let mut idx = 0;
                let bs = self.to_be_bytes();
                for b in bs {
                    if b != 0 {
                        return (idx, b);
                    }
                    idx += 1;
                }
                (idx - 1, 0)
            }
        }
    };
}

impl_num_for_lnzb!(i16);
impl_num_for_lnzb!(u16);
impl_num_for_lnzb!(i32);
impl_num_for_lnzb!(u32);
impl_num_for_lnzb!(i64);
impl_num_for_lnzb!(u64);

impl LeadingNonZeroByte for u8 {
    #[inline]
    fn leading_non_zero_byte(self) -> (usize, u8) {
        (0, self)
    }
}
