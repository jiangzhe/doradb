use crate::mysql::error::{Error, Result, ensure_empty};
use crate::mysql::serde::{LenEncInt, LenEncStr, SerdeCtx};

/// Defines how to deserialize objects from bytes.
/// All MySQL commands and events should implement this trait.
/// This trait is designed to handle network packets with
/// minumum memory copy. That means if the buffer is sufficient,
/// the deserialized object will has same lifetime as the
/// buffer.
/// If buffer is not sufficent to hold a complete packet,
/// we allocate new memory to concatenate the content before
/// deserializing. The new memory will be held by context and
/// release when calling [`SerdeCtx::release_bufs`].
pub trait MyDeser<'a>: Sized {
    /// Deserialize objects from input.
    /// Returns the remaining input.
    /// If [`Error::InsufficientInput`] is returned, it means more inputs
    /// should be concatenated.
    fn my_deser(ctx: &mut SerdeCtx, input: &'a [u8]) -> Result<(&'a [u8], Self)>;
}

/// Read data from a fixed memory area.
pub trait MyDeserExt<'a> {
    fn advance(&mut self, len: usize);

    fn try_advance(&mut self, len: usize) -> Result<()>;

    fn deser_u8(&mut self) -> u8;

    fn try_deser_u8(&mut self) -> Result<u8>;

    fn deser_le_u16(&mut self) -> u16;

    fn try_deser_le_u16(&mut self) -> Result<u16>;

    fn deser_le_u24(&mut self) -> u32;

    fn try_deser_le_u24(&mut self) -> Result<u32>;

    fn deser_le_u32(&mut self) -> u32;

    fn try_deser_le_u32(&mut self) -> Result<u32>;

    fn deser_le_u64(&mut self) -> u64;

    fn try_deser_le_u64(&mut self) -> Result<u64>;

    fn deser_le_u128(&mut self) -> u128;

    fn try_deser_le_u128(&mut self) -> Result<u128>;

    fn deser_le_f32(&mut self) -> f32;

    fn try_deser_le_f32(&mut self) -> Result<f32>;

    fn deser_le_f64(&mut self) -> f64;

    fn try_deser_le_f64(&mut self) -> Result<f64>;

    fn deser_bytes(&mut self, len: usize) -> &'a [u8];

    fn try_deser_bytes(&mut self, len: usize) -> Result<&'a [u8]>;

    fn try_deser_until(&mut self, b: u8, inclusive: bool) -> Result<&'a [u8]>;

    fn deser_to_end(&mut self) -> &'a [u8];

    fn deser_len_enc_int(&mut self) -> LenEncInt;

    fn try_deser_len_enc_int(&mut self) -> Result<LenEncInt>;

    fn deser_len_enc_str(&mut self) -> LenEncStr<'a>;

    fn try_deser_len_enc_str(&mut self) -> Result<LenEncStr<'a>>;

    fn try_deser_prefix_1b_str(&mut self) -> Result<&'a [u8]> {
        let len = self.try_deser_u8()?;
        self.try_deser_bytes(len as usize)
    }
}

macro_rules! impl_read_primitive {
    ($f1:ident, $f2:ident, $ty:tt::$conv:tt) => {
        #[inline]
        fn $f1(&mut self) -> $ty {
            const SIZE: usize = std::mem::size_of::<$ty>();
            let src: [_; SIZE] = (&*self)[..SIZE].try_into().unwrap();
            let res = $ty::$conv(src);
            self.advance(SIZE);
            res
        }

        #[inline]
        fn $f2(&mut self) -> Result<$ty> {
            if self.len() < std::mem::size_of::<$ty>() {
                return Err(Error::InsufficientInput());
            }
            Ok(self.$f1())
        }
    };
}

impl<'a> MyDeserExt<'a> for &'a [u8] {
    #[inline]
    fn advance(&mut self, len: usize) {
        *self = &(*self)[len..]
    }

    #[inline]
    fn try_advance(&mut self, len: usize) -> Result<()> {
        if self.len() < len {
            return Err(Error::InsufficientInput());
        }
        self.advance(len);
        Ok(())
    }

    #[inline]
    fn deser_u8(&mut self) -> u8 {
        let n = self[0];
        self.advance(1);
        n
    }

    #[inline]
    fn try_deser_u8(&mut self) -> Result<u8> {
        if self.is_empty() {
            return Err(Error::InsufficientInput());
        }
        Ok(self.deser_u8())
    }

    impl_read_primitive!(deser_le_u16, try_deser_le_u16, u16::from_le_bytes);
    impl_read_primitive!(deser_le_u32, try_deser_le_u32, u32::from_le_bytes);
    impl_read_primitive!(deser_le_u64, try_deser_le_u64, u64::from_le_bytes);
    impl_read_primitive!(deser_le_u128, try_deser_le_u128, u128::from_le_bytes);

    #[inline]
    fn deser_le_u24(&mut self) -> u32 {
        let res = deser_le_u24(self);
        self.advance(3);
        res
    }

    #[inline]
    fn try_deser_le_u24(&mut self) -> Result<u32> {
        if self.len() < 3 {
            return Err(Error::InsufficientInput());
        }
        Ok(self.deser_le_u24())
    }

    #[inline]
    fn deser_le_f32(&mut self) -> f32 {
        let n = self.deser_le_u32();
        f32::from_bits(n)
    }

    #[inline]
    fn try_deser_le_f32(&mut self) -> Result<f32> {
        if self.len() < std::mem::size_of::<f32>() {
            return Err(Error::InsufficientInput());
        }
        Ok(self.deser_le_f32())
    }

    #[inline]
    fn deser_le_f64(&mut self) -> f64 {
        let n = self.deser_le_u64();
        f64::from_bits(n)
    }

    #[inline]
    fn try_deser_le_f64(&mut self) -> Result<f64> {
        if self.len() < std::mem::size_of::<f64>() {
            return Err(Error::InsufficientInput());
        }
        Ok(self.deser_le_f64())
    }

    #[inline]
    fn deser_bytes(&mut self, len: usize) -> &'a [u8] {
        let res = &(*self)[..len];
        self.advance(len);
        res
    }

    #[inline]
    fn try_deser_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.len() < len {
            return Err(Error::InsufficientInput());
        }
        Ok(self.deser_bytes(len))
    }

    #[inline]
    fn try_deser_until(&mut self, b: u8, inclusive: bool) -> Result<&'a [u8]> {
        match (*self).iter().position(|&x| x == b) {
            Some(pos) => {
                let res = if inclusive {
                    &(*self)[..pos + 1]
                } else {
                    &(*self)[..pos]
                };
                self.advance(pos + 1);
                Ok(res)
            }
            None => Err(Error::MalformedPacket()),
        }
    }

    #[inline]
    fn deser_to_end(&mut self) -> &'a [u8] {
        let len = self.len();
        // self.read_bytes(len)
        let res = &(*self)[..len];
        self.advance(len);
        res
    }

    #[inline]
    fn deser_len_enc_int(&mut self) -> LenEncInt {
        let len = self.deser_u8();
        match len {
            0xfb => LenEncInt::Null,
            0xfc => {
                let n = self.deser_le_u16();
                LenEncInt::Len3(n)
            }
            0xfd => {
                let n = self.deser_le_u24();
                LenEncInt::Len4(n)
            }
            0xfe => {
                let n = self.deser_le_u64();
                LenEncInt::Len9(n)
            }
            0xff => LenEncInt::Err,
            _ => LenEncInt::Len1(len),
        }
    }

    #[inline]
    fn try_deser_len_enc_int(&mut self) -> Result<LenEncInt> {
        let len = self.try_deser_u8()?;
        let res = match len {
            0xfb => LenEncInt::Null,
            0xfc => {
                let n = self.try_deser_le_u16()?;
                LenEncInt::Len3(n)
            }
            0xfd => {
                let n = self.try_deser_le_u24()?;
                LenEncInt::Len4(n)
            }
            0xfe => {
                let n = self.try_deser_le_u64()?;
                LenEncInt::Len9(n)
            }
            0xff => LenEncInt::Err,
            _ => LenEncInt::Len1(len),
        };
        Ok(res)
    }

    #[inline]
    fn deser_len_enc_str(&mut self) -> LenEncStr<'a> {
        let lei = self.deser_len_enc_int();
        match lei {
            LenEncInt::Err => LenEncStr::Err,
            LenEncInt::Null => LenEncStr::Null,
            _ => {
                let len = u64::try_from(lei).unwrap() as usize;
                let res = self.deser_bytes(len);
                LenEncStr::Bytes(res)
            }
        }
    }

    #[inline]
    fn try_deser_len_enc_str(&mut self) -> Result<LenEncStr<'a>> {
        let lei = self.try_deser_len_enc_int()?;
        let res = match lei {
            LenEncInt::Err => LenEncStr::Err,
            LenEncInt::Null => LenEncStr::Null,
            _ => {
                let len = u64::try_from(lei)? as usize;
                let res = self.try_deser_bytes(len)?;
                LenEncStr::Bytes(res)
            }
        };
        Ok(res)
    }
}

#[inline]
pub fn deser_le_u24(data: &[u8]) -> u32 {
    data[0] as u32 + ((data[1] as u32) << 8) + ((data[2] as u32) << 16)
}

/// Deserialize MySQL packet from a fixed slice.
#[inline]
pub fn my_deser_packet<'a, T: MyDeser<'a>>(ctx: &mut SerdeCtx, mut buf: &'a [u8]) -> Result<T> {
    let (input, payload_len, pkt_nr) = {
        let buf = &mut buf;
        let payload_len = buf.try_deser_le_u24()?;
        let pkt_nr = buf.try_deser_u8()?;
        (*buf, payload_len as usize, pkt_nr)
    };
    ctx.check_and_inc_pkt_nr(pkt_nr)?;
    if payload_len != input.len() {
        return Err(Error::MalformedPacket());
    }
    let (next, res) = T::my_deser(ctx, buf)?;
    ensure_empty(next)?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use crate::mysql::serde::{LenEncInt, LenEncStr, MyDeserExt, MySerExt};

    #[test]
    fn test_deser_primitives() {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut buf = vec![0u8; 1024 * 1024 * 18];
        let data: Vec<u8> = (0..1024 * 1024 * 17).map(|_| rng.r#gen()).collect();
        // serialize
        let ser_buf = &mut buf[..];
        let ser_buf = ser_buf.ser_le_f32(0.1);
        let ser_buf = ser_buf.ser_le_f64(0.2);
        let ser_buf = ser_buf.ser_len_enc_int(LenEncInt::Null);
        let ser_buf = ser_buf.ser_len_enc_int(LenEncInt::Err);
        let ser_buf = ser_buf.ser_len_enc_int(LenEncInt::from(1024u16)); // u16
        let ser_buf = ser_buf.ser_len_enc_int(LenEncInt::from(1024u32 * 1024)); // u24
        let ser_buf = ser_buf.ser_len_enc_int(LenEncInt::from(1024u64 * 1024 * 1024 * 1024)); // u64
        let ser_buf = ser_buf.ser_len_enc_str(LenEncStr::Null);
        let ser_buf = ser_buf.ser_len_enc_str(LenEncStr::Err);
        let ser_buf = ser_buf.ser_len_enc_str(LenEncStr::from(&data[..1024])); // u16 les
        let ser_buf = ser_buf.ser_len_enc_str(LenEncStr::from(&data[..1024 * 128])); // u24 les
        let _ = ser_buf.ser_len_enc_str(LenEncStr::from(&data[..1024 * 1024 * 17])); // u64 les
        // deserialize
        let de_buf = &mut &buf[..];
        let v = de_buf.try_deser_le_f32().unwrap();
        assert_eq!(v, 0.1);
        let v = de_buf.try_deser_le_f64().unwrap();
        assert_eq!(v, 0.2);
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::Null);
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::Err);
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::from(1024u16));
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::from(1024u32 * 1024));
        let v = de_buf.try_deser_len_enc_int().unwrap();
        assert_eq!(v, LenEncInt::from(1024u64 * 1024 * 1024 * 1024));
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::Null);
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::Err);
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::from(&data[..1024]));
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::from(&data[..1024 * 128]));
        let v = de_buf.try_deser_len_enc_str().unwrap();
        assert_eq!(v, LenEncStr::from(&data[..1024 * 1024 * 17]));
    }
}
