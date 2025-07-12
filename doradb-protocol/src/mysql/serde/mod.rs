mod de;
mod ser;

use crate::buf::ByteBuffer;
use crate::mysql::cmd::CmdCode;
use crate::mysql::error::{Error, Result};
use crate::mysql::flag::{CapabilityFlags, StatusFlags};
use semistr::SemiStr;

pub use de::{deser_le_u24, my_deser_packet, MyDeser, MyDeserExt};
pub use ser::{MySer, MySerElem, MySerExt, MySerKind, MySerPackets, NewMySer};

/// Server and client modes have different behaviors.
pub enum SerdeMode {
    Server,
    Client,
}

/// Context contains session level runtime status, user variables
/// and buffer to store intermediate data.
pub struct SerdeCtx {
    /// Capability flags indicates the expectation and behavior
    /// of both server and client.
    pub cap_flags: CapabilityFlags,
    /// Status flags indicates server status, e.g. transaction
    /// status, cursor status, result set status, etc.
    pub status_flags: StatusFlags,
    /// current command code if exists.
    /// this may be used to distinguish deserialization behavior.
    pub curr_cmd: Option<CmdCode>,
    /// Packet number starts from 0, auto increment by 1 per packet.
    /// Wrapped if overflow.
    pkt_nr: u8,
    /// Maximum payload size, used to split packets if payload is too long.
    /// It should not be changed once the context is used.
    /// Otherwise, the serializaion/deserialization may be wrong.
    pub(crate) max_payload_size: usize,

    pub mode: SerdeMode,
}

unsafe impl Send for SerdeCtx {}
unsafe impl Sync for SerdeCtx {}

impl Default for SerdeCtx {
    #[inline]
    fn default() -> Self {
        SerdeCtx {
            cap_flags: CapabilityFlags::default(),
            status_flags: StatusFlags::empty(),
            curr_cmd: None,
            pkt_nr: 0,
            max_payload_size: 0xffffff,
            mode: SerdeMode::Client, // by default use client mode
        }
    }
}

impl SerdeCtx {
    #[inline]
    pub fn with_mode(mut self, mode: SerdeMode) -> Self {
        self.mode = mode;
        self
    }

    #[inline]
    pub fn pkt_nr(&self) -> u8 {
        self.pkt_nr
    }

    /// Update max payload size.
    /// This method is used only for test.
    #[cfg(test)]
    #[inline]
    pub fn set_max_payload_size(&mut self, size: usize) {
        self.max_payload_size = size;
    }

    /// Reset packet number to zero.
    /// There are two cases to reset packet number:
    /// 1. After handshake process is completed.
    /// 2. Before each command sent from client.
    #[inline]
    pub fn reset_pkt_nr(&mut self) {
        self.pkt_nr = 0;
    }

    #[inline]
    pub fn set_pkt_nr(&mut self, pkt_nr: u8) {
        self.pkt_nr = pkt_nr;
    }

    #[inline]
    pub fn check_and_inc_pkt_nr(&mut self, pkt_nr: u8) -> Result<()> {
        if self.pkt_nr != pkt_nr {
            return Err(Error::NetPacketOutOfOrder());
        }
        self.pkt_nr = self.pkt_nr.wrapping_add(1);
        Ok(())
    }

    #[inline]
    pub fn add_pkt_nr(&mut self, nr: usize) {
        self.pkt_nr = ((self.pkt_nr as usize) + nr) as u8
    }
}

/// MySQL length encoded integer.
/// Meaning of 1-byte prefix:
/// 0x00 ~ 0xfa => 1-byte integer.
/// 0xfb        => Null.
/// 0xfc        => followed by 2-byte integer.
/// 0xfd        => followed by 3-byte integer.
/// 0xfe        => followed by 8-byte integer.
/// 0xff        => Error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LenEncInt {
    Null,
    Err,
    Len1(u8),
    Len3(u16),
    Len4(u32),
    Len9(u64),
}

impl LenEncInt {
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            LenEncInt::Null | LenEncInt::Err | LenEncInt::Len1(_) => 1,
            LenEncInt::Len3(_) => 3,
            LenEncInt::Len4(_) => 4,
            LenEncInt::Len9(_) => 9,
        }
    }
}

impl TryFrom<LenEncInt> for u64 {
    type Error = Error;
    #[inline]
    fn try_from(src: LenEncInt) -> Result<Self> {
        match src {
            LenEncInt::Len1(n) => Ok(n as u64),
            LenEncInt::Len3(n) => Ok(n as u64),
            LenEncInt::Len4(n) => Ok(n as u64),
            LenEncInt::Len9(n) => Ok(n),
            _ => Err(Error::InvalidLengthEncoding()),
        }
    }
}

/// convert u64 to len-enc-int
impl From<u64> for LenEncInt {
    fn from(src: u64) -> Self {
        if src < 0xfb {
            LenEncInt::Len1(src as u8)
        } else if src <= 0xffff {
            LenEncInt::Len3(src as u16)
        } else if src <= 0xffffff {
            LenEncInt::Len4(src as u32)
        } else {
            LenEncInt::Len9(src)
        }
    }
}

/// convert u8 to len-enc-int
impl From<u8> for LenEncInt {
    fn from(src: u8) -> Self {
        LenEncInt::Len1(src)
    }
}

/// convert u16 to len-enc-int
impl From<u16> for LenEncInt {
    fn from(src: u16) -> Self {
        if src <= 0xfb {
            LenEncInt::Len1(src as u8)
        } else {
            LenEncInt::Len3(src)
        }
    }
}

/// convert u32 to len-enc-int
impl From<u32> for LenEncInt {
    fn from(src: u32) -> Self {
        if src <= 0xfb {
            LenEncInt::Len1(src as u8)
        } else if src <= 0xffff {
            LenEncInt::Len3(src as u16)
        } else {
            LenEncInt::Len4(src)
        }
    }
}

/// MySQL length encoded string.
/// Prefixed a length encoded integer, indicating the string length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LenEncStr<'a> {
    Null,
    Err,
    Bytes(&'a [u8]),
}

impl<'a> LenEncStr<'a> {
    #[inline]
    pub fn into_bytes(self) -> Option<&'a [u8]> {
        match self {
            LenEncStr::Bytes(bs) => Some(bs),
            _ => None,
        }
    }

    /// Returns encoded(prefix) length and total length of this string.
    #[inline]
    pub fn len(&self) -> (LenEncInt, usize) {
        match self {
            LenEncStr::Null => (LenEncInt::Null, 1),
            LenEncStr::Err => (LenEncInt::Err, 1),
            LenEncStr::Bytes(bs) => {
                let lei = LenEncInt::from(bs.len() as u64);
                (lei, lei.len() + bs.len())
            }
        }
    }
}

impl<'a> TryFrom<LenEncStr<'a>> for &'a [u8] {
    type Error = Error;
    #[inline]
    fn try_from(src: LenEncStr<'a>) -> Result<Self> {
        match src {
            LenEncStr::Bytes(bs) => Ok(bs),
            _ => Err(Error::InvalidLengthEncoding()),
        }
    }
}

impl TryFrom<LenEncStr<'_>> for SemiStr {
    type Error = Error;
    #[inline]
    fn try_from(src: LenEncStr) -> Result<Self> {
        match src {
            LenEncStr::Bytes(bs) => {
                let s = std::str::from_utf8(bs).map_err(|_| Error::InvalidUtf8String())?;
                SemiStr::try_from(s).map_err(|_| Error::WrongStringLength())
            }
            _ => Err(Error::InvalidLengthEncoding()),
        }
    }
}

impl<'a> From<&'a [u8]> for LenEncStr<'a> {
    #[inline]
    fn from(src: &'a [u8]) -> Self {
        LenEncStr::Bytes(src)
    }
}

impl<'a> From<&'a str> for LenEncStr<'a> {
    #[inline]
    fn from(src: &'a str) -> Self {
        LenEncStr::Bytes(src.as_bytes())
    }
}

pub trait LenEncStrExt {
    /// Returns number of bytes if converted to len-enc-str
    fn len_enc_str_len(&self) -> usize;
}

impl LenEncStrExt for [u8] {
    #[inline]
    fn len_enc_str_len(&self) -> usize {
        let slen = self.len();
        LenEncInt::from(slen as u64).len() + slen
    }
}

pub(crate) mod tests {
    use super::*;

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn check_ser_and_deser<
        'a,
        T: NewMySer + MyDeser<'a> + PartialEq + std::fmt::Debug,
    >(
        ctx: &mut SerdeCtx,
        val: &T,
        buf: &'a ByteBuffer,
    ) {
        // serialize
        check_ser(ctx, val, buf);
        // deserialize
        let v2: T = check_deser(ctx, buf);
        assert_eq!(val, &v2);
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn check_ser<T: NewMySer>(ctx: &mut SerdeCtx, val: &T, buf: &ByteBuffer) {
        // serialize
        ctx.reset_pkt_nr();
        let mut wg = buf.writable().unwrap();
        let w_len = val.new_my_ser(ctx).my_ser(ctx, &mut wg, 0);
        drop(wg);
        buf.advance_w_idx(w_len).unwrap();
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn check_deser<'a, T: MyDeser<'a>>(ctx: &mut SerdeCtx, buf: &'a ByteBuffer) -> T {
        // deserialize
        ctx.reset_pkt_nr();
        let rg = buf.readable().unwrap();
        let res = my_deser_packet(ctx, rg.all()).unwrap();
        rg.advance_to_end();
        res
    }
}
