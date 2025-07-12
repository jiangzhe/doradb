use crate::mysql::error::{Error, Result};
use crate::mysql::flag::*;
use crate::mysql::serde::{
    LenEncInt, MyDeser, MyDeserExt, MySerElem, MySerPackets, NewMySer, SerdeCtx,
};
use std::borrow::Cow;

/// Ok Packet
///
/// reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OkPacket<'a> {
    pub header: u8,
    // actually len-enc-int
    pub affected_rows: u64,
    // actually len-enc-int
    pub last_insert_id: u64,
    // if PROTOCOL_41 or TRANSACTIONS enabled
    pub status_flags: StatusFlags,
    // if PROTOCOL_41 enabled
    pub warnings: u16,
    // if SESSION_TRACK enabled: len-enc-str
    // else: EOF-terminated string
    pub info: Cow<'a, [u8]>,
    // if SESSION_TRACK and SESSION_STATE_CHANGED enabled
    pub session_state_changes: Cow<'a, [u8]>,
}

impl NewMySer for OkPacket<'_> {
    type Ser<'s>
        = MySerPackets<'s, 7>
    where
        Self: 's;

    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        // u8 header
        let header = MySerElem::one_byte(self.header);
        // len-enc-int affected rows
        let affected_rows = MySerElem::len_enc_int(LenEncInt::from(self.affected_rows));
        // len-enc-int last insert id
        let last_insert_id = MySerElem::len_enc_int(LenEncInt::from(self.last_insert_id));
        // if PROTOCOL_41 or TRANSACTIONS enabled
        let status_flags = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41)
            || ctx.cap_flags.contains(CapabilityFlags::TRANSACTIONS)
        {
            MySerElem::le_u16(self.status_flags.bits())
        } else {
            MySerElem::empty()
        };
        // if PROTOCOL_41 enabled
        let warnings = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            MySerElem::le_u16(self.warnings)
        } else {
            MySerElem::empty()
        };
        // if SESSION_TRACK enabled: len-enc-str
        // else: EOF-terminated string
        let info = if ctx.cap_flags.contains(CapabilityFlags::SESSION_TRACK) {
            MySerElem::len_enc_str(&*self.info)
        } else {
            MySerElem::slice(&self.info)
        };
        // if SESSION_TRACK and SESSION_STATE_CHANGED enabled
        let session_state_changes = if ctx.cap_flags.contains(CapabilityFlags::SESSION_TRACK)
            && self
                .status_flags
                .contains(StatusFlags::SESSION_STATE_CHANGED)
        {
            MySerElem::len_enc_str(&*self.session_state_changes)
        } else {
            MySerElem::empty()
        };
        MySerPackets::new(
            ctx,
            [
                header,
                affected_rows,
                last_insert_id,
                status_flags,
                warnings,
                info,
                session_state_changes,
            ],
        )
    }
}

impl<'a> MyDeser<'a> for OkPacket<'a> {
    #[inline]
    fn my_deser(ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        // header can be either 0x00 or 0xfe
        let header = input.try_deser_u8()?;
        if header != 0x00 && header != 0xfe {
            return Err(Error::MalformedPacket());
        }
        let affected_rows = input.try_deser_len_enc_int()?.try_into()?;
        let last_insert_id = input.try_deser_len_enc_int()?.try_into()?;
        let status_flags = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41)
            || ctx.cap_flags.contains(CapabilityFlags::TRANSACTIONS)
        {
            let status_flags = input.try_deser_le_u16()?;
            StatusFlags::from_bits(status_flags).ok_or(Error::InvalidStatusFlags(status_flags))?
        } else {
            StatusFlags::empty()
        };
        let warnings = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            input.try_deser_le_u16()?
        } else {
            0
        };
        let info = if ctx.cap_flags.contains(CapabilityFlags::SESSION_TRACK) {
            input.try_deser_len_enc_str()?.try_into()?
        } else {
            input.deser_to_end()
        };
        let session_state_changes = if ctx.cap_flags.contains(CapabilityFlags::SESSION_TRACK)
            && status_flags.contains(StatusFlags::SESSION_STATE_CHANGED)
        {
            input.try_deser_len_enc_str()?.try_into()?
        } else {
            &[][..]
        };
        let res = OkPacket {
            header,
            affected_rows,
            last_insert_id,
            status_flags,
            warnings,
            info: Cow::Borrowed(info),
            session_state_changes: Cow::Borrowed(session_state_changes),
        };
        Ok((*input, res))
    }
}

impl_from_ref!(OkPacket: header, affected_rows, last_insert_id, status_flags, warnings; info, session_state_changes);

/// Err Packet
///
/// reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrPacket<'a> {
    // pub header: u8,
    pub code: u16,
    // if PROTOCOL_41 enabled: string[1]
    // pub sql_state_marker: u8,
    // if PROTOCOL_41 enabled: string[5]
    pub state: [u8; 5],
    // EOF-terminated string
    pub msg: Cow<'a, [u8]>,
}

impl NewMySer for ErrPacket<'_> {
    type Ser<'s>
        = MySerPackets<'s, 3>
    where
        Self: 's;

    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        let mut elem0 = [0u8; 4]; // combine header, error code, state marker
        elem0[0] = 0xff;
        elem0[1] = self.code as u8;
        elem0[2] = (self.code >> 8) as u8;
        let sql_state = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            // set marker to '#'
            elem0[3] = b'#';
            MySerElem::slice(&self.state)
        } else {
            MySerElem::empty()
        };
        MySerPackets::new(
            ctx,
            [
                MySerElem::inline_bytes(&elem0),
                sql_state,
                MySerElem::slice(&self.msg),
            ],
        )
    }
}

impl<'a> MyDeser<'a> for ErrPacket<'a> {
    #[inline]
    fn my_deser(ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let header = input.try_deser_u8()?;
        if header != 0xff {
            return Err(Error::MalformedPacket());
        }
        let code = input.try_deser_le_u16()?;
        // handshake stage has different behavior?
        let state = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            let marker = input.try_deser_u8()?;
            if marker != b'#' {
                return Err(Error::MalformedPacket());
            }
            let b = input.try_deser_bytes(5usize)?;
            b.try_into().unwrap()
        } else {
            [0u8; 5]
        };
        let msg = input.deser_to_end();
        let res = ErrPacket {
            code,
            state,
            msg: Cow::Borrowed(msg),
        };
        Ok((*input, res))
    }
}

impl_from_ref!(ErrPacket: code, state; msg);

/// EOF Packet
/// This kind of packet is deprecated.
/// Since MySQL 5.7.5, it's replace with Ok Packet.
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EofPacket {
    // if PROTOCOL_41 enabled
    pub warnings: u16,
    // if PROTOCOL_41 enabled
    pub status_flags: StatusFlags,
}

impl EofPacket {
    #[inline]
    pub fn new(warnings: u16, status_flags: StatusFlags) -> Self {
        EofPacket {
            warnings,
            status_flags,
        }
    }
}

impl NewMySer for EofPacket {
    type Ser<'s>
        = MySerPackets<'s, 1>
    where
        Self: 's;
    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        let mut b = [0u8; 5];
        b[0] = 0xFE; // EOF header is 0xFE
        if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            // warnings in little endian.
            b[1] = self.warnings as u8;
            b[2] = (self.warnings >> 8) as u8;
            // status flags in little endian.
            b[3] = self.status_flags.bits() as u8;
            b[4] = (self.status_flags.bits() >> 8) as u8;
        }
        MySerPackets::new(ctx, [MySerElem::inline_bytes(&b)])
    }
}

impl<'a> MyDeser<'a> for EofPacket {
    #[inline]
    fn my_deser(ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let header = input.try_deser_u8()?;
        debug_assert_eq!(header, 0xFE, "EOF packet header mismatch");
        let (warnings, status_flags) = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            let warnings = input.try_deser_le_u16()?;
            let status_flags = input.try_deser_le_u16()?;
            let status_flags = StatusFlags::from_bits(status_flags)
                .ok_or(Error::InvalidStatusFlags(status_flags))?;
            (warnings, status_flags)
        } else {
            (0, StatusFlags::empty())
        };
        let res = EofPacket {
            warnings,
            status_flags,
        };
        Ok((*input, res))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::ByteBuffer;
    use crate::mysql::serde::tests::check_ser_and_deser;

    #[test]
    fn test_serde_ok_packet() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let ok = OkPacket {
            header: 0,
            affected_rows: 1,
            last_insert_id: 3,
            status_flags: StatusFlags::AUTOCOMMIT,
            warnings: 0,
            info: Cow::Borrowed(b""),
            session_state_changes: Cow::Borrowed(b""),
        };
        check_ser_and_deser(&mut ctx, &ok, &buf);
    }

    #[test]
    fn test_serde_err_packet() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let err = ErrPacket {
            code: 1304,
            state: *b"01S01",
            msg: Cow::Borrowed(b"internal error"),
        };
        check_ser_and_deser(&mut ctx, &err, &buf);
    }

    #[test]
    fn test_serde_eof_packet() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let eof = EofPacket {
            status_flags: StatusFlags::AUTOCOMMIT,
            warnings: 0,
        };
        check_ser_and_deser(&mut ctx, &eof, &buf);
    }
}
