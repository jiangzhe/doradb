use crate::mysql::error::{Error, Result};
use crate::mysql::flag::CapabilityFlags;
use crate::mysql::packet::{ErrPacket, OkPacket};
use crate::mysql::serde::{
    LenEncInt, LenEncStr, MyDeser, MyDeserExt, MySerElem, MySerExt, MySerPackets, NewMySer,
    SerdeCtx,
};
use std::borrow::Cow;

use super::serde::LenEncStrExt;
pub const AUTH_PLUGIN_DATA_LEN1: usize = 8;

/// Initial handshake packet.
/// Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_initial_handshake
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialHandshake<'a> {
    pub protocol_version: u8,
    pub server_version: Cow<'a, [u8]>,
    pub connection_id: u32,
    pub auth_plugin_data_1: Cow<'a, [u8]>,
    // filler 0x00
    // pub capability_flags_lower: u16,
    pub charset: u8,
    pub status_flags: u16,
    // pub capability_flags_upper: u16,
    pub capability_flags: u32,
    pub auth_plugin_data_length: u8,
    // reserved 10 bytes
    pub auth_plugin_data_2: Cow<'a, [u8]>,
    pub auth_plugin_name: Cow<'a, [u8]>,
}

impl<'a> NewMySer for InitialHandshake<'a> {
    type Ser<'s>
        = MySerPackets<'s, 8>
    where
        Self: 's;
    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        let mut elem4 = [0u8; 9]; // 1B filler + 2B cap flags lower + 1B charset + 2B status flags + 2B cap flags upper + 1B auth plugin data len
                                  // first byte is filler
        elem4[1..3].copy_from_slice(&(self.capability_flags as u16).to_le_bytes());
        elem4[3] = self.charset;
        elem4[4..6].copy_from_slice(&self.status_flags.to_le_bytes());
        elem4[6..8].copy_from_slice(&((self.capability_flags >> 16) as u16).to_le_bytes());
        if ctx.cap_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
            assert_eq!(
                self.auth_plugin_data_length as usize,
                self.auth_plugin_data_1.len() + self.auth_plugin_data_2.len()
            );
            elem4[8] = self.auth_plugin_data_length;
        }
        assert_eq!(self.auth_plugin_data_1.len(), AUTH_PLUGIN_DATA_LEN1);
        let auth_plugin_name = if ctx.cap_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
            MySerElem::null_end_str(&self.auth_plugin_name)
        } else {
            MySerElem::empty()
        };
        MySerPackets::new(
            ctx,
            [
                MySerElem::one_byte(self.protocol_version), // 1B protocol version
                MySerElem::null_end_str(&self.server_version), // null-end-str server version
                MySerElem::inline_bytes(&self.connection_id.to_le_bytes()), // 4B connection id
                MySerElem::inline_bytes(&self.auth_plugin_data_1), // 8B auth plugin data part 1
                MySerElem::inline_bytes(&elem4),
                MySerElem::filler(10),
                MySerElem::slice(&self.auth_plugin_data_2),
                auth_plugin_name,
            ],
        )
    }
}

impl<'a> MyDeser<'a> for InitialHandshake<'a> {
    #[inline]
    fn my_deser(_ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let protocol_version = input.try_deser_u8()?;
        let server_version = input.try_deser_until(0, false)?;
        let connection_id = input.try_deser_le_u32()?;
        let auth_plugin_data_1 = input.try_deser_bytes(8)?;
        input.try_advance(1)?;
        let capability_flags_lower = input.try_deser_le_u16()?;
        let charset = input.try_deser_u8()?;
        let status_flags = input.try_deser_le_u16()?;
        let capability_flags_upper = input.try_deser_le_u16()?;
        let auth_plugin_data_length = input.try_deser_u8()?;
        input.try_advance(10)?;
        // construct complete capability_flags
        let capability_flags =
            (capability_flags_lower as u32) | ((capability_flags_upper as u32) << 16);
        let cap_flags = CapabilityFlags::from_bits_truncate(capability_flags);
        let auth_plugin_data_2 = {
            // let len = std::cmp::max(13, auth_plugin_data_length - 8);
            let len = auth_plugin_data_length - 8;
            input.try_deser_bytes(len as usize)?
        };
        let auth_plugin_name = if cap_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
            input.try_deser_until(0, false)?
        } else {
            &[]
        };
        let res = InitialHandshake {
            protocol_version,
            server_version: Cow::Borrowed(server_version),
            connection_id,
            auth_plugin_data_1: Cow::Borrowed(auth_plugin_data_1),
            charset,
            status_flags,
            capability_flags,
            auth_plugin_data_length,
            auth_plugin_data_2: Cow::Borrowed(auth_plugin_data_2),
            auth_plugin_name: Cow::Borrowed(auth_plugin_name),
        };
        Ok((*input, res))
    }
}

impl_from_ref!(
    InitialHandshake:
    protocol_version, connection_id, charset, status_flags, capability_flags, auth_plugin_data_length;
    server_version, auth_plugin_data_1, auth_plugin_data_2, auth_plugin_name
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeSvrResp<'a> {
    Ok(OkPacket<'a>),
    Err(ErrPacket<'a>),
    Switch(AuthSwitchRequest<'a>),
    More(AuthMoreData<'a>),
}

impl<'a> MyDeser<'a> for HandshakeSvrResp<'a> {
    #[inline]
    fn my_deser(ctx: &mut SerdeCtx, input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        match input[0] {
            0x00 => {
                let (next, ok) = OkPacket::my_deser(ctx, input)?;
                if !next.is_empty() {
                    return Err(Error::MalformedPacket());
                }
                Ok((next, HandshakeSvrResp::Ok(ok)))
            }
            0x01 => {
                let (next, more) = AuthMoreData::my_deser(ctx, input)?;
                if !next.is_empty() {
                    return Err(Error::MalformedPacket());
                }
                Ok((next, HandshakeSvrResp::More(more)))
            }
            0xff => {
                let (next, err) = ErrPacket::my_deser(ctx, input)?;
                if !next.is_empty() {
                    return Err(Error::MalformedPacket());
                }
                Ok((next, HandshakeSvrResp::Err(err)))
            }
            0xfe => {
                let (next, switch) = AuthSwitchRequest::my_deser(ctx, input)?;
                if !next.is_empty() {
                    return Err(Error::MalformedPacket());
                }
                Ok((next, HandshakeSvrResp::Switch(switch)))
            }
            c => Err(Error::InvalidPacketHeader(c)),
        }
    }
}

/// handshake response of client protocol 41
///
/// reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html
/// this struct should be constructed by user and will be sent to
/// MySQL server to finish handshake process
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeCliResp41<'a> {
    pub cap_flags: CapabilityFlags,
    pub max_packet_size: u32,
    pub charset: u8,
    // 23 bytes of 0x00, reserved
    pub username: Cow<'a, [u8]>,
    // vary according to capability flags and auth setting
    pub auth_response: Cow<'a, [u8]>,
    // not empty if db is specified
    pub database: Cow<'a, [u8]>,
    // not empty if plugin auth
    pub auth_plugin_name: Cow<'a, [u8]>,
    // connect attributes
    pub connect_attrs: Vec<ConnectAttr<'a>>,
}

impl<'a> NewMySer for HandshakeCliResp41<'a> {
    type Ser<'s>
        = MySerPackets<'s, 7>
    where
        Self: 's;
    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        // combine cap_flags, max_packet_size and charset
        let mut bs = [0u8; 9];
        bs[..4].copy_from_slice(&self.cap_flags.bits().to_le_bytes());
        bs[4..8].copy_from_slice(&self.max_packet_size.to_le_bytes());
        bs[8] = self.charset;
        let cf_mps_cs = MySerElem::inline_bytes(&bs);
        let auth_response = if self
            .cap_flags
            .contains(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA)
        {
            MySerElem::len_enc_str(&*self.auth_response)
        } else {
            MySerElem::prefix_1b_str(&self.auth_response)
        };
        let database = if self.cap_flags.contains(CapabilityFlags::CONNECT_WITH_DB) {
            MySerElem::null_end_str(&self.database)
        } else {
            MySerElem::empty()
        };
        let auth_plugin_name = if self.cap_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
            MySerElem::null_end_str(&self.auth_plugin_name)
        } else {
            MySerElem::empty()
        };
        // connect attrs is nested structure, to simplify the serialization.
        // we create a temporary byte vector to store it.
        let connect_attrs = if self.cap_flags.contains(CapabilityFlags::CONNECT_ATTRS) {
            assert!(!self.connect_attrs.is_empty());
            let (lei, datalen) = calc_connect_attrs_my_len(&self.connect_attrs);
            let mut buf = vec![0; lei.len() + datalen];
            my_ser_connect_attrs(datalen, &self.connect_attrs, &mut buf).unwrap();
            MySerElem::buffer(buf)
        } else {
            assert!(self.connect_attrs.is_empty());
            MySerElem::empty()
        };
        // todo: 1-byte zstd compression level?
        MySerPackets::new(
            ctx,
            [
                cf_mps_cs,                               // cap flags, max packet size, and charset
                MySerElem::filler(23),                   // 23-byte filler
                MySerElem::null_end_str(&self.username), // null-end username
                auth_response,    // len-enc-str or prefix-1b-str auth response
                database,         // null-end database
                auth_plugin_name, // null-end auth plugin name
                connect_attrs,
            ],
        )
    }
}

impl<'a> MyDeser<'a> for HandshakeCliResp41<'a> {
    #[inline]
    fn my_deser(_ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let cap_flags = CapabilityFlags::from_bits_truncate(input.try_deser_le_u32()?);
        log::debug!("cap_flags={:?}", cap_flags);
        let max_packet_size = input.try_deser_le_u32()?;
        let charset = input.try_deser_u8()?;
        // 23B filler
        input.try_advance(23)?;
        let username = input.try_deser_until(0, false)?;
        let auth_response = if cap_flags.contains(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            input.try_deser_len_enc_str()?.try_into()?
        } else {
            input.try_deser_prefix_1b_str()?
        };
        let database = if cap_flags.contains(CapabilityFlags::CONNECT_WITH_DB) {
            input.try_deser_until(0, false)?
        } else {
            &[]
        };
        let auth_plugin_name = if cap_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
            input.try_deser_until(0, false)?
        } else {
            &[]
        };
        let connect_attrs =
            if input.is_empty() || !cap_flags.contains(CapabilityFlags::CONNECT_ATTRS) {
                vec![]
            } else {
                let len: u64 = input.try_deser_len_enc_int()?.try_into()?;
                if len as usize != input.len() {
                    return Err(Error::MalformedPacket());
                }
                let mut attrs = vec![];
                let mut key: &[u8];
                let mut val: &[u8];
                while !input.is_empty() {
                    key = input.try_deser_len_enc_str()?.try_into()?;
                    val = input.try_deser_len_enc_str()?.try_into()?;
                    attrs.push(ConnectAttr {
                        key: Cow::Borrowed(key),
                        value: Cow::Borrowed(val),
                    });
                }
                attrs
            };
        let res = HandshakeCliResp41 {
            cap_flags,
            max_packet_size,
            charset,
            username: Cow::Borrowed(username),
            auth_response: Cow::Borrowed(auth_response),
            database: Cow::Borrowed(database),
            auth_plugin_name: Cow::Borrowed(auth_plugin_name),
            connect_attrs,
        };
        Ok((*input, res))
    }
}

#[inline]
fn calc_connect_attrs_my_len(connect_attrs: &[ConnectAttr<'_>]) -> (LenEncInt, usize) {
    let len_of_kvs: usize = connect_attrs
        .iter()
        .map(|ca| ca.key.len_enc_str_len() + ca.value.len_enc_str_len())
        .sum();
    (LenEncInt::from(len_of_kvs as u64), len_of_kvs)
}

#[inline]
fn my_ser_connect_attrs(
    dlen: usize,
    connect_attrs: &[ConnectAttr<'_>],
    mut buf: &mut [u8],
) -> Result<()> {
    buf = buf.ser_len_enc_int(LenEncInt::from(dlen as u64));
    for attr in connect_attrs {
        buf = buf.ser_len_enc_str(LenEncStr::from(&*attr.key));
        buf = buf.ser_len_enc_str(LenEncStr::from(&*attr.value));
    }
    Ok(())
}

impl_from_ref!(
    HandshakeCliResp41: src:
    cap_flags, max_packet_size, charset;
    username, auth_response, database, auth_plugin_name;
    connect_attrs = src.connect_attrs.iter().map(ConnectAttr::from_ref).collect()
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectAttr<'a> {
    pub key: Cow<'a, [u8]>,
    pub value: Cow<'a, [u8]>,
}

impl_from_ref!(ConnectAttr: ; key, value);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthSwitchRequest<'a> {
    // must be 0xfe
    pub header: u8,
    // null terminated string
    pub plugin_name: Cow<'a, [u8]>,
    // EOF terminated string
    pub auth_plugin_data: Cow<'a, [u8]>,
}

impl<'a> NewMySer for AuthSwitchRequest<'a> {
    type Ser<'s>
        = MySerPackets<'s, 3>
    where
        Self: 's;
    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        assert_eq!(self.header, 0xfe);
        MySerPackets::new(
            ctx,
            [
                MySerElem::one_byte(self.header),
                MySerElem::null_end_str(&self.plugin_name),
                MySerElem::slice(&self.auth_plugin_data),
            ],
        )
    }
}

impl<'a> MyDeser<'a> for AuthSwitchRequest<'a> {
    #[inline]
    fn my_deser(_ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let header = input.try_deser_u8()?;
        if header != 0xfe {
            return Err(Error::InvalidPacketHeader(header));
        }
        let plugin_name = input.try_deser_until(0, false)?;
        let auth_plugin_data = input.deser_to_end();
        let res = AuthSwitchRequest {
            header,
            plugin_name: Cow::Borrowed(plugin_name),
            auth_plugin_data: Cow::Borrowed(auth_plugin_data),
        };
        Ok((*input, res))
    }
}

impl_from_ref!(AuthSwitchRequest: header; plugin_name, auth_plugin_data);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthMoreData<'a> {
    // must be 0x01
    pub header: u8,
    // EOF string
    pub plugin_data: Cow<'a, [u8]>,
}

impl<'a> NewMySer for AuthMoreData<'a> {
    type Ser<'s>
        = MySerPackets<'s, 2>
    where
        Self: 's;
    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        assert_eq!(self.header, 0x01);
        MySerPackets::new(
            ctx,
            [
                MySerElem::one_byte(self.header),
                MySerElem::slice(&self.plugin_data),
            ],
        )
    }
}

impl<'a> MyDeser<'a> for AuthMoreData<'a> {
    #[inline]
    fn my_deser(_ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let header = input.try_deser_u8()?;
        if header != 0x01 {
            return Err(Error::InvalidPacketHeader(header));
        }
        let plugin_data = input.deser_to_end();
        let res = AuthMoreData {
            header,
            plugin_data: Cow::Borrowed(plugin_data),
        };
        Ok((*input, res))
    }
}

impl_from_ref!(AuthMoreData: header; plugin_data);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::ByteBuffer;
    use crate::mysql::flag::StatusFlags;
    use crate::mysql::serde::tests::{check_deser, check_ser, check_ser_and_deser};

    #[test]
    fn test_serde_init_handshake() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let eof = InitialHandshake {
            protocol_version: 10,
            server_version: Cow::Borrowed(b"8.0.30"),
            connection_id: 1,
            auth_plugin_data_1: Cow::Borrowed(b"12345678"),
            charset: 255,
            status_flags: 16,
            capability_flags: CapabilityFlags::default().bits(),
            auth_plugin_data_length: 16,
            auth_plugin_data_2: Cow::Borrowed(b"45678901"),
            auth_plugin_name: Cow::Borrowed(b"mysql_native_password"),
        };
        check_ser_and_deser(&mut ctx, &eof, &buf);
    }

    #[test]
    fn test_serde_handshake_cli_resp() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let mut resp = HandshakeCliResp41 {
            cap_flags: CapabilityFlags::default(),
            max_packet_size: 0xffffff,
            charset: 255,
            username: Cow::Borrowed(b"root"),
            auth_response: Cow::Borrowed(b"12345678"),
            database: Cow::Borrowed(b""),
            auth_plugin_name: Cow::Borrowed(b"mysql_native_password"),
            connect_attrs: vec![],
        };
        check_ser_and_deser(&mut ctx, &resp, &buf);

        ctx.cap_flags
            .remove(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA);
        resp.cap_flags
            .remove(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA);
        check_ser_and_deser(&mut ctx, &resp, &buf);
    }

    #[test]
    fn test_serde_handshake_connect_attrs() {
        let mut ctx = SerdeCtx::default();
        ctx.cap_flags.insert(CapabilityFlags::CONNECT_ATTRS);
        let buf = ByteBuffer::with_capacity(1024);
        let resp = HandshakeCliResp41 {
            cap_flags: ctx.cap_flags,
            max_packet_size: 0xffffff,
            charset: 255,
            username: Cow::Borrowed(b"root"),
            auth_response: Cow::Borrowed(b"12345678"),
            database: Cow::Borrowed(b""),
            auth_plugin_name: Cow::Borrowed(b"mysql_native_password"),
            connect_attrs: vec![ConnectAttr {
                key: Cow::Borrowed(b"useServerPrepStmt"),
                value: Cow::Borrowed(b"true"),
            }],
        };
        check_ser_and_deser(&mut ctx, &resp, &buf);
    }

    #[test]
    fn test_serde_handshake_srv_resp() {
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
        check_ser(&mut ctx, &ok, &buf);
        let r2: HandshakeSvrResp = check_deser(&mut ctx, &buf);
        assert_eq!(r2, HandshakeSvrResp::Ok(ok));

        let err = ErrPacket {
            code: 1304,
            state: *b"01S01",
            msg: Cow::Borrowed(b"internal error"),
        };
        check_ser(&mut ctx, &err, &buf);
        let r3: HandshakeSvrResp = check_deser(&mut ctx, &buf);
        assert_eq!(r3, HandshakeSvrResp::Err(err));

        let switch = AuthSwitchRequest {
            header: 0xfe,
            plugin_name: Cow::Borrowed(b"mysql_native_password"),
            auth_plugin_data: Cow::Borrowed(b"12345678"),
        };
        check_ser(&mut ctx, &switch, &buf);
        let r4: HandshakeSvrResp = check_deser(&mut ctx, &buf);
        assert_eq!(r4, HandshakeSvrResp::Switch(switch));

        let more = AuthMoreData {
            header: 0x01,
            plugin_data: Cow::Borrowed(b"12345678"),
        };
        check_ser(&mut ctx, &more, &buf);
        let r4: HandshakeSvrResp = check_deser(&mut ctx, &buf);
        assert_eq!(r4, HandshakeSvrResp::More(more));
    }
}
