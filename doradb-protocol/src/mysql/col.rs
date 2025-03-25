//! defines structure and metadata for mysql columns
use crate::mysql::cmd::CmdCode;
use crate::mysql::error::{Error, Result};
use crate::mysql::serde::{
    LenEncStr, MyDeser, MyDeserExt, MySerElem, MySerPackets, NewMySer, SerdeCtx,
};
use bitflags::bitflags;
use semistr::SemiStr;
use std::convert::TryFrom;
use std::sync::Arc;

/// ColumnType defined in binlog
///
/// the complete types listed in
/// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/export/binary_log_types.h
///
/// several types are missing in binlog, refer to:
/// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/rows_event.h#L174
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Decimal = 0x00,
    Tiny = 0x01,
    Short = 0x02,
    Long = 0x03,
    Float = 0x04,
    Double = 0x05,
    Null = 0x06,
    Timestamp = 0x07,
    LongLong = 0x08,
    Int24 = 0x09,
    Date = 0x0a,
    Time = 0x0b,
    DateTime = 0x0c,
    Year = 0x0d,
    // NewDate = 0x0e,
    Varchar = 0x0f,
    Bit = 0x10,
    Timestamp2 = 0x11,
    DateTime2 = 0x12,
    Time2 = 0x13,
    // Json = 0xf5,
    NewDecimal = 0xf6,
    // Enum = 0xf7,
    // Set = 0xf8,
    TinyBlob = 0xf9,
    MediumBlob = 0xfa,
    LongBlob = 0xfb,
    Blob = 0xfc,
    VarString = 0xfd,
    String = 0xfe,
    Geometry = 0xff,
}

impl TryFrom<u8> for ColumnType {
    type Error = Error;

    fn try_from(code: u8) -> Result<Self> {
        let ct = match code {
            0x00 => ColumnType::Decimal,
            0x01 => ColumnType::Tiny,
            0x02 => ColumnType::Short,
            0x03 => ColumnType::Long,
            0x04 => ColumnType::Float,
            0x05 => ColumnType::Double,
            0x06 => ColumnType::Null,
            0x07 => ColumnType::Timestamp,
            0x08 => ColumnType::LongLong,
            0x09 => ColumnType::Int24,
            0x0a => ColumnType::Date,
            0x0b => ColumnType::Time,
            0x0c => ColumnType::DateTime,
            0x0d => ColumnType::Year,
            // 0x0e => ColumnType::NewDate,
            0x0f => ColumnType::Varchar,
            0x10 => ColumnType::Bit,
            0x11 => ColumnType::Timestamp2,
            0x12 => ColumnType::DateTime2,
            0x13 => ColumnType::Time2,
            // 0xf5 => ColumnType::Json,
            0xf6 => ColumnType::NewDecimal,
            // 0xf7 => ColumnType::Enum,
            // 0xf8 => ColumnType::Set,
            0xf9 => ColumnType::TinyBlob,
            0xfa => ColumnType::MediumBlob,
            0xfb => ColumnType::LongBlob,
            0xfc => ColumnType::Blob,
            0xfd => ColumnType::VarString,
            0xfe => ColumnType::String,
            0xff => ColumnType::Geometry,
            _ => return Err(Error::InvalidColumnType()),
        };
        Ok(ct)
    }
}

/// Column definition.
/// Old format(ColumnDefinition320) is not supported.
/// Column definition has 7 len-enc-str fields.
/// The 7th field has fixed length(12 bytes), and is composite of several flags.
/// An optional default value field may be appended at end if the command is COM_FIELD_LIST.
///
/// reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    // len-enc-str
    pub catalog: SemiStr,
    // len-enc-str
    pub schema: SemiStr,
    // len-enc-str
    pub table: SemiStr,
    // len-enc-str
    pub org_table: SemiStr,
    // len-enc-str
    pub name: SemiStr,
    // len-enc-str
    pub org_name: SemiStr,
    // len-enc-int, always 0x0c
    pub charset: u16,
    pub col_len: u32,
    pub col_type: ColumnType,
    pub flags: ColumnFlags,
    // 0x00, 0x1f, 0x00-0x51
    pub decimals: u8,
    pub default_value: Option<SemiStr>,
}

impl ColumnDefinition {
    pub fn unsigned(&self) -> bool {
        self.flags.contains(ColumnFlags::UNSIGNED)
    }
}

impl NewMySer for ColumnDefinition {
    type Ser<'s>
        = MySerPackets<'s, 8>
    where
        Self: 's;

    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        // combine charset, col_len, col_type, flags, decimals to one element.
        // the reserved byte is actually the length of this element.
        let mut bs = [0u8; 13];
        bs[0] = 0x0c; // length of following bytes.
        bs[1..3].copy_from_slice(&self.charset.to_le_bytes()); // 2B charset
        bs[3..7].copy_from_slice(&self.col_len.to_le_bytes()); // 4B col_len
        bs[7] = self.col_type as u8; // 1B col_type
        bs[8..10].copy_from_slice(&self.flags.bits().to_le_bytes()); // 2B flags
        bs[10] = self.decimals;
        // two bytes reserved.
        let elem6 = MySerElem::inline_bytes(&bs);
        let default_value = if let Some(CmdCode::FieldList) = ctx.curr_cmd.as_ref() {
            self.default_value
                .as_ref()
                .map(|s| MySerElem::len_enc_str(LenEncStr::from(&**s)))
                .unwrap_or_else(|| MySerElem::len_enc_str(LenEncStr::Null))
        } else {
            MySerElem::empty()
        };
        MySerPackets::new(
            ctx,
            [
                MySerElem::len_enc_str(&*self.catalog),
                MySerElem::len_enc_str(&*self.schema),
                MySerElem::len_enc_str(&*self.table),
                MySerElem::len_enc_str(&*self.org_table),
                MySerElem::len_enc_str(&*self.name),
                MySerElem::len_enc_str(&*self.org_name),
                elem6,
                default_value,
            ],
        )
    }
}

impl<'a> MyDeser<'a> for ColumnDefinition {
    #[inline]
    fn my_deser(ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let catalog: SemiStr = input.try_deser_len_enc_str()?.try_into()?;
        let schema: SemiStr = input.try_deser_len_enc_str()?.try_into()?;
        let table: SemiStr = input.try_deser_len_enc_str()?.try_into()?;
        let org_table: SemiStr = input.try_deser_len_enc_str()?.try_into()?;
        let name: SemiStr = input.try_deser_len_enc_str()?.try_into()?;
        let org_name: SemiStr = input.try_deser_len_enc_str()?.try_into()?;
        // reserved len-enc-int, but always be 0x0c.
        // so it's safe to just read 1-byte.
        let reserved = input.try_deser_u8()?;
        debug_assert_eq!(reserved, 0x0c, "mismatch reserved byte");
        let charset = input.try_deser_le_u16()?;
        let col_len = input.try_deser_le_u32()?;
        let col_type = input.try_deser_u8()?;
        let col_type = ColumnType::try_from(col_type)?;
        let flags = input.try_deser_le_u16()?;
        let flags = ColumnFlags::from_bits(flags).ok_or(Error::MalformedPacket())?;
        let decimals = input.try_deser_u8()?;
        input.try_advance(2)?;
        let default_value = if let Some(CmdCode::FieldList) = ctx.curr_cmd.as_ref() {
            match input.try_deser_len_enc_str()? {
                LenEncStr::Null => None,
                LenEncStr::Err => return Err(Error::MalformedPacket()),
                LenEncStr::Bytes(b) => {
                    Some(SemiStr::try_from(b).map_err(|_| Error::WrongStringLength())?)
                }
            }
        } else {
            None
        };
        let res = ColumnDefinition {
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            charset,
            col_len,
            col_type,
            flags,
            decimals,
            default_value,
        };
        Ok((*input, res))
    }
}

pub type ColumnDefinitions = Vec<Arc<ColumnDefinition>>;

bitflags! {
    /// flags of column
    ///
    /// the actual column flags is u32, but truncate to u16 to send to client
    ///
    /// references:
    /// https://github.com/mysql/mysql-server/blob/5.7/sql/field.h#L4504
    /// https://github.com/mysql/mysql-server/blob/5.7/sql/protocol_classic.cc#L1163
    pub struct ColumnFlags: u16 {
        const NOT_NULL      = 0x0001;
        const PRIMARY_KEY   = 0x0002;
        const UNIQUE_KEY    = 0x0004;
        const MULTIPLE_KEY  = 0x0008;
        const BLOB          = 0x0010;
        const UNSIGNED      = 0x0020;
        const ZEROFILL      = 0x0040;
        const BINARY        = 0x0080;
        const ENUM          = 0x0100;
        const AUTO_INCREMENT    = 0x0200;
        const TIMESTAMP     = 0x0400;
        const SET           = 0x0800;
        const NO_DEFAULT_VALUE  = 0x1000;
        const ON_UPDATE_NOW = 0x2000;
        const NUM           = 0x4000;
        const PART_KEY      = 0x8000;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::ByteBuffer;
    use crate::mysql::serde::tests::check_ser_and_deser;

    #[test]
    fn test_serde_col_def() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let cd = ColumnDefinition {
            catalog: SemiStr::inline("def"),
            schema: SemiStr::inline("db1"),
            table: SemiStr::inline("bb1"),
            org_table: SemiStr::inline("bb1"),
            name: SemiStr::inline("c0"),
            org_name: SemiStr::inline("c0"),
            charset: 63,
            col_len: 64,
            col_type: ColumnType::Bit,
            flags: ColumnFlags::UNSIGNED,
            decimals: 0,
            default_value: None,
        };
        check_ser_and_deser(&mut ctx, &cd, &buf);
    }
}
