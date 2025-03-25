//! Defines MySQL commands.
mod query;

use crate::mysql::error::{Error, Result};
use crate::mysql::serde::{MyDeser, MyDeserExt, MySerElem, MySerPackets, NewMySer, SerdeCtx};
pub use query::ComQuery;
use std::borrow::{Borrow, Cow};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CmdCode {
    Sleep = 0x00,
    Quit = 0x01,
    InitDB = 0x02,
    Query = 0x03,
    FieldList = 0x04,
    CreateDB = 0x05,
    DropDB = 0x06,
    Refresh = 0x07,
    Shutdown = 0x08,
    Statistics = 0x09,
    ProcessInfo = 0x0a,
    Connect = 0x0b,
    ProcessKill = 0x0c,
    Debug = 0x0d,
    Ping = 0x0e,
    Time = 0x0f,
    DelayedInsert = 0x10,
    ChangeUser = 0x11,
    BinlogDump = 0x12,
    TableDump = 0x13,
    ConnectOut = 0x14,
    RegisterSlave = 0x15,
    StmtPrepare = 0x16,
    StmtExecute = 0x17,
    StmtSendLongData = 0x18,
    StmtClose = 0x19,
    StmtReset = 0x1a,
    SetOption = 0x1b,
    StmtFetch = 0x1c,
    Daemon = 0x1d,
    BinlogDumpGtid = 0x1e,
    ResetConnection = 0x1f,
}

impl TryFrom<u8> for CmdCode {
    type Error = Error;
    fn try_from(src: u8) -> Result<Self> {
        let cmd = match src {
            0x00 => CmdCode::Sleep,
            0x01 => CmdCode::Quit,
            0x02 => CmdCode::InitDB,
            0x03 => CmdCode::Query,
            0x04 => CmdCode::FieldList,
            0x05 => CmdCode::CreateDB,
            0x06 => CmdCode::DropDB,
            0x07 => CmdCode::Refresh,
            0x08 => CmdCode::Shutdown,
            0x09 => CmdCode::Statistics,
            0x0a => CmdCode::ProcessInfo,
            0x0b => CmdCode::Connect,
            0x0c => CmdCode::ProcessKill,
            0x0d => CmdCode::Debug,
            0x0e => CmdCode::Ping,
            0x0f => CmdCode::Time,
            0x10 => CmdCode::DelayedInsert,
            0x11 => CmdCode::ChangeUser,
            0x12 => CmdCode::BinlogDump,
            0x13 => CmdCode::TableDump,
            0x14 => CmdCode::ConnectOut,
            0x15 => CmdCode::RegisterSlave,
            0x16 => CmdCode::StmtPrepare,
            0x17 => CmdCode::StmtExecute,
            0x18 => CmdCode::StmtSendLongData,
            0x19 => CmdCode::StmtClose,
            0x1a => CmdCode::StmtReset,
            0x1b => CmdCode::SetOption,
            0x1c => CmdCode::StmtFetch,
            0x1d => CmdCode::Daemon,
            0x1e => CmdCode::BinlogDumpGtid,
            0x1f => CmdCode::ResetConnection,
            _ => return Err(Error::InvalidCommandCode(src)),
        };
        Ok(cmd)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MyCmd<'a> {
    Query(ComQuery<'a>),
    FieldList(ComFieldList<'a>),
}

impl<'a> MyDeser<'a> for MyCmd<'a> {
    #[inline]
    fn my_deser(ctx: &mut SerdeCtx, input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        if input.is_empty() {
            return Err(Error::MalformedPacket());
        }
        match input[0] {
            0x03 => {
                let (next, query) = ComQuery::my_deser(ctx, input)?;
                Ok((next, MyCmd::Query(query)))
            }
            0x04 => {
                let (next, field_list) = ComFieldList::my_deser(ctx, input)?;
                Ok((next, MyCmd::FieldList(field_list)))
            }
            _ => Err(Error::MalformedPacket()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComFieldList<'a> {
    pub table: Cow<'a, str>,
    pub wildcard: Cow<'a, str>,
}

impl<'a> ComFieldList<'a> {
    #[inline]
    pub fn code(&self) -> CmdCode {
        CmdCode::FieldList
    }

    #[inline]
    pub fn new(table: impl Into<String>, wildcard: impl Into<String>) -> Self {
        ComFieldList {
            table: Cow::Owned(table.into()),
            wildcard: Cow::Owned(wildcard.into()),
        }
    }

    #[inline]
    pub fn new_ref<T, U>(table: &'a T, wildcard: &'a U) -> Self
    where
        T: Borrow<str> + ?Sized,
        U: Borrow<str> + ?Sized,
    {
        ComFieldList {
            table: Cow::Borrowed(table.borrow()),
            wildcard: Cow::Borrowed(wildcard.borrow()),
        }
    }
}

impl<'a> NewMySer for ComFieldList<'a> {
    type Ser<'s>
        = MySerPackets<'s, 3>
    where
        Self: 's;
    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        MySerPackets::new(
            ctx,
            [
                MySerElem::one_byte(CmdCode::FieldList as u8),
                MySerElem::null_end_str(self.table.as_bytes()),
                MySerElem::slice(self.wildcard.as_bytes()),
            ],
        )
    }
}

impl<'a> MyDeser<'a> for ComFieldList<'a> {
    #[inline]
    fn my_deser(_ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let code: CmdCode = input.try_deser_u8()?.try_into()?;
        if code != CmdCode::FieldList {
            return Err(Error::InvalidCommandCode(code as u8));
        }
        let table = input.try_deser_until(0, false)?;
        let wildcard = input.deser_to_end();
        let table = std::str::from_utf8(table).map_err(|_| Error::InvalidUtf8String())?;
        let wildcard = std::str::from_utf8(wildcard).map_err(|_| Error::InvalidUtf8String())?;
        Ok((
            *input,
            ComFieldList {
                table: Cow::Borrowed(table),
                wildcard: Cow::Borrowed(wildcard),
            },
        ))
    }
}

impl_from_ref!(ComFieldList: ; table, wildcard);

impl<'a> From<(&'a str, &'a str)> for ComFieldList<'a> {
    #[inline]
    fn from((table, wildcard): (&'a str, &'a str)) -> Self {
        ComFieldList::new_ref(table, wildcard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::ByteBuffer;
    use crate::mysql::serde::tests::check_ser_and_deser;

    #[test]
    fn test_serde_com_field_list() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let cmd = ComFieldList::new("t1", "%");
        check_ser_and_deser(&mut ctx, &cmd, &buf);
    }
}
