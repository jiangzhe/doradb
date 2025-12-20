//! Defines MySQL commands.
use crate::mysql::cmd::CmdCode;
use crate::mysql::error::{Error, Result};
use crate::mysql::serde::{MyDeser, MyDeserExt, MySerElem, MySerPackets, NewMySer, SerdeCtx};
use std::borrow::{Borrow, Cow, ToOwned};

/// Query Command with text format.
/// See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComQuery<'a> {
    pub query: Cow<'a, str>,
}

impl<'a> ComQuery<'a> {
    #[inline]
    pub fn code(&self) -> CmdCode {
        CmdCode::Query
    }

    pub fn new(query: impl Into<String>) -> Self {
        ComQuery {
            query: Cow::Owned(query.into()),
        }
    }

    pub fn new_ref<T>(query: &'a T) -> Self
    where
        T: Borrow<str> + ?Sized,
    {
        ComQuery {
            query: Cow::Borrowed(query.borrow()),
        }
    }
}

impl_from_ref!(ComQuery: ; query);

impl NewMySer for ComQuery<'_> {
    type Ser<'s>
        = MySerPackets<'s, 2>
    where
        Self: 's;

    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        MySerPackets::new(
            ctx,
            [
                MySerElem::one_byte(self.code() as u8),
                MySerElem::slice(self.query.as_bytes()),
            ],
        )
    }
}

impl<'a> MyDeser<'a> for ComQuery<'a> {
    fn my_deser(_ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let code: CmdCode = input.try_deser_u8()?.try_into()?;
        if code != CmdCode::Query {
            return Err(Error::MalformedPacket());
        }
        let query = input.deser_to_end();
        let query = std::str::from_utf8(query).map_err(|_| Error::InvalidUtf8String())?;
        let res = ComQuery {
            query: Cow::Borrowed(query),
        };
        Ok((*input, res))
    }
}

impl<'a> From<&'a str> for ComQuery<'a> {
    #[inline]
    fn from(src: &'a str) -> ComQuery<'a> {
        ComQuery::new_ref(src)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::ByteBuffer;
    use crate::mysql::serde::tests::check_ser_and_deser;

    #[test]
    fn test_serde_com_query() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let cmd = ComQuery::new("select 1");
        check_ser_and_deser(&mut ctx, &cmd, &buf);
    }
}
