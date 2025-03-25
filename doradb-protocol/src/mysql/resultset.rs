use crate::mysql::error::{Error, Result};
use crate::mysql::serde::{
    deser_le_u24, LenEncStr, MyDeserExt, MySer, MySerElem, MySerExt, MySerPackets, NewMySer,
    SerdeCtx,
};
use crate::mysql::value::Value;
use std::ops::{Deref, DerefMut};

pub struct TextRow<'a> {
    n_cols: usize,
    data: &'a [u8],
}

impl<'a> TextRow<'a> {
    /// Wrap a byte slice and construct a text row.
    /// Caller must make sure the row format is valid.
    #[inline]
    pub fn wrap(n_cols: usize, data: &'a [u8]) -> Self {
        TextRow { n_cols, data }
    }

    /// Convert text row to multiple values.
    #[inline]
    pub fn as_values(&self) -> Result<Vec<Option<&[u8]>>> {
        let mut vals = Vec::with_capacity(self.n_cols);
        let input = &mut &*self.data;
        for _ in 0..self.n_cols {
            match input.deser_len_enc_str() {
                LenEncStr::Null => vals.push(None),
                LenEncStr::Bytes(bs) => vals.push(Some(bs)),
                LenEncStr::Err => return Err(Error::MalformedPacket()),
            }
        }
        Ok(vals)
    }
}

impl<'a> TryFrom<&'a [u8]> for TextRow<'a> {
    type Error = Error;
    #[inline]
    fn try_from(data: &'a [u8]) -> Result<Self> {
        let n_cols = text_n_cols(data)?;
        Ok(TextRow { n_cols, data })
    }
}

impl<'a> NewMySer for TextRow<'a> {
    type Ser<'s>
        = MySerPackets<'s, 1>
    where
        Self: 's;

    #[inline]
    fn new_my_ser(&self, ctx: &SerdeCtx) -> Self::Ser<'_> {
        MySerPackets::new(ctx, [MySerElem::slice(self.data)])
    }
}

#[derive(Default)]
pub struct TextRows {
    pub start_pkt_nr: u8,
    pub end_pkt_nr: u8,
    data: Vec<u8>,
}

impl Deref for TextRows {
    type Target = Vec<u8>;
    #[inline]
    fn deref(&self) -> &Vec<u8> {
        &self.data
    }
}

impl DerefMut for TextRows {
    #[inline]
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}

impl TextRows {
    #[inline]
    pub fn start_row<'a, 'c>(&'a mut self, ctx: &'c SerdeCtx) -> RowBuilder<'a, 'c> {
        // each row is a packet but we do not know its length ahead,
        // so we setup 4-byte placeholder before add any value.
        let start_idx = self.data.len();
        self.data.extend([0u8; 4]);
        RowBuilder {
            ctx,
            start_idx,
            curr_bytes: 0,
            data: self,
        }
    }

    /// adjust packet number in row packets.
    /// e.g. If previous packet number starts from 4
    /// and we need to adjust it to start from 6.
    /// we will traverse all packets and add 2 to each packet number.
    #[inline]
    pub fn adjust_pkt_nr(&mut self, start_pkt_nr: u8) {
        if self.start_pkt_nr == start_pkt_nr {
            return;
        }
        let delta = start_pkt_nr.wrapping_sub(self.start_pkt_nr);
        let mut data = &mut self.data[..];
        while !data.is_empty() {
            let payload_len = deser_le_u24(data) as usize;
            data[3] = data[3].wrapping_add(delta);
            data = &mut data[payload_len + 4..];
        }
    }
}

#[must_use = "RowBuilder must be used"]
pub struct RowBuilder<'a, 'c> {
    ctx: &'c SerdeCtx,
    start_idx: usize,
    curr_bytes: usize,
    data: &'a mut TextRows,
}

impl RowBuilder<'_, '_> {
    #[inline]
    pub fn add_value(&mut self, _value: &Value) {
        todo!()
    }

    #[inline]
    pub fn add_text_value(&mut self, value: &[u8]) {
        let mut elem = MySerElem::len_enc_str(value);
        let mut len = elem.my_len(self.ctx);
        // check if we need to split packets
        while self.curr_bytes + len >= self.ctx.max_payload_size {
            // exceeds max payload size, split with additional packet header
            let partial_len = self.ctx.max_payload_size - self.curr_bytes;
            let start_idx = self.data.len();
            self.data.reserve(partial_len + 4); // with additional 4-byte header
            unsafe {
                self.data.set_len(start_idx + partial_len);
            }
            elem.my_ser_partial(self.ctx, self.data, start_idx);
            len -= partial_len;
            unsafe {
                // fill current packet header
                self.data[self.start_idx..].ser_le_u24(self.ctx.max_payload_size as u32);
                self.data[self.start_idx + 3] = self.data.end_pkt_nr;
                // extend next packet header
                self.data.set_len(start_idx + partial_len + 4);
            }
            // raise start index to next packet
            self.start_idx += self.ctx.max_payload_size;
            self.data.end_pkt_nr = self.data.end_pkt_nr.wrapping_add(1);
            self.curr_bytes = 0;
        }
        let start_idx = self.data.len();
        self.data.reserve(len);
        unsafe {
            self.data.set_len(start_idx + len);
        }
        elem.my_ser(self.ctx, self.data, start_idx);
        self.curr_bytes += len;
    }

    #[inline]
    pub fn finish(self) -> u8 {
        self.data[self.start_idx..].ser_le_u24(self.curr_bytes as u32);
        self.data[self.start_idx + 3] = self.data.end_pkt_nr;
        self.data.end_pkt_nr.wrapping_add(1)
    }
}

#[inline]
pub fn text_n_cols(mut row: &[u8]) -> Result<usize> {
    let input = &mut row;
    let mut n_col = 0;
    while !input.is_empty() {
        let _ = input.try_deser_len_enc_str()?;
        n_col += 1;
    }
    Ok(n_col)
}

// #[inline]
// pub fn mock_text_row_res(ctx: &SerdeCtx) -> TextRowResult {
//     let col_defs = vec![
//         Arc::new(
//             ColumnDefinition{
//                 catalog: SemiStr::new("def"),
//                 schema: SemiStr::default(),
//                 table: SemiStr::default(),
//                 org_table: SemiStr::default(),
//                 name : SemiStr::new("1"),
//                 org_name: SemiStr::new("1"),
//                 charset: 63,
//                 col_len: 2,
//                 col_type: ColumnType::LongLong,
//                 flags: ColumnFlags::NOT_NULL | ColumnFlags::BINARY | ColumnFlags::NUM,
//                 decimals: 0,
//                 default_value: None,
//             }
//         )
//     ];

//     let mut rows = TextRows::default();
//     let mut row = rows.start_row(ctx);
//     row.add_text_value(b"1");
//     row.finish();
//     TextRowResult{
//         col_defs,
//         affected_rows: 1,
//         last_insert_id: 0,
//         status_flags: StatusFlags::AUTOCOMMIT,
//         warnings: 0,
//         info: String::new(),
//         rows,
//         next: None,
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_builder() {
        let ctx = SerdeCtx::default();
        let mut rows = TextRows::default();
        let mut row = rows.start_row(&ctx);
        row.add_text_value(b"1");
        row.finish();
        assert_eq!(&rows.data, &[2u8, 0, 0, 0, 1, 49]);
    }
}
