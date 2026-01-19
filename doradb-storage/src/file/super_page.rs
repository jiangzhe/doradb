use crate::buffer::page::PageID;
use crate::error::Result;
use crate::file::table_file::TABLE_FILE_SUPER_PAGE_FOOTER_SIZE;
use crate::serde::{Deser, Ser, Serde};
use crate::trx::TrxID;
use std::mem;

pub const SUPER_PAGE_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperPageHeader {
    /// Magic word of table file, by default 'DORA\0\0\0\0'
    pub magic_word: [u8; 8],
    /// Version of super page format.
    pub version: u64,
    /// Page number of this super page.
    pub page_no: PageID,
    /// checkpoint timestamp of this super page.
    pub checkpoint_cts: TrxID,
}

impl Ser<'_> for SuperPageHeader {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<[u8; 8]>() // magic word
            + mem::size_of::<u64>() // version
            + mem::size_of::<PageID>() // page no
            + mem::size_of::<TrxID>() // checkpoint timestamp
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.magic_word);
        let idx = out.ser_u64(idx, self.version);
        let idx = out.ser_u64(idx, self.page_no);
        out.ser_u64(idx, self.checkpoint_cts)
    }
}

impl Deser for SuperPageHeader {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, magic_word) = input.deser_byte_array::<8>(start_idx)?;
        let (idx, version) = input.deser_u64(idx)?;
        let (idx, page_no) = input.deser_u64(idx)?;
        let (idx, checkpoint_cts) = input.deser_u64(idx)?;
        let res = SuperPageHeader {
            magic_word,
            version,
            page_no,
            checkpoint_cts,
        };
        Ok((idx, res))
    }
}

#[derive(PartialEq, Eq)]
pub struct SuperPageBody {
    pub meta_page_id: PageID,
}

impl Ser<'_> for SuperPageBody {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<PageID>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u64(start_idx, self.meta_page_id)
    }
}

impl Deser for SuperPageBody {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, meta_page_id) = input.deser_u64(start_idx)?;
        Ok((idx, SuperPageBody { meta_page_id }))
    }
}

#[derive(Default, PartialEq, Eq)]
pub struct SuperPageFooter {
    pub b3sum: [u8; 32],
    pub checkpoint_cts: TrxID,
}

impl Deser for SuperPageFooter {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, b3sum) = input.deser_byte_array::<32>(start_idx)?;
        let (idx, checkpoint_cts) = input.deser_u64(idx)?;
        Ok((
            idx,
            SuperPageFooter {
                b3sum,
                checkpoint_cts,
            },
        ))
    }
}

impl Ser<'_> for SuperPageFooter {
    #[inline]
    fn ser_len(&self) -> usize {
        TABLE_FILE_SUPER_PAGE_FOOTER_SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.b3sum);
        out.ser_u64(idx, self.checkpoint_cts)
    }
}

#[derive(PartialEq, Eq)]
pub struct SuperPage {
    pub header: SuperPageHeader,
    pub body: SuperPageBody,
    pub footer: SuperPageFooter,
}

pub struct SuperPageSerView {
    pub header: SuperPageHeader,
    pub body: SuperPageBody,
}

impl Ser<'_> for SuperPageSerView {
    #[inline]
    fn ser_len(&self) -> usize {
        self.header.ser_len() + self.body.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = self.header.ser(out, start_idx);
        self.body.ser(out, idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::table_file::TABLE_FILE_MAGIC_WORD;

    #[test]
    fn test_super_page_serde() {
        let header = SuperPageHeader {
            magic_word: TABLE_FILE_MAGIC_WORD,
            version: SUPER_PAGE_VERSION,
            page_no: 1,
            checkpoint_cts: 12,
        };
        let body = SuperPageBody { meta_page_id: 7 };
        let ser_view = SuperPageSerView {
            header: header.clone(),
            body,
        };
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (idx, deser_header) = SuperPageHeader::deser(&data[..], 0).unwrap();
        let (_, deser_body) = SuperPageBody::deser(&data[..], idx).unwrap();
        assert_eq!(deser_header, header);
        assert_eq!(deser_body.meta_page_id, 7);
    }
}
