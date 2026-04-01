use crate::buffer::page::PAGE_SIZE;
use crate::error::{Error, Result};
use crate::file::BlockID;
use crate::serde::{Deser, Ser, Serde};
use crate::trx::TrxID;
use std::mem;

pub const SUPER_PAGE_VERSION: u64 = 1;
pub const SUPER_PAGE_SIZE: usize = PAGE_SIZE / 2;
pub const SUPER_PAGE_FOOTER_SIZE: usize = mem::size_of::<SuperPageFooter>();
pub const SUPER_PAGE_FOOTER_OFFSET: usize = SUPER_PAGE_SIZE - SUPER_PAGE_FOOTER_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperPageHeader {
    /// Magic word of table file, by default 'DORA\0\0\0\0'
    pub magic_word: [u8; 8],
    /// Version of super page format.
    pub version: u64,
    /// Slot number of this super page.
    pub slot_no: u64,
    /// checkpoint timestamp of this super page.
    pub checkpoint_cts: TrxID,
}

impl Ser<'_> for SuperPageHeader {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<[u8; 8]>() // magic word
            + mem::size_of::<u64>() // version
            + mem::size_of::<u64>() // slot no
            + mem::size_of::<TrxID>() // checkpoint timestamp
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.magic_word);
        let idx = out.ser_u64(idx, self.version);
        let idx = out.ser_u64(idx, self.slot_no);
        out.ser_u64(idx, self.checkpoint_cts)
    }
}

impl Deser for SuperPageHeader {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, magic_word) = input.deser_byte_array::<8>(start_idx)?;
        let (idx, version) = input.deser_u64(idx)?;
        let (idx, slot_no) = input.deser_u64(idx)?;
        let (idx, checkpoint_cts) = input.deser_u64(idx)?;
        let res = SuperPageHeader {
            magic_word,
            version,
            slot_no,
            checkpoint_cts,
        };
        Ok((idx, res))
    }
}

#[derive(PartialEq, Eq)]
pub struct SuperPageBody {
    pub meta_block_id: BlockID,
}

impl Ser<'_> for SuperPageBody {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<BlockID>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u64(start_idx, self.meta_block_id.into())
    }
}

impl Deser for SuperPageBody {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, meta_block_id) = input.deser_u64(start_idx)?;
        Ok((
            idx,
            SuperPageBody {
                meta_block_id: BlockID::from(meta_block_id),
            },
        ))
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
        SUPER_PAGE_FOOTER_SIZE
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

#[inline]
pub fn parse_super_page(
    buf: &[u8],
    expected_magic_word: [u8; 8],
    expected_version: u64,
) -> Result<SuperPage> {
    let (body_start, header) = SuperPageHeader::deser(buf, 0)?;
    if header.magic_word != expected_magic_word {
        return Err(Error::InvalidFormat);
    }
    if header.version != expected_version {
        return Err(Error::InvalidFormat);
    }
    let (_, footer) = SuperPageFooter::deser(buf, SUPER_PAGE_FOOTER_OFFSET)?;
    if header.checkpoint_cts != footer.checkpoint_cts {
        return Err(Error::TornWrite);
    }
    let b3sum = blake3::hash(&buf[..SUPER_PAGE_FOOTER_OFFSET]);
    if b3sum != footer.b3sum {
        return Err(Error::ChecksumMismatch);
    }
    let (_, body) = SuperPageBody::deser(buf, body_start)?;
    Ok(SuperPage {
        header,
        body,
        footer,
    })
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
            slot_no: 1,
            checkpoint_cts: 12,
        };
        let body = SuperPageBody {
            meta_block_id: BlockID::from(7),
        };
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
        assert_eq!(deser_body.meta_block_id, 7);
    }
}
