use crate::buffer::page::PAGE_SIZE;
use crate::error::{DataIntegrityError, Error, Result};
use crate::file::BlockID;
use crate::serde::{Deser, Ser, Serde};
use crate::trx::TrxID;
use error_stack::Report;
use std::mem;

pub const SUPER_BLOCK_VERSION: u64 = 1;
pub const SUPER_BLOCK_SIZE: usize = PAGE_SIZE / 2;
pub const SUPER_BLOCK_FOOTER_SIZE: usize = mem::size_of::<SuperBlockFooter>();
pub const SUPER_BLOCK_FOOTER_OFFSET: usize = SUPER_BLOCK_SIZE - SUPER_BLOCK_FOOTER_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperBlockHeader {
    /// Magic word of table file, by default 'DORA\0\0\0\0'
    pub magic_word: [u8; 8],
    /// Version of super block format.
    pub version: u64,
    /// Slot number of this super block.
    pub slot_no: u64,
    /// checkpoint timestamp of this super block.
    pub checkpoint_cts: TrxID,
}

impl Ser<'_> for SuperBlockHeader {
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

impl Deser for SuperBlockHeader {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, magic_word) = input.deser_byte_array::<8>(start_idx)?;
        let (idx, version) = input.deser_u64(idx)?;
        let (idx, slot_no) = input.deser_u64(idx)?;
        let (idx, checkpoint_cts) = input.deser_u64(idx)?;
        let res = SuperBlockHeader {
            magic_word,
            version,
            slot_no,
            checkpoint_cts,
        };
        Ok((idx, res))
    }
}

#[derive(PartialEq, Eq)]
pub struct SuperBlockBody {
    pub meta_block_id: BlockID,
}

impl Ser<'_> for SuperBlockBody {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<BlockID>()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_u64(start_idx, self.meta_block_id.into())
    }
}

impl Deser for SuperBlockBody {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, meta_block_id) = input.deser_u64(start_idx)?;
        Ok((
            idx,
            SuperBlockBody {
                meta_block_id: BlockID::from(meta_block_id),
            },
        ))
    }
}

#[derive(Default, PartialEq, Eq)]
pub struct SuperBlockFooter {
    pub b3sum: [u8; 32],
    pub checkpoint_cts: TrxID,
}

impl Deser for SuperBlockFooter {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, b3sum) = input.deser_byte_array::<32>(start_idx)?;
        let (idx, checkpoint_cts) = input.deser_u64(idx)?;
        Ok((
            idx,
            SuperBlockFooter {
                b3sum,
                checkpoint_cts,
            },
        ))
    }
}

impl Ser<'_> for SuperBlockFooter {
    #[inline]
    fn ser_len(&self) -> usize {
        SUPER_BLOCK_FOOTER_SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.b3sum);
        out.ser_u64(idx, self.checkpoint_cts)
    }
}

#[derive(PartialEq, Eq)]
pub struct SuperBlock {
    pub header: SuperBlockHeader,
    pub body: SuperBlockBody,
    pub footer: SuperBlockFooter,
}

pub struct SuperBlockSerView {
    pub header: SuperBlockHeader,
    pub body: SuperBlockBody,
}

impl Ser<'_> for SuperBlockSerView {
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
pub fn parse_super_block(
    buf: &[u8],
    expected_magic_word: [u8; 8],
    expected_version: u64,
) -> Result<SuperBlock> {
    let (body_start, header) = SuperBlockHeader::deser(buf, 0).map_err(|_| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=super-block, section=header"),
        )
    })?;
    if header.magic_word != expected_magic_word {
        return Err(Report::new(DataIntegrityError::InvalidMagic)
            .attach(format!(
                "block=super-block, expected_magic={expected_magic_word:?}, actual_magic={:?}",
                header.magic_word
            ))
            .into());
    }
    if header.version != expected_version {
        return Err(Report::new(DataIntegrityError::InvalidVersion)
            .attach(format!(
                "block=super-block, expected_version={expected_version}, actual_version={}",
                header.version
            ))
            .into());
    }
    let (_, footer) = SuperBlockFooter::deser(buf, SUPER_BLOCK_FOOTER_OFFSET).map_err(|_| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=super-block, section=footer"),
        )
    })?;
    if header.checkpoint_cts != footer.checkpoint_cts {
        return Err(Report::new(DataIntegrityError::TornWrite)
            .attach(format!(
                "block=super-block, header_checkpoint_cts={}, footer_checkpoint_cts={}",
                header.checkpoint_cts, footer.checkpoint_cts
            ))
            .into());
    }
    let b3sum = blake3::hash(&buf[..SUPER_BLOCK_FOOTER_OFFSET]);
    if b3sum != footer.b3sum {
        return Err(Report::new(DataIntegrityError::ChecksumMismatch)
            .attach("block=super-block")
            .into());
    }
    let (_, body) = SuperBlockBody::deser(buf, body_start).map_err(|_| {
        Error::from(
            Report::new(DataIntegrityError::InvalidPayload)
                .attach("block=super-block, section=body"),
        )
    })?;
    Ok(SuperBlock {
        header,
        body,
        footer,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::table_file::TABLE_FILE_MAGIC_WORD;
    use crate::file::test_block_id;

    #[test]
    fn test_super_block_serde() {
        let header = SuperBlockHeader {
            magic_word: TABLE_FILE_MAGIC_WORD,
            version: SUPER_BLOCK_VERSION,
            slot_no: 1,
            checkpoint_cts: 12,
        };
        let body = SuperBlockBody {
            meta_block_id: test_block_id(7),
        };
        let ser_view = SuperBlockSerView {
            header: header.clone(),
            body,
        };
        let ser_len = ser_view.ser_len();
        let mut data = vec![0u8; ser_len];
        let res_idx = ser_view.ser(&mut data[..], 0);
        assert_eq!(res_idx, ser_len);

        let (idx, deser_header) = SuperBlockHeader::deser(&data[..], 0).unwrap();
        let (_, deser_body) = SuperBlockBody::deser(&data[..], idx).unwrap();
        assert_eq!(deser_header, header);
        assert_eq!(deser_body.meta_block_id, test_block_id(7));
    }
}
