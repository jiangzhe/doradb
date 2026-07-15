use crate::buffer::page::PAGE_SIZE;
use crate::error::{DataIntegrityError, Error, Result};
use crate::id::{BlockID, TrxID};
use crate::serde::{Deser, MinBytesHint, Ser, Serde, min_bytes_hint};
use error_stack::Report;
use std::mem;

/// On-disk super-block format version.
pub(crate) const SUPER_BLOCK_VERSION: u64 = 1;
/// Size in bytes of one ping-pong super-block slot.
pub(crate) const SUPER_BLOCK_SIZE: usize = PAGE_SIZE / 2;
/// Size in bytes of the checksummed super-block footer.
pub(crate) const SUPER_BLOCK_FOOTER_SIZE: usize = mem::size_of::<SuperBlockFooter>();
/// Byte offset where the footer starts inside one super-block slot.
pub(crate) const SUPER_BLOCK_FOOTER_OFFSET: usize = SUPER_BLOCK_SIZE - SUPER_BLOCK_FOOTER_SIZE;

/// Header fields stored at the front of every super-block slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SuperBlockHeader {
    /// Magic word of table file, by default 'DORA\0\0\0\0'
    pub(crate) magic_word: [u8; 8],
    /// Version of super block format.
    pub(crate) version: u64,
    /// Slot number of this super block.
    pub(crate) slot_no: u64,
    /// checkpoint timestamp of this super block.
    pub(crate) checkpoint_cts: TrxID,
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
        out.ser_u64(idx, self.checkpoint_cts.as_u64())
    }
}

impl Deser for SuperBlockHeader {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(
        mem::size_of::<[u8; 8]>()
            + mem::size_of::<u64>()
            + mem::size_of::<u64>()
            + mem::size_of::<TrxID>(),
    );

    #[inline]
    fn deser<S: Serde + ?Sized>(
        input: &S,
        start_idx: usize,
    ) -> crate::serde::DeserResult<(usize, Self)> {
        let (idx, magic_word) = input.deser_byte_array::<8>(start_idx)?;
        let (idx, version) = input.deser_u64(idx)?;
        let (idx, slot_no) = input.deser_u64(idx)?;
        let (idx, checkpoint_cts) = TrxID::deser(input, idx)?;
        let res = SuperBlockHeader {
            magic_word,
            version,
            slot_no,
            checkpoint_cts,
        };
        Ok((idx, res))
    }
}

/// Super-block body fields covered by the footer checksum.
#[derive(PartialEq, Eq)]
pub(crate) struct SuperBlockBody {
    /// Active meta-block id referenced by this super-block slot.
    pub(crate) meta_block_id: BlockID,
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
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(mem::size_of::<BlockID>());

    #[inline]
    fn deser<S: Serde + ?Sized>(
        input: &S,
        start_idx: usize,
    ) -> crate::serde::DeserResult<(usize, Self)> {
        let (idx, meta_block_id) = input.deser_u64(start_idx)?;
        Ok((
            idx,
            SuperBlockBody {
                meta_block_id: BlockID::from(meta_block_id),
            },
        ))
    }
}

/// Footer fields stored at the end of every super-block slot.
#[derive(Default, PartialEq, Eq)]
pub(crate) struct SuperBlockFooter {
    /// Blake3 checksum of the super-block bytes before the footer.
    pub(crate) b3sum: [u8; 32],
    /// Checkpoint timestamp repeated from the header to detect torn writes.
    pub(crate) checkpoint_cts: TrxID,
}

impl Deser for SuperBlockFooter {
    const MIN_BYTES_HINT: MinBytesHint = min_bytes_hint(SUPER_BLOCK_FOOTER_SIZE);

    #[inline]
    fn deser<S: Serde + ?Sized>(
        input: &S,
        start_idx: usize,
    ) -> crate::serde::DeserResult<(usize, Self)> {
        let (idx, b3sum) = input.deser_byte_array::<32>(start_idx)?;
        let (idx, checkpoint_cts) = TrxID::deser(input, idx)?;
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
        out.ser_u64(idx, self.checkpoint_cts.as_u64())
    }
}

/// Fully decoded and validated super-block slot.
#[derive(PartialEq, Eq)]
pub(crate) struct SuperBlock {
    /// Decoded super-block header.
    pub(crate) header: SuperBlockHeader,
    /// Decoded super-block body.
    pub(crate) body: SuperBlockBody,
    /// Decoded super-block footer.
    pub(crate) footer: SuperBlockFooter,
}

/// Borrowed serialization view used when writing one super-block slot.
pub(crate) struct SuperBlockSerView {
    /// Header to serialize.
    pub(crate) header: SuperBlockHeader,
    /// Body to serialize.
    pub(crate) body: SuperBlockBody,
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

/// Parse and validate one super-block slot.
#[inline]
pub(crate) fn parse_super_block(
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
            checkpoint_cts: TrxID::new(12),
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
