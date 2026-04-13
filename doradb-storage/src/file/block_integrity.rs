use crate::error::{BlockCorruptionCause, Result};
use crate::serde::{Deser, Ser, Serde};
use bytemuck::{Pod, Zeroable};
use std::mem;

/// Size in bytes of the fixed block-integrity header.
pub(crate) const BLOCK_INTEGRITY_HEADER_SIZE: usize = mem::size_of::<BlockIntegrityHeader>();
/// Size in bytes of the fixed BLAKE3 checksum trailer.
pub(crate) const BLOCK_INTEGRITY_TRAILER_SIZE: usize = mem::size_of::<BlockIntegrityTrailer>();

/// Block-integrity markers for persisted LWC blocks.
pub(crate) const LWC_BLOCK_SPEC: BlockIntegritySpec = BlockIntegritySpec::new(*b"LWCPAGE\0", 1);
/// Block-integrity markers for persisted column block-index nodes.
pub(crate) const COLUMN_BLOCK_INDEX_BLOCK_SPEC: BlockIntegritySpec =
    BlockIntegritySpec::new(*b"CBINDEX\0", 2);
/// Block-integrity markers for persisted column auxiliary-blob blocks.
pub(crate) const COLUMN_DELETION_BLOB_BLOCK_SPEC: BlockIntegritySpec =
    BlockIntegritySpec::new(*b"CDBLOB\0\0", 2);
/// Expected block-envelope markers for one persisted CoW block kind.
///
/// The shared integrity helpers use this to validate that a block belongs to
/// the intended format before its payload is parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockIntegritySpec {
    /// Block-kind-specific magic bytes stored at the start of the block.
    pub magic_word: [u8; 8],
    /// Block-envelope version expected for the payload format.
    pub version: u64,
}

impl BlockIntegritySpec {
    /// Build one block-integrity specification for a concrete persisted block kind.
    #[inline]
    pub(crate) const fn new(magic_word: [u8; 8], version: u64) -> Self {
        BlockIntegritySpec {
            magic_word,
            version,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockIntegrityHeader {
    magic_word: [u8; 8],
    version: u64,
}

impl Ser<'_> for BlockIntegrityHeader {
    #[inline]
    fn ser_len(&self) -> usize {
        BLOCK_INTEGRITY_HEADER_SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.magic_word);
        out.ser_u64(idx, self.version)
    }
}

impl Deser for BlockIntegrityHeader {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, magic_word) = input.deser_byte_array::<8>(start_idx)?;
        let (idx, version) = input.deser_u64(idx)?;
        Ok((
            idx,
            BlockIntegrityHeader {
                magic_word,
                version,
            },
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub(crate) struct BlockIntegrityTrailer {
    b3sum: [u8; 32],
}

impl Ser<'_> for BlockIntegrityTrailer {
    #[inline]
    fn ser_len(&self) -> usize {
        BLOCK_INTEGRITY_TRAILER_SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_byte_array(start_idx, &self.b3sum)
    }
}

impl Deser for BlockIntegrityTrailer {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, b3sum) = input.deser_byte_array::<32>(start_idx)?;
        Ok((idx, BlockIntegrityTrailer { b3sum }))
    }
}

/// Returns the starting byte offset of the checksum trailer for one block image.
///
/// Callers must provide a full persisted block image large enough to hold the
/// shared header and trailer.
#[inline]
pub(crate) const fn checksum_offset(page_len: usize) -> usize {
    page_len - BLOCK_INTEGRITY_TRAILER_SIZE
}

/// Returns the largest payload size that fits inside the shared integrity envelope.
#[inline]
pub(crate) const fn max_payload_len(page_len: usize) -> usize {
    page_len - BLOCK_INTEGRITY_HEADER_SIZE - BLOCK_INTEGRITY_TRAILER_SIZE
}

/// Writes the integrity header for one persisted block and returns payload start.
#[inline]
pub(crate) fn write_block_header(buf: &mut [u8], spec: BlockIntegritySpec) -> usize {
    let header = BlockIntegrityHeader {
        magic_word: spec.magic_word,
        version: spec.version,
    };
    header.ser(buf, 0)
}

/// Computes and writes the trailing BLAKE3 checksum for one full block image.
#[inline]
pub(crate) fn write_block_checksum(buf: &mut [u8]) {
    let trailer = BlockIntegrityTrailer {
        b3sum: *blake3::hash(&buf[..checksum_offset(buf.len())]).as_bytes(),
    };
    let idx = trailer.ser(buf, checksum_offset(buf.len()));
    debug_assert_eq!(idx, buf.len());
}

/// Validates only the trailing BLAKE3 checksum for one full block image.
///
/// This is used by block formats, such as DiskTree nodes, that reuse the
/// shared checksum trailer without the shared magic/version header.
#[inline]
pub(crate) fn validate_block_checksum(buf: &[u8]) -> std::result::Result<(), BlockCorruptionCause> {
    if buf.len() < BLOCK_INTEGRITY_TRAILER_SIZE {
        return Err(BlockCorruptionCause::InvalidPayload);
    }
    let checksum_offset = checksum_offset(buf.len());
    let (_, trailer) = BlockIntegrityTrailer::deser(buf, checksum_offset)
        .map_err(|_| BlockCorruptionCause::ChecksumMismatch)?;
    let b3sum = blake3::hash(&buf[..checksum_offset]);
    if b3sum.as_bytes() != &trailer.b3sum {
        return Err(BlockCorruptionCause::ChecksumMismatch);
    }
    Ok(())
}

/// Validates one persisted block envelope and returns the payload slice on success.
///
/// This helper expects a full fixed-size persisted block image. The checksum
/// covers the whole block except the trailing checksum bytes.
#[inline]
pub(crate) fn validate_block(
    buf: &[u8],
    expected: BlockIntegritySpec,
) -> std::result::Result<&[u8], BlockCorruptionCause> {
    debug_assert!(
        buf.len() >= BLOCK_INTEGRITY_HEADER_SIZE + BLOCK_INTEGRITY_TRAILER_SIZE,
        "validate_block expects a full persisted block image"
    );
    let (payload_start, header) =
        BlockIntegrityHeader::deser(buf, 0).map_err(|_| BlockCorruptionCause::InvalidMagic)?;
    if header.magic_word != expected.magic_word {
        return Err(BlockCorruptionCause::InvalidMagic);
    }
    if header.version != expected.version {
        return Err(BlockCorruptionCause::InvalidVersion);
    }
    validate_block_checksum(buf)?;
    let checksum_offset = checksum_offset(buf.len());
    Ok(&buf[payload_start..checksum_offset])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_integrity_roundtrip() {
        let spec = BlockIntegritySpec::new(*b"TSTMETA\0", 7);
        let mut buf = vec![0u8; 4096];
        let payload_start = write_block_header(&mut buf, spec);
        buf[payload_start..payload_start + 5].copy_from_slice(b"hello");
        write_block_checksum(&mut buf);

        let payload = validate_block(&buf, spec).unwrap();
        assert_eq!(&payload[..5], b"hello");
    }

    #[test]
    fn test_block_integrity_rejects_bad_checksum() {
        let spec = BlockIntegritySpec::new(*b"TSTMETA\0", 7);
        let mut buf = vec![0u8; 4096];
        let payload_start = write_block_header(&mut buf, spec);
        buf[payload_start] = 1;
        write_block_checksum(&mut buf);
        let checksum_idx = checksum_offset(buf.len());
        buf[checksum_idx] ^= 0xff;

        let err = validate_block(&buf, spec).unwrap_err();
        assert_eq!(err, BlockCorruptionCause::ChecksumMismatch);
    }

    #[test]
    fn test_block_integrity_checksum_only_trailer() {
        let mut buf = vec![0u8; 4096];
        buf[10] = 3;
        write_block_checksum(&mut buf);
        validate_block_checksum(&buf).unwrap();

        buf[10] ^= 0xff;
        let err = validate_block_checksum(&buf).unwrap_err();
        assert_eq!(err, BlockCorruptionCause::ChecksumMismatch);
    }
}
