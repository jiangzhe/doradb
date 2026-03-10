use crate::error::{PersistedPageCorruptionCause, Result};
use crate::serde::{Deser, Ser, Serde};
use std::mem;

/// Size in bytes of the fixed page-integrity header.
pub(crate) const PAGE_INTEGRITY_HEADER_SIZE: usize = mem::size_of::<PageIntegrityHeader>();
/// Size in bytes of the fixed BLAKE3 checksum trailer.
pub(crate) const PAGE_INTEGRITY_TRAILER_SIZE: usize = mem::size_of::<PageIntegrityTrailer>();

/// Page-integrity markers for persisted LWC pages.
pub(crate) const LWC_PAGE_SPEC: PageIntegritySpec = PageIntegritySpec::new(*b"LWCPAGE\0", 1);
/// Page-integrity markers for persisted column block-index nodes.
pub(crate) const COLUMN_BLOCK_INDEX_PAGE_SPEC: PageIntegritySpec =
    PageIntegritySpec::new(*b"CBINDEX\0", 1);
/// Page-integrity markers for persisted deletion-blob pages.
pub(crate) const COLUMN_DELETION_BLOB_PAGE_SPEC: PageIntegritySpec =
    PageIntegritySpec::new(*b"CDBLOB\0\0", 1);

/// Expected page-envelope markers for one persisted CoW page kind.
///
/// The shared integrity helpers use this to validate that a page belongs to
/// the intended format before its payload is parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PageIntegritySpec {
    /// Page-kind-specific magic bytes stored at the start of the page.
    pub magic_word: [u8; 8],
    /// Page-envelope version expected for the payload format.
    pub version: u64,
}

impl PageIntegritySpec {
    /// Build one page-integrity specification for a concrete persisted page kind.
    #[inline]
    pub(crate) const fn new(magic_word: [u8; 8], version: u64) -> Self {
        PageIntegritySpec {
            magic_word,
            version,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageIntegrityHeader {
    magic_word: [u8; 8],
    version: u64,
}

impl Ser<'_> for PageIntegrityHeader {
    #[inline]
    fn ser_len(&self) -> usize {
        PAGE_INTEGRITY_HEADER_SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_byte_array(start_idx, &self.magic_word);
        out.ser_u64(idx, self.version)
    }
}

impl Deser for PageIntegrityHeader {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, magic_word) = input.deser_byte_array::<8>(start_idx)?;
        let (idx, version) = input.deser_u64(idx)?;
        Ok((
            idx,
            PageIntegrityHeader {
                magic_word,
                version,
            },
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageIntegrityTrailer {
    b3sum: [u8; 32],
}

impl Ser<'_> for PageIntegrityTrailer {
    #[inline]
    fn ser_len(&self) -> usize {
        PAGE_INTEGRITY_TRAILER_SIZE
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        out.ser_byte_array(start_idx, &self.b3sum)
    }
}

impl Deser for PageIntegrityTrailer {
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, b3sum) = input.deser_byte_array::<32>(start_idx)?;
        Ok((idx, PageIntegrityTrailer { b3sum }))
    }
}

/// Returns the starting byte offset of the checksum trailer for one page image.
///
/// Callers must provide a full persisted page image large enough to hold the
/// shared header and trailer.
#[inline]
pub(crate) const fn checksum_offset(page_len: usize) -> usize {
    page_len - PAGE_INTEGRITY_TRAILER_SIZE
}

/// Returns the largest payload size that fits inside the shared integrity envelope.
#[inline]
pub(crate) const fn max_payload_len(page_len: usize) -> usize {
    page_len - PAGE_INTEGRITY_HEADER_SIZE - PAGE_INTEGRITY_TRAILER_SIZE
}

/// Writes the integrity header for one persisted page and returns payload start.
#[inline]
pub(crate) fn write_page_header(buf: &mut [u8], spec: PageIntegritySpec) -> usize {
    let header = PageIntegrityHeader {
        magic_word: spec.magic_word,
        version: spec.version,
    };
    header.ser(buf, 0)
}

/// Computes and writes the trailing BLAKE3 checksum for one full page image.
#[inline]
pub(crate) fn write_page_checksum(buf: &mut [u8]) {
    let trailer = PageIntegrityTrailer {
        b3sum: *blake3::hash(&buf[..checksum_offset(buf.len())]).as_bytes(),
    };
    let idx = trailer.ser(buf, checksum_offset(buf.len()));
    debug_assert_eq!(idx, buf.len());
}

/// Validates one persisted page envelope and returns the payload slice on success.
///
/// This helper expects a full fixed-size persisted page image. The checksum
/// covers the whole page except the trailing checksum bytes.
#[inline]
pub(crate) fn validate_page(
    buf: &[u8],
    expected: PageIntegritySpec,
) -> std::result::Result<&[u8], PersistedPageCorruptionCause> {
    debug_assert!(
        buf.len() >= PAGE_INTEGRITY_HEADER_SIZE + PAGE_INTEGRITY_TRAILER_SIZE,
        "validate_page expects a full persisted page image"
    );
    let (payload_start, header) = PageIntegrityHeader::deser(buf, 0)
        .map_err(|_| PersistedPageCorruptionCause::InvalidMagic)?;
    if header.magic_word != expected.magic_word {
        return Err(PersistedPageCorruptionCause::InvalidMagic);
    }
    if header.version != expected.version {
        return Err(PersistedPageCorruptionCause::InvalidVersion);
    }
    let checksum_offset = checksum_offset(buf.len());
    let (_, trailer) = PageIntegrityTrailer::deser(buf, checksum_offset)
        .map_err(|_| PersistedPageCorruptionCause::ChecksumMismatch)?;
    let b3sum = blake3::hash(&buf[..checksum_offset]);
    if b3sum.as_bytes() != &trailer.b3sum {
        return Err(PersistedPageCorruptionCause::ChecksumMismatch);
    }
    Ok(&buf[payload_start..checksum_offset])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_integrity_roundtrip() {
        let spec = PageIntegritySpec::new(*b"TSTMETA\0", 7);
        let mut buf = vec![0u8; 4096];
        let payload_start = write_page_header(&mut buf, spec);
        buf[payload_start..payload_start + 5].copy_from_slice(b"hello");
        write_page_checksum(&mut buf);

        let payload = validate_page(&buf, spec).unwrap();
        assert_eq!(&payload[..5], b"hello");
    }

    #[test]
    fn test_page_integrity_rejects_bad_checksum() {
        let spec = PageIntegritySpec::new(*b"TSTMETA\0", 7);
        let mut buf = vec![0u8; 4096];
        let payload_start = write_page_header(&mut buf, spec);
        buf[payload_start] = 1;
        write_page_checksum(&mut buf);
        let checksum_idx = checksum_offset(buf.len());
        buf[checksum_idx] ^= 0xff;

        let err = validate_page(&buf, spec).unwrap_err();
        assert_eq!(err, PersistedPageCorruptionCause::ChecksumMismatch);
    }
}
