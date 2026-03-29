use crate::buffer::ReadonlyBufferPool;
use crate::buffer::guard::PageGuard;
use crate::buffer::page::PageID;
use crate::error::{
    Error, PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind, Result,
};
use crate::file::cow_file::{COW_FILE_PAGE_SIZE, MutableCowFile};
use crate::file::page_integrity::{
    COLUMN_DELETION_BLOB_PAGE_SPEC, PAGE_INTEGRITY_HEADER_SIZE, max_payload_len, validate_page,
    write_page_checksum, write_page_header,
};
use crate::index::column_payload::BlobRef;
use crate::io::DirectBuf;
use futures::future::try_join_all;
use std::mem;

const COLUMN_DELETION_BLOB_NEXT_PAGE_OFFSET: usize = 0;
const COLUMN_DELETION_BLOB_USED_SIZE_OFFSET: usize = mem::size_of::<PageID>();
const COLUMN_AUX_BLOB_PAYLOAD_LEN_OFFSET: usize = 4;
const RAW_U32_CODEC_VERSION: u8 = 1;

/// Shared page-local header for one immutable auxiliary-blob page.
pub const COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE: usize =
    mem::size_of::<PageID>() + mem::size_of::<u16>();
/// Remaining bytes available for framed blob payload data on one page.
pub const COLUMN_DELETION_BLOB_PAGE_BODY_SIZE: usize =
    max_payload_len(COW_FILE_PAGE_SIZE) - COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE;
/// Stable per-blob framing header length for v2 column auxiliary blobs.
pub const COLUMN_AUX_BLOB_HEADER_SIZE: usize = 8;
/// Blob kind for persisted row-id-delta delete payloads.
pub const COLUMN_AUX_BLOB_KIND_DELETE_DELTAS: u8 = 1;
/// Codec kind for little-endian `u32` delete-delta payload bytes.
pub const COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST: u8 = 1;

struct BlobPageHeader {
    next_page_id: PageID,
    used_size: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColumnAuxBlobHeader {
    blob_kind: u8,
    codec_kind: u8,
    codec_version: u8,
    flags: u8,
    payload_len: u32,
}

impl ColumnAuxBlobHeader {
    #[inline]
    pub(crate) fn new(
        blob_kind: u8,
        codec_kind: u8,
        codec_version: u8,
        flags: u8,
        payload_len: usize,
    ) -> Result<Self> {
        if payload_len == 0 || payload_len > u32::MAX as usize {
            return Err(Error::InvalidArgument);
        }
        Ok(ColumnAuxBlobHeader {
            blob_kind,
            codec_kind,
            codec_version,
            flags,
            payload_len: payload_len as u32,
        })
    }

    #[inline]
    pub(crate) fn deletion_deltas(payload_len: usize) -> Result<Self> {
        Self::new(
            COLUMN_AUX_BLOB_KIND_DELETE_DELTAS,
            COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST,
            RAW_U32_CODEC_VERSION,
            0,
            payload_len,
        )
    }

    #[inline]
    pub(crate) fn blob_kind(&self) -> u8 {
        self.blob_kind
    }

    #[inline]
    pub(crate) fn codec_kind(&self) -> u8 {
        self.codec_kind
    }

    #[inline]
    pub(crate) fn codec_version(&self) -> u8 {
        self.codec_version
    }

    #[inline]
    pub(crate) fn payload_len(&self) -> usize {
        self.payload_len as usize
    }

    #[inline]
    fn encode(self) -> [u8; COLUMN_AUX_BLOB_HEADER_SIZE] {
        let mut out = [0u8; COLUMN_AUX_BLOB_HEADER_SIZE];
        out[0] = self.blob_kind;
        out[1] = self.codec_kind;
        out[2] = self.codec_version;
        out[3] = self.flags;
        out[COLUMN_AUX_BLOB_PAYLOAD_LEN_OFFSET..COLUMN_AUX_BLOB_PAYLOAD_LEN_OFFSET + 4]
            .copy_from_slice(&self.payload_len.to_le_bytes());
        out
    }

    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < COLUMN_AUX_BLOB_HEADER_SIZE {
            return Err(Error::InvalidFormat);
        }
        let payload_len = u32::from_le_bytes(
            bytes[COLUMN_AUX_BLOB_PAYLOAD_LEN_OFFSET..COLUMN_AUX_BLOB_PAYLOAD_LEN_OFFSET + 4]
                .try_into()?,
        );
        if payload_len == 0 {
            return Err(Error::InvalidFormat);
        }
        Ok(ColumnAuxBlobHeader {
            blob_kind: bytes[0],
            codec_kind: bytes[1],
            codec_version: bytes[2],
            flags: bytes[3],
            payload_len,
        })
    }
}

#[inline]
fn validate_delete_delta_header(header: &ColumnAuxBlobHeader) -> Result<()> {
    if header.blob_kind() != COLUMN_AUX_BLOB_KIND_DELETE_DELTAS
        || header.codec_kind() != COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST
        || header.codec_version() != RAW_U32_CODEC_VERSION
        || header.flags != 0
    {
        return Err(Error::InvalidFormat);
    }
    Ok(())
}

fn decode_blob_page_header(page: &[u8]) -> Result<BlobPageHeader> {
    let next_page_id = u64::from_le_bytes(
        page[COLUMN_DELETION_BLOB_NEXT_PAGE_OFFSET..COLUMN_DELETION_BLOB_NEXT_PAGE_OFFSET + 8]
            .try_into()?,
    );
    let used_size = u16::from_le_bytes(
        page[COLUMN_DELETION_BLOB_USED_SIZE_OFFSET..COLUMN_DELETION_BLOB_USED_SIZE_OFFSET + 2]
            .try_into()?,
    );
    if used_size as usize > COLUMN_DELETION_BLOB_PAGE_BODY_SIZE {
        return Err(Error::InvalidFormat);
    }
    Ok(BlobPageHeader {
        next_page_id,
        used_size,
    })
}

#[inline]
fn invalid_blob_payload(file_kind: PersistedFileKind, page_id: PageID) -> Error {
    Error::persisted_page_corrupted(
        file_kind,
        PersistedPageKind::ColumnDeletionBlob,
        page_id,
        PersistedPageCorruptionCause::InvalidPayload,
    )
}

#[inline]
fn validate_blob_page(page: &[u8], file_kind: PersistedFileKind, page_id: PageID) -> Result<&[u8]> {
    validate_page(page, COLUMN_DELETION_BLOB_PAGE_SPEC).map_err(|cause| {
        Error::persisted_page_corrupted(
            file_kind,
            PersistedPageKind::ColumnDeletionBlob,
            page_id,
            cause,
        )
    })
}

#[inline]
pub(crate) fn validate_persisted_blob_page(
    page: &[u8],
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<()> {
    let payload = validate_blob_page(page, file_kind, page_id)?;
    decode_blob_page_header(payload).map_err(|err| match err {
        Error::InvalidFormat => invalid_blob_payload(file_kind, page_id),
        other => other,
    })?;
    Ok(())
}

#[inline]
fn validated_blob_page_payload(page: &[u8]) -> &[u8] {
    let payload_start = PAGE_INTEGRITY_HEADER_SIZE;
    let payload_end = payload_start + max_payload_len(COW_FILE_PAGE_SIZE);
    &page[payload_start..payload_end]
}

fn encode_blob_page_header(buf: &mut [u8], next_page_id: PageID, used_size: usize) -> Result<()> {
    if used_size > COLUMN_DELETION_BLOB_PAGE_BODY_SIZE {
        return Err(Error::InvalidArgument);
    }
    buf[..COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE].fill(0);
    buf[COLUMN_DELETION_BLOB_NEXT_PAGE_OFFSET..COLUMN_DELETION_BLOB_NEXT_PAGE_OFFSET + 8]
        .copy_from_slice(&next_page_id.to_le_bytes());
    buf[COLUMN_DELETION_BLOB_USED_SIZE_OFFSET..COLUMN_DELETION_BLOB_USED_SIZE_OFFSET + 2]
        .copy_from_slice(&(used_size as u16).to_le_bytes());
    Ok(())
}

struct PendingBlobPage {
    page_id: PageID,
    used_size: usize,
    buf: DirectBuf,
}

impl PendingBlobPage {
    #[inline]
    fn new(page_id: PageID) -> Self {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let payload_start = write_page_header(buf.data_mut(), COLUMN_DELETION_BLOB_PAGE_SPEC);
        debug_assert_eq!(payload_start, PAGE_INTEGRITY_HEADER_SIZE);
        PendingBlobPage {
            page_id,
            used_size: 0,
            buf,
        }
    }

    #[inline]
    fn free_space(&self) -> usize {
        COLUMN_DELETION_BLOB_PAGE_BODY_SIZE - self.used_size
    }

    #[inline]
    fn write_bytes(&mut self, src: &[u8]) {
        debug_assert!(src.len() <= self.free_space());
        let start =
            PAGE_INTEGRITY_HEADER_SIZE + COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE + self.used_size;
        let end = start + src.len();
        self.buf.data_mut()[start..end].copy_from_slice(src);
        self.used_size += src.len();
    }
}

struct SealedBlobPage {
    page: PendingBlobPage,
    next_page_id: PageID,
}

/// Append-only writer for immutable shared delete-payload blobs.
pub struct ColumnDeletionBlobWriter<'a, M: MutableCowFile> {
    mutable_file: &'a mut M,
    current_page: Option<PendingBlobPage>,
    sealed_pages: Vec<SealedBlobPage>,
}

impl<'a, M: MutableCowFile> ColumnDeletionBlobWriter<'a, M> {
    #[inline]
    pub fn new(mutable_file: &'a mut M) -> Self {
        ColumnDeletionBlobWriter {
            mutable_file,
            current_page: None,
            sealed_pages: Vec::new(),
        }
    }

    /// Appends one delete-delta blob and returns the persisted blob reference.
    pub async fn append(&mut self, bytes: &[u8]) -> Result<BlobRef> {
        let header = ColumnAuxBlobHeader::deletion_deltas(bytes.len())?;
        self.append_framed_blob(header, bytes).await
    }

    async fn append_framed_blob(
        &mut self,
        header: ColumnAuxBlobHeader,
        bytes: &[u8],
    ) -> Result<BlobRef> {
        let framed_len = COLUMN_AUX_BLOB_HEADER_SIZE + bytes.len();
        if framed_len > u32::MAX as usize {
            return Err(Error::InvalidArgument);
        }
        self.ensure_current_page()?;
        let (start_page_id, start_offset) = {
            let current = self.current_page.as_ref().ok_or(Error::InvalidState)?;
            (current.page_id, current.used_size)
        };
        let header_bytes = header.encode();
        self.write_stream(&header_bytes)?;
        self.write_stream(bytes)?;
        Ok(BlobRef {
            start_page_id,
            start_offset: start_offset as u16,
            byte_len: framed_len as u32,
        })
    }

    /// Flushes every pending blob page into the mutable CoW file.
    pub async fn finish(&mut self) -> Result<()> {
        if let Some(page) = self.current_page.take()
            && page.used_size > 0
        {
            self.sealed_pages.push(SealedBlobPage {
                page,
                next_page_id: 0,
            });
        }
        let sealed_pages = std::mem::take(&mut self.sealed_pages);
        let mut writes = Vec::with_capacity(sealed_pages.len());
        for mut sealed in sealed_pages {
            let payload_end = PAGE_INTEGRITY_HEADER_SIZE + max_payload_len(COW_FILE_PAGE_SIZE);
            encode_blob_page_header(
                &mut sealed.page.buf.data_mut()[PAGE_INTEGRITY_HEADER_SIZE..payload_end],
                sealed.next_page_id,
                sealed.page.used_size,
            )?;
            write_page_checksum(sealed.page.buf.data_mut());
            writes.push(
                self.mutable_file
                    .write_page(sealed.page.page_id, sealed.page.buf),
            );
        }
        try_join_all(writes).await?;
        Ok(())
    }

    #[inline]
    fn ensure_current_page(&mut self) -> Result<()> {
        match self.current_page.as_ref() {
            Some(current) if current.free_space() == 0 => self.roll_to_next_page()?,
            Some(_) => {}
            None => {
                let page_id = self.mutable_file.allocate_page_id()?;
                self.current_page = Some(PendingBlobPage::new(page_id));
            }
        }
        Ok(())
    }

    fn write_stream(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            let free_space = self
                .current_page
                .as_ref()
                .ok_or(Error::InvalidState)?
                .free_space();
            if free_space == 0 {
                self.roll_to_next_page()?;
                continue;
            }
            let take = bytes.len().min(free_space);
            let current = self.current_page.as_mut().ok_or(Error::InvalidState)?;
            current.write_bytes(&bytes[..take]);
            bytes = &bytes[take..];
        }
        Ok(())
    }

    fn roll_to_next_page(&mut self) -> Result<()> {
        let next_page_id = self.mutable_file.allocate_page_id()?;
        let page = self.current_page.take().ok_or(Error::InvalidState)?;
        self.sealed_pages
            .push(SealedBlobPage { page, next_page_id });
        self.current_page = Some(PendingBlobPage::new(next_page_id));
        Ok(())
    }
}

/// Reader for immutable auxiliary blobs referenced by `BlobRef`.
pub struct ColumnDeletionBlobReader<'a> {
    disk_pool: &'a ReadonlyBufferPool,
}

impl<'a> ColumnDeletionBlobReader<'a> {
    #[inline]
    pub fn new(disk_pool: &'a ReadonlyBufferPool) -> Self {
        ColumnDeletionBlobReader { disk_pool }
    }

    /// Reads and validates one framed blob, returning its header and payload bytes.
    pub(crate) async fn read_framed_blob(
        &self,
        blob_ref: BlobRef,
    ) -> Result<(ColumnAuxBlobHeader, Vec<u8>)> {
        let mut bytes = self.read_raw(blob_ref).await?;
        if bytes.len() < COLUMN_AUX_BLOB_HEADER_SIZE {
            return Err(Error::InvalidFormat);
        }
        let header = ColumnAuxBlobHeader::decode(&bytes[..COLUMN_AUX_BLOB_HEADER_SIZE])?;
        if bytes.len() != COLUMN_AUX_BLOB_HEADER_SIZE + header.payload_len() {
            return Err(Error::InvalidFormat);
        }
        let payload = bytes.split_off(COLUMN_AUX_BLOB_HEADER_SIZE);
        Ok((header, payload))
    }

    /// Reads the payload bytes of one delete-delta blob after validating its framing header.
    pub async fn read(&self, blob_ref: BlobRef) -> Result<Vec<u8>> {
        let (header, payload) = self.read_framed_blob(blob_ref).await?;
        validate_delete_delta_header(&header)?;
        Ok(payload)
    }

    async fn read_raw(&self, blob_ref: BlobRef) -> Result<Vec<u8>> {
        if blob_ref.start_page_id == 0 || blob_ref.byte_len == 0 {
            return Err(Error::InvalidFormat);
        }
        let file_kind = self.disk_pool.persisted_file_kind();
        let mut out = Vec::with_capacity(blob_ref.byte_len as usize);
        let mut remaining = blob_ref.byte_len as usize;
        let mut page_id = blob_ref.start_page_id;
        let mut offset = blob_ref.start_offset as usize;

        while remaining > 0 {
            let guard = self
                .disk_pool
                .get_validated_page_shared(page_id, validate_persisted_blob_page)
                .await?;
            let payload = validated_blob_page_payload(guard.page());
            let header = decode_blob_page_header(payload).map_err(|err| match err {
                Error::InvalidFormat => invalid_blob_payload(file_kind, page_id),
                other => other,
            })?;
            let used_size = header.used_size as usize;
            if offset > used_size {
                return Err(invalid_blob_payload(file_kind, page_id));
            }
            let available = used_size - offset;
            if available == 0 && remaining > 0 {
                return Err(invalid_blob_payload(file_kind, page_id));
            }
            let take = remaining.min(available);
            let body_start = COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE + offset;
            let body_end = body_start + take;
            out.extend_from_slice(&payload[body_start..body_end]);
            remaining -= take;
            if remaining == 0 {
                break;
            }
            if header.next_page_id == 0 {
                return Err(invalid_blob_payload(file_kind, page_id));
            }
            page_id = header.next_page_id;
            offset = 0;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::file::build_test_fs;
    use crate::file::table_file::MutableTableFile;
    use crate::value::ValKind;
    use std::sync::Arc;

    #[test]
    fn test_blob_header_roundtrip() {
        let header = ColumnAuxBlobHeader::deletion_deltas(27).unwrap();
        let bytes = header.encode();
        let decoded = ColumnAuxBlobHeader::decode(&bytes).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_blob_header_rejects_zero_payload() {
        let bytes = [1u8, 2, 3, 4, 0, 0, 0, 0];
        assert!(matches!(
            ColumnAuxBlobHeader::decode(&bytes),
            Err(Error::InvalidFormat)
        ));
    }

    #[test]
    fn test_validate_delete_delta_header_accepts_expected_framing() {
        let header = ColumnAuxBlobHeader::deletion_deltas(27).unwrap();
        assert!(validate_delete_delta_header(&header).is_ok());
    }

    #[test]
    fn test_validate_delete_delta_header_rejects_wrong_blob_kind() {
        let header = ColumnAuxBlobHeader::new(
            2,
            COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST,
            RAW_U32_CODEC_VERSION,
            0,
            27,
        )
        .unwrap();
        assert!(matches!(
            validate_delete_delta_header(&header),
            Err(Error::InvalidFormat)
        ));
    }

    #[test]
    fn test_validate_delete_delta_header_rejects_wrong_codec_kind() {
        let header = ColumnAuxBlobHeader::new(
            COLUMN_AUX_BLOB_KIND_DELETE_DELTAS,
            2,
            RAW_U32_CODEC_VERSION,
            0,
            27,
        )
        .unwrap();
        assert!(matches!(
            validate_delete_delta_header(&header),
            Err(Error::InvalidFormat)
        ));
    }

    #[test]
    fn test_validate_delete_delta_header_rejects_wrong_codec_version() {
        let header = ColumnAuxBlobHeader::new(
            COLUMN_AUX_BLOB_KIND_DELETE_DELTAS,
            COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST,
            RAW_U32_CODEC_VERSION + 1,
            0,
            27,
        )
        .unwrap();
        assert!(matches!(
            validate_delete_delta_header(&header),
            Err(Error::InvalidFormat)
        ));
    }

    #[test]
    fn test_validate_delete_delta_header_rejects_non_zero_flags() {
        let header = ColumnAuxBlobHeader::new(
            COLUMN_AUX_BLOB_KIND_DELETE_DELTAS,
            COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST,
            RAW_U32_CODEC_VERSION,
            1,
            27,
        )
        .unwrap();
        assert!(matches!(
            validate_delete_delta_header(&header),
            Err(Error::InvalidFormat)
        ));
    }

    #[test]
    fn test_blob_writer_reader_roundtrip() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = Arc::new(TableMetadata::new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U64,
                    ColumnAttributes::empty(),
                )],
                vec![],
            ));
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);

            let mut mutable = MutableTableFile::fork(&table);
            let blob = vec![9u8; 513];
            let blob_ref = {
                let mut writer = ColumnDeletionBlobWriter::new(&mut mutable);
                let blob_ref = writer.append(&blob).await.unwrap();
                writer.finish().await.unwrap();
                blob_ref
            };
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let reader = ColumnDeletionBlobReader::new(&disk_pool);
            let (header, payload) = reader.read_framed_blob(blob_ref).await.unwrap();
            assert_eq!(header.blob_kind(), COLUMN_AUX_BLOB_KIND_DELETE_DELTAS);
            assert_eq!(header.codec_kind(), COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST);
            assert_eq!(header.codec_version(), RAW_U32_CODEC_VERSION);
            assert_eq!(payload, blob);
        });
    }

    #[test]
    fn test_blob_writer_reader_crosses_pages() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = Arc::new(TableMetadata::new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U64,
                    ColumnAttributes::empty(),
                )],
                vec![],
            ));
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);

            let mut mutable = MutableTableFile::fork(&table);
            let blob = vec![7u8; COLUMN_DELETION_BLOB_PAGE_BODY_SIZE * 2 + 113];
            let blob_ref = {
                let mut writer = ColumnDeletionBlobWriter::new(&mut mutable);
                let blob_ref = writer.append(&blob).await.unwrap();
                writer.finish().await.unwrap();
                blob_ref
            };
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let reader = ColumnDeletionBlobReader::new(&disk_pool);
            let payload = reader.read(blob_ref).await.unwrap();
            assert_eq!(payload, blob);
        });
    }

    #[test]
    fn test_blob_writer_starts_next_blob_on_fresh_page_after_exact_fill() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = Arc::new(TableMetadata::new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U64,
                    ColumnAttributes::empty(),
                )],
                vec![],
            ));
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);

            let mut mutable = MutableTableFile::fork(&table);
            let first_blob =
                vec![3u8; COLUMN_DELETION_BLOB_PAGE_BODY_SIZE - COLUMN_AUX_BLOB_HEADER_SIZE];
            let second_blob = vec![5u8; 17];
            let (first_ref, second_ref) = {
                let mut writer = ColumnDeletionBlobWriter::new(&mut mutable);
                let first_ref = writer.append(&first_blob).await.unwrap();
                let second_ref = writer.append(&second_blob).await.unwrap();
                writer.finish().await.unwrap();
                (first_ref, second_ref)
            };
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            assert_ne!(first_ref.start_page_id, 0);
            assert_eq!(first_ref.start_offset, 0);
            assert_ne!(second_ref.start_page_id, first_ref.start_page_id);
            assert_eq!(second_ref.start_offset, 0);

            let reader = ColumnDeletionBlobReader::new(&disk_pool);
            let first_payload = reader.read(first_ref).await.unwrap();
            let second_payload = reader.read(second_ref).await.unwrap();
            assert_eq!(first_payload, first_blob);
            assert_eq!(second_payload, second_blob);
        });
    }
}
