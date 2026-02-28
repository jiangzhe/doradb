use crate::buffer::ReadonlyBufferPool;
use crate::buffer::guard::PageGuard;
use crate::buffer::page::{Page, PageID};
use crate::error::{Error, Result};
use crate::file::table_file::{MutableTableFile, TABLE_FILE_PAGE_SIZE};
use crate::index::column_block_index::BlobRef;
use crate::io::DirectBuf;
use futures::future::try_join_all;

const COLUMN_DELETION_BLOB_MAGIC: [u8; 4] = *b"CDBP";
const COLUMN_DELETION_BLOB_VERSION: u8 = 1;
const COLUMN_DELETION_BLOB_NEXT_PAGE_OFFSET: usize = 8;
const COLUMN_DELETION_BLOB_USED_SIZE_OFFSET: usize = 16;
pub const COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE: usize = 20;
pub const COLUMN_DELETION_BLOB_PAGE_BODY_SIZE: usize =
    TABLE_FILE_PAGE_SIZE - COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE;

struct BlobPageHeader {
    next_page_id: PageID,
    used_size: u16,
}

fn decode_blob_page_header(page: &[u8]) -> Result<BlobPageHeader> {
    if page.len() != TABLE_FILE_PAGE_SIZE {
        return Err(Error::InvalidState);
    }
    if page[..4] != COLUMN_DELETION_BLOB_MAGIC {
        return Err(Error::InvalidFormat);
    }
    if page[4] != COLUMN_DELETION_BLOB_VERSION {
        return Err(Error::InvalidFormat);
    }
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

fn encode_blob_page_header(buf: &mut [u8], next_page_id: PageID, used_size: usize) -> Result<()> {
    if used_size > COLUMN_DELETION_BLOB_PAGE_BODY_SIZE {
        return Err(Error::InvalidArgument);
    }
    buf[..COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE].fill(0);
    buf[..4].copy_from_slice(&COLUMN_DELETION_BLOB_MAGIC);
    buf[4] = COLUMN_DELETION_BLOB_VERSION;
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
        PendingBlobPage {
            page_id,
            used_size: 0,
            buf: DirectBuf::zeroed(TABLE_FILE_PAGE_SIZE),
        }
    }

    #[inline]
    fn free_space(&self) -> usize {
        COLUMN_DELETION_BLOB_PAGE_BODY_SIZE - self.used_size
    }

    #[inline]
    fn write_bytes(&mut self, src: &[u8]) {
        debug_assert!(src.len() <= self.free_space());
        let start = COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE + self.used_size;
        let end = start + src.len();
        self.buf.data_mut()[start..end].copy_from_slice(src);
        self.used_size += src.len();
    }
}

struct SealedBlobPage {
    page: PendingBlobPage,
    next_page_id: PageID,
}

/// Append-only writer for immutable shared bitmap-blob pages.
pub struct ColumnDeletionBlobWriter<'a> {
    mutable_file: &'a mut MutableTableFile,
    current_page: Option<PendingBlobPage>,
    sealed_pages: Vec<SealedBlobPage>,
}

impl<'a> ColumnDeletionBlobWriter<'a> {
    #[inline]
    pub fn new(mutable_file: &'a mut MutableTableFile) -> Self {
        ColumnDeletionBlobWriter {
            mutable_file,
            current_page: None,
            sealed_pages: Vec::new(),
        }
    }

    /// Appends one blob and returns its reference.
    pub async fn append(&mut self, bytes: &[u8]) -> Result<BlobRef> {
        if bytes.is_empty() {
            return Err(Error::InvalidArgument);
        }
        self.ensure_current_page()?;
        let (start_page_id, start_offset) = {
            let current = self.current_page.as_ref().ok_or(Error::InvalidState)?;
            (current.page_id, current.used_size)
        };
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let free_space = self
                .current_page
                .as_ref()
                .ok_or(Error::InvalidState)?
                .free_space();
            if free_space == 0 {
                self.roll_to_next_page()?;
                continue;
            }
            let take = remaining.len().min(free_space);
            {
                let current = self.current_page.as_mut().ok_or(Error::InvalidState)?;
                current.write_bytes(&remaining[..take]);
            }
            remaining = &remaining[take..];
        }
        Ok(BlobRef {
            start_page_id,
            start_offset: start_offset as u16,
            byte_len: bytes.len() as u32,
        })
    }

    /// Flushes the tail page that still has pending blob bytes.
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
            encode_blob_page_header(
                sealed.page.buf.data_mut(),
                sealed.next_page_id,
                sealed.page.used_size,
            )?;
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
        if self.current_page.is_none() {
            let page_id = self.mutable_file.allocate_page_id()?;
            self.current_page = Some(PendingBlobPage::new(page_id));
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

/// Reader for immutable deletion blobs referenced by `BlobRef`.
pub struct ColumnDeletionBlobReader<'a> {
    disk_pool: &'a ReadonlyBufferPool,
}

impl<'a> ColumnDeletionBlobReader<'a> {
    #[inline]
    pub fn new(disk_pool: &'a ReadonlyBufferPool) -> Self {
        ColumnDeletionBlobReader { disk_pool }
    }

    /// Reads one blob by traversing linked immutable blob pages.
    pub async fn read(&self, blob_ref: BlobRef) -> Result<Vec<u8>> {
        if blob_ref.start_page_id == 0 || blob_ref.byte_len == 0 {
            return Err(Error::InvalidFormat);
        }
        let mut out = Vec::with_capacity(blob_ref.byte_len as usize);
        let mut remaining = blob_ref.byte_len as usize;
        let mut page_id = blob_ref.start_page_id;
        let mut offset = blob_ref.start_offset as usize;
        while remaining > 0 {
            let g = self.disk_pool.try_get_page_shared::<Page>(page_id).await?;
            let page = g.page();
            let header = decode_blob_page_header(page)?;
            let used_size = header.used_size as usize;
            if offset >= used_size {
                return Err(Error::InvalidFormat);
            }
            let available = used_size - offset;
            let take = available.min(remaining);
            let body_start = COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE + offset;
            let body_end = body_start + take;
            out.extend_from_slice(&page[body_start..body_end]);
            remaining -= take;
            if remaining == 0 {
                break;
            }
            if header.next_page_id == 0 {
                return Err(Error::InvalidFormat);
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
    use crate::buffer::{GlobalReadonlyBufferPool, ReadonlyBufferPool};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableID, TableMetadata,
    };
    use crate::file::table_file::TableFile;
    use crate::file::table_fs::TableFileSystemConfig;
    use crate::index::column_block_index::BlobRef;
    use crate::value::ValKind;
    use std::sync::{Arc, OnceLock};

    fn build_test_metadata() -> Arc<TableMetadata> {
        Arc::new(TableMetadata::new(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
            ],
            vec![IndexSpec::new(
                "idx1",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        ))
    }

    fn run_with_large_stack<F>(f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(f)
            .expect("spawn test thread")
            .join()
            .expect("join test thread");
    }

    fn global_readonly_pool() -> &'static GlobalReadonlyBufferPool {
        static GLOBAL: OnceLock<&'static GlobalReadonlyBufferPool> = OnceLock::new();
        *GLOBAL.get_or_init(|| {
            GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap()
        })
    }

    fn readonly_pool(table_id: TableID, table_file: &Arc<TableFile>) -> ReadonlyBufferPool {
        ReadonlyBufferPool::new(table_id, Arc::clone(table_file), global_readonly_pool())
    }

    #[test]
    fn test_blob_writer_reader_shared_page() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let _ = std::fs::remove_file("251.tbl");
                let table_file = fs
                    .create_table_file(251, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(251, &table_file);

                let blob_a = b"aaaa-bitmap-bytes".to_vec();
                let blob_b = b"bbbb-bitmap-bytes".to_vec();

                let mut mutable = MutableTableFile::fork(&table_file);
                let mut writer = ColumnDeletionBlobWriter::new(&mut mutable);
                let ref_a = writer.append(&blob_a).await.unwrap();
                let ref_b = writer.append(&blob_b).await.unwrap();
                writer.finish().await.unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                assert_eq!(ref_a.start_page_id, ref_b.start_page_id);
                assert_eq!(ref_a.start_offset, 0);
                assert_eq!(ref_b.start_offset, blob_a.len() as u16);

                let reader = ColumnDeletionBlobReader::new(&disk_pool);
                assert_eq!(reader.read(ref_a).await.unwrap(), blob_a);
                assert_eq!(reader.read(ref_b).await.unwrap(), blob_b);

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("251.tbl");
            })
        });
    }

    #[test]
    fn test_blob_writer_reader_cross_page() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let _ = std::fs::remove_file("252.tbl");
                let table_file = fs
                    .create_table_file(252, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(252, &table_file);

                let blob = vec![7u8; COLUMN_DELETION_BLOB_PAGE_BODY_SIZE + 257];
                let mut mutable = MutableTableFile::fork(&table_file);
                let mut writer = ColumnDeletionBlobWriter::new(&mut mutable);
                let blob_ref = writer.append(&blob).await.unwrap();
                writer.finish().await.unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                assert_eq!(
                    blob_ref,
                    BlobRef {
                        start_page_id: blob_ref.start_page_id,
                        start_offset: 0,
                        byte_len: blob.len() as u32
                    }
                );
                let reader = ColumnDeletionBlobReader::new(&disk_pool);
                assert_eq!(reader.read(blob_ref).await.unwrap(), blob);

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("252.tbl");
            })
        });
    }
}
