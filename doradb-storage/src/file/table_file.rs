use crate::bitmap::AllocMap;
use crate::buffer::page::PageID;
use crate::catalog::table::TableMetadata;
use crate::error::{Error, Result};
use crate::file::super_page::{
    SuperPage, SuperPageAlloc, SuperPageBody, SuperPageBodySerView, SuperPageFooter,
    SuperPageHeader, SuperPageMeta, SuperPageSerView,
};
use crate::file::{FileIO, FileIOListener, FileIOResult, SparseFile};
use crate::free_list::FreeList;
use crate::io::DirectBuf;
use crate::io::{AIOBuf, AIOClient, AIOContext, AIOKind, STORAGE_SECTOR_SIZE};
use crate::serde::{Deser, Ser, SerdeCtx};
use crate::trx::TrxID;
use std::mem;
use std::ops::Deref;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::thread::JoinHandle;

pub const TABLE_FILE_MAGIC_WORD: [u8; 8] = [b'D', b'O', b'R', b'A', 0, 0, 0, 0];

/// Page size of table file is 256KB.
pub const TABLE_FILE_PAGE_SIZE: usize = 256 * 1024;
/// Super page size of table file is 128KB.
pub const TABLE_FILE_SUPER_PAGE_SIZE: usize = 128 * 1024;
/// Super page blake3 checksum size is 32B.
pub const TABLE_FILE_SUPER_PAGE_HEADER_SIZE: usize = mem::size_of::<SuperPageHeader>();
/// Super page footer: blake3 checksum + trx id.
pub const TABLE_FILE_SUPER_PAGE_FOOTER_SIZE: usize = mem::size_of::<SuperPageFooter>();
/// Super page data size.
pub const TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET: usize =
    TABLE_FILE_SUPER_PAGE_SIZE - TABLE_FILE_SUPER_PAGE_FOOTER_SIZE;

/// TableFileSystem provides functionalities including
/// creating, opening, closing and removing table files.
pub struct TableFileSystem {
    io_client: AIOClient<FileIO>,
    handle: Option<JoinHandle<()>>,
    buf_list: FixedSizeBufferFreeList,
}

impl TableFileSystem {
    /// Create a new table file system.
    #[inline]
    pub fn new(io_depth: usize) -> Result<Self> {
        let ctx = AIOContext::new(io_depth)?;
        let buf_list = FixedSizeBufferFreeList::new(TABLE_FILE_PAGE_SIZE, io_depth, io_depth * 2);
        let (event_loop, io_client) = ctx.event_loop();
        let listener = FileIOListener::new(buf_list.clone());
        let handle = event_loop.start_thread(listener);
        Ok(TableFileSystem {
            io_client,
            handle: Some(handle),
            buf_list,
        })
    }

    /// Create a new table file.
    #[inline]
    pub fn create_table_file(
        &self,
        path: impl AsRef<str>,
        max_size: usize,
        trx_id: TrxID,
        metadata: TableMetadata,
    ) -> Result<MutableTableFile> {
        let table_file = TableFile::new(
            path,
            max_size,
            self.io_client.clone(),
            self.buf_list.clone(),
        )?;
        let initial_pages = max_size / TABLE_FILE_PAGE_SIZE;
        let active_root = ActiveRoot::new(trx_id, initial_pages, metadata);
        Ok(MutableTableFile {
            table_file: Arc::new(table_file),
            active_root,
        })
    }

    /// Open an existing table file.
    #[inline]
    pub async fn open_table_file(&self, path: impl AsRef<str>) -> Result<Arc<TableFile>> {
        let table_file = TableFile::open(path, self.io_client.clone(), self.buf_list.clone())?;
        let active_root = table_file.load_active_root().await?;
        let old_root = table_file.swap_active_root(active_root);
        debug_assert!(old_root.is_none());
        Ok(Arc::new(table_file))
    }
}

impl Drop for TableFileSystem {
    #[inline]
    fn drop(&mut self) {
        self.io_client.shutdown();
        self.handle.take().unwrap().join().unwrap();
    }
}

/// Table file is a wrapper of file, IO channel and cache.
pub struct TableFile {
    /// Underlying sparse file storing table metadata and data.
    file: SparseFile,
    /// Active root of this table file.
    /// This root can be switched once a modification of
    /// the file is committed.
    active_root: AtomicPtr<ActiveRoot>,
    /// Client to submit IO reads/writes.
    io_client: AIOClient<FileIO>,
    /// Reusable buffer list.
    buf_list: FixedSizeBufferFreeList,
}

impl TableFile {
    /// Create a table file.
    #[inline]
    fn new(
        file_path: impl AsRef<str>,
        initial_size: usize,
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
    ) -> Result<Self> {
        debug_assert!(initial_size.is_multiple_of(TABLE_FILE_PAGE_SIZE));
        let file = SparseFile::create(file_path, initial_size)?;
        Ok(TableFile {
            file,
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            io_client,
            buf_list,
        })
    }

    #[inline]
    fn open(
        file_path: impl AsRef<str>,
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
    ) -> Result<Self> {
        let file = SparseFile::open(file_path)?;
        Ok(TableFile {
            file,
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            io_client,
            buf_list,
        })
    }

    #[inline]
    pub fn buf_list(&self) -> &FixedSizeBufferFreeList {
        &self.buf_list
    }

    /// Returns active root of the table file.
    #[inline]
    pub fn active_root(&self) -> &ActiveRoot {
        let ptr = self.active_root.load(Ordering::Relaxed);
        unsafe { &*ptr }
    }

    #[inline]
    pub async fn read_page(&self, page_id: PageID) -> Result<DirectBuf> {
        let buf = self.buf_list.pop_async(true).await;
        debug_assert!(buf.capacity() == TABLE_FILE_PAGE_SIZE);
        let offset = page_id as usize * TABLE_FILE_PAGE_SIZE;
        let (fio, promise) =
            FileIO::prepare(AIOKind::Read, self.file.as_raw_fd(), offset, buf, true);
        if let Err(err) = self.io_client.send_async(fio).await {
            if let Some(buf) = err.into_inner().take_buf() {
                self.buf_list.recycle(buf);
            }
            return Err(Error::SendError);
        }
        let res = promise.wait_async().await;
        match res {
            FileIOResult::ReadOk(buf) => Ok(buf),
            FileIOResult::WriteOk => panic!("invalid state"),
            FileIOResult::Err(err) => Err(err.into()),
        }
    }

    #[inline]
    pub async fn write_page(&self, page_id: PageID, buf: DirectBuf) -> Result<()> {
        debug_assert!(buf.capacity() == TABLE_FILE_PAGE_SIZE);
        let offset = page_id as usize * TABLE_FILE_PAGE_SIZE;
        self.write(offset, buf, true).await
    }

    #[inline]
    async fn write(&self, offset: usize, buf: DirectBuf, recycle: bool) -> Result<()> {
        let (fio, promise) =
            FileIO::prepare(AIOKind::Write, self.file.as_raw_fd(), offset, buf, recycle);
        if let Err(err) = self.io_client.send_async(fio).await {
            if let Some(buf) = err.into_inner().take_buf()
                && recycle
            {
                self.buf_list.recycle(buf);
            }
            return Err(Error::SendError);
        }
        let res = promise.wait_async().await;
        match res {
            FileIOResult::WriteOk => Ok(()),
            FileIOResult::ReadOk(_) => panic!("invalid state"),
            FileIOResult::Err(err) => Err(err.into()),
        }
    }

    /// Replace active root with new root, and return old root.
    #[inline]
    pub fn swap_active_root(&self, active_root: ActiveRoot) -> Option<OldRoot> {
        let new = Box::leak(Box::new(active_root)) as *mut ActiveRoot;
        let old = self.active_root.swap(new, Ordering::SeqCst);
        if old.is_null() {
            return None;
        }
        Some(OldRoot(old))
    }

    /// Load active root from two super pages.
    /// The page which passes validation and has larger transaction id
    /// will win.
    #[inline]
    pub async fn load_active_root(&self) -> Result<ActiveRoot> {
        // First page contains two(ping-pong) super pages.
        let buf = self.read_page(0).await?;
        let super_page = self.pick_super_page(buf.as_bytes())?;
        self.buf_list.push(buf);
        let alloc_map = match super_page.body.alloc {
            SuperPageAlloc::Inline(alloc) => alloc,
            SuperPageAlloc::PageNo(_) => todo!("standalone alloc page"),
        };
        let metadata = match super_page.body.meta {
            SuperPageMeta::Inline(meta) => meta,
            SuperPageMeta::PageNo(_) => todo!("standalone metadata page"),
        };
        Ok(ActiveRoot {
            page_no: super_page.header.page_no,
            trx_id: super_page.header.trx_id,
            alloc_map,
            metadata,
        })
    }

    #[inline]
    fn pick_super_page(&self, buf: &[u8]) -> Result<SuperPage> {
        debug_assert!(buf.len() == TABLE_FILE_SUPER_PAGE_SIZE * 2);
        let first = self.parse_super_page(&buf[..TABLE_FILE_SUPER_PAGE_SIZE]);
        let second = self.parse_super_page(&buf[TABLE_FILE_SUPER_PAGE_SIZE..]);
        match (first, second) {
            (Err(err), Err(_)) => Err(err),
            (Ok(root), Err(_)) | (Err(_), Ok(root)) => Ok(root),
            (Ok(r1), Ok(r2)) => {
                // pick the one with larger transaction id.
                if r1.header.trx_id < r2.header.trx_id {
                    Ok(r2)
                } else {
                    Ok(r1)
                }
            }
        }
    }

    #[inline]
    fn parse_super_page(&self, buf: &[u8]) -> Result<SuperPage> {
        // first we extract and validate checksum and transaction id.
        let mut ctx = SerdeCtx::default();
        let (idx, header) = SuperPageHeader::deser(&mut ctx, buf, 0)?;
        debug_assert!(idx == TABLE_FILE_SUPER_PAGE_HEADER_SIZE);
        let (idx, footer) =
            SuperPageFooter::deser(&mut ctx, buf, TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET)?;
        debug_assert!(idx == TABLE_FILE_SUPER_PAGE_SIZE);
        if header.trx_id != footer.trx_id {
            // torn write happens
            return Err(Error::TornWrite);
        }
        let b3sum = blake3::hash(&buf[..TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET]);
        if b3sum != footer.b3sum {
            return Err(Error::ChecksumMismatch);
        }
        let (_, body) = SuperPageBody::deser(&mut ctx, buf, TABLE_FILE_SUPER_PAGE_HEADER_SIZE)?;
        Ok(SuperPage {
            header,
            body,
            footer,
        })
    }

    /// fsync to make all data written persisted to disk.
    #[inline]
    pub fn fsync(&self) {
        self.file.syncer().fsync();
    }
}

impl Drop for TableFile {
    #[inline]
    fn drop(&mut self) {
        let active_root_ptr = self.active_root.load(Ordering::Relaxed);
        unsafe {
            drop(Box::from_raw(active_root_ptr));
        }
    }
}

/// MutableTableFile represents a table file being modified.
/// It's safe to share the original table file with other threads
/// because the modification is done in Copy-on-Write way.
/// All changes will be committed once the new active root
/// is persisted to disk.
pub struct MutableTableFile {
    table_file: Arc<TableFile>,
    active_root: ActiveRoot,
}

impl MutableTableFile {
    /// Fork the whole table file with a new root.
    #[inline]
    pub fn fork(table_file: &Arc<TableFile>, trx_id: TrxID) -> Self {
        MutableTableFile {
            table_file: Arc::clone(table_file),
            active_root: table_file.active_root().fork(trx_id),
        }
    }

    /// Commit the modification of table file.
    /// Returns the new table file and previous
    /// active root if exists.
    #[inline]
    pub async fn commit(self) -> Result<(Arc<TableFile>, Option<OldRoot>)> {
        let MutableTableFile {
            table_file,
            active_root,
        } = self;

        // serialize header and body of super page.
        let super_page = active_root.ser_view();
        let mut buf = DirectBuf::zeroed(TABLE_FILE_SUPER_PAGE_SIZE);
        let ctx = SerdeCtx::default();
        let ser_len = super_page.ser_len(&ctx);
        if ser_len > TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET {
            // single super page cannot hold all data
            unimplemented!("multiple pages are required to hold super data");
        }
        let ser_idx = super_page.ser(&ctx, buf.as_bytes_mut(), 0);
        debug_assert!(ser_idx == ser_len);

        // serialize footer of super page.
        let b3sum = blake3::hash(&buf.as_bytes()[..TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET]);
        let footer = SuperPageFooter {
            b3sum: *b3sum.as_bytes(),
            trx_id: super_page.header.trx_id,
        };
        let ser_idx = footer.ser(
            &ctx,
            buf.as_bytes_mut(),
            TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET,
        );
        debug_assert!(ser_idx == TABLE_FILE_SUPER_PAGE_SIZE);

        // write page down.
        let offset = super_page.header.page_no as usize * TABLE_FILE_SUPER_PAGE_SIZE;
        table_file.write(offset, buf, false).await?;

        // fsync for persistence.
        table_file.fsync();

        // swap active root at the end.
        let old_root = table_file.swap_active_root(active_root);

        Ok((table_file, old_root))
    }
}

/// Fixed size buffer free list hold a given number of
/// buffer pages.
/// It's used to reuse pages in heavy IO environment.
#[derive(Clone)]
pub struct FixedSizeBufferFreeList(Arc<FreeList<DirectBuf>>);

impl FixedSizeBufferFreeList {
    /// Create a new buffer free list with given number of
    /// pre-allocated buffer pages.
    #[inline]
    pub fn new(page_size: usize, init_pages: usize, max_pages: usize) -> Self {
        debug_assert!(page_size.is_multiple_of(STORAGE_SECTOR_SIZE));
        let free_list: FreeList<_> = FreeList::new(init_pages, max_pages, move || {
            let mut buf = DirectBuf::zeroed(page_size);
            buf.truncate(0);
            buf
        });
        FixedSizeBufferFreeList(Arc::new(free_list))
    }

    /// Recycle the buffer for future use.
    #[inline]
    pub fn recycle(&self, mut buf: DirectBuf) {
        buf.reset();
        buf.truncate(0);
        self.push(buf);
    }
}

impl Deref for FixedSizeBufferFreeList {
    type Target = FreeList<DirectBuf>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Active root of table file.
/// It contains bitmap of allocated pages,
/// table schema(metadata), column index and statistics.
#[derive(Clone)]
pub struct ActiveRoot {
    /// root page number.
    /// Can be either 0 or 1.
    pub page_no: u64,
    /// Version of this table file.
    pub trx_id: TrxID,
    /// Page allocation map.
    pub alloc_map: AllocMap,
    /// Metadata of this table.
    pub metadata: TableMetadata,
    // page index (todo): this is two-layer index, persistent block index
    //                    + intra-block page index
    // secondary index (todo)
    // statistics (todo)
}

impl ActiveRoot {
    /// Create a new active root.
    /// Page number is set to zero(the first page of this file)
    #[inline]
    pub fn new(trx_id: TrxID, max_pages: usize, metadata: TableMetadata) -> Self {
        const DEFALT_ROOT_PAGE_NO: PageID = 0;

        let alloc_map = AllocMap::new(max_pages);
        let super_page_allocated = alloc_map.allocate_at(DEFALT_ROOT_PAGE_NO as usize);
        assert!(super_page_allocated);

        ActiveRoot {
            page_no: DEFALT_ROOT_PAGE_NO,
            trx_id,
            alloc_map,
            metadata,
        }
    }

    #[inline]
    pub fn ser_view(&self) -> SuperPageSerView<'_> {
        SuperPageSerView {
            header: SuperPageHeader {
                magic_word: TABLE_FILE_MAGIC_WORD,
                page_no: self.page_no,
                trx_id: self.trx_id,
            },
            body: SuperPageBodySerView::new(&self.alloc_map, self.metadata.ser_view()),
        }
    }

    #[inline]
    pub fn fork(&self, trx_id: TrxID) -> Self {
        let mut new = self.clone();
        new.trx_id = trx_id;
        // flip the page number.
        new.page_no = 1 - self.page_no;
        new
    }
}

pub struct OldRoot(*mut ActiveRoot);

impl Drop for OldRoot {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let res = Box::from_raw(self.0);
            drop(res);
        }
    }
}

unsafe impl Send for OldRoot {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::AIOBuf;
    use doradb_catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use doradb_datatype::PreciseType;

    #[test]
    fn test_table_file() {
        smol::block_on(async {
            let fs = TableFileSystem::new(32).unwrap();
            let metadata = TableMetadata::new(
                vec![
                    ColumnSpec::new("c0", PreciseType::Int(4, true), ColumnAttributes::empty()),
                    ColumnSpec::new("c1", PreciseType::Int(8, true), ColumnAttributes::NULLABLE),
                ],
                vec![IndexSpec::new(
                    "idx1",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            );
            let table_file = fs
                .create_table_file("table_file1.tbl", 64 * 1024 * 1024, 1, metadata)
                .unwrap();
            let (table_file, old_root) = table_file.commit().await.unwrap();
            assert!(old_root.is_none());
            assert_eq!(table_file.active_root().page_no, 0);
            assert_eq!(table_file.active_root().trx_id, 1);

            // write
            let mut buf = table_file.buf_list().pop_async(true).await;
            buf.extend_from_slice(b"hello, world");
            let res = table_file.write_page(3, buf).await;
            assert!(res.is_ok());

            let res = table_file.read_page(3).await;
            assert!(res.is_ok());
            let buf = res.unwrap();
            assert_eq!(&buf.as_bytes()[..12], b"hello, world");

            drop(table_file);

            let table_file2 = fs.open_table_file("table_file1.tbl").await.unwrap();
            assert_eq!(table_file2.active_root().trx_id, 1);

            let mutable = MutableTableFile::fork(&table_file2, 2);
            let (table_file3, old_root) = mutable.commit().await.unwrap();
            drop(old_root);
            let active_root = table_file3.load_active_root().await.unwrap();
            assert_eq!(active_root.page_no, 1);
            assert_eq!(active_root.trx_id, 2);
            drop(table_file2);
            drop(table_file3);

            drop(fs);
            let _ = std::fs::remove_file("table_file1.tbl");
        });
    }

    #[test]
    fn test_table_file_system() {
        smol::block_on(async {
            let fs = TableFileSystem::new(32).unwrap();
            let metadata = TableMetadata::new(
                vec![
                    ColumnSpec::new("c0", PreciseType::Int(4, true), ColumnAttributes::empty()),
                    ColumnSpec::new("c1", PreciseType::Int(8, true), ColumnAttributes::NULLABLE),
                ],
                vec![IndexSpec::new(
                    "idx1",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            );
            let table_file = fs
                .create_table_file("table_file2.tbl", 64 * 1024 * 1024, 1, metadata)
                .unwrap();
            let (table_file, old_root) = table_file.commit().await.unwrap();
            assert!(old_root.is_none());
            assert_eq!(table_file.active_root().page_no, 0);
            assert_eq!(table_file.active_root().trx_id, 1);

            // We first drop file system, then send the IO request,
            // it should fail.
            drop(fs);

            let res = table_file.read_page(1).await;
            assert!(res.is_err());

            drop(table_file);
            let _ = std::fs::remove_file("table_file2.tbl");
        });
    }
}
