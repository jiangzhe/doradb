use crate::bitmap::AllocMap;
use crate::buffer::page::{PAGE_SIZE, PageID};
use crate::catalog::table::TableMetadata;
use crate::error::{Error, Result};
use crate::file::FixedSizeBufferFreeList;
use crate::file::meta_page::{MetaPage, MetaPageSerView};
use crate::file::super_page::{
    SUPER_PAGE_VERSION, SuperPage, SuperPageBody, SuperPageFooter, SuperPageHeader,
    SuperPageSerView,
};
use crate::file::{FileIO, FileIOResult, SparseFile};
use crate::io::DirectBuf;
use crate::io::{AIOBuf, AIOClient, AIOKind};
use crate::ptr::UnsafePtr;
use crate::row::RowID;
use crate::serde::{Deser, Ser};
use crate::trx::TrxID;
use futures::future::try_join_all;
use std::fs;
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

pub const TABLE_FILE_MAGIC_WORD: [u8; 8] = [b'D', b'O', b'R', b'A', 0, 0, 0, 0];

/// Initial size of new table file.
pub const TABLE_FILE_INITIAL_SIZE: usize = 16 * 1024 * 1024;
/// Page size of table file is 64KB.
/// This is equivalent to page size of in-memory row store
/// (row page and secondary index page).
pub const TABLE_FILE_PAGE_SIZE: usize = PAGE_SIZE;
/// Super page size of table file is 32KB.
pub const TABLE_FILE_SUPER_PAGE_SIZE: usize = TABLE_FILE_PAGE_SIZE / 2;
/// Super page header size.
pub const TABLE_FILE_SUPER_PAGE_HEADER_SIZE: usize = mem::size_of::<[u8; 8]>()
    + mem::size_of::<u64>()
    + mem::size_of::<PageID>()
    + mem::size_of::<TrxID>();
/// Super page footer: blake3 checksum + trx id.
pub const TABLE_FILE_SUPER_PAGE_FOOTER_SIZE: usize = mem::size_of::<SuperPageFooter>();
/// Super page data size.
pub const TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET: usize =
    TABLE_FILE_SUPER_PAGE_SIZE - TABLE_FILE_SUPER_PAGE_FOOTER_SIZE;

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
    pub(super) fn create(
        file_path: impl AsRef<str>,
        initial_size: usize,
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
        trunc: bool,
    ) -> Result<Self> {
        debug_assert!(initial_size.is_multiple_of(TABLE_FILE_PAGE_SIZE));
        let file = if trunc {
            SparseFile::create_or_trunc(file_path, initial_size)
        } else {
            SparseFile::create_or_fail(file_path, initial_size)
        }?;
        Ok(TableFile {
            file,
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            io_client,
            buf_list,
        })
    }

    #[inline]
    pub(super) fn open(
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
        Self::active_root_from_raw(self.load_active_root_raw())
    }

    /// Returns copy of active root.
    /// The returned pointer cannot outlive the root object,
    /// which is guaranteed by GC logic.
    #[inline]
    pub fn active_root_ptr(&self) -> AtomicPtr<ActiveRoot> {
        AtomicPtr::new(self.load_active_root_raw())
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
            FileIOResult::ReadStaticOk => panic!("invalid state"),
            FileIOResult::WriteOk => panic!("invalid state"),
            FileIOResult::Err(err) => Err(err.into()),
        }
    }

    /// Reads one table-file page directly into caller-provided memory.
    ///
    /// # Safety
    ///
    /// Caller must ensure `ptr` points to writable, sector-aligned memory
    /// of at least `TABLE_FILE_PAGE_SIZE` bytes, and remains valid until
    /// async completion.
    #[inline]
    pub async unsafe fn read_page_into_ptr(
        &self,
        page_id: PageID,
        ptr: UnsafePtr<u8>,
    ) -> Result<()> {
        let offset = page_id as usize * TABLE_FILE_PAGE_SIZE;
        // SAFETY: caller upholds pointer validity/alignment until promise resolves.
        let (fio, promise) = unsafe {
            FileIO::prepare_static_read(self.file.as_raw_fd(), offset, ptr, TABLE_FILE_PAGE_SIZE)
        };
        if self.io_client.send_async(fio).await.is_err() {
            return Err(Error::SendError);
        }
        let res = promise.wait_async().await;
        match res {
            FileIOResult::ReadStaticOk => Ok(()),
            FileIOResult::ReadOk(_) => panic!("invalid state"),
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
            FileIOResult::ReadStaticOk => panic!("invalid state"),
            FileIOResult::ReadOk(_) => panic!("invalid state"),
            FileIOResult::Err(err) => Err(err.into()),
        }
    }

    /// Replace active root with new root, and return old root.
    #[inline]
    pub fn swap_active_root(&self, active_root: ActiveRoot) -> Option<OldRoot> {
        let new = Self::allocate_active_root(active_root);
        let old = self.active_root.swap(new, Ordering::SeqCst);
        NonNull::new(old).map(OldRoot)
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
        let meta_page_id = super_page.body.meta_page_id;
        let meta_buf = self.read_page(meta_page_id).await?;
        let meta_page = self.parse_meta_page(meta_buf.as_bytes())?;
        self.buf_list.push(meta_buf);
        Ok(ActiveRoot {
            page_no: super_page.header.page_no,
            trx_id: super_page.header.checkpoint_cts,
            pivot_row_id: meta_page.pivot_row_id,
            heap_redo_start_ts: meta_page.heap_redo_start_ts,
            alloc_map: meta_page.space_map,
            gc_page_list: meta_page.gc_page_list,
            metadata: Arc::new(meta_page.schema),
            column_block_index_root: meta_page.column_block_index_root,
            meta_page_id,
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
                if r1.header.checkpoint_cts < r2.header.checkpoint_cts {
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
        let (idx, header) = SuperPageHeader::deser(buf, 0)?;
        debug_assert!(idx == TABLE_FILE_SUPER_PAGE_HEADER_SIZE);
        if header.version != SUPER_PAGE_VERSION {
            return Err(Error::InvalidFormat);
        }
        let (idx, footer) = SuperPageFooter::deser(buf, TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET)?;
        debug_assert!(idx == TABLE_FILE_SUPER_PAGE_SIZE);
        if header.checkpoint_cts != footer.checkpoint_cts {
            // torn write happens
            return Err(Error::TornWrite);
        }
        let b3sum = blake3::hash(&buf[..TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET]);
        if b3sum != footer.b3sum {
            return Err(Error::ChecksumMismatch);
        }
        let (_, body) = SuperPageBody::deser(buf, TABLE_FILE_SUPER_PAGE_HEADER_SIZE)?;
        Ok(SuperPage {
            header,
            body,
            footer,
        })
    }

    #[inline]
    fn parse_meta_page(&self, buf: &[u8]) -> Result<MetaPage> {
        let (_, meta_page) = MetaPage::deser(buf, 0)?;
        Ok(meta_page)
    }

    /// fsync to make all data written persisted to disk.
    #[inline]
    pub fn fsync(&self) {
        self.file.syncer().fsync();
    }

    #[inline]
    pub fn delete(self) {
        let _ = remove_file_by_fd(self.file.as_raw_fd());
    }

    #[inline]
    fn load_active_root_raw(&self) -> *mut ActiveRoot {
        self.active_root.load(Ordering::Relaxed)
    }

    #[inline]
    fn active_root_from_raw<'a>(ptr: *mut ActiveRoot) -> &'a ActiveRoot {
        match NonNull::new(ptr) {
            Some(ptr) => {
                // SAFETY: active root pointers are created from `Box<ActiveRoot>` and remain
                // valid until reclaimed by swap/drop.
                unsafe { ptr.as_ref() }
            }
            None => panic!("active root is not initialized"),
        }
    }

    #[inline]
    fn allocate_active_root(active_root: ActiveRoot) -> *mut ActiveRoot {
        Box::into_raw(Box::new(active_root))
    }

    #[inline]
    fn reclaim_active_root(ptr: *mut ActiveRoot) {
        if let Some(ptr) = NonNull::new(ptr) {
            // SAFETY: pointer was allocated by `allocate_active_root` and must be reclaimed once.
            unsafe {
                drop(Box::from_raw(ptr.as_ptr()));
            }
        }
    }
}

impl Drop for TableFile {
    #[inline]
    fn drop(&mut self) {
        TableFile::reclaim_active_root(self.load_active_root_raw());
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

pub struct LwcPagePersist {
    pub start_row_id: RowID,
    pub end_row_id: RowID,
    pub buf: DirectBuf,
}

impl MutableTableFile {
    /// Create new mutable table file.
    #[inline]
    pub fn new(table_file: Arc<TableFile>, active_root: ActiveRoot) -> Self {
        MutableTableFile {
            table_file,
            active_root,
        }
    }

    /// Fork the whole table file with a new root.
    #[inline]
    pub fn fork(table_file: &Arc<TableFile>) -> Self {
        MutableTableFile {
            table_file: Arc::clone(table_file),
            active_root: table_file.active_root().flip(),
        }
    }

    /// Allocate a new page id for copy-on-write updates.
    #[inline]
    pub fn allocate_page_id(&mut self) -> Result<PageID> {
        self.active_root
            .alloc_map
            .try_allocate()
            .map(|page_id| page_id as PageID)
            .ok_or(Error::InvalidState)
    }

    /// Record an obsolete page id to be reclaimed on commit.
    #[inline]
    pub fn record_gc_page(&mut self, page_id: PageID) {
        self.active_root.gc_page_list.push(page_id);
    }

    /// Commit the modification of table file.
    /// Returns the new table file and previous
    /// active root if exists.
    #[inline]
    pub async fn commit(
        self,
        trx_id: TrxID,
        try_delete_if_fail: bool,
    ) -> Result<(Arc<TableFile>, Option<OldRoot>)> {
        let MutableTableFile {
            table_file,
            mut active_root,
        } = self;
        debug_assert!(active_root.trx_id == 0 || active_root.trx_id < trx_id);
        active_root.trx_id = trx_id;

        if active_root.meta_page_id != 0 {
            active_root.gc_page_list.push(active_root.meta_page_id);
        }
        let new_meta_page_id = active_root
            .alloc_map
            .try_allocate()
            .ok_or(Error::InvalidState)? as PageID;
        active_root.meta_page_id = new_meta_page_id;

        // serialize meta page.
        let meta_page = active_root.meta_page_ser_view();
        let mut meta_buf = DirectBuf::zeroed(TABLE_FILE_PAGE_SIZE);
        let meta_len = meta_page.ser_len();
        if meta_len > TABLE_FILE_PAGE_SIZE {
            return Err(Error::InvalidState);
        }
        let meta_idx = meta_page.ser(meta_buf.as_bytes_mut(), 0);
        debug_assert!(meta_idx == meta_len);

        // write meta page down.
        match table_file.write_page(new_meta_page_id, meta_buf).await {
            Ok(_) => {}
            Err(e) => {
                if try_delete_if_fail && let Some(f) = Arc::into_inner(table_file) {
                    f.delete();
                }
                return Err(e);
            }
        }

        // serialize header and body of super page.
        let super_page = active_root.ser_view();
        let mut buf = DirectBuf::zeroed(TABLE_FILE_SUPER_PAGE_SIZE);
        let ser_len = super_page.ser_len();
        if ser_len > TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET {
            // single super page cannot hold all data
            unimplemented!("multiple pages are required to hold super data");
        }
        let ser_idx = super_page.ser(buf.as_bytes_mut(), 0);
        debug_assert!(ser_idx == ser_len);

        // serialize footer of super page.
        let b3sum = blake3::hash(&buf.as_bytes()[..TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET]);
        let footer = SuperPageFooter {
            b3sum: *b3sum.as_bytes(),
            checkpoint_cts: super_page.header.checkpoint_cts,
        };
        let ser_idx = footer.ser(buf.as_bytes_mut(), TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET);
        debug_assert!(ser_idx == TABLE_FILE_SUPER_PAGE_SIZE);

        // write page down.
        let offset = super_page.header.page_no as usize * TABLE_FILE_SUPER_PAGE_SIZE;
        match table_file.write(offset, buf, false).await {
            Ok(_) => {}
            Err(e) => {
                if try_delete_if_fail && let Some(f) = Arc::into_inner(table_file) {
                    f.delete();
                }
                return Err(e);
            }
        }

        // fsync for persistence.
        table_file.fsync();

        // swap active root at the end.
        let old_root = table_file.swap_active_root(active_root);

        Ok((table_file, old_root))
    }

    pub async fn persist_lwc_pages(
        mut self,
        lwc_pages: Vec<LwcPagePersist>,
        heap_redo_start_ts: TrxID,
        ts: TrxID,
    ) -> Result<(Arc<TableFile>, Option<OldRoot>)> {
        let mut max_row_id = self.active_root.pivot_row_id;
        let mut writes = Vec::with_capacity(lwc_pages.len());
        let mut new_entries = Vec::with_capacity(lwc_pages.len());
        let mut last_end = self.active_root.pivot_row_id;

        for page in lwc_pages {
            if page.start_row_id >= page.end_row_id {
                return Err(Error::InvalidArgument);
            }
            let page_id = self
                .active_root
                .alloc_map
                .try_allocate()
                .ok_or(Error::InvalidState)? as PageID;
            max_row_id = max_row_id.max(page.end_row_id);
            if page.start_row_id < last_end {
                return Err(Error::InvalidArgument);
            }
            last_end = page.end_row_id;
            new_entries.push((page.start_row_id, page_id as u64));
            writes.push(self.table_file.write_page(page_id, page.buf));
        }

        try_join_all(writes).await?;

        let column_index = crate::index::ColumnBlockIndex::new(
            Arc::clone(&self.table_file),
            self.active_root.column_block_index_root,
            self.active_root.pivot_row_id,
        );
        let new_root = column_index
            .batch_insert(&mut self, &new_entries, max_row_id, ts)
            .await?;
        self.active_root.column_block_index_root = new_root;
        self.active_root.pivot_row_id = max_row_id;
        self.active_root.heap_redo_start_ts = heap_redo_start_ts;

        self.commit(ts, false).await
    }

    pub async fn update_checkpoint(
        mut self,
        pivot_row_id: RowID,
        heap_redo_start_ts: TrxID,
        ts: TrxID,
    ) -> Result<(Arc<TableFile>, Option<OldRoot>)> {
        if pivot_row_id < self.active_root.pivot_row_id {
            return Err(Error::InvalidArgument);
        }
        self.active_root.pivot_row_id = pivot_row_id;
        self.active_root.heap_redo_start_ts = heap_redo_start_ts;
        self.commit(ts, false).await
    }

    #[inline]
    pub fn try_delete(self) -> bool {
        if let Some(table_file) = Arc::into_inner(self.table_file) {
            table_file.delete();
            return true;
        }
        false
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
    /// Version/Transaction ID of this table file.
    pub trx_id: TrxID,
    /// Upper bound of row id in this file.
    /// There might be gap between data in file and the bound.
    /// For example, user might insert a lot of data then
    /// delete most of them. This will cause very few rows
    /// to be transfered to LWC(LightWeight Columnar) pages, and
    /// maximum row id might be small. The upper bound is large
    /// according the first row page which is not transfered.
    pub pivot_row_id: RowID,
    /// Redo log start point for in-memory heap.
    pub heap_redo_start_ts: TrxID,
    /// Page allocation map.
    pub alloc_map: AllocMap,
    /// Pages that became obsolete in this version.
    pub gc_page_list: Vec<PageID>,
    /// Metadata of this table.
    pub metadata: Arc<TableMetadata>,
    /// Root page id of column block index.
    pub column_block_index_root: PageID,
    /// Current meta page id.
    pub meta_page_id: PageID,
    // page index (todo): this is two-layer index, persistent block index
    //                    + intra-block page index
    // secondary index (todo)
    // statistics (todo)
}

impl ActiveRoot {
    /// Create a new active root.
    /// Page number is set to zero(the first page of this file)
    #[inline]
    pub fn new(trx_id: TrxID, max_pages: usize, metadata: Arc<TableMetadata>) -> Self {
        const DEFALT_ROOT_PAGE_NO: PageID = 0;

        let alloc_map = AllocMap::new(max_pages);
        let super_page_allocated = alloc_map.allocate_at(DEFALT_ROOT_PAGE_NO as usize);
        assert!(super_page_allocated);

        ActiveRoot {
            page_no: DEFALT_ROOT_PAGE_NO,
            trx_id,
            pivot_row_id: 0,
            heap_redo_start_ts: trx_id,
            alloc_map,
            gc_page_list: vec![],
            metadata,
            column_block_index_root: 0,
            meta_page_id: 0,
        }
    }

    #[inline]
    pub fn ser_view(&self) -> SuperPageSerView {
        SuperPageSerView {
            header: SuperPageHeader {
                magic_word: TABLE_FILE_MAGIC_WORD,
                version: SUPER_PAGE_VERSION,
                page_no: self.page_no,
                checkpoint_cts: self.trx_id,
            },
            body: SuperPageBody {
                meta_page_id: self.meta_page_id,
            },
        }
    }

    #[inline]
    pub fn meta_page_ser_view(&self) -> MetaPageSerView<'_> {
        MetaPageSerView::new(
            self.metadata.ser_view(),
            self.column_block_index_root,
            &self.alloc_map,
            &self.gc_page_list,
            self.pivot_row_id,
            self.heap_redo_start_ts,
            0,
        )
    }

    #[inline]
    pub fn flip(&self) -> Self {
        let mut new = self.clone();
        // flip the page number.
        new.page_no = 1 - self.page_no;
        new
    }
}

pub struct OldRoot(NonNull<ActiveRoot>);

impl Drop for OldRoot {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: old roots are created from previous active-root pointers and reclaimed once.
        unsafe {
            drop(Box::from_raw(self.0.as_ptr()));
        }
    }
}

unsafe impl Send for OldRoot {}

#[inline]
fn remove_file_by_fd(fd: RawFd) -> std::io::Result<()> {
    let proc_path = format!("/proc/self/fd/{}", fd);
    let real_path = fs::read_link(&proc_path)?;
    fs::remove_file(real_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::error::Error;
    use crate::file::table_fs::TableFileSystemConfig;
    use crate::io::AIOBuf;
    use crate::value::ValKind;
    use tempfile::TempDir;

    #[test]
    fn test_table_file() {
        smol::block_on(async {
            let fs = TableFileSystemConfig::default().build().unwrap();
            let metadata = Arc::new(TableMetadata::new(
                vec![
                    ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                    ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
                ],
                vec![IndexSpec::new(
                    "idx1",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            ));
            let table_file = fs.create_table_file(41, metadata, false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
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

            let table_file2 = fs.open_table_file(41).await.unwrap();
            assert_eq!(table_file2.active_root().trx_id, 1);

            let mutable = MutableTableFile::fork(&table_file2);
            let (table_file3, old_root) = mutable.commit(2, false).await.unwrap();
            drop(old_root);
            let active_root = table_file3.load_active_root().await.unwrap();
            assert_eq!(active_root.page_no, 1);
            assert_eq!(active_root.trx_id, 2);
            drop(table_file2);
            drop(table_file3);

            drop(fs);
            let _ = std::fs::remove_file("41.tbl");
        });
    }

    #[test]
    fn test_table_file_system() {
        smol::block_on(async {
            let fs = TableFileSystemConfig::default().build().unwrap();
            let metadata = Arc::new(TableMetadata::new(
                vec![
                    ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                    ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
                ],
                vec![IndexSpec::new(
                    "idx1",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            ));
            let table_file = fs.create_table_file(42, metadata, false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            assert!(old_root.is_none());
            assert_eq!(table_file.active_root().page_no, 0);
            assert_eq!(table_file.active_root().trx_id, 1);

            // We first drop file system, then send the IO request,
            // it should fail.
            drop(fs);

            let res = table_file.read_page(1).await;
            assert!(res.is_err());

            drop(table_file);
            let _ = std::fs::remove_file("42.tbl");
        });
    }

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

    fn page_buf(payload: &[u8]) -> DirectBuf {
        let mut buf = DirectBuf::zeroed(TABLE_FILE_PAGE_SIZE);
        buf.data_mut()[..payload.len()].copy_from_slice(payload);
        buf
    }

    #[test]
    fn test_persist_lwc_pages_appends_entries() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let metadata = build_test_metadata();
            let table_file = fs.create_table_file(43, metadata, false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let lwc_pages = vec![
                LwcPagePersist {
                    start_row_id: 0,
                    end_row_id: 10,
                    buf: page_buf(b"lwc-page-1"),
                },
                LwcPagePersist {
                    start_row_id: 10,
                    end_row_id: 20,
                    buf: page_buf(b"lwc-page-2"),
                },
            ];

            let (table_file, old_root) = MutableTableFile::fork(&table_file)
                .persist_lwc_pages(lwc_pages, 7, 2)
                .await
                .unwrap();
            drop(old_root);

            let active_root = table_file.active_root();
            assert_eq!(active_root.trx_id, 2);
            assert_eq!(active_root.pivot_row_id, 20);
            assert_eq!(active_root.heap_redo_start_ts, 7);
            assert_ne!(active_root.column_block_index_root, 0);

            let column_index = crate::index::ColumnBlockIndex::new(
                Arc::clone(&table_file),
                active_root.column_block_index_root,
                active_root.pivot_row_id,
            );
            let payload1 = column_index.find(0).await.unwrap().unwrap();
            let payload2 = column_index.find(15).await.unwrap().unwrap();
            let page1 = table_file.read_page(payload1.block_id).await.unwrap();
            let page2 = table_file.read_page(payload2.block_id).await.unwrap();
            assert_eq!(&page1.as_bytes()[..10], b"lwc-page-1");
            assert_eq!(&page2.as_bytes()[..10], b"lwc-page-2");

            drop(table_file);
            drop(fs);
            let _ = std::fs::remove_file("43.tbl");
        });
    }

    #[test]
    fn test_persist_lwc_pages_rejects_overlapping_ranges() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let metadata = build_test_metadata();
            let table_file = fs.create_table_file(44, metadata, false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let lwc_pages = vec![
                LwcPagePersist {
                    start_row_id: 0,
                    end_row_id: 10,
                    buf: page_buf(b"lwc-overlap-1"),
                },
                LwcPagePersist {
                    start_row_id: 5,
                    end_row_id: 15,
                    buf: page_buf(b"lwc-overlap-2"),
                },
            ];

            let result = MutableTableFile::fork(&table_file)
                .persist_lwc_pages(lwc_pages, 7, 2)
                .await;

            assert!(matches!(result, Err(Error::InvalidArgument)));
            let active_root = table_file.active_root();
            assert_eq!(active_root.pivot_row_id, 0);
            assert_eq!(active_root.column_block_index_root, 0);

            drop(table_file);
            drop(fs);
            let _ = std::fs::remove_file("44.tbl");
        });
    }
}
