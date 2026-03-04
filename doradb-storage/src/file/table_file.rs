use crate::bitmap::AllocMap;
use crate::buffer::ReadonlyBufferPool;
use crate::buffer::page::PageID;
use crate::catalog::table::TableMetadata;
use crate::error::{Error, Result};
use crate::file::FileIO;
use crate::file::FixedSizeBufferFreeList;
use crate::file::cow_file::{
    COW_FILE_PAGE_SIZE, CoWFile, CoWFileOwner, CoWRoot, MutableCoWFile, OldCoWRoot,
};
use crate::file::meta_page::{MetaPage, MetaPageSerView};
use crate::file::super_page::{
    SUPER_PAGE_VERSION, SuperPage, SuperPageBody, SuperPageFooter, SuperPageHeader,
    SuperPageSerView, parse_super_page, pick_latest_valid_super_page,
};
use crate::io::{AIOBuf, AIOClient, DirectBuf};
use crate::ptr::UnsafePtr;
use crate::row::RowID;
use crate::serde::{Deser, Ser};
use crate::trx::TrxID;
use futures::future::try_join_all;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub const TABLE_FILE_MAGIC_WORD: [u8; 8] = [b'D', b'O', b'R', b'A', 0, 0, 0, 0];

/// Initial size of new table file.
pub const TABLE_FILE_INITIAL_SIZE: usize = 16 * 1024 * 1024;
/// Super page size of table file is 32KB.
pub const TABLE_FILE_SUPER_PAGE_SIZE: usize = COW_FILE_PAGE_SIZE / 2;
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

/// Table file wrapper over generic copy-on-write file mechanics.
pub struct TableFile(CoWFile<ActiveRoot>);

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
        debug_assert!(initial_size.is_multiple_of(COW_FILE_PAGE_SIZE));
        let cow_file = CoWFile::create(file_path, initial_size, io_client, buf_list, trunc)?;
        Ok(TableFile(cow_file))
    }

    #[inline]
    pub(super) fn open(
        file_path: impl AsRef<str>,
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
    ) -> Result<Self> {
        let cow_file = CoWFile::open(file_path, io_client, buf_list)?;
        Ok(TableFile(cow_file))
    }

    #[inline]
    pub async fn read_page(&self, page_id: PageID) -> Result<DirectBuf> {
        self.0.read_page(page_id).await
    }

    /// Reads one table-file page directly into caller-provided memory.
    ///
    /// # Safety
    ///
    /// Caller must ensure `ptr` points to writable, sector-aligned memory
    /// of at least `COW_FILE_PAGE_SIZE` bytes, and remains valid until
    /// async completion.
    #[inline]
    pub async unsafe fn read_page_into_ptr(
        &self,
        page_id: PageID,
        ptr: UnsafePtr<u8>,
    ) -> Result<()> {
        // SAFETY: caller upholds pointer validity/alignment until async completion.
        unsafe { self.0.read_page_into_ptr(page_id, ptr).await }
    }

    #[inline]
    pub async fn write_page(&self, page_id: PageID, buf: DirectBuf) -> Result<()> {
        self.0.write_page(page_id, buf).await
    }

    #[inline]
    pub fn delete(self) {
        self.0.delete();
    }
}

impl Deref for TableFile {
    type Target = CoWFile<ActiveRoot>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl CoWFileOwner<ActiveRoot> for TableFile {
    #[inline]
    fn cow_file(&self) -> &CoWFile<ActiveRoot> {
        &self.0
    }
}

/// MutableTableFile represents a table file being modified.
/// It's safe to share the original table file with other threads
/// because the modification is done in Copy-on-Write way.
/// All changes will be committed once the new active root
/// is persisted to disk.
pub struct MutableTableFile {
    cow_file: MutableCoWFile<TableFile, ActiveRoot>,
}

impl Deref for MutableTableFile {
    type Target = MutableCoWFile<TableFile, ActiveRoot>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cow_file
    }
}

impl DerefMut for MutableTableFile {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cow_file
    }
}

pub struct LwcPagePersist {
    pub start_row_id: RowID,
    pub end_row_id: RowID,
    pub buf: DirectBuf,
}

impl MutableTableFile {
    /// Create new mutable table file.
    #[inline]
    pub fn new(table_file: Arc<TableFile>, new_root: ActiveRoot) -> Self {
        MutableTableFile {
            cow_file: MutableCoWFile::new(table_file, new_root),
        }
    }

    /// Fork the whole table file with a new root.
    #[inline]
    pub fn fork(table_file: &Arc<TableFile>) -> Self {
        MutableTableFile {
            cow_file: MutableCoWFile::fork(table_file),
        }
    }

    /// Updates mutable root column-index pointer.
    #[inline]
    pub fn set_column_block_index_root(&mut self, root_page_id: PageID) {
        self.cow_file.root_mut().column_block_index_root = root_page_id;
    }

    /// Updates mutable root checkpoint metadata.
    #[inline]
    pub fn apply_checkpoint_metadata(
        &mut self,
        pivot_row_id: RowID,
        heap_redo_start_ts: TrxID,
    ) -> Result<()> {
        let root = self.cow_file.root_mut();
        if pivot_row_id < root.pivot_row_id {
            return Err(Error::InvalidArgument);
        }
        root.pivot_row_id = pivot_row_id;
        root.heap_redo_start_ts = heap_redo_start_ts;
        Ok(())
    }

    /// Allocate a new page id for copy-on-write updates.
    #[inline]
    pub fn allocate_page_id(&mut self) -> Result<PageID> {
        self.cow_file
            .root_mut()
            .alloc_map
            .try_allocate()
            .map(|page_id| page_id as PageID)
            .ok_or(Error::InvalidState)
    }

    /// Record an obsolete page id to be reclaimed on commit.
    #[inline]
    pub fn record_gc_page(&mut self, page_id: PageID) {
        self.cow_file.root_mut().gc_page_list.push(page_id);
    }

    /// Write one page into the underlying table file.
    #[inline]
    pub async fn write_page(&self, page_id: PageID, buf: DirectBuf) -> Result<()> {
        self.cow_file.file().write_page(page_id, buf).await
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
        let mut cow_file = self.cow_file;
        {
            let new_root = cow_file.root_mut();
            debug_assert!(new_root.trx_id == 0 || new_root.trx_id < trx_id);
            new_root.trx_id = trx_id;
        }

        match cow_file.commit().await {
            Ok((table_file, old_root)) => Ok((table_file, old_root)),
            Err((table_file, err)) => {
                if try_delete_if_fail && let Some(file) = Arc::into_inner(table_file) {
                    file.delete();
                }
                Err(err)
            }
        }
    }

    pub async fn apply_lwc_pages(
        &mut self,
        lwc_pages: Vec<LwcPagePersist>,
        heap_redo_start_ts: TrxID,
        ts: TrxID,
        disk_pool: &ReadonlyBufferPool,
    ) -> Result<()> {
        let table_file = Arc::clone(self.cow_file.file());
        let mut max_row_id = self.cow_file.root().pivot_row_id;
        let mut writes = Vec::with_capacity(lwc_pages.len());
        let mut new_entries = Vec::with_capacity(lwc_pages.len());
        let mut last_end = self.cow_file.root().pivot_row_id;

        for page in lwc_pages {
            if page.start_row_id >= page.end_row_id {
                return Err(Error::InvalidArgument);
            }
            let page_id = self
                .cow_file
                .root_mut()
                .alloc_map
                .try_allocate()
                .ok_or(Error::InvalidState)? as PageID;
            max_row_id = max_row_id.max(page.end_row_id);
            if page.start_row_id < last_end {
                return Err(Error::InvalidArgument);
            }
            last_end = page.end_row_id;
            new_entries.push((page.start_row_id, page_id as u64));
            writes.push(table_file.write_page(page_id, page.buf));
        }

        try_join_all(writes).await?;

        let root = self.cow_file.root();
        let column_index = crate::index::ColumnBlockIndex::new(
            root.column_block_index_root,
            root.pivot_row_id,
            disk_pool,
        );
        let new_root = column_index
            .batch_insert(self, &new_entries, max_row_id, ts)
            .await?;
        let root = self.cow_file.root_mut();
        root.column_block_index_root = new_root;
        root.pivot_row_id = max_row_id;
        root.heap_redo_start_ts = heap_redo_start_ts;
        Ok(())
    }

    pub async fn persist_lwc_pages(
        mut self,
        lwc_pages: Vec<LwcPagePersist>,
        heap_redo_start_ts: TrxID,
        ts: TrxID,
        disk_pool: &ReadonlyBufferPool,
    ) -> Result<(Arc<TableFile>, Option<OldRoot>)> {
        self.apply_lwc_pages(lwc_pages, heap_redo_start_ts, ts, disk_pool)
            .await?;
        self.commit(ts, false).await
    }

    /// Updates checkpoint metadata in the mutable root and commits it as a new table-file root.
    ///
    /// This method consumes `self`; on `Err`, the caller must treat the failed mutable instance
    /// as discarded and start from `MutableTableFile::fork` again.
    pub async fn update_checkpoint(
        mut self,
        pivot_row_id: RowID,
        heap_redo_start_ts: TrxID,
        ts: TrxID,
    ) -> Result<(Arc<TableFile>, Option<OldRoot>)> {
        self.apply_checkpoint_metadata(pivot_row_id, heap_redo_start_ts)?;
        self.commit(ts, false).await
    }

    #[inline]
    pub fn try_delete(self) -> bool {
        if let Some(table_file) = Arc::into_inner(self.cow_file.into_file()) {
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

impl CoWRoot for ActiveRoot {
    type Meta = MetaPage;

    #[inline]
    fn flip(&self) -> Self {
        ActiveRoot::flip(self)
    }

    #[inline]
    fn page_no(&self) -> u64 {
        self.page_no
    }

    #[inline]
    fn meta_page_id(&self) -> u64 {
        self.meta_page_id
    }

    #[inline]
    fn set_meta_page_id(&mut self, page_id: u64) {
        self.meta_page_id = page_id;
    }

    #[inline]
    fn push_gc_meta_page(&mut self, page_id: u64) {
        self.gc_page_list.push(page_id);
    }

    #[inline]
    fn try_allocate_page_id(&mut self) -> Option<u64> {
        self.alloc_map.try_allocate().map(|page_id| page_id as u64)
    }

    #[inline]
    fn pick_super_page(buf: &[u8]) -> Result<SuperPage> {
        pick_latest_valid_super_page(buf, TABLE_FILE_SUPER_PAGE_SIZE, |super_page_buf| {
            parse_super_page(
                super_page_buf,
                TABLE_FILE_MAGIC_WORD,
                SUPER_PAGE_VERSION,
                TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET,
            )
        })
    }

    #[inline]
    fn parse_meta_page(buf: &[u8]) -> Result<Self::Meta> {
        let (_, meta_page) = MetaPage::deser(buf, 0)?;
        Ok(meta_page)
    }

    #[inline]
    fn from_loaded(super_page: SuperPage, meta_page: Self::Meta) -> Result<Self> {
        Ok(ActiveRoot {
            page_no: super_page.header.page_no,
            trx_id: super_page.header.checkpoint_cts,
            pivot_row_id: meta_page.pivot_row_id,
            heap_redo_start_ts: meta_page.heap_redo_start_ts,
            alloc_map: meta_page.space_map,
            gc_page_list: meta_page.gc_page_list,
            metadata: Arc::new(meta_page.schema),
            column_block_index_root: meta_page.column_block_index_root,
            meta_page_id: super_page.body.meta_page_id,
        })
    }

    #[inline]
    fn build_meta_page(&self) -> Result<DirectBuf> {
        let meta_page = self.meta_page_ser_view();
        let mut meta_buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        let meta_len = meta_page.ser_len();
        if meta_len > COW_FILE_PAGE_SIZE {
            return Err(Error::InvalidState);
        }
        let meta_idx = meta_page.ser(meta_buf.as_bytes_mut(), 0);
        debug_assert_eq!(meta_idx, meta_len);
        Ok(meta_buf)
    }

    #[inline]
    fn build_super_page(&self) -> Result<DirectBuf> {
        let super_page = self.ser_view();
        let mut buf = DirectBuf::zeroed(TABLE_FILE_SUPER_PAGE_SIZE);
        let ser_len = super_page.ser_len();
        if ser_len > TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET {
            // single super page cannot hold all data
            unimplemented!("multiple pages are required to hold super data");
        }
        let ser_idx = super_page.ser(buf.as_bytes_mut(), 0);
        debug_assert_eq!(ser_idx, ser_len);

        let b3sum = blake3::hash(&buf.as_bytes()[..TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET]);
        let footer = SuperPageFooter {
            b3sum: *b3sum.as_bytes(),
            checkpoint_cts: super_page.header.checkpoint_cts,
        };
        let ser_idx = footer.ser(buf.as_bytes_mut(), TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET);
        debug_assert_eq!(ser_idx, TABLE_FILE_SUPER_PAGE_SIZE);
        Ok(buf)
    }
}

/// Guard object of swapped table-file roots.
pub type OldRoot = OldCoWRoot<ActiveRoot>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{GlobalReadonlyBufferPool, ReadonlyBufferPool};
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::error::Error;
    use crate::file::table_fs::TableFileSystemConfig;
    use crate::io::AIOBuf;
    use crate::value::ValKind;
    use std::sync::OnceLock;
    use tempfile::TempDir;

    fn global_readonly_pool() -> &'static GlobalReadonlyBufferPool {
        static GLOBAL: OnceLock<&'static GlobalReadonlyBufferPool> = OnceLock::new();
        GLOBAL.get_or_init(|| {
            GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap()
        })
    }

    fn readonly_pool(table_id: u64, table_file: &Arc<TableFile>) -> ReadonlyBufferPool {
        ReadonlyBufferPool::new(table_id, Arc::clone(table_file), global_readonly_pool())
    }

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
    fn test_read_page_into_ptr_validates_byte_count() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let table_file = fs
                .create_table_file(145, build_test_metadata(), false)
                .unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let mut page = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
            let out_of_range_page_id = 1_000_000;
            let res = unsafe {
                table_file
                    .read_page_into_ptr(
                        out_of_range_page_id,
                        UnsafePtr(page.as_bytes_mut().as_mut_ptr()),
                    )
                    .await
            };
            assert!(res.is_err());

            drop(table_file);
            drop(fs);
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
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
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

            let disk_pool = readonly_pool(43, &table_file);
            let (table_file, old_root) = MutableTableFile::fork(&table_file)
                .persist_lwc_pages(lwc_pages, 7, 2, &disk_pool)
                .await
                .unwrap();
            drop(old_root);

            let active_root = table_file.active_root();
            assert_eq!(active_root.trx_id, 2);
            assert_eq!(active_root.pivot_row_id, 20);
            assert_eq!(active_root.heap_redo_start_ts, 7);
            assert_ne!(active_root.column_block_index_root, 0);
            let disk_pool = readonly_pool(43, &table_file);

            let column_index = crate::index::ColumnBlockIndex::new(
                active_root.column_block_index_root,
                active_root.pivot_row_id,
                &disk_pool,
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

            let disk_pool = readonly_pool(44, &table_file);
            let result = MutableTableFile::fork(&table_file)
                .persist_lwc_pages(lwc_pages, 7, 2, &disk_pool)
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
