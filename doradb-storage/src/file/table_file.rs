use crate::bitmap::AllocMap;
use crate::buffer::ReadonlyBufferPool;
use crate::buffer::page::PageID;
use crate::catalog::table::TableMetadata;
use crate::error::{Error, Result};
use crate::file::cow_file::{
    ActiveRoot as GenericActiveRoot, COW_FILE_PAGE_SIZE, CowCodec, CowFile, OldCowRoot, ParsedMeta,
};
use crate::file::meta_page::{MetaPage, MetaPageSerView};
use crate::file::super_page::{
    SUPER_PAGE_FOOTER_OFFSET, SUPER_PAGE_SIZE, SUPER_PAGE_VERSION, SuperPage, SuperPageBody,
    SuperPageFooter, SuperPageHeader, SuperPageSerView, parse_super_page,
};
use crate::file::{FileIO, FixedSizeBufferFreeList};
use crate::io::{AIOBuf, AIOClient, DirectBuf};
use crate::ptr::UnsafePtr;
use crate::row::RowID;
use crate::serde::{Deser, Ser};
use crate::trx::TrxID;
use futures::future::try_join_all;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;

pub const TABLE_FILE_MAGIC_WORD: [u8; 8] = [b'D', b'O', b'R', b'A', 0, 0, 0, 0];

/// Initial size of new table file.
pub const TABLE_FILE_INITIAL_SIZE: usize = 16 * 1024 * 1024;
/// Super page size of table file is 32KB.
pub const TABLE_FILE_SUPER_PAGE_SIZE: usize = SUPER_PAGE_SIZE;
/// Super page header size.
pub const TABLE_FILE_SUPER_PAGE_HEADER_SIZE: usize = mem::size_of::<[u8; 8]>()
    + mem::size_of::<u64>()
    + mem::size_of::<PageID>()
    + mem::size_of::<TrxID>();

/// Table-specific metadata payload embedded in shared active root.
///
/// These fields are persisted in table meta pages and kept in memory through
/// `ActiveRoot` for fast access on hot paths.
#[derive(Clone)]
pub struct TableMeta {
    /// Metadata of this table.
    pub metadata: Arc<TableMetadata>,
    /// Root page id of column block index.
    pub column_block_index_root: PageID,
    /// Upper bound of row id in this file.
    pub pivot_row_id: RowID,
    /// Redo log start point for in-memory heap.
    pub heap_redo_start_ts: TrxID,
}

/// Active root type of table files.
///
/// This combines generic CoW root state with [`TableMeta`].
pub type ActiveRoot = GenericActiveRoot<TableMeta>;

impl ActiveRoot {
    /// Create a new active root.
    /// Page number is set to zero (the first super-page slot of this file).
    #[inline]
    pub fn new(trx_id: TrxID, max_pages: usize, metadata: Arc<TableMetadata>) -> Self {
        const DEFAULT_ROOT_PAGE_NO: PageID = 0;

        let alloc_map = AllocMap::new(max_pages);
        let super_page_allocated = alloc_map.allocate_at(DEFAULT_ROOT_PAGE_NO as usize);
        assert!(super_page_allocated);

        ActiveRoot::from_parts(
            DEFAULT_ROOT_PAGE_NO,
            trx_id,
            0,
            alloc_map,
            vec![],
            TableMeta {
                metadata,
                column_block_index_root: 0,
                pivot_row_id: 0,
                heap_redo_start_ts: trx_id,
            },
        )
    }

    /// Build super-page serialization view for the current active root.
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

    /// Build meta-page serialization view for the current active root.
    #[inline]
    pub fn meta_page_ser_view(&self) -> MetaPageSerView<'_> {
        MetaPageSerView::new(
            self.metadata.ser_view(),
            self.column_block_index_root,
            &self.alloc_map,
            &self.gc_page_list,
            self.pivot_row_id,
            self.heap_redo_start_ts,
        )
    }
}

#[inline]
fn parse_table_super_page(buf: &[u8]) -> Result<SuperPage> {
    parse_super_page(buf, TABLE_FILE_MAGIC_WORD, SUPER_PAGE_VERSION)
}

#[inline]
fn parse_table_meta_page(buf: &[u8]) -> Result<ParsedMeta<TableMeta>> {
    let (_, meta_page) = MetaPage::deser(buf, 0)?;
    Ok(ParsedMeta {
        meta: TableMeta {
            metadata: Arc::new(meta_page.schema),
            column_block_index_root: meta_page.column_block_index_root,
            pivot_row_id: meta_page.pivot_row_id,
            heap_redo_start_ts: meta_page.heap_redo_start_ts,
        },
        alloc_map: meta_page.alloc_map,
        gc_page_list: meta_page.gc_page_list,
    })
}

#[inline]
fn build_table_meta_page(root: &ActiveRoot) -> Result<DirectBuf> {
    let meta_page = root.meta_page_ser_view();
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
fn build_table_super_page(root: &ActiveRoot) -> Result<DirectBuf> {
    let super_page = root.ser_view();
    let mut buf = DirectBuf::zeroed(SUPER_PAGE_SIZE);
    let ser_len = super_page.ser_len();
    if ser_len > SUPER_PAGE_FOOTER_OFFSET {
        // single super page cannot hold all data
        unimplemented!("multiple pages are required to hold super data");
    }
    let ser_idx = super_page.ser(buf.as_bytes_mut(), 0);
    debug_assert_eq!(ser_idx, ser_len);

    let b3sum = blake3::hash(&buf.as_bytes()[..SUPER_PAGE_FOOTER_OFFSET]);
    let footer = SuperPageFooter {
        b3sum: *b3sum.as_bytes(),
        checkpoint_cts: root.trx_id,
    };
    let ser_idx = footer.ser(buf.as_bytes_mut(), SUPER_PAGE_FOOTER_OFFSET);
    debug_assert_eq!(ser_idx, SUPER_PAGE_SIZE);
    Ok(buf)
}

#[inline]
fn table_codec() -> CowCodec<TableMeta> {
    CowCodec {
        parse_super_page: parse_table_super_page,
        parse_meta_page: parse_table_meta_page,
        build_meta_page: build_table_meta_page,
        build_super_page: build_table_super_page,
    }
}

/// Table-file facade over generic copy-on-write file mechanics.
pub struct TableFile(CowFile<TableMeta>);

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
        let cow_file = CowFile::create(
            file_path,
            initial_size,
            io_client,
            buf_list,
            table_codec(),
            trunc,
        )?;
        Ok(TableFile(cow_file))
    }

    #[inline]
    pub(super) fn open(
        file_path: impl AsRef<str>,
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
    ) -> Result<Self> {
        let cow_file = CowFile::open(file_path, io_client, buf_list, table_codec())?;
        Ok(TableFile(cow_file))
    }

    /// Load active root by parsing ping-pong super pages and referenced meta page.
    #[inline]
    pub async fn load_active_root(&self) -> Result<ActiveRoot> {
        self.0.load_active_root().await
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
    /// Remove this table file from disk.
    pub fn delete(self) {
        self.0.delete();
    }
}

impl Deref for TableFile {
    type Target = CowFile<TableMeta>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// MutableTableFile represents a table file being modified.
/// It's safe to share the original table file with other threads
/// because the modification is done in Copy-on-Write way.
/// All changes will be committed once the new active root
/// is persisted to disk.
pub struct MutableTableFile {
    file: Arc<TableFile>,
    new_root: ActiveRoot,
}

/// One LWC page payload that should be persisted into table file pages.
pub struct LwcPagePersist {
    /// Inclusive start row id covered by this page.
    pub start_row_id: RowID,
    /// Exclusive end row id covered by this page.
    pub end_row_id: RowID,
    /// Serialized LWC page bytes.
    pub buf: DirectBuf,
}

impl MutableTableFile {
    /// Create new mutable table file.
    #[inline]
    pub fn new(table_file: Arc<TableFile>, new_root: ActiveRoot) -> Self {
        MutableTableFile {
            file: table_file,
            new_root,
        }
    }

    /// Fork the whole table file with a new root.
    #[inline]
    pub fn fork(table_file: &Arc<TableFile>) -> Self {
        MutableTableFile {
            file: Arc::clone(table_file),
            new_root: table_file.active_root().flip(),
        }
    }

    /// Returns immutable reference to mutable root snapshot.
    #[inline]
    pub fn root(&self) -> &ActiveRoot {
        &self.new_root
    }

    /// Consume mutable handle and return wrapped table file.
    #[inline]
    pub fn into_file(self) -> Arc<TableFile> {
        self.file
    }

    /// Updates mutable root column-index pointer.
    #[inline]
    pub fn set_column_block_index_root(&mut self, root_page_id: PageID) {
        self.new_root.column_block_index_root = root_page_id;
    }

    /// Updates mutable root checkpoint metadata.
    #[inline]
    pub fn apply_checkpoint_metadata(
        &mut self,
        pivot_row_id: RowID,
        heap_redo_start_ts: TrxID,
    ) -> Result<()> {
        let root = &mut self.new_root;
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
        self.new_root
            .try_allocate_page_id()
            .ok_or(Error::InvalidState)
    }

    /// Record an obsolete page id to be reclaimed on commit.
    #[inline]
    pub fn record_gc_page(&mut self, page_id: PageID) {
        self.new_root.gc_page_list.push(page_id);
    }

    /// Write one page into the underlying table file.
    #[inline]
    pub async fn write_page(&self, page_id: PageID, buf: DirectBuf) -> Result<()> {
        self.file.write_page(page_id, buf).await
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
            file: table_file,
            mut new_root,
        } = self;
        debug_assert!(new_root.trx_id == 0 || new_root.trx_id < trx_id);
        new_root.trx_id = trx_id;

        match table_file.publish_root(new_root).await {
            Ok(old_root) => Ok((table_file, old_root)),
            Err(err) => {
                if try_delete_if_fail && let Some(file) = Arc::into_inner(table_file) {
                    file.delete();
                }
                Err(err)
            }
        }
    }

    /// Persist provided LWC pages and update mutable root/index metadata.
    pub async fn apply_lwc_pages(
        &mut self,
        lwc_pages: Vec<LwcPagePersist>,
        heap_redo_start_ts: TrxID,
        ts: TrxID,
        disk_pool: &ReadonlyBufferPool,
    ) -> Result<()> {
        let table_file = Arc::clone(&self.file);
        let mut max_row_id = self.new_root.pivot_row_id;
        let mut writes = Vec::with_capacity(lwc_pages.len());
        let mut new_entries = Vec::with_capacity(lwc_pages.len());
        let mut last_end = self.new_root.pivot_row_id;

        for page in lwc_pages {
            if page.start_row_id >= page.end_row_id {
                return Err(Error::InvalidArgument);
            }
            let page_id = self
                .new_root
                .try_allocate_page_id()
                .ok_or(Error::InvalidState)?;
            max_row_id = max_row_id.max(page.end_row_id);
            if page.start_row_id < last_end {
                return Err(Error::InvalidArgument);
            }
            last_end = page.end_row_id;
            new_entries.push((page.start_row_id, page_id));
            writes.push(table_file.write_page(page_id, page.buf));
        }

        try_join_all(writes).await?;

        let root = &self.new_root;
        let column_index = crate::index::ColumnBlockIndex::new(
            root.column_block_index_root,
            root.pivot_row_id,
            disk_pool,
        );
        let new_root = column_index
            .batch_insert(self, &new_entries, max_row_id, ts)
            .await?;
        let root = &mut self.new_root;
        root.column_block_index_root = new_root;
        root.pivot_row_id = max_row_id;
        root.heap_redo_start_ts = heap_redo_start_ts;
        Ok(())
    }

    /// Persist LWC pages and commit the resulting root as one CoW publish.
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
        if let Some(table_file) = Arc::into_inner(self.into_file()) {
            table_file.delete();
            return true;
        }
        false
    }
}

/// Guard object of swapped table-file roots.
///
/// Dropping this guard reclaims the replaced active-root pointer.
pub type OldRoot = OldCowRoot<TableMeta>;

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

    async fn read_page_for_test(table_file: &TableFile, page_id: PageID) -> Result<DirectBuf> {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        // SAFETY: `DirectBuf` is sector-aligned and remains alive during async read.
        unsafe {
            table_file
                .read_page_into_ptr(page_id, UnsafePtr(buf.as_bytes_mut().as_mut_ptr()))
                .await?;
        }
        Ok(buf)
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

            let res = read_page_for_test(&table_file, 3).await;
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

            let res = read_page_for_test(&table_file, 1).await;
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
            let page1 = read_page_for_test(&table_file, payload1.block_id)
                .await
                .unwrap();
            let page2 = read_page_for_test(&table_file, payload2.block_id)
                .await
                .unwrap();
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
