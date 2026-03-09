use crate::bitmap::AllocMap;
use crate::buffer::page::PageID;
use crate::catalog::{ObjID, TableID, USER_OBJ_ID_START};
use crate::error::{Error, Result};
use crate::file::cow_file::{
    ActiveRoot as GenericActiveRoot, COW_FILE_PAGE_SIZE, CowCodec, CowFile, MutableCowFile,
    OldCowRoot, ParsedMeta,
};
use crate::file::meta_page::{MultiTableMetaPageData, MultiTableMetaPageSerView};
use crate::file::super_page::{
    SUPER_PAGE_FOOTER_OFFSET, SUPER_PAGE_SIZE, SUPER_PAGE_VERSION, SuperPageBody, SuperPageFooter,
    SuperPageHeader, SuperPageSerView, parse_super_page,
};
use crate::file::table_file::TABLE_FILE_INITIAL_SIZE;
use crate::file::{FileIO, FixedSizeBufferFreeList};
use crate::io::{AIOBuf, AIOClient, DirectBuf};
use crate::row::RowID;
use crate::serde::{Deser, Ser};
use crate::trx::TrxID;
use std::num::NonZeroU64;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

/// On-disk format version of `catalog.mtb`.
pub const CATALOG_MTB_VERSION: u64 = 2;
/// Reserved number of catalog logical-table root descriptors.
pub const CATALOG_TABLE_ROOT_DESC_COUNT: usize = 4;
/// Initial sparse-file size for `catalog.mtb`.
pub const MULTI_TABLE_FILE_INITIAL_SIZE: usize = TABLE_FILE_INITIAL_SIZE;

const MULTI_TABLE_FILE_MAGIC_WORD: [u8; 8] = [b'D', b'O', b'R', b'A', b'M', b'T', b'B', 0];

/// Root descriptor reserved for one catalog logical table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatalogTableRootDesc {
    /// Catalog table id.
    pub table_id: TableID,
    /// Reserved on-disk root page id for future catalog table persistence.
    pub root_page_id: Option<NonZeroU64>,
    /// Reserved pivot row id paired with `root_page_id`.
    pub pivot_row_id: RowID,
}

/// File-specific payload persisted in `catalog.mtb` meta pages.
///
/// Generic CoW bookkeeping fields (`alloc_map`, `gc_page_list`, `meta_page_id`)
/// are stored on the shared active root, not in this payload struct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiTableMetaPage {
    /// Global next user object-id allocator watermark.
    pub next_user_obj_id: ObjID,
    /// Reserved root descriptors for catalog logical tables.
    pub table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
}

impl MultiTableMetaPage {
    /// Create a meta payload initialized with allocator lower bound.
    #[inline]
    pub fn new(next_user_obj_id: ObjID) -> Self {
        let mut table_roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
        for (idx, root) in table_roots.iter_mut().enumerate() {
            root.table_id = idx as TableID;
        }
        MultiTableMetaPage {
            next_user_obj_id: next_user_obj_id.max(USER_OBJ_ID_START),
            table_roots,
        }
    }
}

/// Active-root type for `catalog.mtb`.
pub type MultiTableActiveRoot = GenericActiveRoot<MultiTableMetaPage>;

impl MultiTableActiveRoot {
    /// Create default active root for a newly created `catalog.mtb` file.
    #[inline]
    pub fn new() -> Self {
        let max_pages = MULTI_TABLE_FILE_INITIAL_SIZE / COW_FILE_PAGE_SIZE;
        let alloc_map = AllocMap::new(max_pages);
        let allocated = alloc_map.allocate_at(0);
        debug_assert!(allocated);

        MultiTableActiveRoot::from_parts(
            0,
            0,
            0,
            alloc_map,
            vec![],
            MultiTableMetaPage::new(USER_OBJ_ID_START),
        )
    }

    #[inline]
    pub fn meta_page_ser_view(&self) -> MultiTableMetaPageSerView<'_> {
        MultiTableMetaPageSerView::new(&self.meta, &self.alloc_map, &self.gc_page_list)
    }
}

impl Default for MultiTableActiveRoot {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot returned from loaded `catalog.mtb` active root.
///
/// This is a lightweight clone used by catalog storage layer when publishing
/// or reading checkpoint metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiTableFileSnapshot {
    /// Checkpoint CTS attached to active super page.
    pub checkpoint_cts: TrxID,
    /// Active meta-page payload.
    pub meta: MultiTableMetaPage,
}

#[inline]
fn parse_multi_table_super_page(buf: &[u8]) -> Result<crate::file::super_page::SuperPage> {
    parse_super_page(buf, MULTI_TABLE_FILE_MAGIC_WORD, SUPER_PAGE_VERSION)
}

#[inline]
fn build_multi_table_super_page(root: &MultiTableActiveRoot) -> Result<DirectBuf> {
    Ok(build_super_page(
        root.page_no,
        root.trx_id,
        root.meta_page_id,
    ))
}

#[inline]
fn parse_multi_table_meta_page(buf: &[u8]) -> Result<ParsedMeta<MultiTableMetaPage>> {
    let (_, meta_page) = MultiTableMetaPageData::deser(buf, 0)?;

    Ok(ParsedMeta {
        meta: MultiTableMetaPage {
            next_user_obj_id: meta_page.next_user_obj_id,
            table_roots: meta_page.table_roots,
        },
        alloc_map: meta_page.alloc_map,
        gc_page_list: meta_page.gc_page_list,
    })
}

#[inline]
fn build_multi_table_meta_page(root: &MultiTableActiveRoot) -> Result<DirectBuf> {
    let meta_page = root.meta_page_ser_view();
    let meta_len = meta_page.ser_len();
    if meta_len > COW_FILE_PAGE_SIZE || root.gc_page_list.len() > u32::MAX as usize {
        return Err(Error::InvalidState);
    }
    let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
    let idx = meta_page.ser(buf.as_bytes_mut(), 0);
    debug_assert_eq!(idx, meta_len);
    Ok(buf)
}

#[inline]
fn multi_table_codec() -> CowCodec<MultiTableMetaPage> {
    CowCodec {
        parse_super_page: parse_multi_table_super_page,
        parse_meta_page: parse_multi_table_meta_page,
        build_meta_page: build_multi_table_meta_page,
        build_super_page: build_multi_table_super_page,
    }
}

/// Persistent file facade for unified catalog metadata (`catalog.mtb`).
///
/// The file layout follows `TableFile` conventions:
/// - page 0 contains two ping-pong super pages,
/// - meta pages are CoW-allocated and super pages point to active meta page,
/// - updates are published by writing new meta page then swapping active super page.
pub struct MultiTableFile(CowFile<MultiTableMetaPage>);

impl MultiTableFile {
    /// Open existing file or create a new one, then load/publish initial active root.
    #[inline]
    pub(super) async fn open_or_create(
        file_path: impl AsRef<str>,
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
    ) -> Result<Arc<Self>> {
        let file_path = file_path.as_ref();
        let file_exists = Path::new(file_path).exists();
        let cow_file = if file_exists {
            CowFile::open(file_path, io_client, buf_list, multi_table_codec())?
        } else {
            CowFile::create(
                file_path,
                MULTI_TABLE_FILE_INITIAL_SIZE,
                io_client,
                buf_list,
                multi_table_codec(),
                false,
            )?
        };

        let file = Arc::new(MultiTableFile(cow_file));

        if file_exists {
            let active_root = file.load_active_root().await?;
            let old_root = file.swap_active_root(active_root);
            debug_assert!(old_root.is_none());
        } else {
            let mutable =
                MutableMultiTableFile::new(Arc::clone(&file), MultiTableActiveRoot::new());
            let (_, old_root) = mutable.commit().await?;
            debug_assert!(old_root.is_none());
        }

        Ok(file)
    }

    /// Load active root by parsing ping-pong super pages and referenced meta page.
    #[inline]
    pub async fn load_active_root(&self) -> Result<MultiTableActiveRoot> {
        let root = self.0.load_active_root().await?;
        let meta_page_idx = usize::try_from(root.meta_page_id).map_err(|_| Error::InvalidFormat)?;
        if meta_page_idx == 0
            || meta_page_idx >= root.alloc_map.len()
            || !root.alloc_map.is_allocated(meta_page_idx)
        {
            return Err(Error::InvalidFormat);
        }
        Ok(root)
    }

    /// Returns active-root snapshot from in-memory pointer without additional IO.
    #[inline]
    pub fn load_snapshot(&self) -> Result<MultiTableFileSnapshot> {
        let active_root = self.active_root();
        Ok(MultiTableFileSnapshot {
            checkpoint_cts: active_root.trx_id,
            meta: active_root.meta.clone(),
        })
    }

    /// Publish new checkpoint metadata atomically.
    #[inline]
    pub async fn publish_checkpoint(
        self: &Arc<Self>,
        checkpoint_cts: TrxID,
        next_user_obj_id: ObjID,
        table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    ) -> Result<()> {
        let mut mutable = MutableMultiTableFile::fork(self);
        mutable.apply_checkpoint_metadata(checkpoint_cts, next_user_obj_id, table_roots)?;
        let (_, old_root) = mutable.commit().await?;
        drop(old_root);
        Ok(())
    }
}

impl Deref for MultiTableFile {
    type Target = CowFile<MultiTableMetaPage>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Mutable wrapper for publishing one new multi-table checkpoint root.
///
/// This mirrors `MutableTableFile` semantics for catalog file updates.
pub struct MutableMultiTableFile {
    file: Option<Arc<MultiTableFile>>,
    new_root: Option<MultiTableActiveRoot>,
    mutable_writer_claimed: bool,
}

impl MutableMultiTableFile {
    #[inline]
    fn file_ref(&self) -> &Arc<MultiTableFile> {
        self.file
            .as_ref()
            .expect("mutable multi-table file has been consumed")
    }

    #[inline]
    fn new_root_mut(&mut self) -> &mut MultiTableActiveRoot {
        self.new_root
            .as_mut()
            .expect("mutable multi-table file has been consumed")
    }

    #[inline]
    fn release_mutable_claim_with_file(&mut self, file: &Arc<MultiTableFile>) {
        if self.mutable_writer_claimed {
            file.release_mutable_writer();
            self.mutable_writer_claimed = false;
        }
    }

    #[inline]
    fn release_mutable_claim(&mut self) {
        if self.mutable_writer_claimed {
            let file = self
                .file
                .as_ref()
                .expect("mutable multi-table file has been consumed");
            file.release_mutable_writer();
            self.mutable_writer_claimed = false;
        }
    }

    /// Create mutable handle with caller-provided root.
    #[inline]
    pub fn new(table_file: Arc<MultiTableFile>, new_root: MultiTableActiveRoot) -> Self {
        table_file.claim_mutable_writer();
        MutableMultiTableFile {
            file: Some(table_file),
            new_root: Some(new_root),
            mutable_writer_claimed: true,
        }
    }

    /// Fork mutable handle from current active root.
    #[inline]
    pub fn fork(table_file: &Arc<MultiTableFile>) -> Self {
        table_file.claim_mutable_writer();
        MutableMultiTableFile {
            file: Some(Arc::clone(table_file)),
            new_root: Some(table_file.active_root().flip()),
            mutable_writer_claimed: true,
        }
    }

    /// Returns immutable reference to mutable root snapshot.
    #[inline]
    pub fn root(&self) -> &MultiTableActiveRoot {
        self.new_root
            .as_ref()
            .expect("mutable multi-table file has been consumed")
    }

    /// Returns mutable reference to mutable root snapshot.
    #[inline]
    pub fn root_mut(&mut self) -> &mut MultiTableActiveRoot {
        self.new_root_mut()
    }

    /// Allocate a new page id for copy-on-write updates.
    #[inline]
    pub fn allocate_page_id(&mut self) -> Result<PageID> {
        self.new_root_mut()
            .try_allocate_page_id()
            .ok_or(Error::InvalidState)
    }

    /// Record an obsolete page id to be reclaimed after commit.
    #[inline]
    pub fn record_gc_page(&mut self, page_id: PageID) {
        self.new_root_mut().gc_page_list.push(page_id);
    }

    /// Write one page into the underlying multi-table file.
    #[inline]
    pub async fn write_page(&self, page_id: PageID, buf: DirectBuf) -> Result<()> {
        self.file_ref().write_page(page_id, buf).await
    }

    /// Apply checkpoint metadata to mutable root.
    #[inline]
    pub fn apply_checkpoint_metadata(
        &mut self,
        checkpoint_cts: TrxID,
        next_user_obj_id: ObjID,
        table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    ) -> Result<()> {
        let root = self.new_root_mut();
        if checkpoint_cts < root.trx_id {
            return Err(Error::InvalidArgument);
        }
        if next_user_obj_id < USER_OBJ_ID_START {
            return Err(Error::InvalidArgument);
        }
        for root in &table_roots {
            if root.root_page_id.is_none() && root.pivot_row_id != 0 {
                return Err(Error::InvalidArgument);
            }
        }

        root.trx_id = checkpoint_cts;
        root.next_user_obj_id = next_user_obj_id;
        root.table_roots = table_roots;
        Ok(())
    }

    /// Commit mutable root by writing meta page then ping-pong super page.
    #[inline]
    pub async fn commit(mut self) -> Result<(Arc<MultiTableFile>, Option<OldMultiTableRoot>)> {
        let new_root = self
            .new_root
            .take()
            .expect("mutable multi-table file has been consumed");
        let publish_res = self.file_ref().publish_root(new_root).await;
        let file = self
            .file
            .take()
            .expect("mutable multi-table file has been consumed");
        self.release_mutable_claim_with_file(&file);
        let old_root = publish_res?;
        Ok((file, old_root))
    }
}

impl Drop for MutableMultiTableFile {
    #[inline]
    fn drop(&mut self) {
        self.release_mutable_claim();
    }
}

impl MutableCowFile for MutableMultiTableFile {
    #[inline]
    fn allocate_page_id(&mut self) -> Result<PageID> {
        MutableMultiTableFile::allocate_page_id(self)
    }

    #[inline]
    fn record_gc_page(&mut self, page_id: PageID) {
        MutableMultiTableFile::record_gc_page(self, page_id)
    }

    #[inline]
    fn write_page(
        &self,
        page_id: PageID,
        buf: DirectBuf,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        MutableMultiTableFile::write_page(self, page_id, buf)
    }
}

/// Guard object for reclaimed replaced roots of `catalog.mtb`.
pub type OldMultiTableRoot = OldCowRoot<MultiTableMetaPage>;

#[inline]
fn build_super_page(page_no: PageID, checkpoint_cts: TrxID, meta_page_id: PageID) -> DirectBuf {
    let mut buf = DirectBuf::zeroed(SUPER_PAGE_SIZE);
    let ser_view = SuperPageSerView {
        header: SuperPageHeader {
            magic_word: MULTI_TABLE_FILE_MAGIC_WORD,
            version: SUPER_PAGE_VERSION,
            page_no,
            checkpoint_cts,
        },
        body: SuperPageBody { meta_page_id },
    };
    let ser_len = ser_view.ser_len();
    debug_assert!(ser_len <= SUPER_PAGE_FOOTER_OFFSET);
    let ser_idx = ser_view.ser(buf.as_bytes_mut(), 0);
    debug_assert_eq!(ser_idx, ser_len);

    let b3sum = blake3::hash(&buf.as_bytes()[..SUPER_PAGE_FOOTER_OFFSET]);
    let footer = SuperPageFooter {
        b3sum: *b3sum.as_bytes(),
        checkpoint_cts,
    };
    let ser_idx = footer.ser(buf.as_bytes_mut(), SUPER_PAGE_FOOTER_OFFSET);
    debug_assert_eq!(ser_idx, SUPER_PAGE_SIZE);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::table_fs::TableFileSystemConfig;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::num::NonZeroU64;
    use tempfile::TempDir;

    #[test]
    fn test_multi_table_file_open_publish_and_reload() {
        smol::block_on(async {
            let dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(dir.path())
                .build()
                .unwrap();
            let path = fs.catalog_mtb_file_path();

            let mtb = fs.open_or_create_multi_table_file().await.unwrap();
            let s0 = mtb.load_snapshot().unwrap();
            assert_eq!(s0.checkpoint_cts, 0);
            assert_eq!(s0.meta.next_user_obj_id, USER_OBJ_ID_START);
            let meta_page_id_0 = mtb.active_root().meta_page_id;
            assert!(meta_page_id_0 > 0);

            let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots.iter_mut().enumerate() {
                root.table_id = idx as u64;
                root.root_page_id = NonZeroU64::new((idx + 10) as u64);
                root.pivot_row_id = (idx * 100) as u64;
            }
            mtb.publish_checkpoint(7, USER_OBJ_ID_START + 16, roots)
                .await
                .unwrap();
            let meta_page_id_1 = mtb.active_root().meta_page_id;
            assert_ne!(meta_page_id_0, meta_page_id_1);
            drop(mtb);

            let mtb2 = fs.open_or_create_multi_table_file().await.unwrap();
            let s1 = mtb2.load_snapshot().unwrap();
            assert_eq!(s1.checkpoint_cts, 7);
            assert_eq!(s1.meta.next_user_obj_id, USER_OBJ_ID_START + 16);
            assert_eq!(s1.meta.table_roots, roots);

            drop(mtb2);
            drop(fs);
            let _ = std::fs::remove_file(path);
        });
    }

    #[test]
    fn test_multi_table_file_meta_page_copy_on_write() {
        smol::block_on(async {
            let dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(dir.path())
                .build()
                .unwrap();
            let mtb = fs.open_or_create_multi_table_file().await.unwrap();

            let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots.iter_mut().enumerate() {
                root.table_id = idx as u64;
            }

            let meta_page_id_0 = mtb.active_root().meta_page_id;
            mtb.publish_checkpoint(3, USER_OBJ_ID_START + 1, roots)
                .await
                .unwrap();
            let meta_page_id_1 = mtb.active_root().meta_page_id;
            mtb.publish_checkpoint(4, USER_OBJ_ID_START + 2, roots)
                .await
                .unwrap();
            let meta_page_id_2 = mtb.active_root().meta_page_id;

            assert_ne!(meta_page_id_0, meta_page_id_1);
            assert_ne!(meta_page_id_1, meta_page_id_2);
            assert!(mtb.active_root().gc_page_list.contains(&meta_page_id_0));
            assert!(mtb.active_root().gc_page_list.contains(&meta_page_id_1));
        });
    }

    #[test]
    fn test_multi_table_file_rejects_super_page_version_mismatch() {
        smol::block_on(async {
            let dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(dir.path())
                .build()
                .unwrap();
            let path = fs.catalog_mtb_file_path();
            let mtb = fs.open_or_create_multi_table_file().await.unwrap();
            drop(mtb);
            drop(fs);

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            // overwrite both super-page versions to simulate format mismatch.
            file.seek(SeekFrom::Start(MULTI_TABLE_FILE_MAGIC_WORD.len() as u64))
                .unwrap();
            file.write_all(&2u64.to_le_bytes()).unwrap();
            file.seek(SeekFrom::Start(
                SUPER_PAGE_SIZE as u64 + MULTI_TABLE_FILE_MAGIC_WORD.len() as u64,
            ))
            .unwrap();
            file.write_all(&2u64.to_le_bytes()).unwrap();
            file.sync_all().unwrap();

            let fs = TableFileSystemConfig::default()
                .with_main_dir(dir.path())
                .build()
                .unwrap();
            let res = fs.open_or_create_multi_table_file().await;
            assert!(res.is_err());
        });
    }

    #[test]
    fn test_multi_table_file_rejects_meta_version_mismatch() {
        smol::block_on(async {
            let dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(dir.path())
                .build()
                .unwrap();
            let path = fs.catalog_mtb_file_path();
            let mtb = fs.open_or_create_multi_table_file().await.unwrap();
            let active_meta_page_id = mtb.active_root().meta_page_id;
            drop(mtb);
            drop(fs);

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            // overwrite meta-page version to simulate format mismatch.
            let meta_offset = active_meta_page_id * COW_FILE_PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(
                meta_offset + crate::file::meta_page::MULTI_TABLE_META_MAGIC_WORD.len() as u64,
            ))
            .unwrap();
            file.write_all(&(CATALOG_MTB_VERSION + 1).to_le_bytes())
                .unwrap();
            file.sync_all().unwrap();

            let fs = TableFileSystemConfig::default()
                .with_main_dir(dir.path())
                .build()
                .unwrap();
            let res = fs.open_or_create_multi_table_file().await;
            assert!(res.is_err());
        });
    }

    #[test]
    #[should_panic(expected = "concurrent mutable CoW file modification is not allowed")]
    fn test_multi_table_file_rejects_concurrent_fork() {
        smol::block_on(async {
            let dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(dir.path())
                .build()
                .unwrap();
            let mtb = fs.open_or_create_multi_table_file().await.unwrap();

            let _first = MutableMultiTableFile::fork(&mtb);
            let _second = MutableMultiTableFile::fork(&mtb);
        });
    }

    #[test]
    fn test_multi_table_file_allows_fork_after_drop() {
        smol::block_on(async {
            let dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(dir.path())
                .build()
                .unwrap();
            let mtb = fs.open_or_create_multi_table_file().await.unwrap();

            let first = MutableMultiTableFile::fork(&mtb);
            drop(first);

            let _second = MutableMultiTableFile::fork(&mtb);
        });
    }
}
