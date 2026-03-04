use crate::catalog::{ObjID, TableID, USER_OBJ_ID_START};
use crate::error::{Error, Result};
use crate::file::super_page::{
    SUPER_PAGE_VERSION, SuperPage, SuperPageBody, SuperPageFooter, SuperPageHeader,
    SuperPageSerView, parse_super_page, pick_latest_valid_super_page,
};
use crate::file::table_file::{
    TABLE_FILE_PAGE_SIZE, TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET, TABLE_FILE_SUPER_PAGE_SIZE,
};
use crate::file::{FileIO, FileIOResult, FixedSizeBufferFreeList, SparseFile};
use crate::io::{AIOBuf, AIOClient, AIOKind, DirectBuf};
use crate::row::RowID;
use crate::serde::{Ser, Serde};
use crate::trx::TrxID;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

/// On-disk format version of `catalog.mtb`.
pub const CATALOG_MTB_VERSION: u64 = 1;
/// Reserved number of catalog logical-table root descriptors.
pub const CATALOG_TABLE_ROOT_DESC_COUNT: usize = 4;
/// Initial sparse-file size for `catalog.mtb`.
pub const MULTI_TABLE_FILE_INITIAL_SIZE: usize = TABLE_FILE_PAGE_SIZE * 4;
const CATALOG_MTB_META_PAGE_0: u64 = 2;
const CATALOG_MTB_META_PAGE_1: u64 = 3;

const MULTI_TABLE_FILE_MAGIC_WORD: [u8; 8] = [b'D', b'O', b'R', b'A', b'M', b'T', b'B', 0];
const MULTI_TABLE_META_MAGIC_WORD: [u8; 8] = [b'M', b'T', b'B', b'M', b'E', b'T', b'A', 0];

/// Root descriptor reserved for one catalog logical table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatalogTableRootDesc {
    /// Catalog table id.
    pub table_id: TableID,
    /// Reserved on-disk root page id for future catalog table persistence.
    pub root_page_id: u64,
    /// Reserved pivot row id paired with `root_page_id`.
    pub pivot_row_id: RowID,
}

/// Serialized payload persisted in `catalog.mtb` meta page.
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

/// Snapshot returned from loaded `catalog.mtb` active root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiTableFileSnapshot {
    /// Checkpoint CTS attached to active super page.
    pub checkpoint_cts: TrxID,
    /// Active meta-page payload.
    pub meta: MultiTableMetaPage,
}

/// Persistent file facade for unified catalog metadata (`catalog.mtb`).
///
/// The file layout follows `TableFile` conventions:
/// - page 0 contains two ping-pong super pages,
/// - one full page stores active meta payload,
/// - updates are published by writing new meta page then swapping active super page.
pub struct MultiTableFile {
    file_path: String,
    file: SparseFile,
    active_root: AtomicPtr<MultiTableActiveRoot>,
    io_client: AIOClient<FileIO>,
    buf_list: FixedSizeBufferFreeList,
}

impl MultiTableFile {
    /// Open existing file or create a new one, then load/publish initial active root.
    #[inline]
    pub(super) async fn open_or_create(
        file_path: impl AsRef<str>,
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
    ) -> Result<Arc<Self>> {
        let file_path = file_path.as_ref().to_string();
        let file_exists = Path::new(&file_path).exists();
        let file = if file_exists {
            SparseFile::open(&file_path)?
        } else {
            SparseFile::create_or_fail(&file_path, MULTI_TABLE_FILE_INITIAL_SIZE)?
        };

        let file = Arc::new(MultiTableFile {
            file_path,
            file,
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            io_client,
            buf_list,
        });

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

    /// Returns active root snapshot from in-memory pointer.
    #[inline]
    pub fn load_snapshot(&self) -> Result<MultiTableFileSnapshot> {
        let active_root = self.active_root();
        Ok(MultiTableFileSnapshot {
            checkpoint_cts: active_root.checkpoint_cts,
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

    /// Returns underlying file path.
    #[inline]
    pub fn path(&self) -> &str {
        &self.file_path
    }

    /// Returns active root object for internal checkpoint flows.
    #[inline]
    pub fn active_root(&self) -> &MultiTableActiveRoot {
        Self::active_root_from_raw(self.load_active_root_raw())
    }

    /// Read one page from `catalog.mtb` using async direct IO.
    #[inline]
    pub async fn read_page(&self, page_id: u64) -> Result<DirectBuf> {
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
        match promise.wait_async().await {
            FileIOResult::ReadOk(buf) => Ok(buf),
            FileIOResult::ReadStaticOk => panic!("invalid state"),
            FileIOResult::WriteOk => panic!("invalid state"),
            FileIOResult::Err(err) => Err(err.into()),
        }
    }

    #[inline]
    async fn write_page(&self, page_id: u64, buf: DirectBuf) -> Result<()> {
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
        match promise.wait_async().await {
            FileIOResult::WriteOk => Ok(()),
            FileIOResult::ReadStaticOk => panic!("invalid state"),
            FileIOResult::ReadOk(_) => panic!("invalid state"),
            FileIOResult::Err(err) => Err(err.into()),
        }
    }

    /// Force all file writes to disk.
    #[inline]
    pub fn fsync(&self) {
        self.file.syncer().fsync();
    }

    /// Replace active root and return old root for deferred reclamation.
    #[inline]
    pub fn swap_active_root(&self, active_root: MultiTableActiveRoot) -> Option<OldMultiTableRoot> {
        let new = Self::allocate_active_root(active_root);
        let old = self.active_root.swap(new, Ordering::SeqCst);
        NonNull::new(old).map(OldMultiTableRoot)
    }

    #[inline]
    async fn load_active_root(&self) -> Result<MultiTableActiveRoot> {
        // page 0 contains two ping-pong super pages.
        let buf = self.read_page(0).await?;
        let super_page = self.pick_super_page(buf.as_bytes())?;
        self.buf_list.push(buf);

        let meta_page_id = super_page.body.meta_page_id;
        if !matches!(
            meta_page_id,
            CATALOG_MTB_META_PAGE_0 | CATALOG_MTB_META_PAGE_1
        ) {
            return Err(Error::InvalidFormat);
        }
        let meta_buf = self.read_page(meta_page_id).await?;
        let meta = parse_meta_page(meta_buf.as_bytes())?;
        self.buf_list.push(meta_buf);

        Ok(MultiTableActiveRoot {
            page_no: super_page.header.page_no,
            checkpoint_cts: super_page.header.checkpoint_cts,
            meta_page_id,
            meta,
        })
    }

    #[inline]
    fn pick_super_page(&self, buf: &[u8]) -> Result<SuperPage> {
        pick_latest_valid_super_page(buf, TABLE_FILE_SUPER_PAGE_SIZE, |super_page_buf| {
            parse_super_page(
                super_page_buf,
                MULTI_TABLE_FILE_MAGIC_WORD,
                SUPER_PAGE_VERSION,
                TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET,
            )
        })
    }

    #[inline]
    fn load_active_root_raw(&self) -> *mut MultiTableActiveRoot {
        self.active_root.load(Ordering::Relaxed)
    }

    #[inline]
    fn active_root_from_raw<'a>(ptr: *mut MultiTableActiveRoot) -> &'a MultiTableActiveRoot {
        match NonNull::new(ptr) {
            Some(ptr) => {
                // SAFETY: active roots are allocated from `Box<MultiTableActiveRoot>` and
                // reclaimed only after they are swapped out or file dropped.
                unsafe { ptr.as_ref() }
            }
            None => panic!("active root is not initialized"),
        }
    }

    #[inline]
    fn allocate_active_root(active_root: MultiTableActiveRoot) -> *mut MultiTableActiveRoot {
        Box::into_raw(Box::new(active_root))
    }

    #[inline]
    fn reclaim_active_root(ptr: *mut MultiTableActiveRoot) {
        if let Some(ptr) = NonNull::new(ptr) {
            // SAFETY: pointer was allocated by `allocate_active_root` and must be reclaimed once.
            unsafe {
                drop(Box::from_raw(ptr.as_ptr()));
            }
        }
    }
}

impl Drop for MultiTableFile {
    #[inline]
    fn drop(&mut self) {
        Self::reclaim_active_root(self.load_active_root_raw());
    }
}

/// Mutable wrapper for publishing one new multi-table checkpoint root.
pub struct MutableMultiTableFile {
    table_file: Arc<MultiTableFile>,
    new_root: MultiTableActiveRoot,
}

impl MutableMultiTableFile {
    /// Create mutable handle with caller-provided root.
    #[inline]
    pub fn new(table_file: Arc<MultiTableFile>, new_root: MultiTableActiveRoot) -> Self {
        MutableMultiTableFile {
            table_file,
            new_root,
        }
    }

    /// Fork mutable handle from current active root.
    #[inline]
    pub fn fork(table_file: &Arc<MultiTableFile>) -> Self {
        MutableMultiTableFile {
            table_file: Arc::clone(table_file),
            new_root: table_file.active_root().flip(),
        }
    }

    /// Returns mutable root snapshot being built.
    #[inline]
    pub fn root(&self) -> &MultiTableActiveRoot {
        &self.new_root
    }

    /// Apply checkpoint metadata to mutable root.
    #[inline]
    pub fn apply_checkpoint_metadata(
        &mut self,
        checkpoint_cts: TrxID,
        next_user_obj_id: ObjID,
        table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    ) -> Result<()> {
        if checkpoint_cts < self.new_root.checkpoint_cts {
            return Err(Error::InvalidArgument);
        }
        if next_user_obj_id < USER_OBJ_ID_START {
            return Err(Error::InvalidArgument);
        }

        self.new_root.checkpoint_cts = checkpoint_cts;
        self.new_root.meta.next_user_obj_id = next_user_obj_id;
        self.new_root.meta.table_roots = table_roots;
        Ok(())
    }

    /// Commit mutable root by writing meta page then ping-pong super page.
    #[inline]
    pub async fn commit(self) -> Result<(Arc<MultiTableFile>, Option<OldMultiTableRoot>)> {
        let MutableMultiTableFile {
            table_file,
            new_root,
        } = self;

        // Write meta page first.
        let meta_buf = build_meta_page(&new_root.meta);
        table_file
            .write_page(new_root.meta_page_id, meta_buf)
            .await?;

        // Then write corresponding super page.
        let super_buf = build_super_page(
            new_root.page_no,
            new_root.checkpoint_cts,
            new_root.meta_page_id,
        );
        let offset = new_root.page_no as usize * TABLE_FILE_SUPER_PAGE_SIZE;
        table_file.write(offset, super_buf, false).await?;

        // Persist and publish active root.
        table_file.fsync();
        let old_root = table_file.swap_active_root(new_root);
        Ok((table_file, old_root))
    }
}

/// Active root of multi-table metadata file.
#[derive(Clone)]
pub struct MultiTableActiveRoot {
    /// Ping-pong super-page selector (0 or 1).
    pub page_no: u64,
    /// Checkpoint timestamp of this root.
    pub checkpoint_cts: TrxID,
    /// Meta-page id of this root (2 or 3).
    pub meta_page_id: u64,
    /// Meta payload.
    pub meta: MultiTableMetaPage,
}

impl MultiTableActiveRoot {
    /// Create a default active root for a newly created file.
    #[inline]
    pub fn new() -> Self {
        MultiTableActiveRoot {
            page_no: 0,
            checkpoint_cts: 0,
            meta_page_id: CATALOG_MTB_META_PAGE_0,
            meta: MultiTableMetaPage::new(USER_OBJ_ID_START),
        }
    }

    /// Flip active root to the opposite ping-pong super page and meta page.
    #[inline]
    pub fn flip(&self) -> Self {
        let mut next = self.clone();
        next.page_no = 1 - self.page_no;
        next.meta_page_id = if self.meta_page_id == CATALOG_MTB_META_PAGE_0 {
            CATALOG_MTB_META_PAGE_1
        } else {
            CATALOG_MTB_META_PAGE_0
        };
        next
    }

    /// Build super-page serialization view from current root.
    #[inline]
    pub fn ser_view(&self) -> SuperPageSerView {
        SuperPageSerView {
            header: SuperPageHeader {
                magic_word: MULTI_TABLE_FILE_MAGIC_WORD,
                version: SUPER_PAGE_VERSION,
                page_no: self.page_no,
                checkpoint_cts: self.checkpoint_cts,
            },
            body: SuperPageBody {
                meta_page_id: self.meta_page_id,
            },
        }
    }
}

impl Default for MultiTableActiveRoot {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Guard object for reclaimed active roots.
pub struct OldMultiTableRoot(NonNull<MultiTableActiveRoot>);

impl Drop for OldMultiTableRoot {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: old root pointer is produced by active-root swap and reclaimed once.
        unsafe {
            drop(Box::from_raw(self.0.as_ptr()));
        }
    }
}

unsafe impl Send for OldMultiTableRoot {}

#[inline]
fn build_super_page(page_no: u64, checkpoint_cts: TrxID, meta_page_id: u64) -> DirectBuf {
    let mut buf = DirectBuf::zeroed(TABLE_FILE_SUPER_PAGE_SIZE);
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
    debug_assert!(ser_len <= TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET);
    let ser_idx = ser_view.ser(buf.as_bytes_mut(), 0);
    debug_assert_eq!(ser_idx, ser_len);

    let b3sum = blake3::hash(&buf.as_bytes()[..TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET]);
    let footer = SuperPageFooter {
        b3sum: *b3sum.as_bytes(),
        checkpoint_cts,
    };
    let ser_idx = footer.ser(buf.as_bytes_mut(), TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET);
    debug_assert_eq!(ser_idx, TABLE_FILE_SUPER_PAGE_SIZE);
    buf
}

#[inline]
fn build_meta_page(meta: &MultiTableMetaPage) -> DirectBuf {
    let mut buf = DirectBuf::zeroed(TABLE_FILE_PAGE_SIZE);
    let mut idx = 0;
    idx = buf
        .as_bytes_mut()
        .ser_byte_array(idx, &MULTI_TABLE_META_MAGIC_WORD);
    idx = buf.as_bytes_mut().ser_u64(idx, CATALOG_MTB_VERSION);
    idx = buf.as_bytes_mut().ser_u64(idx, meta.next_user_obj_id);
    idx = buf
        .as_bytes_mut()
        .ser_u32(idx, CATALOG_TABLE_ROOT_DESC_COUNT as u32);
    idx = buf.as_bytes_mut().ser_u32(idx, 0); // reserved
    for root in &meta.table_roots {
        idx = buf.as_bytes_mut().ser_u64(idx, root.table_id);
        idx = buf.as_bytes_mut().ser_u64(idx, root.root_page_id);
        idx = buf.as_bytes_mut().ser_u64(idx, root.pivot_row_id);
    }
    debug_assert!(idx <= TABLE_FILE_PAGE_SIZE);
    buf
}

fn parse_meta_page(buf: &[u8]) -> Result<MultiTableMetaPage> {
    let (idx, magic) = buf.deser_byte_array::<8>(0)?;
    if magic != MULTI_TABLE_META_MAGIC_WORD {
        return Err(Error::InvalidFormat);
    }
    let (idx, version) = buf.deser_u64(idx)?;
    if version != CATALOG_MTB_VERSION {
        return Err(Error::InvalidFormat);
    }
    let (idx, next_user_obj_id) = buf.deser_u64(idx)?;
    if next_user_obj_id < USER_OBJ_ID_START {
        return Err(Error::InvalidFormat);
    }
    let (idx, table_count) = buf.deser_u32(idx)?;
    let (mut idx, _) = buf.deser_u32(idx)?; // reserved
    if table_count as usize != CATALOG_TABLE_ROOT_DESC_COUNT {
        return Err(Error::InvalidFormat);
    }

    let mut table_roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
    for root in &mut table_roots {
        let (next_idx, table_id) = buf.deser_u64(idx)?;
        let (next_idx, root_page_id) = buf.deser_u64(next_idx)?;
        let (next_idx, pivot_row_id) = buf.deser_u64(next_idx)?;
        *root = CatalogTableRootDesc {
            table_id,
            root_page_id,
            pivot_row_id,
        };
        idx = next_idx;
    }

    Ok(MultiTableMetaPage {
        next_user_obj_id,
        table_roots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::table_fs::TableFileSystemConfig;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
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

            let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots.iter_mut().enumerate() {
                root.table_id = idx as u64;
                root.root_page_id = (idx + 10) as u64;
                root.pivot_row_id = (idx * 100) as u64;
            }
            mtb.publish_checkpoint(7, USER_OBJ_ID_START + 16, roots)
                .await
                .unwrap();
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
                TABLE_FILE_SUPER_PAGE_SIZE as u64 + MULTI_TABLE_FILE_MAGIC_WORD.len() as u64,
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
            drop(mtb);
            drop(fs);

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            // overwrite meta-page version to simulate format mismatch.
            let meta_offset = CATALOG_MTB_META_PAGE_0 * TABLE_FILE_PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(
                meta_offset + MULTI_TABLE_META_MAGIC_WORD.len() as u64,
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
}
