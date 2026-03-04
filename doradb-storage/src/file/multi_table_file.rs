use crate::bitmap::AllocMap;
use crate::catalog::{ObjID, TableID, USER_OBJ_ID_START};
use crate::error::{Error, Result};
use crate::file::cow_file::{
    COW_FILE_PAGE_SIZE, CoWFile, CoWFileOwner, CoWRoot, MutableCoWFile, OldCoWRoot,
};
use crate::file::super_page::{
    SUPER_PAGE_VERSION, SuperPage, SuperPageBody, SuperPageFooter, SuperPageHeader,
    SuperPageSerView, parse_super_page, pick_latest_valid_super_page,
};
use crate::file::table_file::{
    TABLE_FILE_INITIAL_SIZE, TABLE_FILE_SUPER_PAGE_FOOTER_OFFSET, TABLE_FILE_SUPER_PAGE_SIZE,
};
use crate::file::{FileIO, FixedSizeBufferFreeList};
use crate::io::{AIOBuf, AIOClient, DirectBuf};
use crate::row::RowID;
use crate::serde::{Deser, Ser, Serde};
use crate::trx::TrxID;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;

/// On-disk format version of `catalog.mtb`.
pub const CATALOG_MTB_VERSION: u64 = 2;
/// Reserved number of catalog logical-table root descriptors.
pub const CATALOG_TABLE_ROOT_DESC_COUNT: usize = 4;
/// Initial sparse-file size for `catalog.mtb`.
pub const MULTI_TABLE_FILE_INITIAL_SIZE: usize = TABLE_FILE_INITIAL_SIZE;

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
    /// Allocation bitmap for catalog file pages.
    pub alloc_map: AllocMap,
    /// Obsolete meta page ids retained for future reclamation.
    pub gc_page_list: Vec<u64>,
}

impl MultiTableMetaPage {
    /// Create a meta payload initialized with allocator lower bound.
    #[inline]
    pub fn new(next_user_obj_id: ObjID) -> Self {
        let max_pages = MULTI_TABLE_FILE_INITIAL_SIZE / COW_FILE_PAGE_SIZE;
        let alloc_map = AllocMap::new(max_pages);
        let allocated = alloc_map.allocate_at(0);
        debug_assert!(allocated);
        let mut table_roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
        for (idx, root) in table_roots.iter_mut().enumerate() {
            root.table_id = idx as TableID;
        }
        MultiTableMetaPage {
            next_user_obj_id: next_user_obj_id.max(USER_OBJ_ID_START),
            table_roots,
            alloc_map,
            gc_page_list: vec![],
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
/// - meta pages are CoW-allocated and super pages point to active meta page,
/// - updates are published by writing new meta page then swapping active super page.
pub struct MultiTableFile(CoWFile<MultiTableActiveRoot>);

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
            CoWFile::open(file_path, io_client, buf_list)?
        } else {
            CoWFile::create(
                file_path,
                MULTI_TABLE_FILE_INITIAL_SIZE,
                io_client,
                buf_list,
                false,
            )?
        };

        let file = Arc::new(MultiTableFile(cow_file));

        if file_exists {
            let active_root = file.0.load_active_root().await?;
            let old_root = file.0.swap_active_root(active_root);
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

    /// Read one page from `catalog.mtb` using async direct IO.
    #[inline]
    pub async fn read_page(&self, page_id: u64) -> Result<DirectBuf> {
        self.0.read_page(page_id).await
    }
}

impl Deref for MultiTableFile {
    type Target = CoWFile<MultiTableActiveRoot>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl CoWFileOwner<MultiTableActiveRoot> for MultiTableFile {
    #[inline]
    fn cow_file(&self) -> &CoWFile<MultiTableActiveRoot> {
        &self.0
    }
}

/// Mutable wrapper for publishing one new multi-table checkpoint root.
pub struct MutableMultiTableFile {
    cow_file: MutableCoWFile<MultiTableFile, MultiTableActiveRoot>,
}

impl Deref for MutableMultiTableFile {
    type Target = MutableCoWFile<MultiTableFile, MultiTableActiveRoot>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cow_file
    }
}

impl DerefMut for MutableMultiTableFile {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cow_file
    }
}

impl MutableMultiTableFile {
    /// Create mutable handle with caller-provided root.
    #[inline]
    pub fn new(table_file: Arc<MultiTableFile>, new_root: MultiTableActiveRoot) -> Self {
        MutableMultiTableFile {
            cow_file: MutableCoWFile::new(table_file, new_root),
        }
    }

    /// Fork mutable handle from current active root.
    #[inline]
    pub fn fork(table_file: &Arc<MultiTableFile>) -> Self {
        MutableMultiTableFile {
            cow_file: MutableCoWFile::fork(table_file),
        }
    }

    /// Apply checkpoint metadata to mutable root.
    #[inline]
    pub fn apply_checkpoint_metadata(
        &mut self,
        checkpoint_cts: TrxID,
        next_user_obj_id: ObjID,
        table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    ) -> Result<()> {
        let root = self.cow_file.root_mut();
        if checkpoint_cts < root.checkpoint_cts {
            return Err(Error::InvalidArgument);
        }
        if next_user_obj_id < USER_OBJ_ID_START {
            return Err(Error::InvalidArgument);
        }

        root.checkpoint_cts = checkpoint_cts;
        root.meta.next_user_obj_id = next_user_obj_id;
        root.meta.table_roots = table_roots;
        Ok(())
    }

    /// Commit mutable root by writing meta page then ping-pong super page.
    #[inline]
    pub async fn commit(self) -> Result<(Arc<MultiTableFile>, Option<OldMultiTableRoot>)> {
        match self.cow_file.commit().await {
            Ok((table_file, old_root)) => Ok((table_file, old_root)),
            Err((_table_file, err)) => Err(err),
        }
    }
}

/// Active root of multi-table metadata file.
#[derive(Clone)]
pub struct MultiTableActiveRoot {
    /// Ping-pong super-page selector (0 or 1).
    pub page_no: u64,
    /// Checkpoint timestamp of this root.
    pub checkpoint_cts: TrxID,
    /// Meta-page id of this root.
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
            meta_page_id: 0,
            meta: MultiTableMetaPage::new(USER_OBJ_ID_START),
        }
    }

    /// Flip active root to the opposite ping-pong super page.
    #[inline]
    pub fn flip(&self) -> Self {
        let mut next = self.clone();
        next.page_no = 1 - self.page_no;
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

impl CoWRoot for MultiTableActiveRoot {
    type Meta = MultiTableMetaPage;

    #[inline]
    fn flip(&self) -> Self {
        MultiTableActiveRoot::flip(self)
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
        self.meta.gc_page_list.push(page_id);
    }

    #[inline]
    fn try_allocate_page_id(&mut self) -> Option<u64> {
        self.meta
            .alloc_map
            .try_allocate()
            .map(|page_id| page_id as u64)
    }

    #[inline]
    fn pick_super_page(buf: &[u8]) -> Result<SuperPage> {
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
    fn parse_meta_page(buf: &[u8]) -> Result<Self::Meta> {
        parse_multi_table_meta_page(buf)
    }

    #[inline]
    fn from_loaded(super_page: SuperPage, meta: Self::Meta) -> Result<Self> {
        let meta_page_id = super_page.body.meta_page_id;
        let meta_page_idx = usize::try_from(meta_page_id).map_err(|_| Error::InvalidFormat)?;
        if meta_page_idx == 0
            || meta_page_idx >= meta.alloc_map.len()
            || !meta.alloc_map.is_allocated(meta_page_idx)
        {
            return Err(Error::InvalidFormat);
        }

        Ok(MultiTableActiveRoot {
            page_no: super_page.header.page_no,
            checkpoint_cts: super_page.header.checkpoint_cts,
            meta_page_id,
            meta,
        })
    }

    #[inline]
    fn build_meta_page(&self) -> Result<DirectBuf> {
        build_meta_page(&self.meta)
    }

    #[inline]
    fn build_super_page(&self) -> Result<DirectBuf> {
        Ok(build_super_page(
            self.page_no,
            self.checkpoint_cts,
            self.meta_page_id,
        ))
    }
}

/// Guard object for reclaimed active roots.
pub type OldMultiTableRoot = OldCoWRoot<MultiTableActiveRoot>;

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
fn meta_page_ser_len(meta: &MultiTableMetaPage) -> usize {
    mem::size_of::<[u8; 8]>() // magic
        + mem::size_of::<u64>() // version
        + mem::size_of::<u64>() // next_user_obj_id
        + mem::size_of::<u32>() // table_root_count
        + mem::size_of::<u32>() // reserved
        + CATALOG_TABLE_ROOT_DESC_COUNT * mem::size_of::<CatalogTableRootDesc>()
        + meta.alloc_map.ser_len()
        + mem::size_of::<u32>() // gc count
        + mem::size_of::<u32>() // reserved
        + meta.gc_page_list.len() * mem::size_of::<u64>()
}

#[inline]
fn build_meta_page(meta: &MultiTableMetaPage) -> Result<DirectBuf> {
    let meta_len = meta_page_ser_len(meta);
    if meta_len > COW_FILE_PAGE_SIZE || meta.gc_page_list.len() > u32::MAX as usize {
        return Err(Error::InvalidState);
    }
    let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
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
    idx = meta.alloc_map.ser(buf.as_bytes_mut(), idx);
    idx = buf
        .as_bytes_mut()
        .ser_u32(idx, meta.gc_page_list.len() as u32);
    idx = buf.as_bytes_mut().ser_u32(idx, 0); // reserved
    for page_id in &meta.gc_page_list {
        idx = buf.as_bytes_mut().ser_u64(idx, *page_id);
    }
    debug_assert_eq!(idx, meta_len);
    Ok(buf)
}

fn parse_multi_table_meta_page(buf: &[u8]) -> Result<MultiTableMetaPage> {
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
    let (idx, alloc_map) = AllocMap::deser(buf, idx)?;
    if alloc_map.len() == 0 || !alloc_map.is_allocated(0) {
        return Err(Error::InvalidFormat);
    }
    let (idx, gc_count) = buf.deser_u32(idx)?;
    let (mut idx, _) = buf.deser_u32(idx)?; // reserved
    let mut gc_page_list = Vec::with_capacity(gc_count as usize);
    for _ in 0..gc_count {
        let (next_idx, page_id) = buf.deser_u64(idx)?;
        if page_id as usize >= alloc_map.len() {
            return Err(Error::InvalidFormat);
        }
        gc_page_list.push(page_id);
        idx = next_idx;
    }

    Ok(MultiTableMetaPage {
        next_user_obj_id,
        table_roots,
        alloc_map,
        gc_page_list,
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
            let meta_page_id_0 = mtb.active_root().meta_page_id;
            assert!(meta_page_id_0 > 0);

            let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots.iter_mut().enumerate() {
                root.table_id = idx as u64;
                root.root_page_id = (idx + 10) as u64;
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
            assert!(
                mtb.active_root()
                    .meta
                    .gc_page_list
                    .contains(&meta_page_id_0)
            );
            assert!(
                mtb.active_root()
                    .meta
                    .gc_page_list
                    .contains(&meta_page_id_1)
            );
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
                meta_offset + MULTI_TABLE_META_MAGIC_WORD.len() as u64,
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
}
