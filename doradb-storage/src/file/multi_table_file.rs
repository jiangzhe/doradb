use crate::bitmap::AllocMap;
use crate::buffer::{ReadonlyBackingFile, ReadonlyBufferPool};
use crate::catalog::{ObjID, TableID, USER_OBJ_ID_START};
use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind, Result, StorageOp};
use crate::file::block_integrity::{
    BLOCK_INTEGRITY_HEADER_SIZE, BlockIntegritySpec, max_payload_len, validate_block,
    write_block_checksum, write_block_header,
};
use crate::file::cow_file::{
    ActiveRoot as GenericActiveRoot, BlockID, COW_FILE_PAGE_SIZE, CowCodec, CowFile,
    MutableCowFile, OldCowRoot, ParsedMeta, SUPER_BLOCK_ID, validate_active_meta_block_id,
};
use crate::file::fs::BackgroundWriteRequest;
use crate::file::meta_block::{
    MULTI_TABLE_META_BLOCK_MAGIC_WORD, MultiTableMetaBlockData, MultiTableMetaBlockSerView,
};
use crate::file::super_block::{
    SUPER_BLOCK_FOOTER_OFFSET, SUPER_BLOCK_SIZE, SUPER_BLOCK_VERSION, SuperBlock, SuperBlockBody,
    SuperBlockFooter, SuperBlockHeader, SuperBlockSerView, parse_super_block,
};
use crate::file::table_file::TABLE_FILE_INITIAL_SIZE;
use crate::io::{DirectBuf, IOBuf, IOClient};
use crate::row::RowID;
use crate::serde::{Deser, Ser};
use crate::trx::{MIN_SNAPSHOT_TS, TrxID};
use std::io::ErrorKind;
use std::num::NonZeroU64;
use std::ops::Deref;
use std::sync::Arc;

pub use crate::file::CATALOG_MTB_FILE_ID;

/// On-disk format version of `catalog.mtb`.
pub const CATALOG_MTB_VERSION: u64 = 2;
/// Reserved number of catalog logical-table root descriptors.
pub const CATALOG_TABLE_ROOT_DESC_COUNT: usize = 4;
/// Initial sparse-file size for `catalog.mtb`.
pub const MULTI_TABLE_FILE_INITIAL_SIZE: usize = TABLE_FILE_INITIAL_SIZE;

const MULTI_TABLE_FILE_MAGIC_WORD: [u8; 8] = [b'D', b'O', b'R', b'A', b'M', b'T', b'B', 0];
const MULTI_TABLE_META_BLOCK_SPEC: BlockIntegritySpec =
    BlockIntegritySpec::new(MULTI_TABLE_META_BLOCK_MAGIC_WORD, CATALOG_MTB_VERSION);

/// Root descriptor reserved for one catalog logical table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatalogTableRootDesc {
    /// Catalog table id.
    pub table_id: TableID,
    /// Checkpointed persisted root block id for one catalog logical table.
    ///
    /// `None` means no checkpointed persisted root exists yet. On disk, the
    /// absent case is encoded as raw `0` because block `0` is reserved for the
    /// `catalog.mtb` super block and can never be a logical table root.
    pub root_block_id: Option<NonZeroU64>,
    /// Row-id boundary paired with `root_block_id`.
    pub pivot_row_id: RowID,
}

impl CatalogTableRootDesc {
    /// Returns the checkpointed persisted root block id, if one exists.
    #[inline]
    pub fn checkpoint_root_block_id(&self) -> Option<BlockID> {
        self.root_block_id
            .map(|block_id| BlockID::from(block_id.get()))
    }
}

/// File-specific payload persisted in `catalog.mtb` meta blocks.
///
/// Generic CoW bookkeeping fields (`alloc_map`, `gc_block_list`, `meta_block_id`)
/// are stored on the shared active root, not in this payload struct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiTableMetaBlock {
    /// Global next user object-id allocator watermark.
    pub next_user_obj_id: ObjID,
    /// Reserved root descriptors for catalog logical tables.
    pub table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
}

impl MultiTableMetaBlock {
    /// Create a meta payload initialized with allocator lower bound.
    #[inline]
    pub fn new(next_user_obj_id: ObjID) -> Self {
        let mut table_roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
        for (idx, root) in table_roots.iter_mut().enumerate() {
            root.table_id = idx as TableID;
        }
        MultiTableMetaBlock {
            next_user_obj_id: next_user_obj_id.max(USER_OBJ_ID_START),
            table_roots,
        }
    }
}

/// Active-root type for `catalog.mtb`.
pub type MultiTableActiveRoot = GenericActiveRoot<MultiTableMetaBlock>;

impl MultiTableActiveRoot {
    /// Create default active root for a newly created `catalog.mtb` file.
    #[inline]
    pub fn new() -> Self {
        let max_pages = MULTI_TABLE_FILE_INITIAL_SIZE / COW_FILE_PAGE_SIZE;
        let alloc_map = AllocMap::new(max_pages);
        let allocated = alloc_map.allocate_at(usize::from(SUPER_BLOCK_ID));
        debug_assert!(allocated);

        MultiTableActiveRoot::from_parts(
            0,
            MIN_SNAPSHOT_TS,
            SUPER_BLOCK_ID,
            alloc_map,
            vec![],
            MultiTableMetaBlock::new(USER_OBJ_ID_START),
        )
    }

    #[inline]
    pub fn meta_block_ser_view(&self) -> MultiTableMetaBlockSerView<'_> {
        MultiTableMetaBlockSerView::new(&self.meta, &self.alloc_map, &self.gc_block_list)
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
    /// Inclusive lower replay bound for catalog redo persisted in `catalog.mtb`.
    pub catalog_replay_start_ts: TrxID,
    /// Active meta-block payload.
    pub meta: MultiTableMetaBlock,
}

#[inline]
fn parse_multi_table_super_block(buf: &[u8]) -> Result<SuperBlock> {
    parse_super_block(buf, MULTI_TABLE_FILE_MAGIC_WORD, SUPER_BLOCK_VERSION)
}

#[inline]
fn build_multi_table_super_block(root: &MultiTableActiveRoot) -> Result<DirectBuf> {
    Ok(build_super_block(
        root.slot_no,
        root.trx_id,
        root.meta_block_id,
    ))
}

#[inline]
fn parse_multi_table_meta_block(
    page_id: BlockID,
    buf: &[u8],
) -> Result<ParsedMeta<MultiTableMetaBlock>> {
    let payload = validate_block(buf, MULTI_TABLE_META_BLOCK_SPEC).map_err(|cause| {
        Error::block_corrupted(
            FileKind::CatalogMultiTableFile,
            BlockKind::MultiTableMeta,
            page_id,
            cause,
        )
    })?;
    let (_, meta_block) = MultiTableMetaBlockData::deser(payload, 0).map_err(|err| match err {
        Error::InvalidFormat => Error::block_corrupted(
            FileKind::CatalogMultiTableFile,
            BlockKind::MultiTableMeta,
            page_id,
            BlockCorruptionCause::InvalidPayload,
        ),
        other => other,
    })?;

    Ok(ParsedMeta {
        meta: MultiTableMetaBlock {
            next_user_obj_id: meta_block.next_user_obj_id,
            table_roots: meta_block.table_roots,
        },
        alloc_map: meta_block.alloc_map,
        gc_block_list: meta_block.gc_block_list,
    })
}

#[inline]
fn validate_multi_table_root(
    meta_block_id: BlockID,
    parsed_meta: &ParsedMeta<MultiTableMetaBlock>,
) -> Result<()> {
    validate_active_meta_block_id(
        &parsed_meta.alloc_map,
        meta_block_id,
        FileKind::CatalogMultiTableFile,
        BlockKind::MultiTableMeta,
    )
}

#[inline]
fn build_multi_table_meta_block(root: &MultiTableActiveRoot) -> Result<DirectBuf> {
    let meta_block = root.meta_block_ser_view();
    let meta_len = meta_block.ser_len();
    if meta_len > max_payload_len(COW_FILE_PAGE_SIZE)
        || root.gc_block_list.len() > u32::MAX as usize
    {
        return Err(Error::InvalidState);
    }
    let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
    let idx = write_block_header(buf.as_bytes_mut(), MULTI_TABLE_META_BLOCK_SPEC);
    let idx = meta_block.ser(buf.as_bytes_mut(), idx);
    debug_assert_eq!(idx, BLOCK_INTEGRITY_HEADER_SIZE + meta_len);
    write_block_checksum(buf.as_bytes_mut());
    Ok(buf)
}

#[inline]
fn multi_table_codec() -> CowCodec<MultiTableMetaBlock> {
    CowCodec {
        parse_super_block: parse_multi_table_super_block,
        parse_meta_block: parse_multi_table_meta_block,
        validate_root: validate_multi_table_root,
        build_meta_block: build_multi_table_meta_block,
        build_super_block: build_multi_table_super_block,
    }
}

/// Persistent file facade for unified catalog metadata (`catalog.mtb`).
///
/// The file layout follows `TableFile` conventions:
/// - page 0 contains two ping-pong super blocks,
/// - meta blocks are CoW-allocated and super blocks point to active meta block,
/// - updates are published by writing new meta block then swapping active super block.
pub struct MultiTableFile {
    file: CowFile<MultiTableMetaBlock>,
}

pub(super) enum MultiTableFileOpenOutcome {
    Created(Arc<MultiTableFile>),
    Opened(Arc<MultiTableFile>),
}

impl MultiTableFile {
    /// Open existing file or create a new one, then load/publish initial active root.
    #[inline]
    pub(super) async fn open_or_create(
        file_path: impl AsRef<str>,
    ) -> Result<MultiTableFileOpenOutcome> {
        let file_path = file_path.as_ref();
        let file = match CowFile::create(
            file_path,
            MULTI_TABLE_FILE_INITIAL_SIZE,
            CATALOG_MTB_FILE_ID,
            multi_table_codec(),
            false,
        ) {
            Ok(file) => {
                return Ok(MultiTableFileOpenOutcome::Created(Arc::new(
                    MultiTableFile { file },
                )));
            }
            Err(Error::StorageIOError {
                op: StorageOp::FileCreate,
                kind: ErrorKind::AlreadyExists,
                ..
            }) => CowFile::open(file_path, CATALOG_MTB_FILE_ID, multi_table_codec())?,
            Err(err) => return Err(err),
        };

        Ok(MultiTableFileOpenOutcome::Opened(Arc::new(
            MultiTableFile { file },
        )))
    }

    /// Load the active root after validating the selected `catalog.mtb` meta block.
    #[inline]
    pub async fn load_active_root_from_pool(
        &self,
        disk_pool: &ReadonlyBufferPool,
    ) -> Result<MultiTableActiveRoot> {
        self.file.load_active_root_from_pool(disk_pool).await
    }

    /// Returns active-root snapshot from in-memory pointer without additional IO.
    #[inline]
    pub fn load_snapshot(&self) -> Result<MultiTableFileSnapshot> {
        let active_root = self.active_root();
        Ok(MultiTableFileSnapshot {
            catalog_replay_start_ts: active_root.trx_id,
            meta: active_root.meta.clone(),
        })
    }

    #[inline]
    pub(crate) async fn write_block(
        self: &Arc<Self>,
        block_id: BlockID,
        buf: DirectBuf,
        background_writes: &IOClient<BackgroundWriteRequest>,
    ) -> Result<()> {
        self.file
            .write_block_with_owner(
                background_writes,
                ReadonlyBackingFile::from(Arc::clone(self)),
                block_id,
                buf,
            )
            .await
    }

    #[inline]
    pub(crate) async fn publish_root(
        self: &Arc<Self>,
        new_root: MultiTableActiveRoot,
        background_writes: &IOClient<BackgroundWriteRequest>,
    ) -> Result<Option<OldMultiTableRoot>> {
        self.file
            .publish_root_with_owner(
                background_writes,
                ReadonlyBackingFile::from(Arc::clone(self)),
                new_root,
            )
            .await
    }

    /// Publish new checkpoint metadata atomically.
    #[inline]
    pub(crate) async fn publish_checkpoint(
        self: &Arc<Self>,
        catalog_replay_start_ts: TrxID,
        next_user_obj_id: ObjID,
        table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
        background_writes: &IOClient<BackgroundWriteRequest>,
    ) -> Result<()> {
        let mut mutable = MutableMultiTableFile::fork(self, background_writes);
        mutable.apply_checkpoint_metadata(
            catalog_replay_start_ts,
            next_user_obj_id,
            table_roots,
        )?;
        let (_, old_root) = mutable.commit().await?;
        drop(old_root);
        Ok(())
    }
}

impl Deref for MultiTableFile {
    type Target = CowFile<MultiTableMetaBlock>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

/// Mutable wrapper for publishing one new multi-table checkpoint root.
///
/// This mirrors `MutableTableFile` semantics for catalog file updates.
pub struct MutableMultiTableFile {
    file: Option<Arc<MultiTableFile>>,
    new_root: Option<MultiTableActiveRoot>,
    background_writes: IOClient<BackgroundWriteRequest>,
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
    fn background_writes(&self) -> &IOClient<BackgroundWriteRequest> {
        &self.background_writes
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
    pub(crate) fn new(
        table_file: Arc<MultiTableFile>,
        new_root: MultiTableActiveRoot,
        background_writes: &IOClient<BackgroundWriteRequest>,
    ) -> Self {
        table_file.claim_mutable_writer();
        MutableMultiTableFile {
            file: Some(table_file),
            new_root: Some(new_root),
            background_writes: background_writes.clone(),
            mutable_writer_claimed: true,
        }
    }

    /// Fork mutable handle from current active root.
    #[inline]
    pub(crate) fn fork(
        table_file: &Arc<MultiTableFile>,
        background_writes: &IOClient<BackgroundWriteRequest>,
    ) -> Self {
        table_file.claim_mutable_writer();
        MutableMultiTableFile {
            file: Some(Arc::clone(table_file)),
            new_root: Some(table_file.active_root().flip()),
            background_writes: background_writes.clone(),
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

    /// Allocate a new page id for copy-on-write updates.
    #[inline]
    pub fn allocate_block_id(&mut self) -> Result<BlockID> {
        self.new_root_mut()
            .try_allocate_block_id()
            .ok_or(Error::InvalidState)
    }

    /// Record an obsolete page id to be reclaimed after commit.
    #[inline]
    pub fn record_gc_block(&mut self, block_id: BlockID) {
        self.new_root_mut().gc_block_list.push(block_id);
    }

    /// Write one page into the underlying multi-table file.
    #[inline]
    pub(crate) async fn write_block(&self, block_id: BlockID, buf: DirectBuf) -> Result<()> {
        self.file_ref()
            .write_block(block_id, buf, self.background_writes())
            .await
    }

    /// Apply checkpoint metadata to mutable root.
    #[inline]
    pub fn apply_checkpoint_metadata(
        &mut self,
        catalog_replay_start_ts: TrxID,
        next_user_obj_id: ObjID,
        table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    ) -> Result<()> {
        let root = self.new_root_mut();
        if catalog_replay_start_ts < root.trx_id {
            return Err(Error::InvalidArgument);
        }
        if next_user_obj_id < USER_OBJ_ID_START {
            return Err(Error::InvalidArgument);
        }
        for root in &table_roots {
            if root.root_block_id.is_none() && root.pivot_row_id != 0 {
                return Err(Error::InvalidArgument);
            }
        }

        root.trx_id = catalog_replay_start_ts;
        root.next_user_obj_id = next_user_obj_id;
        root.table_roots = table_roots;
        Ok(())
    }

    /// Commit mutable root by writing meta block then ping-pong super block.
    #[inline]
    pub(crate) async fn commit(
        mut self,
    ) -> Result<(Arc<MultiTableFile>, Option<OldMultiTableRoot>)> {
        let new_root = self
            .new_root
            .take()
            .expect("mutable multi-table file has been consumed");
        let publish_res = self
            .file_ref()
            .publish_root(new_root, self.background_writes())
            .await;
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
    fn allocate_block_id(&mut self) -> Result<BlockID> {
        MutableMultiTableFile::allocate_block_id(self)
    }

    #[inline]
    fn record_gc_block(&mut self, block_id: BlockID) {
        MutableMultiTableFile::record_gc_block(self, block_id)
    }

    #[inline]
    fn write_block(
        &self,
        block_id: BlockID,
        buf: DirectBuf,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        MutableMultiTableFile::write_block(self, block_id, buf)
    }
}

/// Guard object for reclaimed replaced roots of `catalog.mtb`.
pub type OldMultiTableRoot = OldCowRoot<MultiTableMetaBlock>;

#[inline]
fn build_super_block(slot_no: u64, checkpoint_cts: TrxID, meta_block_id: BlockID) -> DirectBuf {
    let mut buf = DirectBuf::zeroed(SUPER_BLOCK_SIZE);
    let ser_view = SuperBlockSerView {
        header: SuperBlockHeader {
            magic_word: MULTI_TABLE_FILE_MAGIC_WORD,
            version: SUPER_BLOCK_VERSION,
            slot_no,
            checkpoint_cts,
        },
        body: SuperBlockBody { meta_block_id },
    };
    let ser_len = ser_view.ser_len();
    debug_assert!(ser_len <= SUPER_BLOCK_FOOTER_OFFSET);
    let ser_idx = ser_view.ser(buf.as_bytes_mut(), 0);
    debug_assert_eq!(ser_idx, ser_len);

    let b3sum = blake3::hash(&buf.as_bytes()[..SUPER_BLOCK_FOOTER_OFFSET]);
    let footer = SuperBlockFooter {
        b3sum: *b3sum.as_bytes(),
        checkpoint_cts,
    };
    let ser_idx = footer.ser(buf.as_bytes_mut(), SUPER_BLOCK_FOOTER_OFFSET);
    debug_assert_eq!(ser_idx, SUPER_BLOCK_SIZE);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::global_readonly_pool_scope;
    use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind};
    use crate::file::block_integrity::BLOCK_INTEGRITY_TRAILER_SIZE;
    use crate::file::test_block_id;
    use crate::file::{build_test_fs, build_test_fs_in};
    use crate::io::IOBuf;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::num::NonZeroU64;

    fn overwrite_file_bytes(path: &str, offset: u64, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }

    fn assert_multi_table_meta_corruption(
        err: Error,
        page_id: BlockID,
        cause: BlockCorruptionCause,
    ) {
        assert!(matches!(
            err,
            Error::BlockCorrupted {
                file_kind: FileKind::CatalogMultiTableFile,
                block_kind: BlockKind::MultiTableMeta,
                block_id: actual_page_id,
                cause: actual_cause,
            } if actual_page_id == page_id && actual_cause == cause
        ));
    }

    #[test]
    fn test_multi_table_file_open_publish_and_reload() {
        smol::block_on(async {
            let (_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let path = fs.catalog_mtb_file_path();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);

            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let s0 = mtb.load_snapshot().unwrap();
            assert_eq!(s0.catalog_replay_start_ts, MIN_SNAPSHOT_TS);
            assert_eq!(s0.meta.next_user_obj_id, USER_OBJ_ID_START);
            let meta_block_id_0 = mtb.active_root().meta_block_id;
            assert!(meta_block_id_0 > test_block_id(0));

            let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots.iter_mut().enumerate() {
                root.table_id = idx as u64;
                root.root_block_id = NonZeroU64::new((idx + 10) as u64);
                root.pivot_row_id = (idx * 100) as u64;
            }
            mtb.publish_checkpoint(7, USER_OBJ_ID_START + 16, roots, background_writes)
                .await
                .unwrap();
            let meta_block_id_1 = mtb.active_root().meta_block_id;
            assert_ne!(meta_block_id_0, meta_block_id_1);
            drop(mtb);

            let (mtb2, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let s1 = mtb2.load_snapshot().unwrap();
            assert_eq!(s1.catalog_replay_start_ts, 7);
            assert_eq!(s1.meta.next_user_obj_id, USER_OBJ_ID_START + 16);
            assert_eq!(s1.meta.table_roots, roots);

            drop(mtb2);
            drop(fs);
            let _ = std::fs::remove_file(path);
        });
    }

    #[test]
    fn test_multi_table_file_meta_block_copy_on_write() {
        smol::block_on(async {
            let (_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots.iter_mut().enumerate() {
                root.table_id = idx as u64;
            }

            let meta_block_id_0 = mtb.active_root().meta_block_id;
            mtb.publish_checkpoint(3, USER_OBJ_ID_START + 1, roots, background_writes)
                .await
                .unwrap();
            let meta_block_id_1 = mtb.active_root().meta_block_id;
            mtb.publish_checkpoint(4, USER_OBJ_ID_START + 2, roots, background_writes)
                .await
                .unwrap();
            let meta_block_id_2 = mtb.active_root().meta_block_id;

            assert_ne!(meta_block_id_0, meta_block_id_1);
            assert_ne!(meta_block_id_1, meta_block_id_2);
            assert!(mtb.active_root().gc_block_list.contains(&meta_block_id_0));
            assert!(mtb.active_root().gc_block_list.contains(&meta_block_id_1));
        });
    }

    #[test]
    fn test_multi_table_file_rejects_super_block_version_mismatch() {
        smol::block_on(async {
            let (dir, fs) = build_test_fs();
            let path = fs.catalog_mtb_file_path();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            drop(mtb);
            drop(fs);

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            // overwrite both super-block versions to simulate format mismatch.
            file.seek(SeekFrom::Start(MULTI_TABLE_FILE_MAGIC_WORD.len() as u64))
                .unwrap();
            file.write_all(&2u64.to_le_bytes()).unwrap();
            file.seek(SeekFrom::Start(
                SUPER_BLOCK_SIZE as u64 + MULTI_TABLE_FILE_MAGIC_WORD.len() as u64,
            ))
            .unwrap();
            file.write_all(&2u64.to_le_bytes()).unwrap();
            file.sync_all().unwrap();

            let fs = build_test_fs_in(dir.path());
            let res = fs.open_or_create_multi_table_file(global.guard()).await;
            assert!(res.is_err());
        });
    }

    #[test]
    fn test_multi_table_file_rejects_meta_version_mismatch() {
        smol::block_on(async {
            let (dir, fs) = build_test_fs();
            let path = fs.catalog_mtb_file_path();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let active_meta_block_id = mtb.active_root().meta_block_id;
            drop(mtb);
            drop(fs);

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            // overwrite meta-block version to simulate format mismatch.
            let meta_offset = u64::from(active_meta_block_id) * COW_FILE_PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(
                meta_offset + MULTI_TABLE_META_BLOCK_MAGIC_WORD.len() as u64,
            ))
            .unwrap();
            file.write_all(&(CATALOG_MTB_VERSION + 1).to_le_bytes())
                .unwrap();
            file.sync_all().unwrap();

            let fs = build_test_fs_in(dir.path());
            let err = match fs.open_or_create_multi_table_file(global.guard()).await {
                Ok(_) => panic!("expected multi-table meta version corruption"),
                Err(err) => err,
            };
            assert_multi_table_meta_corruption(
                err,
                active_meta_block_id,
                BlockCorruptionCause::InvalidVersion,
            );
        });
    }

    #[test]
    fn test_multi_table_file_rejects_meta_checksum_corruption() {
        smol::block_on(async {
            let (dir, fs) = build_test_fs();
            let path = fs.catalog_mtb_file_path();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let active_meta_block_id = mtb.active_root().meta_block_id;
            drop(mtb);
            drop(fs);

            let checksum_offset = u64::from(active_meta_block_id) * COW_FILE_PAGE_SIZE as u64
                + (COW_FILE_PAGE_SIZE - BLOCK_INTEGRITY_TRAILER_SIZE) as u64;
            overwrite_file_bytes(&path, checksum_offset, &[0xff]);

            let fs = build_test_fs_in(dir.path());
            let err = match fs.open_or_create_multi_table_file(global.guard()).await {
                Ok(_) => panic!("expected multi-table meta checksum corruption"),
                Err(err) => err,
            };
            assert_multi_table_meta_corruption(
                err,
                active_meta_block_id,
                BlockCorruptionCause::ChecksumMismatch,
            );
        });
    }

    #[test]
    fn test_multi_table_file_falls_back_to_older_valid_super_slot() {
        smol::block_on(async {
            let (dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let path = fs.catalog_mtb_file_path();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            let mut roots_v1 = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots_v1.iter_mut().enumerate() {
                root.table_id = idx as u64;
                root.root_block_id = NonZeroU64::new((idx + 10) as u64);
                root.pivot_row_id = (idx as u64) + 1;
            }
            mtb.publish_checkpoint(3, USER_OBJ_ID_START + 1, roots_v1, background_writes)
                .await
                .unwrap();
            let older_snapshot = mtb.load_snapshot().unwrap();

            let mut roots_v2 = roots_v1;
            roots_v2[0].root_block_id = NonZeroU64::new(99);
            roots_v2[0].pivot_row_id = 100;
            mtb.publish_checkpoint(4, USER_OBJ_ID_START + 2, roots_v2, background_writes)
                .await
                .unwrap();
            let active_super_slot = mtb.active_root().slot_no;
            drop(mtb);
            drop(fs);

            let version_offset = active_super_slot * SUPER_BLOCK_SIZE as u64
                + MULTI_TABLE_FILE_MAGIC_WORD.len() as u64;
            overwrite_file_bytes(&path, version_offset, &2u64.to_le_bytes());

            let fs = build_test_fs_in(dir.path());
            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let snapshot = mtb.load_snapshot().unwrap();
            assert_eq!(
                snapshot.catalog_replay_start_ts,
                older_snapshot.catalog_replay_start_ts
            );
            assert_eq!(snapshot.meta, older_snapshot.meta);
        });
    }

    #[test]
    fn test_multi_table_file_does_not_fall_back_when_newest_meta_root_is_invalid() {
        smol::block_on(async {
            let (dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let path = fs.catalog_mtb_file_path();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            let mut roots_v1 = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots_v1.iter_mut().enumerate() {
                root.table_id = idx as u64;
                root.root_block_id = NonZeroU64::new((idx + 20) as u64);
                root.pivot_row_id = (idx as u64) + 10;
            }
            mtb.publish_checkpoint(5, USER_OBJ_ID_START + 10, roots_v1, background_writes)
                .await
                .unwrap();

            let mut roots_v2 = roots_v1;
            roots_v2[1].root_block_id = NonZeroU64::new(123);
            roots_v2[1].pivot_row_id = 222;
            mtb.publish_checkpoint(6, USER_OBJ_ID_START + 11, roots_v2, background_writes)
                .await
                .unwrap();
            let active_meta_block_id = mtb.active_root().meta_block_id;
            drop(mtb);
            drop(fs);

            let file_bytes = std::fs::read(&path).unwrap();
            let meta_offset = usize::from(active_meta_block_id) * COW_FILE_PAGE_SIZE;
            let meta_end = meta_offset + COW_FILE_PAGE_SIZE;
            let parsed_meta = parse_multi_table_meta_block(
                active_meta_block_id,
                &file_bytes[meta_offset..meta_end],
            )
            .unwrap();
            assert!(
                parsed_meta
                    .alloc_map
                    .deallocate(usize::from(active_meta_block_id))
            );
            let invalid_root = MultiTableActiveRoot::from_parts(
                0,
                0,
                active_meta_block_id,
                parsed_meta.alloc_map,
                parsed_meta.gc_block_list,
                parsed_meta.meta,
            );
            let invalid_meta_buf = build_multi_table_meta_block(&invalid_root).unwrap();
            overwrite_file_bytes(
                &path,
                u64::from(active_meta_block_id) * COW_FILE_PAGE_SIZE as u64,
                invalid_meta_buf.as_bytes(),
            );

            let fs = build_test_fs_in(dir.path());
            let err = match fs.open_or_create_multi_table_file(global.guard()).await {
                Ok(_) => panic!("expected newest multi-table root invariant failure"),
                Err(err) => err,
            };
            assert_multi_table_meta_corruption(
                err,
                active_meta_block_id,
                BlockCorruptionCause::InvalidRootInvariant,
            );
        });
    }

    #[test]
    #[should_panic(expected = "concurrent mutable CoW file modification is not allowed")]
    fn test_multi_table_file_rejects_concurrent_fork() {
        smol::block_on(async {
            let (_dir, fs) = build_test_fs();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            let _first = MutableMultiTableFile::fork(&mtb, fs.background_writes());
            let _second = MutableMultiTableFile::fork(&mtb, fs.background_writes());
        });
    }

    #[test]
    fn test_multi_table_file_allows_fork_after_drop() {
        smol::block_on(async {
            let (_dir, fs) = build_test_fs();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let (mtb, _) = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            let first = MutableMultiTableFile::fork(&mtb, fs.background_writes());
            drop(first);

            let _second = MutableMultiTableFile::fork(&mtb, fs.background_writes());
        });
    }
}
