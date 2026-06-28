use crate::bitmap::AllocMap;
use crate::buffer::ReadonlyBufferPool;
use crate::catalog::USER_OBJ_ID_START;
use crate::error::{
    DataIntegrityError, Error, FileKind, InternalError, IoError, ResourceError, Result,
};
use crate::file::SparseFile;
use crate::file::block_integrity::{
    BLOCK_INTEGRITY_HEADER_SIZE, BlockIntegritySpec, max_payload_len, validate_block,
    write_block_checksum, write_block_header,
};
use crate::file::cow_file::{
    ActiveRoot as GenericActiveRoot, COW_FILE_PAGE_SIZE, CowCodec, CowFile, CowWriteBarrier,
    MutableCowFile, MutableCowRoot, MutableWriterClaim, MutableWriterFile, OldCowRoot, ParsedMeta,
    SUPER_BLOCK_ID, allocate_cow_block, validate_active_meta_block_id,
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
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::io::{DirectBuf, IOBuf, IOClient};
use crate::quiescent::QuiescentGuard;
use crate::serde::{Deser, Ser};
use crate::trx::MIN_SNAPSHOT_TS;
use error_stack::{Report, ResultExt};
use std::collections::BTreeSet;
use std::io::ErrorKind as IoErrorKind;
use std::num::NonZeroU64;
use std::sync::Arc;

pub(crate) use crate::file::CATALOG_MTB_FILE_ID;

#[cfg(test)]
pub(crate) use tests::publish_first_redo_log_seq_for_test;

/// On-disk format version of `catalog.mtb`.
pub(crate) const CATALOG_MTB_VERSION: u64 = 3;
/// Reserved number of catalog logical-table root descriptors.
pub(crate) const CATALOG_TABLE_ROOT_DESC_COUNT: usize = 4;
/// Initial sparse-file size for `catalog.mtb`.
pub(crate) const MULTI_TABLE_FILE_INITIAL_SIZE: usize = TABLE_FILE_INITIAL_SIZE;

const MULTI_TABLE_FILE_MAGIC_WORD: [u8; 8] = [b'D', b'O', b'R', b'A', b'M', b'T', b'B', 0];
const MULTI_TABLE_META_BLOCK_SPEC: BlockIntegritySpec =
    BlockIntegritySpec::new(MULTI_TABLE_META_BLOCK_MAGIC_WORD, CATALOG_MTB_VERSION);

/// Root descriptor reserved for one catalog logical table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct CatalogTableRootDesc {
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
    pub(crate) fn checkpoint_root_block_id(&self) -> Option<BlockID> {
        self.root_block_id
            .map(|block_id| BlockID::from(block_id.get()))
    }
}

/// File-specific payload persisted in `catalog.mtb` meta blocks.
///
/// Generic CoW bookkeeping fields (`alloc_map`, `meta_block_id`) are stored on
/// the shared active root, not in this payload struct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultiTableMetaBlock {
    /// Global next table-id allocator watermark.
    pub(crate) next_table_id: TableID,
    /// First redo log file sequence retained for recovery.
    pub(crate) first_redo_log_seq: u32,
    /// Reserved root descriptors for catalog logical tables.
    pub(crate) table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
}

impl MultiTableMetaBlock {
    /// Create a meta payload initialized with allocator lower bound.
    #[inline]
    pub(crate) fn new(next_table_id: TableID) -> Self {
        let mut table_roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
        for (idx, root) in table_roots.iter_mut().enumerate() {
            root.table_id = TableID::from(idx);
        }
        MultiTableMetaBlock {
            next_table_id: next_table_id.max(USER_OBJ_ID_START),
            first_redo_log_seq: 0,
            table_roots,
        }
    }
}

/// Active-root type for `catalog.mtb`.
pub(crate) type MultiTableActiveRoot = GenericActiveRoot<MultiTableMetaBlock>;

impl MultiTableActiveRoot {
    /// Create default active root for a newly created `catalog.mtb` file.
    #[inline]
    pub(crate) fn new() -> Self {
        let max_pages = MULTI_TABLE_FILE_INITIAL_SIZE / COW_FILE_PAGE_SIZE;
        let alloc_map = AllocMap::new(max_pages);
        let allocated = alloc_map.allocate_at(usize::from(SUPER_BLOCK_ID));
        debug_assert!(allocated);

        MultiTableActiveRoot::from_parts(
            0,
            MIN_SNAPSHOT_TS,
            SUPER_BLOCK_ID,
            alloc_map,
            MultiTableMetaBlock::new(USER_OBJ_ID_START),
        )
    }

    /// Build meta-block serialization view for the current catalog root.
    #[inline]
    pub(crate) fn meta_block_ser_view(&self) -> MultiTableMetaBlockSerView<'_> {
        MultiTableMetaBlockSerView::new(&self.meta, &self.alloc_map)
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
pub(crate) struct MultiTableFileSnapshot {
    /// Inclusive lower replay bound for catalog redo persisted in `catalog.mtb`.
    pub(crate) catalog_replay_start_ts: TrxID,
    /// Active meta block id referenced by the loaded catalog root.
    pub(crate) meta_block_id: BlockID,
    /// Active meta-block payload.
    pub(crate) meta: MultiTableMetaBlock,
}

pub(super) enum MultiTableFileOpenOutcome {
    Created(Arc<MultiTableFile>),
    Opened(Arc<MultiTableFile>),
}

/// Persistent file facade for unified catalog metadata (`catalog.mtb`).
///
/// The file layout follows `TableFile` conventions:
/// - page 0 contains two ping-pong super blocks,
/// - meta blocks are CoW-allocated and super blocks point to active meta block,
/// - updates are published by writing new meta block then swapping active super block.
pub(crate) struct MultiTableFile {
    file: CowFile<MultiTableMetaBlock>,
}

impl MultiTableFile {
    /// Open existing file or create a new one, returning which case occurred.
    ///
    /// Initial root loading or first-root publish is handled by
    /// `FileSystem::open_or_create_multi_table_file()`.
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
            Err(err)
                if err
                    .report()
                    .downcast_ref::<IoError>()
                    .copied()
                    .is_some_and(|err| err.kind() == IoErrorKind::AlreadyExists) =>
            {
                CowFile::open(file_path, CATALOG_MTB_FILE_ID, multi_table_codec())?
            }
            Err(err) => return Err(err),
        };

        Ok(MultiTableFileOpenOutcome::Opened(Arc::new(
            MultiTableFile { file },
        )))
    }

    /// Load the active root after validating the selected `catalog.mtb` meta block.
    #[inline]
    pub(crate) async fn load_active_root_from_pool(
        &self,
        disk_pool: &QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<MultiTableActiveRoot> {
        self.file
            .load_active_root_from_pool(FileKind::CatalogMultiTableFile, disk_pool)
            .await
    }

    /// Returns the in-memory active catalog root.
    ///
    /// Catalog checkpoint roots are outside the user-table `TableRootSnapshot`
    /// contract. Callers that need multiple catalog-root fields must still bind
    /// one local root reference and reuse it.
    #[inline]
    pub(crate) fn active_root_unchecked(&self) -> &MultiTableActiveRoot {
        self.file.active_root_unchecked()
    }

    /// Returns active-root snapshot from in-memory pointer without additional IO.
    #[inline]
    pub(crate) fn load_snapshot(&self) -> Result<MultiTableFileSnapshot> {
        let active_root = self.active_root_unchecked();
        Ok(MultiTableFileSnapshot {
            catalog_replay_start_ts: active_root.root_ts,
            meta_block_id: active_root.meta_block_id,
            meta: active_root.meta.clone(),
        })
    }

    /// Returns this file's storage kind for validation and cache identity.
    #[inline]
    pub(crate) fn file_kind(&self) -> FileKind {
        FileKind::CatalogMultiTableFile
    }

    /// Returns the backing sparse file.
    #[inline]
    pub(crate) fn sparse_file(&self) -> &Arc<SparseFile> {
        self.file.sparse_file()
    }

    #[inline]
    pub(super) fn install_loaded_root(
        &self,
        active_root: MultiTableActiveRoot,
    ) -> Option<OldMultiTableRoot> {
        self.file.swap_active_root(active_root)
    }

    #[inline]
    fn file(&self) -> &CowFile<MultiTableMetaBlock> {
        &self.file
    }
}

impl MutableWriterFile for MultiTableFile {
    #[inline]
    fn claim_mutable_writer(&self) {
        self.file.claim_mutable_writer();
    }

    #[inline]
    fn release_mutable_writer(&self) {
        self.file.release_mutable_writer();
    }
}

/// Mutable wrapper for publishing one new multi-table checkpoint root.
///
/// This mirrors `MutableTableFile` semantics for catalog file updates.
pub(crate) struct MutableMultiTableFile {
    pub(super) file: Arc<MultiTableFile>,
    pub(super) new_root: MutableCowRoot<MultiTableMetaBlock>,
    background_writes: IOClient<BackgroundWriteRequest>,
    writer_claim: MutableWriterClaim<MultiTableFile>,
}

impl MutableMultiTableFile {
    /// Create mutable handle with caller-provided root.
    #[inline]
    pub(crate) fn new(
        table_file: Arc<MultiTableFile>,
        new_root: MultiTableActiveRoot,
        background_writes: &IOClient<BackgroundWriteRequest>,
    ) -> Self {
        let writer_claim = MutableWriterClaim::new(&table_file);
        MutableMultiTableFile {
            file: table_file,
            new_root: MutableCowRoot::from_root(new_root),
            background_writes: background_writes.clone(),
            writer_claim,
        }
    }

    /// Fork mutable handle from current active root.
    #[inline]
    pub(crate) fn fork(
        table_file: &Arc<MultiTableFile>,
        background_writes: &IOClient<BackgroundWriteRequest>,
    ) -> Self {
        let writer_claim = MutableWriterClaim::new(table_file);
        MutableMultiTableFile {
            file: Arc::clone(table_file),
            new_root: MutableCowRoot::fork(table_file.active_root_unchecked()),
            background_writes: background_writes.clone(),
            writer_claim,
        }
    }

    /// Apply checkpoint metadata to mutable root.
    #[inline]
    pub(crate) fn apply_checkpoint_metadata(
        &mut self,
        catalog_replay_start_ts: TrxID,
        next_table_id: TableID,
        table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    ) -> Result<()> {
        let root = &mut self.new_root.root;
        if catalog_replay_start_ts < root.root_ts {
            return Err(mutable_root_metadata_regression(format!(
                "catalog replay start regressed: current={}, new={catalog_replay_start_ts}",
                root.root_ts
            )));
        }
        if next_table_id < USER_OBJ_ID_START {
            return Err(catalog_root_descriptor_invariant(format!(
                "next_table_id={next_table_id}, minimum={USER_OBJ_ID_START}"
            )));
        }
        for root in &table_roots {
            if root.root_block_id.is_none() && root.pivot_row_id != RowID::new(0) {
                return Err(catalog_root_descriptor_invariant(format!(
                    "table_id={}, root block missing but pivot_row_id={}",
                    root.table_id, root.pivot_row_id
                )));
            }
        }

        root.root_ts = catalog_replay_start_ts;
        root.next_table_id = next_table_id;
        root.table_roots = table_roots;
        Ok(())
    }

    /// Apply a first-retained redo marker to the mutable catalog root.
    #[inline]
    pub(crate) fn apply_first_redo_log_seq(&mut self, first_redo_log_seq: u32) -> Result<bool> {
        let root = &mut self.new_root.root;
        if first_redo_log_seq <= root.first_redo_log_seq {
            return Ok(false);
        }
        root.first_redo_log_seq = first_redo_log_seq;
        Ok(true)
    }

    /// Returns immutable reference to the mutable catalog root snapshot.
    #[inline]
    pub(crate) fn root(&self) -> &MultiTableActiveRoot {
        &self.new_root.root
    }

    /// Reserve the final meta block for a catalog checkpoint publication.
    #[inline]
    pub(crate) fn reserve_publish_meta_block(&mut self) -> Result<BlockID> {
        self.new_root
            .reserve_publish_meta_block("multi-table publish root could not allocate meta block")
    }

    /// Reserve the final meta block and clear the displaced active meta block.
    ///
    /// This is the fast path for checkpoints that only advance catalog overlay
    /// metadata. Catalog logical-table roots and their data blocks are kept
    /// from the inherited allocation map; only the previous meta block becomes
    /// unreachable once the new root is published.
    #[inline]
    pub(crate) fn reserve_publish_meta_block_reclaiming_displaced_meta(
        &mut self,
        displaced_meta_block_id: BlockID,
    ) -> Result<BlockID> {
        let meta_block_id = self.reserve_publish_meta_block()?;
        if displaced_meta_block_id == SUPER_BLOCK_ID {
            return Ok(meta_block_id);
        }
        if displaced_meta_block_id == meta_block_id {
            return Err(Report::new(InternalError::CowFileAllocationInvariant)
                .attach(format!(
                    "displaced meta block matches publish meta block: block_id={meta_block_id}"
                ))
                .into());
        }
        let displaced_meta_idx = usize::from(displaced_meta_block_id);
        if displaced_meta_idx >= self.new_root.root.alloc_map.len()
            || !self.new_root.root.alloc_map.deallocate(displaced_meta_idx)
        {
            return Err(Report::new(InternalError::CowFileAllocationInvariant)
                .attach(format!(
                    "displaced meta block is not allocated: block_id={displaced_meta_block_id}, alloc_map_len={}",
                    self.new_root.root.alloc_map.len()
                ))
                .into());
        }
        Ok(meta_block_id)
    }

    /// Rebuild the mutable root allocation map from root-reachable blocks.
    ///
    /// Returns the number of allocation bits cleared by the rebuild.
    #[inline]
    pub(crate) fn rebuild_alloc_map_from_reachable(
        &mut self,
        reachable: &BTreeSet<BlockID>,
    ) -> Result<usize> {
        self.new_root.rebuild_alloc_map_from_reachable(reachable)
    }

    /// Commit mutable root by writing meta block then ping-pong super block.
    #[inline]
    pub(crate) async fn commit(self) -> Result<(Arc<MultiTableFile>, Option<OldMultiTableRoot>)> {
        let MutableMultiTableFile {
            file,
            new_root,
            background_writes,
            writer_claim,
        } = self;
        let publish_res = file
            .file()
            .publish_root(&background_writes, new_root, CowWriteBarrier::Disabled)
            .await;
        drop(writer_claim);
        let old_root = publish_res?;
        Ok((file, old_root))
    }

    /// Commit a mutable root whose final meta block was already reserved.
    #[inline]
    pub(crate) async fn commit_prepared(
        self,
    ) -> Result<(Arc<MultiTableFile>, Option<OldMultiTableRoot>)> {
        let MutableMultiTableFile {
            file,
            new_root,
            background_writes,
            writer_claim,
        } = self;
        let publish_res = file
            .file()
            .publish_prepared_root(&background_writes, new_root, CowWriteBarrier::Disabled)
            .await;
        drop(writer_claim);
        let old_root = publish_res?;
        Ok((file, old_root))
    }
}

impl MutableCowFile for MutableMultiTableFile {
    #[inline]
    fn allocate_block(&mut self) -> Result<BlockID> {
        allocate_cow_block(
            &mut self.new_root,
            "multi-table file could not allocate block",
        )
    }

    #[inline]
    fn rollback_allocated_block(&mut self, block_id: BlockID) -> Result<()> {
        self.new_root.rollback_allocated_block(block_id)
    }

    #[inline]
    async fn write_block(&self, block_id: BlockID, buf: DirectBuf) -> Result<()> {
        self.file
            .file()
            .write_block(&self.background_writes, block_id, buf)
            .await
    }
}

/// Guard object for reclaimed replaced roots of `catalog.mtb`.
pub(crate) type OldMultiTableRoot = OldCowRoot<MultiTableMetaBlock>;

#[inline]
fn mutable_root_metadata_regression(message: impl Into<String>) -> Error {
    Report::new(InternalError::MutableRootMetadataRegression)
        .attach(message.into())
        .into()
}

#[inline]
fn catalog_root_descriptor_invariant(message: impl Into<String>) -> Error {
    Report::new(InternalError::CatalogRootDescriptorInvariant)
        .attach(message.into())
        .into()
}

#[inline]
fn parse_multi_table_super_block(buf: &[u8]) -> Result<SuperBlock> {
    parse_super_block(buf, MULTI_TABLE_FILE_MAGIC_WORD, SUPER_BLOCK_VERSION)
}

#[inline]
fn build_multi_table_super_block(root: &MultiTableActiveRoot) -> Result<DirectBuf> {
    Ok(build_super_block(
        root.slot_no,
        root.root_ts,
        root.meta_block_id,
    ))
}

#[inline]
fn parse_multi_table_meta_block(
    page_id: BlockID,
    buf: &[u8],
) -> Result<ParsedMeta<MultiTableMetaBlock>> {
    let payload = validate_block(buf, MULTI_TABLE_META_BLOCK_SPEC)
        .attach_with(|| {
            format!(
                "file={}, block=multi-table-meta, block_id={page_id}",
                FileKind::CatalogMultiTableFile
            )
        })
        .map_err(Error::from)?;
    let (_, meta_block) = MultiTableMetaBlockData::deser(payload, 0).map_err(|err| {
        if err.data_integrity_error().is_some() {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "file={}, block=multi-table-meta, block_id={page_id}",
                    FileKind::CatalogMultiTableFile
                ))
                .into()
        } else {
            err
        }
    })?;

    Ok(ParsedMeta {
        meta: MultiTableMetaBlock {
            next_table_id: meta_block.next_table_id,
            first_redo_log_seq: meta_block.first_redo_log_seq,
            table_roots: meta_block.table_roots,
        },
        alloc_map: meta_block.alloc_map,
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
        "multi-table-meta",
    )
}

#[inline]
fn build_multi_table_meta_block(root: &MultiTableActiveRoot) -> Result<DirectBuf> {
    let meta_block = root.meta_block_ser_view();
    let meta_len = meta_block.ser_len();
    if meta_len > max_payload_len(COW_FILE_PAGE_SIZE) {
        return Err(Report::new(ResourceError::StorageFileCapacityExceeded)
            .attach(format!(
                "multi-table meta block payload too large: actual_bytes={meta_len}, max_bytes={}",
                max_payload_len(COW_FILE_PAGE_SIZE)
            ))
            .into());
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
    use crate::error::{DataIntegrityError, Error};
    use crate::file::block_integrity::BLOCK_INTEGRITY_TRAILER_SIZE;
    use crate::file::test_block_id;
    use crate::file::{build_test_fs, build_test_fs_in};
    use crate::io::IOBuf;
    use std::fs::{OpenOptions, read, remove_file};
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
        expected: DataIntegrityError,
    ) {
        assert_eq!(err.data_integrity_error(), Some(expected));
        let report = format!("{err:?}");
        assert!(report.contains("catalog.mtb"), "{report}");
        assert!(report.contains("multi-table-meta"), "{report}");
        assert!(report.contains(&format!("block_id={page_id}")), "{report}");
    }

    async fn publish_checkpoint_for_test(
        mtb: &Arc<MultiTableFile>,
        background_writes: &IOClient<BackgroundWriteRequest>,
        catalog_replay_start_ts: TrxID,
        next_table_id: TableID,
        table_roots: [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
    ) {
        let mut mutable = MutableMultiTableFile::fork(mtb, background_writes);
        mutable
            .apply_checkpoint_metadata(catalog_replay_start_ts, next_table_id, table_roots)
            .unwrap();
        let (_, old_root) = mutable.commit().await.unwrap();
        drop(old_root);
    }

    pub(crate) async fn publish_first_redo_log_seq_for_test(
        mtb: &Arc<MultiTableFile>,
        background_writes: &IOClient<BackgroundWriteRequest>,
        first_redo_log_seq: u32,
    ) -> Result<()> {
        let snapshot = mtb.load_snapshot()?;
        let mut mutable = MutableMultiTableFile::fork(mtb, background_writes);
        mutable.apply_checkpoint_metadata(
            snapshot.catalog_replay_start_ts,
            snapshot.meta.next_table_id,
            snapshot.meta.table_roots,
        )?;
        mutable.new_root.root.first_redo_log_seq = first_redo_log_seq;
        mutable.reserve_publish_meta_block_reclaiming_displaced_meta(snapshot.meta_block_id)?;
        let (_, old_root) = mutable.commit_prepared().await?;
        drop(old_root);
        Ok(())
    }

    #[test]
    fn test_multi_table_file_open_publish_and_reload() {
        smol::block_on(async {
            let (_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let path = fs.catalog_mtb_file_path();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);

            let mtb = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let s0 = mtb.load_snapshot().unwrap();
            assert_eq!(s0.catalog_replay_start_ts, MIN_SNAPSHOT_TS);
            assert_eq!(s0.meta.next_table_id, USER_OBJ_ID_START);
            assert_eq!(s0.meta.first_redo_log_seq, 0);
            let meta_block_id_0 = mtb.active_root_unchecked().meta_block_id;
            assert!(meta_block_id_0 > test_block_id(0));

            let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots.iter_mut().enumerate() {
                root.table_id = TableID::new(idx as u64);
                root.root_block_id = NonZeroU64::new((idx + 10) as u64);
                root.pivot_row_id = RowID::new((idx * 100) as u64);
            }
            publish_checkpoint_for_test(
                &mtb,
                background_writes,
                TrxID::new(7),
                USER_OBJ_ID_START + 16,
                roots,
            )
            .await;
            let meta_block_id_1 = mtb.active_root_unchecked().meta_block_id;
            assert_ne!(meta_block_id_0, meta_block_id_1);
            drop(mtb);

            let mtb2 = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let s1 = mtb2.load_snapshot().unwrap();
            assert_eq!(s1.catalog_replay_start_ts, TrxID::new(7));
            assert_eq!(s1.meta.next_table_id, USER_OBJ_ID_START + 16);
            assert_eq!(s1.meta.first_redo_log_seq, 0);
            assert_eq!(s1.meta.table_roots, roots);

            drop(mtb2);
            drop(fs);
            let _ = remove_file(path);
        });
    }

    #[test]
    fn test_multi_table_file_metadata_checkpoint_preserves_first_redo_log_seq() {
        smol::block_on(async {
            let (dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let mtb = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            publish_first_redo_log_seq_for_test(&mtb, background_writes, 3)
                .await
                .unwrap();
            let marker_snapshot = mtb.load_snapshot().unwrap();
            assert_eq!(marker_snapshot.meta.first_redo_log_seq, 3);

            let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots.iter_mut().enumerate() {
                root.table_id = TableID::new(idx as u64);
            }
            publish_checkpoint_for_test(
                &mtb,
                background_writes,
                TrxID::new(8),
                USER_OBJ_ID_START + 5,
                roots,
            )
            .await;
            let updated_snapshot = mtb.load_snapshot().unwrap();
            assert_eq!(updated_snapshot.catalog_replay_start_ts, TrxID::new(8));
            assert_eq!(updated_snapshot.meta.first_redo_log_seq, 3);
            drop(mtb);
            drop(fs);

            let fs = build_test_fs_in(dir.path());
            let mtb = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let reloaded = mtb.load_snapshot().unwrap();
            assert_eq!(reloaded.catalog_replay_start_ts, TrxID::new(8));
            assert_eq!(reloaded.meta.first_redo_log_seq, 3);
        });
    }

    #[test]
    fn test_multi_table_file_meta_block_copy_on_write() {
        smol::block_on(async {
            let (_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let mtb = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots.iter_mut().enumerate() {
                root.table_id = TableID::new(idx as u64);
            }

            let meta_block_id_0 = mtb.active_root_unchecked().meta_block_id;
            publish_checkpoint_for_test(
                &mtb,
                background_writes,
                TrxID::new(3),
                USER_OBJ_ID_START + 1,
                roots,
            )
            .await;
            let meta_block_id_1 = mtb.active_root_unchecked().meta_block_id;
            publish_checkpoint_for_test(
                &mtb,
                background_writes,
                TrxID::new(4),
                USER_OBJ_ID_START + 2,
                roots,
            )
            .await;
            let meta_block_id_2 = mtb.active_root_unchecked().meta_block_id;

            assert_ne!(meta_block_id_0, meta_block_id_1);
            assert_ne!(meta_block_id_1, meta_block_id_2);
        });
    }

    #[test]
    fn test_multi_table_file_rejects_super_block_version_mismatch() {
        smol::block_on(async {
            let (dir, fs) = build_test_fs();
            let path = fs.catalog_mtb_file_path();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let mtb = fs
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
            let mtb = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let active_meta_block_id = mtb.active_root_unchecked().meta_block_id;
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
                DataIntegrityError::InvalidVersion,
            );
        });
    }

    #[test]
    fn test_multi_table_file_rejects_meta_checksum_corruption() {
        smol::block_on(async {
            let (dir, fs) = build_test_fs();
            let path = fs.catalog_mtb_file_path();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let mtb = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();
            let active_meta_block_id = mtb.active_root_unchecked().meta_block_id;
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
                DataIntegrityError::ChecksumMismatch,
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
            let mtb = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            let mut roots_v1 = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots_v1.iter_mut().enumerate() {
                root.table_id = TableID::new(idx as u64);
                root.root_block_id = NonZeroU64::new((idx + 10) as u64);
                root.pivot_row_id = RowID::new((idx as u64) + 1);
            }
            publish_checkpoint_for_test(
                &mtb,
                background_writes,
                TrxID::new(3),
                USER_OBJ_ID_START + 1,
                roots_v1,
            )
            .await;
            let older_snapshot = mtb.load_snapshot().unwrap();

            let mut roots_v2 = roots_v1;
            roots_v2[0].root_block_id = NonZeroU64::new(99);
            roots_v2[0].pivot_row_id = RowID::new(100);
            publish_checkpoint_for_test(
                &mtb,
                background_writes,
                TrxID::new(4),
                USER_OBJ_ID_START + 2,
                roots_v2,
            )
            .await;
            let active_super_slot = mtb.active_root_unchecked().slot_no;
            drop(mtb);
            drop(fs);

            let version_offset = active_super_slot * SUPER_BLOCK_SIZE as u64
                + MULTI_TABLE_FILE_MAGIC_WORD.len() as u64;
            overwrite_file_bytes(&path, version_offset, &2u64.to_le_bytes());

            let fs = build_test_fs_in(dir.path());
            let mtb = fs
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
            let mtb = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            let mut roots_v1 = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
            for (idx, root) in roots_v1.iter_mut().enumerate() {
                root.table_id = TableID::new(idx as u64);
                root.root_block_id = NonZeroU64::new((idx + 20) as u64);
                root.pivot_row_id = RowID::new((idx as u64) + 10);
            }
            publish_checkpoint_for_test(
                &mtb,
                background_writes,
                TrxID::new(5),
                USER_OBJ_ID_START + 10,
                roots_v1,
            )
            .await;

            let mut roots_v2 = roots_v1;
            roots_v2[1].root_block_id = NonZeroU64::new(123);
            roots_v2[1].pivot_row_id = RowID::new(222);
            publish_checkpoint_for_test(
                &mtb,
                background_writes,
                TrxID::new(6),
                USER_OBJ_ID_START + 11,
                roots_v2,
            )
            .await;
            let active_meta_block_id = mtb.active_root_unchecked().meta_block_id;
            drop(mtb);
            drop(fs);

            let file_bytes = read(&path).unwrap();
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
                TrxID::new(0),
                active_meta_block_id,
                parsed_meta.alloc_map,
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
                DataIntegrityError::InvalidRootInvariant,
            );
        });
    }

    #[test]
    #[should_panic(expected = "concurrent mutable CoW file modification is not allowed")]
    fn test_multi_table_file_rejects_concurrent_fork() {
        smol::block_on(async {
            let (_dir, fs) = build_test_fs();
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let mtb = fs
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
            let mtb = fs
                .open_or_create_multi_table_file(global.guard())
                .await
                .unwrap();

            let first = MutableMultiTableFile::fork(&mtb, fs.background_writes());
            drop(first);

            let _second = MutableMultiTableFile::fork(&mtb, fs.background_writes());
        });
    }
}
