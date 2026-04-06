use crate::bitmap::AllocMap;
use crate::buffer::{ReadonlyBackingFile, ReadonlyBufferPool};
use crate::catalog::{TableID, table::TableMetadata};
use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind, Result};
use crate::file::FileID;
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
    MetaBlock, MetaBlockSerView, TABLE_META_BLOCK_MAGIC_WORD, TABLE_META_BLOCK_VERSION,
};
use crate::file::super_block::{
    SUPER_BLOCK_FOOTER_OFFSET, SUPER_BLOCK_SIZE, SUPER_BLOCK_VERSION, SuperBlock, SuperBlockBody,
    SuperBlockFooter, SuperBlockHeader, SuperBlockSerView, parse_super_block,
};
use crate::index::{ColumnBlockEntryShape, ColumnBlockIndex};
use crate::io::{DirectBuf, IOBuf, IOClient};
use crate::row::RowID;
use crate::serde::{Deser, Ser};
use crate::trx::TrxID;
use futures::future::try_join_all;
use std::mem;
use std::os::fd::RawFd;
use std::sync::Arc;

pub const TABLE_FILE_MAGIC_WORD: [u8; 8] = [b'D', b'O', b'R', b'A', 0, 0, 0, 0];
const TABLE_META_BLOCK_SPEC: BlockIntegritySpec =
    BlockIntegritySpec::new(TABLE_META_BLOCK_MAGIC_WORD, TABLE_META_BLOCK_VERSION);

/// Initial size of new table file.
pub const TABLE_FILE_INITIAL_SIZE: usize = 16 * 1024 * 1024;
/// Super block size of the table file is 32KB.
pub const TABLE_FILE_SUPER_BLOCK_SIZE: usize = SUPER_BLOCK_SIZE;
/// Super block header size.
pub const TABLE_FILE_SUPER_BLOCK_HEADER_SIZE: usize = mem::size_of::<[u8; 8]>()
    + mem::size_of::<u64>()
    + mem::size_of::<BlockID>()
    + mem::size_of::<TrxID>();

/// Table-specific metadata payload embedded in shared active root.
///
/// These fields are persisted in table meta blocks and kept in memory through
/// `ActiveRoot` for fast access on hot paths.
#[derive(Clone)]
pub struct TableMeta {
    /// Metadata of this table.
    pub metadata: Arc<TableMetadata>,
    /// Root block id of column block index.
    pub column_block_index_root: BlockID,
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
    /// Slot number is set to zero (the first super-block slot of this file).
    #[inline]
    pub fn new(trx_id: TrxID, max_pages: usize, metadata: Arc<TableMetadata>) -> Self {
        let alloc_map = AllocMap::new(max_pages);
        let super_block_allocated = alloc_map.allocate_at(usize::from(SUPER_BLOCK_ID));
        assert!(super_block_allocated);

        ActiveRoot::from_parts(
            0,
            trx_id,
            SUPER_BLOCK_ID,
            alloc_map,
            vec![],
            TableMeta {
                metadata,
                column_block_index_root: SUPER_BLOCK_ID,
                pivot_row_id: 0,
                heap_redo_start_ts: trx_id,
            },
        )
    }

    /// Build super-block serialization view for the current active root.
    #[inline]
    pub fn super_block_ser_view(&self) -> SuperBlockSerView {
        SuperBlockSerView {
            header: SuperBlockHeader {
                magic_word: TABLE_FILE_MAGIC_WORD,
                version: SUPER_BLOCK_VERSION,
                slot_no: self.slot_no,
                checkpoint_cts: self.trx_id,
            },
            body: SuperBlockBody {
                meta_block_id: self.meta_block_id,
            },
        }
    }

    /// Build meta-block serialization view for the current active root.
    #[inline]
    pub fn meta_block_ser_view(&self) -> MetaBlockSerView<'_> {
        MetaBlockSerView::new(
            self.metadata.ser_view(),
            self.column_block_index_root,
            &self.alloc_map,
            &self.gc_block_list,
            self.pivot_row_id,
            self.heap_redo_start_ts,
        )
    }
}

#[inline]
fn parse_table_super_block(buf: &[u8]) -> Result<SuperBlock> {
    parse_super_block(buf, TABLE_FILE_MAGIC_WORD, SUPER_BLOCK_VERSION)
}

#[inline]
fn parse_table_meta_block(page_id: BlockID, buf: &[u8]) -> Result<ParsedMeta<TableMeta>> {
    let payload = validate_block(buf, TABLE_META_BLOCK_SPEC).map_err(|cause| {
        Error::block_corrupted(FileKind::TableFile, BlockKind::TableMeta, page_id, cause)
    })?;
    let (_, meta_block) = MetaBlock::deser(payload, 0).map_err(|err| match err {
        Error::InvalidFormat => Error::block_corrupted(
            FileKind::TableFile,
            BlockKind::TableMeta,
            page_id,
            BlockCorruptionCause::InvalidPayload,
        ),
        other => other,
    })?;
    Ok(ParsedMeta {
        meta: TableMeta {
            metadata: Arc::new(meta_block.schema),
            column_block_index_root: meta_block.column_block_index_root,
            pivot_row_id: meta_block.pivot_row_id,
            heap_redo_start_ts: meta_block.heap_redo_start_ts,
        },
        alloc_map: meta_block.alloc_map,
        gc_block_list: meta_block.gc_block_list,
    })
}

#[inline]
fn validate_table_root(meta_block_id: BlockID, parsed_meta: &ParsedMeta<TableMeta>) -> Result<()> {
    validate_active_meta_block_id(
        &parsed_meta.alloc_map,
        meta_block_id,
        FileKind::TableFile,
        BlockKind::TableMeta,
    )
}

#[inline]
fn build_table_meta_block(root: &ActiveRoot) -> Result<DirectBuf> {
    let meta_block = root.meta_block_ser_view();
    let mut meta_buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
    let meta_len = meta_block.ser_len();
    if meta_len > max_payload_len(COW_FILE_PAGE_SIZE) {
        return Err(Error::InvalidState);
    }
    let meta_idx = write_block_header(meta_buf.as_bytes_mut(), TABLE_META_BLOCK_SPEC);
    let meta_idx = meta_block.ser(meta_buf.as_bytes_mut(), meta_idx);
    debug_assert_eq!(meta_idx, BLOCK_INTEGRITY_HEADER_SIZE + meta_len);
    write_block_checksum(meta_buf.as_bytes_mut());
    Ok(meta_buf)
}

#[inline]
fn build_table_super_block(root: &ActiveRoot) -> Result<DirectBuf> {
    let super_block = root.super_block_ser_view();
    let mut buf = DirectBuf::zeroed(SUPER_BLOCK_SIZE);
    let ser_len = super_block.ser_len();
    if ser_len > SUPER_BLOCK_FOOTER_OFFSET {
        // single super block cannot hold all data
        unimplemented!("multiple pages are required to hold super data");
    }
    let ser_idx = super_block.ser(buf.as_bytes_mut(), 0);
    debug_assert_eq!(ser_idx, ser_len);

    let b3sum = blake3::hash(&buf.as_bytes()[..SUPER_BLOCK_FOOTER_OFFSET]);
    let footer = SuperBlockFooter {
        b3sum: *b3sum.as_bytes(),
        checkpoint_cts: root.trx_id,
    };
    let ser_idx = footer.ser(buf.as_bytes_mut(), SUPER_BLOCK_FOOTER_OFFSET);
    debug_assert_eq!(ser_idx, SUPER_BLOCK_SIZE);
    Ok(buf)
}

#[inline]
fn table_codec() -> CowCodec<TableMeta> {
    CowCodec {
        parse_super_block: parse_table_super_block,
        parse_meta_block: parse_table_meta_block,
        validate_root: validate_table_root,
        build_meta_block: build_table_meta_block,
        build_super_block: build_table_super_block,
    }
}

/// Table-file facade over generic copy-on-write file mechanics.
pub struct TableFile {
    file: CowFile<TableMeta>,
}

impl TableFile {
    /// Create a table file.
    #[inline]
    pub(super) fn create(
        file_path: impl AsRef<str>,
        initial_size: usize,
        table_id: TableID,
        trunc: bool,
    ) -> Result<Self> {
        debug_assert!(initial_size.is_multiple_of(COW_FILE_PAGE_SIZE));
        let file = CowFile::create(
            file_path,
            initial_size,
            FileID::from(table_id),
            table_codec(),
            trunc,
        )?;
        Ok(TableFile { file })
    }

    #[inline]
    pub(super) fn open(file_path: impl AsRef<str>, table_id: TableID) -> Result<Self> {
        let file = CowFile::open(file_path, FileID::from(table_id), table_codec())?;
        Ok(TableFile { file })
    }

    /// Load the active root after validating the selected table meta block through readonly cache.
    #[inline]
    pub async fn load_active_root_from_pool(
        &self,
        disk_pool: &ReadonlyBufferPool,
    ) -> Result<ActiveRoot> {
        self.file.load_active_root_from_pool(disk_pool).await
    }

    /// Returns the in-memory active root snapshot.
    #[inline]
    pub fn active_root(&self) -> &ActiveRoot {
        self.file.active_root()
    }

    /// Returns the physical file identifier used by readonly caching and IO routing.
    #[inline]
    pub(crate) fn file_id(&self) -> FileID {
        self.file.file_id()
    }

    /// Returns the owned raw file descriptor for direct IO operations.
    #[inline]
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.file.raw_fd()
    }

    #[inline]
    pub(super) fn install_loaded_root(&self, active_root: ActiveRoot) -> Option<OldRoot> {
        self.file.swap_active_root(active_root)
    }
}

impl TableFile {
    #[inline]
    fn file(&self) -> &CowFile<TableMeta> {
        &self.file
    }
}

/// MutableTableFile represents a table file being modified.
/// It's safe to share the original table file with other threads
/// because the modification is done in Copy-on-Write way.
/// All changes will be committed once the new active root
/// is persisted to disk.
pub struct MutableTableFile {
    file: Option<Arc<TableFile>>,
    new_root: Option<ActiveRoot>,
    background_writes: IOClient<BackgroundWriteRequest>,
    mutable_writer_claimed: bool,
}

/// One LWC block payload that should be persisted into table-file blocks.
pub struct LwcBlockPersist {
    /// Phase-1 logical index-entry shape for this block before block-id assignment.
    pub shape: ColumnBlockEntryShape,
    /// Serialized LWC block bytes.
    pub buf: DirectBuf,
}

impl MutableTableFile {
    #[inline]
    fn file_ref(&self) -> &Arc<TableFile> {
        self.file
            .as_ref()
            .expect("mutable table file has been consumed")
    }

    #[inline]
    fn new_root_mut(&mut self) -> &mut ActiveRoot {
        self.new_root
            .as_mut()
            .expect("mutable table file has been consumed")
    }

    #[inline]
    fn background_writes(&self) -> &IOClient<BackgroundWriteRequest> {
        &self.background_writes
    }

    #[inline]
    fn release_mutable_claim_with_file(&mut self, file: &Arc<TableFile>) {
        if self.mutable_writer_claimed {
            file.file().release_mutable_writer();
            self.mutable_writer_claimed = false;
        }
    }

    #[inline]
    fn release_mutable_claim(&mut self) {
        if self.mutable_writer_claimed {
            let file = self
                .file
                .as_ref()
                .expect("mutable table file has been consumed");
            file.file().release_mutable_writer();
            self.mutable_writer_claimed = false;
        }
    }

    /// Create new mutable table file.
    #[inline]
    pub(crate) fn new(
        table_file: Arc<TableFile>,
        new_root: ActiveRoot,
        background_writes: &IOClient<BackgroundWriteRequest>,
    ) -> Self {
        table_file.file().claim_mutable_writer();
        MutableTableFile {
            file: Some(table_file),
            new_root: Some(new_root),
            background_writes: background_writes.clone(),
            mutable_writer_claimed: true,
        }
    }

    /// Fork the whole table file with a new root.
    #[inline]
    pub(crate) fn fork(
        table_file: &Arc<TableFile>,
        background_writes: &IOClient<BackgroundWriteRequest>,
    ) -> Self {
        table_file.file().claim_mutable_writer();
        MutableTableFile {
            file: Some(Arc::clone(table_file)),
            new_root: Some(table_file.active_root().flip()),
            background_writes: background_writes.clone(),
            mutable_writer_claimed: true,
        }
    }

    /// Returns immutable reference to mutable root snapshot.
    #[inline]
    pub fn root(&self) -> &ActiveRoot {
        self.new_root
            .as_ref()
            .expect("mutable table file has been consumed")
    }

    /// Updates mutable root column-index pointer.
    #[inline]
    pub fn set_column_block_index_root(&mut self, root_block_id: BlockID) {
        self.new_root_mut().column_block_index_root = root_block_id;
    }

    /// Updates mutable root checkpoint metadata.
    #[inline]
    pub fn apply_checkpoint_metadata(
        &mut self,
        pivot_row_id: RowID,
        heap_redo_start_ts: TrxID,
    ) -> Result<()> {
        let root = self.new_root_mut();
        if pivot_row_id < root.pivot_row_id {
            return Err(Error::InvalidArgument);
        }
        root.pivot_row_id = pivot_row_id;
        root.heap_redo_start_ts = heap_redo_start_ts;
        Ok(())
    }

    /// Allocate a new block id for copy-on-write updates.
    #[inline]
    pub fn allocate_block_id(&mut self) -> Result<BlockID> {
        self.new_root_mut()
            .try_allocate_block_id()
            .ok_or(Error::InvalidState)
    }

    /// Record an obsolete block id to be reclaimed on commit.
    #[inline]
    pub fn record_gc_block(&mut self, block_id: BlockID) {
        self.new_root_mut().gc_block_list.push(block_id);
    }

    /// Write one page into the underlying table file.
    #[inline]
    pub async fn write_block(&self, block_id: BlockID, buf: DirectBuf) -> Result<()> {
        let owner = ReadonlyBackingFile::from(Arc::clone(self.file_ref()));
        self.file_ref()
            .file()
            .write_block_with_owner(self.background_writes(), owner, block_id, buf)
            .await
    }

    #[inline]
    async fn publish_root(&self, new_root: ActiveRoot) -> Result<Option<OldRoot>> {
        let owner = ReadonlyBackingFile::from(Arc::clone(self.file_ref()));
        self.file_ref()
            .file()
            .publish_root_with_owner(self.background_writes(), owner, new_root)
            .await
    }

    /// Commit the modification of table file.
    /// Returns the new table file and previous
    /// active root if exists.
    #[inline]
    pub async fn commit(
        mut self,
        trx_id: TrxID,
        try_delete_if_fail: bool,
    ) -> Result<(Arc<TableFile>, Option<OldRoot>)> {
        let mut new_root = self
            .new_root
            .take()
            .expect("mutable table file has been consumed");
        debug_assert!(new_root.trx_id == 0 || new_root.trx_id < trx_id);
        new_root.trx_id = trx_id;
        let publish_res = self.publish_root(new_root).await;
        let table_file = self
            .file
            .take()
            .expect("mutable table file has been consumed");
        self.release_mutable_claim_with_file(&table_file);

        match publish_res {
            Ok(old_root) => Ok((table_file, old_root)),
            Err(err) => {
                if try_delete_if_fail && let Some(file) = Arc::into_inner(table_file) {
                    file.file.delete();
                }
                Err(err)
            }
        }
    }

    /// Persist provided LWC blocks and update mutable root/index metadata.
    pub(crate) async fn apply_lwc_blocks(
        &mut self,
        lwc_blocks: Vec<LwcBlockPersist>,
        heap_redo_start_ts: TrxID,
        ts: TrxID,
        disk_pool: &ReadonlyBufferPool,
    ) -> Result<()> {
        let table_file = Arc::clone(self.file_ref());
        let background_writes = self.background_writes.clone();
        let disk_pool_guard = disk_pool.pool_guard();
        let mut max_row_id = self.root().pivot_row_id;
        let mut writes = Vec::with_capacity(lwc_blocks.len());
        let mut new_entries = Vec::with_capacity(lwc_blocks.len());
        let mut last_end = self.root().pivot_row_id;

        for block in lwc_blocks {
            let start_row_id = block.shape.start_row_id();
            let end_row_id = block.shape.end_row_id();
            let block_id = self
                .new_root_mut()
                .try_allocate_block_id()
                .ok_or(Error::InvalidState)?;
            max_row_id = max_row_id.max(end_row_id);
            if start_row_id < last_end {
                return Err(Error::InvalidArgument);
            }
            last_end = end_row_id;
            new_entries.push(block.shape.with_block_id(block_id));
            let background_writes = background_writes.clone();
            let file = Arc::clone(&table_file);
            writes.push(async move {
                file.file()
                    .write_block_with_owner(
                        &background_writes,
                        ReadonlyBackingFile::from(Arc::clone(&file)),
                        block_id,
                        block.buf,
                    )
                    .await
            });
        }

        try_join_all(writes).await?;

        let root = self.root();
        let column_index = ColumnBlockIndex::new(
            root.column_block_index_root,
            root.pivot_row_id,
            disk_pool,
            &disk_pool_guard,
        );
        let new_root = column_index
            .batch_insert(self, &new_entries, max_row_id, ts)
            .await?;
        let root = self.new_root_mut();
        root.column_block_index_root = new_root;
        root.pivot_row_id = max_row_id;
        root.heap_redo_start_ts = heap_redo_start_ts;
        Ok(())
    }

    /// Persist LWC blocks and commit the resulting root as one CoW publish.
    #[cfg(test)]
    pub(crate) async fn persist_lwc_blocks(
        mut self,
        lwc_blocks: Vec<LwcBlockPersist>,
        heap_redo_start_ts: TrxID,
        ts: TrxID,
        disk_pool: &ReadonlyBufferPool,
    ) -> Result<(Arc<TableFile>, Option<OldRoot>)> {
        self.apply_lwc_blocks(lwc_blocks, heap_redo_start_ts, ts, disk_pool)
            .await?;
        self.commit(ts, false).await
    }

    #[inline]
    pub fn try_delete(mut self) -> bool {
        let table_file = self
            .file
            .take()
            .expect("mutable table file has been consumed");
        self.release_mutable_claim_with_file(&table_file);
        if let Some(table_file) = Arc::into_inner(table_file) {
            table_file.file.delete();
            return true;
        }
        false
    }
}

impl Drop for MutableTableFile {
    #[inline]
    fn drop(&mut self) {
        self.release_mutable_claim();
    }
}

impl MutableCowFile for MutableTableFile {
    #[inline]
    fn allocate_block_id(&mut self) -> Result<BlockID> {
        MutableTableFile::allocate_block_id(self)
    }

    #[inline]
    fn record_gc_block(&mut self, block_id: BlockID) {
        MutableTableFile::record_gc_block(self, block_id)
    }

    #[inline]
    fn write_block(
        &self,
        block_id: BlockID,
        buf: DirectBuf,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        MutableTableFile::write_block(self, block_id, buf)
    }
}

/// Guard object of swapped table-file roots.
///
/// Dropping this guard reclaims the replaced active-root pointer.
pub type OldRoot = OldCowRoot<TableMeta>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{
        GlobalReadonlyBufferPool, PoolRole, ReadonlyBufferPool, global_readonly_pool_scope,
        table_readonly_pool,
    };
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind};
    use crate::file::block_integrity::BLOCK_INTEGRITY_TRAILER_SIZE;
    use crate::file::{build_test_fs, build_test_fs_in, test_block_id};
    use crate::io::IOBuf;
    use crate::quiescent::QuiescentBox;
    use crate::value::ValKind;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    fn accept_any_page(_page: &[u8], _file_kind: FileKind, _page_id: BlockID) -> Result<()> {
        Ok(())
    }

    async fn read_page_for_test(
        table_file: &Arc<TableFile>,
        page_id: BlockID,
    ) -> Result<DirectBuf> {
        let global = global_readonly_pool_scope(64 * 1024 * 1024);
        let disk_pool = table_readonly_pool(&global, 0, table_file);
        let disk_pool_guard = disk_pool.pool_guard();
        let page = disk_pool
            .read_validated_block(&disk_pool_guard, page_id, accept_any_page)
            .await?;
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        buf.as_bytes_mut().copy_from_slice(page.page());
        Ok(buf)
    }

    #[test]
    fn test_table_file() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
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
            assert_eq!(table_file.active_root().slot_no, 0);
            assert_eq!(table_file.active_root().trx_id, 1);

            // write
            let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
            buf.reset();
            buf.extend_from_slice(b"hello, world");
            let mutable = MutableTableFile::fork(&table_file, background_writes);
            let res = mutable.write_block(test_block_id(3), buf).await;
            assert!(res.is_ok());
            drop(mutable);

            let res = read_page_for_test(&table_file, test_block_id(3)).await;
            assert!(res.is_ok());
            let buf = res.unwrap();
            assert_eq!(&buf.as_bytes()[..12], b"hello, world");

            drop(table_file);

            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let (table_file2, disk_pool) = fs.open_table_file(41, global.guard()).await.unwrap();
            assert_eq!(table_file2.active_root().trx_id, 1);

            let mutable = MutableTableFile::fork(&table_file2, background_writes);
            let (table_file3, old_root) = mutable.commit(2, false).await.unwrap();
            drop(old_root);
            let active_root = table_file3
                .load_active_root_from_pool(&disk_pool)
                .await
                .unwrap();
            assert_eq!(active_root.slot_no, 1);
            assert_eq!(active_root.trx_id, 2);
            drop(table_file2);
            drop(table_file3);

            drop(fs);
        });
    }

    #[test]
    fn test_readonly_buffer_read_propagates_io_error() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table_file = fs
                .create_table_file(145, build_test_metadata(), false)
                .unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 145, &table_file);
            let disk_pool_guard = disk_pool.pool_guard();
            let out_of_range_page_id = test_block_id(1_000_000);
            let res = disk_pool
                .read_validated_block(&disk_pool_guard, out_of_range_page_id, accept_any_page)
                .await;
            assert!(res.is_err());

            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_table_file_system() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
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
            assert_eq!(table_file.active_root().slot_no, 0);
            assert_eq!(table_file.active_root().trx_id, 1);

            let global = QuiescentBox::new(
                GlobalReadonlyBufferPool::with_capacity(
                    PoolRole::Disk,
                    64 * 1024 * 1024,
                    fs.guard(),
                )
                .unwrap(),
            );
            let disk_pool = ReadonlyBufferPool::from_table_file(
                FileKind::TableFile,
                Arc::clone(&table_file),
                global.guard(),
            );
            let disk_pool_guard = disk_pool.pool_guard();

            // We first shut down the file system, then send the IO request,
            // it should fail.
            fs.shutdown();

            let res = disk_pool
                .read_validated_block(&disk_pool_guard, test_block_id(1), accept_any_page)
                .await;
            assert!(res.is_err());

            drop(disk_pool_guard);
            drop(disk_pool);
            drop(global);
            drop(fs);
            drop(table_file);
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

    fn assert_table_meta_corruption(err: Error, page_id: BlockID, cause: BlockCorruptionCause) {
        assert!(matches!(
            err,
            Error::BlockCorrupted {
                file_kind: FileKind::TableFile,
                block_kind: BlockKind::TableMeta,
                block_id: actual_page_id,
                cause: actual_cause,
            } if actual_page_id == page_id && actual_cause == cause
        ));
    }

    #[test]
    fn test_table_file_rejects_meta_checksum_corruption() {
        smol::block_on(async {
            let (temp_dir, fs) = build_test_fs();
            let table_id = 146;
            let path = fs.table_file_path(table_id);
            let table_file = fs
                .create_table_file(table_id, build_test_metadata(), false)
                .unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            let active_meta_block_id = table_file.active_root().meta_block_id;
            drop(table_file);
            drop(fs);

            let checksum_offset = u64::from(active_meta_block_id) * COW_FILE_PAGE_SIZE as u64
                + (COW_FILE_PAGE_SIZE - BLOCK_INTEGRITY_TRAILER_SIZE) as u64;
            overwrite_file_bytes(&path, checksum_offset, &[0xff]);

            let fs = build_test_fs_in(temp_dir.path());
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let err = match fs.open_table_file(table_id, global.guard()).await {
                Ok(_) => panic!("expected table meta checksum corruption"),
                Err(err) => err,
            };
            assert_table_meta_corruption(
                err,
                active_meta_block_id,
                BlockCorruptionCause::ChecksumMismatch,
            );
        });
    }

    #[test]
    fn test_table_file_rejects_meta_version_mismatch() {
        smol::block_on(async {
            let (temp_dir, fs) = build_test_fs();
            let table_id = 147;
            let path = fs.table_file_path(table_id);
            let table_file = fs
                .create_table_file(table_id, build_test_metadata(), false)
                .unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);
            let active_meta_block_id = table_file.active_root().meta_block_id;
            drop(table_file);
            drop(fs);

            let version_offset = u64::from(active_meta_block_id) * COW_FILE_PAGE_SIZE as u64
                + TABLE_META_BLOCK_MAGIC_WORD.len() as u64;
            overwrite_file_bytes(
                &path,
                version_offset,
                &(TABLE_META_BLOCK_VERSION + 1).to_le_bytes(),
            );

            let fs = build_test_fs_in(temp_dir.path());
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let err = match fs.open_table_file(table_id, global.guard()).await {
                Ok(_) => panic!("expected table meta version corruption"),
                Err(err) => err,
            };
            assert_table_meta_corruption(
                err,
                active_meta_block_id,
                BlockCorruptionCause::InvalidVersion,
            );
        });
    }

    #[test]
    fn test_persist_lwc_blocks_appends_entries() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = build_test_metadata();
            let table_file = fs.create_table_file(43, metadata, false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let lwc_blocks = vec![
                LwcBlockPersist {
                    shape: ColumnBlockEntryShape::new(0, 10, (0..10).collect(), Vec::new())
                        .unwrap(),
                    buf: page_buf(b"lwc-page-1"),
                },
                LwcBlockPersist {
                    shape: ColumnBlockEntryShape::new(10, 20, (10..20).collect(), Vec::new())
                        .unwrap(),
                    buf: page_buf(b"lwc-page-2"),
                },
            ];

            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 43, &table_file);
            let (table_file, old_root) = MutableTableFile::fork(&table_file, background_writes)
                .persist_lwc_blocks(lwc_blocks, 7, 2, &disk_pool)
                .await
                .unwrap();
            drop(old_root);

            let active_root = table_file.active_root();
            assert_eq!(active_root.trx_id, 2);
            assert_eq!(active_root.pivot_row_id, 20);
            assert_eq!(active_root.heap_redo_start_ts, 7);
            assert_ne!(active_root.column_block_index_root, SUPER_BLOCK_ID);
            let disk_pool = table_readonly_pool(&global, 43, &table_file);
            let disk_pool_guard = disk_pool.pool_guard();

            let column_index = ColumnBlockIndex::new(
                active_root.column_block_index_root,
                active_root.pivot_row_id,
                &disk_pool,
                &disk_pool_guard,
            );
            let entry1 = column_index.locate_block(0).await.unwrap().unwrap();
            let entry2 = column_index.locate_block(15).await.unwrap().unwrap();
            let page1 = read_page_for_test(&table_file, entry1.block_id())
                .await
                .unwrap();
            let page2 = read_page_for_test(&table_file, entry2.block_id())
                .await
                .unwrap();
            assert_eq!(&page1.as_bytes()[..10], b"lwc-page-1");
            assert_eq!(&page2.as_bytes()[..10], b"lwc-page-2");

            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    fn test_persist_lwc_blocks_rejects_overlapping_ranges() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = build_test_metadata();
            let table_file = fs.create_table_file(44, metadata, false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let lwc_blocks = vec![
                LwcBlockPersist {
                    shape: ColumnBlockEntryShape::new(0, 10, (0..10).collect(), Vec::new())
                        .unwrap(),
                    buf: page_buf(b"lwc-overlap-1"),
                },
                LwcBlockPersist {
                    shape: ColumnBlockEntryShape::new(5, 15, (5..15).collect(), Vec::new())
                        .unwrap(),
                    buf: page_buf(b"lwc-overlap-2"),
                },
            ];

            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 44, &table_file);
            let result = MutableTableFile::fork(&table_file, background_writes)
                .persist_lwc_blocks(lwc_blocks, 7, 2, &disk_pool)
                .await;

            assert!(matches!(result, Err(Error::InvalidArgument)));
            let active_root = table_file.active_root();
            assert_eq!(active_root.pivot_row_id, 0);
            assert_eq!(active_root.column_block_index_root, SUPER_BLOCK_ID);

            drop(table_file);
            drop(fs);
        });
    }

    #[test]
    #[should_panic(expected = "concurrent mutable CoW file modification is not allowed")]
    fn test_mutable_table_file_rejects_concurrent_fork() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = build_test_metadata();
            let table_file = fs.create_table_file(45, metadata, false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let _first = MutableTableFile::fork(&table_file, background_writes);
            let _second = MutableTableFile::fork(&table_file, background_writes);
        });
    }

    #[test]
    fn test_mutable_table_file_allows_fork_after_drop() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = build_test_metadata();
            let table_file = fs.create_table_file(46, metadata, false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let first = MutableTableFile::fork(&table_file, background_writes);
            drop(first);

            let _second = MutableTableFile::fork(&table_file, background_writes);
        });
    }
}
