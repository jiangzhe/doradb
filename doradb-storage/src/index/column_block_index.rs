use crate::buffer::ReadonlyBufferPool;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::page::{Page, PageID};
use crate::error::{
    Error, PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind, Result,
};
use crate::file::cow_file::{COW_FILE_PAGE_SIZE, MutableCowFile};
use crate::file::page_integrity::{
    COLUMN_BLOCK_INDEX_PAGE_SPEC, PAGE_INTEGRITY_HEADER_SIZE, max_payload_len, validate_page,
    write_page_checksum, write_page_header,
};
use crate::index::column_deletion_blob::{ColumnDeletionBlobReader, ColumnDeletionBlobWriter};
use crate::index::column_payload::{BlobRef, ColumnPagePayload};
use crate::io::DirectBuf;
use crate::row::RowID;
use bytemuck::{Pod, Zeroable, cast_slice, cast_slice_mut};
use std::future::Future;
use std::mem;
use std::pin::Pin;

pub const COLUMN_BLOCK_PAGE_SIZE: usize = COW_FILE_PAGE_SIZE;
pub const COLUMN_BLOCK_NODE_PAYLOAD_SIZE: usize = max_payload_len(COLUMN_BLOCK_PAGE_SIZE);
pub const COLUMN_BLOCK_HEADER_SIZE: usize = mem::size_of::<ColumnBlockNodeHeader>();
pub const COLUMN_BLOCK_DATA_SIZE: usize = COLUMN_BLOCK_NODE_PAYLOAD_SIZE - COLUMN_BLOCK_HEADER_SIZE;
pub const COLUMN_PAGE_PAYLOAD_SIZE: usize = mem::size_of::<ColumnPagePayload>();
pub const COLUMN_BRANCH_ENTRY_SIZE: usize = mem::size_of::<ColumnBlockBranchEntry>();
pub const COLUMN_BLOCK_MAX_ENTRIES: usize =
    COLUMN_BLOCK_DATA_SIZE / (mem::size_of::<RowID>() + COLUMN_PAGE_PAYLOAD_SIZE);
pub const COLUMN_BLOCK_MAX_BRANCH_ENTRIES: usize =
    COLUMN_BLOCK_DATA_SIZE / COLUMN_BRANCH_ENTRY_SIZE;

const _: () = assert!(
    COLUMN_BLOCK_HEADER_SIZE
        + COLUMN_BLOCK_MAX_ENTRIES * (mem::size_of::<RowID>() + COLUMN_PAGE_PAYLOAD_SIZE)
        <= COLUMN_BLOCK_NODE_PAYLOAD_SIZE,
    "ColumnBlockNode should fit into the persisted column-block payload"
);

const _: () = assert!(
    mem::size_of::<ColumnBlockNode>() == COLUMN_BLOCK_NODE_PAYLOAD_SIZE,
    "ColumnBlockNode size must match the persisted column-block payload size"
);
const _: () = assert!(mem::size_of::<ColumnBlockBranchEntry>() == 16);
const _: () = assert!(COLUMN_BLOCK_HEADER_SIZE.is_multiple_of(mem::align_of::<RowID>()));
const _: () =
    assert!(COLUMN_BLOCK_HEADER_SIZE.is_multiple_of(mem::align_of::<ColumnPagePayload>()));
const _: () =
    assert!(COLUMN_BLOCK_HEADER_SIZE.is_multiple_of(mem::align_of::<ColumnBlockBranchEntry>()));
const _: () = assert!(mem::size_of::<RowID>().is_multiple_of(mem::align_of::<ColumnPagePayload>()));

#[repr(C)]
/// Header stored at the beginning of each on-disk column block-index node.
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct ColumnBlockNodeHeader {
    pub height: u32,
    pub count: u32,
    pub start_row_id: RowID,
    pub create_ts: u64,
}

#[repr(C)]
/// Branch entry mapping a start row id to child node page id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Pod, Zeroable)]
pub struct ColumnBlockBranchEntry {
    pub start_row_id: RowID,
    pub page_id: PageID,
}

#[repr(C)]
/// One column block-index node, either leaf (`height == 0`) or branch.
#[derive(Clone)]
pub struct ColumnBlockNode {
    pub header: ColumnBlockNodeHeader,
    data: [u8; COLUMN_BLOCK_DATA_SIZE],
}

impl ColumnBlockNode {
    #[inline]
    fn new_boxed(height: u32, start_row_id: RowID, create_ts: u64) -> Box<Self> {
        // SAFETY: zeroed bytes are valid for `ColumnBlockNode` (all integer/byte fields).
        let mut node = unsafe { Box::<ColumnBlockNode>::new_zeroed().assume_init() };
        node.header = ColumnBlockNodeHeader {
            height,
            count: 0,
            start_row_id,
            create_ts,
        };
        node
    }

    /// Returns mutable leaf start-row-id array.
    #[inline]
    pub fn leaf_start_row_ids_mut(&mut self) -> &mut [RowID] {
        debug_assert!(self.is_leaf());
        self.leaf_row_ids_mut_with_count(self.header.count as usize)
    }

    /// Returns mutable leaf payload array.
    #[inline]
    pub fn leaf_payloads_mut(&mut self) -> &mut [ColumnPagePayload] {
        debug_assert!(self.is_leaf());
        let count = self.header.count as usize;
        self.leaf_payloads_mut_with_count(count)
    }

    /// Returns mutable views of leaf row ids and payloads in one call.
    #[inline]
    pub fn leaf_arrays_mut(&mut self) -> (&mut [RowID], &mut [ColumnPagePayload]) {
        debug_assert!(self.is_leaf());
        let count = self.header.count as usize;
        debug_assert!(count <= COLUMN_BLOCK_MAX_ENTRIES);
        let row_bytes = count * mem::size_of::<RowID>();
        let payload_bytes = count * mem::size_of::<ColumnPagePayload>();
        let (row_region, remain) = self.data.split_at_mut(row_bytes);
        let (payload_region, _) = remain.split_at_mut(payload_bytes);
        (cast_slice_mut(row_region), cast_slice_mut(payload_region))
    }

    /// Returns mutable branch entries.
    #[inline]
    pub fn branch_entries_mut(&mut self) -> &mut [ColumnBlockBranchEntry] {
        debug_assert!(self.is_branch());
        self.branch_entries_mut_with_count(self.header.count as usize)
    }

    /// Appends one branch entry.
    #[inline]
    pub fn branch_add_entry(&mut self, start_row_id: RowID, page_id: PageID) {
        debug_assert!(self.is_branch());
        debug_assert!((self.header.count as usize) < COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
        let idx = self.header.count as usize;
        self.header.count += 1;
        self.branch_entries_mut()[idx] = ColumnBlockBranchEntry {
            start_row_id,
            page_id,
        };
    }

    #[inline]
    fn leaf_row_ids_mut_with_count(&mut self, count: usize) -> &mut [RowID] {
        leaf_row_ids_from_bytes_mut(&mut self.data, count)
    }

    #[inline]
    fn leaf_payloads_mut_with_count(&mut self, count: usize) -> &mut [ColumnPagePayload] {
        leaf_payloads_from_bytes_mut(&mut self.data, count)
    }

    #[inline]
    fn branch_entries_mut_with_count(&mut self, count: usize) -> &mut [ColumnBlockBranchEntry] {
        branch_entries_from_bytes_mut(&mut self.data, count)
    }
}

#[inline]
fn leaf_row_ids_from_bytes(data: &[u8], count: usize) -> &[RowID] {
    debug_assert!(count <= COLUMN_BLOCK_MAX_ENTRIES);
    let row_bytes = count * mem::size_of::<RowID>();
    cast_slice(&data[..row_bytes])
}

#[inline]
fn leaf_row_ids_from_bytes_mut(data: &mut [u8], count: usize) -> &mut [RowID] {
    debug_assert!(count <= COLUMN_BLOCK_MAX_ENTRIES);
    let row_bytes = count * mem::size_of::<RowID>();
    cast_slice_mut(&mut data[..row_bytes])
}

#[inline]
fn leaf_payloads_from_bytes(data: &[u8], count: usize) -> &[ColumnPagePayload] {
    debug_assert!(count <= COLUMN_BLOCK_MAX_ENTRIES);
    let row_bytes = count * mem::size_of::<RowID>();
    let payload_bytes = count * mem::size_of::<ColumnPagePayload>();
    cast_slice(&data[row_bytes..row_bytes + payload_bytes])
}

#[inline]
fn leaf_payloads_from_bytes_mut(data: &mut [u8], count: usize) -> &mut [ColumnPagePayload] {
    debug_assert!(count <= COLUMN_BLOCK_MAX_ENTRIES);
    let row_bytes = count * mem::size_of::<RowID>();
    let payload_bytes = count * mem::size_of::<ColumnPagePayload>();
    cast_slice_mut(&mut data[row_bytes..row_bytes + payload_bytes])
}

#[inline]
fn branch_entries_from_bytes(data: &[u8], count: usize) -> &[ColumnBlockBranchEntry] {
    debug_assert!(count <= COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
    let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
    cast_slice(&data[..bytes_len])
}

#[inline]
fn branch_entries_from_bytes_mut(data: &mut [u8], count: usize) -> &mut [ColumnBlockBranchEntry] {
    debug_assert!(count <= COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
    let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
    cast_slice_mut(&mut data[..bytes_len])
}

trait ColumnBlockNodeRead {
    fn header_ref(&self) -> &ColumnBlockNodeHeader;
    fn data_ref(&self) -> &[u8];

    #[inline]
    fn is_leaf(&self) -> bool {
        self.header_ref().height == 0
    }

    #[inline]
    fn is_branch(&self) -> bool {
        !self.is_leaf()
    }

    #[inline]
    fn leaf_start_row_ids(&self) -> &[RowID] {
        debug_assert!(self.is_leaf());
        leaf_row_ids_from_bytes(self.data_ref(), self.header_ref().count as usize)
    }

    #[inline]
    fn leaf_payloads(&self) -> &[ColumnPagePayload] {
        debug_assert!(self.is_leaf());
        leaf_payloads_from_bytes(self.data_ref(), self.header_ref().count as usize)
    }

    #[inline]
    fn branch_entries(&self) -> &[ColumnBlockBranchEntry] {
        debug_assert!(self.is_branch());
        branch_entries_from_bytes(self.data_ref(), self.header_ref().count as usize)
    }
}

impl ColumnBlockNodeRead for ColumnBlockNode {
    #[inline]
    fn header_ref(&self) -> &ColumnBlockNodeHeader {
        &self.header
    }

    #[inline]
    fn data_ref(&self) -> &[u8] {
        &self.data
    }
}

pub struct ColumnBlockIndex<'a> {
    disk_pool: &'a ReadonlyBufferPool,
    root_page_id: PageID,
    end_row_id: RowID,
}

/// One resolved leaf entry from the persisted column block-index tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnLeafEntry {
    pub leaf_page_id: PageID,
    pub start_row_id: RowID,
    pub payload: ColumnPagePayload,
}

/// One bitmap update patch keyed by leaf `start_row_id`.
#[derive(Clone, Copy, Debug)]
pub struct OffloadedBitmapPatch<'a> {
    pub start_row_id: RowID,
    pub bitmap_bytes: &'a [u8],
}

/// One full payload replacement patch keyed by leaf `start_row_id`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnPagePayloadPatch {
    pub start_row_id: RowID,
    pub payload: ColumnPagePayload,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OffloadedBitmapRefPatch {
    start_row_id: RowID,
    blob_ref: BlobRef,
}

trait LeafPayloadPatch: Copy {
    fn start_row_id(&self) -> RowID;
    fn apply_payload(&self, payload: &mut ColumnPagePayload);
}

impl LeafPayloadPatch for OffloadedBitmapRefPatch {
    #[inline]
    fn start_row_id(&self) -> RowID {
        self.start_row_id
    }

    #[inline]
    fn apply_payload(&self, payload: &mut ColumnPagePayload) {
        payload.clear_inline_and_set_offloaded(self.blob_ref);
    }
}

impl LeafPayloadPatch for ColumnPagePayloadPatch {
    #[inline]
    fn start_row_id(&self) -> RowID {
        self.start_row_id
    }

    #[inline]
    fn apply_payload(&self, payload: &mut ColumnPagePayload) {
        *payload = self.payload;
    }
}

#[derive(Clone, Copy, Debug)]
struct NodeUpdateResult {
    new_page_id: PageID,
    touched: bool,
}

#[inline]
fn invalid_node_payload(file_kind: PersistedFileKind, page_id: PageID) -> Error {
    Error::persisted_page_corrupted(
        file_kind,
        PersistedPageKind::ColumnBlockIndex,
        page_id,
        PersistedPageCorruptionCause::InvalidPayload,
    )
}

#[inline]
fn validate_node_payload(
    page: &[u8],
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<(&[u8], ColumnBlockNodeHeader)> {
    let payload = validate_page(page, COLUMN_BLOCK_INDEX_PAGE_SPEC).map_err(|cause| {
        Error::persisted_page_corrupted(
            file_kind,
            PersistedPageKind::ColumnBlockIndex,
            page_id,
            cause,
        )
    })?;
    let header =
        bytemuck::try_from_bytes::<ColumnBlockNodeHeader>(&payload[..COLUMN_BLOCK_HEADER_SIZE])
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
    let count = header.count as usize;
    if (header.height == 0 && count > COLUMN_BLOCK_MAX_ENTRIES)
        || (header.height > 0 && count > COLUMN_BLOCK_MAX_BRANCH_ENTRIES)
    {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    Ok((payload, *header))
}

#[inline]
pub(crate) fn validate_persisted_column_block_index_page(
    page: &[u8],
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<()> {
    validate_node_payload(page, file_kind, page_id).map(|_| ())
}

#[inline]
fn validated_node_payload(page: &[u8]) -> &[u8] {
    let payload_start = PAGE_INTEGRITY_HEADER_SIZE;
    &page[payload_start..payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE]
}

#[inline]
fn parse_validated_node_header(
    page: &[u8],
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<ColumnBlockNodeHeader> {
    let payload = validated_node_payload(page);
    let header =
        bytemuck::try_from_bytes::<ColumnBlockNodeHeader>(&payload[..COLUMN_BLOCK_HEADER_SIZE])
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
    let count = header.count as usize;
    if (header.height == 0 && count > COLUMN_BLOCK_MAX_ENTRIES)
        || (header.height > 0 && count > COLUMN_BLOCK_MAX_BRANCH_ENTRIES)
    {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    Ok(*header)
}

struct ValidatedColumnBlockNode {
    guard: PageSharedGuard<Page>,
    header: ColumnBlockNodeHeader,
}

impl ValidatedColumnBlockNode {
    #[inline]
    fn try_from_guard(
        guard: PageSharedGuard<Page>,
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<Self> {
        let header = parse_validated_node_header(guard.page(), file_kind, page_id)?;
        Ok(ValidatedColumnBlockNode { guard, header })
    }
}

impl ColumnBlockNodeRead for ValidatedColumnBlockNode {
    #[inline]
    fn header_ref(&self) -> &ColumnBlockNodeHeader {
        &self.header
    }

    #[inline]
    fn data_ref(&self) -> &[u8] {
        let payload = validated_node_payload(self.guard.page());
        &payload[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE]
    }
}

impl<'a> ColumnBlockIndex<'a> {
    /// Creates a column block-index view for one root page snapshot.
    #[inline]
    pub fn new(root_page_id: PageID, end_row_id: RowID, disk_pool: &'a ReadonlyBufferPool) -> Self {
        ColumnBlockIndex {
            disk_pool,
            root_page_id,
            end_row_id,
        }
    }

    /// Returns current column block-index root page id.
    #[inline]
    pub fn root_page_id(&self) -> PageID {
        self.root_page_id
    }

    /// Returns exclusive upper row-id boundary covered by this index snapshot.
    #[inline]
    pub fn end_row_id(&self) -> RowID {
        self.end_row_id
    }

    #[inline]
    pub(crate) fn file_kind(&self) -> PersistedFileKind {
        self.disk_pool.persisted_file_kind()
    }

    #[inline]
    async fn read_node(&self, page_id: PageID) -> Result<ValidatedColumnBlockNode> {
        let g = self
            .disk_pool
            .get_validated_page_shared(page_id, validate_persisted_column_block_index_page)
            .await?;
        ValidatedColumnBlockNode::try_from_guard(g, self.file_kind(), page_id)
    }

    /// Finds the column page payload containing `row_id`.
    ///
    /// Returns `Ok(None)` when `row_id` is not persisted in this column range.
    pub async fn find(&self, row_id: RowID) -> Result<Option<ColumnPagePayload>> {
        Ok(self.find_entry(row_id).await?.map(|entry| entry.payload))
    }

    /// Finds the leaf entry containing `row_id`.
    ///
    /// Returns `Ok(None)` when `row_id` is not persisted in this column range.
    pub async fn find_entry(&self, row_id: RowID) -> Result<Option<ColumnLeafEntry>> {
        if self.root_page_id == 0 || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut page_id = self.root_page_id;
        loop {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                let start_row_ids = node.leaf_start_row_ids();
                let idx = match search_start_row_id(start_row_ids, row_id) {
                    Some(idx) => idx,
                    None => return Ok(None),
                };
                return Ok(Some(ColumnLeafEntry {
                    leaf_page_id: page_id,
                    start_row_id: start_row_ids[idx],
                    payload: node.leaf_payloads()[idx],
                }));
            }
            let entries = node.branch_entries();
            let idx = match search_branch_entry(entries, row_id) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            page_id = entries[idx].page_id;
        }
    }

    /// Reads offloaded bitmap bytes referenced by payload.
    ///
    /// Returns `Ok(None)` when payload is inline-only.
    pub async fn read_offloaded_bitmap_bytes(
        &self,
        entry: &ColumnLeafEntry,
    ) -> Result<Option<Vec<u8>>> {
        let blob_ref = match entry.payload.try_offloaded_ref() {
            Ok(Some(blob_ref)) => blob_ref,
            Ok(None) => return Ok(None),
            Err(Error::InvalidFormat) => {
                return Err(invalid_node_payload(self.file_kind(), entry.leaf_page_id));
            }
            Err(err) => return Err(err),
        };
        let reader = ColumnDeletionBlobReader::new(self.disk_pool);
        let bytes = reader.read(blob_ref).await?;
        Ok(Some(bytes))
    }

    /// Collects all leaf entries in ascending `start_row_id` order.
    pub async fn collect_leaf_entries(&self) -> Result<Vec<ColumnLeafEntry>> {
        if self.root_page_id == 0 {
            return Ok(Vec::new());
        }
        let mut stack = vec![self.root_page_id];
        let mut entries = Vec::new();
        while let Some(page_id) = stack.pop() {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                entries.extend(
                    node.leaf_start_row_ids()
                        .iter()
                        .copied()
                        .zip(node.leaf_payloads().iter().copied())
                        .map(|(start_row_id, payload)| ColumnLeafEntry {
                            leaf_page_id: page_id,
                            start_row_id,
                            payload,
                        }),
                );
                continue;
            }
            for entry in node.branch_entries().iter().rev() {
                if entry.page_id == 0 {
                    return Err(invalid_node_payload(self.file_kind(), page_id));
                }
                stack.push(entry.page_id);
            }
        }
        Ok(entries)
    }

    /// Applies sorted start-row-id bitmap patches with copy-on-write updates.
    ///
    /// Constraints:
    /// - `patches` must be sorted and unique by `start_row_id`.
    /// - every `start_row_id` must already exist in the current index snapshot.
    ///
    /// Behavior:
    /// - this API is sticky offload: updated payloads always store offloaded refs.
    /// - it mutates the provided `MutableCowFile` as part of CoW writes.
    /// - callers should discard the mutable file after `Err` and fork a new one for retries.
    pub async fn batch_update_offloaded_bitmaps<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        patches: &[OffloadedBitmapPatch<'_>],
        create_ts: u64,
    ) -> Result<PageID> {
        if patches.is_empty() {
            return Ok(self.root_page_id);
        }
        if self.root_page_id == 0 {
            return Err(Error::InvalidState);
        }
        if !patches_sorted_unique(patches) {
            return Err(Error::InvalidArgument);
        }

        let mut writer = ColumnDeletionBlobWriter::new(mutable_file);
        let mut resolved = Vec::with_capacity(patches.len());
        for patch in patches {
            let blob_ref = writer.append(patch.bitmap_bytes).await?;
            resolved.push(OffloadedBitmapRefPatch {
                start_row_id: patch.start_row_id,
                blob_ref,
            });
        }
        writer.finish().await?;

        let res = self
            .update_subtree_with_patches(mutable_file, self.root_page_id, &resolved, create_ts)
            .await?;
        if !res.touched {
            return Err(Error::InvalidState);
        }
        Ok(res.new_page_id)
    }

    /// Replaces sorted leaf payloads keyed by `start_row_id` with copy-on-write updates.
    ///
    /// Constraints:
    /// - `patches` must be sorted and unique by `start_row_id`.
    /// - every `start_row_id` must already exist in the current index snapshot.
    ///
    /// Behavior:
    /// - it mutates the provided `MutableCowFile` as part of CoW writes.
    /// - callers should discard the mutable file after `Err` and fork a new one for retries.
    pub async fn batch_replace_payloads<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        patches: &[ColumnPagePayloadPatch],
        create_ts: u64,
    ) -> Result<PageID> {
        if patches.is_empty() {
            return Ok(self.root_page_id);
        }
        if self.root_page_id == 0 {
            return Err(Error::InvalidState);
        }
        if !patches_sorted_unique_by_start_row_id(patches, |patch| patch.start_row_id) {
            return Err(Error::InvalidArgument);
        }

        let res = self
            .update_subtree_with_patches(mutable_file, self.root_page_id, patches, create_ts)
            .await?;
        if !res.touched {
            return Err(Error::InvalidState);
        }
        Ok(res.new_page_id)
    }

    /// Appends sorted `(start_row_id, block_id)` entries and returns new root page id.
    ///
    /// The update follows copy-on-write semantics on the right-most path.
    pub async fn batch_insert<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[(RowID, u64)],
        new_end_row_id: RowID,
        create_ts: u64,
    ) -> Result<PageID> {
        debug_assert!(entries_sorted(entries));
        debug_assert!(
            entries
                .first()
                .is_none_or(|entry| entry.0 >= self.end_row_id)
        );
        debug_assert!(entries.last().is_none_or(|entry| entry.0 < new_end_row_id));
        debug_assert!(new_end_row_id >= self.end_row_id);
        if entries.is_empty() {
            return Ok(self.root_page_id);
        }

        if self.root_page_id == 0 {
            return self
                .build_tree_from_entries(mutable_file, entries, create_ts)
                .await;
        }
        let root_height = self.read_node(self.root_page_id).await?.header.height;
        let append_result = self
            .append_rightmost_path(mutable_file, entries, create_ts)
            .await?;
        if append_result.extra_entries.is_empty() {
            return Ok(append_result.new_page_id);
        }
        let mut root_entries = Vec::with_capacity(1 + append_result.extra_entries.len());
        root_entries.push(ColumnBlockBranchEntry {
            start_row_id: append_result.start_row_id,
            page_id: append_result.new_page_id,
        });
        root_entries.extend_from_slice(&append_result.extra_entries);
        let new_root_page_id = self
            .build_branch_levels(mutable_file, root_entries, root_height + 1, create_ts)
            .await?;
        Ok(new_root_page_id)
    }

    fn update_subtree_with_patches<'b, M: MutableCowFile + 'b, P: LeafPayloadPatch + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        page_id: PageID,
        patches: &'b [P],
        create_ts: u64,
    ) -> Pin<Box<dyn Future<Output = Result<NodeUpdateResult>> + 'b>> {
        Box::pin(async move {
            if patches.is_empty() {
                return Ok(NodeUpdateResult {
                    new_page_id: page_id,
                    touched: false,
                });
            }
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                return self
                    .update_leaf_with_patches(mutable_file, page_id, &node, patches, create_ts)
                    .await;
            }
            self.update_branch_with_patches(mutable_file, page_id, &node, patches, create_ts)
                .await
        })
    }

    async fn update_leaf_with_patches<M: MutableCowFile, P: LeafPayloadPatch>(
        &self,
        mutable_file: &mut M,
        page_id: PageID,
        node: &ValidatedColumnBlockNode,
        patches: &[P],
        create_ts: u64,
    ) -> Result<NodeUpdateResult> {
        let row_ids = node.leaf_start_row_ids();
        let mut update_slots = Vec::with_capacity(patches.len());
        for patch in patches {
            match row_ids.binary_search(&patch.start_row_id()) {
                Ok(slot_idx) => update_slots.push((slot_idx, *patch)),
                Err(_) => return Err(Error::InvalidArgument),
            }
        }

        let (new_page_id, mut new_node) =
            self.allocate_node(mutable_file, 0, node.header.start_row_id, create_ts)?;
        new_node.header.count = node.header.count;
        {
            let (new_row_ids, new_payloads) = new_node.leaf_arrays_mut();
            new_row_ids.copy_from_slice(row_ids);
            new_payloads.copy_from_slice(node.leaf_payloads());
            for (slot_idx, patch) in update_slots {
                patch.apply_payload(&mut new_payloads[slot_idx]);
            }
        }
        self.write_node(mutable_file, new_page_id, &new_node)
            .await?;
        self.record_obsolete_node(mutable_file, page_id)?;
        Ok(NodeUpdateResult {
            new_page_id,
            touched: true,
        })
    }

    async fn update_branch_with_patches<M: MutableCowFile, P: LeafPayloadPatch>(
        &self,
        mutable_file: &mut M,
        page_id: PageID,
        node: &ValidatedColumnBlockNode,
        patches: &[P],
        create_ts: u64,
    ) -> Result<NodeUpdateResult> {
        let old_entries = node.branch_entries();
        if old_entries.is_empty() {
            return Err(Error::InvalidState);
        }

        let mut patch_idx = 0usize;
        let mut child_updates = Vec::new();
        // Future improvement: disjoint child ranges can be processed in parallel.
        // We keep this loop sequential today because subtree updates mutate one shared
        // `MutableCowFile` (page-id allocation + obsolete-page recording), so safe
        // parallel writes require an additional staging or allocator-partition design.
        for (child_idx, entry) in old_entries.iter().enumerate() {
            let next_start_row_id = old_entries
                .get(child_idx + 1)
                .map(|next| next.start_row_id)
                .unwrap_or(RowID::MAX);
            let range_start = patch_idx;
            while patch_idx < patches.len() && patches[patch_idx].start_row_id() < next_start_row_id
            {
                patch_idx += 1;
            }
            if patch_idx == range_start {
                continue;
            }
            let child_res = self
                .update_subtree_with_patches(
                    mutable_file,
                    entry.page_id,
                    &patches[range_start..patch_idx],
                    create_ts,
                )
                .await?;
            if child_res.touched {
                child_updates.push((child_idx, child_res.new_page_id));
            }
        }
        if patch_idx != patches.len() {
            return Err(Error::InvalidArgument);
        }
        if child_updates.is_empty() {
            return Ok(NodeUpdateResult {
                new_page_id: page_id,
                touched: false,
            });
        }

        let (new_page_id, mut new_node) = self.allocate_node(
            mutable_file,
            node.header.height,
            node.header.start_row_id,
            create_ts,
        )?;
        new_node.header.count = node.header.count;
        new_node.branch_entries_mut().copy_from_slice(old_entries);
        for (child_idx, child_page_id) in child_updates {
            new_node.branch_entries_mut()[child_idx].page_id = child_page_id;
        }
        self.write_node(mutable_file, new_page_id, &new_node)
            .await?;
        self.record_obsolete_node(mutable_file, page_id)?;
        Ok(NodeUpdateResult {
            new_page_id,
            touched: true,
        })
    }

    /// Allocate a new node page for copy-on-write updates.
    #[inline]
    pub fn allocate_node<M: MutableCowFile>(
        &self,
        table_file: &mut M,
        height: u32,
        start_row_id: RowID,
        create_ts: u64,
    ) -> Result<(PageID, Box<ColumnBlockNode>)> {
        let page_id = table_file.allocate_page_id()?;
        let node = ColumnBlockNode::new_boxed(height, start_row_id, create_ts);
        Ok((page_id, node))
    }

    /// Record an obsolete node page to be reclaimed after commit.
    #[inline]
    pub fn record_obsolete_node<M: MutableCowFile>(
        &self,
        table_file: &mut M,
        page_id: PageID,
    ) -> Result<()> {
        if page_id == 0 {
            return Err(Error::InvalidState);
        }
        table_file.record_gc_page(page_id);
        Ok(())
    }

    async fn write_node<M: MutableCowFile>(
        &self,
        mutable_file: &M,
        page_id: PageID,
        node: &ColumnBlockNode,
    ) -> Result<()> {
        let mut buf = DirectBuf::zeroed(COLUMN_BLOCK_PAGE_SIZE);
        let payload_start = write_page_header(buf.data_mut(), COLUMN_BLOCK_INDEX_PAGE_SPEC);
        let payload_end = payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE;
        let dst = &mut buf.data_mut()[payload_start..payload_end];
        let header_bytes = cast_slice(std::slice::from_ref(&node.header));
        dst[..COLUMN_BLOCK_HEADER_SIZE].copy_from_slice(header_bytes);
        dst[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE]
            .copy_from_slice(&node.data);
        write_page_checksum(buf.data_mut());
        mutable_file.write_page(page_id, buf).await
    }

    async fn build_tree_from_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[(RowID, u64)],
        create_ts: u64,
    ) -> Result<PageID> {
        let leaf_entries = self
            .build_leaf_nodes(mutable_file, entries, create_ts)
            .await?;
        if leaf_entries.len() == 1 {
            return Ok(leaf_entries[0].page_id);
        }
        self.build_branch_levels(mutable_file, leaf_entries, 1, create_ts)
            .await
    }

    async fn build_leaf_nodes<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[(RowID, u64)],
        create_ts: u64,
    ) -> Result<Vec<ColumnBlockBranchEntry>> {
        let mut leaf_entries = Vec::new();
        for chunk in entries.chunks(COLUMN_BLOCK_MAX_ENTRIES) {
            let (page_id, mut node) = self.allocate_node(mutable_file, 0, chunk[0].0, create_ts)?;
            node.header.count = chunk.len() as u32;
            {
                let (row_ids, payloads) = node.leaf_arrays_mut();
                for (idx, entry) in chunk.iter().enumerate() {
                    row_ids[idx] = entry.0;
                    payloads[idx] = ColumnPagePayload {
                        block_id: entry.1,
                        deletion_field: [0u8; 120],
                    };
                }
            }
            self.write_node(mutable_file, page_id, &node).await?;
            leaf_entries.push(ColumnBlockBranchEntry {
                start_row_id: chunk[0].0,
                page_id,
            });
        }
        Ok(leaf_entries)
    }

    async fn build_branch_levels<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        mut entries: Vec<ColumnBlockBranchEntry>,
        mut height: u32,
        create_ts: u64,
    ) -> Result<PageID> {
        loop {
            if entries.len() <= COLUMN_BLOCK_MAX_BRANCH_ENTRIES {
                let (page_id, mut node) =
                    self.allocate_node(mutable_file, height, entries[0].start_row_id, create_ts)?;
                node.header.count = entries.len() as u32;
                node.branch_entries_mut().copy_from_slice(&entries);
                self.write_node(mutable_file, page_id, &node).await?;
                return Ok(page_id);
            }
            let mut next_entries = Vec::new();
            for chunk in entries.chunks(COLUMN_BLOCK_MAX_BRANCH_ENTRIES) {
                let (page_id, mut node) =
                    self.allocate_node(mutable_file, height, chunk[0].start_row_id, create_ts)?;
                node.header.count = chunk.len() as u32;
                node.branch_entries_mut().copy_from_slice(chunk);
                self.write_node(mutable_file, page_id, &node).await?;
                next_entries.push(ColumnBlockBranchEntry {
                    start_row_id: chunk[0].start_row_id,
                    page_id,
                });
            }
            entries = next_entries;
            height += 1;
        }
    }

    async fn append_rightmost_path<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[(RowID, u64)],
        create_ts: u64,
    ) -> Result<NodeAppendResult> {
        let mut path = Vec::new();
        let mut page_id = self.root_page_id;
        loop {
            let node = self.read_node(page_id).await?;
            path.push(page_id);
            if node.is_leaf() {
                break;
            } else {
                let next_page_id = node
                    .branch_entries()
                    .last()
                    .map(|entry| entry.page_id)
                    .ok_or(Error::InvalidState)?;
                page_id = next_page_id;
            }
        }

        let leaf_page_id = path.pop().ok_or(Error::InvalidState)?;
        let mut child_result = self
            .append_to_leaf(mutable_file, leaf_page_id, entries, create_ts)
            .await?;

        for page_id in path.into_iter().rev() {
            child_result = self
                .append_to_branch_with_child(mutable_file, page_id, child_result, create_ts)
                .await?;
        }

        Ok(child_result)
    }

    async fn append_to_leaf<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        page_id: PageID,
        entries: &[(RowID, u64)],
        create_ts: u64,
    ) -> Result<NodeAppendResult> {
        let capacity = COLUMN_BLOCK_MAX_ENTRIES;
        let node = self.read_node(page_id).await?;
        if !node.is_leaf() {
            return Err(Error::InvalidState);
        }

        let old_count = node.header.count as usize;
        let take = entries.len().min(capacity.saturating_sub(old_count));
        let new_count = old_count + take;
        let start_row_id = if old_count == 0 {
            entries
                .first()
                .map(|entry| entry.0)
                .unwrap_or(node.header.start_row_id)
        } else {
            node.header.start_row_id
        };
        let (new_page_id, mut new_node) =
            self.allocate_node(mutable_file, 0, start_row_id, create_ts)?;
        new_node.header.count = new_count as u32;
        {
            let (row_ids, payloads) = new_node.leaf_arrays_mut();
            if old_count > 0 {
                row_ids[..old_count].copy_from_slice(node.leaf_start_row_ids());
                payloads[..old_count].copy_from_slice(node.leaf_payloads());
            }
            for (idx, entry) in entries.iter().take(take).enumerate() {
                row_ids[old_count + idx] = entry.0;
                payloads[old_count + idx] = ColumnPagePayload {
                    block_id: entry.1,
                    deletion_field: [0u8; 120],
                };
            }
        }
        let mut remaining = &entries[take..];
        self.write_node(mutable_file, new_page_id, &new_node)
            .await?;
        self.record_obsolete_node(mutable_file, page_id)?;

        let mut extra_entries = Vec::new();
        while !remaining.is_empty() {
            let chunk_len = remaining.len().min(capacity);
            let chunk = &remaining[..chunk_len];
            let (page_id, mut leaf_node) =
                self.allocate_node(mutable_file, 0, chunk[0].0, create_ts)?;
            leaf_node.header.count = chunk_len as u32;
            {
                let (row_ids, payloads) = leaf_node.leaf_arrays_mut();
                for (idx, entry) in chunk.iter().enumerate() {
                    row_ids[idx] = entry.0;
                    payloads[idx] = ColumnPagePayload {
                        block_id: entry.1,
                        deletion_field: [0u8; 120],
                    };
                }
            }
            self.write_node(mutable_file, page_id, &leaf_node).await?;
            extra_entries.push(ColumnBlockBranchEntry {
                start_row_id: chunk[0].0,
                page_id,
            });
            remaining = &remaining[chunk_len..];
        }

        Ok(NodeAppendResult {
            new_page_id,
            start_row_id,
            extra_entries,
        })
    }

    async fn append_to_branch_with_child<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        page_id: PageID,
        child_result: NodeAppendResult,
        create_ts: u64,
    ) -> Result<NodeAppendResult> {
        let node = self.read_node(page_id).await?;
        if !node.is_branch() {
            return Err(Error::InvalidState);
        }
        let old_entries = node.branch_entries();
        if old_entries.is_empty() {
            return Err(Error::InvalidState);
        }
        let last_idx = old_entries.len() - 1;
        let mut new_entries =
            Vec::with_capacity(old_entries.len() + child_result.extra_entries.len());
        new_entries.extend_from_slice(old_entries);
        new_entries[last_idx].page_id = child_result.new_page_id;
        new_entries.extend_from_slice(&child_result.extra_entries);
        let height = node.header.height;
        let start_row_id = node.header.start_row_id;

        let mut remaining = new_entries.as_slice();
        let first_len = remaining.len().min(COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
        let (new_page_id, mut new_node) =
            self.allocate_node(mutable_file, height, start_row_id, create_ts)?;
        new_node.header.count = first_len as u32;
        new_node
            .branch_entries_mut()
            .copy_from_slice(&remaining[..first_len]);
        self.write_node(mutable_file, new_page_id, &new_node)
            .await?;
        self.record_obsolete_node(mutable_file, page_id)?;

        remaining = &remaining[first_len..];
        let mut extra_entries = Vec::new();
        while !remaining.is_empty() {
            let chunk_len = remaining.len().min(COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
            let chunk = &remaining[..chunk_len];
            let (page_id, mut branch_node) =
                self.allocate_node(mutable_file, height, chunk[0].start_row_id, create_ts)?;
            branch_node.header.count = chunk_len as u32;
            branch_node.branch_entries_mut().copy_from_slice(chunk);
            self.write_node(mutable_file, page_id, &branch_node).await?;
            extra_entries.push(ColumnBlockBranchEntry {
                start_row_id: chunk[0].start_row_id,
                page_id,
            });
            remaining = &remaining[chunk_len..];
        }

        Ok(NodeAppendResult {
            new_page_id,
            start_row_id,
            extra_entries,
        })
    }
}

struct NodeAppendResult {
    new_page_id: PageID,
    start_row_id: RowID,
    extra_entries: Vec<ColumnBlockBranchEntry>,
}

fn entries_sorted(entries: &[(RowID, u64)]) -> bool {
    entries.windows(2).all(|pair| pair[0].0 <= pair[1].0)
}

fn patches_sorted_unique(patches: &[OffloadedBitmapPatch<'_>]) -> bool {
    patches_sorted_unique_by_start_row_id(patches, |patch| patch.start_row_id)
        && patches.iter().all(|patch| !patch.bitmap_bytes.is_empty())
}

fn patches_sorted_unique_by_start_row_id<P, F>(patches: &[P], mut key: F) -> bool
where
    F: FnMut(&P) -> RowID,
{
    patches.windows(2).all(|pair| key(&pair[0]) < key(&pair[1]))
}

fn search_start_row_id(start_row_ids: &[RowID], row_id: RowID) -> Option<usize> {
    match start_row_ids.binary_search(&row_id) {
        Ok(idx) => Some(idx),
        Err(0) => None,
        Err(idx) => Some(idx - 1),
    }
}

fn search_branch_entry(entries: &[ColumnBlockBranchEntry], row_id: RowID) -> Option<usize> {
    match entries.binary_search_by_key(&row_id, |entry| entry.start_row_id) {
        Ok(idx) => Some(idx),
        Err(0) => None,
        Err(idx) => Some(idx - 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{PersistedBlockKey, global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
    };
    use crate::error::{PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind};
    use crate::file::build_test_fs;
    use crate::file::table_file::MutableTableFile;
    use crate::index::load_payload_deletion_deltas;
    use crate::value::ValKind;
    use std::sync::Arc;

    fn copy_persisted_node(
        page: &[u8],
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<Box<ColumnBlockNode>> {
        let (payload, header) = validate_node_payload(page, file_kind, page_id)?;
        let mut node =
            ColumnBlockNode::new_boxed(header.height, header.start_row_id, header.create_ts);
        node.header = header;
        node.data.copy_from_slice(
            &payload[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE],
        );
        Ok(node)
    }

    #[inline]
    async fn read_node_from_file(
        disk_pool: &ReadonlyBufferPool,
        page_id: PageID,
    ) -> Result<Box<ColumnBlockNode>> {
        let page = disk_pool
            .get_validated_page_shared(page_id, validate_persisted_column_block_index_page)
            .await?;
        copy_persisted_node(page.page(), PersistedFileKind::TableFile, page_id)
    }

    fn build_persisted_node() -> DirectBuf {
        let mut node = ColumnBlockNode::new_boxed(0, 0, 1);
        node.header.count = 1;
        node.leaf_start_row_ids_mut()[0] = 0;
        node.leaf_payloads_mut()[0] = ColumnPagePayload {
            block_id: 7,
            deletion_field: [0u8; 120],
        };

        let mut buf = DirectBuf::zeroed(COLUMN_BLOCK_PAGE_SIZE);
        let payload_start = write_page_header(buf.data_mut(), COLUMN_BLOCK_INDEX_PAGE_SPEC);
        let payload_end = payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE;
        let dst = &mut buf.data_mut()[payload_start..payload_end];
        let header_bytes = cast_slice(std::slice::from_ref(&node.header));
        dst[..COLUMN_BLOCK_HEADER_SIZE].copy_from_slice(header_bytes);
        dst[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE]
            .copy_from_slice(&node.data);
        write_page_checksum(buf.data_mut());
        buf
    }

    #[test]
    fn test_column_block_node_size() {
        assert_eq!(
            mem::size_of::<ColumnBlockNode>(),
            COLUMN_BLOCK_NODE_PAYLOAD_SIZE
        );
    }

    #[test]
    fn test_parse_persisted_node_rejects_version_corruption() {
        let mut buf = build_persisted_node();
        let version_start = COLUMN_BLOCK_INDEX_PAGE_SPEC.magic_word.len();
        let version_end = version_start + mem::size_of::<u64>();
        buf.data_mut()[version_start..version_end].copy_from_slice(&9u64.to_le_bytes());

        let err =
            match validate_node_payload(buf.data(), PersistedFileKind::CatalogMultiTableFile, 13) {
                Ok(_) => panic!("expected column-block-index version corruption"),
                Err(err) => err,
            };
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::CatalogMultiTableFile,
                page_kind: PersistedPageKind::ColumnBlockIndex,
                page_id: 13,
                cause: PersistedPageCorruptionCause::InvalidVersion,
            }
        ));
    }

    #[test]
    fn test_validate_node_payload_rejects_invalid_leaf_count() {
        let mut buf = build_persisted_node();
        let payload_start = crate::file::page_integrity::PAGE_INTEGRITY_HEADER_SIZE;
        let payload_end = payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE;
        let mut node = copy_persisted_node(buf.data(), PersistedFileKind::TableFile, 17).unwrap();
        node.header.count = (COLUMN_BLOCK_MAX_ENTRIES + 1) as u32;
        let dst = &mut buf.data_mut()[payload_start..payload_end];
        let header_bytes = cast_slice(std::slice::from_ref(&node.header));
        dst[..COLUMN_BLOCK_HEADER_SIZE].copy_from_slice(header_bytes);
        write_page_checksum(buf.data_mut());

        let err = match validate_node_payload(buf.data(), PersistedFileKind::TableFile, 17) {
            Ok(_) => panic!("expected invalid leaf count corruption"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::ColumnBlockIndex,
                page_id: 17,
                cause: PersistedPageCorruptionCause::InvalidPayload,
            }
        ));
    }

    #[test]
    fn test_leaf_layout_offsets() {
        let mut node = ColumnBlockNode::new_boxed(0, 0, 0);
        node.header.count = 2;

        let start_ptr = node.leaf_start_row_ids().as_ptr() as usize;
        let payload_ptr = node.leaf_payloads().as_ptr() as usize;
        assert_eq!(payload_ptr - start_ptr, 2 * mem::size_of::<RowID>());

        let start_mut = node.leaf_start_row_ids_mut();
        start_mut[0] = 10;
        start_mut[1] = 20;
        let payloads = node.leaf_payloads_mut();
        payloads[0].block_id = 1;
        payloads[1].block_id = 2;

        assert_eq!(node.leaf_start_row_ids(), &[10, 20]);
        assert_eq!(node.leaf_payloads()[0].block_id, 1);
        assert_eq!(node.leaf_payloads()[1].block_id, 2);
    }

    #[test]
    fn test_branch_add_entry() {
        let mut node = ColumnBlockNode::new_boxed(1, 0, 0);
        node.branch_add_entry(5, 7);
        assert_eq!(node.header.count, 1);
        assert_eq!(
            node.branch_entries()[0],
            ColumnBlockBranchEntry {
                start_row_id: 5,
                page_id: 7
            }
        );
    }

    fn build_deletion_payload() -> ColumnPagePayload {
        ColumnPagePayload {
            block_id: 0,
            deletion_field: [0u8; 120],
        }
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

    fn build_entries(start: RowID, count: usize, base_block: u64) -> Vec<(RowID, u64)> {
        (0..count)
            .map(|idx| (start + idx as RowID, base_block + idx as u64))
            .collect()
    }

    fn run_with_large_stack<F>(f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(f)
            .expect("spawn test thread")
            .join()
            .expect("join test thread");
    }

    #[test]
    fn test_batch_insert_into_empty_tree_and_find() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(200, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 200, &table_file);

                let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                let entries = vec![(10, 100), (20, 200), (30, 300)];
                let mut mutable = MutableTableFile::fork(&table_file);
                let new_root = index
                    .batch_insert(&mut mutable, &entries, 40, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let index = ColumnBlockIndex::new(new_root, 40, &disk_pool);
                assert_eq!(index.find(9).await.unwrap(), None);
                assert_eq!(index.find(10).await.unwrap().unwrap().block_id, 100);
                assert_eq!(index.find(19).await.unwrap().unwrap().block_id, 100);
                assert_eq!(index.find(20).await.unwrap().unwrap().block_id, 200);
                assert_eq!(index.find(39).await.unwrap().unwrap().block_id, 300);
                assert_eq!(index.find(40).await.unwrap(), None);

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_index_accessors_and_find() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(203, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 203, &table_file);

                let entries = vec![(5, 500), (15, 600)];
                let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_page = index
                    .batch_insert(&mut mutable, &entries, 20, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let index = ColumnBlockIndex::new(root_page, 20, &disk_pool);
                assert_eq!(index.root_page_id(), root_page);
                assert_eq!(index.end_row_id(), 20);
                assert_eq!(index.find(6).await.unwrap().unwrap().block_id, 500);
                assert_eq!(index.find(19).await.unwrap().unwrap().block_id, 600);
                assert_eq!(index.find(4).await.unwrap(), None);

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_batch_insert_appends_within_leaf() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(201, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 201, &table_file);

                let entries = build_entries(0, 8, 1000);
                let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_page = index
                    .batch_insert(&mut mutable, &entries, 8, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let more_entries = build_entries(8, 4, 2000);
                let index = ColumnBlockIndex::new(root_page, 8, &disk_pool);
                let mut mutable = MutableTableFile::fork(&table_file);
                let new_root = index
                    .batch_insert(&mut mutable, &more_entries, 12, 3)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let index = ColumnBlockIndex::new(new_root, 12, &disk_pool);
                assert_eq!(index.find(0).await.unwrap().unwrap().block_id, 1000);
                assert_eq!(index.find(7).await.unwrap().unwrap().block_id, 1007);
                assert_eq!(index.find(8).await.unwrap().unwrap().block_id, 2000);
                assert_eq!(index.find(11).await.unwrap().unwrap().block_id, 2003);

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_build_branch_levels_multiple_layers() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(204, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);

                let entry_count = COLUMN_BLOCK_MAX_BRANCH_ENTRIES + 1;
                let entries = (0..entry_count)
                    .map(|idx| ColumnBlockBranchEntry {
                        start_row_id: idx as RowID,
                        page_id: (1000 + idx) as PageID,
                    })
                    .collect::<Vec<_>>();

                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 204, &table_file);
                let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_page = index
                    .build_branch_levels(&mut mutable, entries, 1, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let root_node = read_node_from_file(&disk_pool, root_page).await.unwrap();
                assert_eq!(root_node.header.height, 2);
                assert_eq!(root_node.header.count, 2);
                let root_entries = root_node.branch_entries();
                assert_eq!(root_entries[0].start_row_id, 0);
                assert_eq!(
                    root_entries[1].start_row_id,
                    COLUMN_BLOCK_MAX_BRANCH_ENTRIES as RowID
                );

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_batch_insert_creates_new_leaf_nodes() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(202, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 202, &table_file);

                let initial_count = COLUMN_BLOCK_MAX_ENTRIES - 1;
                let entries = build_entries(0, initial_count, 1);
                let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_page = index
                    .batch_insert(&mut mutable, &entries, initial_count as RowID, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let append_entries = build_entries(initial_count as RowID, 2, 9000);
                let index = ColumnBlockIndex::new(root_page, initial_count as RowID, &disk_pool);
                let mut mutable = MutableTableFile::fork(&table_file);
                let new_root = index
                    .batch_insert(&mut mutable, &append_entries, initial_count as RowID + 2, 3)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let index = ColumnBlockIndex::new(new_root, initial_count as RowID + 2, &disk_pool);
                assert_eq!(index.find(0).await.unwrap().unwrap().block_id, 1);
                assert_eq!(
                    index
                        .find(initial_count as RowID)
                        .await
                        .unwrap()
                        .unwrap()
                        .block_id,
                    9000
                );
                assert_eq!(
                    index
                        .find(initial_count as RowID + 1)
                        .await
                        .unwrap()
                        .unwrap()
                        .block_id,
                    9001
                );

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_append_to_branch_with_child_splits() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(205, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);

                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let (table_file, obsolete_page_id) = {
                    let disk_pool = table_readonly_pool(&global, 205, &table_file);
                    let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                    let mut setup_mutable = MutableTableFile::fork(&table_file);
                    let obsolete_page_id = setup_mutable.allocate_page_id().unwrap();
                    let mut node = ColumnBlockNode::new_boxed(1, 0, 0);
                    let mut entries = Vec::with_capacity(COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
                    for idx in 0..COLUMN_BLOCK_MAX_BRANCH_ENTRIES {
                        entries.push(ColumnBlockBranchEntry {
                            start_row_id: idx as RowID,
                            page_id: (2000 + idx) as PageID,
                        });
                    }
                    node.header.count = entries.len() as u32;
                    node.branch_entries_mut().copy_from_slice(&entries);
                    index
                        .write_node(&setup_mutable, obsolete_page_id, &node)
                        .await
                        .unwrap();
                    let (table_file, old_root) = setup_mutable.commit(2, false).await.unwrap();
                    drop(old_root);
                    (table_file, obsolete_page_id)
                };

                let (table_file, child_page_id, result) = {
                    let disk_pool = table_readonly_pool(&global, 205, &table_file);
                    let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                    let mut mutable = MutableTableFile::fork(&table_file);
                    let child_page_id = mutable.allocate_page_id().unwrap();
                    let extra_entries = vec![
                        ColumnBlockBranchEntry {
                            start_row_id: COLUMN_BLOCK_MAX_BRANCH_ENTRIES as RowID,
                            page_id: mutable.allocate_page_id().unwrap(),
                        },
                        ColumnBlockBranchEntry {
                            start_row_id: COLUMN_BLOCK_MAX_BRANCH_ENTRIES as RowID + 1,
                            page_id: mutable.allocate_page_id().unwrap(),
                        },
                    ];

                    let result = index
                        .append_to_branch_with_child(
                            &mut mutable,
                            obsolete_page_id,
                            NodeAppendResult {
                                new_page_id: child_page_id,
                                start_row_id: 0,
                                extra_entries,
                            },
                            2,
                        )
                        .await
                        .unwrap();
                    let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                    drop(old_root);
                    (table_file, child_page_id, result)
                };

                let disk_pool = table_readonly_pool(&global, 205, &table_file);
                let new_root = read_node_from_file(&disk_pool, result.new_page_id)
                    .await
                    .unwrap();
                let new_entries = new_root.branch_entries();
                assert_eq!(new_entries.len(), COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
                assert_eq!(
                    new_entries[COLUMN_BLOCK_MAX_BRANCH_ENTRIES - 1].page_id,
                    child_page_id
                );
                assert_eq!(result.extra_entries.len(), 1);
                assert_eq!(
                    result.extra_entries[0].start_row_id,
                    COLUMN_BLOCK_MAX_BRANCH_ENTRIES as RowID
                );

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_batch_update_offloaded_bitmaps_cow_snapshot() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let table_file = fs
                    .create_table_file(207, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 207, &table_file);

                let initial_count = COLUMN_BLOCK_MAX_ENTRIES + 8;
                let entries = build_entries(0, initial_count, 7000);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                    .batch_insert(&mut mutable, &entries, initial_count as RowID, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let index_v1 = ColumnBlockIndex::new(root_v1, initial_count as RowID, &disk_pool);
                let patch_bytes = vec![3u8; 1024];
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v2 = index_v1
                    .batch_update_offloaded_bitmaps(
                        &mut mutable,
                        &[OffloadedBitmapPatch {
                            start_row_id: 0,
                            bitmap_bytes: &patch_bytes,
                        }],
                        3,
                    )
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let old_root_node = read_node_from_file(&disk_pool, root_v1).await.unwrap();
                let new_root_node = read_node_from_file(&disk_pool, root_v2).await.unwrap();
                assert!(old_root_node.is_branch());
                assert!(new_root_node.is_branch());
                assert_eq!(
                    old_root_node.branch_entries().len(),
                    new_root_node.branch_entries().len()
                );
                assert_ne!(
                    old_root_node.branch_entries()[0].page_id,
                    new_root_node.branch_entries()[0].page_id
                );
                assert_eq!(
                    old_root_node.branch_entries()[1].page_id,
                    new_root_node.branch_entries()[1].page_id
                );

                let old_payload =
                    ColumnBlockIndex::new(root_v1, initial_count as RowID, &disk_pool)
                        .find(0)
                        .await
                        .unwrap()
                        .unwrap();
                assert_eq!(old_payload.offloaded_ref(), None);

                let index_v2 = ColumnBlockIndex::new(root_v2, initial_count as RowID, &disk_pool);
                let new_entry = index_v2.find_entry(0).await.unwrap().unwrap();
                assert!(new_entry.payload.offloaded_ref().is_some());
                assert_eq!(
                    index_v2
                        .read_offloaded_bitmap_bytes(&new_entry)
                        .await
                        .unwrap()
                        .unwrap(),
                    patch_bytes
                );
                let untouched = index_v2
                    .find(COLUMN_BLOCK_MAX_ENTRIES as RowID)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(untouched.offloaded_ref(), None);

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_batch_update_offloaded_bitmaps_multi_leaf() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let table_file = fs
                    .create_table_file(208, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 208, &table_file);

                let initial_count = COLUMN_BLOCK_MAX_ENTRIES + 8;
                let entries = build_entries(0, initial_count, 8000);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                    .batch_insert(&mut mutable, &entries, initial_count as RowID, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let left_bytes = vec![1u8; 321];
                let right_bytes = vec![2u8; 777];
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v2 = ColumnBlockIndex::new(root_v1, initial_count as RowID, &disk_pool)
                    .batch_update_offloaded_bitmaps(
                        &mut mutable,
                        &[
                            OffloadedBitmapPatch {
                                start_row_id: 0,
                                bitmap_bytes: &left_bytes,
                            },
                            OffloadedBitmapPatch {
                                start_row_id: COLUMN_BLOCK_MAX_ENTRIES as RowID,
                                bitmap_bytes: &right_bytes,
                            },
                        ],
                        3,
                    )
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let index_v2 = ColumnBlockIndex::new(root_v2, initial_count as RowID, &disk_pool);
                let left_entry = index_v2.find_entry(0).await.unwrap().unwrap();
                let right_entry = index_v2
                    .find_entry(COLUMN_BLOCK_MAX_ENTRIES as RowID)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    index_v2
                        .read_offloaded_bitmap_bytes(&left_entry)
                        .await
                        .unwrap()
                        .unwrap(),
                    left_bytes
                );
                assert_eq!(
                    index_v2
                        .read_offloaded_bitmap_bytes(&right_entry)
                        .await
                        .unwrap()
                        .unwrap(),
                    right_bytes
                );

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_batch_update_offloaded_bitmaps_sticky() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let table_file = fs
                    .create_table_file(209, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 209, &table_file);

                let entries = build_entries(0, 4, 9000);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                    .batch_insert(&mut mutable, &entries, 4, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let first = vec![11u8; 100];
                let second = vec![22u8; 333];
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v2 = ColumnBlockIndex::new(root_v1, 4, &disk_pool)
                    .batch_update_offloaded_bitmaps(
                        &mut mutable,
                        &[OffloadedBitmapPatch {
                            start_row_id: 0,
                            bitmap_bytes: &first,
                        }],
                        3,
                    )
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v3 = ColumnBlockIndex::new(root_v2, 4, &disk_pool)
                    .batch_update_offloaded_bitmaps(
                        &mut mutable,
                        &[OffloadedBitmapPatch {
                            start_row_id: 0,
                            bitmap_bytes: &second,
                        }],
                        4,
                    )
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(4, false).await.unwrap();
                drop(old_root);

                let index_v2 = ColumnBlockIndex::new(root_v2, 4, &disk_pool);
                let entry_v2 = index_v2.find_entry(0).await.unwrap().unwrap();
                assert!(entry_v2.payload.offloaded_ref().is_some());
                assert_eq!(
                    index_v2
                        .read_offloaded_bitmap_bytes(&entry_v2)
                        .await
                        .unwrap()
                        .unwrap(),
                    first
                );

                let index_v3 = ColumnBlockIndex::new(root_v3, 4, &disk_pool);
                let entry_v3 = index_v3.find_entry(0).await.unwrap().unwrap();
                assert!(entry_v3.payload.offloaded_ref().is_some());
                assert_eq!(
                    index_v3
                        .read_offloaded_bitmap_bytes(&entry_v3)
                        .await
                        .unwrap()
                        .unwrap(),
                    second
                );

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_batch_replace_payloads_cow_snapshot() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let table_file = fs
                    .create_table_file(211, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 211, &table_file);

                let initial_count = COLUMN_BLOCK_MAX_ENTRIES + 8;
                let entries = build_entries(0, initial_count, 11_000);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                    .batch_insert(&mut mutable, &entries, initial_count as RowID, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let index_v1 = ColumnBlockIndex::new(root_v1, initial_count as RowID, &disk_pool);
                let mut replacement = index_v1.find(0).await.unwrap().unwrap();
                replacement.block_id = 99_001;
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v2 = index_v1
                    .batch_replace_payloads(
                        &mut mutable,
                        &[ColumnPagePayloadPatch {
                            start_row_id: 0,
                            payload: replacement,
                        }],
                        3,
                    )
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let old_payload = index_v1.find(0).await.unwrap().unwrap();
                assert_eq!(old_payload.block_id, 11_000);
                let index_v2 = ColumnBlockIndex::new(root_v2, initial_count as RowID, &disk_pool);
                let new_payload = index_v2.find(0).await.unwrap().unwrap();
                assert_eq!(new_payload.block_id, 99_001);
                assert_eq!(
                    new_payload.deletion_field, old_payload.deletion_field,
                    "replace path should preserve payload bytes except explicit updates"
                );
                let untouched = index_v2
                    .find(COLUMN_BLOCK_MAX_ENTRIES as RowID)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(untouched.block_id, 11_000 + COLUMN_BLOCK_MAX_ENTRIES as u64);

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_batch_replace_payloads_multi_leaf() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let table_file = fs
                    .create_table_file(212, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 212, &table_file);

                let initial_count = COLUMN_BLOCK_MAX_ENTRIES + 8;
                let entries = build_entries(0, initial_count, 12_000);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                    .batch_insert(&mut mutable, &entries, initial_count as RowID, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let index_v1 = ColumnBlockIndex::new(root_v1, initial_count as RowID, &disk_pool);
                let mut left = index_v1.find(0).await.unwrap().unwrap();
                left.block_id = 88_001;
                let mut right = index_v1
                    .find(COLUMN_BLOCK_MAX_ENTRIES as RowID)
                    .await
                    .unwrap()
                    .unwrap();
                right.block_id = 88_002;
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v2 = index_v1
                    .batch_replace_payloads(
                        &mut mutable,
                        &[
                            ColumnPagePayloadPatch {
                                start_row_id: 0,
                                payload: left,
                            },
                            ColumnPagePayloadPatch {
                                start_row_id: COLUMN_BLOCK_MAX_ENTRIES as RowID,
                                payload: right,
                            },
                        ],
                        3,
                    )
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let index_v2 = ColumnBlockIndex::new(root_v2, initial_count as RowID, &disk_pool);
                assert_eq!(index_v2.find(0).await.unwrap().unwrap().block_id, 88_001);
                assert_eq!(
                    index_v2
                        .find(COLUMN_BLOCK_MAX_ENTRIES as RowID)
                        .await
                        .unwrap()
                        .unwrap()
                        .block_id,
                    88_002
                );

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_batch_replace_payloads_rejects_invalid_patches() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let table_file = fs
                    .create_table_file(213, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 213, &table_file);

                let entries = build_entries(0, COLUMN_BLOCK_MAX_ENTRIES + 8, 13_000);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                    .batch_insert(&mut mutable, &entries, entries.len() as RowID, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let index_v1 = ColumnBlockIndex::new(root_v1, entries.len() as RowID, &disk_pool);
                let base = index_v1.find(0).await.unwrap().unwrap();

                let mut mutable = MutableTableFile::fork(&table_file);
                let unsorted = [
                    ColumnPagePayloadPatch {
                        start_row_id: COLUMN_BLOCK_MAX_ENTRIES as RowID,
                        payload: base,
                    },
                    ColumnPagePayloadPatch {
                        start_row_id: 0,
                        payload: base,
                    },
                ];
                let err = index_v1
                    .batch_replace_payloads(&mut mutable, &unsorted, 3)
                    .await
                    .unwrap_err();
                assert!(matches!(err, Error::InvalidArgument));
                drop(mutable);

                let mut mutable = MutableTableFile::fork(&table_file);
                let missing = [ColumnPagePayloadPatch {
                    start_row_id: entries.len() as RowID + 100,
                    payload: base,
                }];
                let err = index_v1
                    .batch_replace_payloads(&mut mutable, &missing, 3)
                    .await
                    .unwrap_err();
                assert!(matches!(err, Error::InvalidArgument));

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_read_offloaded_bitmap_invalid_ref() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let table_file = fs
                    .create_table_file(210, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 210, &table_file);

                let mut payload = build_deletion_payload();
                payload.set_offloaded_ref(BlobRef {
                    start_page_id: 0,
                    start_offset: 0,
                    byte_len: 1,
                });
                let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                let entry = ColumnLeafEntry {
                    leaf_page_id: 77,
                    start_row_id: 0,
                    payload,
                };
                let res = index.read_offloaded_bitmap_bytes(&entry).await;
                assert!(matches!(
                    res,
                    Err(Error::PersistedPageCorrupted {
                        file_kind: PersistedFileKind::TableFile,
                        page_kind: PersistedPageKind::ColumnBlockIndex,
                        page_id: 77,
                        cause: PersistedPageCorruptionCause::InvalidPayload,
                    })
                ));

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_load_payload_deletion_deltas_maps_invalid_blob_contents_to_corruption() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let table_file = fs
                    .create_table_file(214, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 214, &table_file);

                let entries = build_entries(0, 1, 9100);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                    .batch_insert(&mut mutable, &entries, 1, 2)
                    .await
                    .unwrap();
                let bad_blob = [1u8, 2, 3];
                let root_v2 = ColumnBlockIndex::new(root_v1, 1, &disk_pool)
                    .batch_update_offloaded_bitmaps(
                        &mut mutable,
                        &[OffloadedBitmapPatch {
                            start_row_id: 0,
                            bitmap_bytes: &bad_blob,
                        }],
                        3,
                    )
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let index = ColumnBlockIndex::new(root_v2, 1, &disk_pool);
                let entry = index.find_entry(0).await.unwrap().unwrap();
                let blob_ref = entry.payload.try_offloaded_ref().unwrap().unwrap();
                let err = load_payload_deletion_deltas(&index, entry)
                    .await
                    .unwrap_err();
                assert!(matches!(
                    err,
                    Error::PersistedPageCorrupted {
                        file_kind: PersistedFileKind::TableFile,
                        page_kind: PersistedPageKind::ColumnDeletionBlob,
                        page_id,
                        cause: PersistedPageCorruptionCause::InvalidPayload,
                    } if page_id == blob_ref.start_page_id
                ));

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }

    #[test]
    fn test_cache_reuse_for_unchanged_leaf_across_root_swap() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let (_temp_dir, fs) = build_test_fs();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(206, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, 206, &table_file);

                // Build at least two leaves so right-most append can preserve left leaf pages.
                let initial_count = COLUMN_BLOCK_MAX_ENTRIES + 8;
                let entries = build_entries(0, initial_count, 5000);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                    .batch_insert(&mut mutable, &entries, initial_count as RowID, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let root_node = read_node_from_file(&disk_pool, root_v1).await.unwrap();
                assert!(root_node.is_branch());
                let left_leaf_page_id = root_node.branch_entries()[0].page_id;

                let index_v1 = ColumnBlockIndex::new(root_v1, initial_count as RowID, &disk_pool);
                assert_eq!(index_v1.find(0).await.unwrap().unwrap().block_id, 5000);

                let left_leaf_key = PersistedBlockKey::new(206, left_leaf_page_id);
                let frame_before = global
                    .try_get_frame_id(&left_leaf_key)
                    .expect("left leaf should be cached after first lookup");

                let append = build_entries(initial_count as RowID, 16, 9000);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_v2 = ColumnBlockIndex::new(root_v1, initial_count as RowID, &disk_pool)
                    .batch_insert(
                        &mut mutable,
                        &append,
                        initial_count as RowID + append.len() as RowID,
                        3,
                    )
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let index_v2 = ColumnBlockIndex::new(
                    root_v2,
                    initial_count as RowID + append.len() as RowID,
                    &disk_pool,
                );
                assert_eq!(index_v2.find(0).await.unwrap().unwrap().block_id, 5000);

                let frame_after = global
                    .try_get_frame_id(&left_leaf_key)
                    .expect("left leaf cache entry should remain valid");
                assert_eq!(frame_before, frame_after);

                drop(disk_pool);
                drop(global);
                drop(table_file);
                drop(fs);
            })
        });
    }
}
