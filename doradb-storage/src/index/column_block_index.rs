use crate::buffer::ReadonlyBufferPool;
use crate::buffer::guard::PageGuard;
use crate::buffer::page::{BufferPage, PageID};
use crate::error::{Error, Result};
#[cfg(test)]
use crate::file::table_file::TableFile;
use crate::file::table_file::{MutableTableFile, TABLE_FILE_PAGE_SIZE};
use crate::index::column_deletion_blob::{
    COLUMN_DELETION_BLOB_PAGE_BODY_SIZE, ColumnDeletionBlobReader, ColumnDeletionBlobWriter,
};
use crate::io::DirectBuf;
use crate::row::RowID;
use bytemuck::{Pod, Zeroable, cast_slice, cast_slice_mut};
use std::future::Future;
use std::mem;
use std::pin::Pin;
#[cfg(test)]
use std::sync::Arc;

pub const COLUMN_BLOCK_PAGE_SIZE: usize = TABLE_FILE_PAGE_SIZE;
pub const COLUMN_BLOCK_HEADER_SIZE: usize = mem::size_of::<ColumnBlockNodeHeader>();
pub const COLUMN_BLOCK_DATA_SIZE: usize = COLUMN_BLOCK_PAGE_SIZE - COLUMN_BLOCK_HEADER_SIZE;
pub const COLUMN_PAGE_PAYLOAD_SIZE: usize = mem::size_of::<ColumnPagePayload>();
pub const COLUMN_BRANCH_ENTRY_SIZE: usize = mem::size_of::<ColumnBlockBranchEntry>();
pub const COLUMN_BLOCK_MAX_ENTRIES: usize =
    COLUMN_BLOCK_DATA_SIZE / (mem::size_of::<RowID>() + COLUMN_PAGE_PAYLOAD_SIZE);
pub const COLUMN_BLOCK_MAX_BRANCH_ENTRIES: usize =
    COLUMN_BLOCK_DATA_SIZE / COLUMN_BRANCH_ENTRY_SIZE;

const _: () = assert!(
    COLUMN_BLOCK_HEADER_SIZE
        + COLUMN_BLOCK_MAX_ENTRIES * (mem::size_of::<RowID>() + COLUMN_PAGE_PAYLOAD_SIZE)
        <= COLUMN_BLOCK_PAGE_SIZE,
    "ColumnBlockNode should fit into 64KB pages"
);

const _: () = assert!(
    mem::size_of::<ColumnBlockNode>() == COLUMN_BLOCK_PAGE_SIZE,
    "ColumnBlockNode size must match table file page size"
);
const _: () = assert!(mem::size_of::<ColumnPagePayload>() == 128);
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
/// Leaf payload that points to one persisted lightweight columnar page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnPagePayload {
    pub block_id: u64,
    pub deletion_field: [u8; 120],
}

// SAFETY: `ColumnPagePayload` is `repr(C)` and contains only plain-data fields.
unsafe impl Zeroable for ColumnPagePayload {}
// SAFETY: `ColumnPagePayload` has no padding-sensitive or pointer/reference fields.
unsafe impl Pod for ColumnPagePayload {}

impl ColumnPagePayload {
    /// Returns deletion-list view backed by the payload bytes.
    #[inline]
    pub fn deletion_list(&mut self) -> DeletionList<'_> {
        DeletionList::new(&mut self.deletion_field)
    }

    /// Returns decoded offloaded bitmap reference if this payload is offloaded.
    #[inline]
    pub fn offloaded_ref(&self) -> Option<BlobRef> {
        self.try_offloaded_ref().ok().flatten()
    }

    /// Sets offloaded bitmap reference.
    #[inline]
    pub fn set_offloaded_ref(&mut self, blob_ref: BlobRef) {
        self.deletion_field[0] &= !DELETION_FLAG_U32;
        self.deletion_field[0] |= DELETION_FLAG_OFFLOADED;
        self.deletion_field[1] = 0;
        self.deletion_field[DELETION_OFFLOAD_REF_OFFSET..DELETION_OFFLOAD_REF_OFFSET + 8]
            .copy_from_slice(&blob_ref.start_page_id.to_le_bytes());
        self.deletion_field[DELETION_OFFLOAD_REF_OFFSET + 8..DELETION_OFFLOAD_REF_OFFSET + 10]
            .copy_from_slice(&blob_ref.start_offset.to_le_bytes());
        self.deletion_field[DELETION_OFFLOAD_REF_OFFSET + 10..DELETION_OFFLOAD_REF_OFFSET + 14]
            .copy_from_slice(&blob_ref.byte_len.to_le_bytes());
    }

    /// Clears inline deletion bytes and stores only offloaded bitmap reference.
    #[inline]
    pub fn clear_inline_and_set_offloaded(&mut self, blob_ref: BlobRef) {
        self.deletion_field.fill(0);
        self.set_offloaded_ref(blob_ref);
    }

    #[inline]
    fn try_offloaded_ref(&self) -> Result<Option<BlobRef>> {
        if (self.deletion_field[0] & DELETION_FLAG_OFFLOADED) == 0 {
            return Ok(None);
        }
        let start_page_id = u64::from_le_bytes(
            self.deletion_field[DELETION_OFFLOAD_REF_OFFSET..DELETION_OFFLOAD_REF_OFFSET + 8]
                .try_into()?,
        );
        let start_offset = u16::from_le_bytes(
            self.deletion_field[DELETION_OFFLOAD_REF_OFFSET + 8..DELETION_OFFLOAD_REF_OFFSET + 10]
                .try_into()?,
        );
        let byte_len = u32::from_le_bytes(
            self.deletion_field[DELETION_OFFLOAD_REF_OFFSET + 10..DELETION_OFFLOAD_REF_OFFSET + 14]
                .try_into()?,
        );
        if start_page_id == 0
            || byte_len == 0
            || (start_offset as usize) >= COLUMN_DELETION_BLOB_PAGE_BODY_SIZE
        {
            return Err(Error::InvalidFormat);
        }
        Ok(Some(BlobRef {
            start_page_id,
            start_offset,
            byte_len,
        }))
    }
}

/// Reference to one offloaded bitmap byte range in linked immutable blob pages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobRef {
    pub start_page_id: PageID,
    pub start_offset: u16,
    pub byte_len: u32,
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

    /// Returns whether this node is a leaf node.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.header.height == 0
    }

    /// Returns whether this node is a branch node.
    #[inline]
    pub fn is_branch(&self) -> bool {
        !self.is_leaf()
    }

    /// Returns leaf start-row-id array.
    #[inline]
    pub fn leaf_start_row_ids(&self) -> &[RowID] {
        debug_assert!(self.is_leaf());
        self.leaf_row_ids_with_count(self.header.count as usize)
    }

    /// Returns mutable leaf start-row-id array.
    #[inline]
    pub fn leaf_start_row_ids_mut(&mut self) -> &mut [RowID] {
        debug_assert!(self.is_leaf());
        self.leaf_row_ids_mut_with_count(self.header.count as usize)
    }

    /// Returns leaf payload array.
    #[inline]
    pub fn leaf_payloads(&self) -> &[ColumnPagePayload] {
        debug_assert!(self.is_leaf());
        let count = self.header.count as usize;
        self.leaf_payloads_with_count(count)
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

    /// Returns branch entries.
    #[inline]
    pub fn branch_entries(&self) -> &[ColumnBlockBranchEntry] {
        debug_assert!(self.is_branch());
        self.branch_entries_with_count(self.header.count as usize)
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
    fn leaf_row_ids_with_count(&self, count: usize) -> &[RowID] {
        debug_assert!(count <= COLUMN_BLOCK_MAX_ENTRIES);
        let row_bytes = count * mem::size_of::<RowID>();
        cast_slice(&self.data[..row_bytes])
    }

    #[inline]
    fn leaf_row_ids_mut_with_count(&mut self, count: usize) -> &mut [RowID] {
        debug_assert!(count <= COLUMN_BLOCK_MAX_ENTRIES);
        let row_bytes = count * mem::size_of::<RowID>();
        cast_slice_mut(&mut self.data[..row_bytes])
    }

    #[inline]
    fn leaf_payloads_with_count(&self, count: usize) -> &[ColumnPagePayload] {
        debug_assert!(count <= COLUMN_BLOCK_MAX_ENTRIES);
        let row_bytes = count * mem::size_of::<RowID>();
        let payload_bytes = count * mem::size_of::<ColumnPagePayload>();
        cast_slice(&self.data[row_bytes..row_bytes + payload_bytes])
    }

    #[inline]
    fn leaf_payloads_mut_with_count(&mut self, count: usize) -> &mut [ColumnPagePayload] {
        debug_assert!(count <= COLUMN_BLOCK_MAX_ENTRIES);
        let row_bytes = count * mem::size_of::<RowID>();
        let payload_bytes = count * mem::size_of::<ColumnPagePayload>();
        cast_slice_mut(&mut self.data[row_bytes..row_bytes + payload_bytes])
    }

    #[inline]
    fn branch_entries_with_count(&self, count: usize) -> &[ColumnBlockBranchEntry] {
        debug_assert!(count <= COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
        let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
        cast_slice(&self.data[..bytes_len])
    }

    #[inline]
    fn branch_entries_mut_with_count(&mut self, count: usize) -> &mut [ColumnBlockBranchEntry] {
        debug_assert!(count <= COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
        let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
        cast_slice_mut(&mut self.data[..bytes_len])
    }
}

impl BufferPage for ColumnBlockNode {}

const DELETION_FIELD_SIZE: usize = 120;
const DELETION_HEADER_SIZE: usize = 2;
const DELETION_OFFLOAD_REF_OFFSET: usize = DELETION_HEADER_SIZE;
const DELETION_U16_OFFSET: usize = DELETION_HEADER_SIZE;
const DELETION_U32_OFFSET: usize = 4;
const DELETION_U16_CAPACITY: usize = (DELETION_FIELD_SIZE - DELETION_U16_OFFSET) / 2;
const DELETION_U32_CAPACITY: usize = (DELETION_FIELD_SIZE - DELETION_U32_OFFSET) / 4;
const DELETION_FLAG_U32: u8 = 0b1000_0000;
const DELETION_FLAG_OFFLOADED: u8 = 0b0100_0000;

/// Error returned by in-payload deletion-list operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeletionListError {
    Full,
}

/// Compact sorted deletion-delta list stored inside `ColumnPagePayload`.
pub struct DeletionList<'a> {
    data: &'a mut [u8; DELETION_FIELD_SIZE],
}

impl<'a> DeletionList<'a> {
    /// Creates a deletion-list view over the 120-byte payload field.
    #[inline]
    pub fn new(data: &'a mut [u8; DELETION_FIELD_SIZE]) -> Self {
        DeletionList { data }
    }

    #[allow(clippy::len_without_is_empty)]
    /// Returns number of stored deletion deltas.
    #[inline]
    pub fn len(&self) -> usize {
        self.data[1] as usize
    }

    /// Returns whether deletion rows are offloaded outside payload.
    #[inline]
    pub fn is_offloaded(&self) -> bool {
        (self.data[0] & DELETION_FLAG_OFFLOADED) != 0
    }

    /// Sets or clears offloaded flag.
    #[inline]
    pub fn set_offloaded(&mut self, offloaded: bool) {
        if offloaded {
            self.data[0] |= DELETION_FLAG_OFFLOADED;
        } else {
            self.data[0] &= !DELETION_FLAG_OFFLOADED;
        }
    }

    /// Returns whether `delta` exists in the sorted list.
    #[inline]
    pub fn contains(&self, delta: u32) -> bool {
        if self.is_format_u32() {
            self.deltas_u32().binary_search(&delta).is_ok()
        } else if delta > u16::MAX as u32 {
            false
        } else {
            let delta = delta as u16;
            self.deltas_u16().binary_search(&delta).is_ok()
        }
    }

    /// Inserts one deletion delta.
    ///
    /// Returns:
    /// - `Ok(true)` if inserted
    /// - `Ok(false)` if already exists
    /// - `Err(Full)` if in-payload capacity is exhausted
    pub fn add(&mut self, delta: u32) -> std::result::Result<bool, DeletionListError> {
        if !self.is_format_u32() && delta > u16::MAX as u32 {
            self.promote_to_u32()?;
        }

        if self.is_format_u32() {
            let count = self.len();
            let deltas = self.deltas_u32_mut();
            match deltas[..count].binary_search(&delta) {
                Ok(_) => Ok(false),
                Err(idx) => {
                    if count >= DELETION_U32_CAPACITY {
                        return Err(DeletionListError::Full);
                    }
                    if idx < count {
                        deltas.copy_within(idx..count, idx + 1);
                    }
                    deltas[idx] = delta;
                    self.set_count(count + 1);
                    Ok(true)
                }
            }
        } else {
            let count = self.len();
            let delta = delta as u16;
            let deltas = self.deltas_u16_mut();
            match deltas[..count].binary_search(&delta) {
                Ok(_) => Ok(false),
                Err(idx) => {
                    if count >= DELETION_U16_CAPACITY {
                        return Err(DeletionListError::Full);
                    }
                    if idx < count {
                        deltas.copy_within(idx..count, idx + 1);
                    }
                    deltas[idx] = delta;
                    self.set_count(count + 1);
                    Ok(true)
                }
            }
        }
    }

    /// Iterates deletion deltas as row-id offsets.
    #[inline]
    pub fn iter(&self) -> DeletionListIter<'_> {
        if self.is_format_u32() {
            DeletionListIter::U32 {
                data: self.deltas_u32(),
                index: 0,
            }
        } else {
            DeletionListIter::U16 {
                data: self.deltas_u16(),
                index: 0,
            }
        }
    }

    /// Iterates absolute row ids using `start_row_id + delta`.
    #[inline]
    pub fn iter_row_ids(&self, start_row_id: RowID) -> impl Iterator<Item = RowID> + '_ {
        self.iter().map(move |delta| start_row_id + delta as RowID)
    }

    #[inline]
    fn set_count(&mut self, count: usize) {
        self.data[1] = count as u8;
    }

    #[inline]
    fn is_format_u32(&self) -> bool {
        (self.data[0] & DELETION_FLAG_U32) != 0
    }

    #[inline]
    fn set_format_u32(&mut self, enabled: bool) {
        if enabled {
            self.data[0] |= DELETION_FLAG_U32;
        } else {
            self.data[0] &= !DELETION_FLAG_U32;
        }
    }

    #[inline]
    fn deltas_u16(&self) -> &[u16] {
        let count = self.len();
        debug_assert!(count <= DELETION_U16_CAPACITY);
        let end = DELETION_U16_OFFSET + count * 2;
        cast_slice(&self.data[DELETION_U16_OFFSET..end])
    }

    #[inline]
    fn deltas_u16_mut(&mut self) -> &mut [u16] {
        cast_slice_mut(&mut self.data[DELETION_U16_OFFSET..])
    }

    #[inline]
    fn deltas_u32(&self) -> &[u32] {
        let count = self.len();
        debug_assert!(count <= DELETION_U32_CAPACITY);
        let end = DELETION_U32_OFFSET + count * 4;
        cast_slice(&self.data[DELETION_U32_OFFSET..end])
    }

    #[inline]
    fn deltas_u32_mut(&mut self) -> &mut [u32] {
        cast_slice_mut(&mut self.data[DELETION_U32_OFFSET..])
    }

    fn promote_to_u32(&mut self) -> std::result::Result<(), DeletionListError> {
        let count = self.len();
        if count > DELETION_U32_CAPACITY {
            return Err(DeletionListError::Full);
        }
        let mut temp = [0u32; DELETION_U32_CAPACITY];
        for (idx, delta) in self.deltas_u16().iter().enumerate() {
            temp[idx] = *delta as u32;
        }
        self.deltas_u32_mut()[..count].copy_from_slice(&temp[..count]);
        self.set_format_u32(true);
        Ok(())
    }
}

/// Iterator over deletion deltas in either u16 or u32 encoding mode.
pub enum DeletionListIter<'a> {
    U16 { data: &'a [u16], index: usize },
    U32 { data: &'a [u32], index: usize },
}

impl<'a> Iterator for DeletionListIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DeletionListIter::U16 { data, index } => {
                let value = data.get(*index).copied().map(u32::from);
                *index += 1;
                value
            }
            DeletionListIter::U32 { data, index } => {
                let value = data.get(*index).copied();
                *index += 1;
                value
            }
        }
    }
}

/// Thin wrapper over column block-index root and readonly pool.
///
/// It is cheap to instantiate per lookup/checkpoint operation.
pub struct ColumnBlockIndex<'a> {
    disk_pool: &'a ReadonlyBufferPool,
    root_page_id: PageID,
    end_row_id: RowID,
}

/// One bitmap update patch keyed by leaf `start_row_id`.
#[derive(Clone, Copy, Debug)]
pub struct OffloadedBitmapPatch<'a> {
    pub start_row_id: RowID,
    pub bitmap_bytes: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OffloadedBitmapRefPatch {
    start_row_id: RowID,
    blob_ref: BlobRef,
}

#[derive(Clone, Copy, Debug)]
struct NodeUpdateResult {
    new_page_id: PageID,
    touched: bool,
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

    /// Finds the column page payload containing `row_id`.
    ///
    /// Returns `Ok(None)` when `row_id` is not persisted in this column range.
    pub async fn find(&self, row_id: RowID) -> Result<Option<ColumnPagePayload>> {
        if self.root_page_id == 0 || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut page_id = self.root_page_id;
        loop {
            let g = self
                .disk_pool
                .try_get_page_shared::<ColumnBlockNode>(page_id)
                .await?;
            let node = g.page();
            if node.is_leaf() {
                let start_row_ids = node.leaf_start_row_ids();
                let idx = match search_start_row_id(start_row_ids, row_id) {
                    Some(idx) => idx,
                    None => return Ok(None),
                };
                return Ok(Some(node.leaf_payloads()[idx]));
            }
            let entries = node.branch_entries();
            let idx = match search_branch_entry(entries, row_id) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            page_id = entries[idx].page_id;
        }
    }

    /// Finds leaf entry `(start_row_id, payload)` containing `row_id`.
    ///
    /// Returns `Ok(None)` when `row_id` is not persisted in this column range.
    pub async fn find_entry(&self, row_id: RowID) -> Result<Option<(RowID, ColumnPagePayload)>> {
        if self.root_page_id == 0 || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut page_id = self.root_page_id;
        loop {
            let g = self
                .disk_pool
                .try_get_page_shared::<ColumnBlockNode>(page_id)
                .await?;
            let node = g.page();
            if node.is_leaf() {
                let start_row_ids = node.leaf_start_row_ids();
                let idx = match search_start_row_id(start_row_ids, row_id) {
                    Some(idx) => idx,
                    None => return Ok(None),
                };
                return Ok(Some((start_row_ids[idx], node.leaf_payloads()[idx])));
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
        payload: &ColumnPagePayload,
    ) -> Result<Option<Vec<u8>>> {
        let blob_ref = match payload.try_offloaded_ref()? {
            Some(blob_ref) => blob_ref,
            None => return Ok(None),
        };
        let reader = ColumnDeletionBlobReader::new(self.disk_pool);
        let bytes = reader.read(blob_ref).await?;
        Ok(Some(bytes))
    }

    /// Applies sorted start-row-id bitmap patches with copy-on-write updates.
    ///
    /// Constraints:
    /// - `patches` must be sorted and unique by `start_row_id`.
    /// - every `start_row_id` must already exist in the current index snapshot.
    ///
    /// Behavior:
    /// - this API is sticky offload: updated payloads always store offloaded refs.
    /// - it mutates the provided `MutableTableFile` as part of CoW writes.
    /// - callers should discard the mutable file after `Err` and fork a new one for retries.
    pub async fn batch_update_offloaded_bitmaps(
        &self,
        mutable_file: &mut MutableTableFile,
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

    /// Appends sorted `(start_row_id, block_id)` entries and returns new root page id.
    ///
    /// The update follows copy-on-write semantics on the right-most path.
    pub async fn batch_insert(
        &self,
        mutable_file: &mut MutableTableFile,
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

        let root_height = {
            let g = self
                .disk_pool
                .try_get_page_shared::<ColumnBlockNode>(self.root_page_id)
                .await?;
            g.page().header.height
        };
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

    fn update_subtree_with_patches<'b>(
        &'b self,
        mutable_file: &'b mut MutableTableFile,
        page_id: PageID,
        patches: &'b [OffloadedBitmapRefPatch],
        create_ts: u64,
    ) -> Pin<Box<dyn Future<Output = Result<NodeUpdateResult>> + 'b>> {
        Box::pin(async move {
            if patches.is_empty() {
                return Ok(NodeUpdateResult {
                    new_page_id: page_id,
                    touched: false,
                });
            }
            let g = self
                .disk_pool
                .try_get_page_shared::<ColumnBlockNode>(page_id)
                .await?;
            let node = g.page();
            if node.is_leaf() {
                return self
                    .update_leaf_with_patches(mutable_file, page_id, node, patches, create_ts)
                    .await;
            }
            self.update_branch_with_patches(mutable_file, page_id, node, patches, create_ts)
                .await
        })
    }

    async fn update_leaf_with_patches(
        &self,
        mutable_file: &mut MutableTableFile,
        page_id: PageID,
        node: &ColumnBlockNode,
        patches: &[OffloadedBitmapRefPatch],
        create_ts: u64,
    ) -> Result<NodeUpdateResult> {
        let row_ids = node.leaf_start_row_ids();
        let mut update_slots = Vec::with_capacity(patches.len());
        for patch in patches {
            match row_ids.binary_search(&patch.start_row_id) {
                Ok(slot_idx) => update_slots.push((slot_idx, patch.blob_ref)),
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
            for (slot_idx, blob_ref) in update_slots {
                new_payloads[slot_idx].clear_inline_and_set_offloaded(blob_ref);
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

    async fn update_branch_with_patches(
        &self,
        mutable_file: &mut MutableTableFile,
        page_id: PageID,
        node: &ColumnBlockNode,
        patches: &[OffloadedBitmapRefPatch],
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
        // `MutableTableFile` (page-id allocation + obsolete-page recording), so safe
        // parallel writes require an additional staging or allocator-partition design.
        for (child_idx, entry) in old_entries.iter().enumerate() {
            let next_start_row_id = old_entries
                .get(child_idx + 1)
                .map(|next| next.start_row_id)
                .unwrap_or(RowID::MAX);
            let range_start = patch_idx;
            while patch_idx < patches.len() && patches[patch_idx].start_row_id < next_start_row_id {
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
    pub fn allocate_node(
        &self,
        table_file: &mut MutableTableFile,
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
    pub fn record_obsolete_node(
        &self,
        table_file: &mut MutableTableFile,
        page_id: PageID,
    ) -> Result<()> {
        if page_id == 0 {
            return Err(Error::InvalidState);
        }
        table_file.record_gc_page(page_id);
        Ok(())
    }

    async fn write_node(
        &self,
        mutable_file: &MutableTableFile,
        page_id: PageID,
        node: &ColumnBlockNode,
    ) -> Result<()> {
        let mut buf = DirectBuf::zeroed(COLUMN_BLOCK_PAGE_SIZE);
        let dst = buf.data_mut();
        let header_bytes = cast_slice(std::slice::from_ref(&node.header));
        dst[..COLUMN_BLOCK_HEADER_SIZE].copy_from_slice(header_bytes);
        dst[COLUMN_BLOCK_HEADER_SIZE..].copy_from_slice(&node.data);
        mutable_file.write_page(page_id, buf).await
    }

    async fn build_tree_from_entries(
        &self,
        mutable_file: &mut MutableTableFile,
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

    async fn build_leaf_nodes(
        &self,
        mutable_file: &mut MutableTableFile,
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

    async fn build_branch_levels(
        &self,
        mutable_file: &mut MutableTableFile,
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

    async fn append_rightmost_path(
        &self,
        mutable_file: &mut MutableTableFile,
        entries: &[(RowID, u64)],
        create_ts: u64,
    ) -> Result<NodeAppendResult> {
        let mut path = Vec::new();
        let mut page_id = self.root_page_id;
        loop {
            let g = self
                .disk_pool
                .try_get_page_shared::<ColumnBlockNode>(page_id)
                .await?;
            let node = g.page();
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

    async fn append_to_leaf(
        &self,
        mutable_file: &mut MutableTableFile,
        page_id: PageID,
        entries: &[(RowID, u64)],
        create_ts: u64,
    ) -> Result<NodeAppendResult> {
        let capacity = COLUMN_BLOCK_MAX_ENTRIES;
        let (new_page_id, new_node, start_row_id, mut remaining) = {
            let g = self
                .disk_pool
                .try_get_page_shared::<ColumnBlockNode>(page_id)
                .await?;
            let node = g.page();
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
            (new_page_id, new_node, start_row_id, &entries[take..])
        };
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

    async fn append_to_branch_with_child(
        &self,
        mutable_file: &mut MutableTableFile,
        page_id: PageID,
        child_result: NodeAppendResult,
        create_ts: u64,
    ) -> Result<NodeAppendResult> {
        let (height, start_row_id, new_entries) = {
            let g = self
                .disk_pool
                .try_get_page_shared::<ColumnBlockNode>(page_id)
                .await?;
            let node = g.page();
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
            (node.header.height, node.header.start_row_id, new_entries)
        };

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

#[cfg(test)]
#[inline]
async fn read_node_from_file(
    table_file: &TableFile,
    page_id: PageID,
) -> Result<Box<ColumnBlockNode>> {
    let buf = table_file.read_page(page_id).await?;
    let src = buf.data();
    let header = cast_slice::<u8, ColumnBlockNodeHeader>(&src[..COLUMN_BLOCK_HEADER_SIZE])[0];
    let mut node = ColumnBlockNode::new_boxed(header.height, header.start_row_id, header.create_ts);
    node.header = header;
    node.data
        .copy_from_slice(&src[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_PAGE_SIZE]);
    table_file.buf_list().recycle(buf);
    Ok(node)
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
    patches
        .windows(2)
        .all(|pair| pair[0].start_row_id < pair[1].start_row_id)
        && patches.iter().all(|patch| !patch.bitmap_bytes.is_empty())
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
    use crate::buffer::{GlobalReadonlyBufferPool, ReadonlyBufferPool, ReadonlyCacheKey};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableID, TableMetadata,
    };
    use crate::file::table_fs::TableFileSystemConfig;
    use crate::value::ValKind;
    use std::sync::OnceLock;

    #[test]
    fn test_column_block_node_size() {
        assert_eq!(mem::size_of::<ColumnBlockNode>(), COLUMN_BLOCK_PAGE_SIZE);
        assert_eq!(mem::size_of::<ColumnPagePayload>(), 128);
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

    #[test]
    fn test_deletion_list_empty() {
        let mut payload = build_deletion_payload();
        let list = payload.deletion_list();
        assert_eq!(list.len(), 0);
        assert!(!list.contains(10));
        assert_eq!(list.iter().collect::<Vec<_>>(), Vec::<u32>::new());
    }

    #[test]
    fn test_deletion_list_add_u16_full() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        for delta in 0..DELETION_U16_CAPACITY as u32 {
            assert_eq!(list.add(delta).unwrap(), true);
        }
        assert_eq!(list.len(), DELETION_U16_CAPACITY);
        assert_eq!(
            list.add(DELETION_U16_CAPACITY as u32).unwrap_err(),
            DeletionListError::Full
        );
    }

    #[test]
    fn test_deletion_list_promotion_to_u32() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        assert_eq!(list.add(10).unwrap(), true);
        assert_eq!(list.add(5).unwrap(), true);
        assert_eq!(list.add(u16::MAX as u32 + 4).unwrap(), true);
        assert!(list.contains(u16::MAX as u32 + 4));
        assert_eq!(list.len(), 3);
        assert_eq!(
            list.iter().collect::<Vec<_>>(),
            vec![5, 10, u16::MAX as u32 + 4]
        );
    }

    #[test]
    fn test_deletion_list_promotion_full() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        for delta in 0..(DELETION_U32_CAPACITY as u32 + 1) {
            assert_eq!(list.add(delta).unwrap(), true);
        }
        assert_eq!(
            list.add(u16::MAX as u32 + 1).unwrap_err(),
            DeletionListError::Full
        );
    }

    #[test]
    fn test_deletion_list_sorted_and_duplicates() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        assert_eq!(list.add(10).unwrap(), true);
        assert_eq!(list.add(3).unwrap(), true);
        assert_eq!(list.add(8).unwrap(), true);
        assert_eq!(list.add(8).unwrap(), false);
        assert_eq!(list.iter().collect::<Vec<_>>(), vec![3, 8, 10]);
    }

    #[test]
    fn test_deletion_list_offloaded_flag() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        assert!(!list.is_offloaded());
        list.set_offloaded(true);
        assert!(list.is_offloaded());
        list.set_offloaded(false);
        assert!(!list.is_offloaded());
    }

    #[test]
    fn test_deletion_list_iter_row_ids() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        assert!(list.add(2).unwrap());
        assert!(list.add(5).unwrap());
        let row_ids = list.iter_row_ids(100).collect::<Vec<_>>();
        assert_eq!(row_ids, vec![102, 105]);
    }

    #[test]
    fn test_blob_ref_encode_decode_roundtrip() {
        let mut payload = build_deletion_payload();
        let blob_ref = BlobRef {
            start_page_id: 77,
            start_offset: 9,
            byte_len: 2048,
        };
        payload.clear_inline_and_set_offloaded(blob_ref);
        assert_eq!(payload.offloaded_ref(), Some(blob_ref));
        assert_eq!(payload.try_offloaded_ref().unwrap(), Some(blob_ref));
        let list = payload.deletion_list();
        assert!(list.is_offloaded());
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_blob_ref_invalid_decode() {
        let mut payload = build_deletion_payload();
        payload.clear_inline_and_set_offloaded(BlobRef {
            start_page_id: 1,
            start_offset: 0,
            byte_len: 12,
        });
        payload.deletion_field[DELETION_OFFLOAD_REF_OFFSET..DELETION_OFFLOAD_REF_OFFSET + 8]
            .copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(payload.offloaded_ref(), None);
        assert!(matches!(
            payload.try_offloaded_ref(),
            Err(Error::InvalidFormat)
        ));
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

    fn global_readonly_pool() -> &'static GlobalReadonlyBufferPool {
        static GLOBAL: OnceLock<&'static GlobalReadonlyBufferPool> = OnceLock::new();
        *GLOBAL.get_or_init(|| {
            GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap()
        })
    }

    fn readonly_pool(table_id: TableID, table_file: &Arc<TableFile>) -> ReadonlyBufferPool {
        ReadonlyBufferPool::new(table_id, Arc::clone(table_file), global_readonly_pool())
    }

    #[test]
    fn test_batch_insert_into_empty_tree_and_find() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(200, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(200, &table_file);

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

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("200.tbl");
            })
        });
    }

    #[test]
    fn test_index_accessors_and_find() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let _ = std::fs::remove_file("203.tbl");
                let table_file = fs.create_table_file(203, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(203, &table_file);

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

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("203.tbl");
            })
        });
    }

    #[test]
    fn test_batch_insert_appends_within_leaf() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(201, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(201, &table_file);

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

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("201.tbl");
            })
        });
    }

    #[test]
    fn test_build_branch_levels_multiple_layers() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let _ = std::fs::remove_file("204.tbl");
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

                let disk_pool = readonly_pool(204, &table_file);
                let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_page = index
                    .build_branch_levels(&mut mutable, entries, 1, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let root_node = read_node_from_file(&table_file, root_page).await.unwrap();
                assert_eq!(root_node.header.height, 2);
                assert_eq!(root_node.header.count, 2);
                let root_entries = root_node.branch_entries();
                assert_eq!(root_entries[0].start_row_id, 0);
                assert_eq!(
                    root_entries[1].start_row_id,
                    COLUMN_BLOCK_MAX_BRANCH_ENTRIES as RowID
                );

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("204.tbl");
            })
        });
    }

    #[test]
    fn test_batch_insert_creates_new_leaf_nodes() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(202, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(202, &table_file);

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

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("202.tbl");
            })
        });
    }

    #[test]
    fn test_append_to_branch_with_child_splits() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let _ = std::fs::remove_file("205.tbl");
                let table_file = fs.create_table_file(205, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);

                let disk_pool = readonly_pool(205, &table_file);
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

                let disk_pool = readonly_pool(205, &table_file);
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

                let new_root = read_node_from_file(&table_file, result.new_page_id)
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

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("205.tbl");
            })
        });
    }

    #[test]
    fn test_batch_update_offloaded_bitmaps_cow_snapshot() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let _ = std::fs::remove_file("207.tbl");
                let table_file = fs
                    .create_table_file(207, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(207, &table_file);

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

                let old_root_node = read_node_from_file(&table_file, root_v1).await.unwrap();
                let new_root_node = read_node_from_file(&table_file, root_v2).await.unwrap();
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
                let new_payload = index_v2.find(0).await.unwrap().unwrap();
                assert!(new_payload.offloaded_ref().is_some());
                assert_eq!(
                    index_v2
                        .read_offloaded_bitmap_bytes(&new_payload)
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

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("207.tbl");
            })
        });
    }

    #[test]
    fn test_batch_update_offloaded_bitmaps_multi_leaf() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let _ = std::fs::remove_file("208.tbl");
                let table_file = fs
                    .create_table_file(208, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(208, &table_file);

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
                let left_payload = index_v2.find(0).await.unwrap().unwrap();
                let right_payload = index_v2
                    .find(COLUMN_BLOCK_MAX_ENTRIES as RowID)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    index_v2
                        .read_offloaded_bitmap_bytes(&left_payload)
                        .await
                        .unwrap()
                        .unwrap(),
                    left_bytes
                );
                assert_eq!(
                    index_v2
                        .read_offloaded_bitmap_bytes(&right_payload)
                        .await
                        .unwrap()
                        .unwrap(),
                    right_bytes
                );

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("208.tbl");
            })
        });
    }

    #[test]
    fn test_batch_update_offloaded_bitmaps_sticky() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let _ = std::fs::remove_file("209.tbl");
                let table_file = fs
                    .create_table_file(209, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(209, &table_file);

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
                let payload_v2 = index_v2.find(0).await.unwrap().unwrap();
                assert!(payload_v2.offloaded_ref().is_some());
                assert_eq!(
                    index_v2
                        .read_offloaded_bitmap_bytes(&payload_v2)
                        .await
                        .unwrap()
                        .unwrap(),
                    first
                );

                let index_v3 = ColumnBlockIndex::new(root_v3, 4, &disk_pool);
                let payload_v3 = index_v3.find(0).await.unwrap().unwrap();
                assert!(payload_v3.offloaded_ref().is_some());
                assert_eq!(
                    index_v3
                        .read_offloaded_bitmap_bytes(&payload_v3)
                        .await
                        .unwrap()
                        .unwrap(),
                    second
                );

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("209.tbl");
            })
        });
    }

    #[test]
    fn test_read_offloaded_bitmap_invalid_ref() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let _ = std::fs::remove_file("210.tbl");
                let table_file = fs
                    .create_table_file(210, build_test_metadata(), false)
                    .unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(210, &table_file);

                let mut payload = build_deletion_payload();
                payload.set_offloaded_ref(BlobRef {
                    start_page_id: 0,
                    start_offset: 0,
                    byte_len: 1,
                });
                let index = ColumnBlockIndex::new(0, 0, &disk_pool);
                let res = index.read_offloaded_bitmap_bytes(&payload).await;
                assert!(matches!(res, Err(Error::InvalidFormat)));

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("210.tbl");
            })
        });
    }

    #[test]
    fn test_cache_reuse_for_unchanged_leaf_across_root_swap() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let _ = std::fs::remove_file("206.tbl");
                let table_file = fs.create_table_file(206, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);
                let disk_pool = readonly_pool(206, &table_file);
                let global = global_readonly_pool();

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

                let root_node = read_node_from_file(&table_file, root_v1).await.unwrap();
                assert!(root_node.is_branch());
                let left_leaf_page_id = root_node.branch_entries()[0].page_id;

                let index_v1 = ColumnBlockIndex::new(root_v1, initial_count as RowID, &disk_pool);
                assert_eq!(index_v1.find(0).await.unwrap().unwrap().block_id, 5000);

                let left_leaf_key = ReadonlyCacheKey::new(206, left_leaf_page_id);
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

                drop(table_file);
                drop(fs);
                let _ = std::fs::remove_file("206.tbl");
            })
        });
    }
}
