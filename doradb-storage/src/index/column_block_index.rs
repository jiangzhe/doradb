use crate::buffer::page::PageID;
use crate::error::{Error, Result};
use crate::file::table_file::{MutableTableFile, TABLE_FILE_PAGE_SIZE, TableFile};
use crate::io::DirectBuf;
use crate::row::RowID;
use bytemuck::{cast_slice, cast_slice_mut};
use std::mem;
use std::slice;
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

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ColumnBlockNodeHeader {
    pub height: u32,
    pub count: u32,
    pub start_row_id: RowID,
    pub create_ts: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnPagePayload {
    pub block_id: u64,
    pub deletion_field: [u8; 120],
}

impl ColumnPagePayload {
    #[inline]
    pub fn deletion_list(&mut self) -> DeletionList<'_> {
        DeletionList::new(&mut self.deletion_field)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnBlockBranchEntry {
    pub start_row_id: RowID,
    pub page_id: PageID,
}

#[repr(C)]
#[derive(Clone)]
pub struct ColumnBlockNode {
    pub header: ColumnBlockNodeHeader,
    data: [u8; COLUMN_BLOCK_DATA_SIZE],
}

impl ColumnBlockNode {
    #[inline]
    pub fn new(height: u32, start_row_id: RowID, create_ts: u64) -> Self {
        ColumnBlockNode {
            header: ColumnBlockNodeHeader {
                height,
                count: 0,
                start_row_id,
                create_ts,
            },
            data: [0u8; COLUMN_BLOCK_DATA_SIZE],
        }
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.header.height == 0
    }

    #[inline]
    pub fn is_branch(&self) -> bool {
        !self.is_leaf()
    }

    #[inline]
    fn data_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    #[inline]
    fn data_ptr_mut(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    #[inline]
    pub fn leaf_start_row_ids(&self) -> &[RowID] {
        debug_assert!(self.is_leaf());
        unsafe {
            slice::from_raw_parts(self.data_ptr() as *const RowID, self.header.count as usize)
        }
    }

    #[inline]
    pub fn leaf_start_row_ids_mut(&mut self) -> &mut [RowID] {
        debug_assert!(self.is_leaf());
        unsafe {
            slice::from_raw_parts_mut(
                self.data_ptr_mut() as *mut RowID,
                self.header.count as usize,
            )
        }
    }

    #[inline]
    pub fn leaf_payloads(&self) -> &[ColumnPagePayload] {
        debug_assert!(self.is_leaf());
        let count = self.header.count as usize;
        let payload_ptr = unsafe {
            self.data_ptr().add(count * mem::size_of::<RowID>()) as *const ColumnPagePayload
        };
        unsafe { slice::from_raw_parts(payload_ptr, count) }
    }

    #[inline]
    pub fn leaf_payloads_mut(&mut self) -> &mut [ColumnPagePayload] {
        debug_assert!(self.is_leaf());
        let count = self.header.count as usize;
        let payload_ptr = unsafe {
            self.data_ptr_mut().add(count * mem::size_of::<RowID>()) as *mut ColumnPagePayload
        };
        unsafe { slice::from_raw_parts_mut(payload_ptr, count) }
    }

    #[inline]
    pub fn leaf_arrays_mut(&mut self) -> (&mut [RowID], &mut [ColumnPagePayload]) {
        debug_assert!(self.is_leaf());
        let count = self.header.count as usize;
        let row_ptr = self.data_ptr_mut() as *mut RowID;
        let payload_ptr = unsafe {
            self.data_ptr_mut().add(count * mem::size_of::<RowID>()) as *mut ColumnPagePayload
        };
        unsafe {
            (
                slice::from_raw_parts_mut(row_ptr, count),
                slice::from_raw_parts_mut(payload_ptr, count),
            )
        }
    }

    #[inline]
    pub fn branch_entries(&self) -> &[ColumnBlockBranchEntry] {
        debug_assert!(self.is_branch());
        unsafe {
            slice::from_raw_parts(
                self.data_ptr() as *const ColumnBlockBranchEntry,
                self.header.count as usize,
            )
        }
    }

    #[inline]
    pub fn branch_entries_mut(&mut self) -> &mut [ColumnBlockBranchEntry] {
        debug_assert!(self.is_branch());
        unsafe {
            slice::from_raw_parts_mut(
                self.data_ptr_mut() as *mut ColumnBlockBranchEntry,
                self.header.count as usize,
            )
        }
    }

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
}

const DELETION_FIELD_SIZE: usize = 120;
const DELETION_HEADER_SIZE: usize = 2;
const DELETION_U16_OFFSET: usize = DELETION_HEADER_SIZE;
const DELETION_U32_OFFSET: usize = 4;
const DELETION_U16_CAPACITY: usize = (DELETION_FIELD_SIZE - DELETION_U16_OFFSET) / 2;
const DELETION_U32_CAPACITY: usize = (DELETION_FIELD_SIZE - DELETION_U32_OFFSET) / 4;
const DELETION_FLAG_U32: u8 = 0b1000_0000;
const DELETION_FLAG_OFFLOADED: u8 = 0b0100_0000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeletionListError {
    Full,
}

pub struct DeletionList<'a> {
    data: &'a mut [u8; DELETION_FIELD_SIZE],
}

impl<'a> DeletionList<'a> {
    #[inline]
    pub fn new(data: &'a mut [u8; DELETION_FIELD_SIZE]) -> Self {
        DeletionList { data }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data[1] as usize
    }

    #[inline]
    pub fn is_offloaded(&self) -> bool {
        (self.data[0] & DELETION_FLAG_OFFLOADED) != 0
    }

    #[inline]
    pub fn set_offloaded(&mut self, offloaded: bool) {
        if offloaded {
            self.data[0] |= DELETION_FLAG_OFFLOADED;
        } else {
            self.data[0] &= !DELETION_FLAG_OFFLOADED;
        }
    }

    #[inline]
    pub fn contains(&self, delta: u32) -> bool {
        if self.is_format_u32() {
            self.deltas_u32()
                .binary_search(&delta)
                .is_ok()
        } else if delta > u16::MAX as u32 {
            false
        } else {
            let delta = delta as u16;
            self.deltas_u16().binary_search(&delta).is_ok()
        }
    }

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

    #[inline]
    pub fn iter_row_ids(&self, start_row_id: RowID) -> impl Iterator<Item = RowID> + '_ {
        self.iter()
            .map(move |delta| start_row_id + delta as RowID)
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

pub struct ColumnBlockIndex {
    table_file: Arc<TableFile>,
    root_page_id: PageID,
    end_row_id: RowID,
}

impl ColumnBlockIndex {
    #[inline]
    pub fn new(table_file: Arc<TableFile>, root_page_id: PageID, end_row_id: RowID) -> Self {
        ColumnBlockIndex {
            table_file,
            root_page_id,
            end_row_id,
        }
    }

    #[inline]
    pub fn table_file(&self) -> &Arc<TableFile> {
        &self.table_file
    }

    #[inline]
    pub fn root_page_id(&self) -> PageID {
        self.root_page_id
    }

    #[inline]
    pub fn end_row_id(&self) -> RowID {
        self.end_row_id
    }

    pub async fn find(&self, row_id: RowID) -> Result<Option<ColumnPagePayload>> {
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
                .map_or(true, |entry| entry.0 >= self.end_row_id)
        );
        debug_assert!(
            entries
                .last()
                .map_or(true, |entry| entry.0 < new_end_row_id)
        );
        debug_assert!(new_end_row_id >= self.end_row_id);
        if entries.is_empty() {
            return Ok(self.root_page_id);
        }

        if self.root_page_id == 0 {
            return self
                .build_tree_from_entries(mutable_file, entries, create_ts)
                .await;
        }

        let root_node = self.read_node(self.root_page_id).await?;
        let root_height = root_node.header.height;
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
        let node = Box::new(ColumnBlockNode::new(height, start_row_id, create_ts));
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

    async fn read_node(&self, page_id: PageID) -> Result<Box<ColumnBlockNode>> {
        read_node_from_file(&self.table_file, page_id).await
    }

    async fn write_node(&self, page_id: PageID, node: &ColumnBlockNode) -> Result<()> {
        let mut buf = DirectBuf::zeroed(COLUMN_BLOCK_PAGE_SIZE);
        unsafe {
            std::ptr::copy_nonoverlapping(
                node as *const ColumnBlockNode as *const u8,
                buf.data_mut().as_mut_ptr(),
                COLUMN_BLOCK_PAGE_SIZE,
            );
        }
        self.table_file.write_page(page_id, buf).await
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
            self.write_node(page_id, &node).await?;
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
                self.write_node(page_id, &node).await?;
                return Ok(page_id);
            }
            let mut next_entries = Vec::new();
            for chunk in entries.chunks(COLUMN_BLOCK_MAX_BRANCH_ENTRIES) {
                let (page_id, mut node) =
                    self.allocate_node(mutable_file, height, chunk[0].start_row_id, create_ts)?;
                node.header.count = chunk.len() as u32;
                node.branch_entries_mut().copy_from_slice(chunk);
                self.write_node(page_id, &node).await?;
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
            let node = self.read_node(page_id).await?;
            let next_page_id = if node.is_leaf() {
                None
            } else {
                node.branch_entries().last().map(|entry| entry.page_id)
            };
            path.push((page_id, node));
            if let Some(next_page_id) = next_page_id {
                page_id = next_page_id;
            } else {
                break;
            }
        }

        let (leaf_page_id, leaf_node) = path.pop().ok_or(Error::InvalidState)?;
        let mut child_result = self
            .append_to_leaf(mutable_file, leaf_page_id, leaf_node, entries, create_ts)
            .await?;

        for (page_id, node) in path.into_iter().rev() {
            child_result = self
                .append_to_branch_with_child(mutable_file, page_id, node, child_result, create_ts)
                .await?;
        }

        Ok(child_result)
    }

    async fn append_to_leaf(
        &self,
        mutable_file: &mut MutableTableFile,
        page_id: PageID,
        node: Box<ColumnBlockNode>,
        entries: &[(RowID, u64)],
        create_ts: u64,
    ) -> Result<NodeAppendResult> {
        let old_count = node.header.count as usize;
        let capacity = COLUMN_BLOCK_MAX_ENTRIES;
        let mut remaining = entries;

        let take = remaining.len().min(capacity.saturating_sub(old_count));
        let new_count = old_count + take;
        let start_row_id = if old_count == 0 {
            remaining
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
            for (idx, entry) in remaining.iter().take(take).enumerate() {
                row_ids[old_count + idx] = entry.0;
                payloads[old_count + idx] = ColumnPagePayload {
                    block_id: entry.1,
                    deletion_field: [0u8; 120],
                };
            }
        }
        self.write_node(new_page_id, &new_node).await?;
        self.record_obsolete_node(mutable_file, page_id)?;

        remaining = &remaining[take..];
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
            self.write_node(page_id, &leaf_node).await?;
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
        node: Box<ColumnBlockNode>,
        child_result: NodeAppendResult,
        create_ts: u64,
    ) -> Result<NodeAppendResult> {
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

        let mut remaining = new_entries.as_slice();
        let first_len = remaining.len().min(COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
        let (new_page_id, mut new_node) = self.allocate_node(
            mutable_file,
            node.header.height,
            node.header.start_row_id,
            create_ts,
        )?;
        new_node.header.count = first_len as u32;
        new_node
            .branch_entries_mut()
            .copy_from_slice(&remaining[..first_len]);
        self.write_node(new_page_id, &new_node).await?;
        self.record_obsolete_node(mutable_file, page_id)?;

        remaining = &remaining[first_len..];
        let mut extra_entries = Vec::new();
        while !remaining.is_empty() {
            let chunk_len = remaining.len().min(COLUMN_BLOCK_MAX_BRANCH_ENTRIES);
            let chunk = &remaining[..chunk_len];
            let (page_id, mut branch_node) = self.allocate_node(
                mutable_file,
                node.header.height,
                chunk[0].start_row_id,
                create_ts,
            )?;
            branch_node.header.count = chunk_len as u32;
            branch_node.branch_entries_mut().copy_from_slice(chunk);
            self.write_node(page_id, &branch_node).await?;
            extra_entries.push(ColumnBlockBranchEntry {
                start_row_id: chunk[0].start_row_id,
                page_id,
            });
            remaining = &remaining[chunk_len..];
        }

        Ok(NodeAppendResult {
            new_page_id,
            start_row_id: node.header.start_row_id,
            extra_entries,
        })
    }
}

#[inline]
async fn read_node_from_file(
    table_file: &TableFile,
    page_id: PageID,
) -> Result<Box<ColumnBlockNode>> {
    let buf = table_file.read_page(page_id).await?;
    let mut node = Box::<ColumnBlockNode>::new_uninit();
    unsafe {
        std::ptr::copy_nonoverlapping(
            buf.data().as_ptr(),
            node.as_mut_ptr() as *mut u8,
            COLUMN_BLOCK_PAGE_SIZE,
        );
    }
    table_file.buf_list().recycle(buf);
    Ok(unsafe { node.assume_init() })
}

/// Find the payload for a row id using the column block index stored in file.
#[inline]
pub async fn find_in_file(
    table_file: &TableFile,
    root_page_id: PageID,
    row_id: RowID,
) -> Result<Option<ColumnPagePayload>> {
    if root_page_id == 0 {
        return Ok(None);
    }
    let mut page_id = root_page_id;
    loop {
        let node = read_node_from_file(table_file, page_id).await?;
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

struct NodeAppendResult {
    new_page_id: PageID,
    start_row_id: RowID,
    extra_entries: Vec<ColumnBlockBranchEntry>,
}

fn entries_sorted(entries: &[(RowID, u64)]) -> bool {
    entries.windows(2).all(|pair| pair[0].0 <= pair[1].0)
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
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
    };
    use crate::file::table_fs::TableFileSystemConfig;
    use crate::value::ValKind;

    #[test]
    fn test_column_block_node_size() {
        assert_eq!(mem::size_of::<ColumnBlockNode>(), COLUMN_BLOCK_PAGE_SIZE);
        assert_eq!(mem::size_of::<ColumnPagePayload>(), 128);
    }

    #[test]
    fn test_leaf_layout_offsets() {
        let mut node = ColumnBlockNode::new(0, 0, 0);
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
        assert_eq!(list.iter().collect::<Vec<_>>(), vec![5, 10, u16::MAX as u32 + 4]);
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
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(200, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);

                let index = ColumnBlockIndex::new(Arc::clone(&table_file), 0, 0);
                let entries = vec![(10, 100), (20, 200), (30, 300)];
                let mut mutable = MutableTableFile::fork(&table_file);
                let new_root = index
                    .batch_insert(&mut mutable, &entries, 40, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let index = ColumnBlockIndex::new(Arc::clone(&table_file), new_root, 40);
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
    fn test_batch_insert_appends_within_leaf() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(201, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);

                let entries = build_entries(0, 8, 1000);
                let index = ColumnBlockIndex::new(Arc::clone(&table_file), 0, 0);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_page = index
                    .batch_insert(&mut mutable, &entries, 8, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let more_entries = build_entries(8, 4, 2000);
                let index = ColumnBlockIndex::new(Arc::clone(&table_file), root_page, 8);
                let mut mutable = MutableTableFile::fork(&table_file);
                let new_root = index
                    .batch_insert(&mut mutable, &more_entries, 12, 3)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let index = ColumnBlockIndex::new(Arc::clone(&table_file), new_root, 12);
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
    fn test_batch_insert_creates_new_leaf_nodes() {
        run_with_large_stack(|| {
            smol::block_on(async {
                let fs = TableFileSystemConfig::default().build().unwrap();
                let metadata = build_test_metadata();
                let table_file = fs.create_table_file(202, metadata, false).unwrap();
                let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
                drop(old_root);

                let initial_count = COLUMN_BLOCK_MAX_ENTRIES - 1;
                let entries = build_entries(0, initial_count, 1);
                let index = ColumnBlockIndex::new(Arc::clone(&table_file), 0, 0);
                let mut mutable = MutableTableFile::fork(&table_file);
                let root_page = index
                    .batch_insert(&mut mutable, &entries, initial_count as RowID, 2)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(2, false).await.unwrap();
                drop(old_root);

                let append_entries = build_entries(initial_count as RowID, 2, 9000);
                let index = ColumnBlockIndex::new(
                    Arc::clone(&table_file),
                    root_page,
                    initial_count as RowID,
                );
                let mut mutable = MutableTableFile::fork(&table_file);
                let new_root = index
                    .batch_insert(&mut mutable, &append_entries, initial_count as RowID + 2, 3)
                    .await
                    .unwrap();
                let (table_file, old_root) = mutable.commit(3, false).await.unwrap();
                drop(old_root);

                let index = ColumnBlockIndex::new(
                    Arc::clone(&table_file),
                    new_root,
                    initial_count as RowID + 2,
                );
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
}
