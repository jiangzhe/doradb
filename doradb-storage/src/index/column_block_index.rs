use crate::buffer::page::PageID;
use crate::error::{Error, Result};
use crate::file::table_file::{MutableTableFile, TABLE_FILE_PAGE_SIZE, TableFile};
use crate::row::RowID;
use std::mem;
use std::slice;
use std::sync::Arc;

pub const COLUMN_BLOCK_PAGE_SIZE: usize = TABLE_FILE_PAGE_SIZE;
pub const COLUMN_BLOCK_HEADER_SIZE: usize = mem::size_of::<ColumnBlockNodeHeader>();
pub const COLUMN_BLOCK_DATA_SIZE: usize = COLUMN_BLOCK_PAGE_SIZE - COLUMN_BLOCK_HEADER_SIZE;
pub const COLUMN_PAGE_PAYLOAD_SIZE: usize = mem::size_of::<ColumnPagePayload>();
pub const COLUMN_BLOCK_MAX_ENTRIES: usize =
    COLUMN_BLOCK_DATA_SIZE / (mem::size_of::<RowID>() + COLUMN_PAGE_PAYLOAD_SIZE);

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
}

pub struct ColumnBlockIndex {
    table_file: Arc<TableFile>,
    root_page_id: PageID,
}

impl ColumnBlockIndex {
    #[inline]
    pub fn new(table_file: Arc<TableFile>, root_page_id: PageID) -> Self {
        ColumnBlockIndex {
            table_file,
            root_page_id,
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

    /// Allocate a new node page for copy-on-write updates.
    #[inline]
    pub fn allocate_node(
        &self,
        table_file: &mut MutableTableFile,
        height: u32,
        start_row_id: RowID,
        create_ts: u64,
    ) -> Result<(PageID, ColumnBlockNode)> {
        let page_id = table_file.allocate_page_id()?;
        let node = ColumnBlockNode::new(height, start_row_id, create_ts);
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
