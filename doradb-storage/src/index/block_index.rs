use crate::buffer::frame::{BufferFrame, BufferFrameAware};
use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::page::{PageID, LSN, PAGE_SIZE};
use crate::buffer::FixedBufferPool;
use crate::error::{
    Error, Result, Validation,
    Validation::{Invalid, Valid},
};
use crate::latch::LatchFallbackMode;
use crate::row::{RowID, RowPage};
use crate::table::layout::Layout;
use parking_lot::Mutex;
use std::mem;

pub const BLOCK_PAGE_SIZE: usize = PAGE_SIZE;
pub const BLOCK_HEADER_SIZE: usize = mem::size_of::<BlockNodeHeader>();
pub const BLOCK_SIZE: usize = 1272;
pub const NBR_BLOCKS_IN_LEAF: usize = 51;
pub const NBR_ENTRIES_IN_BRANCH: usize = 4093;
pub const ENTRY_SIZE: usize = mem::size_of::<PageEntry>();
pub const NBR_PAGES_IN_ROW_BLOCK: usize = 78;
pub const NBR_SEGMENTS_IN_COL_BLOCK: usize = 16;
// pub type Block = [u8; BLOCK_SIZE];
// header 32 bytes, padding 16 bytes.
pub const BLOCK_BRANCH_ENTRY_START: usize = 48;
// header 32 bytes, padding 640 bytes.
pub const BLOCK_LEAF_ENTRY_START: usize = 672;

const _: () = assert!(
    { mem::size_of::<BlockNode>() == BLOCK_PAGE_SIZE },
    "Size of node of BlockIndex should equal to 64KB"
);

const _: () = assert!(
    { BLOCK_HEADER_SIZE + NBR_ENTRIES_IN_BRANCH * ENTRY_SIZE <= BLOCK_PAGE_SIZE },
    "Size of branch node of BlockIndex can be at most 64KB"
);

const _: () = assert!(
    { BLOCK_HEADER_SIZE + NBR_BLOCKS_IN_LEAF * BLOCK_SIZE <= BLOCK_PAGE_SIZE },
    "Size of leaf node of BlockIndex can be at most 64KB"
);

/// BlockKind can be Row or Col.
/// Row Block contains 78 row page ids.
/// Col block represent a columnar file and
/// stores segment information inside the block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum BlockKind {
    Row = 1,
    Col = 2,
}

/// BlockNode is B-Tree node of block index.
/// It can be either branch or leaf.
/// Branch contains at most 4093 child node pointers.
/// Leaf contains at most 51 block headers.
#[repr(C)]
#[derive(Clone)]
pub struct BlockNode {
    pub header: BlockNodeHeader,
    data: [u8; BLOCK_PAGE_SIZE - BLOCK_HEADER_SIZE],
}

impl BlockNode {
    /// Returns whether the block node is leaf.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.header.height == 0
    }

    /// Returns whether the block node is branch.
    #[inline]
    pub fn is_branch(&self) -> bool {
        !self.is_leaf()
    }

    #[inline]
    fn data_ptr<T>(&self) -> *const T {
        self.data.as_ptr() as *const _
    }

    #[inline]
    fn data_ptr_mut<T>(&mut self) -> *mut T {
        self.data.as_mut_ptr() as *mut _
    }

    /* branch methods */

    /// Returns whether the branch node is full.
    #[inline]
    pub fn branch_is_full(&self) -> bool {
        debug_assert!(self.is_branch());
        self.header.count as usize == NBR_ENTRIES_IN_BRANCH
    }

    /// Returns whether the branch node is empty.
    #[inline]
    pub fn branch_is_empty(&self) -> bool {
        debug_assert!(self.is_branch());
        self.header.count == 0
    }

    /// Returns the entry slice in branch node.
    #[inline]
    pub fn branch_entries(&self) -> &[PageEntry] {
        debug_assert!(self.is_branch());
        unsafe { std::slice::from_raw_parts(self.data_ptr(), self.header.count as usize) }
    }

    /// Returns mutable entry slice in branch node.
    #[inline]
    pub fn branch_entries_mut(&mut self) -> &mut [PageEntry] {
        debug_assert!(self.is_branch());
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr_mut(), self.header.count as usize) }
    }

    /// Returns entry in branch node by given index.
    #[inline]
    pub fn branch_entry(&self, idx: usize) -> &PageEntry {
        &self.branch_entries()[idx]
    }

    /// Returns last entry in branch node.
    #[inline]
    pub fn branch_last_entry(&self) -> &PageEntry {
        debug_assert!(self.is_branch());
        &self.branch_entries()[self.header.count as usize - 1]
    }

    /// Add a new entry in branch node.
    #[inline]
    pub fn branch_add_entry(&mut self, entry: PageEntry) {
        debug_assert!(self.is_branch());
        assert!((self.header.count as usize) < NBR_ENTRIES_IN_BRANCH);
        let idx = self.header.count;
        self.header.count += 1;
        self.branch_entries_mut()[idx as usize] = entry;
    }

    /* leaf methods */

    /// Returns whether the node is leaf.
    #[inline]
    pub fn leaf_is_full(&self) -> bool {
        debug_assert!(self.is_leaf());
        self.header.count as usize == NBR_BLOCKS_IN_LEAF
    }

    /// Returns whether the leaf node is empty.
    #[inline]
    pub fn leaf_is_empty(&self) -> bool {
        debug_assert!(self.is_leaf());
        self.header.count == 0
    }

    /// Returns block slice of leaf node.
    #[inline]
    pub fn leaf_blocks(&self) -> &[Block] {
        debug_assert!(self.is_leaf());
        unsafe { std::slice::from_raw_parts(self.data_ptr(), self.header.count as usize) }
    }

    /// Returns block in leaf node by given index.
    #[inline]
    pub fn leaf_block(&self, idx: usize) -> &Block {
        debug_assert!(self.is_leaf());
        &self.leaf_blocks()[idx]
    }

    /// Returns last block in leaf node.
    #[inline]
    pub fn leaf_last_block(&self) -> &Block {
        debug_assert!(self.is_leaf());
        &self.leaf_blocks()[self.header.count as usize - 1]
    }

    /// Returns mutable last block in leaf node.
    #[inline]
    pub fn leaf_last_block_mut(&mut self) -> &mut Block {
        debug_assert!(self.is_leaf());
        let count = self.header.count as usize;
        &mut self.leaf_blocks_mut()[count - 1]
    }

    /// Returns mutable block slice in leaf node.
    #[inline]
    pub fn leaf_blocks_mut(&mut self) -> &mut [Block] {
        debug_assert!(self.is_leaf());
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr_mut(), self.header.count as usize) }
    }

    /// Add a new block in leaf node.
    #[inline]
    pub fn leaf_add_block(&mut self, count: u64, page_id: PageID) {
        debug_assert!(self.is_leaf());
        debug_assert!(!self.leaf_is_full());
        let end_row_id = self.header.end_row_id;
        self.header.count += 1;
        self.leaf_last_block_mut()
            .init_row(end_row_id, count, page_id);
        self.header.end_row_id += count;
    }
}

impl BufferFrameAware for BlockNode {}

#[repr(C)]
#[derive(Clone)]
pub struct BlockNodeHeader {
    // height of the node
    pub height: u32,
    // count of entry.
    pub count: u32,
    // log sequence number
    pub lsn: LSN,
    // start row id of the node.
    pub start_row_id: RowID,
    // end row id of the node.
    // note: this value may not be valid if the node is branch.
    pub end_row_id: RowID,
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct PageEntry {
    pub row_id: RowID,
    pub page_id: PageID,
}

impl PageEntry {
    #[inline]
    pub fn new(row_id: RowID, page_id: PageID) -> Self {
        PageEntry { row_id, page_id }
    }
}

#[repr(C)]
pub struct BlockHeader {
    pub kind: BlockKind,
    pub count: u32,
    pub start_row_id: RowID,
    pub end_row_id: RowID,
}

/// Block is an abstraction on data distribution.
/// Block has two kinds: row and column.
/// Row block contains at most 78 row page ids with
/// its associated min row id.
/// Column block represents one on-disk columnar file
/// with its row id range and statistics.
#[repr(C)]
pub struct Block {
    pub header: BlockHeader,
    padding: [u8; BLOCK_SIZE - mem::size_of::<BlockHeader>()],
}

impl Block {
    /// Initialize block with single row page info.
    #[inline]
    pub fn init_row(&mut self, row_id: RowID, count: u64, page_id: PageID) {
        self.header.kind = BlockKind::Row;
        self.header.count = 0;
        self.header.start_row_id = row_id;
        self.header.end_row_id = row_id;
        self.row_add_page(count, page_id);
    }

    /// Returns whether the block is row block.
    #[inline]
    pub fn is_row(&self) -> bool {
        self.header.kind == BlockKind::Row
    }

    /// Returns whether the block is column block.
    #[inline]
    pub fn is_col(&self) -> bool {
        !self.is_row()
    }

    #[inline]
    fn data_ptr(&self) -> *const u8 {
        self.padding.as_ptr()
    }

    #[inline]
    fn data_ptr_mut(&mut self) -> *mut u8 {
        self.padding.as_mut_ptr()
    }

    /* row block methods */

    /// Returns page entry slice in row block.
    #[inline]
    pub fn row_page_entries(&self) -> &[PageEntry] {
        let ptr = self.data_ptr() as *const PageEntry;
        unsafe { std::slice::from_raw_parts(ptr, self.header.count as usize) }
    }

    /// Returns mutable page entry slice in row block.
    #[inline]
    pub fn row_page_entries_mut(&mut self) -> &mut [PageEntry] {
        let ptr = self.data_ptr_mut() as *mut PageEntry;
        unsafe { std::slice::from_raw_parts_mut(ptr, self.header.count as usize) }
    }

    /// Returns last entry in row block.
    #[inline]
    pub fn row_last_entry(&self) -> &PageEntry {
        &self.row_page_entries()[self.header.count as usize - 1]
    }

    /// Add a new page entry in row block.
    #[inline]
    pub fn row_add_page(&mut self, count: u64, page_id: PageID) -> u64 {
        assert!((self.header.count as usize) < NBR_PAGES_IN_ROW_BLOCK);
        let entry = PageEntry {
            row_id: self.header.end_row_id,
            page_id,
        };
        let idx = self.header.count as usize;
        self.header.count += 1;
        self.row_page_entries_mut()[idx] = entry;
        self.header.end_row_id += count;
        self.header.end_row_id
    }

    /// Returns whether the row block is full.
    #[inline]
    pub fn row_is_full(&self) -> bool {
        self.header.count as usize == NBR_PAGES_IN_ROW_BLOCK
    }

    /* col block methods */
}

#[repr(C)]
pub struct ColSegmentMeta {
    pub row_id: RowID,
    pub count: u64,
}

/// The base index of blocks.
///
/// It controls block/page level storage information
/// of column-store and row-store.
///
/// The index is sorted by RowID, which is global unique identifier
/// of each row.
/// When inserting a new row, a row page must be located, either by
/// allocating a new page from buffer pool, or by reusing a non-full
/// page.
/// The row page determines its RowID range by increasing max row id
/// of the block index with estimated row count.
///
/// Old rows have smaller RowID, new rows have bigger RowID.
/// Once all data in one row page can be seen by all active transactions,
/// it is qualified to be persisted to disk in column format.
///
/// Multiple row pages are merged to be a column file.
/// Block index will also be updated to reflect the change.
///
/// The block index supports two operations.
///
/// 1. index search with row id: determine which column file or row page
///    one RowID belongs to.
/// 2. table scan: traverse all column files and row pages to perform
///    full table scan.
///
pub struct BlockIndex<'a> {
    buf_pool: &'a FixedBufferPool,
    root: PageID,
    insert_free_list: Mutex<Vec<PageID>>,
}

impl<'a> BlockIndex<'a> {
    /// Create a new block index backed by buffer pool.
    #[inline]
    pub fn new(buf_pool: &'a FixedBufferPool) -> Result<Self> {
        let mut g: PageExclusiveGuard<'a, BlockNode> = buf_pool.allocate_page()?;
        let page_id = g.page_id();
        let page = g.page_mut();
        page.header.height = 0;
        page.header.count = 0;
        page.header.lsn = 0;
        page.header.start_row_id = 0;
        page.header.end_row_id = 0;
        Ok(BlockIndex {
            buf_pool,
            root: page_id,
            insert_free_list: Mutex::new(Vec::with_capacity(64)),
        })
    }

    /// Get row page for insertion.
    /// Caller should cache insert page id to avoid invoking this method frequently.
    #[inline]
    pub fn get_insert_page(
        &self,
        count: usize,
        cols: &[Layout],
    ) -> Result<PageExclusiveGuard<'a, RowPage>> {
        match self.get_insert_page_from_free_list() {
            Valid(Ok(free_page)) => return Ok(free_page),
            Valid(_) | Invalid => {
                // we just ignore the free list error and latch error, and continue to get new page.
            }
        }
        let mut new_page: PageExclusiveGuard<'a, RowPage> = self.buf_pool.allocate_page()?;
        let new_page_id = new_page.page_id();
        loop {
            match self.insert_row_page(count as u64, new_page_id) {
                Invalid => (),
                Valid(Ok((start_row_id, end_row_id))) => {
                    // initialize row page.
                    debug_assert!(end_row_id == start_row_id + count as u64);
                    let p = new_page.page_mut();
                    p.init(start_row_id, count as usize, cols);
                    return Ok(new_page);
                }
                Valid(Err(e)) => {
                    // any error occured, we deallocate the page for future reuse.
                    self.free_exclusive_insert_page(new_page);
                    return Err(e);
                }
            }
        }
    }

    /// Find location of given row id, maybe in column file or row page.
    #[inline]
    pub fn find_row_id(&self, row_id: RowID) -> Result<RowLocation> {
        loop {
            let res = self.try_find_row_id(row_id);
            let res = verify_continue!(res);
            return res;
        }
    }

    /// Put given page into insert free list.
    #[inline]
    pub fn free_exclusive_insert_page(&self, guard: PageExclusiveGuard<'a, RowPage>) {
        let page_id = guard.page_id();
        drop(guard);
        let mut free_list = self.insert_free_list.lock();
        free_list.push(page_id);
    }

    /// Returns the cursor starts from given row id.
    #[inline]
    pub fn cursor_shared(&self, row_id: RowID) -> Result<CursorShared<'_>> {
        let mut stack = vec![];
        loop {
            let res = self.try_find_leaf_by_row_id(row_id, &mut stack);
            let res = verify_continue!(res);
            return res.map(|_| CursorShared {
                blk_idx: self,
                stack,
            });
        }
    }

    #[inline]
    fn get_insert_page_from_free_list(
        &self,
    ) -> Validation<Result<PageExclusiveGuard<'a, RowPage>>> {
        let page_id = {
            let mut g = self.insert_free_list.lock();
            if g.is_empty() {
                return Valid(Err(Error::EmptyFreeListOfBufferPool));
            }
            g.pop().unwrap()
        };
        let page: PageGuard<'a, RowPage> = {
            let res = self
                .buf_pool
                .get_page(page_id, LatchFallbackMode::Exclusive);
            bypass_res!(res)
        };
        page.try_exclusive().map(Ok)
    }

    #[inline]
    fn insert_row_page_split_root(
        &self,
        mut p_guard: PageExclusiveGuard<'_, BlockNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> Result<(u64, u64)> {
        debug_assert!(p_guard.page_id() == self.root);
        debug_assert!({
            let p = p_guard.page();
            (p.is_leaf() && p.leaf_is_full()) || (p.is_branch() && p.branch_is_full())
        });
        debug_assert!({
            let p = p_guard.page();
            p.is_leaf()
                && (p.leaf_last_block().is_col() || p_guard.page().leaf_last_block().row_is_full())
        });
        let new_height = p_guard.page().header.height + 1;
        let l_row_id = p_guard.page().header.start_row_id;
        let r_row_id = row_id;
        let max_row_id = r_row_id + count;

        // create left child and copy all contents to it.
        let mut l_guard: PageExclusiveGuard<'_, BlockNode> = self.buf_pool.allocate_page()?;
        let l_page_id = l_guard.page_id();
        l_guard.page_mut().clone_from(p_guard.page()); // todo: LSN

        // create right child, add one row block with one page entry.
        let mut r_guard: PageExclusiveGuard<'_, BlockNode> = self.buf_pool.allocate_page()?;
        let r_page_id = r_guard.page_id();
        {
            let r = r_guard.page_mut();
            r.header.height = 0; // leaf
            r.header.start_row_id = r_row_id;
            r.header.end_row_id = r_row_id;
            r.header.count = 0;
            // todo: LSN
            r.leaf_add_block(count, insert_page_id);
            debug_assert!(r.header.end_row_id == max_row_id);
        }

        // initialize parent again.
        {
            let p = p_guard.page_mut();
            p.header.height = new_height; // branch
            p.header.start_row_id = l_row_id;
            p.header.end_row_id = max_row_id;
            // todo: LSN
            p.header.count = 0;
            p.branch_add_entry(PageEntry {
                row_id: l_row_id,
                page_id: l_page_id,
            });
            p.branch_add_entry(PageEntry {
                row_id: r_row_id,
                page_id: r_page_id,
            });
        }
        Ok((r_row_id, max_row_id))
    }

    #[inline]
    fn insert_row_page_to_new_leaf(
        &self,
        stack: &mut Vec<PageGuard<'_, BlockNode>>,
        c_guard: PageExclusiveGuard<'_, BlockNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> Validation<Result<(u64, u64)>> {
        debug_assert!(!stack.is_empty());
        let mut p_guard;
        loop {
            // try to lock parent.
            let g = stack.pop().unwrap();
            // if lock failed, just retry the whole process.
            p_guard = verify!(g.try_exclusive());
            if !p_guard.page().branch_is_full() {
                break;
            } else if stack.is_empty() {
                // root is full, should split
                return Valid(self.insert_row_page_split_root(
                    p_guard,
                    row_id,
                    count,
                    insert_page_id,
                ));
            }
        }
        // create new leaf node with one insert page id
        let mut leaf: PageExclusiveGuard<'_, BlockNode> = {
            let res = self.buf_pool.allocate_page();
            bypass_res!(res)
        };
        let leaf_page_id = leaf.page_id();
        {
            let b: &mut BlockNode = leaf.page_mut();
            b.header.height = 0;
            b.header.count = 0;
            b.header.start_row_id = row_id;
            b.header.end_row_id = row_id;
            // todo: LSN
            b.leaf_add_block(count, insert_page_id);
            debug_assert!(b.header.end_row_id == row_id + count);
        }
        // attach new leaf to parent
        {
            let p = p_guard.page_mut();
            p.branch_add_entry(PageEntry::new(row_id, leaf_page_id));
            p.header.end_row_id = row_id + count;
        }
        Valid(Ok((row_id, row_id + count)))
    }

    #[inline]
    fn insert_row_page(
        &self,
        count: u64,
        insert_page_id: PageID,
    ) -> Validation<Result<(u64, u64)>> {
        let mut stack = vec![];
        let mut p_guard = {
            let res = verify!(self.find_right_most_leaf(&mut stack, LatchFallbackMode::Exclusive));
            let guard = bypass_res!(res);
            verify!(guard.try_exclusive())
        };
        debug_assert!(p_guard.page().is_leaf());
        let end_row_id = p_guard.page().header.end_row_id;
        if p_guard.page().leaf_is_full() {
            let block = p_guard.page_mut().leaf_last_block_mut();
            if (block.is_row() && block.row_is_full()) || block.is_col() {
                // leaf is full and block is full, we must add new leaf to block index
                if stack.is_empty() {
                    // root is full and already exclusive locked
                    return Valid(self.insert_row_page_split_root(
                        p_guard,
                        end_row_id,
                        count,
                        insert_page_id,
                    ));
                }
                return self.insert_row_page_to_new_leaf(
                    &mut stack,
                    p_guard,
                    end_row_id,
                    count,
                    insert_page_id,
                );
            }
            // insert to current row block
            block.row_add_page(count, insert_page_id);
            p_guard.page_mut().header.end_row_id += count;
            return Valid(Ok((end_row_id, end_row_id + count)));
        }
        if p_guard.page().leaf_is_empty()
            || p_guard.page().leaf_last_block().is_col()
            || p_guard.page().leaf_last_block().row_is_full()
        {
            p_guard.page_mut().leaf_add_block(count, insert_page_id);
            return Valid(Ok((end_row_id, end_row_id + count)));
        }
        p_guard
            .page_mut()
            .leaf_last_block_mut()
            .row_add_page(count, insert_page_id);
        p_guard.page_mut().header.end_row_id += count;
        Valid(Ok((end_row_id, end_row_id + count)))
    }

    #[inline]
    fn find_right_most_leaf(
        &self,
        stack: &mut Vec<PageGuard<'a, BlockNode>>,
        mode: LatchFallbackMode,
    ) -> Validation<Result<PageGuard<BlockNode>>> {
        let mut p_guard: PageGuard<'_, BlockNode> = {
            let g = self.buf_pool.get_page(self.root, LatchFallbackMode::Spin);
            bypass_res!(g)
        };
        // optimistic mode, should always check version after use protected data.
        let mut pu = unsafe { p_guard.page_unchecked() };
        let height = pu.header.height;
        let mut level = 1;
        while !pu.is_leaf() {
            let count = pu.header.count;
            let idx = 1.max(count as usize).min(NBR_ENTRIES_IN_BRANCH) - 1;
            let page_id = pu.branch_entries()[idx].page_id;
            verify!(p_guard.validate());
            let g = bypass_res!(p_guard.copy_keepalive());
            stack.push(g);
            p_guard = if level == height {
                let g = self.buf_pool.get_child_page(p_guard, page_id, mode);
                let res = verify!(g);
                bypass_res!(res)
            } else {
                let g = self
                    .buf_pool
                    .get_child_page(p_guard, page_id, LatchFallbackMode::Spin);
                let res = verify!(g);
                bypass_res!(res)
            };
            pu = unsafe { p_guard.page_unchecked() };
            level += 1;
        }
        Valid(Ok(p_guard))
    }

    #[inline]
    fn try_find_leaf_by_row_id(
        &self,
        row_id: RowID,
        stack: &mut Vec<CursorPosition<'a>>,
    ) -> Validation<Result<()>> {
        let mut g: PageGuard<'a, BlockNode> = {
            let res = self.buf_pool.get_page(self.root, LatchFallbackMode::Spin);
            bypass_res!(res)
        };
        loop {
            let pu = unsafe { g.page_unchecked() };
            if pu.is_leaf() {
                debug_assert!(g.is_optimistic());
                let row_id = pu.header.start_row_id;
                verify!(g.validate());
                stack.push(CursorPosition {
                    g,
                    next: NextKind::Leaf(row_id),
                });
                return Valid(Ok(()));
            }
            let entries = pu.branch_entries();
            let idx = match entries.binary_search_by_key(&row_id, |block| block.row_id) {
                Ok(idx) => idx,
                Err(0) => 0, // even it's out of range, we assign first page.
                Err(idx) => idx - 1,
            };
            // let row_id = entries[idx].row_id;
            let page_id = entries[idx].page_id;
            let next_row_id = if idx + 1 == pu.header.count as usize {
                pu.header.end_row_id
            } else {
                entries[idx + 1].row_id
            };
            verify!(g.validate());
            g.downgrade();
            stack.push(CursorPosition {
                g,
                next: NextKind::Branch(idx + 1, next_row_id),
            });
            g = {
                let res = self.buf_pool.get_page(page_id, LatchFallbackMode::Spin);
                bypass_res!(res)
            }
        }
    }

    #[inline]
    fn try_find_row_id(&self, row_id: RowID) -> Validation<Result<RowLocation>> {
        let mut g: PageGuard<'a, BlockNode> = {
            let res = self.buf_pool.get_page(self.root, LatchFallbackMode::Spin);
            bypass_res!(res)
        };
        loop {
            let pu = unsafe { g.page_unchecked() };
            if pu.is_leaf() {
                // for leaf node, end_row_id is always correct,
                // so we can quickly determine if row id exists
                // in current node.
                if row_id >= pu.header.end_row_id {
                    verify!(g.validate());
                    return Valid(Ok(RowLocation::NotFound));
                }
                let blocks = pu.leaf_blocks();
                let idx =
                    match blocks.binary_search_by_key(&row_id, |block| block.header.start_row_id) {
                        Ok(idx) => idx,
                        Err(0) => {
                            verify!(g.validate());
                            return Valid(Ok(RowLocation::NotFound));
                        }
                        Err(idx) => idx - 1,
                    };
                let block = &blocks[idx];
                if row_id >= block.header.end_row_id {
                    verify!(g.validate());
                    return Valid(Ok(RowLocation::NotFound));
                }
                if block.is_col() {
                    todo!();
                }
                let entries = block.row_page_entries();
                let idx = match entries.binary_search_by_key(&row_id, |entry| entry.row_id) {
                    Ok(idx) => idx,
                    Err(0) => {
                        verify!(g.validate());
                        return Valid(Ok(RowLocation::NotFound));
                    }
                    Err(idx) => idx - 1,
                };
                verify!(g.validate());
                return Valid(Ok(RowLocation::RowPage(entries[idx].page_id)));
            }
            // For branch node, end_row_id is not always correct.
            //
            // With current page insert logic, at most time end_row_id
            // equals to its right-most child's start_row_id plus
            // row count of one row page.
            //
            // All leaf nodes maintain correct row id range.
            // so if input row id exceeds end_row_id, we just redirect
            // it to right-most leaf.
            let page_id = if row_id >= pu.header.end_row_id {
                pu.branch_last_entry().page_id
            } else {
                let entries = pu.branch_entries();
                let idx = match entries.binary_search_by_key(&row_id, |entry| entry.row_id) {
                    Ok(idx) => idx,
                    Err(0) => {
                        verify!(g.validate());
                        return Valid(Ok(RowLocation::NotFound));
                    }
                    Err(idx) => idx - 1,
                };
                entries[idx].page_id
            };
            verify!(g.validate());
            g = {
                let v = self
                    .buf_pool
                    .get_child_page(g, page_id, LatchFallbackMode::Spin);
                let res = verify!(v);
                bypass_res!(res)
            };
        }
    }
}

pub enum RowLocation {
    ColSegment(u64, u64),
    RowPage(PageID),
    NotFound,
}

/// A cursor to
pub struct CursorShared<'a> {
    blk_idx: &'a BlockIndex<'a>,
    stack: Vec<CursorPosition<'a>>,
}

impl<'a> CursorShared<'a> {
    #[inline]
    fn fill_stack_by_row_id_search(&mut self, row_id: RowID) -> Result<()> {
        loop {
            self.stack.clear();
            let v = self
                .blk_idx
                .try_find_leaf_by_row_id(row_id, &mut self.stack);
            let res = verify_continue!(v);
            match res {
                Ok(_) => return Ok(()),
                Err(e) => {
                    // any error occur, we just clear the stack and return.
                    self.stack.clear();
                    return Err(e);
                }
            }
        }
    }
}

impl<'a> Iterator for CursorShared<'a> {
    type Item = Result<PageSharedGuard<'a, BlockNode>>;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        'LOOP: while let Some(pos) = self.stack.pop() {
            match pos.next {
                NextKind::Leaf(row_id) => {
                    // todo: maybe block_until_shared is better
                    match pos.g.try_shared() {
                        Valid(g) => return Some(Ok(g)),
                        Invalid => (),
                    }
                    // validation failed, must retry with row id
                    match self.fill_stack_by_row_id_search(row_id) {
                        Ok(_) => continue 'LOOP,
                        Err(e) => return Some(Err(e)),
                    }
                }
                NextKind::Branch(idx, row_id) => {
                    let pu = unsafe { pos.g.page_unchecked() };
                    // all entries in branch have been traversed.
                    if idx == pu.header.count as usize {
                        if pos.g.validate().is_valid() {
                            // we can just ignore this element
                            // and start from its parent(in stack)
                            continue 'LOOP;
                        }
                        // validation failed, must retry with row id
                        match self.fill_stack_by_row_id_search(row_id) {
                            Ok(_) => continue 'LOOP,
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    let next_row_id = if idx + 1 < pu.header.count as usize {
                        pu.branch_entries()[idx as usize + 1].row_id
                    } else {
                        pu.header.end_row_id
                    };
                    let page_id = pu.branch_entry(idx).page_id;
                    // try get child page, may fail due to version change.
                    let p_guard = pos.g.copy_keepalive().expect("optimistic parent guard");
                    match self.blk_idx.buf_pool.get_child_page(
                        p_guard,
                        page_id,
                        LatchFallbackMode::Shared,
                    ) {
                        Valid(Ok(g)) => {
                            // push next entry to stack
                            self.stack.push(CursorPosition {
                                g: pos.g,
                                next: NextKind::Branch(idx + 1, next_row_id),
                            });
                            match g.try_shared() {
                                Valid(g) => return Some(Ok(g)),
                                Invalid => match self.fill_stack_by_row_id_search(row_id) {
                                    Ok(_) => continue 'LOOP,
                                    Err(e) => return Some(Err(e)),
                                },
                            }
                        }
                        Valid(Err(e)) => {
                            self.stack.clear();
                            return Some(Err(e));
                        }
                        Invalid => {
                            // validation failed, must retry with row id
                            match self.fill_stack_by_row_id_search(row_id) {
                                Ok(_) => continue 'LOOP,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                    }
                }
            }
        }
        None
    }
}

struct CursorPosition<'a> {
    g: PageGuard<'a, BlockNode>,
    // next position: branch entry index or leaf end row id.
    next: NextKind,
}

enum NextKind {
    // branch entry index
    Branch(usize, RowID),
    // leaf row id
    Leaf(RowID),
}

#[inline]
fn estimate_row_count_in_page(_row_len: usize) -> u64 {
    // todo: calculate based on page payload size
    100
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_index_free_list() {
        let buf_pool = FixedBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap();
        {
            let cols = vec![Layout::Byte8, Layout::Byte8];
            let blk_idx = BlockIndex::new(buf_pool).unwrap();
            let p1 = blk_idx.get_insert_page(100, &cols).unwrap();
            let pid1 = p1.page_id();
            blk_idx.free_exclusive_insert_page(p1);
            assert!(blk_idx.insert_free_list.lock().len() == 1);
            let p2 = blk_idx.get_insert_page(100, &cols).unwrap();
            assert!(pid1 == p2.page_id());
            assert!(blk_idx.insert_free_list.lock().is_empty());
        }
        unsafe {
            FixedBufferPool::drop_static(buf_pool);
        }
    }

    #[test]
    fn test_block_index_insert_row_page() {
        let buf_pool = FixedBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap();
        {
            let cols = vec![Layout::Byte8, Layout::Byte8];
            let blk_idx = BlockIndex::new(buf_pool).unwrap();
            let p1 = blk_idx.get_insert_page(100, &cols).unwrap();
            let pid1 = p1.page_id();
            blk_idx.free_exclusive_insert_page(p1);
            assert!(blk_idx.insert_free_list.lock().len() == 1);
            let p2 = blk_idx.get_insert_page(100, &cols).unwrap();
            assert!(pid1 == p2.page_id());
            assert!(blk_idx.insert_free_list.lock().is_empty());
        }
        unsafe {
            FixedBufferPool::drop_static(buf_pool);
        }
    }

    #[test]
    fn test_block_index_cursor_shared() {
        let row_pages = 10240usize;
        // allocate 1GB buffer pool is enough: 10240 pages ~= 640MB
        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024 * 1024).unwrap();
        {
            let cols = vec![Layout::Byte8, Layout::Byte8];
            let blk_idx = BlockIndex::new(buf_pool).unwrap();
            for _ in 0..row_pages {
                let _ = blk_idx.get_insert_page(100, &cols).unwrap();
            }
            let mut count = 0usize;
            for res in blk_idx.cursor_shared(0).unwrap() {
                assert!(res.is_ok());
                count += 1;
                let g = res.unwrap();
                let node = g.page();
                assert!(node.is_leaf());
                let row_pages: usize = node
                    .leaf_blocks()
                    .iter()
                    .map(|block| {
                        if block.is_row() {
                            block.row_page_entries().iter().count()
                        } else {
                            0usize
                        }
                    })
                    .sum();
                println!(
                    "start_row_id={:?}, end_row_id={:?}, blocks={:?}, row_pages={:?}",
                    node.header.start_row_id, node.header.end_row_id, node.header.count, row_pages
                );
            }
            let row_pages_per_leaf = NBR_BLOCKS_IN_LEAF * NBR_PAGES_IN_ROW_BLOCK;
            assert!(count == (row_pages + row_pages_per_leaf - 1) / row_pages_per_leaf);
        }
        unsafe {
            FixedBufferPool::drop_static(buf_pool);
        }
    }

    #[test]
    fn test_block_index_search() {
        let row_pages = 10240usize;
        let rows_per_page = 100usize;
        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024 * 1024).unwrap();
        {
            let cols = vec![Layout::Byte8, Layout::Byte8];
            let blk_idx = BlockIndex::new(buf_pool).unwrap();
            for _ in 0..row_pages {
                let _ = blk_idx.get_insert_page(rows_per_page, &cols).unwrap();
            }
            {
                let res = blk_idx
                    .buf_pool
                    .get_page::<BlockNode>(blk_idx.root, LatchFallbackMode::Spin)
                    .unwrap();
                let p = res.try_exclusive().unwrap();
                let bn = p.page();
                println!("root is leaf ? {:?}", bn.is_leaf());
                println!(
                    "root page_id={:?}, start_row_id={:?}, end_row_id={:?}",
                    p.page_id(),
                    bn.header.start_row_id,
                    bn.header.end_row_id
                );
                println!("root entries {:?}", bn.branch_entries());
            }
            for i in 0..row_pages {
                let row_id = (i * rows_per_page + rows_per_page / 2) as u64;
                let res = blk_idx.find_row_id(row_id).unwrap();
                match res {
                    RowLocation::RowPage(page_id) => {
                        let g: PageGuard<'_, RowPage> = buf_pool
                            .get_page(page_id, LatchFallbackMode::Shared)
                            .unwrap();
                        let g = g.try_shared().unwrap();
                        let p = g.page();
                        assert!(p.header.start_row_id as usize == i * rows_per_page);
                    }
                    _ => panic!("invalid search result for i={:?}", i),
                }
            }
        }
        unsafe {
            FixedBufferPool::drop_static(buf_pool);
        }
    }
}
