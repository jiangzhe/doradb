use crate::buffer::frame::FrameContext;
use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageOptimisticGuard, PageSharedGuard};
use crate::buffer::page::{BufferPage, PageID, PAGE_SIZE};
use crate::buffer::BufferPool;
use crate::catalog::TableMetadata;
use crate::error::{
    Error, Result, Validation,
    Validation::{Invalid, Valid},
};
use crate::latch::LatchFallbackMode;
use crate::row::{RowID, RowPage, INVALID_ROW_ID};
use crate::trx::sys::TransactionSystem;
use crate::trx::undo::UndoMap;
use doradb_catalog::TableID;
use either::Either::{Left, Right};
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::mem;

pub const BLOCK_PAGE_SIZE: usize = PAGE_SIZE;
pub const BLOCK_HEADER_SIZE: usize = mem::size_of::<BlockNodeHeader>();
pub const BLOCK_SIZE: usize = 1272;
pub const NBR_BLOCKS_IN_LEAF: usize = 51;
pub const NBR_ENTRIES_IN_BRANCH: usize = 4093;
pub const ENTRY_SIZE: usize = mem::size_of::<PageEntry>();
pub const NBR_PAGES_IN_ROW_BLOCK: usize = 78;
pub const NBR_SEGMENTS_IN_COL_BLOCK: usize = 16;
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
    /// Initilaize block node with given height, start row id.
    /// End row id is set to MAX_ROW_ID.
    #[inline]
    pub fn init(&mut self, height: u32, start_row_id: RowID, count: u64, insert_page_id: PageID) {
        self.header.height = height;
        self.header.start_row_id = start_row_id;
        self.header.end_row_id = INVALID_ROW_ID;
        self.header.count = 0;
        self.leaf_add_block(start_row_id, count, insert_page_id);
    }

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
    pub fn leaf_add_block(&mut self, start_row_id: RowID, count: u64, page_id: PageID) {
        debug_assert!(self.is_leaf());
        debug_assert!(!self.leaf_is_full());
        self.header.count += 1;
        self.leaf_last_block_mut()
            .init_row(start_row_id, count, page_id);
        // keep leaf header end row id as MAX_ROW_ID, and only when leaf is full, we will update
        // end row id.
    }
}

impl BufferPage for BlockNode {}

#[repr(C)]
#[derive(Clone)]
pub struct BlockNodeHeader {
    // height of the node
    pub height: u32,
    // count of entry.
    pub count: u32,
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
    pub fn init_row(&mut self, start_row_id: RowID, count: u64, page_id: PageID) {
        self.header.kind = BlockKind::Row;
        self.header.start_row_id = start_row_id;
        self.header.end_row_id = start_row_id + count;
        self.header.count = 1;
        let entry = &mut self.row_page_entries_mut()[0];
        entry.row_id = start_row_id;
        entry.page_id = page_id;
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
    pub fn row_add_page(&mut self, count: u64, page_id: PageID) {
        debug_assert!((self.header.count as usize) < NBR_PAGES_IN_ROW_BLOCK);
        let entry = PageEntry {
            row_id: self.header.end_row_id,
            page_id,
        };
        let idx = self.header.count as usize;
        self.header.count += 1;
        self.row_page_entries_mut()[idx] = entry;
        self.header.end_row_id += count;
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
pub struct BlockIndex<P: BufferPool> {
    pub table_id: TableID,
    root: PageID,
    insert_free_list: Mutex<Vec<PageID>>,
    // Reference to storage engine,
    // used for committing new page.
    page_committer: RedoLogCommitter<P>,
    _marker: PhantomData<P>,
}

impl<P: BufferPool> BlockIndex<P> {
    /// Create a new block index backed by buffer pool.
    #[inline]
    pub async fn new(
        buf_pool: &'static P,
        trx_sys: &'static TransactionSystem<P>,
        table_id: TableID,
    ) -> Self {
        let mut g: PageExclusiveGuard<BlockNode> = buf_pool.allocate_page().await;
        let page_id = g.page_id();
        let page = g.page_mut();
        page.header.height = 0;
        page.header.count = 0;
        page.header.start_row_id = 0;
        page.header.end_row_id = INVALID_ROW_ID;
        BlockIndex {
            table_id,
            root: page_id,
            insert_free_list: Mutex::new(Vec::with_capacity(64)),
            page_committer: RedoLogCommitter::new(trx_sys, table_id),
            _marker: PhantomData,
        }
    }

    /// Get row page for insertion.
    /// Caller should cache insert page id to avoid invoking this method frequently.
    #[inline]
    pub async fn get_insert_page(
        &self,
        buf_pool: &'static P,
        count: usize,
        schema: &TableMetadata,
    ) -> PageSharedGuard<RowPage> {
        match self.get_insert_page_from_free_list(buf_pool).await {
            Ok(free_page) => return free_page,
            _ => (), // we just ignore the free list error and latch error, and continue to get new page.
        }
        let mut new_page: PageExclusiveGuard<RowPage> = buf_pool.allocate_page().await;
        let new_page_id = new_page.page_id();
        loop {
            match self
                .insert_row_page(buf_pool, count as u64, new_page_id)
                .await
            {
                Invalid => (),
                Valid((start_row_id, end_row_id)) => {
                    // initialize row page.
                    debug_assert!(end_row_id == start_row_id + count as u64);
                    new_page
                        .page_mut()
                        .init(start_row_id, count as usize, schema);
                    // create and attach a new empty undo map.
                    let undo_map = UndoMap::new(count);
                    new_page.bf_mut().ctx = Some(Box::new(FrameContext::UndoMap(undo_map)));

                    // persist log to commit this page.
                    self.page_committer
                        .commit_new_page(new_page_id, start_row_id, end_row_id)
                        .await;

                    // finally, we downgrade the page lock for shared mode.
                    return new_page.downgrade_shared();
                }
            }
        }
    }

    /// Find location of given row id, maybe in column file or row page.
    #[inline]
    pub async fn find_row_id(&self, buf_pool: &'static P, row_id: RowID) -> RowLocation {
        loop {
            let res = self.try_find_row_id(buf_pool, row_id).await;
            let res = verify_continue!(res);
            return res;
        }
    }

    /// Put given page into insert free list.
    #[inline]
    pub fn free_exclusive_insert_page(&self, guard: PageExclusiveGuard<RowPage>) {
        let page_id = guard.page_id();
        drop(guard);
        let mut free_list = self.insert_free_list.lock();
        free_list.push(page_id);
    }

    /// Returns the cursor for range scan.
    #[inline]
    pub fn cursor(&self, buf_pool: &'static P) -> Cursor<P> {
        Cursor {
            buf_pool,
            blk_idx: self,
            parent: None,
            child: None,
        }
    }

    #[inline]
    async fn get_insert_page_from_free_list(
        &self,
        buf_pool: &'static P,
    ) -> Result<PageSharedGuard<RowPage>> {
        let page_id = {
            let mut g = self.insert_free_list.lock();
            if g.is_empty() {
                return Err(Error::EmptyFreeListOfBufferPool);
            }
            g.pop().unwrap()
        };
        let page_guard = buf_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
            .await
            .shared_async()
            .await;
        Ok(page_guard)
    }

    #[inline]
    async fn insert_row_page_split_root(
        &self,
        buf_pool: &'static P,
        mut p_guard: PageExclusiveGuard<BlockNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> (RowID, RowID) {
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
        let mut l_guard: PageExclusiveGuard<BlockNode> = buf_pool.allocate_page().await;
        let l_page_id = l_guard.page_id();
        l_guard.page_mut().clone_from(p_guard.page());
        l_guard.page_mut().header.end_row_id = row_id; // update original page's end row id

        // create right child, add one row block with one page entry.
        let mut r_guard: PageExclusiveGuard<BlockNode> = buf_pool.allocate_page().await;
        let r_page_id = r_guard.page_id();
        {
            let r = r_guard.page_mut();
            r.init(0, r_row_id, count, insert_page_id);
            debug_assert!(r.header.end_row_id == INVALID_ROW_ID);
        }

        // initialize parent again.
        {
            let p = p_guard.page_mut();
            p.header.height = new_height; // branch
            p.header.start_row_id = l_row_id;
            // reinitialize root's end row id to MAX_ROW_ID
            p.header.end_row_id = INVALID_ROW_ID;
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
        (r_row_id, max_row_id)
    }

    #[inline]
    async fn insert_row_page_to_new_leaf(
        &self,
        buf_pool: &'static P,
        stack: &mut Vec<PageOptimisticGuard<BlockNode>>,
        c_guard: PageExclusiveGuard<BlockNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> Validation<(RowID, RowID)> {
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
                let res = self
                    .insert_row_page_split_root(buf_pool, p_guard, row_id, count, insert_page_id)
                    .await;
                return Valid(res);
            }
        }
        // create new leaf node with one insert page id
        let mut leaf: PageExclusiveGuard<BlockNode> = buf_pool.allocate_page().await;
        let leaf_page_id = leaf.page_id();
        {
            let b: &mut BlockNode = leaf.page_mut();
            b.init(0, row_id, count, insert_page_id);
            debug_assert!(b.header.end_row_id == INVALID_ROW_ID);
        }
        // attach new leaf to parent
        {
            let p = p_guard.page_mut();
            p.branch_add_entry(PageEntry::new(row_id, leaf_page_id));
        }
        Valid((row_id, row_id + count))
    }

    #[inline]
    async fn insert_row_page(
        &self,
        buf_pool: &'static P,
        count: u64,
        insert_page_id: PageID,
    ) -> Validation<(RowID, RowID)> {
        let mut stack = vec![];
        let mut p_guard = {
            let g = self
                .find_right_most_leaf(buf_pool, &mut stack, LatchFallbackMode::Exclusive)
                .await;
            let mut guard = verify!(g);
            verify!(guard.try_exclusive());
            guard.exclusive_async().await
        };
        debug_assert!(p_guard.page().is_leaf());
        // only empty block index will have empty leaf.
        if p_guard.page().leaf_is_empty() {
            let start_row_id = p_guard.page().header.start_row_id;
            p_guard
                .page_mut()
                .leaf_add_block(start_row_id, count, insert_page_id);
            return Valid((start_row_id, start_row_id + count));
        }
        // end row id of leaf header is maximum value of ROW ID.
        // the precise end row id is stored inside the header of last block.
        let end_row_id = p_guard.page().leaf_last_block().header.end_row_id;
        if p_guard.page().leaf_is_full() {
            let block = p_guard.page_mut().leaf_last_block_mut();
            if (block.is_row() && block.row_is_full()) || block.is_col() {
                // leaf is full and block is full, we must add new leaf to block index
                if stack.is_empty() {
                    // root is full and already exclusive locked
                    let res = self
                        .insert_row_page_split_root(
                            buf_pool,
                            p_guard,
                            end_row_id,
                            count,
                            insert_page_id,
                        )
                        .await;
                    return Valid(res);
                }
                return self
                    .insert_row_page_to_new_leaf(
                        buf_pool,
                        &mut stack,
                        p_guard,
                        end_row_id,
                        count,
                        insert_page_id,
                    )
                    .await;
            }
            // insert to current row block
            block.row_add_page(count, insert_page_id);
            return Valid((end_row_id, end_row_id + count));
        }
        if p_guard.page().leaf_last_block().is_col()
            || p_guard.page().leaf_last_block().row_is_full()
        {
            let start_row_id = p_guard.page().leaf_last_block().header.end_row_id;
            p_guard
                .page_mut()
                .leaf_add_block(start_row_id, count, insert_page_id);
            return Valid((end_row_id, end_row_id + count));
        }
        p_guard
            .page_mut()
            .leaf_last_block_mut()
            .row_add_page(count, insert_page_id);
        Valid((end_row_id, end_row_id + count))
    }

    #[inline]
    async fn find_right_most_leaf(
        &self,
        buf_pool: &'static P,
        stack: &mut Vec<PageOptimisticGuard<BlockNode>>,
        mode: LatchFallbackMode,
    ) -> Validation<PageGuard<BlockNode>> {
        let mut p_guard: PageGuard<BlockNode> =
            buf_pool.get_page(self.root, LatchFallbackMode::Spin).await;
        // optimistic mode, should always check version after use protected data.
        let mut pu = unsafe { p_guard.page_unchecked() };
        let height = pu.header.height;
        let mut level = 1;
        while !pu.is_leaf() {
            let count = pu.header.count;
            let idx = 1.max(count as usize).min(NBR_ENTRIES_IN_BRANCH) - 1;
            let page_id = pu.branch_entries()[idx].page_id;
            verify!(p_guard.validate());
            p_guard = if level == height {
                let g = buf_pool.get_child_page(&p_guard, page_id, mode).await;
                stack.push(p_guard.downgrade());
                verify!(g)
            } else {
                let g = buf_pool
                    .get_child_page(&p_guard, page_id, LatchFallbackMode::Spin)
                    .await;
                stack.push(p_guard.downgrade());
                verify!(g)
            };
            pu = unsafe { p_guard.page_unchecked() };
            level += 1;
        }
        Valid(p_guard)
    }

    #[inline]
    async fn try_find_row_id(
        &self,
        buf_pool: &'static P,
        row_id: RowID,
    ) -> Validation<RowLocation> {
        let mut g: PageGuard<BlockNode> =
            buf_pool.get_page(self.root, LatchFallbackMode::Spin).await;
        loop {
            let pu = unsafe { g.page_unchecked() };
            if pu.is_leaf() {
                // for leaf node, end_row_id is always correct,
                // so we can quickly determine if row id exists
                // in current node.
                if pu.leaf_is_empty() || row_id >= pu.header.end_row_id {
                    verify!(g.validate());
                    return Valid(RowLocation::NotFound);
                }
                let blocks = pu.leaf_blocks();
                let idx =
                    match blocks.binary_search_by_key(&row_id, |block| block.header.start_row_id) {
                        Ok(idx) => idx,
                        Err(0) => {
                            verify!(g.validate());
                            return Valid(RowLocation::NotFound);
                        }
                        Err(idx) => idx - 1,
                    };
                let block = &blocks[idx];
                if row_id >= block.header.end_row_id {
                    verify!(g.validate());
                    return Valid(RowLocation::NotFound);
                }
                if block.is_col() {
                    todo!();
                }
                let entries = block.row_page_entries();
                let idx = match entries.binary_search_by_key(&row_id, |entry| entry.row_id) {
                    Ok(idx) => idx,
                    Err(0) => {
                        verify!(g.validate());
                        return Valid(RowLocation::NotFound);
                    }
                    Err(idx) => idx - 1,
                };
                verify!(g.validate());
                return Valid(RowLocation::RowPage(entries[idx].page_id));
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
                        return Valid(RowLocation::NotFound);
                    }
                    Err(idx) => idx - 1,
                };
                entries[idx].page_id
            };
            verify!(g.validate());
            g = {
                let v = buf_pool
                    .get_child_page(&g, page_id, LatchFallbackMode::Spin)
                    .await;
                verify!(v)
            };
        }
    }
}

unsafe impl<P: BufferPool> Send for BlockIndex<P> {}

pub enum RowLocation {
    ColSegment(u64, u64),
    RowPage(PageID),
    NotFound,
}

pub enum CursorState {
    Uninitialize,
    EOF,
}

/// A cursor to read all leaf values.
pub struct Cursor<'a, P: BufferPool> {
    buf_pool: &'static P,
    blk_idx: &'a BlockIndex<P>,
    // The parent node of current located
    parent: Option<BranchLookup>,
    child: Option<PageGuard<BlockNode>>,
}

impl<'a, P: BufferPool> Cursor<'a, P> {
    #[inline]
    pub async fn seek(mut self, row_id: RowID) -> Self {
        self.clear();
        while let Invalid = self.try_find_leaf_with_parent(row_id).await {
            self.clear();
        }
        self
    }

    #[inline]
    pub async fn next(&mut self) -> Option<PageGuard<BlockNode>> {
        if let Some(child) = self.child.take() {
            debug_assert!(child.is_shared());
            return Some(child);
        }
        if let Some(parent) = self.parent.as_ref() {
            debug_assert!(parent.g.is_shared());
            let shared = unsafe { parent.g.as_shared() };
            let page = shared.page();
            let entries = page.branch_entries();
            let next_idx = parent.idx + 1;
            if next_idx == entries.len() {
                // current parent is exhausted.
                let row_id = page.header.end_row_id;
                self.parent.take();
                if row_id == INVALID_ROW_ID {
                    // the traverse is done.
                    return None;
                }
                // otherwise, we rerun the search on given row id to get next leaf.
                while let Invalid = self.try_find_leaf_with_parent(row_id).await {
                    self.clear();
                }
                let child = self.child.take().unwrap();
                debug_assert!(child.is_shared());
                return Some(child);
            }
            // otherwise, we jump to next slot and get leaf node.
            let page_id = entries[next_idx].page_id;
            self.parent.as_mut().unwrap().idx = next_idx; // update parent position.
            let child = self
                .buf_pool
                .get_page(page_id, LatchFallbackMode::Shared)
                .await;
            return Some(child.shared_async().await.facade(false));
        }
        None
    }

    #[inline]
    fn clear(&mut self) {
        self.parent.take();
        self.child.take();
    }

    #[inline]
    async fn try_find_leaf_with_parent(&mut self, row_id: RowID) -> Validation<()> {
        debug_assert!(row_id != INVALID_ROW_ID); // every row id other than MAX_ROW_ID can find a leaf.
        let mut g: PageGuard<BlockNode> = self
            .buf_pool
            .get_page(self.blk_idx.root, LatchFallbackMode::Shared)
            .await;
        'SEARCH: loop {
            let pu = unsafe { g.page_unchecked() };
            if pu.is_leaf() {
                // share lock for read
                if let Some(mut parent) = self.parent.take() {
                    // we first lock parent for share, to make sure
                    // the range is fixed on child node.
                    verify!(parent.g.try_shared());
                    self.parent = Some(parent);
                }
                match g.try_shared_either() {
                    Left(c) => {
                        // share lock on child succeeds
                        self.child = Some(c.facade(false));
                        return Valid(());
                    }
                    Right(new_g) => {
                        // since we successfully lock parent node,
                        // that means the range of child can not change,
                        // so we can just wait for other thread finish its modification.
                        // NOTE: at this time, the parent is locked. That means SMO
                        // must acquire lock from top down, otherwise, deadlock will happen.
                        g = new_g.shared_async().await.facade(false);
                        continue 'SEARCH;
                    }
                }
            }
            let entries = pu.branch_entries();
            let idx = match entries.binary_search_by_key(&row_id, |block| block.row_id) {
                Ok(idx) => idx,
                Err(0) => 0, // even it's out of range, we assign first page.
                Err(idx) => idx - 1,
            };
            let page_id = entries[idx].page_id;
            verify!(g.validate());
            let c = self
                .buf_pool
                .get_child_page(&g, page_id, LatchFallbackMode::Spin)
                .await;
            let c = verify!(c);
            self.parent = Some(BranchLookup { g, idx });
            g = c;
        }
    }
}

struct BranchLookup {
    g: PageGuard<BlockNode>,
    idx: usize,
}

struct RedoLogCommitter<P: BufferPool> {
    trx_sys: &'static TransactionSystem<P>,
    table_id: TableID,
}

impl<P: BufferPool> RedoLogCommitter<P> {
    #[inline]
    pub fn new(trx_sys: &'static TransactionSystem<P>, table_id: TableID) -> Self {
        RedoLogCommitter { trx_sys, table_id }
    }

    pub async fn commit_new_page(&self, page_id: PageID, start_row_id: RowID, end_row_id: RowID) {
        let mut trx = self.trx_sys.begin_sys_trx();
        let table_id = self.table_id;
        // Once a row page is added to block index, we start
        // a new internal transaction and log its information.
        trx.create_row_page(table_id, page_id, start_row_id, end_row_id);
        let res = self.trx_sys.commit_sys(trx).await;
        assert!(res.is_ok());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::lifetime::StaticLifetime;
    use crate::trx::sys::TrxSysConfig;
    use doradb_catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use doradb_datatype::PreciseType;
    use semistr::SemiStr;

    #[test]
    fn test_block_index_free_list() {
        smol::block_on(async {
            let engine = Engine::new_fixed(64 * 1024 * 1024, TrxSysConfig::default())
                .await
                .unwrap();
            {
                let metadata = TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                );
                let blk_idx = BlockIndex::new(&engine.buf_pool, &engine.trx_sys, 101).await;
                let p1 = blk_idx
                    .get_insert_page(&engine.buf_pool, 100, &metadata)
                    .await;
                let pid1 = p1.page_id();
                let p1 = p1.downgrade().exclusive_async().await;
                blk_idx.free_exclusive_insert_page(p1);
                assert!(blk_idx.insert_free_list.lock().len() == 1);
                let p2 = blk_idx
                    .get_insert_page(&engine.buf_pool, 100, &metadata)
                    .await;
                assert!(pid1 == p2.page_id());
                assert!(blk_idx.insert_free_list.lock().is_empty());
            }
            unsafe {
                StaticLifetime::drop_static(engine);
            }
        })
    }

    #[test]
    fn test_block_index_insert_row_page() {
        smol::block_on(async {
            let engine = Engine::new_fixed(64 * 1024 * 1024, TrxSysConfig::default())
                .await
                .unwrap();
            {
                let metadata = TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                );
                let blk_idx = BlockIndex::new(&engine.buf_pool, &engine.trx_sys, 101).await;
                let p1 = blk_idx
                    .get_insert_page(&engine.buf_pool, 100, &metadata)
                    .await;
                let pid1 = p1.page_id();
                let p1 = p1.downgrade().exclusive_async().await;
                blk_idx.free_exclusive_insert_page(p1);
                assert!(blk_idx.insert_free_list.lock().len() == 1);
                let p2 = blk_idx
                    .get_insert_page(&engine.buf_pool, 100, &metadata)
                    .await;
                assert!(pid1 == p2.page_id());
                assert!(blk_idx.insert_free_list.lock().is_empty());
            }
            unsafe {
                StaticLifetime::drop_static(engine);
            }
        })
    }

    #[test]
    fn test_block_index_cursor_shared() {
        smol::block_on(async {
            let row_pages = 10240usize;
            // allocate 1GB buffer pool is enough: 10240 pages ~= 640MB
            let engine = Engine::new_fixed(1024 * 1024 * 1024, TrxSysConfig::default())
                .await
                .unwrap();
            {
                let metadata = TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                );
                let blk_idx = BlockIndex::new(&engine.buf_pool, &engine.trx_sys, 101).await;
                for _ in 0..row_pages {
                    let _ = blk_idx
                        .get_insert_page(&engine.buf_pool, 100, &metadata)
                        .await;
                }
                let mut count = 0usize;
                let mut cursor = blk_idx.cursor(&engine.buf_pool).seek(0).await;
                while let Some(res) = cursor.next().await {
                    count += 1;
                    if count == 10000 {
                        println!("{}", count);
                    }
                    let g = unsafe { res.as_shared() };
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
                        node.header.start_row_id,
                        node.header.end_row_id,
                        node.header.count,
                        row_pages
                    );
                }
                let row_pages_per_leaf = NBR_BLOCKS_IN_LEAF * NBR_PAGES_IN_ROW_BLOCK;
                assert!(count == (row_pages + row_pages_per_leaf - 1) / row_pages_per_leaf);
            }
            unsafe {
                StaticLifetime::drop_static(engine);
            }
        })
    }

    fn first_i32_unique_index() -> IndexSpec {
        IndexSpec::new("idx_id", vec![IndexKey::new(0)], IndexAttributes::UK)
    }

    #[test]
    fn test_block_index_search() {
        smol::block_on(async {
            let row_pages = 10240usize;
            let rows_per_page = 100usize;
            let engine = Engine::new_fixed(1024 * 1024 * 1024, TrxSysConfig::default())
                .await
                .unwrap();
            {
                let metadata = TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                );
                let blk_idx = BlockIndex::new(&engine.buf_pool, &engine.trx_sys, 101).await;
                for _ in 0..row_pages {
                    let _ = blk_idx
                        .get_insert_page(&engine.buf_pool, rows_per_page, &metadata)
                        .await;
                }
                {
                    let res = engine
                        .buf_pool
                        .get_page::<BlockNode>(blk_idx.root, LatchFallbackMode::Spin)
                        .await;
                    let p = res.shared_async().await;
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
                    let res = blk_idx.find_row_id(&engine.buf_pool, row_id).await;
                    match res {
                        RowLocation::RowPage(page_id) => {
                            let g: PageGuard<RowPage> = engine
                                .buf_pool
                                .get_page(page_id, LatchFallbackMode::Shared)
                                .await;
                            let g = g.shared_async().await;
                            let p = g.page();
                            assert!(p.header.start_row_id as usize == i * rows_per_page);
                        }
                        _ => panic!("invalid search result for i={:?}", i),
                    }
                }
            }
            unsafe {
                StaticLifetime::drop_static(engine);
            }
        })
    }

    #[test]
    fn test_block_index_log() {
        smol::block_on(async {
            let rows_per_page = 100;
            let engine = Engine::new_fixed(64 * 1024 * 1024, TrxSysConfig::default())
                .await
                .unwrap();
            {
                let metadata = TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                );
                let blk_idx = BlockIndex::new(&engine.buf_pool, &engine.trx_sys, 101).await;
                // create a new page for rowid=0..100
                let _ = blk_idx
                    .get_insert_page(&engine.buf_pool, rows_per_page, &metadata)
                    .await;
                // todo: analyze log to see the log is persisted.
            }
            unsafe {
                StaticLifetime::drop_static(engine);
            }
        })
    }
}
