use crate::buffer::guard::{
    FacadePageGuard, PageExclusiveGuard, PageGuard, PageOptimisticGuard, PageSharedGuard,
};
use crate::buffer::page::{BufferPage, PAGE_SIZE, PageID};
use crate::buffer::{BufferPool, FixedBufferPool};
use crate::catalog::{TableID, TableMetadata};
use crate::error::{
    Error, Result, Validation,
    Validation::{Invalid, Valid},
};
use crate::file::table_file::{ActiveRoot, TableFile};
use crate::index::find_in_file;
use crate::index::util::{Maskable, ParentPosition, RedoLogPageCommitter};
use crate::latch::{HybridGuard, HybridLatch, LatchFallbackMode};
use crate::row::{INVALID_ROW_ID, RowID, RowPage};
use crate::trx::sys::TransactionSystem;
use bytemuck::{Pod, Zeroable, cast_slice, cast_slice_mut};
use either::Either::{self, Left, Right};
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

pub const BLOCK_PAGE_SIZE: usize = PAGE_SIZE;
pub const BLOCK_HEADER_SIZE: usize = mem::size_of::<BlockNodeHeader>();
pub const NBR_ENTRIES_IN_BRANCH: usize = 4093;
pub const ENTRY_SIZE: usize = mem::size_of::<PageEntry>();
pub const NBR_PAGE_ENTRIES_IN_LEAF: usize = (BLOCK_PAGE_SIZE - BLOCK_HEADER_SIZE) / ENTRY_SIZE;

const _: () = assert!(
    { mem::size_of::<BlockNode>() == BLOCK_PAGE_SIZE },
    "Size of node of BlockIndex should equal to 64KB"
);

const _: () = assert!(
    { BLOCK_HEADER_SIZE + NBR_ENTRIES_IN_BRANCH * ENTRY_SIZE <= BLOCK_PAGE_SIZE },
    "Size of branch node of BlockIndex can be at most 64KB"
);

const _: () = assert!(
    { BLOCK_HEADER_SIZE + NBR_PAGE_ENTRIES_IN_LEAF * ENTRY_SIZE <= BLOCK_PAGE_SIZE },
    "Size of leaf node of BlockIndex can be at most 64KB"
);

const _: () = assert!(
    { BLOCK_HEADER_SIZE.is_multiple_of(mem::align_of::<PageEntry>()) },
    "BlockNode data area must align with PageEntry alignment"
);

const _: () = assert!(
    { mem::size_of::<PageEntry>() == ENTRY_SIZE },
    "ENTRY_SIZE must match PageEntry size"
);

/// BlockNode is B-Tree node of block index.
/// It can be either branch or leaf.
/// Branch contains at most 4093 child node pointers.
/// Leaf contains at most NBR_PAGE_ENTRIES_IN_LEAF page entries.
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
        self.leaf_add_entry(start_row_id, count, insert_page_id);
    }

    #[inline]
    pub fn init_empty(&mut self, height: u32, start_row_id: RowID) {
        self.header.height = height;
        self.header.start_row_id = start_row_id;
        self.header.end_row_id = INVALID_ROW_ID;
        self.header.count = 0;
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
        self.entries(self.header.count as usize)
    }

    /// Returns mutable entry slice in branch node.
    #[inline]
    pub fn branch_entries_mut(&mut self) -> &mut [PageEntry] {
        debug_assert!(self.is_branch());
        self.entries_mut(self.header.count as usize)
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

    /// Returns mutable last entry in branch node.
    #[inline]
    pub fn branch_last_entry_mut(&mut self) -> &mut PageEntry {
        debug_assert!(self.is_branch());
        let idx = self.header.count as usize - 1;
        &mut self.branch_entries_mut()[idx]
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

    /// Returns whether the leaf node is full.
    #[inline]
    pub fn leaf_is_full(&self) -> bool {
        debug_assert!(self.is_leaf());
        self.header.count as usize == NBR_PAGE_ENTRIES_IN_LEAF
    }

    /// Returns whether the leaf node is empty.
    #[inline]
    pub fn leaf_is_empty(&self) -> bool {
        debug_assert!(self.is_leaf());
        self.header.count == 0
    }

    /// Returns entry slice of leaf node.
    #[inline]
    pub fn leaf_entries(&self) -> &[PageEntry] {
        debug_assert!(self.is_leaf());
        self.entries(self.header.count as usize)
    }

    /// Returns entry in leaf node by given index.
    #[inline]
    pub fn leaf_entry(&self, idx: usize) -> &PageEntry {
        debug_assert!(self.is_leaf());
        &self.leaf_entries()[idx]
    }

    /// Returns last entry in leaf node.
    #[inline]
    pub fn leaf_last_entry(&self) -> &PageEntry {
        self.leaf_entry(self.header.count as usize - 1)
    }

    /// Returns mutable last entry in leaf node.
    #[inline]
    pub fn leaf_last_entry_mut(&mut self) -> &mut PageEntry {
        debug_assert!(self.is_leaf());
        let count = self.header.count as usize;
        &mut self.leaf_entries_mut()[count - 1]
    }

    /// Returns mutable entry slice in leaf node.
    #[inline]
    pub fn leaf_entries_mut(&mut self) -> &mut [PageEntry] {
        debug_assert!(self.is_leaf());
        self.entries_mut(self.header.count as usize)
    }

    /// Add a new entry in leaf node.
    #[inline]
    pub fn leaf_add_entry(&mut self, start_row_id: RowID, count: u64, page_id: PageID) {
        debug_assert!(self.is_leaf());
        debug_assert!(!self.leaf_is_full());
        self.header.count += 1;
        let entry = self.leaf_last_entry_mut();
        entry.row_id = start_row_id;
        entry.page_id = page_id;
        self.header.end_row_id = start_row_id + count;
    }

    #[inline]
    fn entries(&self, len: usize) -> &[PageEntry] {
        let bytes_len = len * mem::size_of::<PageEntry>();
        cast_slice(&self.data[..bytes_len])
    }

    #[inline]
    fn entries_mut(&mut self, len: usize) -> &mut [PageEntry] {
        let bytes_len = len * mem::size_of::<PageEntry>();
        cast_slice_mut(&mut self.data[..bytes_len])
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
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
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
pub struct BlockIndex {
    pub table_id: TableID,
    root: BlockIndexRoot,
    height: AtomicUsize,
    insert_free_list: Mutex<Vec<PageID>>,
    // Fixed buffer pool to hold block nodes.
    pool: &'static FixedBufferPool,
    table_file: Arc<TableFile>,
    // Reference to storage engine,
    // used for committing new page.
    page_committer: Mutex<Option<RedoLogPageCommitter>>,
}

impl BlockIndex {
    /// Create a new block index backed by buffer pool.
    #[inline]
    pub async fn new(
        pool: &'static FixedBufferPool,
        table_id: TableID,
        pivot: RowID,
        file_root: AtomicPtr<ActiveRoot>,
        table_file: Arc<TableFile>,
    ) -> Self {
        let mut g = pool.allocate_page::<BlockNode>().await;
        let page_id = g.page_id();
        let page = g.page_mut();
        page.init_empty(0, 0);
        let root = BlockIndexRoot::new(page_id, pivot, file_root);
        BlockIndex {
            table_id,
            root,
            height: AtomicUsize::new(0),
            pool,
            insert_free_list: Mutex::new(Vec::with_capacity(64)),
            table_file,
            page_committer: Mutex::new(None),
        }
    }

    /// Returns height of block index.
    #[inline]
    pub fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }

    /// Enable page committer by injecting transaction system for redo logging
    #[inline]
    pub fn enable_page_committer(&self, trx_sys: &'static TransactionSystem) {
        let mut g = self.page_committer.lock();
        *g = Some(RedoLogPageCommitter::new(trx_sys, self.table_id))
    }

    /// Returns true if page committer is enabled.
    #[inline]
    pub fn is_page_committer_enabled(&self) -> bool {
        self.page_committer.lock().is_some()
    }

    #[inline]
    pub async fn update_file_root(&self, active_root: &ActiveRoot) {
        let _g = self.root.latch.exclusive_async().await;
        unsafe {
            *self.root.pivot.get() = active_root.pivot_row_id;
        }
        self.root
            .file
            .store(active_root as *const _ as *mut _, Ordering::Release);
    }

    /// Get row page for insertion.
    /// Caller should cache insert page id to avoid invoking this method frequently.
    #[inline]
    pub async fn get_insert_page<P: BufferPool>(
        &self,
        buf_pool: &'static P,
        count: usize,
    ) -> PageSharedGuard<RowPage> {
        if let Ok(free_page) = self.get_insert_page_from_free_list(buf_pool).await {
            return free_page;
        }
        // we just ignore the free list error and latch error, and continue to get new page.
        let mut new_page = buf_pool.allocate_page::<RowPage>().await;
        self.insert_page_guard(count, &mut new_page).await;
        new_page.downgrade_shared()
    }

    /// Get exclusive row page for insertion.
    #[inline]
    pub async fn get_insert_page_exclusive<P: BufferPool>(
        &self,
        buf_pool: &'static P,
        count: usize,
    ) -> PageExclusiveGuard<RowPage> {
        if let Ok(free_page) = self
            .get_insert_page_exclusive_from_free_list(buf_pool)
            .await
        {
            return free_page;
        }
        // we just ignore the free list error and latch error, and continue to get new page.
        let mut new_page = buf_pool.allocate_page::<RowPage>().await;
        self.insert_page_guard(count, &mut new_page).await;
        new_page
    }

    /// Allocate a row page with given page id.
    /// This method is used for data recovery, which replay all commit logs including row page creation.
    #[inline]
    pub async fn allocate_row_page_at<P: BufferPool>(
        &self,
        buf_pool: &'static P,
        count: usize,
        page_id: PageID,
    ) -> PageExclusiveGuard<RowPage> {
        let mut new_page = buf_pool
            .allocate_page_at::<RowPage>(page_id)
            .await
            .expect("allocate page with specific page id failed");
        self.insert_page_guard(count, &mut new_page).await;
        new_page
    }

    #[inline]
    async fn insert_page_guard(&self, count: usize, new_page: &mut PageExclusiveGuard<RowPage>) {
        let new_page_id = new_page.page_id();
        let metadata = self.root.clone_metadata().unwrap();
        loop {
            match self.insert_row_page(count as u64, new_page_id).await {
                Invalid => (),
                Valid((start_row_id, end_row_id)) => {
                    // initialize row page.
                    debug_assert!(end_row_id == start_row_id + count as u64);
                    new_page.page_mut().init(start_row_id, count, &metadata);
                    // create and attach a new empty undo map.
                    new_page.bf_mut().init_undo_map(metadata, count);

                    // persist log to commit this page.
                    if let Some(page_committer) = {
                        let page_committer_guard = self.page_committer.lock();
                        page_committer_guard.as_ref().cloned()
                    } {
                        let create_cts =
                            page_committer.commit_row_page(new_page_id, start_row_id, end_row_id);
                        if let Some(row_ver) =
                            new_page.bf().ctx.as_ref().and_then(|ctx| ctx.row_ver())
                        {
                            row_ver.set_create_cts(create_cts);
                        }
                    }
                    // finally, we downgrade the page lock for shared mode.
                    return;
                }
            }
        }
    }

    /// Find location of given row id, maybe in column file or row page.
    #[inline]
    pub async fn find_row(&self, row_id: RowID) -> RowLocation {
        debug_assert!(!row_id.is_deleted());
        loop {
            let res = self.try_find_row(row_id).await;
            let res = verify_continue!(res);
            match res {
                RowLocation::NotFound => {
                    // If not found in row store, re-check if transfered
                    // to column store.
                    let file_root = match self.root.try_file(row_id) {
                        Some(root) => root,
                        None => return RowLocation::NotFound,
                    };
                    match find_in_file(&self.table_file, file_root, row_id).await {
                        Ok(Some(payload)) => {
                            return RowLocation::LwcPage(payload.block_id as PageID);
                        }
                        Ok(None) | Err(_) => return RowLocation::NotFound,
                    }
                }
                found => return found,
            }
        }
    }

    /// Put given page into insert free list.
    #[inline]
    pub fn cache_exclusive_insert_page(&self, guard: PageExclusiveGuard<RowPage>) {
        let page_id = guard.page_id();
        drop(guard);
        let mut free_list = self.insert_free_list.lock();
        free_list.push(page_id);
    }

    /// Returns the cursor for range scan.
    #[inline]
    pub fn mem_cursor(&self) -> BlockIndexMemCursor<'_> {
        BlockIndexMemCursor {
            blk_idx: self,
            parent: None,
            child: None,
        }
    }

    #[inline]
    async fn get_insert_page_from_free_list<P: BufferPool>(
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
            .lock_shared_async()
            .await
            .unwrap();
        Ok(page_guard)
    }

    #[inline]
    async fn get_insert_page_exclusive_from_free_list<P: BufferPool>(
        &self,
        buf_pool: &'static P,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        let page_id = {
            let mut g = self.insert_free_list.lock();
            if g.is_empty() {
                return Err(Error::EmptyFreeListOfBufferPool);
            }
            g.pop().unwrap()
        };
        let page_guard = buf_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
            .await
            .lock_exclusive_async()
            .await
            .unwrap();
        Ok(page_guard)
    }

    /// Insert row page by splitting root.
    #[inline]
    async fn insert_row_page_split_root(
        &self,
        mut p_guard: PageExclusiveGuard<BlockNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> (RowID, RowID) {
        debug_assert!(p_guard.page_id() == self.root.mem);
        debug_assert!({
            let p = p_guard.page();
            (p.is_leaf() && p.leaf_is_full()) || (p.is_branch() && p.branch_is_full())
        });
        let root = p_guard.page();
        let new_height = root.header.height + 1;
        let l_row_id = root.header.start_row_id;
        let r_row_id = row_id;
        let max_row_id = r_row_id + count;

        // create left child and copy all contents to it.
        let mut l_guard = self.pool.allocate_page::<BlockNode>().await;
        let l_page_id = l_guard.page_id();
        l_guard.page_mut().clone_from(p_guard.page());
        l_guard.page_mut().header.end_row_id = row_id; // update original page's end row id
        drop(l_guard);

        // We may need to create a sub-tree on the right side.
        // Because we disable branch split. We have to always construct right tree
        // as same height as left.
        let r_page_id = if root.header.height == 0 {
            let mut r_guard = self.pool.allocate_page::<BlockNode>().await;
            r_guard.page_mut().init(0, r_row_id, count, insert_page_id);
            r_guard.page_id()
        } else {
            self.create_sub_tree(root.header.height, r_row_id, count, insert_page_id)
                .await
        };

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
            self.height.store(new_height as usize, Ordering::Relaxed);
        }
        (r_row_id, max_row_id)
    }

    #[inline]
    async fn create_sub_tree(
        &self,
        mut height: u32,
        start_row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> PageID {
        debug_assert!(height > 0);
        let mut p_g = self.pool.allocate_page::<BlockNode>().await;
        let page_id = p_g.page_id();
        p_g.page_mut().init_empty(height, start_row_id);
        loop {
            height -= 1;
            let mut c_g = self.pool.allocate_page::<BlockNode>().await;
            let c_page_id = c_g.page_id();
            p_g.page_mut().branch_add_entry(PageEntry {
                row_id: start_row_id,
                page_id: c_page_id,
            });
            if height == 0 {
                // insert row page to leaf.
                c_g.page_mut()
                    .init(height, start_row_id, count, insert_page_id);
                break;
            }
            // initialize empty node.
            c_g.page_mut().init_empty(height, start_row_id);
            p_g = c_g;
        }
        page_id
    }

    #[inline]
    async fn insert_row_page_to_new_leaf(
        &self,
        stack: &mut Vec<PageOptimisticGuard<BlockNode>>,
        c_guard: PageExclusiveGuard<BlockNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> Validation<(RowID, RowID)> {
        debug_assert!(!stack.is_empty());
        let mut p_guard;
        // Block index is a special type of B+ tree, which does not implement
        // branch node split.
        // Split is only applied to root node. This is because block index is
        // an append-only index, no random access is allowed (except the merge
        // of multiple leaf nodes, but not implemented yet). So we prefer to
        // always hold full branch node.
        // That means we may have a block index of depth a little larger than a
        // normal B+ tree.
        loop {
            // try to lock parent.
            let g = stack.pop().unwrap();
            // if lock failed, just retry the whole process.
            p_guard = verify!(g.try_exclusive());
            if !p_guard.page().branch_is_full() {
                break;
            } else if stack.is_empty() {
                // root is full, should split.
                let res = self
                    .insert_row_page_split_root(p_guard, row_id, count, insert_page_id)
                    .await;
                return Valid(res);
            } // do not split branch node.
        }
        // create new leaf node with one insert page id
        // or subtree containing only insert page id.
        let p_height = p_guard.page().header.height;
        debug_assert!(p_height >= 1);
        let c_page_id = if p_height == 1 {
            let mut leaf = self.pool.allocate_page::<BlockNode>().await;
            leaf.page_mut().init(0, row_id, count, insert_page_id);
            debug_assert!(leaf.page_mut().header.end_row_id == row_id + count);
            leaf.page_id()
        } else {
            self.create_sub_tree(p_height - 1, row_id, count, insert_page_id)
                .await
        };
        p_guard
            .page_mut()
            .branch_add_entry(PageEntry::new(row_id, c_page_id));
        drop(c_guard);
        Valid((row_id, row_id + count))
    }

    /// Insert row page id into block index.
    #[inline]
    async fn insert_row_page(
        &self,
        count: u64,
        insert_page_id: PageID,
    ) -> Validation<(RowID, RowID)> {
        // Stack holds the path from root to leaf.
        let mut stack = vec![];
        let mut p_guard = {
            let g = self
                .find_right_most_leaf(&mut stack, LatchFallbackMode::Exclusive)
                .await;
            let mut guard = verify!(g);
            verify!(guard.try_exclusive());
            guard.must_exclusive()
        };
        debug_assert!(p_guard.page().is_leaf());
        if p_guard.page().leaf_is_empty() {
            let start_row_id = p_guard.page().header.start_row_id;
            p_guard
                .page_mut()
                .leaf_add_entry(start_row_id, count, insert_page_id);
            return Valid((start_row_id, start_row_id + count));
        }
        let end_row_id = p_guard.page().header.end_row_id;
        if p_guard.page().leaf_is_full() {
            // leaf is full, we must add new leaf to block index
            if stack.is_empty() {
                // root is full and already exclusive locked
                let res = self
                    .insert_row_page_split_root(p_guard, end_row_id, count, insert_page_id)
                    .await;
                return Valid(res);
            }
            return self
                .insert_row_page_to_new_leaf(&mut stack, p_guard, end_row_id, count, insert_page_id)
                .await;
        }
        p_guard
            .page_mut()
            .leaf_add_entry(end_row_id, count, insert_page_id);
        Valid((end_row_id, end_row_id + count))
    }

    #[inline]
    async fn find_right_most_leaf(
        &self,
        stack: &mut Vec<PageOptimisticGuard<BlockNode>>,
        mode: LatchFallbackMode,
    ) -> Validation<FacadePageGuard<BlockNode>> {
        let mut p_guard = self
            .pool
            .get_page::<BlockNode>(self.root.mem, LatchFallbackMode::Spin)
            .await;
        loop {
            let step = verify!(p_guard.with_page_ref_validated(|page| {
                if page.is_leaf() {
                    return None;
                }
                let count = page.header.count;
                let idx = 1.max(count as usize).min(NBR_ENTRIES_IN_BRANCH) - 1;
                Some((page.header.height, page.branch_entries()[idx].page_id))
            }));
            let Some((height, page_id)) = step else {
                return Valid(p_guard);
            };
            debug_assert!(height >= 1);
            let c_guard = if height == 1 {
                self.pool
                    .get_child_page::<BlockNode>(&p_guard, page_id, mode)
                    .await
            } else {
                self.pool
                    .get_child_page::<BlockNode>(&p_guard, page_id, LatchFallbackMode::Spin)
                    .await
            };
            stack.push(p_guard.downgrade());
            p_guard = verify!(c_guard);
        }
    }

    #[inline]
    async fn try_find_row(&self, row_id: RowID) -> Validation<RowLocation> {
        enum SearchStep {
            Found(RowLocation),
            Next(PageID),
        }

        let root = match self.root.guide(row_id) {
            Left(file_root) => {
                let payload = match find_in_file(&self.table_file, file_root, row_id).await {
                    Ok(payload) => payload,
                    Err(_) => return Valid(RowLocation::NotFound),
                };
                return Valid(match payload {
                    Some(payload) => RowLocation::LwcPage(payload.block_id as PageID),
                    None => RowLocation::NotFound,
                });
            }
            Right(mem) => mem,
        };
        let mut g = self
            .pool
            .get_page::<BlockNode>(root, LatchFallbackMode::Spin)
            .await;
        loop {
            let step = verify!(g.with_page_ref_validated(|page| {
                if page.is_leaf() {
                    // for leaf node, end_row_id is always correct,
                    // so we can quickly determine if row id exists
                    // in current node.
                    if page.leaf_is_empty() || row_id >= page.header.end_row_id {
                        return SearchStep::Found(RowLocation::NotFound);
                    }
                    let entries = page.leaf_entries();
                    let idx = match entries.binary_search_by_key(&row_id, |entry| entry.row_id) {
                        Ok(idx) => idx,
                        Err(0) => return SearchStep::Found(RowLocation::NotFound),
                        Err(idx) => idx - 1,
                    };
                    let entry_end_row_id = entries
                        .get(idx + 1)
                        .map(|entry| entry.row_id)
                        .unwrap_or(page.header.end_row_id);
                    if row_id >= entry_end_row_id {
                        return SearchStep::Found(RowLocation::NotFound);
                    }
                    return SearchStep::Found(RowLocation::RowPage(entries[idx].page_id));
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
                let page_id = if row_id >= page.header.end_row_id {
                    page.branch_last_entry().page_id
                } else {
                    let entries = page.branch_entries();
                    let idx = match entries.binary_search_by_key(&row_id, |entry| entry.row_id) {
                        Ok(idx) => idx,
                        Err(0) => return SearchStep::Found(RowLocation::NotFound),
                        Err(idx) => idx - 1,
                    };
                    entries[idx].page_id
                };
                SearchStep::Next(page_id)
            }));
            match step {
                SearchStep::Found(found) => return Valid(found),
                SearchStep::Next(page_id) => {
                    g = verify!(
                        self.pool
                            .get_child_page(&g, page_id, LatchFallbackMode::Spin)
                            .await
                    );
                }
            }
        }
    }
}

unsafe impl Send for BlockIndex {}
unsafe impl Sync for BlockIndex {}

pub struct BlockIndexRoot {
    /// Root of in-memory rows.
    mem: PageID,
    /// latch to protect below fields.
    latch: HybridLatch,
    /// minimum row id of row pages.
    /// If row id is less than this value, it
    /// goes to column store.
    pivot: UnsafeCell<RowID>,
    /// Root of in-file rows.
    /// This can be changed if compaction (conversion
    /// from row to col) happens.
    file: AtomicPtr<ActiveRoot>,
}

impl BlockIndexRoot {
    /// Create a block index root.
    #[inline]
    pub fn new(mem: PageID, pivot: RowID, file: AtomicPtr<ActiveRoot>) -> Self {
        BlockIndexRoot {
            mem,
            latch: HybridLatch::new(),
            pivot: UnsafeCell::new(pivot),
            file,
        }
    }

    /// Guide the search path of given row id.
    /// The search path can be determined by comparison
    /// to pivot row id.
    #[inline]
    pub fn guide(&self, row_id: RowID) -> Either<PageID, PageID> {
        loop {
            let g = self.optimistic_guard();
            let pivot = self.pivot_value();
            if row_id < pivot {
                // go to file
                let file_root = self.file_root_ptr(Ordering::Acquire);
                if g.validate() {
                    return Left(Self::column_block_index_root(file_root));
                }
                continue; // retry
            }
            // go to mem
            if g.validate() {
                return Right(self.mem);
            }
        }
    }

    /// Try to serach row id in file if row id within boundary.
    #[inline]
    pub fn try_file(&self, row_id: RowID) -> Option<PageID> {
        loop {
            let g = self.optimistic_guard();
            let pivot = self.pivot_value();
            if row_id < pivot {
                // go to file
                let file_root = self.file_root_ptr(Ordering::Acquire);
                if g.validate() {
                    return Some(Self::column_block_index_root(file_root));
                }
                continue; // retry
            }
            return None;
        }
    }

    #[inline]
    pub fn metadata(&self) -> Option<&TableMetadata> {
        self.file_root_ref(Ordering::Relaxed)
            .map(|root| root.metadata.as_ref())
    }

    #[inline]
    pub fn clone_metadata(&self) -> Option<Arc<TableMetadata>> {
        self.file_root_ref(Ordering::Relaxed)
            .map(|root| Arc::clone(&root.metadata))
    }

    #[inline]
    fn optimistic_guard(&self) -> HybridGuard<'_> {
        self.latch.optimistic_spin()
    }

    #[inline]
    fn pivot_value(&self) -> RowID {
        // SAFETY: pivot reads are validated by latch optimistic version checks.
        unsafe { *self.pivot.get() }
    }

    #[inline]
    fn file_root_ptr(&self, ordering: Ordering) -> *const ActiveRoot {
        self.file.load(ordering) as *const ActiveRoot
    }

    #[inline]
    fn file_root_ref(&self, ordering: Ordering) -> Option<&ActiveRoot> {
        let ptr = self.file_root_ptr(ordering);
        if ptr.is_null() {
            return None;
        }
        // SAFETY: pointer is published by table-file root management and stays
        // valid while referenced by block index root.
        Some(unsafe { &*ptr })
    }

    #[inline]
    fn column_block_index_root(ptr: *const ActiveRoot) -> PageID {
        debug_assert!(!ptr.is_null());
        // SAFETY: caller ensures `ptr` was loaded from `self.file` and validated.
        unsafe { (*ptr).column_block_index_root }
    }
}

pub enum RowLocation {
    // Lightweight columnar page.
    LwcPage(PageID),
    // Row page.
    RowPage(PageID),
    NotFound,
}

/// A cursor to read all in-mem leaf values.
pub struct BlockIndexMemCursor<'a> {
    blk_idx: &'a BlockIndex,
    // The parent node of current located
    parent: Option<ParentPosition<PageSharedGuard<BlockNode>>>,
    child: Option<PageSharedGuard<BlockNode>>,
}

impl BlockIndexMemCursor<'_> {
    #[inline]
    pub async fn seek(&mut self, row_id: RowID) {
        loop {
            self.reset();
            let res = self.try_find_leaf_with_parent_in_mem(row_id).await;
            verify_continue!(res);
            return;
        }
    }

    #[inline]
    pub async fn next(&mut self) -> Option<FacadePageGuard<BlockNode>> {
        if let Some(child) = self.child.take() {
            return Some(child.facade(false));
        }
        if let Some(parent) = self.parent.as_ref() {
            let page = parent.g.page();
            let entries = page.branch_entries();
            let next_idx = (parent.idx + 1) as usize;
            if next_idx == entries.len() {
                // current parent is exhausted.
                let row_id = page.header.end_row_id;
                self.parent.take();
                if row_id == INVALID_ROW_ID {
                    // the traverse is done.
                    return None;
                }
                // otherwise, we rerun the search on given row id to get next leaf.
                while let Invalid = self.try_find_leaf_with_parent_in_mem(row_id).await {
                    self.reset();
                }
                let child = self.child.take().unwrap();
                return Some(child.facade(false));
            }
            // otherwise, we jump to next slot and get leaf node.
            let page_id = entries[next_idx].page_id;
            self.parent.as_mut().unwrap().idx = next_idx as isize; // update parent position.
            let child = self
                .blk_idx
                .pool
                .get_page::<BlockNode>(page_id, LatchFallbackMode::Shared)
                .await
                .lock_shared_async()
                .await
                .unwrap();
            return Some(child.facade(false));
        }
        None
    }

    #[inline]
    fn reset(&mut self) {
        self.parent.take();
        self.child.take();
    }

    #[inline]
    async fn try_find_leaf_with_parent_in_mem(&mut self, row_id: RowID) -> Validation<()> {
        enum TraverseStep {
            Leaf,
            Branch { idx: usize, page_id: PageID },
        }

        debug_assert!(row_id != INVALID_ROW_ID); // every row id other than MAX_ROW_ID can find a leaf.
        let mut g = self
            .blk_idx
            .pool
            .get_page::<BlockNode>(self.blk_idx.root.mem, LatchFallbackMode::Shared)
            .await;
        'SEARCH: loop {
            let step = verify!(g.with_page_ref_validated(|page| {
                if page.is_leaf() {
                    return TraverseStep::Leaf;
                }
                let entries = page.branch_entries();
                let idx = match entries.binary_search_by_key(&row_id, |block| block.row_id) {
                    Ok(idx) => idx,
                    Err(0) => 0, // even it's out of range, we assign first page.
                    Err(idx) => idx - 1,
                };
                TraverseStep::Branch {
                    idx,
                    page_id: entries[idx].page_id,
                }
            }));
            if matches!(step, TraverseStep::Leaf) {
                // share lock for read
                if let Some(parent) = self.parent.take() {
                    self.parent = Some(parent);
                }
                match g.try_shared_either() {
                    Left(c) => {
                        // share lock on child succeeds
                        self.child = Some(c);
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
            let TraverseStep::Branch { idx, page_id } = step else {
                unreachable!();
            };
            let c = self
                .blk_idx
                .pool
                .get_child_page::<BlockNode>(&g, page_id, LatchFallbackMode::Spin)
                .await;
            let c = verify!(c);
            let Some(parent_g) = g.lock_shared_async().await else {
                return Invalid;
            };
            self.parent = Some(ParentPosition {
                g: parent_g,
                idx: idx as isize,
            });
            g = c;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::EvictableBufferPoolConfig;
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::engine::EngineConfig;
    use crate::file::table_fs::TableFileSystemConfig;
    use crate::lifetime::StaticLifetime;
    use crate::trx::sys_conf::TrxSysConfig;
    use crate::value::ValKind;
    use semistr::SemiStr;
    use tempfile::TempDir;

    #[test]
    fn test_block_index_free_list() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("redo_bi")
                        .skip_recovery(true),
                )
                .build()
                .await
                .unwrap();
            {
                let metadata = Arc::new(TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                ));
                let table_id = 101;
                let uninit_table_file = engine
                    .table_fs
                    .create_table_file(table_id, metadata, true)
                    .unwrap();
                let (table_file, _) = uninit_table_file.commit(1, false).await.unwrap();
                let blk_idx = BlockIndex::new(
                    engine.meta_pool,
                    table_id,
                    0,
                    table_file.active_root_ptr(),
                    Arc::clone(&table_file),
                )
                .await;
                let p1 = blk_idx.get_insert_page(engine.data_pool, 100).await;
                let pid1 = p1.page_id();
                let p1 = p1.downgrade().exclusive_async().await;
                blk_idx.cache_exclusive_insert_page(p1);
                assert!(blk_idx.insert_free_list.lock().len() == 1);
                let p2 = blk_idx.get_insert_page(engine.data_pool, 100).await;
                assert!(pid1 == p2.page_id());
                assert!(blk_idx.insert_free_list.lock().is_empty());
            }
            drop(engine);
        })
    }

    #[test]
    fn test_block_index_insert_row_page() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("redo_bi")
                        .skip_recovery(true),
                )
                .build()
                .await
                .unwrap();
            {
                let metadata = Arc::new(TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                ));
                let table_id = 101;
                let uninit_table_file = engine
                    .table_fs
                    .create_table_file(table_id, metadata, true)
                    .unwrap();
                let (table_file, _) = uninit_table_file.commit(1, false).await.unwrap();
                let blk_idx = BlockIndex::new(
                    engine.meta_pool,
                    table_id,
                    0,
                    table_file.active_root_ptr(),
                    Arc::clone(&table_file),
                )
                .await;
                let p1 = blk_idx.get_insert_page(engine.data_pool, 100).await;
                let pid1 = p1.page_id();
                let p1 = p1.downgrade().exclusive_async().await;
                blk_idx.cache_exclusive_insert_page(p1);
                assert!(blk_idx.insert_free_list.lock().len() == 1);
                let p2 = blk_idx.get_insert_page(engine.data_pool, 100).await;
                assert!(pid1 == p2.page_id());
                assert!(blk_idx.insert_free_list.lock().is_empty());
            }
            drop(engine);
        })
    }

    #[test]
    fn test_block_index_cursor_shared() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let row_pages = 1024usize;
            // allocate 100MB buffer pool is enough: 1024 pages ~= 64MB
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(100usize * 1024 * 1024)
                        .max_file_size(1usize * 1024 * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("redo_bi")
                        .skip_recovery(true),
                )
                .build()
                .await
                .unwrap();
            {
                let metadata = Arc::new(TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                ));
                let table_id = 101;
                let uninit_table_file = engine
                    .table_fs
                    .create_table_file(table_id, metadata, true)
                    .unwrap();
                let (table_file, _) = uninit_table_file.commit(1, false).await.unwrap();
                let blk_idx = BlockIndex::new(
                    engine.meta_pool,
                    table_id,
                    0,
                    table_file.active_root_ptr(),
                    Arc::clone(&table_file),
                )
                .await;
                for _ in 0..row_pages {
                    let _ = blk_idx.get_insert_page(engine.data_pool, 100).await;
                }
                let mut count = 0usize;
                let mut cursor = blk_idx.mem_cursor();
                cursor.seek(0).await;
                while let Some(res) = cursor.next().await {
                    count += 1;
                    let g = res.try_into_shared().unwrap();
                    let node = g.page();
                    assert!(node.is_leaf());
                    let row_pages: usize = node.leaf_entries().len();
                    println!(
                        "start_row_id={:?}, end_row_id={:?}, entries={:?}, row_pages={:?}",
                        node.header.start_row_id,
                        node.header.end_row_id,
                        node.header.count,
                        row_pages
                    );
                }
                let row_pages_per_leaf = NBR_PAGE_ENTRIES_IN_LEAF;
                assert!(count == (row_pages + row_pages_per_leaf - 1) / row_pages_per_leaf);
            }
            drop(engine);
        })
    }

    fn first_i32_unique_index() -> IndexSpec {
        IndexSpec::new("idx_id", vec![IndexKey::new(0)], IndexAttributes::UK)
    }

    #[test]
    fn test_block_index_search() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            // at least we need a two-level block index to search.
            // todo: add test hook to avoid expensive memory allocation of row pages.
            let row_pages = 5000usize;
            let rows_per_page = 100usize;
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(100usize * 1024 * 1024)
                        .max_file_size(2usize * 1024 * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("redo_bi")
                        .skip_recovery(true),
                )
                .build()
                .await
                .unwrap();
            {
                let metadata = Arc::new(TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                ));
                let table_id = 101;
                let uninit_table_file = engine
                    .table_fs
                    .create_table_file(table_id, metadata, true)
                    .unwrap();
                let (table_file, _) = uninit_table_file.commit(1, false).await.unwrap();
                let blk_idx = BlockIndex::new(
                    engine.meta_pool,
                    table_id,
                    0,
                    table_file.active_root_ptr(),
                    Arc::clone(&table_file),
                )
                .await;
                for _ in 0..row_pages {
                    let _ = blk_idx
                        .get_insert_page(engine.data_pool, rows_per_page)
                        .await;
                }
                {
                    let res = engine
                        .meta_pool
                        .get_page::<BlockNode>(blk_idx.root.mem, LatchFallbackMode::Spin)
                        .await;
                    let p = res.lock_shared_async().await.unwrap();
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
                    let res = blk_idx.find_row(row_id).await;
                    match res {
                        RowLocation::RowPage(page_id) => {
                            let g = engine
                                .data_pool
                                .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                                .await;
                            let g = g.lock_shared_async().await.unwrap();
                            let p = g.page();
                            assert!(p.header.start_row_id as usize == i * rows_per_page);
                        }
                        _ => panic!("invalid search result for i={:?}", i),
                    }
                }
            }
            drop(engine);
        })
    }

    #[test]
    fn test_block_index_log() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let rows_per_page = 100;
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("redo_bi")
                        .skip_recovery(true),
                )
                .build()
                .await
                .unwrap();
            {
                let metadata = Arc::new(TableMetadata::new(
                    vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: ValKind::I32,
                        column_attributes: ColumnAttributes::empty(),
                    }],
                    vec![first_i32_unique_index()],
                ));
                let table_id = 101;
                let uninit_table_file = engine
                    .table_fs
                    .create_table_file(table_id, metadata, true)
                    .unwrap();
                let (table_file, _) = uninit_table_file.commit(1, false).await.unwrap();
                let blk_idx = BlockIndex::new(
                    engine.meta_pool,
                    table_id,
                    0,
                    table_file.active_root_ptr(),
                    Arc::clone(&table_file),
                )
                .await;
                // create a new page for rowid=0..100
                let _ = blk_idx
                    .get_insert_page(engine.data_pool, rows_per_page)
                    .await;
                // todo: analyze log to see the log is persisted.
            }
            drop(engine);
        })
    }

    #[test]
    fn test_block_index_split() {
        smol::block_on(async {
            let pool = FixedBufferPool::with_capacity_static(1024usize * 1024 * 1024).unwrap();
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let metadata = Arc::new(TableMetadata::new(
                vec![ColumnSpec {
                    column_name: SemiStr::new("id"),
                    column_type: ValKind::I32,
                    column_attributes: ColumnAttributes::empty(),
                }],
                vec![first_i32_unique_index()],
            ));
            let uninit_table_file = fs.create_table_file(1, metadata, true).unwrap();
            let (table_file, _) = uninit_table_file.commit(1, false).await.unwrap();
            {
                let blk_idx = BlockIndex::new(
                    pool,
                    1,
                    0,
                    table_file.active_root_ptr(),
                    Arc::clone(&table_file),
                )
                .await;
                assert!(!blk_idx.is_page_committer_enabled());
                assert!(blk_idx.height() == 0);
                for row_page_id in 0..10000 {
                    blk_idx.insert_row_page(100, row_page_id).await;
                }
                assert!(blk_idx.height() == 1);
                let mut root = pool
                    .get_page_spin::<BlockNode>(blk_idx.root.mem)
                    .lock_exclusive_async()
                    .await
                    .unwrap();
                // mark root as full to trigger split.
                root.page_mut().header.count = NBR_ENTRIES_IN_BRANCH as u32;
                // assign right-most leaf node
                let mut r_g = pool.allocate_page::<BlockNode>().await;
                r_g.page_mut().init(0, 50000, 10000, 10001);
                r_g.page_mut().header.count = NBR_PAGE_ENTRIES_IN_LEAF as u32;
                let r_page_id = r_g.page_id();
                drop(r_g);
                root.page_mut().branch_last_entry_mut().row_id = 50000;
                root.page_mut().branch_last_entry_mut().page_id = r_page_id;
                drop(root);
                blk_idx.insert_row_page(100, 20000).await;
                assert!(blk_idx.height() == 2);
            }
            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }
}
