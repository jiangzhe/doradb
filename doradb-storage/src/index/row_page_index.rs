use crate::buffer::guard::{
    FacadePageGuard, PageExclusiveGuard, PageGuard, PageOptimisticGuard, PageSharedGuard,
};
use crate::buffer::page::{BufferPage, PAGE_SIZE};
use crate::buffer::{BufferPool, FixedBufferPool, PageID, PoolGuard};
use crate::catalog::TableMetadata;
use crate::error::{
    Result, Validation,
    Validation::{Invalid, Valid},
};
use crate::file::BlockID;
use crate::index::util::{Maskable, ParentPosition, RowPageCreateRedoCtx};
use crate::latch::LatchFallbackMode;
use crate::quiescent::QuiescentGuard;
use crate::row::{INVALID_ROW_ID, RowID, RowPage};
use bytemuck::{Pod, Zeroable, cast_slice, cast_slice_mut};
use either::Either::{Left, Right};
use parking_lot::Mutex;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const ROW_PAGE_INDEX_NODE_SIZE: usize = PAGE_SIZE;
pub const ROW_PAGE_INDEX_NODE_HEADER_SIZE: usize = mem::size_of::<RowPageIndexNodeHeader>();
pub const NBR_ENTRIES_IN_BRANCH: usize = 4093;
pub const ENTRY_SIZE: usize = mem::size_of::<PageEntry>();
pub const NBR_ROW_PAGE_ENTRIES_IN_LEAF: usize =
    (ROW_PAGE_INDEX_NODE_SIZE - ROW_PAGE_INDEX_NODE_HEADER_SIZE) / ENTRY_SIZE;

const _: () = assert!(
    { mem::size_of::<RowPageIndexNode>() == ROW_PAGE_INDEX_NODE_SIZE },
    "Size of node of BlockIndex should equal to 64KB"
);

const _: () = assert!(
    {
        ROW_PAGE_INDEX_NODE_HEADER_SIZE + NBR_ENTRIES_IN_BRANCH * ENTRY_SIZE
            <= ROW_PAGE_INDEX_NODE_SIZE
    },
    "Size of branch node of BlockIndex can be at most 64KB"
);

const _: () = assert!(
    {
        ROW_PAGE_INDEX_NODE_HEADER_SIZE + NBR_ROW_PAGE_ENTRIES_IN_LEAF * ENTRY_SIZE
            <= ROW_PAGE_INDEX_NODE_SIZE
    },
    "Size of leaf node of BlockIndex can be at most 64KB"
);

const _: () = assert!(
    { ROW_PAGE_INDEX_NODE_HEADER_SIZE.is_multiple_of(mem::align_of::<PageEntry>()) },
    "RowPageIndexNode data area must align with PageEntry alignment"
);

const _: () = assert!(
    { mem::size_of::<PageEntry>() == ENTRY_SIZE },
    "ENTRY_SIZE must match PageEntry size"
);

enum InsertFreeListProbe<T> {
    Hit(T),
    Miss(InsertFreeListMiss),
}

enum InsertFreeListMiss {
    Empty,
    BufferPageGenerationMismatch,
}

/// RowPageIndexNode is one B-tree node of the row-page index.
/// It can be either branch or leaf.
/// Branch contains at most 4093 child node pointers.
/// Leaf contains at most NBR_ROW_PAGE_ENTRIES_IN_LEAF page entries.
#[repr(C)]
#[derive(Clone)]
pub struct RowPageIndexNode {
    pub header: RowPageIndexNodeHeader,
    data: [u8; ROW_PAGE_INDEX_NODE_SIZE - ROW_PAGE_INDEX_NODE_HEADER_SIZE],
}

impl RowPageIndexNode {
    /// Initializes one node with the given height and starting row id.
    /// End row id is set to MAX_ROW_ID.
    #[inline]
    pub fn init(&mut self, height: u32, start_row_id: RowID, count: u64, insert_page_id: PageID) {
        self.header.height = height;
        self.header.start_row_id = start_row_id;
        self.header.end_row_id = INVALID_ROW_ID;
        self.header.count = 0;
        self.leaf_add_entry(start_row_id, count, insert_page_id);
    }

    /// Initialize an empty node without entries.
    #[inline]
    pub fn init_empty(&mut self, height: u32, start_row_id: RowID) {
        self.header.height = height;
        self.header.start_row_id = start_row_id;
        self.header.end_row_id = INVALID_ROW_ID;
        self.header.count = 0;
    }

    /// Returns whether the node is leaf.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.header.height == 0
    }

    /// Returns whether the node is branch.
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
        self.header.count as usize == NBR_ROW_PAGE_ENTRIES_IN_LEAF
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

impl BufferPage for RowPageIndexNode {}

#[repr(C)]
/// Header metadata for one in-memory row-page-index node.
#[derive(Clone)]
pub struct RowPageIndexNodeHeader {
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
/// Entry mapping a row-id boundary to a row-page or child node page id.
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct PageEntry {
    pub row_id: RowID,
    pub page_id: PageID,
}

impl PageEntry {
    /// Creates a page entry `(row_id -> page_id)`.
    #[inline]
    pub fn new(row_id: RowID, page_id: PageID) -> Self {
        PageEntry { row_id, page_id }
    }
}

/// In-memory index for runtime row pages.
///
/// The index is sorted by RowID, which is global unique identifier
/// of each row.
/// When inserting a new row, a row page must be located, either by
/// allocating a new page from buffer pool, or by reusing a non-full
/// page.
/// The row page determines its RowID range by increasing the max row id
/// of the row-page index with estimated row count.
///
/// Old rows have smaller RowID, new rows have bigger RowID.
/// Once all data in one row page can be seen by all active transactions,
/// it is qualified to be persisted to disk in column format.
///
/// Multiple row pages are merged to be a column file.
/// The row-page index will also be updated to reflect the change.
///
/// The row-page index supports two operations.
///
/// 1. index search with row id: determine which column file or row page
///    one RowID belongs to.
/// 2. table scan: traverse all column files and row pages to perform
///    full table scan.
///
pub struct GenericRowPageIndex<P: 'static> {
    root_page_id: PageID,
    height: AtomicUsize,
    insert_free_list: Mutex<Vec<PageID>>,
    // Fixed buffer pool to hold block nodes.
    pool: QuiescentGuard<P>,
}

/// Compatibility alias for the runtime row-page index backed by `FixedBufferPool`.
pub type RowPageIndex = GenericRowPageIndex<FixedBufferPool>;

impl<P: BufferPool> GenericRowPageIndex<P> {
    /// Creates a new row-page index backed by the buffer pool.
    #[inline]
    pub async fn new(
        pool: QuiescentGuard<P>,
        pool_guard: &PoolGuard,
        start_row_id: RowID,
    ) -> Result<Self> {
        let mut g = pool.allocate_page::<RowPageIndexNode>(pool_guard).await?;
        let page_id = g.page_id();
        let page = g.page_mut();
        page.init_empty(0, start_row_id);
        Ok(GenericRowPageIndex {
            root_page_id: page_id,
            height: AtomicUsize::new(0),
            pool,
            insert_free_list: Mutex::new(Vec::with_capacity(64)),
        })
    }

    #[inline]
    async fn allocate_node_page(
        &self,
        pool_guard: &PoolGuard,
    ) -> Result<PageExclusiveGuard<RowPageIndexNode>> {
        self.pool
            .allocate_page::<RowPageIndexNode>(pool_guard)
            .await
    }

    /// Returns the height of the row-page index.
    #[inline]
    pub fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }

    /// Returns root page id of in-memory row index.
    #[inline]
    pub fn root_page_id(&self) -> PageID {
        self.root_page_id
    }

    /// Get row page for insertion.
    /// Caller should cache insert page ids to avoid invoking this method frequently.
    #[inline]
    pub async fn get_insert_page<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
    ) -> PageSharedGuard<RowPage> {
        self.try_get_insert_page_with_redo(
            meta_pool_guard,
            mem_pool,
            mem_pool_guard,
            metadata,
            count,
            None,
        )
        .await
        .expect("row-page-index get_insert_page should not ignore row-page I/O failures")
    }

    #[cfg(test)]
    #[inline]
    pub(crate) async fn get_insert_page_with_redo<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> PageSharedGuard<RowPage> {
        self.try_get_insert_page_with_redo(
            meta_pool_guard,
            mem_pool,
            mem_pool_guard,
            metadata,
            count,
            redo_ctx,
        )
        .await
        .expect("row-page-index get_insert_page_with_redo should not ignore row-page I/O failures")
    }

    #[inline]
    pub(crate) async fn try_get_insert_page_with_redo<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageSharedGuard<RowPage>> {
        if let Some(free_page) = self
            .get_insert_page_from_free_list(mem_pool, mem_pool_guard)
            .await?
        {
            return Ok(free_page);
        }
        // Empty or stale free-list entries fall back to allocating a fresh row page.
        let mut new_page = mem_pool.allocate_page::<RowPage>(mem_pool_guard).await?;
        if let Err(err) = self
            .insert_page_guard(meta_pool_guard, metadata, count, redo_ctx, &mut new_page)
            .await
        {
            // The row page was allocated locally and never published, so reclaim it
            // before surfacing the metadata-side insertion failure.
            mem_pool.deallocate_page(new_page);
            return Err(err);
        }
        Ok(new_page.downgrade_shared())
    }

    /// Get exclusive row page for insertion.
    #[inline]
    pub async fn get_insert_page_exclusive<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        self.get_insert_page_exclusive_with_redo(
            meta_pool_guard,
            mem_pool,
            mem_pool_guard,
            metadata,
            count,
            None,
        )
        .await
    }

    #[inline]
    pub(crate) async fn get_insert_page_exclusive_with_redo<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        use self::InsertFreeListMiss::{BufferPageGenerationMismatch, Empty};
        use self::InsertFreeListProbe::{Hit, Miss};

        match self
            .get_insert_page_exclusive_from_free_list(mem_pool, mem_pool_guard)
            .await
        {
            Ok(Hit(free_page)) => return Ok(free_page),
            Ok(Miss(Empty | BufferPageGenerationMismatch)) => (),
            Err(err) => return Err(err),
        }
        // Empty or stale free-list entries fall back to allocating a fresh row page.
        let mut new_page = mem_pool.allocate_page::<RowPage>(mem_pool_guard).await?;
        if let Err(err) = self
            .insert_page_guard(meta_pool_guard, metadata, count, redo_ctx, &mut new_page)
            .await
        {
            mem_pool.deallocate_page(new_page);
            return Err(err);
        }
        Ok(new_page)
    }

    /// Allocate a row page with given page id.
    /// This method is used for data recovery, which replay all commit logs including row page creation.
    #[inline]
    pub async fn allocate_row_page_at<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        let mut new_page = mem_pool
            .allocate_page_at::<RowPage>(mem_pool_guard, page_id)
            .await?;
        if let Err(err) = self
            .insert_page_guard(meta_pool_guard, metadata, count, None, &mut new_page)
            .await
        {
            mem_pool.deallocate_page(new_page);
            return Err(err);
        }
        Ok(new_page)
    }

    #[inline]
    async fn insert_page_guard(
        &self,
        meta_pool_guard: &PoolGuard,
        metadata: &Arc<TableMetadata>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
        new_page: &mut PageExclusiveGuard<RowPage>,
    ) -> Result<()> {
        let new_page_id = new_page.page_id();
        let metadata = Arc::clone(metadata);
        loop {
            match self
                .insert_row_page(meta_pool_guard, count as u64, new_page_id)
                .await?
            {
                Invalid => (),
                Valid((start_row_id, end_row_id)) => {
                    // initialize row page.
                    debug_assert!(end_row_id == start_row_id + count as u64);
                    new_page.page_mut().init(start_row_id, count, &metadata);
                    // create and attach a new empty undo map.
                    new_page.bf_mut().init_undo_map(metadata, count);

                    if let Some(redo_ctx) = redo_ctx {
                        let create_cts =
                            redo_ctx.commit_row_page(new_page_id, start_row_id, end_row_id);
                        if let Some(row_ver) =
                            new_page.bf().ctx.as_ref().and_then(|ctx| ctx.row_ver())
                        {
                            row_ver.set_create_cts(create_cts);
                        }
                    }
                    // finally, we downgrade the page lock for shared mode.
                    return Ok(());
                }
            }
        }
    }

    /// Find location of given row id in in-memory row store.
    #[inline]
    pub async fn find_row(&self, pool_guard: &PoolGuard, row_id: RowID) -> RowLocation {
        debug_assert!(!row_id.is_deleted());
        loop {
            let res = self.try_find_row(pool_guard, row_id).await;
            let res = verify_continue!(res);
            return res;
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
    pub fn mem_cursor<'a>(
        &'a self,
        pool_guard: &'a PoolGuard,
    ) -> GenericRowPageIndexMemCursor<'a, P> {
        GenericRowPageIndexMemCursor {
            blk_idx: self,
            pool_guard,
            parent: None,
            child: None,
        }
    }

    #[inline]
    async fn get_insert_page_from_free_list<B: BufferPool>(
        &self,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        let page_id = {
            let mut g = self.insert_free_list.lock();
            if g.is_empty() {
                return Ok(None);
            }
            g.pop().unwrap()
        };
        let page_guard = mem_pool
            .get_page::<RowPage>(mem_pool_guard, page_id, LatchFallbackMode::Shared)
            .await?
            .lock_shared_async()
            .await;
        Ok(page_guard)
    }

    #[inline]
    async fn get_insert_page_exclusive_from_free_list<B: BufferPool>(
        &self,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
    ) -> Result<InsertFreeListProbe<PageExclusiveGuard<RowPage>>> {
        use self::InsertFreeListMiss::{BufferPageGenerationMismatch, Empty};
        use self::InsertFreeListProbe::{Hit, Miss};

        let page_id = {
            let mut g = self.insert_free_list.lock();
            if g.is_empty() {
                return Ok(Miss(Empty));
            }
            g.pop().unwrap()
        };
        let page_guard = mem_pool
            .get_page::<RowPage>(mem_pool_guard, page_id, LatchFallbackMode::Exclusive)
            .await?;
        let Some(page_guard) = page_guard.lock_exclusive_async().await else {
            return Ok(Miss(BufferPageGenerationMismatch));
        };
        Ok(Hit(page_guard))
    }

    /// Insert row page by splitting root.
    #[inline]
    async fn insert_row_page_split_root(
        &self,
        pool_guard: &PoolGuard,
        mut p_guard: PageExclusiveGuard<RowPageIndexNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> Result<(RowID, RowID)> {
        debug_assert!(p_guard.page_id() == self.root_page_id);
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
        let mut l_guard = self.allocate_node_page(pool_guard).await?;
        let l_page_id = l_guard.page_id();
        l_guard.page_mut().clone_from(p_guard.page());
        l_guard.page_mut().header.end_row_id = row_id; // update original page's end row id
        drop(l_guard);

        // We may need to create a sub-tree on the right side.
        // Because we disable branch split. We have to always construct right tree
        // as same height as left.
        let r_page_id = if root.header.height == 0 {
            let mut r_guard = self.allocate_node_page(pool_guard).await?;
            r_guard.page_mut().init(0, r_row_id, count, insert_page_id);
            r_guard.page_id()
        } else {
            self.create_sub_tree(
                pool_guard,
                root.header.height,
                r_row_id,
                count,
                insert_page_id,
            )
            .await?
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
        Ok((r_row_id, max_row_id))
    }

    #[inline]
    async fn create_sub_tree(
        &self,
        pool_guard: &PoolGuard,
        mut height: u32,
        start_row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> Result<PageID> {
        debug_assert!(height > 0);
        let mut p_g = self.allocate_node_page(pool_guard).await?;
        let page_id = p_g.page_id();
        p_g.page_mut().init_empty(height, start_row_id);
        loop {
            height -= 1;
            let mut c_g = self.allocate_node_page(pool_guard).await?;
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
        Ok(page_id)
    }

    #[inline]
    async fn insert_row_page_to_new_leaf(
        &self,
        pool_guard: &PoolGuard,
        stack: &mut Vec<PageOptimisticGuard<RowPageIndexNode>>,
        c_guard: PageExclusiveGuard<RowPageIndexNode>,
        row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) -> Result<Validation<(RowID, RowID)>> {
        debug_assert!(!stack.is_empty());
        let mut p_guard;
        // Block index is a special type of B+ tree, which does not implement
        // branch node split.
        // Split is only applied to root node. This is because the row-page index is
        // an append-only index, no random access is allowed (except the merge
        // of multiple leaf nodes, but not implemented yet). So we prefer to
        // always hold full branch node.
        // That means we may have a row-page index of depth a little larger than a
        // normal B+ tree.
        loop {
            // try to lock parent.
            let g = stack.pop().unwrap();
            // if lock failed, just retry the whole process.
            p_guard = match g.try_exclusive() {
                Valid(v) => v,
                Invalid => return Ok(Invalid),
            };
            if !p_guard.page().branch_is_full() {
                break;
            } else if stack.is_empty() {
                // root is full, should split.
                let res = self
                    .insert_row_page_split_root(pool_guard, p_guard, row_id, count, insert_page_id)
                    .await?;
                return Ok(Valid(res));
            } // do not split branch node.
        }
        // create new leaf node with one insert page id
        // or subtree containing only insert page id.
        let p_height = p_guard.page().header.height;
        debug_assert!(p_height >= 1);
        let c_page_id = if p_height == 1 {
            let mut leaf = self.allocate_node_page(pool_guard).await?;
            leaf.page_mut().init(0, row_id, count, insert_page_id);
            debug_assert!(leaf.page_mut().header.end_row_id == row_id + count);
            leaf.page_id()
        } else {
            self.create_sub_tree(pool_guard, p_height - 1, row_id, count, insert_page_id)
                .await?
        };
        p_guard
            .page_mut()
            .branch_add_entry(PageEntry::new(row_id, c_page_id));
        drop(c_guard);
        Ok(Valid((row_id, row_id + count)))
    }

    /// Inserts a row page id into the row-page index.
    #[inline]
    async fn insert_row_page(
        &self,
        pool_guard: &PoolGuard,
        count: u64,
        insert_page_id: PageID,
    ) -> Result<Validation<(RowID, RowID)>> {
        // Stack holds the path from root to leaf.
        let mut stack = vec![];
        let mut p_guard = {
            let g = self
                .find_right_most_leaf(pool_guard, &mut stack, LatchFallbackMode::Exclusive)
                .await;
            let mut guard = match g {
                Valid(v) => v,
                Invalid => return Ok(Invalid),
            };
            if guard.try_exclusive().is_invalid() {
                return Ok(Invalid);
            }
            guard.must_exclusive()
        };
        debug_assert!(p_guard.page().is_leaf());
        if p_guard.page().leaf_is_empty() {
            let start_row_id = p_guard.page().header.start_row_id;
            p_guard
                .page_mut()
                .leaf_add_entry(start_row_id, count, insert_page_id);
            return Ok(Valid((start_row_id, start_row_id + count)));
        }
        let end_row_id = p_guard.page().header.end_row_id;
        if p_guard.page().leaf_is_full() {
            // Leaf is full, so we must add a new leaf to the row-page index.
            if stack.is_empty() {
                // root is full and already exclusive locked
                let res = self
                    .insert_row_page_split_root(
                        pool_guard,
                        p_guard,
                        end_row_id,
                        count,
                        insert_page_id,
                    )
                    .await?;
                return Ok(Valid(res));
            }
            return self
                .insert_row_page_to_new_leaf(
                    pool_guard,
                    &mut stack,
                    p_guard,
                    end_row_id,
                    count,
                    insert_page_id,
                )
                .await;
        }
        p_guard
            .page_mut()
            .leaf_add_entry(end_row_id, count, insert_page_id);
        Ok(Valid((end_row_id, end_row_id + count)))
    }

    #[inline]
    async fn find_right_most_leaf(
        &self,
        pool_guard: &PoolGuard,
        stack: &mut Vec<PageOptimisticGuard<RowPageIndexNode>>,
        mode: LatchFallbackMode,
    ) -> Validation<FacadePageGuard<RowPageIndexNode>> {
        let mut p_guard = self
            .pool
            .get_page::<RowPageIndexNode>(pool_guard, self.root_page_id, LatchFallbackMode::Spin)
            .await
            .expect("row-page-index traversal should not ignore buffer-pool errors");
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
                    .get_child_page::<RowPageIndexNode>(pool_guard, &p_guard, page_id, mode)
                    .await
                    .expect("row-page-index traversal should not ignore buffer-pool errors")
            } else {
                self.pool
                    .get_child_page::<RowPageIndexNode>(
                        pool_guard,
                        &p_guard,
                        page_id,
                        LatchFallbackMode::Spin,
                    )
                    .await
                    .expect("row-page-index traversal should not ignore buffer-pool errors")
            };
            stack.push(p_guard.downgrade());
            p_guard = verify!(c_guard);
        }
    }

    #[inline]
    async fn try_find_row(&self, pool_guard: &PoolGuard, row_id: RowID) -> Validation<RowLocation> {
        enum SearchStep {
            Found(RowLocation),
            Next(PageID),
        }

        let mut g = self
            .pool
            .get_page::<RowPageIndexNode>(pool_guard, self.root_page_id, LatchFallbackMode::Spin)
            .await
            .expect("row-page-index lookup should not ignore buffer-pool errors");
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
                            .get_child_page(pool_guard, &g, page_id, LatchFallbackMode::Spin)
                            .await
                            .expect("row-page-index lookup should not ignore buffer-pool errors")
                    );
                }
            }
        }
    }
}

/// Physical lookup target returned by row/column block-index search.
pub enum RowLocation {
    /// Persisted lightweight columnar page plus the resolved row ordinal.
    LwcBlock {
        /// Persisted LWC block id.
        block_id: BlockID,
        /// Resolved row ordinal within the persisted LWC block.
        row_idx: usize,
        /// Canonical authoritative row-shape fingerprint bound to the block.
        row_shape_fingerprint: u128,
    },
    /// Row page.
    RowPage(PageID),
    /// Row id was not found in either route.
    NotFound,
}

/// A cursor to read all in-mem leaf values.
pub struct GenericRowPageIndexMemCursor<'a, P: 'static> {
    blk_idx: &'a GenericRowPageIndex<P>,
    pool_guard: &'a PoolGuard,
    // The parent node of current located
    parent: Option<ParentPosition<PageSharedGuard<RowPageIndexNode>>>,
    child: Option<PageSharedGuard<RowPageIndexNode>>,
}

/// Compatibility alias for the runtime row-page-index in-memory cursor.
pub type RowPageIndexMemCursor<'a> = GenericRowPageIndexMemCursor<'a, FixedBufferPool>;

impl<P: BufferPool> GenericRowPageIndexMemCursor<'_, P> {
    /// Seeks to the in-memory leaf that can serve `row_id`.
    #[inline]
    pub async fn seek(&mut self, row_id: RowID) {
        loop {
            self.reset();
            let res = self.try_find_leaf_with_parent_in_mem(row_id).await;
            verify_continue!(res);
            return;
        }
    }

    /// Returns current in-memory leaf and advances to the next leaf.
    #[inline]
    pub async fn next(&mut self) -> Option<FacadePageGuard<RowPageIndexNode>> {
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
                .get_page::<RowPageIndexNode>(self.pool_guard, page_id, LatchFallbackMode::Shared)
                .await
                .expect("row-page-index cursor should not ignore buffer-pool errors")
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
            .get_page::<RowPageIndexNode>(
                self.pool_guard,
                self.blk_idx.root_page_id,
                LatchFallbackMode::Shared,
            )
            .await
            .expect("row-page-index cursor should not ignore buffer-pool errors");
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
                .get_child_page::<RowPageIndexNode>(
                    self.pool_guard,
                    &g,
                    page_id,
                    LatchFallbackMode::Spin,
                )
                .await
                .expect("row-page-index cursor should not ignore buffer-pool errors");
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
    use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
    use crate::buffer::page::{BufferPage, VersionedPageID};
    use crate::buffer::test_page_id;
    use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard};
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::error::{ResourceError, Validation};
    use crate::latch::LatchFallbackMode;
    use crate::quiescent::{QuiescentBox, QuiescentGuard};
    use crate::trx::log::list_log_files;
    use crate::trx::log_replay::ReadLog;
    use crate::trx::redo::DDLRedo;
    use crate::value::ValKind;
    use semistr::SemiStr;
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;

    fn owned_index_pool(pool_size: usize) -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(
            FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, pool_size).unwrap(),
        )
    }

    fn owned_mem_pool(pool_size: usize) -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(
            FixedBufferPool::with_capacity(crate::buffer::PoolRole::Mem, pool_size).unwrap(),
        )
    }

    fn fixed_pool_bytes(page_count: usize) -> usize {
        page_count
            * (std::mem::size_of::<crate::buffer::frame::BufferFrame>()
                + std::mem::size_of::<crate::buffer::page::Page>())
    }

    async fn fill_root_leaf_full<P: BufferPool>(
        blk_idx: &GenericRowPageIndex<P>,
        pool_guard: &PoolGuard,
    ) {
        let mut root = blk_idx
            .pool
            .get_page::<RowPageIndexNode>(
                pool_guard,
                blk_idx.root_page_id(),
                LatchFallbackMode::Exclusive,
            )
            .await
            .expect("test row-page-index root read should succeed")
            .lock_exclusive_async()
            .await
            .unwrap();
        let root = root.page_mut();
        root.init_empty(0, 0);
        for i in 0..NBR_ROW_PAGE_ENTRIES_IN_LEAF {
            root.leaf_add_entry(i as RowID, 1, test_page_id(i as i32));
        }
    }

    struct FailingInsertPagePool {
        inner: QuiescentGuard<FixedBufferPool>,
        fail_page_id: AtomicU64,
    }

    impl FailingInsertPagePool {
        #[inline]
        fn new(inner: QuiescentGuard<FixedBufferPool>, fail_page_id: PageID) -> Self {
            Self {
                inner,
                fail_page_id: AtomicU64::new(u64::from(fail_page_id)),
            }
        }
    }

    impl BufferPool for FailingInsertPagePool {
        #[inline]
        fn capacity(&self) -> usize {
            self.inner.capacity()
        }

        #[inline]
        fn allocated(&self) -> usize {
            self.inner.allocated()
        }

        #[inline]
        fn pool_guard(&self) -> PoolGuard {
            self.inner.pool_guard()
        }

        #[inline]
        fn allocate_page<T: BufferPage>(
            &self,
            guard: &PoolGuard,
        ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send {
            self.inner.allocate_page(guard)
        }

        #[inline]
        fn allocate_page_at<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            page_id: PageID,
        ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send {
            self.inner.allocate_page_at(guard, page_id)
        }

        #[inline]
        async fn get_page<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            page_id: PageID,
            mode: LatchFallbackMode,
        ) -> Result<FacadePageGuard<T>> {
            if page_id == self.fail_page_id.load(Ordering::Acquire) {
                return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
            }
            self.inner.get_page(guard, page_id, mode).await
        }

        #[inline]
        fn get_page_versioned<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            id: VersionedPageID,
            mode: LatchFallbackMode,
        ) -> impl Future<Output = Result<Option<FacadePageGuard<T>>>> + Send {
            self.inner.get_page_versioned(guard, id, mode)
        }

        #[inline]
        fn deallocate_page<T: BufferPage>(&self, g: PageExclusiveGuard<T>) {
            self.inner.deallocate_page(g)
        }

        #[inline]
        fn get_child_page<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            p_guard: &FacadePageGuard<T>,
            page_id: PageID,
            mode: LatchFallbackMode,
        ) -> impl Future<Output = Result<Validation<FacadePageGuard<T>>>> + Send {
            self.inner.get_child_page(guard, p_guard, page_id, mode)
        }
    }

    #[test]
    fn test_row_page_index_free_list_shared() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(crate::buffer::PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_idx"))
                .build()
                .await
                .unwrap();
            {
                let metadata = make_test_metadata();
                let meta_guard = engine.meta_pool.pool_guard();
                let blk_idx = RowPageIndex::new(engine.meta_pool.clone_inner(), &meta_guard, 0)
                    .await
                    .expect("test row-page-index construction should succeed");
                let mem_guard = engine.mem_pool.pool_guard();
                let p1 = blk_idx
                    .get_insert_page(&meta_guard, &*engine.mem_pool, &mem_guard, &metadata, 100)
                    .await;
                let pid1 = p1.page_id();
                let p1 = p1.downgrade().exclusive_async().await;
                blk_idx.cache_exclusive_insert_page(p1);
                assert_eq!(blk_idx.insert_free_list.lock().len(), 1);
                let p2 = blk_idx
                    .get_insert_page(&meta_guard, &*engine.mem_pool, &mem_guard, &metadata, 100)
                    .await;
                assert_eq!(pid1, p2.page_id());
                assert!(blk_idx.insert_free_list.lock().is_empty());
            }
            drop(engine);
        })
    }

    #[test]
    fn test_row_page_index_free_list_exclusive() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(crate::buffer::PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_idx"))
                .build()
                .await
                .unwrap();
            {
                let metadata = make_test_metadata();
                let meta_guard = engine.meta_pool.pool_guard();
                let blk_idx = RowPageIndex::new(engine.meta_pool.clone_inner(), &meta_guard, 0)
                    .await
                    .expect("test row-page-index construction should succeed");
                let mem_guard = engine.mem_pool.pool_guard();
                let p1 = blk_idx
                    .get_insert_page_exclusive(
                        &meta_guard,
                        &*engine.mem_pool,
                        &mem_guard,
                        &metadata,
                        100,
                    )
                    .await
                    .expect("test insert-page allocation should succeed");
                let pid1 = p1.page_id();
                blk_idx.cache_exclusive_insert_page(p1);
                assert_eq!(blk_idx.insert_free_list.lock().len(), 1);
                let p2 = blk_idx
                    .get_insert_page_exclusive(
                        &meta_guard,
                        &*engine.mem_pool,
                        &mem_guard,
                        &metadata,
                        100,
                    )
                    .await
                    .expect("test insert-page allocation should succeed");
                assert_eq!(pid1, p2.page_id());
                assert!(blk_idx.insert_free_list.lock().is_empty());
            }
            drop(engine);
        })
    }

    #[test]
    fn test_try_get_insert_page_with_redo_returns_error_on_free_list_io_failure() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(64 * 1024 * 1024);
            let mem_pool = owned_mem_pool(64 * 1024 * 1024);
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, 0)
                .await
                .expect("test row-page-index construction should succeed");

            let page_guard = blk_idx
                .get_insert_page_exclusive(&meta_guard, &*mem_pool, &mem_guard, &metadata, 100)
                .await
                .expect("test insert-page allocation should succeed");
            let page_id = page_guard.page_id();
            blk_idx.cache_exclusive_insert_page(page_guard);

            let failing_pool = FailingInsertPagePool::new(mem_pool.guard(), page_id);
            let res = blk_idx
                .try_get_insert_page_with_redo(
                    &meta_guard,
                    &failing_pool,
                    &mem_guard,
                    &metadata,
                    100,
                    None,
                )
                .await;
            let err = match res {
                Ok(_) => panic!("expected free-list page reload failure"),
                Err(err) => err,
            };
            assert!(err.io_error().is_some());
        });
    }

    #[test]
    fn test_get_insert_page_exclusive_with_redo_propagates_free_list_io_failure() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(64 * 1024 * 1024);
            let mem_pool = owned_mem_pool(64 * 1024 * 1024);
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, 0)
                .await
                .expect("test row-page-index construction should succeed");

            let page_guard = blk_idx
                .get_insert_page_exclusive(&meta_guard, &*mem_pool, &mem_guard, &metadata, 100)
                .await
                .expect("test insert-page allocation should succeed");
            let failed_page_id = page_guard.page_id();
            blk_idx.cache_exclusive_insert_page(page_guard);

            let failing_pool = FailingInsertPagePool::new(mem_pool.guard(), failed_page_id);
            let res = blk_idx
                .get_insert_page_exclusive_with_redo(
                    &meta_guard,
                    &failing_pool,
                    &mem_guard,
                    &metadata,
                    100,
                    None,
                )
                .await;
            let err = match res {
                Ok(_) => panic!("expected exclusive free-list page reload failure"),
                Err(err) => err,
            };
            assert!(err.io_error().is_some());
            assert!(blk_idx.insert_free_list.lock().is_empty());
        });
    }

    #[test]
    fn test_try_get_insert_page_with_redo_reclaims_allocated_row_page_on_insert_error() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(fixed_pool_bytes(1));
            let mem_pool = owned_mem_pool(fixed_pool_bytes(1));
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, 0)
                .await
                .expect("test row-page-index construction should succeed");
            fill_root_leaf_full(&blk_idx, &meta_guard).await;

            let err = match blk_idx
                .try_get_insert_page_with_redo(
                    &meta_guard,
                    &*mem_pool,
                    &mem_guard,
                    &metadata,
                    100,
                    None,
                )
                .await
            {
                Ok(_) => panic!("metadata split should fail in one-page meta pool"),
                Err(err) => err,
            };
            assert_eq!(err.resource_error(), Some(ResourceError::BufferPoolFull));
            assert_eq!(mem_pool.allocated(), 0);
        });
    }

    #[test]
    fn test_get_insert_page_exclusive_with_redo_reclaims_allocated_row_page_on_insert_error() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(fixed_pool_bytes(1));
            let mem_pool = owned_mem_pool(fixed_pool_bytes(1));
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, 0)
                .await
                .expect("test row-page-index construction should succeed");
            fill_root_leaf_full(&blk_idx, &meta_guard).await;

            let err = match blk_idx
                .get_insert_page_exclusive_with_redo(
                    &meta_guard,
                    &*mem_pool,
                    &mem_guard,
                    &metadata,
                    100,
                    None,
                )
                .await
            {
                Ok(_) => panic!("metadata split should fail in one-page meta pool"),
                Err(err) => err,
            };
            assert_eq!(err.resource_error(), Some(ResourceError::BufferPoolFull));
            assert_eq!(mem_pool.allocated(), 0);
        });
    }

    #[test]
    fn test_allocate_row_page_at_reclaims_reserved_page_id_on_insert_error() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(fixed_pool_bytes(1));
            let mem_pool = owned_mem_pool(fixed_pool_bytes(1));
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, 0)
                .await
                .expect("test row-page-index construction should succeed");
            fill_root_leaf_full(&blk_idx, &meta_guard).await;

            let page_id = test_page_id(0);
            let err = match blk_idx
                .allocate_row_page_at(&meta_guard, &*mem_pool, &mem_guard, &metadata, 100, page_id)
                .await
            {
                Ok(_) => panic!("metadata split should fail in one-page meta pool"),
                Err(err) => err,
            };
            assert_eq!(err.resource_error(), Some(ResourceError::BufferPoolFull));
            assert_eq!(mem_pool.allocated(), 0);

            let page = mem_pool
                .allocate_page_at::<RowPage>(&mem_guard, page_id)
                .await
                .expect("fixed page id should be released on rollback");
            mem_pool.deallocate_page(page);
        });
    }

    #[test]
    fn test_row_page_index_cursor_shared() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let row_pages = 1024usize;
            // 1024 row pages ~= 64MB.
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(crate::buffer::PoolRole::Mem)
                        .max_mem_size(100usize * 1024 * 1024)
                        .max_file_size(1024usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_idx"))
                .build()
                .await
                .unwrap();
            {
                let metadata = make_test_metadata();
                let meta_guard = engine.meta_pool.pool_guard();
                let blk_idx = RowPageIndex::new(engine.meta_pool.clone_inner(), &meta_guard, 0)
                    .await
                    .expect("test row-page-index construction should succeed");
                let mem_guard = engine.mem_pool.pool_guard();
                for _ in 0..row_pages {
                    let _ = blk_idx
                        .get_insert_page(&meta_guard, &*engine.mem_pool, &mem_guard, &metadata, 100)
                        .await;
                }
                let mut count = 0usize;
                let mut cursor = blk_idx.mem_cursor(&meta_guard);
                cursor.seek(0).await;
                while let Some(res) = cursor.next().await {
                    count += 1;
                    let g = res.try_into_shared().unwrap();
                    assert!(g.page().is_leaf());
                }
                let row_pages_per_leaf = NBR_ROW_PAGE_ENTRIES_IN_LEAF;
                assert_eq!(count, row_pages.div_ceil(row_pages_per_leaf));
            }
            drop(engine);
        })
    }

    #[test]
    fn test_row_page_index_cursor_two_level_tree() {
        smol::block_on(async {
            let pool = owned_index_pool(1024usize * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let blk_idx = RowPageIndex::new(pool.guard(), &pool_guard, 0)
                .await
                .expect("test row-page-index construction should succeed");

            let overflow_entries = 3usize;
            let row_pages = NBR_ROW_PAGE_ENTRIES_IN_LEAF + overflow_entries;
            for row_page_id in 0..row_pages {
                loop {
                    if let Valid(_) = blk_idx
                        .insert_row_page(&pool_guard, 1, PageID::from(row_page_id))
                        .await
                        .expect("test row-page insertion should succeed")
                    {
                        break;
                    }
                }
            }
            assert_eq!(blk_idx.height(), 1);

            let mut root_leaf_page_ids = vec![];
            {
                let root = pool
                    .get_page_spin::<RowPageIndexNode>(&pool_guard, blk_idx.root_page_id())
                    .lock_shared_async()
                    .await
                    .unwrap();
                let page = root.page();
                assert!(page.is_branch());
                assert_eq!(page.branch_entries().len(), 2);
                root_leaf_page_ids.extend(page.branch_entries().iter().map(|e| e.page_id));
            }

            let mut cursor_leaf_page_ids = vec![];
            let mut leaf_headers = vec![];
            let mut cursor = blk_idx.mem_cursor(&pool_guard);
            cursor.seek(0).await;
            while let Some(res) = cursor.next().await {
                let g = res.try_into_shared().unwrap();
                let node = g.page();
                assert!(node.is_leaf());
                cursor_leaf_page_ids.push(g.page_id());
                leaf_headers.push((
                    node.header.start_row_id,
                    node.header.end_row_id,
                    node.header.count as usize,
                ));
            }

            assert_eq!(leaf_headers.len(), 2);
            assert_eq!(leaf_headers[0].0, 0);
            assert_eq!(leaf_headers[0].2, NBR_ROW_PAGE_ENTRIES_IN_LEAF);
            assert_eq!(leaf_headers[1].0, leaf_headers[0].1);
            assert_eq!(leaf_headers[1].1, row_pages as RowID);
            assert_eq!(leaf_headers[1].2, overflow_entries);
            assert_eq!(cursor_leaf_page_ids, root_leaf_page_ids);
            assert!(cursor.next().await.is_none());
        })
    }

    #[test]
    fn test_row_page_index_search() {
        smol::block_on(async {
            let pool = owned_index_pool(512usize * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let blk_idx = RowPageIndex::new(pool.guard(), &pool_guard, 0)
                .await
                .expect("test row-page-index construction should succeed");
            let row_pages = 5000usize;
            let rows_per_page = 100usize;
            for i in 0..row_pages {
                loop {
                    if let Valid(_) = blk_idx
                        .insert_row_page(&pool_guard, rows_per_page as u64, PageID::from(i))
                        .await
                        .expect("test row-page insertion should succeed")
                    {
                        break;
                    }
                }
            }
            for i in 0..row_pages {
                let row_id = (i * rows_per_page + rows_per_page / 2) as RowID;
                match blk_idx.find_row(&pool_guard, row_id).await {
                    RowLocation::RowPage(page_id) => assert_eq!(page_id, PageID::from(i)),
                    _ => panic!("invalid search result for i={i}"),
                }
            }
            assert!(matches!(
                blk_idx
                    .find_row(&pool_guard, (row_pages * rows_per_page) as RowID)
                    .await,
                RowLocation::NotFound
            ));
        })
    }

    #[test]
    fn test_row_page_index_split() {
        smol::block_on(async {
            let pool = owned_index_pool(1024usize * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let blk_idx = RowPageIndex::new(pool.guard(), &pool_guard, 0)
                .await
                .expect("test row-page-index construction should succeed");
            assert_eq!(blk_idx.height(), 0);

            for row_page_id in 0..10000 {
                loop {
                    if let Valid(_) = blk_idx
                        .insert_row_page(&pool_guard, 100, test_page_id(row_page_id))
                        .await
                        .expect("test row-page insertion should succeed")
                    {
                        break;
                    }
                }
            }
            assert_eq!(blk_idx.height(), 1);

            let mut root = pool
                .get_page::<RowPageIndexNode>(
                    &pool_guard,
                    blk_idx.root_page_id(),
                    LatchFallbackMode::Exclusive,
                )
                .await
                .expect("buffer-pool read failed in test")
                .lock_exclusive_async()
                .await
                .unwrap();
            // Mark root as full to trigger root split.
            root.page_mut().header.count = NBR_ENTRIES_IN_BRANCH as u32;

            // Assign right-most leaf node.
            let mut r_g = pool
                .allocate_page::<RowPageIndexNode>(&pool_guard)
                .await
                .expect("test page allocation should succeed");
            r_g.page_mut().init(0, 50000, 10000, test_page_id(10001));
            r_g.page_mut().header.count = NBR_ROW_PAGE_ENTRIES_IN_LEAF as u32;
            let r_page_id = r_g.page_id();
            drop(r_g);
            root.page_mut().branch_last_entry_mut().row_id = 50000;
            root.page_mut().branch_last_entry_mut().page_id = r_page_id;
            drop(root);

            loop {
                if let Valid(_) = blk_idx
                    .insert_row_page(&pool_guard, 100, test_page_id(20000))
                    .await
                    .expect("test row-page insertion should succeed")
                {
                    break;
                }
            }
            assert_eq!(blk_idx.height(), 2);
        })
    }

    #[test]
    fn test_row_page_index_inline_redo_ctx_commits_create_row_page_once() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(crate::buffer::PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_idx"))
                .build()
                .await
                .unwrap();
            {
                let metadata = make_test_metadata();
                let meta_guard = engine.meta_pool.pool_guard();
                let blk_idx = RowPageIndex::new(engine.meta_pool.clone_inner(), &meta_guard, 0)
                    .await
                    .expect("test row-page-index construction should succeed");
                let mem_guard = engine.mem_pool.pool_guard();
                let redo_ctx = RowPageCreateRedoCtx::new(&engine.trx_sys, 104);
                let page_guard = blk_idx
                    .get_insert_page_with_redo(
                        &meta_guard,
                        &*engine.mem_pool,
                        &mem_guard,
                        &metadata,
                        100,
                        Some(redo_ctx),
                    )
                    .await;
                let create_cts = page_guard
                    .bf()
                    .ctx
                    .as_ref()
                    .and_then(|ctx| ctx.row_ver())
                    .map(|row_ver| row_ver.create_cts())
                    .unwrap();
                assert!(create_cts > 0);

                let page_id = page_guard.page_id();
                let page_guard = page_guard.downgrade().exclusive_async().await;
                blk_idx.cache_exclusive_insert_page(page_guard);

                let reused_page = blk_idx
                    .get_insert_page_with_redo(
                        &meta_guard,
                        &*engine.mem_pool,
                        &mem_guard,
                        &metadata,
                        100,
                        Some(redo_ctx),
                    )
                    .await;
                assert_eq!(reused_page.page_id(), page_id);
            }
            engine.shutdown().unwrap();

            let mut create_row_page_logs = 0usize;
            let file_prefix = temp_dir.path().join("redo_row_page_idx");
            let file_prefix = file_prefix.to_str().unwrap();
            let logs = list_log_files(file_prefix, 0, false).unwrap();
            for log in logs {
                let mut reader = engine.trx_sys.log_reader(&log).unwrap();
                loop {
                    match reader.read() {
                        ReadLog::SizeLimit | ReadLog::DataCorrupted => panic!("invalid log data"),
                        ReadLog::DataEnd => break,
                        ReadLog::Some(mut group) => {
                            while let Some(log) = group.try_next().unwrap() {
                                if let Some(ddl) = log.payload.ddl.as_deref()
                                    && matches!(ddl, DDLRedo::CreateRowPage { table_id: 104, .. })
                                {
                                    create_row_page_logs += 1;
                                }
                            }
                        }
                    }
                }
            }
            assert_eq!(create_row_page_logs, 1);
        })
    }

    fn first_i32_unique_index() -> IndexSpec {
        IndexSpec::new("idx_id", vec![IndexKey::new(0)], IndexAttributes::UK)
    }

    fn make_test_metadata() -> Arc<TableMetadata> {
        Arc::new(TableMetadata::new(
            vec![ColumnSpec {
                column_name: SemiStr::new("id"),
                column_type: ValKind::I32,
                column_attributes: ColumnAttributes::empty(),
            }],
            vec![first_i32_unique_index()],
        ))
    }
}
