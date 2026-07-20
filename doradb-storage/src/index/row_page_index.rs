use crate::buffer::guard::{
    FacadePageGuard, PageExclusiveGuard, PageGuard, PageOptimisticGuard, PageSharedGuard,
};
use crate::buffer::page::{
    BufferPage, BufferPageKind, PAGE_SIZE, VersionedPageID, assert_buffer_page,
};
use crate::buffer::{BufferPool, PoolGuard, get_page_versioned_shared};
use crate::catalog::TableColumnLayout;
use crate::error::{
    Error, InternalError, Result, Validation,
    Validation::{Invalid, Valid},
};
use crate::file::block_integrity::BLOCK_INTEGRITY_TRAILER_SIZE;
use crate::id::{BlockID, PageID, RowID, TrxID};
use crate::index::util::{Maskable, ParentPosition, RowPageCreateRedoCtx};
use crate::latch::LatchFallbackMode;
use crate::layout;
use crate::quiescent::QuiescentGuard;
use crate::row::{INVALID_ROW_ID, RowPage};
use either::Either::{Left, Right};
use error_stack::Report;
use parking_lot::Mutex;
use std::mem;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Bytes in one row-page-index node.
pub(crate) const ROW_PAGE_INDEX_NODE_SIZE: usize = PAGE_SIZE;
/// Bytes reserved at the end of every row-page-index node for spill checksums.
pub(crate) const ROW_PAGE_INDEX_NODE_FOOTER_SIZE: usize = BLOCK_INTEGRITY_TRAILER_SIZE;
/// Bytes available to row-page-index node entries before the checksum footer.
pub(crate) const ROW_PAGE_INDEX_NODE_USABLE_SIZE: usize =
    ROW_PAGE_INDEX_NODE_SIZE - ROW_PAGE_INDEX_NODE_FOOTER_SIZE;
/// Bytes occupied by the row-page-index node header.
pub(crate) const ROW_PAGE_INDEX_NODE_HEADER_SIZE: usize = mem::size_of::<RowPageIndexNodeHeader>();
/// Bytes occupied by one row-page-index entry.
pub(crate) const ENTRY_SIZE: usize = mem::size_of::<PageEntry>();
/// Maximum number of child entries in one branch node.
pub(crate) const NBR_ENTRIES_IN_BRANCH: usize =
    (ROW_PAGE_INDEX_NODE_USABLE_SIZE - ROW_PAGE_INDEX_NODE_HEADER_SIZE) / ENTRY_SIZE;
/// Maximum number of row-page entries in one leaf node.
pub(crate) const NBR_ROW_PAGE_ENTRIES_IN_LEAF: usize =
    (ROW_PAGE_INDEX_NODE_USABLE_SIZE - ROW_PAGE_INDEX_NODE_HEADER_SIZE) / ENTRY_SIZE;

const _: () = assert!(
    { mem::size_of::<RowPageIndexNode>() == ROW_PAGE_INDEX_NODE_SIZE },
    "Size of node of BlockIndex should equal to 64KB"
);

const _: () = assert!(
    { mem::offset_of!(RowPageIndexNode, data) == ROW_PAGE_INDEX_NODE_HEADER_SIZE },
    "RowPageIndexNode data offset should match RowPageIndexNodeHeader size"
);

const _: () = assert!(
    { mem::offset_of!(RowPageIndexNode, footer) == ROW_PAGE_INDEX_NODE_USABLE_SIZE },
    "RowPageIndexNode checksum footer must start after logical usable bytes"
);

const _: () = assert!(
    {
        ROW_PAGE_INDEX_NODE_HEADER_SIZE + NBR_ENTRIES_IN_BRANCH * ENTRY_SIZE
            <= ROW_PAGE_INDEX_NODE_USABLE_SIZE
    },
    "Size of branch node of BlockIndex must not overlap checksum footer"
);

const _: () = assert!(
    {
        ROW_PAGE_INDEX_NODE_HEADER_SIZE + NBR_ROW_PAGE_ENTRIES_IN_LEAF * ENTRY_SIZE
            <= ROW_PAGE_INDEX_NODE_USABLE_SIZE
    },
    "Size of leaf node of BlockIndex must not overlap checksum footer"
);

const _: () = assert!(
    { ROW_PAGE_INDEX_NODE_HEADER_SIZE.is_multiple_of(mem::align_of::<PageEntry>()) },
    "RowPageIndexNode data area must align with PageEntry alignment"
);

const _: () = assert!(
    { mem::size_of::<PageEntry>() == ENTRY_SIZE },
    "ENTRY_SIZE must match PageEntry size"
);

const _: () = assert_buffer_page::<RowPageIndexNode>();

enum InsertPageGuardError {
    // The row-page index did not publish the new page id. The caller still
    // owns an unlinked allocation and can deallocate it before returning.
    Unpublished(Error),
    // The row-page index already points at the new page id. The caller must
    // keep the initialized empty page allocated so the index never contains a
    // dangling page reference, even though the create-redo commit failed.
    Published(Error),
}

struct RowPageIndexInsert {
    start_row_id: RowID,
    end_row_id: RowID,
    create_redo: Result<Option<TrxID>>,
}

#[derive(Clone, Copy)]
struct RowPageIndexAppend<'a> {
    start_row_id: RowID,
    count: u64,
    page_id: PageID,
    redo_ctx: Option<RowPageCreateRedoCtx<'a>>,
}

impl RowPageIndexAppend<'_> {
    #[inline]
    fn end_row_id(&self) -> RowID {
        self.start_row_id + self.count
    }

    #[inline]
    fn finish(self) -> RowPageIndexInsert {
        let end_row_id = self.end_row_id();
        let create_redo = match self.redo_ctx {
            Some(redo_ctx) => redo_ctx
                .commit_row_page(self.page_id, self.start_row_id, end_row_id)
                .map(Some),
            None => Ok(None),
        };
        RowPageIndexInsert {
            start_row_id: self.start_row_id,
            end_row_id,
            create_redo,
        }
    }
}

/// RowPageIndexNode is one B-tree node of the row-page index.
/// It can be either branch or leaf.
/// Branch contains at most 4093 child node pointers.
/// Leaf contains at most NBR_ROW_PAGE_ENTRIES_IN_LEAF page entries.
#[repr(C)]
#[derive(Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub(crate) struct RowPageIndexNode {
    /// Fixed node header containing height and row-id bounds.
    pub(crate) header: RowPageIndexNodeHeader,
    data: [u8; ROW_PAGE_INDEX_NODE_USABLE_SIZE - ROW_PAGE_INDEX_NODE_HEADER_SIZE],
    footer: [u8; ROW_PAGE_INDEX_NODE_FOOTER_SIZE],
}

impl RowPageIndexNode {
    /// Initializes one node with the given height and starting row id.
    /// End row id is set to MAX_ROW_ID.
    #[inline]
    pub(crate) fn init(
        &mut self,
        height: u32,
        start_row_id: RowID,
        count: u64,
        insert_page_id: PageID,
    ) {
        self.header.height = height;
        self.header.start_row_id = start_row_id;
        self.header.end_row_id = INVALID_ROW_ID;
        self.header.count = 0;
        self.leaf_add_entry(start_row_id, count, insert_page_id);
    }

    /// Initialize an empty node without entries.
    #[inline]
    pub(crate) fn init_empty(&mut self, height: u32, start_row_id: RowID) {
        self.header.height = height;
        self.header.start_row_id = start_row_id;
        self.header.end_row_id = INVALID_ROW_ID;
        self.header.count = 0;
    }

    /// Returns whether the node is leaf.
    #[inline]
    pub(crate) fn is_leaf(&self) -> bool {
        self.header.height == 0
    }

    /// Returns whether the node is branch.
    #[inline]
    pub(crate) fn is_branch(&self) -> bool {
        !self.is_leaf()
    }

    /* branch methods */

    /// Returns whether the branch node is full.
    #[inline]
    pub(crate) fn branch_is_full(&self) -> bool {
        debug_assert!(self.is_branch());
        self.header.count as usize == NBR_ENTRIES_IN_BRANCH
    }

    /// Returns the entry slice in branch node.
    #[inline]
    pub(crate) fn branch_entries(&self) -> &[PageEntry] {
        debug_assert!(self.is_branch());
        self.entries(self.header.count as usize)
    }

    /// Returns mutable entry slice in branch node.
    #[inline]
    pub(crate) fn branch_entries_mut(&mut self) -> &mut [PageEntry] {
        debug_assert!(self.is_branch());
        self.entries_mut(self.header.count as usize)
    }

    /// Returns entry in branch node by given index.
    #[inline]
    pub(crate) fn branch_entry(&self, idx: usize) -> &PageEntry {
        &self.branch_entries()[idx]
    }

    /// Returns last entry in branch node.
    #[inline]
    pub(crate) fn branch_last_entry(&self) -> &PageEntry {
        debug_assert!(self.is_branch());
        &self.branch_entries()[self.header.count as usize - 1]
    }

    /// Returns mutable last entry in branch node.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "test-only branch_last_entry_mut")
    )]
    pub(crate) fn branch_last_entry_mut(&mut self) -> &mut PageEntry {
        debug_assert!(self.is_branch());
        let idx = self.header.count as usize - 1;
        &mut self.branch_entries_mut()[idx]
    }

    /// Add a new entry in branch node.
    #[inline]
    pub(crate) fn branch_add_entry(&mut self, entry: PageEntry) {
        debug_assert!(self.is_branch());
        assert!((self.header.count as usize) < NBR_ENTRIES_IN_BRANCH);
        let idx = self.header.count;
        self.header.count += 1;
        self.branch_entries_mut()[idx as usize] = entry;
    }

    /// Removes `count` leading branch entries and compacts the suffix.
    #[inline]
    fn branch_remove_prefix(&mut self, count: usize) {
        debug_assert!(self.is_branch());
        let old_count = self.header.count as usize;
        debug_assert!(count <= old_count);
        self.entries_mut(old_count).copy_within(count..old_count, 0);
        self.header.count = (old_count - count) as u32;
    }

    /* leaf methods */

    /// Returns whether the leaf node is full.
    #[inline]
    pub(crate) fn leaf_is_full(&self) -> bool {
        debug_assert!(self.is_leaf());
        self.header.count as usize == NBR_ROW_PAGE_ENTRIES_IN_LEAF
    }

    /// Returns whether the leaf node is empty.
    #[inline]
    pub(crate) fn leaf_is_empty(&self) -> bool {
        debug_assert!(self.is_leaf());
        self.header.count == 0
    }

    /// Returns entry slice of leaf node.
    #[inline]
    pub(crate) fn leaf_entries(&self) -> &[PageEntry] {
        debug_assert!(self.is_leaf());
        self.entries(self.header.count as usize)
    }

    /// Returns mutable last entry in leaf node.
    #[inline]
    pub(crate) fn leaf_last_entry_mut(&mut self) -> &mut PageEntry {
        debug_assert!(self.is_leaf());
        let count = self.header.count as usize;
        &mut self.leaf_entries_mut()[count - 1]
    }

    /// Returns mutable entry slice in leaf node.
    #[inline]
    pub(crate) fn leaf_entries_mut(&mut self) -> &mut [PageEntry] {
        debug_assert!(self.is_leaf());
        self.entries_mut(self.header.count as usize)
    }

    /// Add a new entry in leaf node.
    #[inline]
    pub(crate) fn leaf_add_entry(&mut self, start_row_id: RowID, count: u64, page_id: PageID) {
        debug_assert!(self.is_leaf());
        debug_assert!(!self.leaf_is_full());
        self.header.count += 1;
        let entry = self.leaf_last_entry_mut();
        entry.row_id = start_row_id;
        entry.page_id = page_id;
        self.header.end_row_id = start_row_id + count;
    }

    /// Removes `count` leading leaf entries and compacts the suffix.
    #[inline]
    fn leaf_remove_prefix(&mut self, count: usize) {
        debug_assert!(self.is_leaf());
        let old_count = self.header.count as usize;
        debug_assert!(count <= old_count);
        self.entries_mut(old_count).copy_within(count..old_count, 0);
        self.header.count = (old_count - count) as u32;
    }

    #[inline]
    fn entries(&self, len: usize) -> &[PageEntry] {
        let bytes_len = len * mem::size_of::<PageEntry>();
        layout::slice_from_bytes(&self.data[..bytes_len])
    }

    #[inline]
    fn entries_mut(&mut self, len: usize) -> &mut [PageEntry] {
        let bytes_len = len * mem::size_of::<PageEntry>();
        layout::slice_from_bytes_mut(&mut self.data[..bytes_len])
    }
}

// SAFETY: `RowPageIndexNode` is one explicit `repr(C)` page image, derives the
// required zerocopy layout traits, has no drop glue, and is accessed through the
// buffer-pool and row-page-index latch protocol.
unsafe impl BufferPage for RowPageIndexNode {
    const KIND: BufferPageKind = BufferPageKind::RowPageIndexNode;
}

/// Header metadata for one in-memory row-page-index node.
#[repr(C)]
#[derive(Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub(crate) struct RowPageIndexNodeHeader {
    /// Height of the node, where zero means leaf.
    pub(crate) height: u32,
    /// Number of valid entries stored in the node.
    pub(crate) count: u32,
    /// Inclusive starting row id covered by the node.
    pub(crate) start_row_id: RowID,
    /// Exclusive ending row id covered by the node.
    ///
    /// This value may not be valid when the node is a branch.
    pub(crate) end_row_id: RowID,
}

/// Entry mapping a row-id boundary to a row-page or child node page id.
#[repr(C)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable)]
pub(crate) struct PageEntry {
    /// Row-id boundary represented by this entry.
    pub(crate) row_id: RowID,
    /// Row page or child-node page id for this entry.
    pub(crate) page_id: PageID,
}

impl PageEntry {
    /// Creates a page entry `(row_id -> page_id)`.
    #[inline]
    pub(crate) fn new(row_id: RowID, page_id: PageID) -> Self {
        PageEntry { row_id, page_id }
    }
}

/// Result of one successful checkpoint-prefix prune.
pub(crate) struct RowPagePrefixPrune {
    /// Row pages unlinked in canonical RowID order.
    pub(crate) page_ids: Box<[PageID]>,
    /// Detached metadata nodes reclaimed by the operation.
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved prune statistics"))]
    pub(crate) reclaimed_nodes: usize,
    /// Redundant fixed-root levels collapsed by the operation.
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved prune statistics"))]
    pub(crate) collapsed_levels: usize,
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
pub(crate) struct RowPageIndex<P: 'static> {
    root_page_id: PageID,
    height: AtomicUsize,
    insert_free_list: Mutex<Vec<VersionedPageID>>,
    // Buffer pool that owns row-page-index nodes.
    pool: QuiescentGuard<P>,
}

impl<P: BufferPool> RowPageIndex<P> {
    /// Creates a new row-page index backed by the buffer pool.
    #[inline]
    pub(crate) async fn new(
        pool: QuiescentGuard<P>,
        pool_guard: &PoolGuard,
        start_row_id: RowID,
    ) -> Result<Self> {
        let mut g = pool.allocate_page::<RowPageIndexNode>(pool_guard).await?;
        let page_id = g.page_id();
        let page = g.page_mut();
        page.init_empty(0, start_row_id);
        Ok(RowPageIndex {
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
        Ok(self
            .pool
            .allocate_page::<RowPageIndexNode>(pool_guard)
            .await?)
    }

    /// Returns the height of the row-page index.
    #[inline]
    pub(crate) fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }

    /// Returns root page id of in-memory row index.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "test-only root_page_id"))]
    pub(crate) fn root_page_id(&self) -> PageID {
        self.root_page_id
    }

    /// Destroy this row-page index and its still-hot row pages at or above the pivot.
    #[inline]
    pub(crate) async fn destroy<B: BufferPool>(
        self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        pivot_row_id: RowID,
    ) -> Result<()> {
        self.validate_destroy_pivot(meta_pool_guard, pivot_row_id)
            .await?;
        let mut stack = vec![(self.root_page_id, false)];
        while let Some((page_id, visited_children)) = stack.pop() {
            let guard = self
                .pool
                .get_page::<RowPageIndexNode>(
                    meta_pool_guard,
                    page_id,
                    LatchFallbackMode::Exclusive,
                )
                .await?
                .lock_exclusive_async()
                .await
                .ok_or_else(|| {
                    Report::new(InternalError::RowPageMissing)
                        .attach(format!("row-page-index node missing: page_id={page_id}"))
                })?;
            if guard.page().is_branch() && !visited_children {
                let child_page_ids = guard
                    .page()
                    .branch_entries()
                    .iter()
                    .map(|entry| entry.page_id)
                    .collect::<Vec<_>>();
                drop(guard);
                stack.push((page_id, true));
                stack.extend(child_page_ids.into_iter().rev().map(|child| (child, false)));
                continue;
            }
            if guard.page().is_leaf() {
                let row_page_ids = guard
                    .page()
                    .leaf_entries()
                    .iter()
                    .map(|entry| entry.page_id)
                    .collect::<Vec<_>>();
                for row_page_id in row_page_ids {
                    let row_page = mem_pool
                        .get_page::<RowPage>(
                            mem_pool_guard,
                            row_page_id,
                            LatchFallbackMode::Exclusive,
                        )
                        .await?
                        .lock_exclusive_async()
                        .await
                        .ok_or_else(|| {
                            Report::new(InternalError::RowPageMissing)
                                .attach(format!("row page missing: page_id={row_page_id}"))
                        })?;
                    mem_pool.deallocate_page(row_page);
                }
            }
            self.pool.deallocate_page(guard);
        }
        Ok(())
    }

    #[inline]
    async fn validate_destroy_pivot(
        &self,
        pool_guard: &PoolGuard,
        pivot_row_id: RowID,
    ) -> Result<()> {
        let mut stack = vec![self.root_page_id];
        while let Some(page_id) = stack.pop() {
            let guard = self
                .pool
                .get_page::<RowPageIndexNode>(pool_guard, page_id, LatchFallbackMode::Shared)
                .await?
                .lock_shared_async()
                .await
                .ok_or_else(|| row_page_index_missing(page_id))?;
            let page = guard.page();
            if page.is_branch() {
                stack.extend(
                    page.branch_entries()
                        .iter()
                        .rev()
                        .map(|entry| entry.page_id),
                );
            } else {
                for entry in page.leaf_entries() {
                    assert!(
                        entry.row_id >= pivot_row_id,
                        "row-page index retains entry below pivot during destroy: pivot_row_id={pivot_row_id}, entry_row_id={}, page_id={}",
                        entry.row_id,
                        entry.page_id
                    );
                }
            }
        }
        Ok(())
    }

    /// Validates and removes the exact current left row-page prefix.
    #[inline]
    pub(crate) async fn prune_checkpoint_prefix(
        &self,
        pool_guard: &PoolGuard,
        start_row_id: RowID,
        end_row_id: RowID,
        expected_page_ids: &[PageID],
    ) -> Result<RowPagePrefixPrune> {
        assert!(
            !expected_page_ids.is_empty(),
            "checkpoint row-page prefix has no page ids"
        );
        assert!(
            start_row_id < end_row_id,
            "checkpoint row-page prefix has invalid range: start_row_id={start_row_id}, end_row_id={end_row_id}"
        );

        let root = self
            .pool
            .get_page::<RowPageIndexNode>(
                pool_guard,
                self.root_page_id,
                LatchFallbackMode::Exclusive,
            )
            .await?
            .lock_exclusive_async()
            .await
            .ok_or_else(|| row_page_index_missing(self.root_page_id))?;
        assert_eq!(
            root.page().header.start_row_id,
            start_row_id,
            "checkpoint row-page prefix does not start at index boundary"
        );
        self.validate_checkpoint_prefix(
            pool_guard,
            root.page(),
            start_row_id,
            end_row_id,
            expected_page_ids,
        )
        .await?;

        let (mut root, detached_subtrees, mut detached_nodes) = self
            .apply_checkpoint_prefix(pool_guard, root, end_row_id)
            .await?;
        let mut collapsed_levels = 0usize;
        loop {
            if root.page().is_leaf() {
                if root.page().leaf_is_empty() {
                    root.page_mut().init_empty(0, end_row_id);
                    self.height.store(0, Ordering::Relaxed);
                }
                break;
            }
            match root.page().header.count {
                0 => {
                    root.page_mut().init_empty(0, end_row_id);
                    self.height.store(0, Ordering::Relaxed);
                    break;
                }
                1 => {
                    let child_page_id = root.page().branch_entry(0).page_id;
                    let child = self
                        .pool
                        .get_page::<RowPageIndexNode>(
                            pool_guard,
                            child_page_id,
                            LatchFallbackMode::Exclusive,
                        )
                        .await?
                        .lock_exclusive_async()
                        .await
                        .ok_or_else(|| row_page_index_missing(child_page_id))?;
                    root.page_mut().clone_from(child.page());
                    self.height
                        .store(root.page().header.height as usize, Ordering::Relaxed);
                    detached_nodes.push(child_page_id);
                    drop(child);
                    collapsed_levels += 1;
                }
                _ => {
                    self.height
                        .store(root.page().header.height as usize, Ordering::Relaxed);
                    break;
                }
            }
        }
        drop(root);

        let mut reclaimed_nodes = self
            .reclaim_detached_subtrees(pool_guard, detached_subtrees)
            .await?;
        for page_id in detached_nodes {
            let guard = self
                .pool
                .get_page::<RowPageIndexNode>(pool_guard, page_id, LatchFallbackMode::Exclusive)
                .await?
                .lock_exclusive_async()
                .await
                .ok_or_else(|| row_page_index_missing(page_id))?;
            self.pool.deallocate_page(guard);
            reclaimed_nodes += 1;
        }

        Ok(RowPagePrefixPrune {
            page_ids: expected_page_ids.to_vec().into_boxed_slice(),
            reclaimed_nodes,
            collapsed_levels,
        })
    }

    async fn validate_checkpoint_prefix(
        &self,
        pool_guard: &PoolGuard,
        root: &RowPageIndexNode,
        start_row_id: RowID,
        end_row_id: RowID,
        expected_page_ids: &[PageID],
    ) -> Result<()> {
        let mut next_row_id = start_row_id;
        let mut expected_idx = 0usize;
        let mut stack = Vec::new();
        if root.is_branch() {
            stack.extend(
                root.branch_entries()
                    .iter()
                    .rev()
                    .map(|entry| entry.page_id),
            );
        } else {
            validate_leaf_prefix(
                root,
                &mut next_row_id,
                end_row_id,
                expected_page_ids,
                &mut expected_idx,
            );
        }
        while expected_idx < expected_page_ids.len() {
            let page_id = stack
                .pop()
                .expect("checkpoint row-page prefix exceeds index");
            let guard = self
                .pool
                .get_page::<RowPageIndexNode>(pool_guard, page_id, LatchFallbackMode::Shared)
                .await?
                .lock_shared_async()
                .await
                .ok_or_else(|| row_page_index_missing(page_id))?;
            let page = guard.page();
            if page.is_branch() {
                stack.extend(
                    page.branch_entries()
                        .iter()
                        .rev()
                        .map(|entry| entry.page_id),
                );
            } else {
                validate_leaf_prefix(
                    page,
                    &mut next_row_id,
                    end_row_id,
                    expected_page_ids,
                    &mut expected_idx,
                );
            }
        }
        assert_eq!(
            next_row_id, end_row_id,
            "checkpoint row-page prefix end mismatch"
        );
        Ok(())
    }

    async fn apply_checkpoint_prefix(
        &self,
        pool_guard: &PoolGuard,
        root: PageExclusiveGuard<RowPageIndexNode>,
        end_row_id: RowID,
    ) -> Result<(
        PageExclusiveGuard<RowPageIndexNode>,
        Vec<PageID>,
        Vec<PageID>,
    )> {
        let mut path = vec![root];
        let mut boundary_indexes = Vec::new();
        loop {
            let page = path.last().unwrap().page();
            if page.is_leaf() {
                break;
            }
            let entries = page.branch_entries();
            let first_at_or_after = entries.partition_point(|entry| entry.row_id < end_row_id);
            if first_at_or_after < entries.len() && entries[first_at_or_after].row_id == end_row_id
            {
                boundary_indexes.push((first_at_or_after, false));
                break;
            }
            let boundary_idx = first_at_or_after.saturating_sub(1);
            let child_page_id = entries[boundary_idx].page_id;
            boundary_indexes.push((boundary_idx, true));
            let child = self
                .pool
                .get_page::<RowPageIndexNode>(
                    pool_guard,
                    child_page_id,
                    LatchFallbackMode::Exclusive,
                )
                .await?
                .lock_exclusive_async()
                .await
                .ok_or_else(|| row_page_index_missing(child_page_id))?;
            path.push(child);
        }

        let mut detached_subtrees = Vec::new();
        let mut detached_nodes = Vec::new();
        let mut child_empty = false;
        for idx in (0..path.len()).rev() {
            let page_id = path[idx].page_id();
            let page = path[idx].page_mut();
            let empty = if page.is_leaf() {
                let trim = page
                    .leaf_entries()
                    .partition_point(|entry| entry.row_id < end_row_id);
                page.leaf_remove_prefix(trim);
                page.header.start_row_id = end_row_id;
                if !page.leaf_is_empty() {
                    page.leaf_entries_mut()[0].row_id = end_row_id;
                }
                page.leaf_is_empty()
            } else {
                let (boundary_idx, descended) = boundary_indexes[idx];
                let trim = boundary_idx + usize::from(descended && child_empty);
                detached_subtrees.extend(
                    page.branch_entries()[..boundary_idx]
                        .iter()
                        .map(|entry| entry.page_id),
                );
                page.branch_remove_prefix(trim);
                page.header.start_row_id = end_row_id;
                if page.header.count != 0 {
                    page.branch_entries_mut()[0].row_id = end_row_id;
                }
                page.header.count == 0
            };
            if idx != 0 && empty {
                detached_nodes.push(page_id);
            }
            child_empty = empty;
        }

        while path.len() > 1 {
            drop(path.pop());
        }
        Ok((path.pop().unwrap(), detached_subtrees, detached_nodes))
    }

    async fn reclaim_detached_subtrees(
        &self,
        pool_guard: &PoolGuard,
        roots: Vec<PageID>,
    ) -> Result<usize> {
        let mut reclaimed = 0usize;
        let mut stack = roots;
        while let Some(page_id) = stack.pop() {
            let guard = self
                .pool
                .get_page::<RowPageIndexNode>(pool_guard, page_id, LatchFallbackMode::Exclusive)
                .await?
                .lock_exclusive_async()
                .await
                .ok_or_else(|| row_page_index_missing(page_id))?;
            if guard.page().is_branch() {
                stack.extend(
                    guard
                        .page()
                        .branch_entries()
                        .iter()
                        .map(|entry| entry.page_id),
                );
            }
            self.pool.deallocate_page(guard);
            reclaimed += 1;
        }
        Ok(reclaimed)
    }

    /// Get row page for insertion.
    /// Caller should cache insert page ids to avoid invoking this method frequently.
    #[inline]
    pub(crate) async fn get_insert_page<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        col_layout: &Arc<TableColumnLayout>,
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
            .insert_page_guard(meta_pool_guard, col_layout, count, redo_ctx, &mut new_page)
            .await
        {
            return Err(cleanup_failed_insert_page(mem_pool, new_page, err));
        }
        Ok(new_page.downgrade_shared())
    }

    /// Get exclusive row page for insertion.
    #[inline]
    pub(crate) async fn get_insert_page_exclusive<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        col_layout: &Arc<TableColumnLayout>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        if let Some(free_page) = self
            .get_insert_page_exclusive_from_free_list(mem_pool, mem_pool_guard)
            .await?
        {
            return Ok(free_page);
        }
        // Empty or stale free-list entries fall back to allocating a fresh row page.
        let mut new_page = mem_pool.allocate_page::<RowPage>(mem_pool_guard).await?;
        if let Err(err) = self
            .insert_page_guard(meta_pool_guard, col_layout, count, redo_ctx, &mut new_page)
            .await
        {
            return Err(cleanup_failed_insert_page(mem_pool, new_page, err));
        }
        Ok(new_page)
    }

    /// Allocate a row page with given page id.
    /// This method is used for data recovery, which replay all commit logs including row page creation.
    #[inline]
    pub(crate) async fn allocate_row_page_at<B: BufferPool>(
        &self,
        meta_pool_guard: &PoolGuard,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
        col_layout: &Arc<TableColumnLayout>,
        count: usize,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        let mut new_page = mem_pool
            .allocate_page_at::<RowPage>(mem_pool_guard, page_id)
            .await?;
        if let Err(err) = self
            .insert_page_guard(meta_pool_guard, col_layout, count, None, &mut new_page)
            .await
        {
            return Err(cleanup_failed_insert_page(mem_pool, new_page, err));
        }
        Ok(new_page)
    }

    #[inline]
    async fn insert_page_guard(
        &self,
        meta_pool_guard: &PoolGuard,
        col_layout: &Arc<TableColumnLayout>,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
        new_page: &mut PageExclusiveGuard<RowPage>,
    ) -> StdResult<(), InsertPageGuardError> {
        let new_page_id = new_page.page_id();
        loop {
            match self
                .insert_row_page(meta_pool_guard, count as u64, new_page_id, redo_ctx)
                .await
                .map_err(InsertPageGuardError::Unpublished)?
            {
                Invalid => (),
                Valid(inserted) => {
                    // initialize row page.
                    let start_row_id = inserted.start_row_id;
                    let end_row_id = inserted.end_row_id;
                    debug_assert!(end_row_id == start_row_id + count as u64);
                    new_page
                        .page_mut()
                        .init(start_row_id, count, col_layout.as_ref());
                    // create and attach a new empty undo map.
                    new_page
                        .bf_mut()
                        .init_undo_map(Arc::clone(col_layout), count);

                    match inserted.create_redo {
                        Ok(Some(create_cts)) => {
                            new_page.unwrap_vmap().set_create_cts(create_cts);
                        }
                        Ok(None) => (),
                        Err(err) => {
                            // The append became visible before the create-redo
                            // commit failed. Keep the page initialized so the
                            // published index entry points at a valid empty page.
                            return Err(InsertPageGuardError::Published(err));
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
    pub(crate) async fn find_row(
        &self,
        pool_guard: &PoolGuard,
        row_id: RowID,
    ) -> Result<RowLocation> {
        debug_assert!(!row_id.is_deleted());
        loop {
            match self.try_find_row(pool_guard, row_id).await? {
                Valid(row_location) => return Ok(row_location),
                Invalid => {}
            }
        }
    }

    /// Caches the guarded page version for a later no-transaction insert.
    #[inline]
    pub(crate) fn cache_exclusive_insert_page(&self, guard: PageExclusiveGuard<RowPage>) {
        let page_id = guard.versioned_page_id();
        drop(guard);
        self.cache_insert_page_version(page_id);
    }

    /// Caches a page version for a later insert.
    #[inline]
    pub(crate) fn cache_insert_page_version(&self, page_id: VersionedPageID) {
        let mut free_list = self.insert_free_list.lock();
        // Exact duplicates violate the cache-token ownership invariant. Older
        // generations of the same physical page remain valid stale candidates.
        debug_assert!(!free_list.contains(&page_id));
        free_list.push(page_id);
    }

    /// Returns the cursor for range scan.
    #[inline]
    pub(crate) fn mem_cursor<'a>(
        &'a self,
        pool_guard: &'a PoolGuard,
    ) -> RowPageIndexMemCursor<'a, P> {
        RowPageIndexMemCursor {
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
        loop {
            let page_id = {
                let mut g = self.insert_free_list.lock();
                let Some(page_id) = g.pop() else {
                    return Ok(None);
                };
                page_id
            };
            let Some(page_guard) =
                get_page_versioned_shared::<RowPage, _>(mem_pool, mem_pool_guard, page_id).await?
            else {
                continue;
            };
            return Ok(Some(page_guard));
        }
    }

    #[inline]
    async fn get_insert_page_exclusive_from_free_list<B: BufferPool>(
        &self,
        mem_pool: &B,
        mem_pool_guard: &PoolGuard,
    ) -> Result<Option<PageExclusiveGuard<RowPage>>> {
        loop {
            let page_id = {
                let mut g = self.insert_free_list.lock();
                let Some(page_id) = g.pop() else {
                    return Ok(None);
                };
                page_id
            };
            let Some(page_guard) = mem_pool
                .get_page_versioned::<RowPage>(
                    mem_pool_guard,
                    page_id,
                    LatchFallbackMode::Exclusive,
                )
                .await?
            else {
                continue;
            };
            if let Some(page_guard) = page_guard.lock_exclusive_async().await {
                return Ok(Some(page_guard));
            }
        }
    }

    /// Insert row page by splitting root.
    #[inline]
    async fn insert_row_page_split_root(
        &self,
        pool_guard: &PoolGuard,
        mut p_guard: PageExclusiveGuard<RowPageIndexNode>,
        append: RowPageIndexAppend<'_>,
    ) -> Result<RowPageIndexInsert> {
        debug_assert!(p_guard.page_id() == self.root_page_id);
        debug_assert!({
            let p = p_guard.page();
            (p.is_leaf() && p.leaf_is_full()) || (p.is_branch() && p.branch_is_full())
        });
        let root = p_guard.page();
        let new_height = root.header.height + 1;
        let l_row_id = root.header.start_row_id;
        let r_row_id = append.start_row_id;

        // create left child and copy all contents to it.
        let mut l_guard = self.allocate_node_page(pool_guard).await?;
        let l_page_id = l_guard.page_id();
        l_guard.page_mut().clone_from(p_guard.page());
        l_guard.page_mut().header.end_row_id = r_row_id; // update original page's end row id
        drop(l_guard);

        // We may need to create a sub-tree on the right side.
        // Because we disable branch split. We have to always construct right tree
        // as same height as left.
        let r_page_id = if root.header.height == 0 {
            let mut r_guard = self.allocate_node_page(pool_guard).await?;
            r_guard
                .page_mut()
                .init(0, r_row_id, append.count, append.page_id);
            r_guard.page_id()
        } else {
            self.create_sub_tree(
                pool_guard,
                root.header.height,
                r_row_id,
                append.count,
                append.page_id,
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
        Ok(append.finish())
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
        append: RowPageIndexAppend<'_>,
    ) -> Result<Validation<RowPageIndexInsert>> {
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
                    .insert_row_page_split_root(pool_guard, p_guard, append)
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
            leaf.page_mut()
                .init(0, append.start_row_id, append.count, append.page_id);
            debug_assert!(leaf.page_mut().header.end_row_id == append.end_row_id());
            leaf.page_id()
        } else {
            self.create_sub_tree(
                pool_guard,
                p_height - 1,
                append.start_row_id,
                append.count,
                append.page_id,
            )
            .await?
        };
        p_guard
            .page_mut()
            .branch_add_entry(PageEntry::new(append.start_row_id, c_page_id));
        drop(c_guard);
        Ok(Valid(append.finish()))
    }

    /// Inserts a row page id into the row-page index.
    #[inline]
    async fn insert_row_page(
        &self,
        pool_guard: &PoolGuard,
        count: u64,
        insert_page_id: PageID,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<Validation<RowPageIndexInsert>> {
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
            let append = RowPageIndexAppend {
                start_row_id,
                count,
                page_id: insert_page_id,
                redo_ctx,
            };
            p_guard
                .page_mut()
                .leaf_add_entry(start_row_id, count, insert_page_id);
            return Ok(Valid(append.finish()));
        }
        let end_row_id = p_guard.page().header.end_row_id;
        let append = RowPageIndexAppend {
            start_row_id: end_row_id,
            count,
            page_id: insert_page_id,
            redo_ctx,
        };
        if p_guard.page().leaf_is_full() {
            // Leaf is full, so we must add a new leaf to the row-page index.
            if stack.is_empty() {
                // root is full and already exclusive locked
                let res = self
                    .insert_row_page_split_root(pool_guard, p_guard, append)
                    .await?;
                return Ok(Valid(res));
            }
            return self
                .insert_row_page_to_new_leaf(pool_guard, &mut stack, p_guard, append)
                .await;
        }
        p_guard
            .page_mut()
            .leaf_add_entry(end_row_id, count, insert_page_id);
        Ok(Valid(append.finish()))
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
                Some((page.header.height, page.branch_entry(idx).page_id))
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
    async fn try_find_row(
        &self,
        pool_guard: &PoolGuard,
        row_id: RowID,
    ) -> Result<Validation<RowLocation>> {
        enum SearchStep {
            Found(RowLocation),
            Next(PageID),
        }

        let mut g = self
            .pool
            .get_page::<RowPageIndexNode>(pool_guard, self.root_page_id, LatchFallbackMode::Spin)
            .await?;
        loop {
            let step = match g.with_page_ref_validated(|page| {
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
            }) {
                Valid(step) => step,
                Invalid => return Ok(Invalid),
            };
            match step {
                SearchStep::Found(found) => return Ok(Valid(found)),
                SearchStep::Next(page_id) => {
                    g = match self
                        .pool
                        .get_child_page(pool_guard, &g, page_id, LatchFallbackMode::Spin)
                        .await?
                    {
                        Valid(g) => g,
                        Invalid => return Ok(Invalid),
                    };
                }
            }
        }
    }
}

/// Physical lookup target returned by row/column block-index search.
pub(crate) enum RowLocation {
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
pub(crate) struct RowPageIndexMemCursor<'a, P: 'static> {
    blk_idx: &'a RowPageIndex<P>,
    pool_guard: &'a PoolGuard,
    // The parent node of current located
    parent: Option<ParentPosition<PageSharedGuard<RowPageIndexNode>>>,
    child: Option<PageSharedGuard<RowPageIndexNode>>,
}

impl<P: BufferPool> RowPageIndexMemCursor<'_, P> {
    /// Seeks to the in-memory leaf that can serve `row_id`.
    #[inline]
    pub(crate) async fn seek(&mut self, row_id: RowID) -> Result<()> {
        loop {
            self.reset();
            let res = self.try_find_leaf_with_parent_in_mem(row_id).await?;
            verify_continue!(res);
            return Ok(());
        }
    }

    /// Returns current in-memory leaf and advances to the next leaf.
    #[inline]
    pub(crate) async fn next(&mut self) -> Result<Option<FacadePageGuard<RowPageIndexNode>>> {
        if let Some(child) = self.child.take() {
            return Ok(Some(child.facade(false)));
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
                    return Ok(None);
                }
                // otherwise, we rerun the search on given row id to get next leaf.
                while let Invalid = self.try_find_leaf_with_parent_in_mem(row_id).await? {
                    self.reset();
                }
                // A successful search always installs the located child.
                let child = self.child.take().unwrap();
                return Ok(Some(child.facade(false)));
            }
            // otherwise, we jump to next slot and get leaf node.
            let page_id = entries[next_idx].page_id;
            // This branch is entered through `parent.as_ref()`, so the mutable
            // parent position must still be installed.
            self.parent.as_mut().unwrap().idx = next_idx as isize; // update parent position.
            let child = self
                .blk_idx
                .pool
                .get_page::<RowPageIndexNode>(self.pool_guard, page_id, LatchFallbackMode::Shared)
                .await?
                .lock_shared_async()
                .await
                // The held parent prevents the indexed child from being
                // unlinked before this immediate latch conversion.
                .unwrap();
            return Ok(Some(child.facade(false)));
        }
        Ok(None)
    }

    #[inline]
    fn reset(&mut self) {
        self.parent.take();
        self.child.take();
    }

    #[inline]
    async fn try_find_leaf_with_parent_in_mem(&mut self, row_id: RowID) -> Result<Validation<()>> {
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
            .await?;
        'SEARCH: loop {
            let step = match g.with_page_ref_validated(|page| {
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
            }) {
                Valid(step) => step,
                Invalid => return Ok(Invalid),
            };
            if matches!(step, TraverseStep::Leaf) {
                // share lock for read
                let lock_result = match g.try_shared_either() {
                    Valid(lock_result) => lock_result,
                    Invalid => return Ok(Invalid),
                };
                match lock_result {
                    Left(c) => {
                        // share lock on child succeeds
                        self.child = Some(c);
                        return Ok(Valid(()));
                    }
                    Right(new_g) => {
                        // since we successfully lock parent node,
                        // that means the range of child can not change,
                        // so we can just wait for other thread finish its modification.
                        // NOTE: at this time, the parent is locked. That means SMO
                        // must acquire lock from top down, otherwise, deadlock will happen.
                        let Some(new_g) = new_g.lock_shared_async().await else {
                            return Ok(Invalid);
                        };
                        g = new_g.facade(false);
                        continue 'SEARCH;
                    }
                }
            }
            let TraverseStep::Branch { idx, page_id } = step else {
                // The leaf case returns or retries in the branch above.
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
                .await?;
            let c = match c {
                Valid(c) => c,
                Invalid => return Ok(Invalid),
            };
            let Some(parent_g) = g.lock_shared_async().await else {
                return Ok(Invalid);
            };
            self.parent = Some(ParentPosition {
                g: parent_g,
                idx: idx as isize,
            });
            g = c;
        }
    }
}

#[inline]
fn validate_leaf_prefix(
    leaf: &RowPageIndexNode,
    next_row_id: &mut RowID,
    end_row_id: RowID,
    expected_page_ids: &[PageID],
    expected_idx: &mut usize,
) {
    debug_assert!(leaf.is_leaf());
    let entries = leaf.leaf_entries();
    for (idx, entry) in entries.iter().enumerate() {
        if *expected_idx == expected_page_ids.len() {
            break;
        }
        assert_eq!(
            entry.row_id, *next_row_id,
            "checkpoint row-page prefix row-id mismatch at index {expected_idx}"
        );
        assert_eq!(
            entry.page_id, expected_page_ids[*expected_idx],
            "checkpoint row-page prefix page-id mismatch at index {expected_idx}"
        );
        let entry_end_row_id = entries
            .get(idx + 1)
            .map(|next| next.row_id)
            .unwrap_or(leaf.header.end_row_id);
        assert!(
            entry_end_row_id > entry.row_id && entry_end_row_id <= end_row_id,
            "checkpoint row-page prefix range mismatch: page_id={}, entry_start={}, entry_end={}, batch_end={end_row_id}",
            entry.page_id,
            entry.row_id,
            entry_end_row_id
        );
        *next_row_id = entry_end_row_id;
        *expected_idx += 1;
    }
}

// TODO(backlog 000159): prove whether a published row-page-index node can be
// absent under valid cleanup, eviction, and concurrent structural schedules
// before changing this recoverable internal error into an assertion.
#[inline]
fn row_page_index_missing(page_id: PageID) -> Error {
    Report::new(InternalError::RowPageMissing)
        .attach(format!("row-page-index node missing: page_id={page_id}"))
        .into()
}

#[inline]
fn cleanup_failed_insert_page<B: BufferPool>(
    mem_pool: &B,
    new_page: PageExclusiveGuard<RowPage>,
    err: InsertPageGuardError,
) -> Error {
    match err {
        InsertPageGuardError::Unpublished(err) => {
            // The row page was allocated locally and never published, so
            // reclaim it before surfacing the metadata-side failure.
            mem_pool.deallocate_page(new_page);
            err
        }
        InsertPageGuardError::Published(err) => {
            // The index entry is already visible. Leave the initialized empty
            // page allocated; dropping the guard only releases the latch.
            drop(new_page);
            err
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::frame::BufferFrame;
    use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
    use crate::buffer::page::{BufferPage, INVALID_PAGE_ID, Page, VersionedPageID};
    use crate::buffer::test_page_id;
    use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard, PoolRole};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
    };
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::error::{
        ErrorKind, FatalError, IoError, ResourceError, RuntimeError, RuntimeResult, Validation,
    };
    use crate::file::block_integrity::BLOCK_INTEGRITY_TRAILER_SIZE;
    use crate::id::{TableID, TrxID};
    use crate::latch::LatchFallbackMode;
    use crate::log::discover_redo_log_files;
    use crate::log::redo::DDLRedo;
    use crate::quiescent::{QuiescentBox, QuiescentGuard};
    use crate::recovery::stream::RedoReplayPlanner;
    use crate::value::ValKind;
    use futures::{FutureExt, future::join_all};
    use semistr::SemiStr;
    use std::future::Future;
    use std::io::Error as StdIoError;
    use std::mem::size_of;
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;

    fn owned_index_pool(pool_size: usize) -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(FixedBufferPool::with_capacity(PoolRole::Index, pool_size).unwrap())
    }

    fn owned_mem_pool(pool_size: usize) -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(FixedBufferPool::with_capacity(PoolRole::Mem, pool_size).unwrap())
    }

    fn fixed_pool_bytes(page_count: usize) -> usize {
        page_count * (size_of::<BufferFrame>() + size_of::<Page>())
    }

    async fn append_test_row_pages(
        index: &RowPageIndex<FixedBufferPool>,
        pool_guard: &PoolGuard,
        count: usize,
        rows_per_page: u64,
    ) -> Vec<PageID> {
        let mut page_ids = Vec::with_capacity(count);
        for idx in 0..count {
            let page_id = test_page_id(10_000 + idx as i32);
            loop {
                if let Valid(_) = index
                    .insert_row_page(pool_guard, rows_per_page, page_id, None)
                    .await
                    .expect("test row-page insertion should succeed")
                {
                    break;
                }
            }
            page_ids.push(page_id);
        }
        page_ids
    }

    #[test]
    fn test_row_page_index_node_reserves_checksum_footer() {
        assert_eq!(mem::size_of::<RowPageIndexNode>(), PAGE_SIZE);
        assert_eq!(
            ROW_PAGE_INDEX_NODE_FOOTER_SIZE,
            BLOCK_INTEGRITY_TRAILER_SIZE
        );
        assert_eq!(
            ROW_PAGE_INDEX_NODE_USABLE_SIZE,
            PAGE_SIZE - ROW_PAGE_INDEX_NODE_FOOTER_SIZE
        );
        assert_eq!(
            mem::offset_of!(RowPageIndexNode, data),
            ROW_PAGE_INDEX_NODE_HEADER_SIZE
        );
        assert_eq!(
            mem::offset_of!(RowPageIndexNode, footer),
            ROW_PAGE_INDEX_NODE_USABLE_SIZE
        );
        assert_eq!(
            NBR_ENTRIES_IN_BRANCH,
            (ROW_PAGE_INDEX_NODE_USABLE_SIZE - ROW_PAGE_INDEX_NODE_HEADER_SIZE) / ENTRY_SIZE
        );
        assert_eq!(NBR_ROW_PAGE_ENTRIES_IN_LEAF, NBR_ENTRIES_IN_BRANCH);
    }

    async fn fill_root_leaf_full<P: BufferPool>(blk_idx: &RowPageIndex<P>, pool_guard: &PoolGuard) {
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
        root.init_empty(0, RowID::new(0));
        for i in 0..NBR_ROW_PAGE_ENTRIES_IN_LEAF {
            root.leaf_add_entry(RowID::from(i), 1, test_page_id(i as i32));
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

        #[inline]
        fn set_fail_page_id(&self, fail_page_id: PageID) {
            self.fail_page_id
                .store(u64::from(fail_page_id), Ordering::Release);
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
        ) -> impl Future<Output = RuntimeResult<PageExclusiveGuard<T>>> + Send {
            self.inner.allocate_page(guard)
        }

        #[inline]
        fn allocate_page_at<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            page_id: PageID,
        ) -> impl Future<Output = RuntimeResult<PageExclusiveGuard<T>>> + Send {
            self.inner.allocate_page_at(guard, page_id)
        }

        #[inline]
        async fn get_page<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            page_id: PageID,
            mode: LatchFallbackMode,
        ) -> RuntimeResult<FacadePageGuard<T>> {
            if page_id == self.fail_page_id.load(Ordering::Acquire) {
                let source = StdIoError::from_raw_os_error(libc::EIO);
                return Err(Report::new(IoError::from(source.kind()))
                    .attach(format!("injected row-page-index access failure: {source}"))
                    .change_context(RuntimeError::BufferPageAccess)
                    .attach(format!(
                        "buffer_pool_type=fixed, buffer_pool_role=index, operation=get_page, page_id={page_id}"
                    )));
            }
            self.inner.get_page(guard, page_id, mode).await
        }

        #[inline]
        async fn get_page_versioned<T: BufferPage>(
            &self,
            guard: &PoolGuard,
            id: VersionedPageID,
            mode: LatchFallbackMode,
        ) -> RuntimeResult<Option<FacadePageGuard<T>>> {
            if id.page_id == self.fail_page_id.load(Ordering::Acquire) {
                let source = StdIoError::from_raw_os_error(libc::EIO);
                return Err(Report::new(IoError::from(source.kind()))
                    .attach(format!("injected versioned row-page-index access failure: {source}"))
                    .change_context(RuntimeError::BufferPageAccess)
                    .attach(format!(
                        "buffer_pool_type=fixed, buffer_pool_role=index, operation=get_page_versioned, page_id={}, generation={}",
                        id.page_id, id.generation
                    )));
            }
            self.inner.get_page_versioned(guard, id, mode).await
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
        ) -> impl Future<Output = RuntimeResult<Validation<FacadePageGuard<T>>>> + Send {
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
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_idx"))
                .build()
                .await
                .unwrap();
            {
                let metadata = make_test_metadata();
                let meta_guard = engine.inner().meta_pool.pool_guard();
                let blk_idx = RowPageIndex::new(
                    engine.inner().meta_pool.clone_inner(),
                    &meta_guard,
                    RowID::new(0),
                )
                .await
                .expect("test row-page-index construction should succeed");
                let mem_guard = engine.inner().mem_pool.pool_guard();
                let p1 = blk_idx
                    .get_insert_page(
                        &meta_guard,
                        &*engine.inner().mem_pool,
                        &mem_guard,
                        &metadata.col,
                        100,
                        None,
                    )
                    .await
                    .expect("test insert-page allocation should succeed");
                let pid1 = p1.page_id();
                let p1 = p1.downgrade().lock_exclusive_async().await.unwrap();
                blk_idx.cache_exclusive_insert_page(p1);
                assert_eq!(blk_idx.insert_free_list.lock().len(), 1);
                let p2 = blk_idx
                    .get_insert_page(
                        &meta_guard,
                        &*engine.inner().mem_pool,
                        &mem_guard,
                        &metadata.col,
                        100,
                        None,
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
    fn test_row_page_index_free_list_exclusive() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_idx"))
                .build()
                .await
                .unwrap();
            {
                let metadata = make_test_metadata();
                let meta_guard = engine.inner().meta_pool.pool_guard();
                let blk_idx = RowPageIndex::new(
                    engine.inner().meta_pool.clone_inner(),
                    &meta_guard,
                    RowID::new(0),
                )
                .await
                .expect("test row-page-index construction should succeed");
                let mem_guard = engine.inner().mem_pool.pool_guard();
                let p1 = blk_idx
                    .get_insert_page_exclusive(
                        &meta_guard,
                        &*engine.inner().mem_pool,
                        &mem_guard,
                        &metadata.col,
                        100,
                        None,
                    )
                    .await
                    .expect("test insert-page allocation should succeed");
                let pid1 = p1.page_id();
                blk_idx.cache_exclusive_insert_page(p1);
                assert_eq!(blk_idx.insert_free_list.lock().len(), 1);
                let p2 = blk_idx
                    .get_insert_page_exclusive(
                        &meta_guard,
                        &*engine.inner().mem_pool,
                        &mem_guard,
                        &metadata.col,
                        100,
                        None,
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
    fn test_row_page_index_free_list_skips_recycled_page_versions() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(fixed_pool_bytes(1));
            let mem_pool = owned_mem_pool(fixed_pool_bytes(1));
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");

            let old_page = mem_pool
                .allocate_page::<RowPage>(&mem_guard)
                .await
                .expect("old row-page allocation should succeed");
            let stale_id = old_page.versioned_page_id();
            mem_pool.deallocate_page(old_page);
            let replacement = mem_pool
                .allocate_page::<RowPage>(&mem_guard)
                .await
                .expect("replacement row-page allocation should succeed");
            let replacement_id = replacement.versioned_page_id();
            assert_eq!(replacement_id.page_id, stale_id.page_id);
            assert_ne!(replacement_id.generation, stale_id.generation);
            drop(replacement);

            blk_idx.insert_free_list.lock().push(stale_id);
            assert!(
                blk_idx
                    .get_insert_page_from_free_list(&*mem_pool, &mem_guard)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(blk_idx.insert_free_list.lock().is_empty());

            let replacement = mem_pool
                .get_page_versioned::<RowPage>(
                    &mem_guard,
                    replacement_id,
                    LatchFallbackMode::Exclusive,
                )
                .await
                .unwrap()
                .unwrap()
                .lock_exclusive_async()
                .await
                .unwrap();
            mem_pool.deallocate_page(replacement);
            let replacement = mem_pool
                .allocate_page::<RowPage>(&mem_guard)
                .await
                .expect("second replacement row-page allocation should succeed");
            assert_eq!(replacement.page_id(), replacement_id.page_id);
            assert_ne!(
                replacement.versioned_page_id().generation,
                replacement_id.generation
            );
            drop(replacement);

            blk_idx.insert_free_list.lock().push(replacement_id);
            assert!(
                blk_idx
                    .get_insert_page_exclusive_from_free_list(&*mem_pool, &mem_guard)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(blk_idx.insert_free_list.lock().is_empty());
        })
    }

    #[test]
    fn test_row_page_index_destroy_reclaims_leaf_row_pages_and_root() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(64 * 1024 * 1024);
            let mem_pool = owned_mem_pool(64 * 1024 * 1024);
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");

            for _ in 0..3 {
                let page = blk_idx
                    .get_insert_page_exclusive(
                        &meta_guard,
                        &*mem_pool,
                        &mem_guard,
                        &metadata.col,
                        100,
                        None,
                    )
                    .await
                    .expect("test insert-page allocation should succeed");
                drop(page);
            }
            assert_eq!((*meta_pool).allocated(), 1);
            assert_eq!((*mem_pool).allocated(), 3);

            blk_idx
                .destroy(&meta_guard, &*mem_pool, &mem_guard, RowID::new(0))
                .await
                .unwrap();
            assert_eq!((*meta_pool).allocated(), 0);
            assert_eq!((*mem_pool).allocated(), 0);
        });
    }

    #[test]
    fn test_row_page_index_destroy_rejects_entry_below_pivot_before_deallocation() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(64 * 1024 * 1024);
            let mem_pool = owned_mem_pool(64 * 1024 * 1024);
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let index = RowPageIndex::new(meta_pool.guard(), &meta_guard, RowID::new(0))
                .await
                .unwrap();
            for _ in 0..2 {
                drop(
                    index
                        .get_insert_page_exclusive(
                            &meta_guard,
                            &*mem_pool,
                            &mem_guard,
                            &metadata.col,
                            100,
                            None,
                        )
                        .await
                        .unwrap(),
                );
            }

            let panic = AssertUnwindSafe(index.destroy(
                &meta_guard,
                &*mem_pool,
                &mem_guard,
                RowID::new(100),
            ))
            .catch_unwind()
            .await;
            assert!(panic.is_err());
            assert_eq!((*meta_pool).allocated(), 1);
            assert_eq!((*mem_pool).allocated(), 2);
        });
    }

    #[test]
    fn test_get_insert_page_returns_error_on_free_list_io_failure() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(64 * 1024 * 1024);
            let mem_pool = owned_mem_pool(64 * 1024 * 1024);
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");

            let page_guard = blk_idx
                .get_insert_page_exclusive(
                    &meta_guard,
                    &*mem_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    None,
                )
                .await
                .expect("test insert-page allocation should succeed");
            let page_id = page_guard.page_id();
            blk_idx.cache_exclusive_insert_page(page_guard);

            let failing_pool = FailingInsertPagePool::new(mem_pool.guard(), page_id);
            let res = blk_idx
                .get_insert_page(
                    &meta_guard,
                    &failing_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    None,
                )
                .await;
            let err = match res {
                Ok(_) => panic!("expected free-list page reload failure"),
                Err(err) => err,
            };
            assert!(err.report().downcast_ref::<IoError>().is_some());
        });
    }

    #[test]
    fn test_get_insert_page_exclusive_propagates_free_list_io_failure() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(64 * 1024 * 1024);
            let mem_pool = owned_mem_pool(64 * 1024 * 1024);
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");

            let page_guard = blk_idx
                .get_insert_page_exclusive(
                    &meta_guard,
                    &*mem_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    None,
                )
                .await
                .expect("test insert-page allocation should succeed");
            let failed_page_id = page_guard.page_id();
            blk_idx.cache_exclusive_insert_page(page_guard);

            let failing_pool = FailingInsertPagePool::new(mem_pool.guard(), failed_page_id);
            let res = blk_idx
                .get_insert_page_exclusive(
                    &meta_guard,
                    &failing_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    None,
                )
                .await;
            let err = match res {
                Ok(_) => panic!("expected exclusive free-list page reload failure"),
                Err(err) => err,
            };
            assert!(err.report().downcast_ref::<IoError>().is_some());
            assert!(blk_idx.insert_free_list.lock().is_empty());
        });
    }

    #[test]
    fn test_get_insert_page_reclaims_allocated_row_page_on_insert_error() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(fixed_pool_bytes(1));
            let mem_pool = owned_mem_pool(fixed_pool_bytes(1));
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");
            fill_root_leaf_full(&blk_idx, &meta_guard).await;

            let err = match blk_idx
                .get_insert_page(
                    &meta_guard,
                    &*mem_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    None,
                )
                .await
            {
                Ok(_) => panic!("metadata split should fail in one-page meta pool"),
                Err(err) => err,
            };
            assert_eq!(
                err.report().downcast_ref::<ResourceError>().copied(),
                Some(ResourceError::BufferPoolFull)
            );
            assert_eq!(mem_pool.allocated(), 0);
        });
    }

    #[test]
    fn test_get_insert_page_exclusive_reclaims_allocated_row_page_on_insert_error() {
        smol::block_on(async {
            let meta_pool = owned_index_pool(fixed_pool_bytes(1));
            let mem_pool = owned_mem_pool(fixed_pool_bytes(1));
            let meta_guard = (*meta_pool).pool_guard();
            let mem_guard = (*mem_pool).pool_guard();
            let metadata = make_test_metadata();
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");
            fill_root_leaf_full(&blk_idx, &meta_guard).await;

            let err = match blk_idx
                .get_insert_page_exclusive(
                    &meta_guard,
                    &*mem_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    None,
                )
                .await
            {
                Ok(_) => panic!("metadata split should fail in one-page meta pool"),
                Err(err) => err,
            };
            assert_eq!(
                err.report().downcast_ref::<ResourceError>().copied(),
                Some(ResourceError::BufferPoolFull)
            );
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
            let blk_idx = RowPageIndex::new(meta_pool.guard(), &meta_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");
            fill_root_leaf_full(&blk_idx, &meta_guard).await;

            let page_id = test_page_id(0);
            let err = match blk_idx
                .allocate_row_page_at(
                    &meta_guard,
                    &*mem_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    page_id,
                )
                .await
            {
                Ok(_) => panic!("metadata split should fail in one-page meta pool"),
                Err(err) => err,
            };
            assert_eq!(
                err.report().downcast_ref::<ResourceError>().copied(),
                Some(ResourceError::BufferPoolFull)
            );
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
                        .role(PoolRole::Mem)
                        .max_mem_size(100usize * 1024 * 1024)
                        .max_file_size(1024usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_idx"))
                .build()
                .await
                .unwrap();
            {
                let metadata = make_test_metadata();
                let meta_guard = engine.inner().meta_pool.pool_guard();
                let blk_idx = RowPageIndex::new(
                    engine.inner().meta_pool.clone_inner(),
                    &meta_guard,
                    RowID::new(0),
                )
                .await
                .expect("test row-page-index construction should succeed");
                let mem_guard = engine.inner().mem_pool.pool_guard();
                for _ in 0..row_pages {
                    let _ = blk_idx
                        .get_insert_page(
                            &meta_guard,
                            &*engine.inner().mem_pool,
                            &mem_guard,
                            &metadata.col,
                            100,
                            None,
                        )
                        .await
                        .expect("test insert-page allocation should succeed");
                }
                let mut count = 0usize;
                let mut cursor = blk_idx.mem_cursor(&meta_guard);
                cursor.seek(RowID::new(0)).await.unwrap();
                while let Some(res) = cursor.next().await.unwrap() {
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
            let blk_idx = RowPageIndex::new(pool.guard(), &pool_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");

            let overflow_entries = 3usize;
            let row_pages = NBR_ROW_PAGE_ENTRIES_IN_LEAF + overflow_entries;
            for row_page_id in 0..row_pages {
                loop {
                    if let Valid(_) = blk_idx
                        .insert_row_page(&pool_guard, 1, PageID::from(row_page_id), None)
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
                    .get_page::<RowPageIndexNode>(
                        &pool_guard,
                        blk_idx.root_page_id(),
                        LatchFallbackMode::Spin,
                    )
                    .await
                    .expect("buffer-pool read failed in test")
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
            cursor.seek(RowID::new(0)).await.unwrap();
            while let Some(res) = cursor.next().await.unwrap() {
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
            assert_eq!(leaf_headers[0].0, RowID::new(0));
            assert_eq!(leaf_headers[0].2, NBR_ROW_PAGE_ENTRIES_IN_LEAF);
            assert_eq!(leaf_headers[1].0, leaf_headers[0].1);
            assert_eq!(leaf_headers[1].1, RowID::from(row_pages));
            assert_eq!(leaf_headers[1].2, overflow_entries);
            assert_eq!(cursor_leaf_page_ids, root_leaf_page_ids);
            assert!(cursor.next().await.unwrap().is_none());
        })
    }

    #[test]
    fn test_prune_checkpoint_prefix_in_root_leaf_and_reset_empty_root() {
        smol::block_on(async {
            let pool = owned_index_pool(64 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let index = RowPageIndex::new(pool.guard(), &pool_guard, RowID::new(0))
                .await
                .unwrap();
            let page_ids = append_test_row_pages(&index, &pool_guard, 4, 10).await;

            let before_height = index.height();
            let panic = AssertUnwindSafe(index.prune_checkpoint_prefix(
                &pool_guard,
                RowID::new(0),
                RowID::new(20),
                &[page_ids[1], page_ids[0]],
            ))
            .catch_unwind()
            .await;
            assert!(panic.is_err());
            assert_eq!(index.height(), before_height);

            let pruned = index
                .prune_checkpoint_prefix(&pool_guard, RowID::new(0), RowID::new(20), &page_ids[..2])
                .await
                .unwrap();
            assert_eq!(pruned.page_ids.as_ref(), &page_ids[..2]);
            assert_eq!(pruned.reclaimed_nodes, 0);
            assert_eq!(pruned.collapsed_levels, 0);
            assert!(matches!(
                index.find_row(&pool_guard, RowID::new(19)).await.unwrap(),
                RowLocation::NotFound
            ));
            assert!(matches!(
                index.find_row(&pool_guard, RowID::new(20)).await.unwrap(),
                RowLocation::RowPage(page_id) if page_id == page_ids[2]
            ));

            let pruned = index
                .prune_checkpoint_prefix(
                    &pool_guard,
                    RowID::new(20),
                    RowID::new(40),
                    &page_ids[2..],
                )
                .await
                .unwrap();
            assert_eq!(pruned.page_ids.as_ref(), &page_ids[2..]);
            assert_eq!(index.height(), 0);

            let appended = test_page_id(20_000);
            let inserted = loop {
                if let Valid(inserted) = index
                    .insert_row_page(&pool_guard, 5, appended, None)
                    .await
                    .unwrap()
                {
                    break inserted;
                }
            };
            assert_eq!(inserted.start_row_id, RowID::new(40));
            assert_eq!(inserted.end_row_id, RowID::new(45));
        })
    }

    #[test]
    fn test_prune_checkpoint_prefix_reclaims_leaf_and_collapses_root() {
        smol::block_on(async {
            let pool = owned_index_pool(128 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let index = RowPageIndex::new(pool.guard(), &pool_guard, RowID::new(0))
                .await
                .unwrap();
            let total = NBR_ROW_PAGE_ENTRIES_IN_LEAF + 3;
            let page_ids = append_test_row_pages(&index, &pool_guard, total, 1).await;
            assert_eq!(index.height(), 1);

            let pruned = index
                .prune_checkpoint_prefix(
                    &pool_guard,
                    RowID::new(0),
                    RowID::from(NBR_ROW_PAGE_ENTRIES_IN_LEAF + 1),
                    &page_ids[..NBR_ROW_PAGE_ENTRIES_IN_LEAF + 1],
                )
                .await
                .unwrap();
            assert_eq!(
                pruned.page_ids.as_ref(),
                &page_ids[..NBR_ROW_PAGE_ENTRIES_IN_LEAF + 1]
            );
            assert_eq!(pruned.reclaimed_nodes, 2);
            assert_eq!(pruned.collapsed_levels, 1);
            assert_eq!(index.height(), 0);
            assert!(matches!(
                index
                    .find_row(
                        &pool_guard,
                        RowID::from(NBR_ROW_PAGE_ENTRIES_IN_LEAF + 1),
                    )
                    .await
                    .unwrap(),
                RowLocation::RowPage(page_id)
                    if page_id == page_ids[NBR_ROW_PAGE_ENTRIES_IN_LEAF + 1]
            ));
        })
    }

    #[test]
    fn test_prune_checkpoint_prefix_across_multiple_branch_levels() {
        smol::block_on(async {
            let pool = owned_index_pool(64 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let index = RowPageIndex::new(pool.guard(), &pool_guard, RowID::new(0))
                .await
                .unwrap();

            async fn leaf(
                pool: &FixedBufferPool,
                guard: &PoolGuard,
                entries: &[(u64, i32)],
                end: u64,
            ) -> PageID {
                let mut node = pool.allocate_page::<RowPageIndexNode>(guard).await.unwrap();
                node.page_mut().init_empty(0, RowID::new(entries[0].0));
                for (start, page_id) in entries {
                    let count = end - start;
                    node.page_mut().leaf_add_entry(
                        RowID::new(*start),
                        count,
                        test_page_id(*page_id),
                    );
                }
                node.page_mut().header.end_row_id = RowID::new(end);
                node.page_id()
            }

            let leaf_a = leaf(&pool, &pool_guard, &[(0, 1)], 10).await;
            let leaf_b = leaf(&pool, &pool_guard, &[(10, 2)], 20).await;
            let leaf_c = leaf(&pool, &pool_guard, &[(20, 3), (25, 4)], 30).await;
            let leaf_d = leaf(&pool, &pool_guard, &[(30, 5)], 40).await;
            let mut branch_a = pool
                .allocate_page::<RowPageIndexNode>(&pool_guard)
                .await
                .unwrap();
            branch_a.page_mut().init_empty(1, RowID::new(0));
            branch_a
                .page_mut()
                .branch_add_entry(PageEntry::new(RowID::new(0), leaf_a));
            branch_a
                .page_mut()
                .branch_add_entry(PageEntry::new(RowID::new(10), leaf_b));
            branch_a.page_mut().header.end_row_id = RowID::new(20);
            let branch_a_id = branch_a.page_id();
            drop(branch_a);
            let mut branch_b = pool
                .allocate_page::<RowPageIndexNode>(&pool_guard)
                .await
                .unwrap();
            branch_b.page_mut().init_empty(1, RowID::new(20));
            branch_b
                .page_mut()
                .branch_add_entry(PageEntry::new(RowID::new(20), leaf_c));
            branch_b
                .page_mut()
                .branch_add_entry(PageEntry::new(RowID::new(30), leaf_d));
            let branch_b_id = branch_b.page_id();
            drop(branch_b);
            let mut root = pool
                .get_page::<RowPageIndexNode>(
                    &pool_guard,
                    index.root_page_id(),
                    LatchFallbackMode::Exclusive,
                )
                .await
                .unwrap()
                .lock_exclusive_async()
                .await
                .unwrap();
            root.page_mut().init_empty(2, RowID::new(0));
            root.page_mut()
                .branch_add_entry(PageEntry::new(RowID::new(0), branch_a_id));
            root.page_mut()
                .branch_add_entry(PageEntry::new(RowID::new(20), branch_b_id));
            index.height.store(2, Ordering::Relaxed);
            drop(root);

            let expected = [test_page_id(1), test_page_id(2), test_page_id(3)];
            let pruned = index
                .prune_checkpoint_prefix(&pool_guard, RowID::new(0), RowID::new(25), &expected)
                .await
                .unwrap();
            assert_eq!(pruned.page_ids.as_ref(), expected);
            assert_eq!(pruned.reclaimed_nodes, 4);
            assert_eq!(pruned.collapsed_levels, 1);
            assert_eq!(index.height(), 1);
            assert!(matches!(
                index.find_row(&pool_guard, RowID::new(24)).await.unwrap(),
                RowLocation::NotFound
            ));
            assert!(matches!(
                index.find_row(&pool_guard, RowID::new(25)).await.unwrap(),
                RowLocation::RowPage(page_id) if page_id == test_page_id(4)
            ));
            assert!(matches!(
                index.find_row(&pool_guard, RowID::new(30)).await.unwrap(),
                RowLocation::RowPage(page_id) if page_id == test_page_id(5)
            ));
        })
    }

    #[test]
    fn test_row_page_index_search() {
        smol::block_on(async {
            let pool = owned_index_pool(512usize * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let blk_idx = RowPageIndex::new(pool.guard(), &pool_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");
            let row_pages = 5000usize;
            let rows_per_page = 100usize;
            for i in 0..row_pages {
                loop {
                    if let Valid(_) = blk_idx
                        .insert_row_page(&pool_guard, rows_per_page as u64, PageID::from(i), None)
                        .await
                        .expect("test row-page insertion should succeed")
                    {
                        break;
                    }
                }
            }
            for i in 0..row_pages {
                let row_id = RowID::from(i * rows_per_page + rows_per_page / 2);
                match blk_idx
                    .find_row(&pool_guard, row_id)
                    .await
                    .expect("test row-page lookup should succeed")
                {
                    RowLocation::RowPage(page_id) => assert_eq!(page_id, PageID::from(i)),
                    _ => panic!("invalid search result for i={i}"),
                }
            }
            assert!(matches!(
                blk_idx
                    .find_row(&pool_guard, RowID::from(row_pages * rows_per_page))
                    .await
                    .expect("test row-page lookup should succeed"),
                RowLocation::NotFound
            ));
        })
    }

    #[test]
    fn test_row_page_index_find_row_returns_error_on_root_lookup_failure() {
        smol::block_on(async {
            let inner = owned_index_pool(64 * 1024 * 1024);
            let pool =
                QuiescentBox::new(FailingInsertPagePool::new(inner.guard(), INVALID_PAGE_ID));
            let pool_guard = (*pool).pool_guard();
            let blk_idx = RowPageIndex::new(pool.guard(), &pool_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");
            pool.set_fail_page_id(blk_idx.root_page_id());

            let err = match blk_idx.find_row(&pool_guard, RowID::new(0)).await {
                Ok(_location) => panic!("expected lookup error"),
                Err(err) => err,
            };
            assert!(err.is_kind(ErrorKind::Runtime));
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::BufferPageAccess)
            );
            assert_eq!(
                err.report()
                    .downcast_ref::<IoError>()
                    .copied()
                    .map(IoError::kind),
                Some(StdIoError::from_raw_os_error(libc::EIO).kind())
            );
        })
    }

    #[test]
    fn test_row_page_index_split() {
        smol::block_on(async {
            let pool = owned_index_pool(1024usize * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let blk_idx = RowPageIndex::new(pool.guard(), &pool_guard, RowID::new(0))
                .await
                .expect("test row-page-index construction should succeed");
            assert_eq!(blk_idx.height(), 0);

            for row_page_id in 0..10000 {
                loop {
                    if let Valid(_) = blk_idx
                        .insert_row_page(&pool_guard, 100, test_page_id(row_page_id), None)
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
            r_g.page_mut()
                .init(0, RowID::new(50000), 10000, test_page_id(10001));
            r_g.page_mut().header.count = NBR_ROW_PAGE_ENTRIES_IN_LEAF as u32;
            let r_page_id = r_g.page_id();
            drop(r_g);
            root.page_mut().branch_last_entry_mut().row_id = RowID::new(50000);
            root.page_mut().branch_last_entry_mut().page_id = r_page_id;
            drop(root);

            loop {
                if let Valid(_) = blk_idx
                    .insert_row_page(&pool_guard, 100, test_page_id(20000), None)
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
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_idx"))
                .build()
                .await
                .unwrap();
            {
                let metadata = make_test_metadata();
                let meta_guard = engine.inner().meta_pool.pool_guard();
                let blk_idx = RowPageIndex::new(
                    engine.inner().meta_pool.clone_inner(),
                    &meta_guard,
                    RowID::new(0),
                )
                .await
                .expect("test row-page-index construction should succeed");
                let mem_guard = engine.inner().mem_pool.pool_guard();
                let redo_ctx = RowPageCreateRedoCtx::new(
                    &engine.inner().trx_sys,
                    &engine.inner().poisoner,
                    TableID::new(104),
                );
                let page_guard = blk_idx
                    .get_insert_page(
                        &meta_guard,
                        &*engine.inner().mem_pool,
                        &mem_guard,
                        &metadata.col,
                        100,
                        Some(redo_ctx),
                    )
                    .await
                    .expect("test insert-page allocation should succeed");
                let create_cts = page_guard.unwrap_vmap().create_cts();
                assert!(create_cts > TrxID::new(0));

                let page_id = page_guard.page_id();
                let page_guard = page_guard.downgrade().lock_exclusive_async().await.unwrap();
                blk_idx.cache_exclusive_insert_page(page_guard);

                let reused_page = blk_idx
                    .get_insert_page(
                        &meta_guard,
                        &*engine.inner().mem_pool,
                        &mem_guard,
                        &metadata.col,
                        100,
                        Some(redo_ctx),
                    )
                    .await
                    .expect("test insert-page allocation should succeed");
                assert_eq!(reused_page.page_id(), page_id);
            }
            engine.shutdown().unwrap();

            let mut create_row_page_logs = 0usize;
            let file_prefix = temp_dir.path().join("redo_row_page_idx");
            let file_prefix = file_prefix.to_str().unwrap();
            let logs = discover_redo_log_files(file_prefix, 0, false).unwrap();
            let planner = RedoReplayPlanner::new(logs);
            let mut stream = planner.plan_recovery(TrxID::new(0), 1).unwrap().stream;
            while let Some(log) = stream.try_next().await.unwrap() {
                if let Some(ddl) = log.payload.ddl.as_deref()
                    && matches!(
                        ddl,
                        DDLRedo::CreateRowPage { table_id, .. }
                            if *table_id == TableID::new(104)
                    )
                {
                    create_row_page_logs += 1;
                }
            }
            assert_eq!(create_row_page_logs, 1);
        })
    }

    #[test]
    fn test_row_page_index_redo_failure_keeps_published_page_initialized() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(temp_dir.path().to_path_buf())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_failure"))
                .build()
                .await
                .unwrap();
            let metadata = make_test_metadata();
            let meta_guard = engine.inner().meta_pool.pool_guard();
            let blk_idx = RowPageIndex::new(
                engine.inner().meta_pool.clone_inner(),
                &meta_guard,
                RowID::new(0),
            )
            .await
            .expect("test row-page-index construction should succeed");
            let mem_guard = engine.inner().mem_pool.pool_guard();
            let redo_ctx = RowPageCreateRedoCtx::new(
                &engine.inner().trx_sys,
                &engine.inner().poisoner,
                TableID::new(206),
            );
            let _ = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test redo write failure"));

            let err = match blk_idx
                .get_insert_page(
                    &meta_guard,
                    &*engine.inner().mem_pool,
                    &mem_guard,
                    &metadata.col,
                    100,
                    Some(redo_ctx),
                )
                .await
            {
                Ok(_) => panic!("poisoned row-page create redo should fail"),
                Err(err) => err,
            };

            assert_eq!(err.kind(), ErrorKind::Fatal);
            let page_id = match blk_idx
                .find_row(&meta_guard, RowID::new(0))
                .await
                .expect("published row-page index entry should remain readable")
            {
                RowLocation::RowPage(page_id) => page_id,
                RowLocation::LwcBlock { .. } | RowLocation::NotFound => {
                    panic!("expected published row page")
                }
            };
            let page = engine
                .inner()
                .mem_pool
                .get_page::<RowPage>(&mem_guard, page_id, LatchFallbackMode::Shared)
                .await
                .expect("published row page should remain allocated")
                .lock_shared_async()
                .await
                .expect("published row page generation should remain current");
            assert_eq!(page.page().header.start_row_id, RowID::new(0));
            assert_eq!(page.page().header.max_row_count, 100);
            assert_eq!(page.page().header.row_count(), 0);
        })
    }

    #[test]
    fn test_row_page_index_create_row_page_redo_follows_append_order() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let table_id = TableID::new(205);
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(TrxSysConfig::default().log_file_stem("redo_row_page_order"))
                .build()
                .await
                .unwrap();
            {
                let meta_pool = &engine.inner().meta_pool;
                let meta_guard = meta_pool.pool_guard();
                let blk_idx =
                    RowPageIndex::new(meta_pool.clone_inner(), &meta_guard, RowID::new(0))
                        .await
                        .expect("test row-page-index construction should succeed");
                let redo_ctx = RowPageCreateRedoCtx::new(
                    &engine.inner().trx_sys,
                    &engine.inner().poisoner,
                    table_id,
                );
                let total_pages = NBR_ROW_PAGE_ENTRIES_IN_LEAF + 64;
                let worker_count = 8usize;
                let pages_per_worker = total_pages.div_ceil(worker_count);
                let jobs = (0..worker_count).map(|worker| {
                    let blk_idx = &blk_idx;
                    let meta_pool_ref = meta_pool;
                    async move {
                        let pool_guard = meta_pool_ref.pool_guard();
                        let start = worker * pages_per_worker;
                        let end = (start + pages_per_worker).min(total_pages);
                        for page_no in start..end {
                            loop {
                                match blk_idx
                                    .insert_row_page(
                                        &pool_guard,
                                        1,
                                        test_page_id(10_000 + page_no as i32),
                                        Some(redo_ctx),
                                    )
                                    .await
                                    .expect("test row-page insertion should succeed")
                                {
                                    Valid(inserted) => {
                                        assert!(matches!(inserted.create_redo, Ok(Some(_))));
                                        break;
                                    }
                                    Invalid => {}
                                }
                            }
                        }
                    }
                });
                join_all(jobs).await;
            }
            engine.shutdown().unwrap();

            let file_prefix = temp_dir.path().join("redo_row_page_order");
            let file_prefix = file_prefix.to_str().unwrap();
            let logs = discover_redo_log_files(file_prefix, 0, false).unwrap();
            let planner = RedoReplayPlanner::new(logs);
            let mut stream = planner.plan_recovery(TrxID::new(0), 1).unwrap().stream;
            let mut expected_start = RowID::new(0);
            let mut create_row_page_logs = 0usize;
            while let Some(log) = stream.try_next().await.unwrap() {
                let Some(DDLRedo::CreateRowPage {
                    table_id: redo_table_id,
                    start_row_id,
                    end_row_id,
                    ..
                }) = log.payload.ddl.as_deref()
                else {
                    continue;
                };
                if *redo_table_id != table_id {
                    continue;
                }
                assert_eq!(*start_row_id, expected_start);
                expected_start = *end_row_id;
                create_row_page_logs += 1;
            }
            assert_eq!(create_row_page_logs, NBR_ROW_PAGE_ENTRIES_IN_LEAF + 64);
        })
    }

    fn first_i32_unique_index() -> IndexSpec {
        IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)
    }

    fn make_test_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec {
                    column_name: SemiStr::new("id"),
                    column_type: ValKind::I32,
                    column_attributes: ColumnAttributes::empty(),
                }],
                vec![first_i32_unique_index()],
            )
            .expect("valid table metadata"),
        )
    }
}
