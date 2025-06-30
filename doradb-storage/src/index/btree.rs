use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::page::PageID;
use crate::buffer::{BufferPool, FixedBufferPool};
use crate::error::Validation;
use crate::error::Validation::{Invalid, Valid};
use crate::index::btree_node::{BTreeNode, SearchResult};
use crate::latch::LatchFallbackMode;
use crate::trx::TrxID;
use std::sync::atomic::{AtomicUsize, Ordering};

/// BTreeValue is the value type stored in leaf node.
/// In branch node, the value type is always page id,
/// which is a logical pointer to child node.
///
/// There are two implementations of BTreeValue.
/// 1. RowID(u64), which supports unique index.
/// 2. single byte(u8), which supports non-unique index.
///    Non-unique index is a bit complicated.
///    The key of non-unique index is user-defined
///    key followed by RowID, to make it unique for
///    B-tree operations(e.g. deletion).
///    As a consequence, if we still use RowID as
///    value type, it's waste of space.
///    Instead, we can only store single byte as its
///    value, in order to represent delete bit.
///    If we want to retrieve value, we can always
///    extract last 8 byte from key and convert it
///    to RowID.
pub trait BTreeValue: Copy + PartialEq + Eq {
    const INVALID_VALUE: Self;

    /// Marks given value as deleted.
    fn deleted(self) -> Self;

    /// Returns value except delete bit.
    fn value(self) -> Self;

    /// Returns whether this value is marked as deleted.
    fn is_deleted(self) -> bool;
}

trait OkThen: Sized {
    fn is_ok(&self) -> bool;

    #[inline]
    fn ok_then<F: FnOnce()>(self, f: F) -> Self {
        if self.is_ok() {
            f()
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeInsert {
    Ok,
    DuplicateKey,
}

impl OkThen for BTreeInsert {
    #[inline]
    fn is_ok(&self) -> bool {
        matches!(self, BTreeInsert::Ok)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeDelete {
    Ok,
    NotFound,
    ValueMismatch,
}

impl OkThen for BTreeDelete {
    #[inline]
    fn is_ok(&self) -> bool {
        matches!(self, BTreeDelete::Ok)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeUpdate<V: BTreeValue> {
    Ok(V),
    NotFound,
    ValueMismatch(V),
}

impl<V: BTreeValue> OkThen for BTreeUpdate<V> {
    #[inline]
    fn is_ok(&self) -> bool {
        matches!(self, BTreeUpdate::Ok(_))
    }
}

pub struct BTree {
    root: PageID,
    /// Height of this btree.
    height: AtomicUsize,
    // buffer pool to hold this index structure.
    // todo: switch to ShadowBufferPool, which
    // supports CoW operations.
    pool: &'static FixedBufferPool,
}

impl BTree {
    /// Create a new B-Tree index.
    #[inline]
    pub async fn new(pool: &'static FixedBufferPool, ts: TrxID) -> Self {
        let mut g = pool.allocate_page::<BTreeNode>().await;
        let page_id = g.page_id();
        let page = g.page_mut();
        page.init(0, ts, &[], PageID::INVALID_VALUE, &[]);
        BTree {
            root: page_id,
            height: AtomicUsize::new(0),
            pool,
        }
    }

    /// Returns height of this btree.
    #[inline]
    pub fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }

    /// Lookup for a key and return associated value if exists.
    ///
    /// This method does not care about delete bit, so a marked deleted value
    /// can also be returned and caller need to take care of it.
    #[inline]
    pub async fn lookup_optimistic<V: BTreeValue>(&self, key: &[u8]) -> BTreeLookup<V> {
        loop {
            let res = self.try_lookup_optimistic(key).await;
            let res = verify_continue!(res);
            return res;
        }
    }

    /// Insert a new key value pair into the tree.
    #[inline]
    pub async fn insert<V: BTreeValue>(&self, key: &[u8], value: V, ts: TrxID) -> BTreeInsert {
        // Stack holds the path from root to leaf.
        let mut stack = vec![];
        loop {
            stack.clear();
            let res = self
                .try_find_leaf(key, LatchFallbackMode::Exclusive, Some(&mut stack))
                .await;
            let mut res = verify_continue!(res);
            verify_continue!(res.try_exclusive());
            let mut g = res.must_exclusive();
            let node = g.page_mut();
            let idx = match node.search::<V>(key) {
                SearchResult::GreaterThan(idx) => idx + 1,
                SearchResult::Equal(..) | SearchResult::EqualDeleted(..) => {
                    // Do not allow insert even if same key is marked as deleted.
                    return BTreeInsert::DuplicateKey;
                }
                SearchResult::LessThanAllSlots => 0,
                SearchResult::LessThanLowerFence | SearchResult::GreaterEqualUpperFence => {
                    unreachable!()
                }
            };
            // check whether the space is enough for this key value pair.
            if !node.can_insert(key) {
                debug_assert!(node.count() > 1);
                if stack.is_empty() {
                    // Root is leaf and full, should split.
                    self.split_root::<V>(node, true, ts).await;
                    continue;
                }
                match self.try_split_bottom_up::<V>(&mut stack, g, ts).await {
                    // If split is done or tree structure has been changed by other thread,
                    // we can retry the insert.
                    BTreeSplit::Ok | BTreeSplit::Inconsistent => (),
                    BTreeSplit::FullBranch(mut split) => loop {
                        match self
                            .try_split_top_down(
                                &split.lower_fence_key(),
                                split.page_id,
                                split.sep_key(),
                                ts,
                            )
                            .await
                        {
                            BTreeSplit::Ok | BTreeSplit::Inconsistent => break,
                            BTreeSplit::FullBranch(s) => {
                                split = s;
                            }
                        }
                    },
                }
                continue;
            }
            node.insert_at(idx, key, value);
            node.update_ts(ts);
            return BTreeInsert::Ok;
        }
    }

    /// Mark an existing key value pair as deleted.
    #[inline]
    pub async fn mark_as_deleted<V: BTreeValue>(
        &self,
        key: &[u8],
        value: V,
        ts: TrxID,
    ) -> BTreeUpdate<V> {
        loop {
            let mut res = self.find_leaf(key, LatchFallbackMode::Exclusive).await;
            verify_continue!(res.try_exclusive());
            let mut g = res.must_exclusive();
            debug_assert!(g.page().is_leaf());
            let node = g.page_mut();
            return node
                .mark_as_deleted(key, value)
                .ok_then(|| node.update_ts(ts));
        }
    }

    /// Delete an existing key value pair from this tree.
    /// This method will remove matched key value pair no matter
    /// it is marked as deleted or not.
    #[inline]
    pub async fn delete<V: BTreeValue>(&self, key: &[u8], value: V, ts: TrxID) -> BTreeDelete {
        loop {
            let mut res = self.find_leaf(key, LatchFallbackMode::Exclusive).await;
            verify_continue!(res.try_exclusive());
            let mut g = res.must_exclusive();
            debug_assert!(g.page().is_leaf());
            let node = g.page_mut();
            return node.delete(key, value).ok_then(|| node.update_ts(ts));
        }
    }

    /// Update an existing key value pair with new value
    #[inline]
    pub async fn update<V: BTreeValue>(
        &self,
        key: &[u8],
        old_value: V,
        new_value: V,
        ts: TrxID,
    ) -> BTreeUpdate<V> {
        loop {
            let mut res = self.find_leaf(key, LatchFallbackMode::Exclusive).await;
            verify_continue!(res.try_exclusive());
            let mut g = res.must_exclusive();
            debug_assert!(g.page().is_leaf());
            let node = g.page_mut();
            return node
                .update(key, old_value, new_value)
                .ok_then(|| node.update_ts(ts));
        }
    }

    /// Try to split node bottom up.
    #[inline]
    async fn try_split_bottom_up<V: BTreeValue>(
        &self,
        stack: &mut Vec<PageGuard<BTreeNode>>,
        mut c_guard: PageExclusiveGuard<BTreeNode>,
        ts: TrxID,
    ) -> BTreeSplit {
        let c_node = c_guard.page_mut();
        debug_assert!(c_node.is_leaf());
        debug_assert!(c_node.count() > 1);
        debug_assert!(!stack.is_empty());
        // Construct separator key.
        let (sep_idx, sep_key) = {
            let node = c_guard.page();
            let sep_idx = node.find_separator();
            (sep_idx, node.create_sep_key(sep_idx, true))
        };
        let mut p_guard = stack.pop().unwrap();
        // Try to gain exclusive lock to avoid dead lock.
        let mut p_guard = match p_guard.try_exclusive() {
            Valid(()) => {
                let mut p_guard = p_guard.must_exclusive();
                // Check if parent is full, trigger top-down split of parent node.
                if !p_guard.page().can_insert(&sep_key) {
                    let page_id = p_guard.page_id();
                    if page_id != self.root {
                        let lower_fence_key = p_guard.page().lower_fence_key();
                        return BTreeSplit::full_branch(&lower_fence_key, page_id, &sep_key);
                    }
                    // Parent is root and full, just split.
                    self.split_root::<PageID>(p_guard.page_mut(), false, ts)
                        .await;
                    return BTreeSplit::Ok;
                }
                p_guard
            }
            Invalid => {
                // Parent lock cannot be acquired,
                // We need to release child lock, then require parent and child locks in order.
                // But other thread may change them before we regain the lock, so we need to recheck the
                // tree structure and split condition still remain the same.
                let c_page_id = c_guard.page_id();
                let c_lower_fence_key = c_guard.page().lower_fence_key();
                let c_optimistic_guard = c_guard.downgrade();
                let mut p_guard = p_guard.exclusive_async().await;
                let p_node = p_guard.page();
                match p_node.search::<PageID>(&c_lower_fence_key) {
                    SearchResult::Equal(_, page_id) if page_id == c_page_id => {
                        // Tree structure remains the same.
                        // Check if parent is full.
                        if !p_node.can_insert(&sep_key) {
                            let p_page_id = p_guard.page_id();
                            if p_page_id != self.root {
                                // Parent is full, trigger top-down split of parent node.
                                let p_lower_fence_key = p_node.lower_fence_key();
                                return BTreeSplit::full_branch(
                                    &p_lower_fence_key,
                                    p_page_id,
                                    &sep_key,
                                );
                            }
                            // Parent is root and full, just split.
                            self.split_root::<PageID>(p_guard.page_mut(), false, ts)
                                .await;
                            return BTreeSplit::Ok;
                        }
                        // Re-lock child in exclusive mode.
                        c_guard = c_optimistic_guard.exclusive_async().await;
                        // Check if separator key changes
                        let c_node = c_guard.page();
                        let new_sep_idx = c_node.find_separator();
                        if new_sep_idx != sep_idx {
                            return BTreeSplit::Inconsistent;
                        }
                        let new_sep_key = c_node.create_sep_key(new_sep_idx, true);
                        if new_sep_key != sep_key {
                            return BTreeSplit::Inconsistent;
                        }
                        p_guard
                    }
                    _ => {
                        // Tree structure changes, abort the split.
                        return BTreeSplit::Inconsistent;
                    }
                }
            }
        };
        // Now parent and child locks are acquired exclusively, and parent has enough
        // space to insert separator key, so to actual split.
        self.split_node::<V>(
            p_guard.page_mut(),
            c_guard.page_mut(),
            sep_idx,
            &sep_key,
            ts,
        )
        .await;
        BTreeSplit::Ok
    }

    /// Try to split node top down.
    #[inline]
    async fn try_split_top_down(
        &self,
        lower_fence_key: &[u8],
        page_id: PageID,
        sep_key: &[u8],
        ts: TrxID,
    ) -> BTreeSplit {
        debug_assert!(page_id != self.root);
        match self.find_branch_for_split(lower_fence_key, page_id).await {
            None => BTreeSplit::Inconsistent,
            Some(mut p_guard) => {
                let p_page_id = p_guard.page_id();
                let p_node = p_guard.page_mut();
                match p_node.lookup_child(lower_fence_key) {
                    None => BTreeSplit::Inconsistent,
                    Some(c_page_id) => {
                        if page_id != c_page_id {
                            return BTreeSplit::Inconsistent;
                        }
                        let mut c_guard = self
                            .pool
                            .get_page::<BTreeNode>(c_page_id, LatchFallbackMode::Exclusive)
                            .await
                            .exclusive_async()
                            .await;
                        let c_node = c_guard.page_mut();
                        if c_node.can_insert(sep_key) {
                            // The node to split can insert original separator key,
                            // maybe other thread restructure this tree.
                            return BTreeSplit::Inconsistent;
                        }
                        // Now we calculate current separator key for child node.
                        debug_assert!(!c_node.is_leaf());
                        let sep_idx = c_node.find_separator();
                        // Separator key can not be truncated because it's branch node.
                        let sep_key = c_node.create_sep_key(sep_idx, false);
                        if !p_node.can_insert(&sep_key) {
                            // parent node is full
                            if p_page_id != self.root {
                                // not root, split it in a single run.
                                let lower_fence_key = p_node.lower_fence_key();
                                return BTreeSplit::full_branch(
                                    &lower_fence_key,
                                    p_page_id,
                                    &sep_key,
                                );
                            }
                            // split root.
                            self.split_root::<PageID>(p_node, false, ts).await;
                            if !p_node.can_insert(&sep_key) {
                                return BTreeSplit::Inconsistent;
                            }
                        }
                        // now parent and child nodes are exclusively locked and parent has enough
                        // space to insert separator key, so do actual split.
                        self.split_node::<PageID>(p_node, c_node, sep_idx, &sep_key, ts)
                            .await;
                        BTreeSplit::Ok
                    }
                }
            }
        }
    }

    /// Split root node.
    #[inline]
    async fn split_root<V: BTreeValue>(&self, root: &mut BTreeNode, is_leaf: bool, ts: TrxID) {
        debug_assert!(root.is_leaf() == is_leaf);
        let ts = root.ts().max(ts);
        let height = root.height() as u16;
        let sep_idx = root.find_separator();
        let lower_fence_key = root.lower_fence_key();
        let lower_fence_value = root.lower_fence_value();
        // Only truncate separator key if it's leaf.
        let sep_key = root.create_sep_key(sep_idx, is_leaf);
        // Allocate left node.
        let mut left_page = self.pool.allocate_page::<BTreeNode>().await;
        let left_page_id = left_page.page_id();
        let left_node = left_page.page_mut();
        // Allocate right node.
        let mut right_page = self.pool.allocate_page::<BTreeNode>().await;
        let right_page_id = right_page.page_id();
        let right_node = right_page.page_mut();
        // Initialize and copy key values to left node.
        left_node.init(height, ts, &lower_fence_key, lower_fence_value, &sep_key);
        left_node.extend_slots_from::<V>(root, 0, sep_idx);
        // Initialize and copy key values to right node.
        right_node.init(height, ts, &sep_key, PageID::INVALID_VALUE, &[]);
        right_node.extend_slots_from::<V>(root, sep_idx, root.count() as usize - sep_idx);
        // Initialize temporary root node and insert separator key.
        let mut tmp_root = unsafe {
            let mut new = std::mem::MaybeUninit::<BTreeNode>::zeroed();
            new.assume_init_mut()
                .init(height + 1, ts, &lower_fence_key, left_page_id, &[]);
            new.assume_init()
        };
        let slot_idx = tmp_root.insert(&sep_key, right_page_id);
        debug_assert!(slot_idx == 0);
        // Overwrite original root.
        *root = tmp_root;
        self.height.store(root.height(), Ordering::SeqCst);
    }

    /// Split node.
    #[inline]
    async fn split_node<V: BTreeValue>(
        &self,
        p_node: &mut BTreeNode,
        c_node: &mut BTreeNode,
        sep_idx: usize,
        sep_key: &[u8],
        ts: TrxID,
    ) {
        debug_assert!(c_node.find_separator() == sep_idx);

        let ts = ts.max(p_node.ts()).max(c_node.ts());
        let c_height = c_node.height() as u16;
        let c_lower_fence_key = c_node.lower_fence_key();
        let c_upper_fence_key = c_node.upper_fence_key();
        // Create temporary node to store the left half data of child node.
        let mut tmp_left = unsafe {
            let mut new = std::mem::MaybeUninit::<BTreeNode>::zeroed();
            new.assume_init_mut().init(
                c_height,
                ts,
                &c_lower_fence_key,
                c_node.lower_fence_value(),
                sep_key,
            );
            new.assume_init()
        };
        tmp_left.extend_slots_from::<V>(c_node, 0, sep_idx);

        // Allocate right node.
        let mut right_page = self.pool.allocate_page::<BTreeNode>().await;
        let right_page_id = right_page.page_id();
        let right_node = right_page.page_mut();
        // Initialize and copy key values to right node.
        right_node.init(
            c_node.height() as u16,
            ts,
            &sep_key,
            // For leaf node, lower fence value is meaningless.
            // For branch node, only the leftmost branch node has meaningful
            // lower fence value pointing to a valid child.
            // Other branch nodes always have lower fence equal to the first
            // slot key.
            // So, right node does not need to copy lower fence value.
            PageID::INVALID_VALUE,
            &c_upper_fence_key,
        );
        right_node.extend_slots_from::<V>(c_node, sep_idx, c_node.count() as usize - sep_idx);
        // Copy left node to current node.
        *c_node = tmp_left;
        // Insert right node into parent.
        p_node.insert(sep_key, right_page_id);
    }

    /// Try to lookup a key in the tree, break if any of optimistic validation fails.
    #[inline]
    pub async fn try_lookup_optimistic<V: BTreeValue>(
        &self,
        key: &[u8],
    ) -> Validation<BTreeLookup<V>> {
        let g = self.find_leaf(key, LatchFallbackMode::Spin).await;
        let leaf = unsafe { g.page_unchecked() };
        match leaf.search(key) {
            SearchResult::Equal(_, value) => {
                verify!(g.validate());
                Valid(BTreeLookup::Exists(value))
            }
            SearchResult::EqualDeleted(_, value) => {
                verify!(g.validate());
                Valid(BTreeLookup::Deleted(value))
            }
            SearchResult::GreaterThan(_) => {
                verify!(g.validate());
                Valid(BTreeLookup::NotFound)
            }
            SearchResult::LessThanAllSlots
            | SearchResult::LessThanLowerFence
            | SearchResult::GreaterEqualUpperFence => {
                verify!(g.validate());
                Validation::Invalid
            }
        }
    }

    /// Find leaf by given key.
    #[inline]
    async fn find_leaf(&self, key: &[u8], mode: LatchFallbackMode) -> PageGuard<BTreeNode> {
        loop {
            let res = self.try_find_leaf(key, mode, None).await;
            let res = verify_continue!(res);
            return res;
        }
    }

    #[inline]
    async fn try_find_leaf(
        &self,
        key: &[u8],
        mode: LatchFallbackMode,
        mut stack: Option<&mut Vec<PageGuard<BTreeNode>>>,
    ) -> Validation<PageGuard<BTreeNode>> {
        let mut g = self
            .pool
            .get_page::<BTreeNode>(self.root, LatchFallbackMode::Spin)
            .await;
        // check root page separately.
        let pu = unsafe { g.page_unchecked() };
        let mut height = pu.height();
        verify!(g.validate());
        if height == 0 {
            // root is leaf.
            match mode {
                LatchFallbackMode::Spin => {
                    return Valid(g);
                }
                LatchFallbackMode::Exclusive => {
                    verify!(g.try_exclusive());
                    return Valid(g);
                }
                LatchFallbackMode::Shared => {
                    verify!(g.try_shared());
                    return Valid(g);
                }
            }
        }
        loop {
            // Current node is not leaf node.
            let pu = unsafe { g.page_unchecked() };
            match pu.lookup_child(key) {
                None => {
                    verify!(g.validate());
                    unreachable!("BTree should always find one leaf for any key");
                }
                Some(page_id) => {
                    verify!(g.validate());
                    height -= 1;
                    let c = {
                        // With chained version validation, we can make sure height of parent and
                        // child are always consistent.
                        // So we can acquire desired lock type if leaf node is reached.
                        let mode = if height == 0 {
                            mode
                        } else {
                            LatchFallbackMode::Spin
                        };
                        let v = self.pool.get_child_page(&g, page_id, mode).await;
                        verify!(v)
                    };
                    if let Some(s) = stack.as_mut() {
                        s.push(g);
                    }
                    let pu = unsafe { c.page_unchecked() };
                    height = pu.height();
                    if height == 0 {
                        verify!(c.validate());
                        return Valid(c);
                    }
                    g = c;
                }
            }
        }
    }

    #[inline]
    async fn find_branch_for_split(
        &self,
        lower_fence_key: &[u8],
        page_id: PageID,
    ) -> Option<PageExclusiveGuard<BTreeNode>> {
        loop {
            let res = self
                .try_find_branch_for_split(lower_fence_key, page_id)
                .await;
            let res = verify_continue!(res);
            return res;
        }
    }

    #[inline]
    async fn try_find_branch_for_split(
        &self,
        lower_fence_key: &[u8],
        page_id: PageID,
    ) -> Validation<Option<PageExclusiveGuard<BTreeNode>>> {
        let mut g = self
            .pool
            .get_page::<BTreeNode>(self.root, LatchFallbackMode::Spin)
            .await;
        let mut pu = unsafe { g.page_unchecked() };
        let mut height = pu.height();
        verify!(g.validate());
        if height == 0 {
            // single-node tree
            return Valid(None);
        }
        loop {
            pu = unsafe { g.page_unchecked() };
            height = pu.height();
            match pu.lookup_child(lower_fence_key) {
                None => {
                    verify!(g.validate());
                    return Valid(None);
                }
                Some(c_page_id) => {
                    verify!(g.validate());
                    if c_page_id == page_id {
                        let g = g.exclusive_async().await;
                        return Valid(Some(g));
                    }
                    if height <= 1 {
                        // next level is leaf.
                        return Valid(None);
                    }
                    // Otherwise, go to next level.
                    let c_guard = self
                        .pool
                        .get_child_page(&g, c_page_id, LatchFallbackMode::Spin)
                        .await;
                    g = verify!(c_guard);
                }
            }
        }
    }

    #[inline]
    async unsafe fn traverse<F: FnMut(PageID, &BTreeNode)>(&self, mut f: F) {
        let mut stack = vec![self.root];
        while let Some(page_id) = stack.pop() {
            let g = self
                .pool
                .get_page::<BTreeNode>(page_id, LatchFallbackMode::Shared)
                .await
                .shared_async()
                .await;
            let node = g.page();
            f(page_id, node);
            if !node.is_leaf() {
                let mut children = node.values::<PageID>();
                children.reverse();
                for c_page_id in children {
                    stack.push(c_page_id);
                }
                if !node.lower_fence_value().is_deleted() {
                    stack.push(node.lower_fence_value());
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeLookup<V: BTreeValue> {
    Exists(V),
    Deleted(V),
    NotFound,
}

impl<V: BTreeValue> BTreeLookup<V> {
    #[inline]
    pub fn exists(&self) -> bool {
        matches!(self, BTreeLookup::Exists(_))
    }

    #[inline]
    pub fn is_deleted(&self) -> bool {
        matches!(self, BTreeLookup::Deleted(_))
    }

    #[inline]
    pub fn not_found(&self) -> bool {
        matches!(self, BTreeLookup::NotFound)
    }
}

enum BTreeSplit {
    // Split success.
    Ok,
    // Branch(parent) node is full, should split it.
    FullBranch(SplitNode),
    // Tree structure or intra-node data changes.
    Inconsistent,
}

impl BTreeSplit {
    #[inline]
    fn full_branch(lower_fence_key: &[u8], page_id: PageID, sep_key: &[u8]) -> Self {
        BTreeSplit::FullBranch(SplitNode::new(lower_fence_key, page_id, sep_key))
    }
}

struct SplitNode {
    keys: Box<[u8]>,
    page_id: PageID,
    sep_idx: usize,
}

impl SplitNode {
    #[inline]
    fn new(lower_fence_key: &[u8], page_id: PageID, sep_key: &[u8]) -> Self {
        let mut keys = Vec::with_capacity(lower_fence_key.len() + sep_key.len());
        keys.extend_from_slice(lower_fence_key);
        keys.extend_from_slice(sep_key);
        SplitNode {
            keys: keys.into_boxed_slice(),
            page_id,
            sep_idx: lower_fence_key.len(),
        }
    }

    #[inline]
    fn lower_fence_key(&self) -> &[u8] {
        &self.keys[..self.sep_idx]
    }

    #[inline]
    fn sep_key(&self) -> &[u8] {
        &self.keys[self.sep_idx..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifetime::StaticLifetime;
    use std::collections::HashMap;

    #[derive(Debug, Default)]
    struct LevelStat {
        nodes: usize,
        keys: usize,
        first_key_len: usize,
        prefix_len: usize,
    }

    #[test]
    fn test_btree_single_node() {
        smol::block_on(async {
            let pool = FixedBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, 200).await;
                // insert 1, 2, 3.
                let res = tree.insert(&1u64.to_be_bytes(), 1, 210).await;
                assert!(res.is_ok());
                let res = tree.insert(&3u64.to_be_bytes(), 3, 220).await;
                assert!(res.is_ok());
                let res = tree.insert(&5u64.to_be_bytes(), 5, 205).await;
                assert!(res.is_ok());
                // look up
                let res = tree.lookup_optimistic::<u64>(&1u64.to_be_bytes()).await;
                assert!(res.exists());
                let res = tree.lookup_optimistic::<u64>(&4u64.to_be_bytes()).await;
                assert!(res.not_found());
                // mark as deleted
                let res = tree.mark_as_deleted(&3u64.to_be_bytes(), 3, 230).await;
                assert!(res.is_ok());
                let res = tree.mark_as_deleted(&5u64.to_be_bytes(), 5, 230).await;
                assert!(res.is_ok());
                let res = tree.mark_as_deleted(&7u64.to_be_bytes(), 7, 235).await;
                assert_eq!(res, BTreeUpdate::NotFound);
                let res = tree.lookup_optimistic::<u64>(&5u64.to_be_bytes()).await;
                assert_eq!(res, BTreeLookup::Deleted(5));
                // update
                let res = tree.update(&5u64.to_be_bytes(), 5, 50, 240).await;
                assert!(res.is_ok());
                let res = tree.update(&5u64.to_be_bytes(), 5, 70, 245).await;
                assert_eq!(res, BTreeUpdate::ValueMismatch(50));
                // delete
                let res = tree.delete(&3u64.to_be_bytes(), 3, 250).await;
                assert!(res.is_ok());
                let res = tree.delete(&5u64.to_be_bytes(), 5, 255).await;
                assert_eq!(res, BTreeDelete::ValueMismatch);
            }

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_scale() {
        const ROWS: u64 = 90122;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(3 * 1024 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, 200).await;

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                let mut printed1 = false;
                let mut printed2 = false;
                for i in 0u64..ROWS {
                    // let k = i.to_be_bytes();
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, i, 201).await;
                    assert!(res.is_ok());
                    if !printed1 && tree.height() == 1 {
                        printed1 = true;
                        println!("record {} to height 1", i);
                    }

                    if !printed2 && tree.height() == 2 {
                        printed2 = true;
                        println!("record {} to height 2", i);
                    }
                }
                println!("tree height {}", tree.height());

                let mut map: HashMap<usize, LevelStat> = HashMap::new();
                unsafe {
                    tree.traverse(|_, node| {
                        let v = map.entry(node.height()).or_default();
                        v.nodes += 1;
                        v.keys += node.count();
                        v.first_key_len += node.key(0).len();
                        v.prefix_len += node.prefix_len();
                        if node.height() > 0 {
                            println!("node height {} keys {}", node.height(), node.count());
                        }
                    })
                    .await;
                }
                for (height, stat) in &map {
                    println!(
                        "height={}, nodes={}, keys={}, keys/node={:.2}, key0len/node={:.2}, prefixlen/node={:.2}",
                        height,
                        stat.nodes,
                        stat.keys,
                        stat.keys as f64 / stat.nodes as f64,
                        stat.first_key_len as f64 / stat.nodes as f64,
                        stat.prefix_len as f64 / stat.nodes as f64,
                    );
                }
                // 90122 rows will construct btree with height 2.
                assert!(map.contains_key(&2));
                assert!(map[&0].keys as u64 == ROWS);
                // level N+1 keys plus one(lower fence key of leftmost node)
                // should be equal to level N nodes.
                assert!(map[&2].keys + 1 == map[&1].nodes);
                assert!(map[&1].keys + 1 == map[&0].nodes);
            }

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }
}
