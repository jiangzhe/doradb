use crate::buffer::guard::{
    ExclusiveLockStrategy, FacadePageGuard, LockStrategy, OptimisticLockStrategy,
    PageExclusiveGuard, PageGuard, PageSharedGuard, SharedLockStrategy,
};
use crate::buffer::page::PageID;
use crate::buffer::{BufferPool, FixedBufferPool};
use crate::error::Validation;
use crate::error::Validation::{Invalid, Valid};
use crate::error::{Error, Result};
use crate::index::btree_node::{BTreeNode, BTreeNodeBox, LookupChild, SpaceEstimation};
use crate::index::btree_scan::{BTreePrefixScan, BTreeSlotCallback};
use crate::index::btree_value::{BTreeU64, BTreeValue};
use crate::index::util::{Maskable, ParentPosition, SpaceStatistics};
use crate::latch::LatchFallbackMode;
use crate::trx::TrxID;
use either::Either;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

pub type SharedStrategy = SharedLockStrategy<BTreeNode>;
pub type ExclusiveStrategy = ExclusiveLockStrategy<BTreeNode>;
pub type OptimisticStrategy = OptimisticLockStrategy<BTreeNode>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeInsert<V: BTreeValue> {
    Ok(bool),
    DuplicateKey(V),
}

impl<V: BTreeValue> BTreeInsert<V> {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, BTreeInsert::Ok(false))
    }

    #[inline]
    pub fn is_merged(&self) -> bool {
        matches!(self, BTreeInsert::Ok(true))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeDelete {
    Ok,
    NotFound,
    ValueMismatch,
}

impl BTreeDelete {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, BTreeDelete::Ok)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeUpdate<V: BTreeValue> {
    Ok(V),
    NotFound,
    ValueMismatch(V),
}

impl<V: BTreeValue> BTreeUpdate<V> {
    #[inline]
    pub fn is_ok(&self) -> bool {
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
    pub async fn new(pool: &'static FixedBufferPool, hints_enabled: bool, ts: TrxID) -> Self {
        let mut g = pool.allocate_page::<BTreeNode>().await;
        let page_id = g.page_id();
        let page = g.page_mut();
        page.init(0, ts, &[], BTreeU64::INVALID_VALUE, &[], hints_enabled);
        BTree {
            root: page_id,
            height: AtomicUsize::new(0),
            pool,
        }
    }

    /// Destroy the tree.
    /// This method will traverse the tree and deallocate all the nodes recursively.
    #[inline]
    pub async fn destory(self) {
        let g = self
            .pool
            .get_page::<BTreeNode>(self.root, LatchFallbackMode::Exclusive)
            .await
            .lock_exclusive_async()
            .await
            .unwrap();
        let p_node = g.page();
        match p_node.height() {
            0 => {
                // single-node tree.
                self.pool.deallocate_page::<BTreeNode>(g);
            }
            1 => {
                self.deallocate_h1(g).await;
            }
            _ => {
                let mut stack = vec![];
                stack.push(ParentPosition { g, idx: -1 });

                while let Some(pos) = stack.last_mut() {
                    let p_node = pos.g.page();
                    if p_node.height() == 1 {
                        let g = stack.pop().unwrap().g;
                        self.deallocate_h1(g).await;
                        continue;
                    }
                    if pos.idx == p_node.count() as isize {
                        // all children are dropped, we can drop the parent itself.
                        let ParentPosition { g, .. } = stack.pop().unwrap();
                        self.pool.deallocate_page(g);
                        continue;
                    }
                    let c_page_id = if pos.idx == -1 {
                        p_node.lower_fence_value().to_u64()
                    } else {
                        p_node.value_as_page_id(pos.idx as usize)
                    };
                    let c_guard = self
                        .pool
                        .get_page::<BTreeNode>(c_page_id, LatchFallbackMode::Exclusive)
                        .await
                        .lock_exclusive_async()
                        .await
                        .unwrap();
                    pos.idx += 1;
                    stack.push(ParentPosition {
                        g: c_guard,
                        idx: -1,
                    });
                }
            }
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
    pub async fn lookup_optimistic<V: BTreeValue>(&self, key: &[u8]) -> Option<V> {
        loop {
            let res = self.try_lookup_optimistic(key).await;
            let res = verify_continue!(res);
            return res;
        }
    }

    /// Insert a new key value pair into the tree.
    /// Returns old value if same key exists.
    #[inline]
    pub async fn insert<V: BTreeValue>(
        &self,
        key: &[u8],
        value: V,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> BTreeInsert<V> {
        // Stack holds the path from root to leaf.
        loop {
            let res = self
                .try_find_leaf_with_optimistic_parent::<ExclusiveStrategy>(key)
                .await;
            let (mut c_guard, p_guard) = verify_continue!(res);
            let node = c_guard.page_mut();
            let idx = match node.search_key(key) {
                Err(idx) => idx,
                Ok(idx) => {
                    // Here we special handle the case that old value is same as input value
                    // but already masked as deleted.
                    // If merge_if_match_deleted set to true, we unset the delete flag
                    // and make the insert success.
                    let old_v = node.value::<V>(idx);
                    if merge_if_match_deleted && old_v.value() == value && old_v.is_deleted() {
                        // This check can be applied to both unique index and non-unique index.
                        node.update_value(idx, value);
                        node.update_ts(ts);
                        return BTreeInsert::Ok(true);
                    }
                    return BTreeInsert::DuplicateKey(old_v);
                }
            };
            if !node.can_insert(key) {
                debug_assert!(node.count() > 1);
                if p_guard.is_none() {
                    // Root is leaf and full, should split.
                    self.split_root::<V>(node, true, ts).await;
                    continue;
                }
                match self
                    .try_split_bottom_up::<V>(p_guard.unwrap(), c_guard, ts)
                    .await
                {
                    // If split is done or tree structure has been changed by other thread,
                    // we can retry the insert.
                    BTreeSplit::Ok | BTreeSplit::Inconsistent => (),
                    BTreeSplit::FullBranch(mut split) => loop {
                        match self
                            .try_split_top_down(
                                split.lower_fence_key(),
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
            // Actual insert here.
            node.insert_at(idx, key, value);
            node.update_hints();
            node.update_ts(ts);
            return BTreeInsert::Ok(false);
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
        debug_assert!(!value.is_deleted());
        let mut g = self.find_leaf::<ExclusiveStrategy>(key).await;
        debug_assert!(g.page().is_leaf());
        let node = g.page_mut();
        let res = node.mark_as_deleted(key, value);
        if res.is_ok() {
            node.update_ts(ts);
        }
        res
    }

    /// Delete an existing key value pair from this tree.
    /// This method will remove matched key value pair no matter
    /// it is marked as deleted or not.
    ///
    /// We do not apply merge logic when deleting keys.
    /// Instead a tree-level compact() method can be used periodically
    /// to make the tree balanced.
    #[inline]
    pub async fn delete<V: BTreeValue>(
        &self,
        key: &[u8],
        value: V,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> BTreeDelete {
        debug_assert!(!value.is_deleted());
        let mut g = self.find_leaf::<ExclusiveStrategy>(key).await;
        debug_assert!(g.page().is_leaf());
        let node = g.page_mut();
        let res = node.delete(key, value, ignore_del_mask);
        if res.is_ok() {
            node.update_hints();
            node.update_ts(ts);
        }
        res
    }

    /// Update an existing key value pair with new value.
    #[inline]
    pub async fn update<V: BTreeValue>(
        &self,
        key: &[u8],
        old_value: V,
        new_value: V,
        ts: TrxID,
    ) -> BTreeUpdate<V> {
        let mut g = self.find_leaf::<ExclusiveStrategy>(key).await;
        debug_assert!(g.page().is_leaf());
        let node = g.page_mut();
        let res = node.update(key, old_value, new_value);
        if res.is_ok() {
            node.update_ts(ts);
        }
        res
    }

    /// Create a cursor to iterator over nodes at given height.
    /// Height equals to 0 means iterating over all leaf nodes.
    #[inline]
    pub fn cursor(&self, height: usize) -> BTreeNodeCursor<'_> {
        BTreeNodeCursor::new(self, height)
    }

    /// Create a prefix scanner to scan keys.
    #[inline]
    pub fn prefix_scanner<C: BTreeSlotCallback>(&self, callback: C) -> BTreePrefixScan<'_, C> {
        BTreePrefixScan::new(self, callback)
    }

    /// Collect space statistics at given height.
    #[inline]
    pub async fn collect_space_statistics_at(&self, height: usize) -> SpaceStatistics {
        let mut cursor = self.cursor(height);
        cursor.seek(&[]).await;
        let mut preview = SpaceStatistics::default();
        while let Some(g) = cursor.next().await {
            let node = g.page();
            preview.nodes += 1;
            preview.total_space += mem::size_of::<BTreeNode>();
            preview.used_space += mem::size_of::<BTreeNode>() - node.free_space();
            preview.effective_space += node.effective_space();
        }
        preview
    }

    /// Collect space statistics of the whole tree.
    #[inline]
    pub async fn collect_space_statistics(&self) -> SpaceStatistics {
        let height = self.height();
        let mut res = SpaceStatistics::default();
        for h in 0..height + 1 {
            let s = self.collect_space_statistics_at(h).await;
            res.nodes += s.nodes;
            res.total_space += s.total_space;
            res.used_space += s.used_space;
            res.effective_space += s.effective_space;
        }
        res
    }

    /// Create a compactor for all nodes at given height.
    #[inline]
    pub fn compact<V: BTreeValue>(
        &self,
        height: usize,
        config: BTreeCompactConfig,
    ) -> BTreeCompactor<'_, V> {
        BTreeCompactor::new(self, height, config)
    }

    /// Compact the whole tree.
    #[inline]
    pub async fn compact_all<V: BTreeValue>(
        &self,
        config: BTreeCompactConfig,
    ) -> Vec<PageExclusiveGuard<BTreeNode>> {
        let height = self.height();
        let mut purge_list = vec![];
        // leaf compaction.
        self.compact::<V>(0, config)
            .run_to_end(&mut purge_list)
            .await;
        // branch-to-root compaction.
        let mut h = 1usize;
        while h <= height {
            self.compact::<BTreeU64>(h, config)
                .run_to_end(&mut purge_list)
                .await;
            h += 1;
        }
        self.shrink(&mut purge_list).await;
        purge_list
    }

    /// Try to shrink the tree height if root node has only one child.
    #[inline]
    pub async fn shrink(&self, purge_list: &mut Vec<PageExclusiveGuard<BTreeNode>>) {
        // test if root has only one child with optimistic lock,
        // to avoid block concurrent operations.
        loop {
            let g = self
                .pool
                .get_page::<BTreeNode>(self.root, LatchFallbackMode::Spin)
                .await;
            // SAFETY: this read is protected by optimistic validation below.
            // `page_unchecked` is only used before `g.validate()` and retried on mismatch.
            let pu = unsafe { g.page_unchecked() };
            let height = pu.height();
            let count = pu.count();
            verify_continue!(g.validate());
            if height == 0 || count > 0 {
                return;
            }
            // should shrink tree height as there is no keys in root.
            // The only child is associated with lower fence key.
            let mut g = g.lock_exclusive_async().await.unwrap();
            // re-check condition
            let root = g.page_mut();
            if root.height() == 0 || root.count() > 0 {
                return;
            }
            let c_page_id = root.lower_fence_value().to_u64();
            let mut c_guard = self
                .pool
                .get_page::<BTreeNode>(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .lock_exclusive_async()
                .await
                .unwrap();
            let c_node = c_guard.page_mut();
            debug_assert!(root.lower_fence_key() == c_node.lower_fence_key());
            debug_assert!(root.has_no_upper_fence());
            debug_assert!(c_node.has_no_upper_fence());
            let ts = c_node.ts().max(root.ts());
            root.clone_from(c_node);
            root.update_ts(ts);
            self.height.store(root.height(), Ordering::Release);
            purge_list.push(c_guard);
        }
    }

    /// Try to lookup a key in the tree, break if any of optimistic validation fails.
    #[inline]
    async fn try_lookup_optimistic<V: BTreeValue>(&self, key: &[u8]) -> Validation<Option<V>> {
        let g = self.find_leaf::<OptimisticStrategy>(key).await;
        // SAFETY: this access is coupled with optimistic guard validation at each return path.
        let leaf = unsafe { g.page_unchecked() };
        match leaf.search_key(key) {
            Ok(idx) => {
                let value = leaf.value::<V>(idx);
                verify!(g.validate());
                Valid(Some(value))
            }
            Err(_) => {
                verify!(g.validate());
                Valid(None)
            }
        }
    }

    /// Try to split node bottom up.
    #[inline]
    async fn try_split_bottom_up<V: BTreeValue>(
        &self,
        mut p_guard: FacadePageGuard<BTreeNode>,
        mut c_guard: PageExclusiveGuard<BTreeNode>,
        ts: TrxID,
    ) -> BTreeSplit {
        let c_node = c_guard.page_mut();
        debug_assert!(c_node.is_leaf());
        debug_assert!(c_node.count() > 1);
        // Construct separator key.
        let (sep_idx, sep_key) = {
            let node = c_guard.page();
            let sep_idx = node.find_separator();
            (sep_idx, node.create_sep_key(sep_idx, true))
        };
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
                    self.split_root::<BTreeU64>(p_guard.page_mut(), false, ts)
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
                let res = self
                    .try_acquire_parent_and_child_locks_for_split(
                        p_guard, c_guard, sep_idx, &sep_key, ts,
                    )
                    .await;
                match res {
                    Either::Left((p, c)) => {
                        c_guard = c;
                        p
                    }
                    Either::Right(s) => return s,
                }
            }
        };
        // Now parent and child locks are acquired exclusively, and parent has enough
        // space to insert separator key, so to actual split.
        let r_guard = self.pool.allocate_page::<BTreeNode>().await;
        self.split_node::<V>(
            p_guard.page_mut(),
            c_guard.page_mut(),
            r_guard,
            sep_idx,
            &sep_key,
            ts,
        )
        .await;
        BTreeSplit::Ok
    }

    /// Try to acquire locks on parent node and child node in order.
    /// Meanwhile, check if any concurrent change is applied to effected nodes.
    #[inline]
    async fn try_acquire_parent_and_child_locks_for_split(
        &self,
        p_guard: FacadePageGuard<BTreeNode>,
        mut c_guard: PageExclusiveGuard<BTreeNode>,
        sep_idx: usize,
        sep_key: &[u8],
        ts: TrxID,
    ) -> Either<(PageExclusiveGuard<BTreeNode>, PageExclusiveGuard<BTreeNode>), BTreeSplit> {
        let c_page_id = c_guard.page_id();
        let c_lower_fence_key = c_guard.page().lower_fence_key();
        let c_optimistic_guard = c_guard.downgrade();
        let mut p_guard = p_guard.lock_exclusive_async().await.unwrap();
        let p_node = p_guard.page();
        match p_node.search_key(&c_lower_fence_key) {
            Ok(idx) => {
                let page_id = p_node.value_as_page_id(idx);
                if page_id == c_page_id {
                    // Tree structure remains the same.
                    // Check if parent is full.
                    if !p_node.can_insert(sep_key) {
                        let p_page_id = p_guard.page_id();
                        if p_page_id != self.root {
                            // Parent is full, trigger top-down split of parent node.
                            let p_lower_fence_key = p_node.lower_fence_key();
                            return Either::Right(BTreeSplit::full_branch(
                                &p_lower_fence_key,
                                p_page_id,
                                sep_key,
                            ));
                        }
                        // Parent is root and full, just split.
                        self.split_root::<BTreeU64>(p_guard.page_mut(), false, ts)
                            .await;
                        return Either::Right(BTreeSplit::Ok);
                    }
                    // Re-lock child in exclusive mode.
                    c_guard = c_optimistic_guard.exclusive_async().await;
                    // Check if separator key changes
                    let c_node = c_guard.page();
                    let new_sep_idx = c_node.find_separator();
                    if new_sep_idx != sep_idx {
                        return Either::Right(BTreeSplit::Inconsistent);
                    }
                    let new_sep_key = c_node.create_sep_key(new_sep_idx, true);
                    if &new_sep_key[..] != sep_key {
                        return Either::Right(BTreeSplit::Inconsistent);
                    }
                    return Either::Left((p_guard, c_guard));
                }
                Either::Right(BTreeSplit::Inconsistent)
            }
            Err(_) => Either::Right(BTreeSplit::Inconsistent),
        }
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
                    LookupChild::NotFound => BTreeSplit::Inconsistent,
                    LookupChild::Slot(_, c_page_id) | LookupChild::LowerFence(c_page_id) => {
                        if page_id != c_page_id {
                            return BTreeSplit::Inconsistent;
                        }
                        let mut c_guard = self
                            .pool
                            .get_page::<BTreeNode>(c_page_id, LatchFallbackMode::Exclusive)
                            .await
                            .lock_exclusive_async()
                            .await
                            .unwrap();
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
                            self.split_root::<BTreeU64>(p_node, false, ts).await;
                            if !p_node.can_insert(&sep_key) {
                                return BTreeSplit::Inconsistent;
                            }
                        }
                        // now parent and child nodes are exclusively locked and parent has enough
                        // space to insert separator key, so do actual split.
                        let r_guard = self.pool.allocate_page::<BTreeNode>().await;
                        self.split_node::<BTreeU64>(p_node, c_node, r_guard, sep_idx, &sep_key, ts)
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
        let hints_enabled = root.header_hints_enabled();
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
        left_node.init(
            height,
            ts,
            &lower_fence_key,
            lower_fence_value,
            &sep_key,
            hints_enabled,
        );
        left_node.extend_slots_from::<V>(root, 0, sep_idx);
        left_node.update_hints();
        // Initialize and copy key values to right node.
        right_node.init(
            height,
            ts,
            &sep_key,
            BTreeU64::INVALID_VALUE,
            &[],
            hints_enabled,
        );
        right_node.extend_slots_from::<V>(root, sep_idx, root.count() - sep_idx);
        right_node.update_hints();
        // Initialize temporary root node and insert separator key.
        let mut tmp_root = unsafe {
            let mut new = std::mem::MaybeUninit::<BTreeNode>::zeroed();
            new.assume_init_mut().init(
                height + 1,
                ts,
                &lower_fence_key,
                BTreeU64::from(left_page_id),
                &[],
                hints_enabled,
            );
            new.assume_init()
        };
        let slot_idx = tmp_root.insert(&sep_key, BTreeU64::from(right_page_id));
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
        mut r_guard: PageExclusiveGuard<BTreeNode>,
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
        let mut tmp_l = BTreeNodeBox::alloc(
            c_height,
            ts,
            &c_lower_fence_key,
            c_node.lower_fence_value(),
            sep_key,
            c_node.header_hints_enabled(),
        );
        tmp_l.extend_slots_from::<V>(c_node, 0, sep_idx);
        tmp_l.update_hints();
        // Process right node.
        let right_page_id = r_guard.page_id();
        let right_node = r_guard.page_mut();
        // Initialize and copy key values to right node.
        right_node.init(
            c_node.height() as u16,
            ts,
            sep_key,
            // For leaf node, lower fence value is meaningless.
            // For branch node, only the leftmost branch node has meaningful
            // lower fence value pointing to a valid child.
            // Other branch nodes always have lower fence equal to the first
            // slot key.
            // So, right node does not need to copy lower fence value.
            BTreeU64::INVALID_VALUE,
            &c_upper_fence_key,
            c_node.header_hints_enabled(),
        );
        right_node.extend_slots_from::<V>(c_node, sep_idx, c_node.count() - sep_idx);
        right_node.update_hints();
        // Copy left node to current node.
        c_node.clone_from(&tmp_l);
        drop(tmp_l);
        // Insert right node into parent.
        p_node.insert(sep_key, BTreeU64::from(right_page_id));
    }

    /// Find leaf by given key.
    #[inline]
    async fn find_leaf<S: LockStrategy<Page = BTreeNode>>(&self, key: &[u8]) -> S::Guard {
        loop {
            let res = self.try_find_leaf::<S>(key).await;
            let res = verify_continue!(res);
            return res;
        }
    }

    /// Merge right node into left node, and update separtor key in parent node accordingly.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn merge_node<V: BTreeValue>(
        &self,
        p_node: &mut BTreeNode,
        p_r_idx: usize, // index of right node in parent node.
        l_node: &mut BTreeNode,
        r_node: &mut BTreeNode,
        lower_fence_key: &[u8], // lower fence key of left node.
        upper_fence_key: &[u8], // upper fence key of right node.
        ts: TrxID,
    ) {
        let value_size = mem::size_of::<V>();
        debug_assert!(l_node.height() == r_node.height());
        debug_assert!(p_r_idx < p_node.count());
        debug_assert!(p_node.lookup_child_idx(lower_fence_key) == Some(p_r_idx as isize - 1));
        debug_assert!({
            let mut estimation =
                SpaceEstimation::with_fences(lower_fence_key, upper_fence_key, value_size);
            estimation.add_key_range(l_node, 0, l_node.count());
            estimation.add_key_range(r_node, 0, r_node.count());
            estimation.total_space() <= mem::size_of::<BTreeNode>()
        });
        let ts = ts.max(l_node.ts()).max(r_node.ts()).max(p_node.ts());
        let mut tmp_l = BTreeNodeBox::alloc(
            l_node.height() as u16,
            ts,
            lower_fence_key,
            l_node.lower_fence_value(),
            upper_fence_key,
            l_node.header_hints_enabled(),
        );
        tmp_l.extend_slots_from::<V>(l_node, 0, l_node.count());
        if r_node.count() > 0 {
            tmp_l.extend_slots_from::<V>(r_node, 0, r_node.count());
        }
        tmp_l.update_hints();
        l_node.clone_from(&tmp_l);
        drop(tmp_l);
        p_node.delete_at(p_r_idx, value_size);
        p_node.update_hints();
        p_node.update_ts(ts);
    }

    /// Merge partial right node into left node.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn merge_partial<V: BTreeValue>(
        &self,
        p_node: &mut BTreeNode,
        p_r_idx: usize, // index of right node in parent node.
        l_node: &mut BTreeNode,
        r_node: &mut BTreeNode,
        lower_fence_key: &[u8], // lower fence key of left node.
        sep_key: &[u8],         // separator key of the partial merge.
        upper_fence_key: &[u8], // upper fence key of right node.
        count: usize,
        ts: TrxID,
    ) {
        let value_size = mem::size_of::<V>();
        debug_assert!(l_node.height() == r_node.height());
        debug_assert!(p_r_idx < p_node.count());
        debug_assert!(p_node.lookup_child_idx(lower_fence_key) == Some(p_r_idx as isize - 1));
        debug_assert!(count > 0 && count < r_node.count());
        debug_assert!(&r_node.create_sep_key(count, r_node.height() == 0)[..] == sep_key);
        debug_assert!({
            let mut estimation = SpaceEstimation::with_fences(lower_fence_key, sep_key, value_size);
            estimation.add_key_range(l_node, 0, l_node.count());
            estimation.add_key_range(r_node, 0, count);
            estimation.total_space() <= mem::size_of::<BTreeNode>()
        });
        let ts = ts.max(l_node.ts()).max(r_node.ts()).max(p_node.ts());
        let mut tmp_l = BTreeNodeBox::alloc(
            l_node.height() as u16,
            ts,
            lower_fence_key,
            l_node.lower_fence_value(),
            sep_key,
            l_node.header_hints_enabled(),
        );
        tmp_l.extend_slots_from::<V>(l_node, 0, l_node.count());
        tmp_l.extend_slots_from::<V>(r_node, 0, count);
        tmp_l.update_hints();
        l_node.clone_from(&tmp_l);
        drop(tmp_l);

        let mut tmp_r = {
            let lower_fence_value = if r_node.height() == 0 {
                BTreeU64::INVALID_VALUE
            } else {
                r_node.value::<BTreeU64>(count)
            };
            BTreeNodeBox::alloc(
                r_node.height() as u16,
                ts,
                sep_key,
                lower_fence_value,
                upper_fence_key,
                r_node.header_hints_enabled(),
            )
        };
        tmp_r.extend_slots_from::<V>(r_node, count, r_node.count() - count);
        tmp_r.update_hints();
        r_node.clone_from(&tmp_r);
        drop(tmp_r);
        p_node.update_key::<BTreeU64>(p_r_idx, sep_key);
        p_node.update_hint_if_needed(p_r_idx, sep_key);
        p_node.update_ts(ts);
    }

    #[inline]
    async fn try_find_leaf<S: LockStrategy<Page = BTreeNode>>(
        &self,
        key: &[u8],
    ) -> Validation<S::Guard> {
        let mut p_guard = self
            .pool
            .get_page::<BTreeNode>(self.root, LatchFallbackMode::Spin)
            .await;
        // check root page separately.
        let pu = unsafe { p_guard.page_unchecked() };
        let height = pu.height();
        verify!(p_guard.validate());
        if height == 0 {
            // root is leaf.
            verify!(S::try_lock(&mut p_guard));
            return Valid(S::must_locked(p_guard));
        }
        loop {
            // Current node is not leaf node.
            let pu = unsafe { p_guard.page_unchecked() };
            let height = pu.height();
            match pu.lookup_child(key) {
                LookupChild::Slot(_, page_id) | LookupChild::LowerFence(page_id) => {
                    verify!(p_guard.validate());
                    // As version validation passes, the height must not be 0.
                    debug_assert!(height != 0);
                    if height == 1 {
                        // child node is leaf.
                        let c_guard = self.pool.get_child_page(&p_guard, page_id, S::MODE).await;
                        let c_guard = verify!(c_guard);
                        let c_guard = S::verify_lock_async::<false>(c_guard).await;
                        let c_guard = verify!(c_guard);
                        return Valid(c_guard);
                    }
                    // child node is branch, continue next iteration.
                    let c_guard = self
                        .pool
                        .get_child_page(&p_guard, page_id, LatchFallbackMode::Spin)
                        .await;
                    let c_guard = verify!(c_guard);
                    p_guard = c_guard;
                }
                LookupChild::NotFound => {
                    verify!(p_guard.validate());
                    unreachable!("BTree should always find one leaf for any key");
                }
            }
        }
    }

    #[inline]
    async fn try_find_leaf_with_optimistic_parent<S: LockStrategy<Page = BTreeNode>>(
        &self,
        key: &[u8],
    ) -> Validation<(S::Guard, Option<FacadePageGuard<BTreeNode>>)> {
        let mut p_guard = self
            .pool
            .get_page::<BTreeNode>(self.root, LatchFallbackMode::Spin)
            .await;
        // check root page separately.
        let pu = unsafe { p_guard.page_unchecked() };
        let height = pu.height();
        verify!(p_guard.validate());
        if height == 0 {
            // root is leaf.
            verify!(S::try_lock(&mut p_guard));
            return Valid((S::must_locked(p_guard), None));
        }
        loop {
            // Current node is not leaf node.
            let pu = unsafe { p_guard.page_unchecked() };
            let height = pu.height();
            match pu.lookup_child(key) {
                LookupChild::Slot(_, page_id) | LookupChild::LowerFence(page_id) => {
                    verify!(p_guard.validate());
                    // As version validation passes, the height must not be 0.
                    debug_assert!(height != 0);
                    if height == 1 {
                        // child node is leaf.
                        let c_guard = self.pool.get_child_page(&p_guard, page_id, S::MODE).await;
                        let c_guard = verify!(c_guard);
                        let c_guard = S::verify_lock_async::<false>(c_guard).await;
                        let c_guard = verify!(c_guard);
                        return Valid((c_guard, Some(p_guard)));
                    }
                    // child node is branch, continue next iteration.
                    let c_guard = self
                        .pool
                        .get_child_page(&p_guard, page_id, LatchFallbackMode::Spin)
                        .await;
                    let c_guard = verify!(c_guard);
                    p_guard = c_guard;
                }
                LookupChild::NotFound => {
                    verify!(p_guard.validate());
                    unreachable!("BTree should always find one leaf for any key");
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
                LookupChild::Slot(_, c_page_id) | LookupChild::LowerFence(c_page_id) => {
                    verify!(g.validate());
                    if c_page_id == page_id {
                        let g = g.lock_exclusive_async().await.unwrap();
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
                LookupChild::NotFound => {
                    verify!(g.validate());
                    return Valid(None);
                }
            }
        }
    }

    #[inline]
    async fn deallocate_h1(&self, g: PageExclusiveGuard<BTreeNode>) {
        let p_node = g.page();
        debug_assert!(p_node.height() == 1);
        // Deallocate child associated with lower fence key.
        // Only left-most node has lower fence child.
        if !p_node.lower_fence_value().is_deleted() {
            let c_page_id = p_node.lower_fence_value().to_u64();
            let c_guard = self
                .pool
                .get_page::<BTreeNode>(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .lock_exclusive_async()
                .await
                .unwrap();
            self.pool.deallocate_page::<BTreeNode>(c_guard);
        }
        // Deallocate all children.
        for i in 0..p_node.count() {
            let c_page_id = p_node.value_as_page_id(i);
            let c_guard = self
                .pool
                .get_page::<BTreeNode>(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .lock_exclusive_async()
                .await
                .unwrap();
            self.pool.deallocate_page::<BTreeNode>(c_guard);
        }
        // Deallocate self.
        self.pool.deallocate_page::<BTreeNode>(g);
    }
}

/// Controls how to access B-Tree nodes with coupling way.
pub struct BTreeCoupling<S: LockStrategy> {
    // Parent position to locate target node.
    // can be optional.
    pub(super) parent: Option<ParentPosition<S::Guard>>,
    pub(super) node: Option<S::Guard>,
}

impl<S: LockStrategy<Page = BTreeNode>> BTreeCoupling<S>
where
    S::Guard: PageGuard<BTreeNode>,
{
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        BTreeCoupling {
            parent: None,
            node: None,
        }
    }

    /// Seek given key and lock the node at given height with its parent.
    #[inline]
    pub async fn seek_and_lock(&mut self, tree: &BTree, height: usize, key: &[u8]) {
        loop {
            self.reset();
            let g = tree
                .pool
                .get_page::<BTreeNode>(tree.root, LatchFallbackMode::Spin)
                .await;
            let res = self.try_seek_and_lock(tree, height, key, g).await;
            verify_continue!(res);
            return;
        }
    }

    /// Release both locks
    #[inline]
    pub fn reset(&mut self) {
        self.parent.take();
        self.node.take();
    }

    #[inline]
    async fn try_seek_and_lock(
        &mut self,
        tree: &BTree,
        height: usize,
        key: &[u8],
        mut p_guard: FacadePageGuard<BTreeNode>,
    ) -> Validation<()> {
        loop {
            let pu = unsafe { p_guard.page_unchecked() };
            let curr_height = pu.height();
            if curr_height < height {
                verify!(p_guard.validate());
                // tree height is smaller than searched height.
                return Valid(());
            }
            if curr_height == height {
                verify!(S::try_lock(&mut p_guard));
                self.node = Some(S::must_locked(p_guard));
                return Valid(());
            }
            if curr_height == height + 1 {
                // Parent is locked with same mode as child.
                verify!(S::try_lock(&mut p_guard));
                let p_guard = S::must_locked(p_guard);
                let p_node = p_guard.page();
                debug_assert!(!p_node.is_leaf());
                let (idx, c_page_id) = match p_node.lookup_child(key) {
                    LookupChild::Slot(idx, c_page_id) => (idx as isize, c_page_id),
                    LookupChild::LowerFence(c_page_id) => (-1, c_page_id),
                    LookupChild::NotFound => unreachable!(),
                };
                let c_guard = tree.pool.get_page::<BTreeNode>(c_page_id, S::MODE).await;
                let res = S::verify_lock_async::<false>(c_guard).await;
                let c_guard = verify!(res);
                self.parent = Some(ParentPosition { g: p_guard, idx });
                self.node = Some(c_guard);
                return Valid(());
            }
            let c_page_id = match pu.lookup_child(key) {
                LookupChild::Slot(_, c_page_id) => c_page_id,
                LookupChild::LowerFence(c_page_id) => c_page_id,
                LookupChild::NotFound => unreachable!(),
            };
            verify!(p_guard.validate());
            // Before access parent node, we always use optimistic spin lock.
            let c_guard = tree
                .pool
                .get_child_page::<BTreeNode>(&p_guard, c_page_id, LatchFallbackMode::Spin)
                .await;
            p_guard = verify!(c_guard);
        }
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

/// NodePurgeList contains all nodes that have been removed from
/// B-tree.
/// It's possible other thread still want to access the node even
/// when the node has been already removed from the tree.
/// From normal path(root to leaf), others can not see them.
/// But sometimes, we cache optimistic guard of node and access it
/// later.
/// To make such operation safe, we cannot just deallocate the node
/// when removing it from tree.
/// But we put it in a purge list and let background thread to remove
/// when the time is safe.
/// Safe time mean no other thread can access them.
/// This is very similar to transactional GC: all active transactions
/// has larger timestamp than the removal timestamp.
pub struct NodePurgeList {
    /// The timestamp when nodes are removed from the tree.
    pub ts: TrxID,
    pub nodes: Vec<PageExclusiveGuard<BTreeNode>>,
}

/// Shared cursor of nodes at given height.
/// At most two locks are held at the same time.
///
/// This cursor does not guarantee consistent snapshot during iterating
/// the tree nodes.
/// But it guarantees if a value(node) does not change during the traverse,
/// it will be always visited.
pub struct BTreeNodeCursor<'a> {
    tree: &'a BTree,
    height: usize,
    coupling: BTreeCoupling<SharedStrategy>,
}

impl<'a> BTreeNodeCursor<'a> {
    /// Create a new cursor.
    #[inline]
    pub fn new(tree: &'a BTree, height: usize) -> Self {
        BTreeNodeCursor {
            tree,
            height,
            coupling: BTreeCoupling::<SharedStrategy>::new(),
        }
    }

    #[inline]
    pub async fn seek(&mut self, key: &[u8]) {
        self.coupling
            .seek_and_lock(self.tree, self.height, key)
            .await
    }

    /// Fetch next node.
    #[inline]
    pub async fn next(&mut self) -> Option<PageSharedGuard<BTreeNode>> {
        if let Some(g) = self.coupling.node.take() {
            return Some(g);
        }
        if let Some(parent) = self.coupling.parent.as_ref() {
            let p_node = parent.g.page();
            let next_idx = (parent.idx + 1) as usize;
            if next_idx == p_node.count() {
                // current parent exhausted.
                if p_node.has_no_upper_fence() {
                    // The tree exhausted.
                    self.coupling.reset();
                    return None;
                }
                let upper_fence_key = p_node.upper_fence_key();
                // reach next key.
                self.coupling
                    .seek_and_lock(self.tree, self.height, &upper_fence_key)
                    .await;
                if let Some(g) = self.coupling.node.take() {
                    return Some(g);
                }
                return None;
            }
            // Get next slot value, then child node
            let c_page_id = p_node.value_as_page_id(next_idx);
            self.coupling.parent.as_mut().unwrap().idx = next_idx as isize;
            let c_guard = self
                .tree
                .pool
                .get_page::<BTreeNode>(c_page_id, LatchFallbackMode::Shared)
                .await
                .lock_shared_async()
                .await
                .unwrap();
            return Some(c_guard);
        }
        None
    }
}

/// Very simple configuration on B-tree Compaction.
/// Compaction will be triggered if node effective space
/// is lower than low ratio, and will try best to fit
/// high ratio after compaction.
/// The most compact strategy is to set the two ratio to
/// 1.0 and 1.0. So every node will be compacted and merged
/// if possible.
#[derive(Debug, Clone, Copy)]
pub struct BTreeCompactConfig {
    low_ratio: f64,
    high_ratio: f64,
}

impl BTreeCompactConfig {
    #[inline]
    pub fn new(low_ratio: f64, high_ratio: f64) -> Result<Self> {
        if !(0.0..=1.0).contains(&low_ratio)
            || !(0.0..=1.0).contains(&high_ratio)
            || high_ratio < low_ratio
        {
            return Err(Error::InvalidArgument);
        }
        Ok(BTreeCompactConfig {
            low_ratio,
            high_ratio,
        })
    }
}

impl Default for BTreeCompactConfig {
    #[inline]
    fn default() -> Self {
        BTreeCompactConfig {
            low_ratio: 1.0,
            high_ratio: 1.0,
        }
    }
}

/// BTree compactor on given height.
/// It behaves like a cursor.
/// It can lock three nodes at the same time(one parent and two children).
/// It will try to merge right node into left.
pub struct BTreeCompactor<'a, V: BTreeValue> {
    tree: &'a BTree,
    // height of nodes to be compacted.
    // If height is greater than 0, V should always be PageID.
    height: usize,
    low_space: usize,
    high_space: usize,
    coupling: BTreeCoupling<ExclusiveStrategy>,
    _marker: PhantomData<V>,
}

impl<'a, V: BTreeValue> BTreeCompactor<'a, V> {
    #[inline]
    pub fn new(tree: &'a BTree, height: usize, config: BTreeCompactConfig) -> Self {
        let low_space = ((mem::size_of::<BTreeNode>() as f64 * config.low_ratio) as usize)
            .min(mem::size_of::<BTreeNode>());
        let high_space = ((mem::size_of::<BTreeNode>() as f64 * config.high_ratio) as usize)
            .min(mem::size_of::<BTreeNode>());
        BTreeCompactor {
            tree,
            height,
            low_space,
            high_space,
            coupling: BTreeCoupling::<ExclusiveStrategy>::new(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub async fn seek(&mut self, key: &[u8]) {
        self.coupling
            .seek_and_lock(self.tree, self.height, key)
            .await
    }

    #[inline]
    pub async fn run_to_end(mut self, purge_list: &mut Vec<PageExclusiveGuard<BTreeNode>>) {
        let mut lower_fence_key_buffer = Vec::new();
        let mut upper_fence_key_buffer = Vec::new();
        self.seek(&[]).await;
        loop {
            let res = self
                .step(
                    &mut lower_fence_key_buffer,
                    &mut upper_fence_key_buffer,
                    purge_list,
                )
                .await;
            match res {
                BTreeCompact::Skip | BTreeCompact::OutOfSpace | BTreeCompact::ChildDone => (),
                BTreeCompact::ParentDone => {
                    if upper_fence_key_buffer.is_empty() {
                        return;
                    }
                    self.seek(&upper_fence_key_buffer).await;
                }
                BTreeCompact::AllDone => {
                    // release all locks.
                    self.coupling.reset();
                    return;
                }
            }
        }
    }

    /// This method compact one node and try to merge right nodes into it.
    #[inline]
    async fn step(
        &mut self,
        lower_fence_key_buffer: &mut Vec<u8>,
        upper_fence_key_buffer: &mut Vec<u8>,
        purge_list: &mut Vec<PageExclusiveGuard<BTreeNode>>,
    ) -> BTreeCompact {
        if self.coupling.parent.is_none() {
            // Single-node compaction.
            if let Some(mut g) = self.coupling.node.take() {
                let node = g.page_mut();
                if node.used_space() != node.effective_space()
                    && node.effective_space() < self.low_space
                {
                    if self.height == 0 {
                        node.self_compact::<V>();
                    } else {
                        node.self_compact::<BTreeU64>();
                    }
                }
            }
            return BTreeCompact::AllDone;
        }

        if !self.lock_current().await {
            return BTreeCompact::AllDone;
        }
        let l_node = self.coupling.node.as_mut().unwrap().page_mut();

        if l_node.effective_space() >= self.low_space {
            // Left node's effective space is larger than low space, skip it.
            if self.skip().await {
                return BTreeCompact::Skip;
            } else {
                return BTreeCompact::AllDone;
            }
        }
        // Left node's effective space is smaller than low space
        loop {
            match self.lock_right().await {
                Some((p_r_idx, mut r_guard)) => {
                    // There are right nodes, so try compact and merge right nodes
                    // until high space is reached.
                    let l_node = self.coupling.node.as_mut().unwrap().page_mut();
                    let r_node = r_guard.page_mut();
                    // Estimate space after compaction.
                    lower_fence_key_buffer.clear();
                    l_node.extend_lower_fence_key(lower_fence_key_buffer);
                    upper_fence_key_buffer.clear();
                    r_node.extend_upper_fence_key(upper_fence_key_buffer);
                    let ts = l_node.ts().max(r_node.ts());
                    let mut estimation = SpaceEstimation::with_fences(
                        lower_fence_key_buffer,
                        upper_fence_key_buffer,
                        mem::size_of::<V>(),
                    );
                    estimation.add_key_range(l_node, 0, l_node.count());
                    if estimation.total_space() > mem::size_of::<BTreeNode>() {
                        // fence key change results in a node out of space.
                        self.coupling.node.replace(r_guard);
                        self.coupling.parent.as_mut().unwrap().idx = p_r_idx as isize;
                        return BTreeCompact::OutOfSpace;
                    }
                    if r_node.count() == 0 {
                        // Special case: right node is empty.
                        // The merge must succeed.
                        let p_node = self.coupling.parent.as_mut().unwrap().g.page_mut();
                        self.tree.merge_node::<V>(
                            p_node,
                            p_r_idx,
                            l_node,
                            r_node,
                            lower_fence_key_buffer,
                            upper_fence_key_buffer,
                            ts,
                        );
                        // Put right node into purge list.
                        purge_list.push(r_guard);
                        continue;
                    }
                    // find a suitable position to merge.
                    let sep_idx = estimation.grow_until_threshold(r_node, self.high_space);
                    if sep_idx == 0 {
                        // can not add one key to left node.
                        let res = self.skip().await;
                        debug_assert!(res);
                        return BTreeCompact::Skip;
                    }
                    if sep_idx == r_node.count() {
                        // All keys in right node can be merged into left node.
                        let p_node = self.coupling.parent.as_mut().unwrap().g.page_mut();
                        self.tree.merge_node::<V>(
                            p_node,
                            p_r_idx,
                            l_node,
                            r_node,
                            lower_fence_key_buffer,
                            upper_fence_key_buffer,
                            ts,
                        );
                        // Put right node into purge list.
                        purge_list.push(r_guard);
                        continue;
                    }
                    let sep_key = r_node.create_sep_key(sep_idx, r_node.height() == 0);
                    let parent = self.coupling.parent.as_mut().unwrap();
                    let p_node = parent.g.page_mut();
                    // check if parent has enough space to update key.
                    if !p_node.prepare_update_key::<BTreeU64>(p_r_idx, &sep_key) {
                        self.coupling.node.replace(r_guard);
                        self.coupling.parent.as_mut().unwrap().idx = p_r_idx as isize;
                        return BTreeCompact::OutOfSpace;
                    }
                    self.tree.merge_partial::<V>(
                        p_node,
                        p_r_idx,
                        l_node,
                        r_node,
                        lower_fence_key_buffer,
                        &sep_key,
                        upper_fence_key_buffer,
                        sep_idx,
                        ts,
                    );
                    parent.idx = p_r_idx as isize;
                    self.coupling.node.replace(r_guard);
                    return BTreeCompact::ChildDone;
                }
                None => {
                    // Current parent done.
                    // We cache parent's upper fence key into buffer
                    // in order to search next parent node.
                    let p_node = self.coupling.parent.as_mut().unwrap().g.page_mut();
                    upper_fence_key_buffer.clear();
                    p_node.extend_upper_fence_key(upper_fence_key_buffer);
                    self.coupling.reset();
                    return BTreeCompact::ParentDone;
                }
            }
        }
    }

    // Skip current node and take next node.
    // Return false if next node not found.
    #[inline]
    async fn skip(&mut self) -> bool {
        drop(self.coupling.node.take());
        if let Some(parent) = self.coupling.parent.as_mut() {
            let p_node = parent.g.page();
            let next_idx = (parent.idx + 1) as usize;
            if next_idx == p_node.count() {
                if p_node.has_no_upper_fence() {
                    self.coupling.parent.take();
                    return false;
                }
                let upper_fence = p_node.upper_fence_key();
                self.seek(&upper_fence).await;
                return true;
            }
            let c_page_id = p_node.value_as_page_id(next_idx);
            let c_guard = self
                .tree
                .pool
                .get_page(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .lock_exclusive_async()
                .await
                .unwrap();

            self.coupling.parent.as_mut().unwrap().idx = next_idx as isize;
            self.coupling.node.replace(c_guard);
            return true;
        }
        false
    }

    #[inline]
    async fn lock_right(&mut self) -> Option<(usize, PageExclusiveGuard<BTreeNode>)> {
        if let Some(parent) = self.coupling.parent.as_mut() {
            let p_node = parent.g.page();
            let next_idx = (parent.idx + 1) as usize;
            if next_idx == p_node.count() {
                return None;
            }
            let c_page_id = p_node.value_as_page_id(next_idx);
            let c_guard = self
                .tree
                .pool
                .get_page(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .lock_exclusive_async()
                .await
                .unwrap();
            return Some((next_idx, c_guard));
        }
        None
    }

    #[inline]
    async fn lock_current(&mut self) -> bool {
        if self.coupling.node.is_some() {
            return true;
        }
        if let Some(parent) = self.coupling.parent.as_mut() {
            let p_node = parent.g.page();
            let c_page_id = if parent.idx == -1 {
                p_node.lower_fence_value().to_u64()
            } else {
                p_node.value_as_page_id(parent.idx as usize)
            };
            let c_guard = self
                .tree
                .pool
                .get_page(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .lock_exclusive_async()
                .await
                .unwrap();
            self.coupling.node = Some(c_guard);
            return true;
        }
        false
    }
}

enum BTreeCompact {
    // Skip current node for compaction.
    Skip,
    // Current node fail to participant compaction because
    // of space is insufficient.
    OutOfSpace,
    // all nodes are done.
    AllDone,
    // one child is done, the argument indicates how many
    // nodes are merged into current one.
    ChildDone,
    // parent is done.
    ParentDone,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifetime::StaticLifetime;
    use event_listener::Event;
    use rand_distr::{Distribution, Uniform};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

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
                let tree = BTree::new(pool, false, 200).await;
                // insert 1, 2, 3.
                let one = BTreeU64::from(1);
                let three = BTreeU64::from(3);
                let five = BTreeU64::from(5);
                let seven = BTreeU64::from(7);
                let fifty = BTreeU64::from(50);
                let seventy = BTreeU64::from(70);
                let res = tree.insert(&1u64.to_be_bytes(), one, false, 210).await;
                assert!(res.is_ok());
                let res = tree.insert(&3u64.to_be_bytes(), three, false, 220).await;
                assert!(res.is_ok());
                let res = tree.insert(&5u64.to_be_bytes(), five, false, 205).await;
                assert!(res.is_ok());

                let res = tree.insert(&5u64.to_be_bytes(), five, false, 230).await;
                assert!(res == BTreeInsert::DuplicateKey(five));

                // look up
                let res = tree
                    .lookup_optimistic::<BTreeU64>(&1u64.to_be_bytes())
                    .await;
                assert!(res.is_some());
                let res = tree
                    .lookup_optimistic::<BTreeU64>(&4u64.to_be_bytes())
                    .await;
                assert!(res.is_none());
                // mark as deleted
                let res = tree.mark_as_deleted(&3u64.to_be_bytes(), three, 230).await;
                assert!(res.is_ok());
                let res = tree.mark_as_deleted(&5u64.to_be_bytes(), five, 230).await;
                assert!(res.is_ok());
                let res = tree.mark_as_deleted(&7u64.to_be_bytes(), seven, 235).await;
                assert_eq!(res, BTreeUpdate::NotFound);
                let res = tree
                    .lookup_optimistic::<BTreeU64>(&5u64.to_be_bytes())
                    .await;
                assert_eq!(res, Some(five.deleted()));
                // update
                let res = tree
                    .update(&5u64.to_be_bytes(), five.deleted(), fifty, 240)
                    .await;
                assert!(res.is_ok());
                let res = tree
                    .update(&5u64.to_be_bytes(), five.deleted(), seventy, 245)
                    .await;
                assert_eq!(res, BTreeUpdate::ValueMismatch(fifty));
                // delete
                let res = tree.delete(&3u64.to_be_bytes(), three, true, 250).await;
                assert!(res.is_ok());
                let res = tree.delete(&5u64.to_be_bytes(), five, true, 255).await;
                assert_eq!(res, BTreeDelete::ValueMismatch);
            }

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_scale() {
        const ROWS: u64 = 90089;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(3 * 1024 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, false, 200).await;

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                let mut printed1 = false;
                let mut printed2 = false;
                for i in 0u64..ROWS {
                    // let k = i.to_be_bytes();
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, BTreeU64::from(i), false, 201).await;
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
                let space_stat = tree.collect_space_statistics().await;
                println!("tree space statistics: {:?}", space_stat);

                let mut map: HashMap<usize, LevelStat> = HashMap::new();
                for height in 0usize..3 {
                    let mut stat = LevelStat::default();
                    let mut cursor = tree.cursor(height);
                    cursor.seek(&[]).await;
                    while let Some(g) = cursor.next().await {
                        let node = g.page();
                        stat.nodes += 1;
                        stat.keys += node.count();
                        if node.count() > 0 {
                            stat.first_key_len += node.key(0).len();
                        }
                        stat.prefix_len += node.prefix_len();
                    }
                    println!(
                        "height={}, nodes={}, keys={}, keys/node={:.2}, key0len/node={:.2}, prefixlen/node={:.2}",
                        height,
                        stat.nodes,
                        stat.keys,
                        stat.keys as f64 / stat.nodes as f64,
                        stat.first_key_len as f64 / stat.nodes as f64,
                        stat.prefix_len as f64 / stat.nodes as f64,
                    );
                    map.insert(height, stat);
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

    #[test]
    fn test_btree_delete() {
        const ROWS: u64 = 90122;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(3 * 1024 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, false, 200).await;

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                for i in 0u64..ROWS {
                    // let k = i.to_be_bytes();
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, BTreeU64::from(i), false, 201).await;
                    assert!(res.is_ok());
                }

                for i in 0u64..ROWS {
                    // let k = i.to_be_bytes();
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.delete(&key, BTreeU64::from(i), true, 202).await;
                    assert!(res.is_ok());
                }

                println!("tree height {}", tree.height());
                let space_stat = tree.collect_space_statistics().await;
                println!("tree space statistics: {:?}", space_stat);

                let mut map: HashMap<usize, LevelStat> = HashMap::new();
                for height in 0usize..3 {
                    let mut stat = LevelStat::default();
                    let mut cursor = tree.cursor(height);
                    cursor.seek(&[]).await;
                    while let Some(g) = cursor.next().await {
                        let node = g.page();
                        stat.nodes += 1;
                        stat.keys += node.count();
                        if node.count() > 0 {
                            stat.first_key_len += node.key(0).len();
                        }
                        stat.prefix_len += node.prefix_len();
                    }
                    println!(
                        "height={}, nodes={}, keys={}, keys/node={:.2}, key0len/node={:.2}, prefixlen/node={:.2}",
                        height,
                        stat.nodes,
                        stat.keys,
                        stat.keys as f64 / stat.nodes as f64,
                        stat.first_key_len as f64 / stat.nodes as f64,
                        stat.prefix_len as f64 / stat.nodes as f64,
                    );
                    map.insert(height, stat);
                }
                // 90122 rows will construct btree with height 2.
                assert!(map.contains_key(&2));
                // all keys are deleted in leaf nodes.
                assert!(map[&0].keys as u64 == 0);
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

    #[test]
    fn test_btree_compact() {
        const ROWS: u64 = 90089;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(3 * 1024 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, false, 200).await;

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                for i in 0u64..ROWS {
                    // let k = i.to_be_bytes();
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, BTreeU64::from(i), false, 201).await;
                    assert!(res.is_ok());
                }

                for i in 0u64..ROWS {
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.delete(&key, BTreeU64::from(i), true, 202).await;
                    assert!(res.is_ok());
                }

                println!("tree height {}", tree.height());
                let space_stat = tree.collect_space_statistics().await;
                println!("Before compaction, tree space statistics: {:?}", space_stat);

                let config = BTreeCompactConfig::new(1.0, 1.0).unwrap();
                let purge_list = tree.compact_all::<BTreeU64>(config).await;

                let space_stat = tree.collect_space_statistics().await;
                println!("After compaction, tree space statistics: {:?}", space_stat);

                println!("{} pages have been removed from the tree", purge_list.len());
                println!("tree height: {}", tree.height());

                for g in purge_list {
                    pool.deallocate_page(g);
                }

                let mut map: HashMap<usize, LevelStat> = HashMap::new();
                for height in 0usize..=tree.height() {
                    let mut stat = LevelStat::default();
                    let mut cursor = tree.cursor(height);
                    cursor.seek(&[]).await;
                    while let Some(g) = cursor.next().await {
                        let node = g.page();
                        stat.nodes += 1;
                        stat.keys += node.count();
                        if node.count() > 0 {
                            stat.first_key_len += node.key(0).len();
                        }
                        stat.prefix_len += node.prefix_len();
                    }
                    println!(
                        "height={}, nodes={}, keys={}, keys/node={:.2}, key0len/node={:.2}, prefixlen/node={:.2}",
                        height,
                        stat.nodes,
                        stat.keys,
                        stat.keys as f64 / stat.nodes as f64,
                        stat.first_key_len as f64 / stat.nodes as f64,
                        stat.prefix_len as f64 / stat.nodes as f64,
                    );
                    map.insert(height, stat);
                }
                // all keys are deleted in leaf nodes.
                assert!(map[&0].keys as u64 == 0);
                for h in 1..=tree.height() {
                    // level N+1 keys plus one(lower fence key of leftmost node)
                    // should be equal to level N nodes.
                    assert!(map[&h].keys + 1 == map[&(h - 1)].nodes);
                }
            }

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_lookup_disable_hints() {
        const ROWS: usize = 100000;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(100 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, false, 200).await;
                let mut map = BTreeMap::new();

                // number less than 10 million
                let between = Uniform::new(0u64, 10_000_000).unwrap();
                let mut rng = rand::rng();
                for i in 0..ROWS {
                    let k = between.sample(&mut rng);
                    let res1 = tree
                        .insert(&k.to_be_bytes(), BTreeU64::from(i as u64), false, 201)
                        .await;
                    let res2 = map.entry(k).or_insert_with(|| i as u64);
                    assert!(res1.is_ok() == (*res2 == i as u64));
                }
                println!("tree height {}", tree.height());
                let space_stat = tree.collect_space_statistics().await;
                println!("tree space statistics: {:?}", space_stat);

                for _ in 0..ROWS {
                    let k = between.sample(&mut rng);
                    let res1 = tree.lookup_optimistic::<BTreeU64>(&k.to_be_bytes()).await;
                    let res2 = map.get(&k);
                    if let Some(v) = res2 {
                        assert!(res1 == Some(BTreeU64::from(*v)));
                    } else {
                        assert!(res1.is_none());
                    }
                }
            }

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_lookup_enable_hints() {
        use rand::prelude::*;
        use rand_chacha::ChaCha8Rng;
        use rand_distr::Distribution;
        const ROWS: usize = 100000;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(100 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, true, 200).await;
                let mut map = BTreeMap::new();

                // number less than 10 million
                let between = Uniform::new(0u64, 10_000_000).unwrap();
                let mut rng = ChaCha8Rng::seed_from_u64(0u64);
                for i in 0..ROWS {
                    let k = between.sample(&mut rng);
                    // println!("row k={}, i={} sampled", k, i);
                    let res1 = tree
                        .insert(&k.to_be_bytes(), BTreeU64::from(i as u64), false, 201)
                        .await;
                    let res2 = map.entry(k).or_insert_with(|| i as u64);
                    assert!(res1.is_ok() == (*res2 == i as u64));
                    // println!("row k={}, i={} inserted", k, i);
                }
                println!("tree height {}", tree.height());
                let space_stat = tree.collect_space_statistics().await;
                println!("tree space statistics: {:?}", space_stat);

                for _ in 0..ROWS {
                    let k = between.sample(&mut rng);
                    let res1 = tree.lookup_optimistic::<BTreeU64>(&k.to_be_bytes()).await;
                    let res2 = map.get(&k);
                    if let Some(v) = res2 {
                        assert!(res1 == Some(BTreeU64::from(*v)));
                    } else {
                        assert!(res1.is_none());
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_with_stdmap() {
        smol::block_on(async {
            const ROWS: u64 = 10_000;
            const MAX_VALUE: u64 = 100_000;
            let pool = FixedBufferPool::with_capacity_static(20 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, false, 1).await;

                let start = Instant::now();
                // insert with random distribution.
                {
                    let between = Uniform::new(0u64, MAX_VALUE).unwrap();
                    let mut thd_rng = rand::rng();
                    for i in 0..ROWS {
                        let k = between.sample(&mut thd_rng);
                        tree.insert(&k.to_be_bytes(), BTreeU64::from(i), false, 100)
                            .await;
                    }
                }
                let dur = start.elapsed();

                let qps = ROWS as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
                let op_nanos = dur.as_nanos() as f64 / ROWS as f64;
                println!(
                    "btree rand insert: dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
                    dur.as_millis(),
                    ROWS,
                    qps,
                    op_nanos
                );

                let start = Instant::now();

                // lookup with random distribution.
                {
                    let between = Uniform::new(0, MAX_VALUE).unwrap();
                    let mut thd_rng = rand::rng();
                    for i in 0..ROWS {
                        let k = between.sample(&mut thd_rng);
                        tree.insert(&k.to_be_bytes(), BTreeU64::from(i), false, 100)
                            .await;
                    }
                }

                let dur = start.elapsed();

                let qps = ROWS as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
                let op_nanos = dur.as_nanos() as f64 / ROWS as f64;
                println!(
                    "btree rand lookup: dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
                    dur.as_millis(),
                    ROWS,
                    qps,
                    op_nanos
                );
            }
            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_split() {
        // number 90088 will trigger 2 level b-tree split.
        const ROWS: u64 = 90088;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(3 * 1024 * 1024 * 1024).unwrap();
            {
                let tree = Arc::new(BTree::new(pool, false, 200).await);

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                for i in 0u64..ROWS {
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, BTreeU64::from(i), false, 201).await;
                    assert!(res.is_ok());
                }
                let event = Event::new();
                let listener = event.listen();
                {
                    let tree = Arc::clone(&tree);
                    std::thread::spawn(move || {
                        smol::block_on(async {
                            let k = 90088u64.to_be_bytes();
                            let res = tree
                                .try_find_leaf_with_optimistic_parent::<SharedStrategy>(&k)
                                .await;
                            let (c_guard, p_guard) = res.unwrap();
                            drop(c_guard);
                            let p_guard = p_guard.unwrap();
                            let shared_guard = p_guard.lock_shared_async().await.unwrap();
                            event.notify(1);
                            smol::Timer::after(Duration::from_millis(1000)).await;
                            println!("going to drop");
                            drop(shared_guard);
                        })
                    });
                }
                // wait for another thread to acquire parent's shared lock.
                listener.await;
                println!("tree height {}", tree.height());
                key[..8].copy_from_slice(&90088u64.to_be_bytes()[..]);
                let res = tree.insert(&key, BTreeU64::from(90088), false, 202).await;
                assert!(res.is_ok());
                println!("insert ok");
                println!("tree height {}", tree.height());
                let stat = tree.collect_space_statistics().await;
                println!("tree space statistics: {:?}", stat)
            }

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_concurrent_split() {
        // number 90088 will trigger 2 level b-tree split.
        const ROWS: u64 = 90088;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(3 * 1024 * 1024 * 1024).unwrap();
            {
                let tree = Arc::new(BTree::new(pool, false, 200).await);

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                for i in 0u64..ROWS {
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, BTreeU64::from(i), false, 201).await;
                    assert!(res.is_ok());
                }
                let event = Event::new();
                let mut handles = Vec::with_capacity(10);
                for j in 90088u64..90098 {
                    let tree = Arc::clone(&tree);
                    let listener = event.listen();
                    let handle = std::thread::spawn(move || {
                        smol::block_on(async {
                            let mut key = vec![0u8; 1000];
                            key[..8].copy_from_slice(&j.to_be_bytes()[..]);

                            listener.await;

                            let res = tree.insert(&key, BTreeU64::from(90088), false, 202).await;
                            assert!(res.is_ok());
                        })
                    });
                    handles.push(handle);
                }
                // wait for another thread to acquire parent's shared lock.
                event.notify(usize::MAX);
                smol::Timer::after(Duration::from_millis(500)).await;
                println!("tree height {}", tree.height());
                let stat = tree.collect_space_statistics().await;
                println!("tree space statistics: {:?}", stat)
            }

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_merge_partial() {
        const ROWS: usize = 100000;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(100 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, false, 200).await;
                // number less than 10 million
                let between = Uniform::new(0u64, 10_000_000).unwrap();
                let mut rng = rand::rng();
                for i in 0..ROWS {
                    let k = between.sample(&mut rng);
                    let _ = tree
                        .insert(&k.to_be_bytes(), BTreeU64::from(i as u64), false, 201)
                        .await;
                }
                let space_stat = tree.collect_space_statistics().await;
                println!(
                    "before compaction, tree height {}, space statistics: {:?}",
                    tree.height(),
                    space_stat
                );

                let config = BTreeCompactConfig::new(1.0, 1.0).unwrap();
                tree.compact_all::<BTreeU64>(config).await;

                let space_stat = tree.collect_space_statistics().await;
                println!(
                    "after compaction, tree height {}, space statistics: {:?}",
                    tree.height(),
                    space_stat
                );
            }

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_destory() {
        const H0_ROWS: u64 = 10;
        const H1_ROWS: u64 = 1000;
        const H2_ROWS: u64 = 100000;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(2 * 1024 * 1024 * 1024).unwrap();
            assert!(pool.allocated() == 0);
            // height=0
            {
                let tree = BTree::new(pool, false, 200).await;
                let mut key = vec![0u8; 1000];
                for i in 0..H0_ROWS {
                    key[..8].copy_from_slice(&i.to_be_bytes());
                    tree.insert(&key, BTreeU64::from(i), false, 201).await;
                }
                println!(
                    "BTree with {} keys occupies {} pages",
                    H2_ROWS,
                    pool.allocated()
                );
                tree.destory().await;
                assert!(pool.allocated() == 0);
            }
            // height=1
            {
                let tree = BTree::new(pool, false, 200).await;
                let mut key = vec![0u8; 1000];
                for i in 0..H1_ROWS {
                    key[..8].copy_from_slice(&i.to_be_bytes());
                    tree.insert(&key, BTreeU64::from(i), false, 201).await;
                }
                println!(
                    "BTree with {} keys occupies {} pages",
                    H2_ROWS,
                    pool.allocated()
                );
                tree.destory().await;
                assert!(pool.allocated() == 0);
            }
            // height=2
            {
                let tree = BTree::new(pool, false, 200).await;
                let mut key = vec![0u8; 1000];
                for i in 0..H2_ROWS {
                    key[..8].copy_from_slice(&i.to_be_bytes());
                    tree.insert(&key, BTreeU64::from(i), false, 201).await;
                }
                println!(
                    "BTree with {} keys occupies {} pages",
                    H2_ROWS,
                    pool.allocated()
                );
                tree.destory().await;
                assert!(pool.allocated() == 0);
            }
            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }
}
