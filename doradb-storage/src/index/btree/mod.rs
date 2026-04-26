pub(crate) mod algo;
mod hint;
mod key;
mod node;
mod scan;
mod value;

use crate::buffer::PageID;
use crate::buffer::guard::{
    ExclusiveLockStrategy, FacadePageGuard, LockStrategy, OptimisticLockStrategy,
    PageExclusiveGuard, PageGuard, PageSharedGuard, SharedLockStrategy,
};
use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard};
use crate::error::Validation;
use crate::error::Validation::{Invalid, Valid};
use crate::error::{ConfigError, Result};
use crate::index::btree::algo::{
    KnownFenceNodeParams, MemTreeSiblingMergePlan, NodeSlotRange, pack_node_range_box,
    pack_node_range_into, pack_node_ranges_box, plan_memtree_sibling_merge,
};
use crate::index::util::{Maskable, ParentPosition, SpaceStatistics};
use crate::latch::LatchFallbackMode;
use crate::quiescent::QuiescentGuard;
use crate::trx::TrxID;
use either::Either;
use error_stack::Report;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) use hint::*;
pub use key::*;
pub use node::*;
pub use scan::*;
pub use value::*;

pub type SharedStrategy = SharedLockStrategy<BTreeNode>;
pub type ExclusiveStrategy = ExclusiveLockStrategy<BTreeNode>;
pub type OptimisticStrategy = OptimisticLockStrategy<BTreeNode>;

macro_rules! verify_result {
    ($expr:expr) => {
        match $expr {
            Valid(v) => v,
            Invalid => return Ok(Invalid),
        }
    };
}

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

/// Generic B-Tree storage wrapper over a buffer-pool implementation.
pub struct GenericBTree<P: 'static> {
    root: PageID,
    /// Height of this btree.
    height: AtomicUsize,
    // buffer pool to hold this index structure.
    // todo: switch to ShadowBufferPool, which
    // supports CoW operations.
    pool: QuiescentGuard<P>,
}

/// Compatibility alias for runtime B-Tree backed by `FixedBufferPool`.
pub type BTree = GenericBTree<FixedBufferPool>;

impl<P: BufferPool> GenericBTree<P> {
    /// Create a new B-Tree index.
    #[inline]
    pub async fn new(
        pool: QuiescentGuard<P>,
        pool_guard: &PoolGuard,
        hints_enabled: bool,
        ts: TrxID,
    ) -> Result<Self> {
        let mut g = pool.allocate_page::<BTreeNode>(pool_guard).await?;
        let page_id = g.page_id();
        let page = g.page_mut();
        page.init(0, ts, &[], BTreeU64::INVALID_VALUE, &[], hints_enabled);
        Ok(GenericBTree {
            root: page_id,
            height: AtomicUsize::new(0),
            pool,
        })
    }

    #[inline]
    async fn allocate_node(&self, pool_guard: &PoolGuard) -> Result<PageExclusiveGuard<BTreeNode>> {
        self.pool.allocate_page::<BTreeNode>(pool_guard).await
    }

    #[inline]
    async fn get_node(
        &self,
        pool_guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Result<FacadePageGuard<BTreeNode>> {
        self.pool
            .get_page::<BTreeNode>(pool_guard, page_id, mode)
            .await
    }

    /// Destroy the tree.
    /// This method will traverse the tree and deallocate all the nodes recursively.
    #[inline]
    pub async fn destory(self, pool_guard: &PoolGuard) -> Result<()> {
        let g = self
            .get_node(pool_guard, self.root, LatchFallbackMode::Exclusive)
            .await?
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
                self.deallocate_h1(pool_guard, g).await?;
            }
            _ => {
                let mut stack = vec![];
                stack.push(ParentPosition { g, idx: -1 });

                while let Some(pos) = stack.last_mut() {
                    let p_node = pos.g.page();
                    if p_node.height() == 1 {
                        let g = stack.pop().unwrap().g;
                        self.deallocate_h1(pool_guard, g).await?;
                        continue;
                    }
                    if pos.idx == p_node.count() as isize {
                        // all children are dropped, we can drop the parent itself.
                        let ParentPosition { g, .. } = stack.pop().unwrap();
                        self.pool.deallocate_page(g);
                        continue;
                    }
                    let c_page_id = if pos.idx == -1 {
                        p_node.lower_fence_value().into()
                    } else {
                        p_node.value_as_page_id(pos.idx as usize)
                    };
                    let c_guard = self
                        .get_node(pool_guard, c_page_id, LatchFallbackMode::Exclusive)
                        .await?
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
        Ok(())
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
    pub async fn lookup_optimistic<V: BTreeValue>(
        &self,
        pool_guard: &PoolGuard,
        key: &[u8],
    ) -> Result<Option<V>> {
        loop {
            let res = self.try_lookup_optimistic(pool_guard, key).await?;
            let res = verify_continue!(res);
            return Ok(res);
        }
    }

    /// Insert a new key value pair into the tree.
    /// Returns old value if same key exists.
    #[inline]
    pub async fn insert<V: BTreeValue>(
        &self,
        pool_guard: &PoolGuard,
        key: &[u8],
        value: V,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> Result<BTreeInsert<V>> {
        // Stack holds the path from root to leaf.
        loop {
            let res = self
                .try_find_leaf_with_optimistic_parent::<ExclusiveStrategy>(pool_guard, key)
                .await;
            let res = res?;
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
                        return Ok(BTreeInsert::Ok(true));
                    }
                    return Ok(BTreeInsert::DuplicateKey(old_v));
                }
            };
            if !node.can_insert::<V>(key) {
                debug_assert!(node.count() > 1);
                if p_guard.is_none() {
                    // Root is leaf and full, should split.
                    self.split_root::<V>(pool_guard, node, true, ts).await?;
                    continue;
                }
                match self
                    .try_split_bottom_up::<V>(pool_guard, p_guard.unwrap(), c_guard, ts)
                    .await?
                {
                    // If split is done or tree structure has been changed by other thread,
                    // we can retry the insert.
                    BTreeSplit::Ok | BTreeSplit::Inconsistent => (),
                    BTreeSplit::FullBranch(mut split) => loop {
                        match self
                            .try_split_top_down(
                                pool_guard,
                                split.lower_fence_key(),
                                split.page_id,
                                split.sep_key(),
                                ts,
                            )
                            .await?
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
            return Ok(BTreeInsert::Ok(false));
        }
    }

    /// Mark an existing key value pair as deleted.
    #[inline]
    pub async fn mark_as_deleted<V: BTreeValue>(
        &self,
        pool_guard: &PoolGuard,
        key: &[u8],
        value: V,
        ts: TrxID,
    ) -> Result<BTreeUpdate<V>> {
        debug_assert!(!value.is_deleted());
        let mut g = self.find_leaf::<ExclusiveStrategy>(pool_guard, key).await?;
        debug_assert!(g.page().is_leaf());
        let node = g.page_mut();
        let res = node.mark_as_deleted(key, value);
        if res.is_ok() {
            node.update_ts(ts);
        }
        Ok(res)
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
        pool_guard: &PoolGuard,
        key: &[u8],
        value: V,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<BTreeDelete> {
        debug_assert!(!value.is_deleted());
        let mut g = self.find_leaf::<ExclusiveStrategy>(pool_guard, key).await?;
        debug_assert!(g.page().is_leaf());
        let node = g.page_mut();
        let res = node.delete(key, value, ignore_del_mask);
        if res.is_ok() {
            node.update_hints();
            node.update_ts(ts);
        }
        Ok(res)
    }

    /// Delete an existing key/value pair only if the current delete-bit state
    /// still matches the caller's snapshot.
    ///
    /// Cleanup paths use this after scanning entries so a concurrent unmask,
    /// remask, or owner change cannot be removed by a stale scan result.
    #[inline]
    pub(crate) async fn delete_exact<V: BTreeValue>(
        &self,
        pool_guard: &PoolGuard,
        key: &[u8],
        value: V,
        expected_deleted: bool,
        ts: TrxID,
    ) -> Result<BTreeDelete> {
        debug_assert!(!value.is_deleted());
        let mut g = self.find_leaf::<ExclusiveStrategy>(pool_guard, key).await?;
        debug_assert!(g.page().is_leaf());
        let node = g.page_mut();
        let res = node.delete_exact(key, value, expected_deleted);
        if res.is_ok() {
            node.update_hints();
            node.update_ts(ts);
        }
        Ok(res)
    }

    /// Update an existing key value pair with new value.
    #[inline]
    pub async fn update<V: BTreeValue>(
        &self,
        pool_guard: &PoolGuard,
        key: &[u8],
        old_value: V,
        new_value: V,
        ts: TrxID,
    ) -> Result<BTreeUpdate<V>> {
        let mut g = self.find_leaf::<ExclusiveStrategy>(pool_guard, key).await?;
        debug_assert!(g.page().is_leaf());
        let node = g.page_mut();
        let res = node.update(key, old_value, new_value);
        if res.is_ok() {
            node.update_ts(ts);
        }
        Ok(res)
    }

    /// Create a cursor to iterator over nodes at given height.
    /// Height equals to 0 means iterating over all leaf nodes.
    #[inline]
    pub fn cursor<'a>(
        &'a self,
        pool_guard: &'a PoolGuard,
        height: usize,
    ) -> BTreeNodeCursor<'a, P> {
        BTreeNodeCursor::new(self, pool_guard, height)
    }

    /// Create a prefix scanner to scan keys.
    #[inline]
    pub fn prefix_scanner<'a, C: BTreeSlotCallback>(
        &'a self,
        pool_guard: &'a PoolGuard,
        callback: C,
    ) -> BTreePrefixScan<'a, C, P> {
        BTreePrefixScan::new(self, pool_guard, callback)
    }

    /// Collect space statistics at given height.
    #[inline]
    pub async fn collect_space_statistics_at(
        &self,
        pool_guard: &PoolGuard,
        height: usize,
    ) -> Result<SpaceStatistics> {
        let mut cursor = self.cursor(pool_guard, height);
        cursor.seek(&[]).await?;
        let mut preview = SpaceStatistics::default();
        while let Some(g) = cursor.next().await? {
            let node = g.page();
            preview.nodes += 1;
            preview.total_space += BTREE_NODE_USABLE_SIZE;
            preview.used_space += BTREE_NODE_USABLE_SIZE - node.free_space();
            preview.effective_space += node.effective_space();
        }
        Ok(preview)
    }

    /// Collect space statistics of the whole tree.
    #[inline]
    pub async fn collect_space_statistics(
        &self,
        pool_guard: &PoolGuard,
    ) -> Result<SpaceStatistics> {
        let height = self.height();
        let mut res = SpaceStatistics::default();
        for h in 0..height + 1 {
            let s = self.collect_space_statistics_at(pool_guard, h).await?;
            res.nodes += s.nodes;
            res.total_space += s.total_space;
            res.used_space += s.used_space;
            res.effective_space += s.effective_space;
        }
        Ok(res)
    }

    /// Create a compactor for all nodes at given height.
    #[inline]
    pub fn compact<'a, V: BTreeValue>(
        &'a self,
        pool_guard: &'a PoolGuard,
        height: usize,
        config: BTreeCompactConfig,
    ) -> BTreeCompactor<'a, V, P> {
        BTreeCompactor::new(self, pool_guard, height, config)
    }

    /// Compact the whole tree.
    #[inline]
    pub async fn compact_all<V: BTreeValue>(
        &self,
        pool_guard: &PoolGuard,
        config: BTreeCompactConfig,
    ) -> Result<Vec<PageExclusiveGuard<BTreeNode>>> {
        let height = self.height();
        let mut purge_list = vec![];
        // leaf compaction.
        self.compact::<V>(pool_guard, 0, config)
            .run_to_end(&mut purge_list)
            .await?;
        // branch-to-root compaction.
        let mut h = 1usize;
        while h <= height {
            self.compact::<BTreeU64>(pool_guard, h, config)
                .run_to_end(&mut purge_list)
                .await?;
            h += 1;
        }
        self.shrink(pool_guard, &mut purge_list).await?;
        Ok(purge_list)
    }

    /// Try to shrink the tree height if root node has only one child.
    #[inline]
    pub async fn shrink(
        &self,
        pool_guard: &PoolGuard,
        purge_list: &mut Vec<PageExclusiveGuard<BTreeNode>>,
    ) -> Result<()> {
        // test if root has only one child with optimistic lock,
        // to avoid block concurrent operations.
        loop {
            let g = self
                .pool
                .get_page::<BTreeNode>(pool_guard, self.root, LatchFallbackMode::Spin)
                .await?;
            let (height, count) =
                verify_continue!(g.with_page_ref_validated(|page| (page.height(), page.count())));
            if height == 0 || count > 0 {
                return Ok(());
            }
            // should shrink tree height as there is no keys in root.
            // The only child is associated with lower fence key.
            let mut g = g.lock_exclusive_async().await.unwrap();
            // re-check condition
            let root = g.page_mut();
            if root.height() == 0 || root.count() > 0 {
                return Ok(());
            }
            let c_page_id = root.lower_fence_value().into();
            let mut c_guard = self
                .pool
                .get_page::<BTreeNode>(pool_guard, c_page_id, LatchFallbackMode::Exclusive)
                .await?
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

    /// Try to lookup a key in the tree, break if any optimistic validation fails.
    ///
    /// The leaf is read under optimistic mode. A concurrent writer can change
    /// node content between `search_key` and value read in the same closure.
    /// We convert such stale observations to `Validation::Invalid`, then rely on
    /// final guard validation in `with_page_ref_validated` to retry safely.
    #[inline]
    async fn try_lookup_optimistic<V: BTreeValue>(
        &self,
        pool_guard: &PoolGuard,
        key: &[u8],
    ) -> Result<Validation<Option<V>>> {
        let g = self
            .find_leaf::<OptimisticStrategy>(pool_guard, key)
            .await?;
        let value = verify_result!(
            g.with_page_ref_validated(|leaf| match leaf.search_key(key) {
                Ok(idx) => {
                    // If `idx` becomes stale, treat it as validation failure and retry.
                    match leaf.value_checked::<V>(idx) {
                        Some(v) => Valid(Some(v)),
                        None => Invalid,
                    }
                }
                Err(_) => Valid(None),
            })
            .and_then(|inner| inner)
        );
        Ok(Valid(value))
    }

    /// Try to split node bottom up.
    #[inline]
    async fn try_split_bottom_up<V: BTreeValue>(
        &self,
        pool_guard: &PoolGuard,
        mut p_guard: FacadePageGuard<BTreeNode>,
        mut c_guard: PageExclusiveGuard<BTreeNode>,
        ts: TrxID,
    ) -> Result<BTreeSplit> {
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
                if !p_guard.page().can_insert::<BTreeU64>(&sep_key) {
                    let page_id = p_guard.page_id();
                    if page_id != self.root {
                        let lower_fence_key = p_guard.page().lower_fence_key();
                        return Ok(BTreeSplit::full_branch(&lower_fence_key, page_id, &sep_key));
                    }
                    // Parent is root and full, just split.
                    self.split_root::<BTreeU64>(pool_guard, p_guard.page_mut(), false, ts)
                        .await?;
                    return Ok(BTreeSplit::Ok);
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
                        pool_guard, p_guard, c_guard, sep_idx, &sep_key, ts,
                    )
                    .await?;
                match res {
                    Either::Left((p, c)) => {
                        c_guard = c;
                        p
                    }
                    Either::Right(s) => return Ok(s),
                }
            }
        };
        // Now parent and child locks are acquired exclusively, and parent has enough
        // space to insert separator key, so to actual split.
        let r_guard = self.allocate_node(pool_guard).await?;
        self.split_node::<V>(
            p_guard.page_mut(),
            c_guard.page_mut(),
            r_guard,
            sep_idx,
            &sep_key,
            ts,
        )
        .await;
        Ok(BTreeSplit::Ok)
    }

    /// Try to acquire locks on parent node and child node in order.
    /// Meanwhile, check if any concurrent change is applied to effected nodes.
    #[inline]
    async fn try_acquire_parent_and_child_locks_for_split(
        &self,
        pool_guard: &PoolGuard,
        p_guard: FacadePageGuard<BTreeNode>,
        mut c_guard: PageExclusiveGuard<BTreeNode>,
        sep_idx: usize,
        sep_key: &[u8],
        ts: TrxID,
    ) -> Result<Either<(PageExclusiveGuard<BTreeNode>, PageExclusiveGuard<BTreeNode>), BTreeSplit>>
    {
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
                    if !p_node.can_insert::<BTreeU64>(sep_key) {
                        let p_page_id = p_guard.page_id();
                        if p_page_id != self.root {
                            // Parent is full, trigger top-down split of parent node.
                            let p_lower_fence_key = p_node.lower_fence_key();
                            return Ok(Either::Right(BTreeSplit::full_branch(
                                &p_lower_fence_key,
                                p_page_id,
                                sep_key,
                            )));
                        }
                        // Parent is root and full, just split.
                        self.split_root::<BTreeU64>(pool_guard, p_guard.page_mut(), false, ts)
                            .await?;
                        return Ok(Either::Right(BTreeSplit::Ok));
                    }
                    // Re-lock child in exclusive mode.
                    c_guard = c_optimistic_guard.exclusive_async().await;
                    // Check if separator key changes
                    let c_node = c_guard.page();
                    let new_sep_idx = c_node.find_separator();
                    if new_sep_idx != sep_idx {
                        return Ok(Either::Right(BTreeSplit::Inconsistent));
                    }
                    let new_sep_key = c_node.create_sep_key(new_sep_idx, true);
                    if &new_sep_key[..] != sep_key {
                        return Ok(Either::Right(BTreeSplit::Inconsistent));
                    }
                    return Ok(Either::Left((p_guard, c_guard)));
                }
                Ok(Either::Right(BTreeSplit::Inconsistent))
            }
            Err(_) => Ok(Either::Right(BTreeSplit::Inconsistent)),
        }
    }

    /// Try to split node top down.
    #[inline]
    async fn try_split_top_down(
        &self,
        pool_guard: &PoolGuard,
        lower_fence_key: &[u8],
        page_id: PageID,
        sep_key: &[u8],
        ts: TrxID,
    ) -> Result<BTreeSplit> {
        debug_assert!(page_id != self.root);
        match self
            .find_branch_for_split(pool_guard, lower_fence_key, page_id)
            .await?
        {
            None => Ok(BTreeSplit::Inconsistent),
            Some(mut p_guard) => {
                let p_page_id = p_guard.page_id();
                let p_node = p_guard.page_mut();
                match p_node.lookup_child(lower_fence_key) {
                    LookupChild::NotFound => Ok(BTreeSplit::Inconsistent),
                    LookupChild::Slot(_, c_page_id) | LookupChild::LowerFence(c_page_id) => {
                        if page_id != c_page_id {
                            return Ok(BTreeSplit::Inconsistent);
                        }
                        let mut c_guard = self
                            .pool
                            .get_page::<BTreeNode>(
                                pool_guard,
                                c_page_id,
                                LatchFallbackMode::Exclusive,
                            )
                            .await?
                            .lock_exclusive_async()
                            .await
                            .unwrap();
                        let c_node = c_guard.page_mut();
                        if c_node.can_insert::<BTreeU64>(sep_key) {
                            // The node to split can insert original separator key,
                            // maybe other thread restructure this tree.
                            return Ok(BTreeSplit::Inconsistent);
                        }
                        // Now we calculate current separator key for child node.
                        debug_assert!(!c_node.is_leaf());
                        let sep_idx = c_node.find_separator();
                        // Separator key can not be truncated because it's branch node.
                        let sep_key = c_node.create_sep_key(sep_idx, false);
                        if !p_node.can_insert::<BTreeU64>(&sep_key) {
                            // parent node is full
                            if p_page_id != self.root {
                                // not root, split it in a single run.
                                let lower_fence_key = p_node.lower_fence_key();
                                return Ok(BTreeSplit::full_branch(
                                    &lower_fence_key,
                                    p_page_id,
                                    &sep_key,
                                ));
                            }
                            // split root.
                            self.split_root::<BTreeU64>(pool_guard, p_node, false, ts)
                                .await?;
                            if !p_node.can_insert::<BTreeU64>(&sep_key) {
                                return Ok(BTreeSplit::Inconsistent);
                            }
                        }
                        // now parent and child nodes are exclusively locked and parent has enough
                        // space to insert separator key, so do actual split.
                        let r_guard = self.allocate_node(pool_guard).await?;
                        self.split_node::<BTreeU64>(p_node, c_node, r_guard, sep_idx, &sep_key, ts)
                            .await;
                        Ok(BTreeSplit::Ok)
                    }
                }
            }
        }
    }

    /// Split root node.
    #[inline]
    async fn split_root<V: BTreeValue>(
        &self,
        pool_guard: &PoolGuard,
        root: &mut BTreeNode,
        is_leaf: bool,
        ts: TrxID,
    ) -> Result<()> {
        debug_assert!(root.is_leaf() == is_leaf);
        let ts = root.ts().max(ts);
        let height = root.height() as u16;
        let hints_enabled = root.header_hints_enabled();
        let sep_idx = root.find_separator();
        let lower_fence_key = root.lower_fence_key();
        let lower_fence_value = root.lower_fence_value();
        // Only truncate separator key if it's leaf.
        let sep_key = root.create_sep_key(sep_idx, is_leaf);
        // Allocate both child nodes before mutating any page state.
        let mut left_page = self.allocate_node(pool_guard).await?;
        let mut right_page = match self.allocate_node(pool_guard).await {
            Ok(page) => page,
            Err(err) => {
                self.pool.deallocate_page(left_page);
                return Err(err);
            }
        };
        let left_page_id = left_page.page_id();
        let right_page_id = right_page.page_id();
        let left_node = left_page.page_mut();
        let right_node = right_page.page_mut();
        pack_node_range_into::<V>(
            left_node,
            root,
            KnownFenceNodeParams {
                height,
                ts,
                lower_fence: &lower_fence_key,
                lower_fence_value,
                upper_fence: Some(&sep_key),
                hints_enabled,
            },
            0..sep_idx,
        );
        pack_node_range_into::<V>(
            right_node,
            root,
            KnownFenceNodeParams {
                height,
                ts,
                lower_fence: &sep_key,
                lower_fence_value: BTreeU64::INVALID_VALUE,
                upper_fence: None,
                hints_enabled,
            },
            sep_idx..root.count(),
        );
        // Re-initialize root in place as branch root and insert right child separator.
        root.init(
            height + 1,
            ts,
            &lower_fence_key,
            BTreeU64::from(left_page_id),
            &[],
            hints_enabled,
        );
        let slot_idx = root.insert(&sep_key, BTreeU64::from(right_page_id));
        debug_assert!(slot_idx == 0);
        self.height.store(root.height(), Ordering::SeqCst);
        Ok(())
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
        let tmp_l = pack_node_range_box::<V>(
            c_node,
            KnownFenceNodeParams {
                height: c_height,
                ts,
                lower_fence: &c_lower_fence_key,
                lower_fence_value: c_node.lower_fence_value(),
                upper_fence: Some(sep_key),
                hints_enabled: c_node.header_hints_enabled(),
            },
            0..sep_idx,
        );
        // Process right node.
        let right_page_id = r_guard.page_id();
        let right_node = r_guard.page_mut();
        pack_node_range_into::<V>(
            right_node,
            c_node,
            KnownFenceNodeParams {
                height: c_height,
                ts,
                lower_fence: sep_key,
                // For leaf node, lower fence value is meaningless.
                // For branch node, only the leftmost branch node has meaningful
                // lower fence value pointing to a valid child. Other branch nodes
                // always have lower fence equal to the first slot key, so the
                // right node does not need to copy lower fence value.
                lower_fence_value: BTreeU64::INVALID_VALUE,
                upper_fence: Some(&c_upper_fence_key),
                hints_enabled: c_node.header_hints_enabled(),
            },
            sep_idx..c_node.count(),
        );
        // Copy left node to current node.
        c_node.clone_from(&tmp_l);
        drop(tmp_l);
        // Insert right node into parent.
        p_node.insert(sep_key, BTreeU64::from(right_page_id));
    }

    /// Find leaf by given key.
    #[inline]
    async fn find_leaf<S: LockStrategy<Page = BTreeNode>>(
        &self,
        pool_guard: &PoolGuard,
        key: &[u8],
    ) -> Result<S::Guard> {
        loop {
            let res = self.try_find_leaf::<S>(pool_guard, key).await?;
            let res = verify_continue!(res);
            return Ok(res);
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
        debug_assert!(l_node.height() == r_node.height());
        debug_assert!(p_r_idx < p_node.count());
        debug_assert!(p_node.lookup_child_idx(lower_fence_key) == Some(p_r_idx as isize - 1));
        debug_assert!(matches!(
            plan_memtree_sibling_merge::<V>(
                l_node,
                r_node,
                lower_fence_key,
                upper_fence_key,
                BTREE_NODE_USABLE_SIZE,
            ),
            MemTreeSiblingMergePlan::Full
        ));
        let ts = ts.max(l_node.ts()).max(r_node.ts()).max(p_node.ts());
        // Full merge preserves the left page identity and purges the right page.
        // The helper only rebuilds node bytes; the caller still owns parent
        // separator deletion and purge-list ordering.
        let tmp_l = pack_node_ranges_box::<V>(
            KnownFenceNodeParams {
                height: l_node.height() as u16,
                ts,
                lower_fence: lower_fence_key,
                lower_fence_value: l_node.lower_fence_value(),
                upper_fence: Some(upper_fence_key),
                hints_enabled: l_node.header_hints_enabled(),
            },
            &[
                NodeSlotRange {
                    node: l_node,
                    range: 0..l_node.count(),
                },
                NodeSlotRange {
                    node: r_node,
                    range: 0..r_node.count(),
                },
            ],
        );
        l_node.clone_from(&tmp_l);
        drop(tmp_l);
        // Parent separators are branch entries and always store child page ids,
        // independent of the leaf value type used by this merge.
        p_node.delete_at(p_r_idx, BTreeU64::ENCODED_LEN);
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
        debug_assert!(l_node.height() == r_node.height());
        debug_assert!(p_r_idx < p_node.count());
        debug_assert!(p_node.lookup_child_idx(lower_fence_key) == Some(p_r_idx as isize - 1));
        debug_assert!(count > 0 && count < r_node.count());
        debug_assert!(&r_node.create_sep_key(count, r_node.height() == 0)[..] == sep_key);
        let ts = ts.max(l_node.ts()).max(r_node.ts()).max(p_node.ts());
        // Partial merge keeps both child pages. The left page absorbs a prefix
        // of the right page, while the remaining right suffix keeps the original
        // upper fence and gets a new lower fence equal to the parent separator.
        let tmp_l = pack_node_ranges_box::<V>(
            KnownFenceNodeParams {
                height: l_node.height() as u16,
                ts,
                lower_fence: lower_fence_key,
                lower_fence_value: l_node.lower_fence_value(),
                upper_fence: Some(sep_key),
                hints_enabled: l_node.header_hints_enabled(),
            },
            &[
                NodeSlotRange {
                    node: l_node,
                    range: 0..l_node.count(),
                },
                NodeSlotRange {
                    node: r_node,
                    range: 0..count,
                },
            ],
        );
        l_node.clone_from(&tmp_l);
        drop(tmp_l);

        let tmp_r = pack_node_range_box::<V>(
            r_node,
            KnownFenceNodeParams {
                height: r_node.height() as u16,
                ts,
                lower_fence: sep_key,
                lower_fence_value: BTreeU64::INVALID_VALUE,
                upper_fence: Some(upper_fence_key),
                hints_enabled: r_node.header_hints_enabled(),
            },
            count..r_node.count(),
        );
        r_node.clone_from(&tmp_r);
        drop(tmp_r);
        p_node.update_key::<BTreeU64>(p_r_idx, sep_key);
        p_node.update_hint_if_needed(p_r_idx, sep_key);
        p_node.update_ts(ts);
    }

    #[inline]
    async fn try_find_leaf<S: LockStrategy<Page = BTreeNode>>(
        &self,
        pool_guard: &PoolGuard,
        key: &[u8],
    ) -> Result<Validation<S::Guard>> {
        let mut p_guard = self
            .pool
            .get_page::<BTreeNode>(pool_guard, self.root, LatchFallbackMode::Spin)
            .await?;
        // check root page separately.
        let height = verify_result!(p_guard.with_page_ref_validated(|page| page.height()));
        if height == 0 {
            // root is leaf.
            verify_result!(S::try_lock(&mut p_guard));
            return Ok(Valid(S::must_locked(p_guard)));
        }
        loop {
            // Current node is not leaf node.
            let (height, child) = verify_result!(
                p_guard.with_page_ref_validated(|page| (page.height(), page.lookup_child(key)))
            );
            match child {
                LookupChild::Slot(_, page_id) | LookupChild::LowerFence(page_id) => {
                    // As version validation passes, the height must not be 0.
                    debug_assert!(height != 0);
                    if height == 1 {
                        // child node is leaf.
                        let c_guard = self
                            .pool
                            .get_child_page(pool_guard, &p_guard, page_id, S::MODE)
                            .await?;
                        let c_guard = verify_result!(c_guard);
                        let c_guard = S::verify_lock_async::<false>(c_guard).await;
                        let c_guard = verify_result!(c_guard);
                        return Ok(Valid(c_guard));
                    }
                    // child node is branch, continue next iteration.
                    let c_guard = self
                        .pool
                        .get_child_page(pool_guard, &p_guard, page_id, LatchFallbackMode::Spin)
                        .await?;
                    let c_guard = verify_result!(c_guard);
                    p_guard = c_guard;
                }
                LookupChild::NotFound => {
                    unreachable!("BTree should always find one leaf for any key");
                }
            }
        }
    }

    #[inline]
    async fn try_find_leaf_with_optimistic_parent<S: LockStrategy<Page = BTreeNode>>(
        &self,
        pool_guard: &PoolGuard,
        key: &[u8],
    ) -> Result<Validation<(S::Guard, Option<FacadePageGuard<BTreeNode>>)>> {
        let mut p_guard = self
            .pool
            .get_page::<BTreeNode>(pool_guard, self.root, LatchFallbackMode::Spin)
            .await?;
        // check root page separately.
        let height = verify_result!(p_guard.with_page_ref_validated(|page| page.height()));
        if height == 0 {
            // root is leaf.
            verify_result!(S::try_lock(&mut p_guard));
            return Ok(Valid((S::must_locked(p_guard), None)));
        }
        loop {
            // Current node is not leaf node.
            let (height, child) = verify_result!(
                p_guard.with_page_ref_validated(|page| (page.height(), page.lookup_child(key)))
            );
            match child {
                LookupChild::Slot(_, page_id) | LookupChild::LowerFence(page_id) => {
                    // As version validation passes, the height must not be 0.
                    debug_assert!(height != 0);
                    if height == 1 {
                        // child node is leaf.
                        let c_guard = self
                            .pool
                            .get_child_page(pool_guard, &p_guard, page_id, S::MODE)
                            .await?;
                        let c_guard = verify_result!(c_guard);
                        let c_guard = S::verify_lock_async::<false>(c_guard).await;
                        let c_guard = verify_result!(c_guard);
                        return Ok(Valid((c_guard, Some(p_guard))));
                    }
                    // child node is branch, continue next iteration.
                    let c_guard = self
                        .pool
                        .get_child_page(pool_guard, &p_guard, page_id, LatchFallbackMode::Spin)
                        .await?;
                    let c_guard = verify_result!(c_guard);
                    p_guard = c_guard;
                }
                LookupChild::NotFound => {
                    unreachable!("BTree should always find one leaf for any key");
                }
            }
        }
    }

    #[inline]
    async fn find_branch_for_split(
        &self,
        pool_guard: &PoolGuard,
        lower_fence_key: &[u8],
        page_id: PageID,
    ) -> Result<Option<PageExclusiveGuard<BTreeNode>>> {
        loop {
            let res = self
                .try_find_branch_for_split(pool_guard, lower_fence_key, page_id)
                .await;
            let res = res?;
            let res = verify_continue!(res);
            return Ok(res);
        }
    }

    #[inline]
    async fn try_find_branch_for_split(
        &self,
        pool_guard: &PoolGuard,
        lower_fence_key: &[u8],
        page_id: PageID,
    ) -> Result<Validation<Option<PageExclusiveGuard<BTreeNode>>>> {
        let mut g = self
            .pool
            .get_page::<BTreeNode>(pool_guard, self.root, LatchFallbackMode::Spin)
            .await?;
        let mut height = verify_result!(g.with_page_ref_validated(|page| page.height()));
        if height == 0 {
            // single-node tree
            return Ok(Valid(None));
        }
        loop {
            let (new_height, child) = verify_result!(g.with_page_ref_validated(|page| (
                page.height(),
                page.lookup_child(lower_fence_key)
            )));
            height = new_height;
            match child {
                LookupChild::Slot(_, c_page_id) | LookupChild::LowerFence(c_page_id) => {
                    if c_page_id == page_id {
                        let g = g.lock_exclusive_async().await.unwrap();
                        return Ok(Valid(Some(g)));
                    }
                    if height <= 1 {
                        // next level is leaf.
                        return Ok(Valid(None));
                    }
                    // Otherwise, go to next level.
                    let c_guard = self
                        .pool
                        .get_child_page(pool_guard, &g, c_page_id, LatchFallbackMode::Spin)
                        .await?;
                    g = verify_result!(c_guard);
                }
                LookupChild::NotFound => {
                    return Ok(Valid(None));
                }
            }
        }
    }

    #[inline]
    async fn deallocate_h1(
        &self,
        pool_guard: &PoolGuard,
        g: PageExclusiveGuard<BTreeNode>,
    ) -> Result<()> {
        let p_node = g.page();
        debug_assert!(p_node.height() == 1);
        // Deallocate child associated with lower fence key.
        // Only left-most node has lower fence child.
        if !p_node.lower_fence_value().is_deleted() {
            let c_page_id = p_node.lower_fence_value().into();
            let c_guard = self
                .pool
                .get_page::<BTreeNode>(pool_guard, c_page_id, LatchFallbackMode::Exclusive)
                .await?
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
                .get_page::<BTreeNode>(pool_guard, c_page_id, LatchFallbackMode::Exclusive)
                .await?
                .lock_exclusive_async()
                .await
                .unwrap();
            self.pool.deallocate_page::<BTreeNode>(c_guard);
        }
        // Deallocate self.
        self.pool.deallocate_page::<BTreeNode>(g);
        Ok(())
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
    pub async fn seek_and_lock<P: BufferPool>(
        &mut self,
        tree: &GenericBTree<P>,
        pool_guard: &PoolGuard,
        height: usize,
        key: &[u8],
    ) -> Result<()> {
        loop {
            self.reset();
            let g = tree
                .pool
                .get_page::<BTreeNode>(pool_guard, tree.root, LatchFallbackMode::Spin)
                .await?;
            let res = self
                .try_seek_and_lock(tree, pool_guard, height, key, g)
                .await;
            let res = res?;
            verify_continue!(res);
            return Ok(());
        }
    }

    /// Release both locks
    #[inline]
    pub fn reset(&mut self) {
        self.parent.take();
        self.node.take();
    }

    #[inline]
    async fn try_seek_and_lock<P: BufferPool>(
        &mut self,
        tree: &GenericBTree<P>,
        pool_guard: &PoolGuard,
        height: usize,
        key: &[u8],
        mut p_guard: FacadePageGuard<BTreeNode>,
    ) -> Result<Validation<()>> {
        loop {
            let curr_height = verify_result!(p_guard.with_page_ref_validated(|page| page.height()));
            if curr_height < height {
                // tree height is smaller than searched height.
                return Ok(Valid(()));
            }
            if curr_height == height {
                verify_result!(S::try_lock(&mut p_guard));
                self.node = Some(S::must_locked(p_guard));
                return Ok(Valid(()));
            }
            if curr_height == height + 1 {
                // Parent is locked with same mode as child.
                verify_result!(S::try_lock(&mut p_guard));
                let p_guard = S::must_locked(p_guard);
                let p_node = p_guard.page();
                debug_assert!(!p_node.is_leaf());
                let (idx, c_page_id) = match p_node.lookup_child(key) {
                    LookupChild::Slot(idx, c_page_id) => (idx as isize, c_page_id),
                    LookupChild::LowerFence(c_page_id) => (-1, c_page_id),
                    LookupChild::NotFound => unreachable!(),
                };
                let c_guard = tree
                    .pool
                    .get_page::<BTreeNode>(pool_guard, c_page_id, S::MODE)
                    .await?;
                let res = S::verify_lock_async::<false>(c_guard).await;
                let c_guard = verify_result!(res);
                self.parent = Some(ParentPosition { g: p_guard, idx });
                self.node = Some(c_guard);
                return Ok(Valid(()));
            }
            let c_page_id =
                verify_result!(p_guard.with_page_ref_validated(
                    |page| match page.lookup_child(key) {
                        LookupChild::Slot(_, c_page_id) => c_page_id,
                        LookupChild::LowerFence(c_page_id) => c_page_id,
                        LookupChild::NotFound => unreachable!(),
                    }
                ));
            // Before access parent node, we always use optimistic spin lock.
            let c_guard = tree
                .pool
                .get_child_page::<BTreeNode>(
                    pool_guard,
                    &p_guard,
                    c_page_id,
                    LatchFallbackMode::Spin,
                )
                .await?;
            p_guard = verify_result!(c_guard);
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

#[inline]
fn build_exhausted_parent_seek_key(node: &BTreeNode, key_buffer: &mut Vec<u8>) {
    key_buffer.clear();
    if node.has_no_upper_fence() {
        return;
    }
    node.extend_upper_fence_key(key_buffer);
}

#[inline]
fn make_strict_successor(key_buffer: &mut Vec<u8>) {
    key_buffer.push(0);
}

/// Shared cursor of nodes at given height.
/// At most two locks are held at the same time.
///
/// This cursor does not guarantee consistent snapshot during iterating
/// the tree nodes.
/// But it guarantees if a value(node) does not change during the traverse,
/// it will be always visited.
pub struct BTreeNodeCursor<'a, P: 'static> {
    tree: &'a GenericBTree<P>,
    pool_guard: &'a PoolGuard,
    height: usize,
    coupling: BTreeCoupling<SharedStrategy>,
    resume_key_buffer: Vec<u8>,
}

impl<'a, P: BufferPool> BTreeNodeCursor<'a, P> {
    #[inline]
    fn resumed_same_parent(&self, exhausted_parent_page_id: PageID) -> bool {
        self.coupling
            .parent
            .as_ref()
            .map(|parent| parent.g.page_id())
            == Some(exhausted_parent_page_id)
    }

    /// Create a new cursor.
    #[inline]
    pub fn new(tree: &'a GenericBTree<P>, pool_guard: &'a PoolGuard, height: usize) -> Self {
        BTreeNodeCursor {
            tree,
            pool_guard,
            height,
            coupling: BTreeCoupling::<SharedStrategy>::new(),
            resume_key_buffer: Vec::new(),
        }
    }

    #[inline]
    pub async fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.coupling
            .seek_and_lock(self.tree, self.pool_guard, self.height, key)
            .await
    }

    /// Fetch next node.
    #[inline]
    pub async fn next(&mut self) -> Result<Option<PageSharedGuard<BTreeNode>>> {
        if let Some(g) = self.coupling.node.take() {
            return Ok(Some(g));
        }
        if let Some(parent) = self.coupling.parent.as_ref() {
            let exhausted_parent_page_id = {
                let p_node = parent.g.page();
                let next_idx = (parent.idx + 1) as usize;
                if next_idx == p_node.count() {
                    // current parent exhausted.
                    if p_node.has_no_upper_fence() {
                        // The tree exhausted.
                        self.coupling.reset();
                        return Ok(None);
                    }
                    build_exhausted_parent_seek_key(p_node, &mut self.resume_key_buffer);
                    Some(parent.g.page_id())
                } else {
                    let c_page_id = p_node.value_as_page_id(next_idx);
                    self.coupling.parent.as_mut().unwrap().idx = next_idx as isize;
                    let c_guard = self
                        .tree
                        .pool
                        .get_page::<BTreeNode>(
                            self.pool_guard,
                            c_page_id,
                            LatchFallbackMode::Shared,
                        )
                        .await?
                        .lock_shared_async()
                        .await
                        .unwrap();
                    return Ok(Some(c_guard));
                }
            };
            self.coupling
                .seek_and_lock(
                    self.tree,
                    self.pool_guard,
                    self.height,
                    &self.resume_key_buffer,
                )
                .await?;
            if let Some(exhausted_parent_page_id) = exhausted_parent_page_id
                && self.resumed_same_parent(exhausted_parent_page_id)
            {
                make_strict_successor(&mut self.resume_key_buffer);
                self.coupling
                    .seek_and_lock(
                        self.tree,
                        self.pool_guard,
                        self.height,
                        &self.resume_key_buffer,
                    )
                    .await?;
            }
            return Ok(self.coupling.node.take());
        }
        Ok(None)
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
            return Err(Report::new(ConfigError::InvalidBTreeCompactRatio)
                .attach(format!("low_ratio={low_ratio}, high_ratio={high_ratio}"))
                .into());
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
pub struct BTreeCompactor<'a, V: BTreeValue, P: 'static> {
    tree: &'a GenericBTree<P>,
    pool_guard: &'a PoolGuard,
    // height of nodes to be compacted.
    // If height is greater than 0, V should always be PageID.
    height: usize,
    low_space: usize,
    high_space: usize,
    coupling: BTreeCoupling<ExclusiveStrategy>,
    _marker: PhantomData<V>,
}

impl<'a, V: BTreeValue, P: BufferPool> BTreeCompactor<'a, V, P> {
    #[inline]
    fn resumed_same_parent(&self, exhausted_parent_page_id: PageID) -> bool {
        self.coupling
            .parent
            .as_ref()
            .map(|parent| parent.g.page_id())
            == Some(exhausted_parent_page_id)
    }

    #[inline]
    pub fn new(
        tree: &'a GenericBTree<P>,
        pool_guard: &'a PoolGuard,
        height: usize,
        config: BTreeCompactConfig,
    ) -> Self {
        let low_space = ((BTREE_NODE_USABLE_SIZE as f64 * config.low_ratio) as usize)
            .min(BTREE_NODE_USABLE_SIZE);
        let high_space = ((BTREE_NODE_USABLE_SIZE as f64 * config.high_ratio) as usize)
            .min(BTREE_NODE_USABLE_SIZE);
        BTreeCompactor {
            tree,
            pool_guard,
            height,
            low_space,
            high_space,
            coupling: BTreeCoupling::<ExclusiveStrategy>::new(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub async fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.coupling
            .seek_and_lock(self.tree, self.pool_guard, self.height, key)
            .await
    }

    #[inline]
    pub async fn run_to_end(
        mut self,
        purge_list: &mut Vec<PageExclusiveGuard<BTreeNode>>,
    ) -> Result<()> {
        let mut lower_fence_key_buffer = Vec::new();
        let mut upper_fence_key_buffer = Vec::new();
        self.seek(&[]).await?;
        loop {
            let res = self
                .step(
                    &mut lower_fence_key_buffer,
                    &mut upper_fence_key_buffer,
                    purge_list,
                )
                .await?;
            match res {
                BTreeCompact::Skip | BTreeCompact::OutOfSpace | BTreeCompact::ChildDone => (),
                BTreeCompact::ParentDone(exhausted_parent_page_id) => {
                    if upper_fence_key_buffer.is_empty() {
                        return Ok(());
                    }
                    self.seek(&upper_fence_key_buffer).await?;
                    if self.resumed_same_parent(exhausted_parent_page_id) {
                        make_strict_successor(&mut upper_fence_key_buffer);
                        self.seek(&upper_fence_key_buffer).await?;
                    }
                }
                BTreeCompact::AllDone => {
                    // release all locks.
                    self.coupling.reset();
                    return Ok(());
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
    ) -> Result<BTreeCompact> {
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
            return Ok(BTreeCompact::AllDone);
        }

        if !self.lock_current().await? {
            return Ok(BTreeCompact::AllDone);
        }
        let l_node = self.coupling.node.as_mut().unwrap().page_mut();

        if l_node.effective_space() >= self.low_space {
            // Left node's effective space is larger than low space, skip it.
            if self.skip().await? {
                return Ok(BTreeCompact::Skip);
            } else {
                return Ok(BTreeCompact::AllDone);
            }
        }
        // Left node's effective space is smaller than low space
        loop {
            match self.lock_right().await? {
                Some((p_r_idx, mut r_guard)) => {
                    // There are right nodes, so try compact and merge right nodes
                    // until high space is reached.
                    let l_node = self.coupling.node.as_mut().unwrap().page_mut();
                    let r_node = r_guard.page_mut();
                    lower_fence_key_buffer.clear();
                    l_node.extend_lower_fence_key(lower_fence_key_buffer);
                    upper_fence_key_buffer.clear();
                    r_node.extend_upper_fence_key(upper_fence_key_buffer);
                    let ts = l_node.ts().max(r_node.ts());
                    // MemTree compaction is anchored at the underfilled left
                    // page and may use the immediate right page as a donor even
                    // when that right page is not itself underfilled. The
                    // configurable high-space threshold controls how much of
                    // the donor can move into the anchor.
                    match plan_memtree_sibling_merge::<V>(
                        l_node,
                        r_node,
                        lower_fence_key_buffer,
                        upper_fence_key_buffer,
                        self.high_space,
                    ) {
                        MemTreeSiblingMergePlan::FenceOutOfSpace => {
                            // fence key change results in a node out of space.
                            self.coupling.node.replace(r_guard);
                            self.coupling.parent.as_mut().unwrap().idx = p_r_idx as isize;
                            return Ok(BTreeCompact::OutOfSpace);
                        }
                        MemTreeSiblingMergePlan::NoProgress => {
                            // can not add one key to left node.
                            let res = self.skip().await?;
                            debug_assert!(res);
                            return Ok(BTreeCompact::Skip);
                        }
                        MemTreeSiblingMergePlan::Full => {
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
                        MemTreeSiblingMergePlan::Partial {
                            right_count: sep_idx,
                        } => {
                            let sep_key = r_node.create_sep_key(sep_idx, r_node.height() == 0);
                            let parent = self.coupling.parent.as_mut().unwrap();
                            let p_node = parent.g.page_mut();
                            // check if parent has enough space to update key.
                            if !p_node.prepare_update_key::<BTreeU64>(p_r_idx, &sep_key) {
                                self.coupling.node.replace(r_guard);
                                self.coupling.parent.as_mut().unwrap().idx = p_r_idx as isize;
                                return Ok(BTreeCompact::OutOfSpace);
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
                            return Ok(BTreeCompact::ChildDone);
                        }
                    }
                }
                None => {
                    // Current parent done.
                    let exhausted_parent_page_id =
                        self.coupling.parent.as_ref().unwrap().g.page_id();
                    let p_node = self.coupling.parent.as_mut().unwrap().g.page_mut();
                    build_exhausted_parent_seek_key(p_node, upper_fence_key_buffer);
                    self.coupling.reset();
                    return Ok(BTreeCompact::ParentDone(exhausted_parent_page_id));
                }
            }
        }
    }

    // Skip current node and take next node.
    // Return false if next node not found.
    #[inline]
    async fn skip(&mut self) -> Result<bool> {
        drop(self.coupling.node.take());
        if let Some(parent) = self.coupling.parent.as_mut() {
            let p_node = parent.g.page();
            let next_idx = (parent.idx + 1) as usize;
            if next_idx == p_node.count() {
                if p_node.has_no_upper_fence() {
                    self.coupling.parent.take();
                    return Ok(false);
                }
                let exhausted_parent_page_id = parent.g.page_id();
                let mut upper_fence_key_buffer = Vec::new();
                build_exhausted_parent_seek_key(p_node, &mut upper_fence_key_buffer);
                self.seek(&upper_fence_key_buffer).await?;
                if self.resumed_same_parent(exhausted_parent_page_id) {
                    make_strict_successor(&mut upper_fence_key_buffer);
                    self.seek(&upper_fence_key_buffer).await?;
                }
                return Ok(self.coupling.node.is_some());
            }
            let c_page_id = p_node.value_as_page_id(next_idx);
            let c_guard = self
                .tree
                .pool
                .get_page(self.pool_guard, c_page_id, LatchFallbackMode::Exclusive)
                .await?
                .lock_exclusive_async()
                .await
                .unwrap();

            self.coupling.parent.as_mut().unwrap().idx = next_idx as isize;
            self.coupling.node.replace(c_guard);
            return Ok(true);
        }
        Ok(false)
    }

    #[inline]
    async fn lock_right(&mut self) -> Result<Option<(usize, PageExclusiveGuard<BTreeNode>)>> {
        if let Some(parent) = self.coupling.parent.as_mut() {
            let p_node = parent.g.page();
            let next_idx = (parent.idx + 1) as usize;
            if next_idx == p_node.count() {
                return Ok(None);
            }
            let c_page_id = p_node.value_as_page_id(next_idx);
            let c_guard = self
                .tree
                .pool
                .get_page(self.pool_guard, c_page_id, LatchFallbackMode::Exclusive)
                .await?
                .lock_exclusive_async()
                .await
                .unwrap();
            return Ok(Some((next_idx, c_guard)));
        }
        Ok(None)
    }

    #[inline]
    async fn lock_current(&mut self) -> Result<bool> {
        if self.coupling.node.is_some() {
            return Ok(true);
        }
        if let Some(parent) = self.coupling.parent.as_mut() {
            let p_node = parent.g.page();
            let c_page_id = if parent.idx == -1 {
                p_node.lower_fence_value().into()
            } else {
                p_node.value_as_page_id(parent.idx as usize)
            };
            let c_guard = self
                .tree
                .pool
                .get_page(self.pool_guard, c_page_id, LatchFallbackMode::Exclusive)
                .await?
                .lock_exclusive_async()
                .await
                .unwrap();
            self.coupling.node = Some(c_guard);
            return Ok(true);
        }
        Ok(false)
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
    ParentDone(PageID),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ResourceError;
    use crate::quiescent::{QuiescentBox, QuiescentGuard};
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use rand_distr::{Distribution, Uniform};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Barrier, mpsc};
    use std::time::Instant;

    const WIDE_KEY_LEN: usize = 1000;
    const WIDE_HEIGHT2_ROWS: u64 = 2_500;
    const LOOKUP_ROWS: usize = 20_000;
    const LOOKUP_PROBES: usize = 20_000;
    const SPLIT_CANDIDATE_SEARCH_LIMIT: u64 = 10_000;

    #[derive(Debug, Default)]
    struct LevelStat {
        nodes: usize,
        keys: usize,
        first_key_len: usize,
        prefix_len: usize,
    }

    struct ExactBoundaryResumeFixture {
        tree: BTree,
        left_branch_page_id: PageID,
        right_branch_page_id: PageID,
        left_leaf_page_id: PageID,
        first_right_leaf_page_id: PageID,
        second_right_leaf_page_id: PageID,
    }

    fn owned_index_pool(pool_size: usize) -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(
            FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, pool_size).unwrap(),
        )
    }

    fn wide_test_key(i: u64) -> [u8; WIDE_KEY_LEN] {
        let mut key = [0u8; WIDE_KEY_LEN];
        key[WIDE_KEY_LEN - std::mem::size_of::<u64>()..].copy_from_slice(&i.to_be_bytes());
        key
    }

    async fn insert_wide_rows(tree: &BTree, pool_guard: &PoolGuard, rows: u64, ts: TrxID) {
        for i in 0..rows {
            let key = wide_test_key(i);
            let res = tree
                .insert(pool_guard, &key, BTreeU64::from(i), false, ts)
                .await
                .unwrap();
            assert!(res.is_ok());
        }
    }

    async fn delete_wide_rows(tree: &BTree, pool_guard: &PoolGuard, rows: u64, ts: TrxID) {
        for i in 0..rows {
            let key = wide_test_key(i);
            let res = tree
                .delete(pool_guard, &key, BTreeU64::from(i), true, ts)
                .await
                .unwrap();
            assert!(res.is_ok());
        }
    }

    async fn collect_level_stats(
        tree: &BTree,
        pool_guard: &PoolGuard,
    ) -> HashMap<usize, LevelStat> {
        let mut map = HashMap::new();
        for height in 0usize..=tree.height() {
            let mut stat = LevelStat::default();
            let mut cursor = tree.cursor(pool_guard, height);
            cursor.seek(&[]).await.unwrap();
            while let Some(g) = cursor.next().await.unwrap() {
                let node = g.page();
                stat.nodes += 1;
                stat.keys += node.count();
                if node.count() > 0 {
                    stat.first_key_len += node.key(0).len();
                }
                stat.prefix_len += node.prefix_len();
            }
            map.insert(height, stat);
        }
        map
    }

    fn assert_level_links(map: &HashMap<usize, LevelStat>, height: usize) {
        for h in 1..=height {
            assert_eq!(map[&h].keys + 1, map[&(h - 1)].nodes);
        }
    }

    async fn next_wide_split_key(tree: &BTree, pool_guard: &PoolGuard, ts: TrxID) -> u64 {
        for i in 0..SPLIT_CANDIDATE_SEARCH_LIMIT {
            let key = wide_test_key(i);
            let res = tree
                .try_find_leaf_with_optimistic_parent::<SharedStrategy>(pool_guard, &key)
                .await
                .unwrap();
            let Validation::Valid((c_guard, p_guard)) = res else {
                continue;
            };
            let next_insert_splits_leaf = tree.height() >= 2
                && p_guard.is_some()
                && !c_guard.page().can_insert::<BTreeU64>(&key);
            drop(c_guard);
            drop(p_guard);
            if next_insert_splits_leaf {
                return i;
            }

            let res = tree
                .insert(pool_guard, &key, BTreeU64::from(i), false, ts)
                .await
                .unwrap();
            assert!(res.is_ok());
        }
        panic!("wide-key fixture did not reach a height-2 split candidate");
    }

    async fn run_lookup_against_map(hints_enabled: bool) {
        let pool = owned_index_pool(64 * 1024 * 1024);
        let pool_guard = (*pool).pool_guard();
        let tree = BTree::new(pool.guard(), &pool_guard, hints_enabled, 200)
            .await
            .expect("test btree construction should succeed");
        let mut map = BTreeMap::new();

        let between = Uniform::new(0u64, 10_000_000).unwrap();
        let mut rng = ChaCha8Rng::seed_from_u64(0u64);
        for i in 0..LOOKUP_ROWS {
            let k = between.sample(&mut rng);
            let res1 = tree
                .insert(
                    &pool_guard,
                    &k.to_be_bytes(),
                    BTreeU64::from(i as u64),
                    false,
                    201,
                )
                .await
                .unwrap();
            let res2 = map.entry(k).or_insert_with(|| i as u64);
            assert_eq!(res1.is_ok(), *res2 == i as u64);
        }

        for _ in 0..LOOKUP_PROBES {
            let k = between.sample(&mut rng);
            let res1 = tree
                .lookup_optimistic::<BTreeU64>(&pool_guard, &k.to_be_bytes())
                .await
                .unwrap();
            let res2 = map.get(&k).copied().map(BTreeU64::from);
            assert_eq!(res1, res2);
        }
    }

    async fn build_exact_boundary_resume_fixture(
        pool: QuiescentGuard<FixedBufferPool>,
        pool_guard: &PoolGuard,
    ) -> ExactBoundaryResumeFixture {
        let tree = BTree::new(pool, pool_guard, false, 200)
            .await
            .expect("test btree construction should succeed");

        let mut left_leaf_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let left_leaf_page_id = left_leaf_guard.page_id();
        let left_leaf = left_leaf_guard.page_mut();
        left_leaf.init(0, 200, &[], BTreeU64::INVALID_VALUE, b"ab", false);
        left_leaf.insert(b"aa", BTreeU64::from(1));
        drop(left_leaf_guard);

        let mut first_right_leaf_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let first_right_leaf_page_id = first_right_leaf_guard.page_id();
        let first_right_leaf = first_right_leaf_guard.page_mut();
        first_right_leaf.init(0, 200, b"ab", BTreeU64::INVALID_VALUE, b"ab\0", false);
        first_right_leaf.insert(b"ab", BTreeU64::from(2));
        drop(first_right_leaf_guard);

        let mut second_right_leaf_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let second_right_leaf_page_id = second_right_leaf_guard.page_id();
        let second_right_leaf = second_right_leaf_guard.page_mut();
        second_right_leaf.init(0, 200, b"ab\0", BTreeU64::INVALID_VALUE, &[], false);
        second_right_leaf.insert(b"ab\0", BTreeU64::from(3));
        drop(second_right_leaf_guard);

        let mut left_branch_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let left_branch_page_id = left_branch_guard.page_id();
        let left_branch = left_branch_guard.page_mut();
        left_branch.init(1, 200, &[], BTreeU64::from(left_leaf_page_id), b"ab", false);
        drop(left_branch_guard);

        let mut right_branch_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let right_branch_page_id = right_branch_guard.page_id();
        let right_branch = right_branch_guard.page_mut();
        right_branch.init(1, 200, b"ab", BTreeU64::INVALID_VALUE, &[], false);
        right_branch.insert(b"ab", BTreeU64::from(first_right_leaf_page_id));
        right_branch.insert(b"ab\0", BTreeU64::from(second_right_leaf_page_id));
        drop(right_branch_guard);

        let mut root_guard = tree
            .get_node(pool_guard, tree.root, LatchFallbackMode::Exclusive)
            .await
            .unwrap()
            .lock_exclusive_async()
            .await
            .unwrap();
        let root = root_guard.page_mut();
        root.init(2, 200, &[], BTreeU64::from(left_branch_page_id), &[], false);
        root.insert(b"ab", BTreeU64::from(right_branch_page_id));
        drop(root_guard);

        tree.height.store(2, Ordering::Release);

        ExactBoundaryResumeFixture {
            tree,
            left_branch_page_id,
            right_branch_page_id,
            left_leaf_page_id,
            first_right_leaf_page_id,
            second_right_leaf_page_id,
        }
    }

    #[test]
    fn test_btree_merge_partial_branch_suffix_drops_lower_fence_child() {
        smol::block_on(async {
            let pool = owned_index_pool(64 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                .await
                .expect("test btree construction should succeed");

            let mut p_node =
                BTreeNodeBox::alloc(2, 100, b"aa00", BTreeU64::from(10), b"zzzz", false);
            p_node.insert(b"mm00", BTreeU64::from(20));

            let mut l_node =
                BTreeNodeBox::alloc(1, 101, b"aa00", BTreeU64::from(100), b"mm00", false);
            l_node.insert(b"bb00", BTreeU64::from(101));

            let mut r_node =
                BTreeNodeBox::alloc(1, 102, b"mm00", BTreeU64::INVALID_VALUE, b"zzzz", false);
            r_node.insert(b"mm00", BTreeU64::from(200));
            r_node.insert(b"nn00", BTreeU64::from(201));
            r_node.insert(b"oo00", BTreeU64::from(202));

            let sep_key = r_node.create_sep_key(1, false);
            tree.merge_partial::<BTreeU64>(
                &mut p_node,
                0,
                &mut l_node,
                &mut r_node,
                b"aa00",
                &sep_key,
                b"zzzz",
                1,
                103,
            );

            assert_eq!(r_node.lower_fence_key().as_bytes(), b"nn00");
            assert_eq!(r_node.lower_fence_value(), BTreeU64::INVALID_VALUE);
            assert_eq!(r_node.count(), 2);
            assert_eq!(
                r_node.lookup_child(b"nn00"),
                LookupChild::Slot(0, PageID::from(201u64))
            );
            assert_eq!(
                r_node.lookup_child(b"oo00"),
                LookupChild::Slot(1, PageID::from(202u64))
            );
        })
    }

    #[test]
    fn test_btree_merge_full_deletes_parent_separator_with_branch_value_width() {
        smol::block_on(async {
            let pool = owned_index_pool(64 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                .await
                .expect("test btree construction should succeed");

            let mut p_node =
                BTreeNodeBox::alloc(1, 100, b"aa00", BTreeU64::from(10), b"zzzz", false);
            p_node.insert(b"mm00", BTreeU64::from(20));

            let mut l_node =
                BTreeNodeBox::alloc(0, 101, b"aa00", BTreeU64::INVALID_VALUE, b"mm00", false);
            l_node.insert(b"bb00", BTREE_BYTE_ZERO);

            let mut r_node =
                BTreeNodeBox::alloc(0, 102, b"mm00", BTreeU64::INVALID_VALUE, b"zzzz", false);
            r_node.insert(b"nn00", BTREE_BYTE_ZERO);
            r_node.insert(b"oo00", BTREE_BYTE_ZERO);

            tree.merge_node::<BTreeByte>(
                &mut p_node,
                0,
                &mut l_node,
                &mut r_node,
                b"aa00",
                b"zzzz",
                103,
            );

            let expected_parent =
                BTreeNodeBox::alloc(1, 103, b"aa00", BTreeU64::from(10), b"zzzz", false);
            assert_eq!(p_node.count(), 0);
            assert_eq!(
                p_node.lookup_child(b"bb00"),
                LookupChild::LowerFence(PageID::from(10u64))
            );
            assert_eq!(p_node.effective_space(), expected_parent.effective_space());
        })
    }

    #[test]
    fn test_btree_delete_exact_checks_value_and_delete_state() {
        smol::block_on(async {
            let pool = owned_index_pool(64 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            let tree = BTree::new(pool.guard(), &pool_guard, false, 100)
                .await
                .expect("test btree construction should succeed");
            tree.insert::<BTreeU64>(&pool_guard, b"k", BTreeU64::from(7), false, 100)
                .await
                .expect("test insert should succeed");

            assert_eq!(
                tree.delete_exact(&pool_guard, b"k", BTreeU64::from(8), false, 101)
                    .await
                    .expect("delete_exact should read the leaf"),
                BTreeDelete::ValueMismatch
            );
            assert_eq!(
                tree.delete_exact(&pool_guard, b"k", BTreeU64::from(7), true, 101)
                    .await
                    .expect("delete_exact should read the leaf"),
                BTreeDelete::ValueMismatch
            );
            assert_eq!(
                tree.lookup_optimistic::<BTreeU64>(&pool_guard, b"k")
                    .await
                    .expect("lookup should succeed"),
                Some(BTreeU64::from(7))
            );

            tree.mark_as_deleted::<BTreeU64>(&pool_guard, b"k", BTreeU64::from(7), 102)
                .await
                .expect("mark_as_deleted should read the leaf");
            assert_eq!(
                tree.delete_exact(&pool_guard, b"k", BTreeU64::from(7), false, 103)
                    .await
                    .expect("delete_exact should read the leaf"),
                BTreeDelete::ValueMismatch
            );
            assert_eq!(
                tree.delete_exact(&pool_guard, b"k", BTreeU64::from(7), true, 104)
                    .await
                    .expect("delete_exact should read the leaf"),
                BTreeDelete::Ok
            );
            assert_eq!(
                tree.lookup_optimistic::<BTreeU64>(&pool_guard, b"k")
                    .await
                    .expect("lookup should succeed"),
                None
            );
        })
    }

    #[test]
    fn test_btree_split_root_releases_left_page_when_right_allocation_fails() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(
                    crate::buffer::PoolRole::Index,
                    2 * (std::mem::size_of::<crate::buffer::frame::BufferFrame>()
                        + std::mem::size_of::<crate::buffer::page::Page>()),
                )
                .unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                .await
                .expect("test btree construction should succeed");
            assert_eq!(pool.allocated(), 1);

            {
                let mut root_guard = tree
                    .get_node(&pool_guard, tree.root, LatchFallbackMode::Exclusive)
                    .await
                    .unwrap()
                    .lock_exclusive_async()
                    .await
                    .unwrap();
                let root = root_guard.page_mut();
                root.init(0, 200, &[], BTreeU64::INVALID_VALUE, &[], false);
                root.insert(b"a", BTreeU64::from(1));
                root.insert(b"b", BTreeU64::from(2));
                root.insert(b"c", BTreeU64::from(3));

                let err = tree
                    .split_root::<BTreeU64>(&pool_guard, root, true, 210)
                    .await
                    .expect_err("second split-root allocation should fail");
                assert_eq!(err.resource_error(), Some(ResourceError::BufferPoolFull));
            }

            assert_eq!(pool.allocated(), 1);

            let extra = tree
                .allocate_node(&pool_guard)
                .await
                .expect("left split page should be returned to the pool on failure");
            pool.deallocate_page(extra);

            let root_guard = tree
                .get_node(&pool_guard, tree.root, LatchFallbackMode::Shared)
                .await
                .unwrap()
                .lock_shared_async()
                .await
                .unwrap();
            let root = root_guard.page();
            assert!(root.is_leaf());
            assert_eq!(root.height(), 0);
            assert_eq!(root.count(), 3);
            assert_eq!(root.search_key(b"a"), Ok(0));
            assert_eq!(root.search_key(b"b"), Ok(1));
            assert_eq!(root.search_key(b"c"), Ok(2));
        })
    }

    #[test]
    fn test_btree_cursor_resumes_with_raw_upper_fence_before_strict_successor() {
        smol::block_on(async {
            let pool = owned_index_pool(64 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let fixture = build_exact_boundary_resume_fixture(pool.guard(), &pool_guard).await;

                let root_guard = fixture
                    .tree
                    .get_node(&pool_guard, fixture.tree.root, LatchFallbackMode::Shared)
                    .await
                    .unwrap()
                    .lock_shared_async()
                    .await
                    .unwrap();
                let root = root_guard.page();
                assert_eq!(
                    root.lookup_child(b"ab"),
                    LookupChild::Slot(0, fixture.right_branch_page_id)
                );
                assert_eq!(
                    root.lookup_child(b"ab\0"),
                    LookupChild::Slot(0, fixture.right_branch_page_id)
                );
                drop(root_guard);

                let right_branch_guard = fixture
                    .tree
                    .get_node(
                        &pool_guard,
                        fixture.right_branch_page_id,
                        LatchFallbackMode::Shared,
                    )
                    .await
                    .unwrap()
                    .lock_shared_async()
                    .await
                    .unwrap();
                let right_branch = right_branch_guard.page();
                assert_eq!(
                    right_branch.lookup_child(b"ab"),
                    LookupChild::Slot(0, fixture.first_right_leaf_page_id)
                );
                assert_eq!(
                    right_branch.lookup_child(b"ab\0"),
                    LookupChild::Slot(1, fixture.second_right_leaf_page_id)
                );
                drop(right_branch_guard);

                let mut cursor = fixture.tree.cursor(&pool_guard, 0);
                cursor.seek(&[]).await.unwrap();

                let first = cursor.next().await.unwrap().unwrap();
                assert_eq!(first.page_id(), fixture.left_leaf_page_id);
                drop(first);

                let second = cursor.next().await.unwrap().unwrap();
                assert_eq!(second.page_id(), fixture.first_right_leaf_page_id);
                assert_ne!(second.page_id(), fixture.left_leaf_page_id);
            }
        })
    }

    #[test]
    fn test_btree_compactor_parent_done_buffers_raw_upper_fence() {
        smol::block_on(async {
            let pool = owned_index_pool(64 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let fixture = build_exact_boundary_resume_fixture(pool.guard(), &pool_guard).await;
                let mut compactor =
                    fixture
                        .tree
                        .compact::<BTreeU64>(&pool_guard, 0, BTreeCompactConfig::default());
                let mut lower_fence_key_buffer = Vec::new();
                let mut upper_fence_key_buffer = Vec::new();
                let mut purge_list = Vec::new();

                compactor.seek(&[]).await.unwrap();
                let res = compactor
                    .step(
                        &mut lower_fence_key_buffer,
                        &mut upper_fence_key_buffer,
                        &mut purge_list,
                    )
                    .await
                    .unwrap();
                assert!(
                    matches!(res, BTreeCompact::ParentDone(page_id) if page_id == fixture.left_branch_page_id)
                );
                assert_eq!(upper_fence_key_buffer, b"ab");

                compactor.seek(&upper_fence_key_buffer).await.unwrap();
                let node = compactor.coupling.node.as_ref().unwrap();
                assert_eq!(node.page_id(), fixture.first_right_leaf_page_id);
            }
        })
    }

    #[test]
    fn test_btree_single_node() {
        smol::block_on(async {
            let pool = owned_index_pool(64 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                    .await
                    .expect("test btree construction should succeed");
                // insert 1, 2, 3.
                let one = BTreeU64::from(1);
                let three = BTreeU64::from(3);
                let five = BTreeU64::from(5);
                let seven = BTreeU64::from(7);
                let fifty = BTreeU64::from(50);
                let seventy = BTreeU64::from(70);
                let res = tree
                    .insert(&pool_guard, &1u64.to_be_bytes(), one, false, 210)
                    .await;
                assert!(res.is_ok());
                let res = tree
                    .insert(&pool_guard, &3u64.to_be_bytes(), three, false, 220)
                    .await;
                assert!(res.is_ok());
                let res = tree
                    .insert(&pool_guard, &5u64.to_be_bytes(), five, false, 205)
                    .await;
                assert!(res.is_ok());

                let res = tree
                    .insert(&pool_guard, &5u64.to_be_bytes(), five, false, 230)
                    .await
                    .unwrap();
                assert!(res == BTreeInsert::DuplicateKey(five));

                // look up
                let res = tree
                    .lookup_optimistic::<BTreeU64>(&pool_guard, &1u64.to_be_bytes())
                    .await
                    .unwrap();
                assert!(res.is_some());
                let res = tree
                    .lookup_optimistic::<BTreeU64>(&pool_guard, &4u64.to_be_bytes())
                    .await
                    .unwrap();
                assert!(res.is_none());
                // mark as deleted
                let res = tree
                    .mark_as_deleted(&pool_guard, &3u64.to_be_bytes(), three, 230)
                    .await;
                assert!(res.is_ok());
                let res = tree
                    .mark_as_deleted(&pool_guard, &5u64.to_be_bytes(), five, 230)
                    .await;
                assert!(res.is_ok());
                let res = tree
                    .mark_as_deleted(&pool_guard, &7u64.to_be_bytes(), seven, 235)
                    .await
                    .unwrap();
                assert_eq!(res, BTreeUpdate::NotFound);
                let res = tree
                    .lookup_optimistic::<BTreeU64>(&pool_guard, &5u64.to_be_bytes())
                    .await
                    .unwrap();
                assert_eq!(res, Some(five.deleted()));
                // update
                let res = tree
                    .update(&pool_guard, &5u64.to_be_bytes(), five.deleted(), fifty, 240)
                    .await;
                assert!(res.is_ok());
                let res = tree
                    .update(
                        &pool_guard,
                        &5u64.to_be_bytes(),
                        five.deleted(),
                        seventy,
                        245,
                    )
                    .await
                    .unwrap();
                assert_eq!(res, BTreeUpdate::ValueMismatch(fifty));
                // delete
                let res = tree
                    .delete(&pool_guard, &3u64.to_be_bytes(), three, true, 250)
                    .await;
                assert!(res.is_ok());
                let res = tree
                    .delete(&pool_guard, &5u64.to_be_bytes(), five, true, 255)
                    .await
                    .unwrap();
                assert_eq!(res, BTreeDelete::ValueMismatch);
            }
        })
    }

    #[test]
    fn test_btree_scale() {
        smol::block_on(async {
            let pool = owned_index_pool(128 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                    .await
                    .expect("test btree construction should succeed");

                insert_wide_rows(&tree, &pool_guard, WIDE_HEIGHT2_ROWS, 201).await;
                assert_eq!(tree.height(), 2);
                let map = collect_level_stats(&tree, &pool_guard).await;
                assert_eq!(map[&0].keys as u64, WIDE_HEIGHT2_ROWS);
                assert_level_links(&map, tree.height());
            }
        })
    }

    #[test]
    fn test_btree_delete() {
        smol::block_on(async {
            let pool = owned_index_pool(128 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                    .await
                    .expect("test btree construction should succeed");

                insert_wide_rows(&tree, &pool_guard, WIDE_HEIGHT2_ROWS, 201).await;
                assert_eq!(tree.height(), 2);
                delete_wide_rows(&tree, &pool_guard, WIDE_HEIGHT2_ROWS, 202).await;

                let map = collect_level_stats(&tree, &pool_guard).await;
                assert_eq!(map[&0].keys, 0);
                assert_level_links(&map, tree.height());
            }
        })
    }

    #[test]
    fn test_btree_compact() {
        smol::block_on(async {
            let pool = owned_index_pool(128 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                    .await
                    .expect("test btree construction should succeed");

                insert_wide_rows(&tree, &pool_guard, WIDE_HEIGHT2_ROWS, 201).await;
                assert_eq!(tree.height(), 2);
                delete_wide_rows(&tree, &pool_guard, WIDE_HEIGHT2_ROWS, 202).await;

                let config = BTreeCompactConfig::new(1.0, 1.0).unwrap();
                let purge_list = tree
                    .compact_all::<BTreeU64>(&pool_guard, config)
                    .await
                    .unwrap();

                for g in purge_list {
                    pool.deallocate_page(g);
                }

                let map = collect_level_stats(&tree, &pool_guard).await;
                assert_eq!(map[&0].keys, 0);
                assert_level_links(&map, tree.height());
            }
        })
    }

    #[test]
    fn test_btree_lookup_disable_hints() {
        smol::block_on(async {
            run_lookup_against_map(false).await;
        })
    }

    #[test]
    fn test_btree_lookup_enable_hints() {
        smol::block_on(async {
            run_lookup_against_map(true).await;
        })
    }

    #[test]
    fn test_btree_with_stdmap() {
        smol::block_on(async {
            const ROWS: u64 = 10_000;
            const MAX_VALUE: u64 = 100_000;
            let pool = owned_index_pool(20 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let tree = BTree::new(pool.guard(), &pool_guard, false, 1)
                    .await
                    .expect("test btree construction should succeed");

                let start = Instant::now();
                // insert with random distribution.
                {
                    let between = Uniform::new(0u64, MAX_VALUE).unwrap();
                    let mut thd_rng = rand::rng();
                    for i in 0..ROWS {
                        let k = between.sample(&mut thd_rng);
                        tree.insert(&pool_guard, &k.to_be_bytes(), BTreeU64::from(i), false, 100)
                            .await
                            .unwrap();
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
                        tree.insert(&pool_guard, &k.to_be_bytes(), BTreeU64::from(i), false, 100)
                            .await
                            .unwrap();
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
        })
    }

    #[test]
    fn test_btree_split() {
        smol::block_on(async {
            let pool = owned_index_pool(128 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let tree = Arc::new(
                    BTree::new(pool.guard(), &pool_guard, false, 200)
                        .await
                        .expect("test btree construction should succeed"),
                );

                let insert_key_idx = next_wide_split_key(&tree, &pool_guard, 201).await;
                let insert_key = wide_test_key(insert_key_idx);
                let (parent_locked_tx, parent_locked_rx) = mpsc::channel();
                let (insert_started_tx, insert_started_rx) = mpsc::channel();
                let (release_parent_tx, release_parent_rx) = mpsc::channel();
                let shared_lock_handle = {
                    let tree = Arc::clone(&tree);
                    let pool_guard = pool_guard.clone();
                    let insert_key = insert_key.to_vec();
                    std::thread::spawn(move || {
                        smol::block_on(async {
                            let res = tree
                                .try_find_leaf_with_optimistic_parent::<SharedStrategy>(
                                    &pool_guard,
                                    &insert_key,
                                )
                                .await
                                .unwrap();
                            let Validation::Valid((c_guard, p_guard)) = res else {
                                panic!("leaf lookup should validate");
                            };
                            drop(c_guard);
                            let p_guard = p_guard.unwrap();
                            let shared_guard = p_guard.lock_shared_async().await.unwrap();
                            parent_locked_tx.send(()).unwrap();
                            release_parent_rx.recv().unwrap();
                            drop(shared_guard);
                        })
                    })
                };
                // Wait for the worker to hold the parent shared lock before starting insert.
                parent_locked_rx.recv().unwrap();
                let insert_handle = {
                    let tree = Arc::clone(&tree);
                    let pool_guard = pool_guard.clone();
                    let insert_key = insert_key.to_vec();
                    std::thread::spawn(move || {
                        smol::block_on(async {
                            let insert_fut = tree.insert(
                                &pool_guard,
                                &insert_key,
                                BTreeU64::from(insert_key_idx),
                                false,
                                202,
                            );
                            futures::pin_mut!(insert_fut);
                            assert!(matches!(
                                futures::poll!(insert_fut.as_mut()),
                                std::task::Poll::Pending
                            ));
                            insert_started_tx.send(()).unwrap();
                            let res = insert_fut.await;
                            assert!(res.is_ok());
                        })
                    })
                };
                insert_started_rx.recv().unwrap();
                release_parent_tx.send(()).unwrap();
                shared_lock_handle.join().unwrap();
                insert_handle.join().unwrap();
                assert_eq!(
                    tree.lookup_optimistic::<BTreeU64>(&pool_guard, &insert_key)
                        .await
                        .unwrap(),
                    Some(BTreeU64::from(insert_key_idx))
                );
            }
        })
    }

    #[test]
    fn test_btree_concurrent_split() {
        smol::block_on(async {
            let pool = owned_index_pool(128 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let tree = Arc::new(
                    BTree::new(pool.guard(), &pool_guard, false, 200)
                        .await
                        .expect("test btree construction should succeed"),
                );

                let first_insert_key = next_wide_split_key(&tree, &pool_guard, 201).await;
                let concurrent_inserts = 10u64;
                let start = Arc::new(Barrier::new(concurrent_inserts as usize + 1));
                let mut handles = Vec::with_capacity(10);
                for j in first_insert_key..first_insert_key + concurrent_inserts {
                    let tree = Arc::clone(&tree);
                    let pool_guard = pool_guard.clone();
                    let start = Arc::clone(&start);
                    let handle = std::thread::spawn(move || {
                        start.wait();
                        smol::block_on(async {
                            let key = wide_test_key(j);
                            let res = tree
                                .insert(&pool_guard, &key, BTreeU64::from(j), false, 202)
                                .await;
                            assert!(res.is_ok());
                        })
                    });
                    handles.push(handle);
                }
                start.wait();
                for handle in handles {
                    handle.join().unwrap();
                }
                for j in first_insert_key..first_insert_key + concurrent_inserts {
                    let key = wide_test_key(j);
                    assert_eq!(
                        tree.lookup_optimistic::<BTreeU64>(&pool_guard, &key)
                            .await
                            .unwrap(),
                        Some(BTreeU64::from(j))
                    );
                }
            }
        })
    }

    #[test]
    fn test_btree_merge_partial() {
        smol::block_on(async {
            let pool = owned_index_pool(128 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            {
                let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                    .await
                    .expect("test btree construction should succeed");
                insert_wide_rows(&tree, &pool_guard, WIDE_HEIGHT2_ROWS, 201).await;
                let before_space = tree.collect_space_statistics(&pool_guard).await.unwrap();
                let before_levels = collect_level_stats(&tree, &pool_guard).await;

                let config = BTreeCompactConfig::new(1.0, 1.0).unwrap();
                let purge_list = tree
                    .compact_all::<BTreeU64>(&pool_guard, config)
                    .await
                    .unwrap();
                for g in purge_list {
                    pool.deallocate_page(g);
                }

                let after_space = tree.collect_space_statistics(&pool_guard).await.unwrap();
                let after_levels = collect_level_stats(&tree, &pool_guard).await;
                assert_eq!(after_levels[&0].keys, before_levels[&0].keys);
                assert!(after_space.nodes <= before_space.nodes);
            }
        })
    }

    #[test]
    fn test_btree_destory() {
        const H0_ROWS: u64 = 10;
        const H1_ROWS: u64 = 1000;
        const H2_ROWS: u64 = WIDE_HEIGHT2_ROWS;
        smol::block_on(async {
            let pool = owned_index_pool(128 * 1024 * 1024);
            let pool_guard = (*pool).pool_guard();
            assert!(pool.allocated() == 0);
            // height=0
            {
                let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                    .await
                    .expect("test btree construction should succeed");
                insert_wide_rows(&tree, &pool_guard, H0_ROWS, 201).await;
                assert_eq!(tree.height(), 0);
                tree.destory(&pool_guard).await.unwrap();
                assert!(pool.allocated() == 0);
            }
            // height=1
            {
                let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                    .await
                    .expect("test btree construction should succeed");
                insert_wide_rows(&tree, &pool_guard, H1_ROWS, 201).await;
                assert_eq!(tree.height(), 1);
                tree.destory(&pool_guard).await.unwrap();
                assert!(pool.allocated() == 0);
            }
            // height=2
            {
                let tree = BTree::new(pool.guard(), &pool_guard, false, 200)
                    .await
                    .expect("test btree construction should succeed");
                insert_wide_rows(&tree, &pool_guard, H2_ROWS, 201).await;
                assert_eq!(tree.height(), 2);
                tree.destory(&pool_guard).await.unwrap();
                assert!(pool.allocated() == 0);
            }
        })
    }
}
