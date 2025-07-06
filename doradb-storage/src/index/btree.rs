use crate::buffer::guard::{
    ExclusiveLockStrategy, FacadePageGuard, LockStrategy, OptimisticLockStrategy,
    PageExclusiveGuard, PageGuard, PageSharedGuard, SharedLockStrategy,
};
use crate::buffer::page::PageID;
use crate::buffer::{BufferPool, FixedBufferPool};
use crate::error::Validation;
use crate::error::Validation::{Invalid, Valid};
use crate::error::{Error, Result};
use crate::index::btree_node::{
    BTreeNode, KeyVec, LookupChild, SearchKey, SearchValue, SpaceEstimation,
};
use crate::index::util::ParentPosition;
use crate::latch::LatchFallbackMode;
use crate::trx::TrxID;
use either::Either;
use std::marker::PhantomData;
use std::mem;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

type SharedStrategy = SharedLockStrategy<BTreeNode>;
type ExclusiveStrategy = ExclusiveLockStrategy<BTreeNode>;
type OptimisticStrategy = OptimisticLockStrategy<BTreeNode>;

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
        loop {
            let res = self
                .try_find_leaf_with_optimistic_parent::<ExclusiveStrategy>(key)
                .await;
            let (mut c_guard, p_guard) = verify_continue!(res);
            let node = c_guard.page_mut();
            let idx = match node.search_key(key) {
                SearchKey::GreaterThan(idx) => idx + 1,
                SearchKey::Equal(..) => {
                    // Do not allow insert even if same key is marked as deleted.
                    return BTreeInsert::DuplicateKey;
                }
                SearchKey::LowerFence => 0,
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
            let mut g = self.find_leaf::<ExclusiveStrategy>(key).await;
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
    ///
    /// We do not apply merge logic when deleting keys.
    /// Instead a tree-level compact() method can be used periodically
    /// to make the tree balanced.
    #[inline]
    pub async fn delete<V: BTreeValue>(&self, key: &[u8], value: V, ts: TrxID) -> BTreeDelete {
        loop {
            let mut g = self.find_leaf::<ExclusiveStrategy>(key).await;
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
            let mut g = self.find_leaf::<ExclusiveStrategy>(key).await;
            debug_assert!(g.page().is_leaf());
            let node = g.page_mut();
            return node
                .update(key, old_value, new_value)
                .ok_then(|| node.update_ts(ts));
        }
    }

    /// Try to lookup a key in the tree, break if any of optimistic validation fails.
    #[inline]
    async fn try_lookup_optimistic<V: BTreeValue>(&self, key: &[u8]) -> Validation<BTreeLookup<V>> {
        let g = self.find_leaf::<OptimisticStrategy>(key).await;
        let leaf = unsafe { g.page_unchecked() };
        match leaf.search_value(key) {
            SearchValue::Equal(_, value) => {
                verify!(g.validate());
                Valid(BTreeLookup::Exists(value))
            }
            SearchValue::EqualDeleted(_, value) => {
                verify!(g.validate());
                Valid(BTreeLookup::Deleted(value))
            }
            SearchValue::GreaterThan(_) | SearchValue::LessThanAllSlots => {
                verify!(g.validate());
                Valid(BTreeLookup::NotFound)
            }
            SearchValue::LessThanLowerFence | SearchValue::GreaterEqualUpperFence => {
                verify!(g.validate());
                Validation::Invalid
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
        let mut p_guard = p_guard.exclusive_async().await;
        let p_node = p_guard.page();
        match p_node.search_value_slow::<PageID>(&c_lower_fence_key) {
            SearchValue::Equal(_, page_id) if page_id == c_page_id => {
                // Tree structure remains the same.
                // Check if parent is full.
                if !p_node.can_insert(&sep_key) {
                    let p_page_id = p_guard.page_id();
                    if p_page_id != self.root {
                        // Parent is full, trigger top-down split of parent node.
                        let p_lower_fence_key = p_node.lower_fence_key();
                        return Either::Right(BTreeSplit::full_branch(
                            &p_lower_fence_key,
                            p_page_id,
                            &sep_key,
                        ));
                    }
                    // Parent is root and full, just split.
                    self.split_root::<PageID>(p_guard.page_mut(), false, ts)
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
                Either::Left((p_guard, c_guard))
            }
            _ => {
                // Tree structure changes, abort the split.
                Either::Right(BTreeSplit::Inconsistent)
            }
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
        let mut tmp_l = unsafe {
            let mut node = MaybeUninit::<BTreeNode>::zeroed();
            node.assume_init_mut().init(
                l_node.height() as u16,
                ts,
                lower_fence_key,
                l_node.lower_fence_value(),
                upper_fence_key,
            );
            node.assume_init()
        };
        tmp_l.extend_slots_from::<V>(l_node, 0, l_node.count());
        if r_node.count() > 0 {
            tmp_l.extend_slots_from::<V>(r_node, 0, r_node.count());
        }
        *l_node = tmp_l;
        p_node.delete_at(p_r_idx, value_size);
        p_node.update_ts(ts);
    }

    /// Merge partial right node into left node.
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
        debug_assert!(count > 0 && count + 1 < r_node.count());
        debug_assert!(&r_node.create_sep_key(count, r_node.height() == 0)[..] == sep_key);
        debug_assert!({
            let mut estimation = SpaceEstimation::with_fences(lower_fence_key, sep_key, value_size);
            estimation.add_key_range(l_node, 0, l_node.count());
            estimation.add_key_range(r_node, 0, count);
            estimation.total_space() <= mem::size_of::<BTreeNode>()
        });
        let ts = ts.max(l_node.ts()).max(r_node.ts()).max(p_node.ts());
        let mut tmp_l = unsafe {
            let mut node = MaybeUninit::<BTreeNode>::zeroed();
            node.assume_init_mut().init(
                l_node.height() as u16,
                ts,
                lower_fence_key,
                l_node.lower_fence_value(),
                sep_key,
            );
            node.assume_init()
        };
        tmp_l.extend_slots_from::<V>(l_node, 0, l_node.count());
        tmp_l.extend_slots_from::<V>(r_node, 0, count);
        *l_node = tmp_l;

        let mut tmp_r = unsafe {
            let lower_fence_value = if r_node.height() == 0 {
                PageID::INVALID_VALUE
            } else {
                r_node.value::<PageID>(count)
            };
            let mut node = MaybeUninit::<BTreeNode>::zeroed();
            node.assume_init_mut().init(
                r_node.height() as u16,
                ts,
                sep_key,
                lower_fence_value,
                &upper_fence_key,
            );
            node.assume_init()
        };
        tmp_r.extend_slots_from::<V>(r_node, count, r_node.count() - count);
        *r_node = tmp_r;

        p_node.update_key::<PageID>(p_r_idx, sep_key);
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
                LookupChild::NotFound => {
                    verify!(g.validate());
                    return Valid(None);
                }
            }
        }
    }

    /// Create a cursor to iterator over nodes at given height.
    /// Height equals to 0 means iterating over all leaf nodes.
    #[inline]
    pub fn cursor(&self, height: usize) -> BTreeNodeCursor {
        BTreeNodeCursor::new(self, height)
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
    ) -> BTreeCompactor<V> {
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
            self.compact::<PageID>(h, config)
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
            let pu = unsafe { g.page_unchecked() };
            let height = pu.height();
            let count = pu.count();
            verify_continue!(g.validate());
            if height == 0 || count > 0 {
                return;
            }
            // should shrink tree height as there is no keys in root.
            // The only child is associated with lower fence key.
            let mut g = g.exclusive_async().await;
            // re-check condition
            let root = g.page_mut();
            if root.height() == 0 || root.count() > 0 {
                return;
            }
            let c_page_id = root.lower_fence_value();
            let mut c_guard = self
                .pool
                .get_page::<BTreeNode>(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .exclusive_async()
                .await;
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
}

/// Controls how to access B-Tree nodes with coupling way.
pub struct BTreeCoupling<S: LockStrategy> {
    // Parent position to locate target node.
    // can be optional.
    parent: Option<ParentPosition<S::Guard>>,
    node: Option<S::Guard>,
}

impl<S: LockStrategy<Page = BTreeNode>> BTreeCoupling<S>
where
    S::Guard: PageGuard<BTreeNode>,
{
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
            let c_page_id = p_node.value::<PageID>(next_idx);
            self.coupling.parent.as_mut().unwrap().idx = next_idx as isize;
            let c_guard = self
                .tree
                .pool
                .get_page::<BTreeNode>(c_page_id, LatchFallbackMode::Shared)
                .await
                .shared_async()
                .await;
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
        if low_ratio < 0.0
            || low_ratio > 1.0
            || high_ratio < 0.0
            || high_ratio > 1.0
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
        let mut lower_fence_key_buffer = KeyVec::new();
        let mut upper_fence_key_buffer = KeyVec::new();
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
        lower_fence_key_buffer: &mut KeyVec,
        upper_fence_key_buffer: &mut KeyVec,
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
                        node.self_compact::<PageID>();
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
                    l_node.extract_lower_fence_key(lower_fence_key_buffer);
                    r_node.extract_upper_fence_key(upper_fence_key_buffer);
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
                    if !p_node.prepare_update_key::<PageID>(p_r_idx, &sep_key) {
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
                    p_node.extract_upper_fence_key(upper_fence_key_buffer);
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
            let c_page_id = p_node.value::<PageID>(next_idx);
            let c_guard = self
                .tree
                .pool
                .get_page(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .exclusive_async()
                .await;

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
            let c_page_id = p_node.value::<PageID>(next_idx);
            let c_guard = self
                .tree
                .pool
                .get_page(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .exclusive_async()
                .await;
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
                p_node.lower_fence_value()
            } else {
                p_node.value::<PageID>(parent.idx as usize)
            };
            let c_guard = self
                .tree
                .pool
                .get_page(c_page_id, LatchFallbackMode::Exclusive)
                .await
                .exclusive_async()
                .await;
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

/// Statistics of space used by nodes.
#[derive(Debug, Default)]
pub struct SpaceStatistics {
    pub nodes: usize,
    pub total_space: usize,
    pub used_space: usize,
    pub effective_space: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifetime::StaticLifetime;
    use crate::notify::Signal;
    use rand::distributions::{Distribution, Uniform};
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
                let tree = BTree::new(pool, 200).await;
                // insert 1, 2, 3.
                let res = tree.insert(&1u64.to_be_bytes(), 1, 210).await;
                assert!(res.is_ok());
                let res = tree.insert(&3u64.to_be_bytes(), 3, 220).await;
                assert!(res.is_ok());
                let res = tree.insert(&5u64.to_be_bytes(), 5, 205).await;
                assert!(res.is_ok());

                let res = tree.insert(&5u64.to_be_bytes(), 5, 230).await;
                assert!(res == BTreeInsert::DuplicateKey);

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
        const ROWS: u64 = 90089;
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
                let tree = BTree::new(pool, 200).await;

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                for i in 0u64..ROWS {
                    // let k = i.to_be_bytes();
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, i, 201).await;
                    assert!(res.is_ok());
                }

                for i in 0u64..ROWS {
                    // let k = i.to_be_bytes();
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.delete(&key, i, 202).await;
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
                let tree = BTree::new(pool, 200).await;

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                for i in 0u64..ROWS {
                    // let k = i.to_be_bytes();
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, i, 201).await;
                    assert!(res.is_ok());
                }

                for i in 0u64..ROWS {
                    // let k = i.to_be_bytes();
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.delete(&key, i, 202).await;
                    assert!(res.is_ok());
                }

                println!("tree height {}", tree.height());
                let space_stat = tree.collect_space_statistics().await;
                println!("Before compaction, tree space statistics: {:?}", space_stat);

                let config = BTreeCompactConfig::new(1.0, 1.0).unwrap();
                let purge_list = tree.compact_all::<u64>(config).await;

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
    fn test_btree_lookup() {
        const ROWS: usize = 500000;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(2 * 1024 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, 200).await;
                let mut map = BTreeMap::new();

                // number less than 10 million
                let between = Uniform::from(0u64..10_000_000);
                let mut rng = rand::thread_rng();
                for i in 0..ROWS {
                    let k = between.sample(&mut rng);
                    let res1 = tree.insert(&k.to_be_bytes(), i as u64, 201).await;
                    let res2 = map.entry(k).or_insert_with(|| i as u64);
                    assert!(res1.is_ok() == (*res2 == i as u64));
                }
                println!("tree height {}", tree.height());
                let space_stat = tree.collect_space_statistics().await;
                println!("tree space statistics: {:?}", space_stat);

                for _ in 0..ROWS {
                    let k = between.sample(&mut rng);
                    let res1 = tree.lookup_optimistic::<u64>(&k.to_be_bytes()).await;
                    let res2 = map.get(&k);
                    if let Some(v) = res2 {
                        assert!(res1 == BTreeLookup::Exists(*v));
                    } else {
                        assert!(res1 == BTreeLookup::NotFound);
                    }
                }
            }

            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_btree_with_stdmap() {
        smol::block_on(async {
            const ROWS: u64 = 1_000_000;
            const MAX_VALUE: u64 = 10_000_000;
            let pool = FixedBufferPool::with_capacity_static(1 * 1024 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, 1).await;

                let start = Instant::now();
                // insert with random distribution.
                {
                    let between = Uniform::from(0u64..MAX_VALUE);
                    let mut thd_rng = rand::thread_rng();
                    for i in 0..ROWS {
                        let k = between.sample(&mut thd_rng);
                        tree.insert(&k.to_be_bytes(), i, 100).await;
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
                    let between = Uniform::from(0..MAX_VALUE);
                    let mut thd_rng = rand::thread_rng();
                    for i in 0..ROWS {
                        let k = between.sample(&mut thd_rng);
                        tree.insert(&k.to_be_bytes(), i, 100).await;
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
                let tree = Arc::new(BTree::new(pool, 200).await);

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                for i in 0u64..ROWS {
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, i, 201).await;
                    assert!(res.is_ok());
                }
                let signal = Signal::default();
                let notify = signal.new_notify();
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
                            let shared_guard = p_guard.shared_async().await;
                            signal.notify(1);
                            smol::Timer::after(Duration::from_millis(1000)).await;
                            println!("going to drop");
                            drop(shared_guard);
                        })
                    });
                }
                // wait for another thread to acquire parent's shared lock.
                notify.wait_async().await;
                println!("tree height {}", tree.height());
                key[..8].copy_from_slice(&90088u64.to_be_bytes()[..]);
                let res = tree.insert(&key, 90088, 202).await;
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
                let tree = Arc::new(BTree::new(pool, 200).await);

                // Key length in leaf node is about 1000 bytes.
                // Key length in branch node is about 8 bytes(with suffix truncation).
                let mut key = [0u8; 1000];
                for i in 0u64..ROWS {
                    key[..8].copy_from_slice(&i.to_be_bytes()[..]);
                    let res = tree.insert(&key, i, 201).await;
                    assert!(res.is_ok());
                }
                let signal = Signal::default();
                let mut handles = Vec::with_capacity(10);
                for j in 90088u64..90098 {
                    let tree = Arc::clone(&tree);
                    let notify = signal.new_notify();
                    let handle = std::thread::spawn(move || {
                        smol::block_on(async {
                            let mut key = vec![0u8; 1000];
                            key[..8].copy_from_slice(&j.to_be_bytes()[..]);

                            notify.wait_async().await;

                            let res = tree.insert(&key, 90088, 202).await;
                            assert!(res.is_ok());
                        })
                    });
                    handles.push(handle);
                }
                // wait for another thread to acquire parent's shared lock.
                signal.done(usize::MAX);
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
        const ROWS: usize = 500000;
        smol::block_on(async {
            // 1GB buffer pool.
            let pool = FixedBufferPool::with_capacity_static(2 * 1024 * 1024 * 1024).unwrap();
            {
                let tree = BTree::new(pool, 200).await;
                // number less than 10 million
                let between = Uniform::from(0u64..10_000_000);
                let mut rng = rand::thread_rng();
                for i in 0..ROWS {
                    let k = between.sample(&mut rng);
                    let _ = tree.insert(&k.to_be_bytes(), i as u64, 201).await;
                }
                let space_stat = tree.collect_space_statistics().await;
                println!(
                    "before compaction, tree height {}, space statistics: {:?}",
                    tree.height(),
                    space_stat
                );

                let config = BTreeCompactConfig::new(1.0, 1.0).unwrap();
                tree.compact_all::<u64>(config).await;

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
}
