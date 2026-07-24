//! Resumable B-tree node cursor traversal.

use super::{BTreeCoupling, BTreeNode, GenericBTree, SharedStrategy};
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::{BufferPool, PoolGuard};
use crate::error::RuntimeResult;
use crate::id::PageID;
use crate::latch::LatchFallbackMode;

/// Shared cursor of nodes at given height.
/// At most two locks are held at the same time.
///
/// This cursor does not guarantee consistent snapshot during iterating
/// the tree nodes.
/// But it guarantees if a value(node) does not change during the traverse,
/// it will be always visited.
pub(crate) struct BTreeNodeCursorState {
    height: usize,
    coupling: BTreeCoupling<SharedStrategy>,
    resume_key_buffer: Vec<u8>,
}

impl BTreeNodeCursorState {
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
    pub(crate) fn new(height: usize) -> Self {
        Self {
            height,
            coupling: BTreeCoupling::<SharedStrategy>::new(),
            resume_key_buffer: Vec::new(),
        }
    }

    /// Seek to the first node at this cursor height that may contain `key`.
    #[inline]
    pub(crate) async fn seek<P: BufferPool>(
        &mut self,
        tree: &GenericBTree<P>,
        pool_guard: &PoolGuard,
        key: &[u8],
    ) -> RuntimeResult<()> {
        self.coupling
            .seek_and_lock(tree, pool_guard, self.height, key)
            .await
    }

    /// Fetch next node.
    #[inline]
    pub(crate) async fn next<P: BufferPool>(
        &mut self,
        tree: &GenericBTree<P>,
        pool_guard: &PoolGuard,
    ) -> RuntimeResult<Option<PageSharedGuard<BTreeNode>>> {
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
                    let c_guard = tree
                        .pool
                        .get_page::<BTreeNode>(pool_guard, c_page_id, LatchFallbackMode::Shared)
                        .await?
                        .lock_shared_async()
                        .await
                        .unwrap();
                    return Ok(Some(c_guard));
                }
            };
            self.coupling
                .seek_and_lock(tree, pool_guard, self.height, &self.resume_key_buffer)
                .await?;
            if let Some(exhausted_parent_page_id) = exhausted_parent_page_id
                && self.resumed_same_parent(exhausted_parent_page_id)
            {
                make_strict_successor(&mut self.resume_key_buffer);
                self.coupling
                    .seek_and_lock(tree, pool_guard, self.height, &self.resume_key_buffer)
                    .await?;
            }
            return Ok(self.coupling.node.take());
        }
        Ok(None)
    }
}

/// Resumable cursor over B-tree nodes at one height.
pub(crate) struct BTreeNodeCursor<'a, P: 'static> {
    tree: &'a GenericBTree<P>,
    pool_guard: &'a PoolGuard,
    state: BTreeNodeCursorState,
}

impl<'a, P: BufferPool> BTreeNodeCursor<'a, P> {
    /// Create a new cursor.
    #[inline]
    pub(crate) fn new(tree: &'a GenericBTree<P>, pool_guard: &'a PoolGuard, height: usize) -> Self {
        BTreeNodeCursor {
            tree,
            pool_guard,
            state: BTreeNodeCursorState::new(height),
        }
    }

    /// Seek to the first node at this cursor height that may contain `key`.
    #[inline]
    pub(crate) async fn seek(&mut self, key: &[u8]) -> RuntimeResult<()> {
        self.state.seek(self.tree, self.pool_guard, key).await
    }

    /// Fetch next node.
    #[inline]
    pub(crate) async fn next(&mut self) -> RuntimeResult<Option<PageSharedGuard<BTreeNode>>> {
        self.state.next(self.tree, self.pool_guard).await
    }
}

#[inline]
pub(super) fn build_exhausted_parent_seek_key(node: &BTreeNode, key_buffer: &mut Vec<u8>) {
    key_buffer.clear();
    if node.has_no_upper_fence() {
        return;
    }
    node.extend_upper_fence_key(key_buffer);
}

#[inline]
pub(super) fn make_strict_successor(key_buffer: &mut Vec<u8>) {
    key_buffer.push(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{FixedBufferPool, PoolRole};
    use crate::id::TrxID;
    use crate::index::btree::{BTree, BTreeCompact, BTreeCompactConfig, BTreeU64, LookupChild};
    use crate::index::util::Maskable;
    use crate::quiescent::{QuiescentBox, QuiescentGuard};
    use std::sync::atomic::Ordering;

    struct ExactBoundaryResumeFixture {
        tree: BTree,
        left_branch_page_id: PageID,
        right_branch_page_id: PageID,
        left_leaf_page_id: PageID,
        first_right_leaf_page_id: PageID,
        second_right_leaf_page_id: PageID,
    }

    fn owned_index_pool(pool_size: usize) -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(FixedBufferPool::with_capacity(PoolRole::Index, pool_size).unwrap())
    }

    async fn build_exact_boundary_resume_fixture(
        pool: QuiescentGuard<FixedBufferPool>,
        pool_guard: &PoolGuard,
    ) -> ExactBoundaryResumeFixture {
        let tree = BTree::new(pool, pool_guard, false, TrxID::new(200))
            .await
            .expect("test btree construction should succeed");

        let mut left_leaf_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let left_leaf_page_id = left_leaf_guard.page_id();
        let left_leaf = left_leaf_guard.page_mut();
        left_leaf.init(
            0,
            TrxID::new(200),
            &[],
            BTreeU64::INVALID_VALUE,
            b"ab",
            false,
        );
        left_leaf.insert(b"aa", BTreeU64::from(1));
        drop(left_leaf_guard);

        let mut first_right_leaf_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let first_right_leaf_page_id = first_right_leaf_guard.page_id();
        let first_right_leaf = first_right_leaf_guard.page_mut();
        first_right_leaf.init(
            0,
            TrxID::new(200),
            b"ab",
            BTreeU64::INVALID_VALUE,
            b"ab\0",
            false,
        );
        first_right_leaf.insert(b"ab", BTreeU64::from(2));
        drop(first_right_leaf_guard);

        let mut second_right_leaf_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let second_right_leaf_page_id = second_right_leaf_guard.page_id();
        let second_right_leaf = second_right_leaf_guard.page_mut();
        second_right_leaf.init(
            0,
            TrxID::new(200),
            b"ab\0",
            BTreeU64::INVALID_VALUE,
            &[],
            false,
        );
        second_right_leaf.insert(b"ab\0", BTreeU64::from(3));
        drop(second_right_leaf_guard);

        let mut left_branch_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let left_branch_page_id = left_branch_guard.page_id();
        let left_branch = left_branch_guard.page_mut();
        left_branch.init(
            1,
            TrxID::new(200),
            &[],
            BTreeU64::from(left_leaf_page_id),
            b"ab",
            false,
        );
        drop(left_branch_guard);

        let mut right_branch_guard = tree
            .allocate_node(pool_guard)
            .await
            .expect("test node allocation should succeed");
        let right_branch_page_id = right_branch_guard.page_id();
        let right_branch = right_branch_guard.page_mut();
        right_branch.init(
            1,
            TrxID::new(200),
            b"ab",
            BTreeU64::INVALID_VALUE,
            &[],
            false,
        );
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
        root.init(
            2,
            TrxID::new(200),
            &[],
            BTreeU64::from(left_branch_page_id),
            &[],
            false,
        );
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
}
