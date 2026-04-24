use crate::buffer::guard::PageGuard;
use crate::buffer::{BufferPool, PoolGuard};
use crate::error::Result;
use crate::index::btree::{BTreeNode, BTreeSlot};
use crate::index::btree::{BTreeNodeCursor, GenericBTree};
use std::ops::{Deref, DerefMut};

/// Abstraction on processing B-Tree slot.
/// Caller can extract key/value from slot and embed own
/// logic inside the callback.
/// This callback is guaranteed to be applied on valid
/// slot data.
pub trait BTreeSlotCallback {
    /// Process a slot.
    /// Note: this method is invoked within a read lock on leaf node,
    /// so it's not suitable to perform blocking operations here and
    /// caller should prevent dead-lock(recursively searching on the
    /// same tree should be avoided).
    /// Returns `Ok(true)` if the scan should continue, `Ok(false)` if it
    /// should stop cleanly, or `Err` if slot processing fails.
    fn apply(&mut self, node: &BTreeNode, slot: &BTreeSlot) -> Result<bool>;
}

/// Convenient blank implemtation of support scan callback.
impl<F> BTreeSlotCallback for F
where
    F: FnMut(&BTreeNode, &BTreeSlot) -> Result<bool>,
{
    #[inline]
    fn apply(&mut self, node: &BTreeNode, slot: &BTreeSlot) -> Result<bool> {
        self(node, slot)
    }
}

/// Scan on B-tree with specific prefix.
pub struct BTreePrefixScan<'a, C, P: 'static> {
    cursor: BTreeNodeCursor<'a, P>,
    callback: C,
}

impl<C, P: BufferPool> Deref for BTreePrefixScan<'_, C, P> {
    type Target = C;
    #[inline]
    fn deref(&self) -> &C {
        &self.callback
    }
}

impl<C, P: BufferPool> DerefMut for BTreePrefixScan<'_, C, P> {
    #[inline]
    fn deref_mut(&mut self) -> &mut C {
        &mut self.callback
    }
}

impl<C, P: BufferPool> BTreePrefixScan<'_, C, P> {
    /// Consume the scanner and return the callback state.
    #[inline]
    pub fn into_callback(self) -> C {
        self.callback
    }
}

impl<'a, C: BTreeSlotCallback, P: BufferPool> BTreePrefixScan<'a, C, P> {
    #[inline]
    pub(super) fn new(tree: &'a GenericBTree<P>, pool_guard: &'a PoolGuard, callback: C) -> Self {
        BTreePrefixScan {
            cursor: BTreeNodeCursor::new(tree, pool_guard, 0),
            callback,
        }
    }

    #[inline]
    pub async fn scan_prefix(&mut self, key: &[u8]) -> Result<()> {
        // find first leaf node of prefix key.
        self.cursor.seek(key).await?;
        let Some(first_g) = self.cursor.next().await? else {
            return Ok(());
        };
        let first_node = first_g.page();
        let start_idx = match first_node.search_key(key) {
            Ok(idx) => {
                let slot = first_node.slot(idx);
                let res = self.callback.apply(first_node, slot)?;
                if !res {
                    return Ok(());
                }
                idx + 1
            }
            Err(idx) => idx,
        };
        let k = &key[first_node.prefix_len()..];
        for idx in start_idx..first_node.count() {
            let slot = first_node.slot(idx);
            if first_node.slot_matches_k(slot, k) {
                let res = self.callback.apply(first_node, slot)?;
                if !res {
                    return Ok(());
                }
            } else {
                return Ok(()); // mismatch
            }
        }
        // first node exhausted
        // check if prefix matches upper fence.
        if first_node.has_no_upper_fence() {
            return Ok(()); // tree exhausted.
        }
        if !first_node.slot_matches_k(first_node.upper_fence_slot(), k) {
            return Ok(()); // mismatch
        }
        drop(first_g); // release lock on first node.
        // try next node.
        while let Some(g) = self.cursor.next().await? {
            let node = g.page();
            // As node changes, common prefix may also change.
            if key.len() < node.prefix_len() {
                return Ok(()); // mismatch
            }
            if node.common_prefix() != &key[..node.prefix_len()] {
                return Ok(()); // mismatch
            }
            let k = &key[node.prefix_len()..];
            for slot in node.slots() {
                if node.slot_matches_k(slot, k) {
                    let res = self.callback.apply(node, slot)?;
                    if !res {
                        return Ok(());
                    }
                } else {
                    return Ok(()); // mismatch
                }
            }
            if node.has_no_upper_fence() {
                return Ok(()); // tree exhausted.
            }
            if !node.slot_matches_k(node.upper_fence_slot(), k) {
                return Ok(()); // mismtach
            }
        }
        // tree exhausted.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::FixedBufferPool;
    use crate::error::Error;
    use crate::index::btree::BTree;
    use crate::index::btree::BTreeU64;
    use crate::quiescent::QuiescentBox;

    #[test]
    fn test_btree_scan_single_node() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, 64 * 1024 * 1024)
                    .unwrap(),
            );
            {
                let pool_guard = (*pool).pool_guard();
                let tree = BTree::new(pool.guard(), &pool_guard, true, 200)
                    .await
                    .expect("test btree construction should succeed");
                let keys = vec![
                    "a", "b", "c", "d", "aa", "bb", "cc", "dd", "aaa", "bbb", "ccc", "ddd",
                ];
                for (idx, k) in keys.iter().enumerate() {
                    let res = tree
                        .insert(
                            &pool_guard,
                            k.as_bytes(),
                            BTreeU64::from(idx as u64),
                            false,
                            100,
                        )
                        .await;
                    assert!(res.is_ok());
                }
                let mut scanner = tree.prefix_scanner(&pool_guard, Count(0));
                scanner.scan_prefix(b"a").await.unwrap();
                assert!(scanner.count() == 3);

                scanner.reset();
                scanner.scan_prefix(b"e").await.unwrap();
                assert!(scanner.count() == 0);

                scanner.reset();
                scanner.scan_prefix(b"bb").await.unwrap();
                assert!(scanner.count() == 2);

                let res = tree
                    .mark_as_deleted(&pool_guard, b"a", BTreeU64::from(0u64), 101)
                    .await;
                assert!(res.is_ok());
                scanner.reset();
                scanner.scan_prefix(b"a").await.unwrap();
                // because the counter does not check delete flag, we still get 3 keys.
                assert!(scanner.count() == 3);

                let res = tree
                    .delete(&pool_guard, b"a", BTreeU64::from(0u64), false, 102)
                    .await;
                assert!(res.is_ok()); // actual deletion.
                scanner.reset();
                scanner.scan_prefix(b"a").await.unwrap();
                assert!(scanner.count() == 2);
            }
        })
    }

    #[test]
    fn test_btree_scan_multi_nodes() {
        const ALPHABETA: &[u8; 26] = b"abcdefghijklmnopqrstuvwxyz";
        const ENTRIES_PER_PREFIX: usize = 128;
        const KEY_LEN: usize = 1000;
        const PREFIX_LEN: usize = 992;
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, 64 * 1024 * 1024)
                    .unwrap(),
            );
            {
                let pool_guard = (*pool).pool_guard();
                let tree = BTree::new(pool.guard(), &pool_guard, true, 200)
                    .await
                    .expect("test btree construction should succeed");

                for (prefix_idx, prefix) in ALPHABETA.iter().enumerate() {
                    for entry_idx in 0..ENTRIES_PER_PREFIX {
                        let idx = prefix_idx * ENTRIES_PER_PREFIX + entry_idx;
                        let mut elem = [0u8; KEY_LEN];
                        elem[..PREFIX_LEN].fill(b'x');
                        elem[0] = *prefix;
                        elem[PREFIX_LEN] = entry_idx as u8;
                        elem[KEY_LEN - std::mem::size_of::<u32>()..]
                            .copy_from_slice(&(entry_idx as u32).to_be_bytes());
                        let res = tree
                            .insert(&pool_guard, &elem, BTreeU64::from(idx as u64), false, 210)
                            .await;
                        assert!(res.is_ok());
                    }
                }
                assert!(tree.height() > 0);

                let mut scanner = tree.prefix_scanner(&pool_guard, Count(0));
                for b in ALPHABETA {
                    let mut k = [b'x'; PREFIX_LEN];
                    k[0] = *b;
                    scanner.reset();
                    scanner.scan_prefix(&k).await.unwrap();
                    assert_eq!(ENTRIES_PER_PREFIX, scanner.count());
                }
            }
        })
    }

    struct Count(usize);

    impl Count {
        #[inline]
        fn reset(&mut self) {
            self.0 = 0;
        }

        #[inline]
        fn count(&self) -> usize {
            self.0
        }
    }

    impl BTreeSlotCallback for Count {
        #[inline]
        fn apply(&mut self, _: &BTreeNode, _: &BTreeSlot) -> Result<bool> {
            self.0 += 1;
            Ok(true)
        }
    }

    struct FailOnFirstSlot;

    impl BTreeSlotCallback for FailOnFirstSlot {
        #[inline]
        fn apply(&mut self, _: &BTreeNode, _: &BTreeSlot) -> Result<bool> {
            Err(Error::invalid_state())
        }
    }

    #[test]
    fn test_btree_scan_propagates_callback_error() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, 64 * 1024 * 1024)
                    .unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let tree = BTree::new(pool.guard(), &pool_guard, true, 200)
                .await
                .expect("test btree construction should succeed");
            tree.insert(&pool_guard, b"a", BTreeU64::from(1), false, 100)
                .await
                .expect("test btree insert should succeed");

            let mut scanner = tree.prefix_scanner(&pool_guard, FailOnFirstSlot);
            let err = scanner
                .scan_prefix(b"a")
                .await
                .expect_err("callback error should abort the scan");
            assert!(err.is_code(crate::error::ErrorCode::InvalidState));
        });
    }
}
