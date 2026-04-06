use crate::buffer::guard::PageGuard;
use crate::buffer::{BufferPool, PoolGuard};
use crate::error::Result;
use crate::index::btree::{BTreeNodeCursor, GenericBTree};
use crate::index::btree_node::{BTreeNode, BTreeSlot};
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
    /// Returns true if the scan should continue. Otherwise, stop.
    fn apply(&mut self, node: &BTreeNode, slot: &BTreeSlot) -> bool;
}

/// Convenient blank implemtation of support scan callback.
impl<F> BTreeSlotCallback for F
where
    F: FnMut(&BTreeNode, &BTreeSlot) -> bool,
{
    #[inline]
    fn apply(&mut self, node: &BTreeNode, slot: &BTreeSlot) -> bool {
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
                let res = self.callback.apply(first_node, slot);
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
                let res = self.callback.apply(first_node, slot);
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
                    let res = self.callback.apply(node, slot);
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
    use crate::index::btree::BTree;
    use crate::index::btree_value::BTreeU64;
    use crate::quiescent::QuiescentBox;
    use rand::prelude::IndexedRandom;
    use std::collections::HashMap;

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
        const COUNT: usize = 100000;
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, 64 * 1024 * 1024)
                    .unwrap(),
            );
            {
                // generate random data
                let pool_guard = (*pool).pool_guard();
                let tree = BTree::new(pool.guard(), &pool_guard, true, 200)
                    .await
                    .expect("test btree construction should succeed");
                let mut data = Vec::with_capacity(COUNT);
                let mut rng = rand::rng();
                for i in 0..COUNT {
                    let mut elem = Vec::with_capacity(8);
                    for _ in 0..4 {
                        let b = ALPHABETA[..].choose(&mut rng).unwrap();
                        elem.push(*b);
                    }
                    let ib = (i as u32).to_be_bytes();
                    elem.extend_from_slice(&ib);
                    data.push(elem);
                }
                // statistics of elements.
                let mut map: HashMap<u8, usize> = HashMap::new();
                for (idx, elem) in data.iter().enumerate() {
                    *map.entry(elem[0]).or_default() += 1;
                    let res = tree
                        .insert(&pool_guard, elem, BTreeU64::from(idx as u64), false, 210)
                        .await;
                    assert!(res.is_ok());
                }
                let mut scanner = tree.prefix_scanner(&pool_guard, Count(0));
                for b in ALPHABETA {
                    let k = std::slice::from_ref(b);
                    scanner.reset();
                    scanner.scan_prefix(k).await.unwrap();
                    println!("prefix={}, count={}", *b as char, scanner.count());
                    assert_eq!(map[b], scanner.count());
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
        fn apply(&mut self, _: &BTreeNode, _: &BTreeSlot) -> bool {
            self.0 += 1;
            true
        }
    }
}
