//! Exact, uncompressed content and reachable geometry of one published DiskTree.
use crate::error::{DataIntegrityError, DataIntegrityResult, RuntimeError, RuntimeResult};
use error_stack::Report;

#[cfg(test)]
pub(crate) use tests::{root_descriptor, verify_root};

/// Required root facts, independent of profiling and physical write counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DiskTreeStats {
    /// Physical leaf entries, including the RowID identity of nonunique keys.
    pub(crate) entries: u64,
    /// Full comparison-encoded leaf keys before page prefix compression.
    pub(crate) encoded_key_bytes: u64,
    /// Reachable leaf nodes.
    pub(crate) leaf_nodes: u64,
    /// Reachable branch nodes.
    pub(crate) branch_nodes: u64,
    /// Final root height; empty trees and leaf roots both use zero.
    pub(crate) height: u16,
}

impl DiskTreeStats {
    /// Validate local present-root invariants against allocation capacity.
    pub(crate) fn validate(self, capacity: u64) -> DataIntegrityResult<()> {
        let nodes = self.leaf_nodes.checked_add(self.branch_nodes);
        if self.entries == 0
            || self.leaf_nodes == 0
            || self.entries < self.leaf_nodes
            || (self.height == 0 && (self.leaf_nodes != 1 || self.branch_nodes != 0))
            || (self.height > 0 && self.branch_nodes < u64::from(self.height))
            || nodes.is_none_or(|nodes| nodes > capacity)
        {
            return Err(
                Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "invalid secondary root statistics: {self:?}, capacity={capacity}"
                )),
            );
        }
        Ok(())
    }

    /// Count one resolved leaf identity exactly once, before packing or repair.
    /// Candidate-local totals are bounded by input admission and table-file geometry.
    #[inline]
    pub(crate) fn add_entry(&mut self, key_bytes: usize) {
        self.entries += 1;
        self.encoded_key_bytes += key_bytes as u64;
    }

    /// Count a constructed or retired node by its actual height.
    /// Candidate-local counts are bounded by file capacity and allocation ownership.
    #[inline]
    pub(crate) fn add_node(&mut self, height: usize) {
        let count = if height == 0 {
            &mut self.leaf_nodes
        } else {
            &mut self.branch_nodes
        };
        *count += 1;
    }

    /// Combine disjoint content and geometry counters; height is assigned at completion.
    /// Both operands are bounded candidate-local totals, not persisted root statistics.
    #[inline]
    pub(crate) fn accumulate(&mut self, other: Self) {
        self.entries += other.entries;
        self.encoded_key_bytes += other.encoded_key_bytes;
        self.leaf_nodes += other.leaf_nodes;
        self.branch_nodes += other.branch_nodes;
    }

    /// Apply candidate-local retirement/removal and construction/insertion totals.
    pub(crate) fn apply_delta(
        self,
        removed: Self,
        added: Self,
        height: u16,
    ) -> RuntimeResult<Self> {
        Ok(Self {
            entries: checked_delta(self.entries, removed.entries, added.entries)?,
            encoded_key_bytes: checked_delta(
                self.encoded_key_bytes,
                removed.encoded_key_bytes,
                added.encoded_key_bytes,
            )?,
            leaf_nodes: checked_delta(self.leaf_nodes, removed.leaf_nodes, added.leaf_nodes)?,
            branch_nodes: checked_delta(
                self.branch_nodes,
                removed.branch_nodes,
                added.branch_nodes,
            )?,
            height,
        })
    }
}

fn checked_delta(old: u64, removed: u64, added: u64) -> RuntimeResult<u64> {
    old.checked_sub(removed)
        .and_then(|n| n.checked_add(added))
        .ok_or_else(|| {
            Report::new(RuntimeError::IndexAccess).attach(format!(
                "DiskTree statistics overflow: old={old}, removed={removed}, added={added}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SecondaryIndexRoot;
    use crate::error::RuntimeOrFatalResult;
    use crate::id::BlockID;
    use crate::index::disk_tree::{DiskTree, DiskTreeSpec};

    /// Independent full traversal for fixture input and exact output assertions only.
    pub(crate) async fn root_descriptor<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
    ) -> RuntimeOrFatalResult<SecondaryIndexRoot> {
        let root = tree.snapshot().1;
        let mut stats = DiskTreeStats::default();
        let mut pending: Vec<_> = root.into_iter().collect();
        while let Some(block) = pending.pop() {
            let guard = tree.read_node(block).await?;
            let node = guard.view();
            if Some(block) == root {
                stats.height = node.height() as u16;
            }
            if node.is_leaf() {
                stats.leaf_nodes += 1;
                stats.entries += node.count() as u64;
                for slot in 0..node.count() {
                    stats.encoded_key_bytes += node.leaf_entry(slot)?.key.len() as u64;
                }
            } else {
                stats.branch_nodes += 1;
                for slot in 0..=node.count() {
                    pending.push(node.child_block(slot)?);
                }
            }
        }
        SecondaryIndexRoot::from_parts(root, stats)
    }

    /// Check a completed descriptor against physical contents before projecting its block.
    pub(crate) async fn verify_root<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
        root: SecondaryIndexRoot,
    ) -> RuntimeOrFatalResult<Option<BlockID>> {
        let (runtime, _, guard) = tree.snapshot();
        assert_eq!(
            root,
            root_descriptor(&runtime.open(root.block_id(), &guard)).await?,
            "constructed statistics differ from independent tree traversal"
        );
        Ok(root.block_id())
    }

    /// Purpose: Reject impossible local geometry and final accounting overflow before root publication or recovery.
    /// Expected: Leaf and unary-branch roots validate within capacity; malformed counts and final deltas fail with typed errors.
    #[test]
    fn required_statistics_validate_geometry_and_arithmetic() {
        let leaf = DiskTreeStats {
            entries: 3,
            encoded_key_bytes: 17,
            leaf_nodes: 1,
            branch_nodes: 0,
            height: 0,
        };
        let tall = DiskTreeStats {
            branch_nodes: 3,
            height: 3,
            ..leaf
        };
        assert!(leaf.validate(1).is_ok());
        assert!(tall.validate(4).is_ok());
        for stats in [
            DiskTreeStats::default(),
            DiskTreeStats { entries: 0, ..leaf },
            DiskTreeStats {
                leaf_nodes: 0,
                ..leaf
            },
            DiskTreeStats {
                entries: 1,
                leaf_nodes: 2,
                branch_nodes: 1,
                height: 1,
                ..leaf
            },
            DiskTreeStats {
                leaf_nodes: 2,
                ..leaf
            },
            DiskTreeStats {
                branch_nodes: 1,
                ..leaf
            },
            DiskTreeStats {
                branch_nodes: 2,
                ..tall
            },
            DiskTreeStats {
                entries: u64::MAX,
                leaf_nodes: u64::MAX,
                branch_nodes: 1,
                height: 1,
                ..leaf
            },
        ] {
            assert!(stats.validate(u64::MAX).is_err(), "{stats:?}");
        }
        assert!(tall.validate(3).is_err());
        assert!(
            leaf.apply_delta(
                DiskTreeStats {
                    entries: 4,
                    ..DiskTreeStats::default()
                },
                DiskTreeStats::default(),
                0
            )
            .is_err()
        );
        for (old, added) in [
            (
                DiskTreeStats {
                    entries: u64::MAX,
                    ..leaf
                },
                DiskTreeStats {
                    entries: 1,
                    ..DiskTreeStats::default()
                },
            ),
            (
                DiskTreeStats {
                    encoded_key_bytes: u64::MAX,
                    ..leaf
                },
                DiskTreeStats {
                    encoded_key_bytes: 1,
                    ..DiskTreeStats::default()
                },
            ),
            (
                DiskTreeStats {
                    leaf_nodes: u64::MAX,
                    ..leaf
                },
                DiskTreeStats {
                    leaf_nodes: 1,
                    ..DiskTreeStats::default()
                },
            ),
            (
                DiskTreeStats {
                    branch_nodes: u64::MAX,
                    ..leaf
                },
                DiskTreeStats {
                    branch_nodes: 1,
                    ..DiskTreeStats::default()
                },
            ),
        ] {
            assert!(
                old.apply_delta(DiskTreeStats::default(), added, 0).is_err(),
                "final statistics overflow: old={old:?}, added={added:?}"
            );
        }
        let removed = DiskTreeStats {
            entries: 2,
            encoded_key_bytes: 10,
            ..DiskTreeStats::default()
        };
        let added = DiskTreeStats {
            entries: 1,
            encoded_key_bytes: 8,
            branch_nodes: 2,
            ..DiskTreeStats::default()
        };
        assert_eq!(
            leaf.apply_delta(removed, added, 2).unwrap(),
            DiskTreeStats {
                entries: 2,
                encoded_key_bytes: 15,
                leaf_nodes: 1,
                branch_nodes: 2,
                height: 2
            }
        );
        assert!(SecondaryIndexRoot::from_parts(None, leaf).is_err());
        assert!(SecondaryIndexRoot::from_parts(Some(BlockID::new(0)), leaf).is_err());
    }
}
