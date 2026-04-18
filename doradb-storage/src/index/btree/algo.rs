//! Shared B+Tree node packing helpers.
//!
//! This module is intentionally narrow: it owns reusable node/fence packing
//! logic only. It does not know about latches, buffer-pool allocation, CoW file
//! writes, checkpoint publication, or DiskTree batch semantics.

use crate::error::{Error, Result};
use crate::index::btree::{
    BTREE_NODE_USABLE_SIZE, BTreeNode, BTreeU64, BTreeValue, PackedNodeSpace, SpaceEstimation,
};
use crate::trx::TrxID;

/// One sorted slot entry to be packed into a `BTreeNode`.
#[derive(Clone, Copy)]
pub(crate) struct PackedNodeEntry<'a, V> {
    /// Full encoded key bytes for the slot.
    pub(crate) key: &'a [u8],
    /// Value bytes associated with the slot.
    pub(crate) value: V,
}

/// Immutable parameters for one packed node build.
#[derive(Clone, Copy)]
pub(crate) struct PackedNodeParams<'a> {
    /// Height to write into the node header.
    pub(crate) height: u16,
    /// Timestamp to write into the node header.
    pub(crate) ts: TrxID,
    /// Inclusive lower fence key for this node.
    pub(crate) lower_fence: &'a [u8],
    /// Value stored beside the lower fence.
    pub(crate) lower_fence_value: BTreeU64,
    /// Minimum number of slot entries the node must consume.
    pub(crate) min_slots: usize,
    /// Whether search hints should be maintained for this node.
    pub(crate) hints_enabled: bool,
}

/// Result of packing one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PackedNodeResult<'a> {
    /// Number of slot entries inserted into the node.
    pub(crate) packed: usize,
    /// Final effective space reported by the initialized node.
    pub(crate) effective_space: usize,
    /// Exclusive upper fence chosen for the node, or `None` for rightmost.
    pub(crate) upper_fence: Option<&'a [u8]>,
}

/// Select the exclusive upper fence for a packed sibling.
///
/// `packed` is the number of slot entries assigned to the current node. When a
/// following entry remains, its key becomes this node's exclusive upper fence;
/// otherwise the node is rightmost and remains open-ended.
#[inline]
pub(crate) fn packed_sibling_upper_fence<'a, V>(
    entries: &'a [PackedNodeEntry<'a, V>],
    packed: usize,
) -> Option<&'a [u8]> {
    entries.get(packed).map(|entry| entry.key)
}

/// Initialize `node` and pack the largest valid prefix of sorted slot entries.
///
/// The upper fence is selected from the first unpacked entry's lower-fence key,
/// so capacity checks run against the same fence bytes that will be persisted.
/// `min_slots` protects callers such as branch packing that must consume at
/// least one child slot whenever a following sibling remains. The split point
/// is selected with a forward space estimator and the output node is initialized
/// only once after the final fence is known.
pub(crate) fn pack_sibling_node<'a, V>(
    node: &mut BTreeNode,
    params: PackedNodeParams<'a>,
    entries: &'a [PackedNodeEntry<'a, V>],
) -> Result<PackedNodeResult<'a>>
where
    V: BTreeValue + Copy,
{
    if params.min_slots > entries.len() {
        return Err(Error::InvalidArgument);
    }

    let rightmost_count = entries.len();
    if packed_node_fits::<V>(params.lower_fence, None, entries, rightmost_count) {
        return build_node(node, params, None, entries, rightmost_count);
    }

    if rightmost_count == 0 {
        return Err(Error::InvalidArgument);
    }

    let packed = select_finite_packed_count::<V>(params.lower_fence, params.min_slots, entries)?;
    let upper_fence = packed_sibling_upper_fence(entries, packed);
    build_node(node, params, upper_fence, entries, packed)
}

fn packed_node_fits<V>(
    lower_fence: &[u8],
    upper_fence: Option<&[u8]>,
    entries: &[PackedNodeEntry<'_, V>],
    count: usize,
) -> bool
where
    V: BTreeValue + Copy,
{
    let upper_fence = upper_fence.unwrap_or(&[]);
    if !fences_fit::<V>(lower_fence, upper_fence) {
        return false;
    }
    estimate_packed_node_space::<V>(lower_fence, upper_fence, &entries[..count])
        .is_some_and(|space| space.total_space() <= BTREE_NODE_USABLE_SIZE)
}

fn select_finite_packed_count<V>(
    lower_fence: &[u8],
    min_slots: usize,
    entries: &[PackedNodeEntry<'_, V>],
) -> Result<usize>
where
    V: BTreeValue + Copy,
{
    // Prefix length can decrease as the candidate upper fence moves right.
    // Recompute included-entry space only when that prefix changes; otherwise
    // advance the estimate by the newly included previous upper fence.
    let mut best = None;
    let mut active_prefix_len = None;
    let mut included_space = 0usize;
    let mut included_count = 0usize;

    for packed in min_slots..entries.len() {
        let upper_fence = entries[packed].key;
        let Some(space) = PackedNodeSpace::with_fences(lower_fence, upper_fence) else {
            continue;
        };
        if !fences_fit::<V>(lower_fence, upper_fence) {
            if space.prefix_is_inline() {
                break;
            }
            continue;
        }
        let prefix_len = space.prefix_len();
        if active_prefix_len != Some(prefix_len) || included_count > packed {
            included_space = entries[..packed]
                .iter()
                .try_fold(0usize, |total, entry| {
                    let entry_space = PackedNodeSpace::entry_space::<V>(entry.key, prefix_len)?;
                    total.checked_add(entry_space)
                })
                .ok_or(Error::InvalidArgument)?;
            active_prefix_len = Some(prefix_len);
            included_count = packed;
        } else {
            while included_count < packed {
                let entry_space =
                    PackedNodeSpace::entry_space::<V>(entries[included_count].key, prefix_len)
                        .ok_or(Error::InvalidArgument)?;
                included_space = included_space
                    .checked_add(entry_space)
                    .ok_or(Error::InvalidArgument)?;
                included_count += 1;
            }
        }

        let total_space = space
            .total_space()
            .checked_add(included_space)
            .ok_or(Error::InvalidArgument)?;
        if total_space <= BTREE_NODE_USABLE_SIZE {
            best = Some(packed);
        } else if space.prefix_is_inline() {
            break;
        }
    }

    best.ok_or(Error::InvalidArgument)
}

fn estimate_packed_node_space<V>(
    lower_fence: &[u8],
    upper_fence: &[u8],
    entries: &[PackedNodeEntry<'_, V>],
) -> Option<PackedNodeSpace>
where
    V: BTreeValue + Copy,
{
    let mut space = PackedNodeSpace::with_fences(lower_fence, upper_fence)?;
    for entry in entries {
        space.add_entry::<V>(entry.key)?;
    }
    Some(space)
}

fn build_node<'a, V>(
    node: &mut BTreeNode,
    params: PackedNodeParams<'a>,
    upper_fence: Option<&'a [u8]>,
    entries: &'a [PackedNodeEntry<'a, V>],
    count: usize,
) -> Result<PackedNodeResult<'a>>
where
    V: BTreeValue + Copy,
{
    if !fences_fit::<V>(params.lower_fence, upper_fence.unwrap_or(&[])) {
        return Err(Error::InvalidArgument);
    }
    node.init(
        params.height,
        params.ts,
        params.lower_fence,
        params.lower_fence_value,
        upper_fence.unwrap_or(&[]),
        params.hints_enabled,
    );
    for entry in &entries[..count] {
        if !node.can_insert::<V>(entry.key) {
            return Err(Error::InvalidArgument);
        }
        let idx = node.count();
        node.insert_at::<V>(idx, entry.key, entry.value);
    }
    node.update_hints();
    Ok(PackedNodeResult {
        packed: count,
        effective_space: node.effective_space(),
        upper_fence,
    })
}

fn fences_fit<V: BTreeValue>(lower_fence: &[u8], upper_fence: &[u8]) -> bool {
    if lower_fence.len() > u16::MAX as usize || upper_fence.len() > u16::MAX as usize {
        return false;
    }
    SpaceEstimation::with_fences(lower_fence, upper_fence, V::ENCODED_LEN).total_space()
        <= BTREE_NODE_USABLE_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::btree::{BTREE_NODE_USABLE_SIZE, BTreeNil, BTreeNodeBox};
    use crate::index::util::Maskable;

    fn scratch_node_fits<V>(
        lower_fence: &[u8],
        upper_fence: Option<&[u8]>,
        entries: &[PackedNodeEntry<'_, V>],
        count: usize,
    ) -> bool
    where
        V: BTreeValue + Copy,
    {
        let upper_fence = upper_fence.unwrap_or(&[]);
        if !fences_fit::<V>(lower_fence, upper_fence) {
            return false;
        }
        let mut node = BTreeNodeBox::alloc(
            0,
            1,
            lower_fence,
            BTreeU64::INVALID_VALUE,
            upper_fence,
            false,
        );
        entries[..count].iter().all(|entry| {
            if !node.can_insert::<V>(entry.key) {
                return false;
            }
            let idx = node.count();
            node.insert_at::<V>(idx, entry.key, entry.value);
            true
        })
    }

    fn assert_estimate_matches_scratch<V>(
        lower_fence: &[u8],
        upper_fence: Option<&[u8]>,
        entries: &[PackedNodeEntry<'_, V>],
        count: usize,
    ) where
        V: BTreeValue + Copy,
    {
        assert_eq!(
            packed_node_fits::<V>(lower_fence, upper_fence, entries, count),
            scratch_node_fits::<V>(lower_fence, upper_fence, entries, count)
        );
    }

    #[test]
    fn test_packed_sibling_upper_fence_selects_next_lower_fence() {
        let entries = [
            PackedNodeEntry {
                key: b"abc1",
                value: BTreeNil,
            },
            PackedNodeEntry {
                key: b"abc2",
                value: BTreeNil,
            },
        ];

        assert_eq!(
            packed_sibling_upper_fence(&entries, 1),
            Some(b"abc2".as_slice())
        );
        assert_eq!(packed_sibling_upper_fence(&entries, 2), None);
    }

    #[test]
    fn test_packed_node_estimate_matches_scratch_node() {
        let u64_entries = [
            PackedNodeEntry {
                key: b"prefix-0001",
                value: BTreeU64::from(1),
            },
            PackedNodeEntry {
                key: b"prefix-0002",
                value: BTreeU64::from(2),
            },
            PackedNodeEntry {
                key: b"prefix-0003",
                value: BTreeU64::from(3),
            },
        ];
        assert_estimate_matches_scratch::<BTreeU64>(
            b"prefix-0001",
            None,
            &u64_entries,
            u64_entries.len(),
        );
        assert_estimate_matches_scratch::<BTreeU64>(
            b"prefix-0001",
            Some(b"prefix-0004"),
            &u64_entries,
            2,
        );

        let nil_entries = [
            PackedNodeEntry {
                key: b"nonunique-key-00000001",
                value: BTreeNil,
            },
            PackedNodeEntry {
                key: b"nonunique-key-00000002",
                value: BTreeNil,
            },
        ];
        assert_estimate_matches_scratch::<BTreeNil>(
            b"nonunique-key-00000001",
            Some(b"nonunique-key-00000003"),
            &nil_entries,
            nil_entries.len(),
        );
    }

    #[test]
    fn test_packed_node_estimate_tracks_prefix_shrink() {
        let lower_fence = b"shared-prefix-long-0001";
        let long_prefix_upper = b"shared-prefix-long-9999";
        let inline_prefix_upper = b"shared-split";
        let long_prefix_space =
            PackedNodeSpace::with_fences(lower_fence, long_prefix_upper).unwrap();
        let inline_prefix_space =
            PackedNodeSpace::with_fences(lower_fence, inline_prefix_upper).unwrap();

        assert!(!long_prefix_space.prefix_is_inline());
        assert!(inline_prefix_space.prefix_is_inline());

        let entries = [
            PackedNodeEntry {
                key: b"shared-prefix-long-0001",
                value: BTreeU64::from(1),
            },
            PackedNodeEntry {
                key: b"shared-prefix-long-0002",
                value: BTreeU64::from(2),
            },
        ];
        assert_estimate_matches_scratch::<BTreeU64>(
            lower_fence,
            Some(long_prefix_upper),
            &entries,
            entries.len(),
        );
        assert_estimate_matches_scratch::<BTreeU64>(
            lower_fence,
            Some(inline_prefix_upper),
            &entries,
            entries.len(),
        );
    }

    #[test]
    fn test_pack_sibling_node_uses_finite_fence_common_prefix() {
        let entries = [
            PackedNodeEntry {
                key: b"prefix-0001",
                value: BTreeU64::from(1),
            },
            PackedNodeEntry {
                key: b"prefix-0002",
                value: BTreeU64::from(2),
            },
            PackedNodeEntry {
                key: b"prefix-0003",
                value: BTreeU64::from(3),
            },
        ];
        let mut node =
            BTreeNodeBox::alloc(0, 9, b"prefix-0001", BTreeU64::INVALID_VALUE, &[], false);

        let result = pack_sibling_node(
            &mut node,
            PackedNodeParams {
                height: 0,
                ts: 9,
                lower_fence: b"prefix-0001",
                lower_fence_value: BTreeU64::INVALID_VALUE,
                min_slots: 1,
                hints_enabled: false,
            },
            &entries,
        )
        .unwrap();

        assert_eq!(result.packed, entries.len());
        assert_eq!(result.upper_fence, None);
        assert!(node.has_no_upper_fence());

        let mut finite_node =
            BTreeNodeBox::alloc(0, 9, b"prefix-0001", BTreeU64::INVALID_VALUE, &[], false);
        let result = build_node(
            &mut finite_node,
            PackedNodeParams {
                height: 0,
                ts: 9,
                lower_fence: b"prefix-0001",
                lower_fence_value: BTreeU64::INVALID_VALUE,
                min_slots: 1,
                hints_enabled: false,
            },
            Some(b"prefix-0004"),
            &entries[..2],
            2,
        )
        .unwrap();

        assert_eq!(result.upper_fence, Some(b"prefix-0004".as_slice()));
        assert_eq!(finite_node.common_prefix(), b"prefix-000");
    }

    #[test]
    fn test_pack_sibling_node_accounts_for_upper_fence_capacity() {
        let long_key = vec![b'x'; BTREE_NODE_USABLE_SIZE];
        let entries = [PackedNodeEntry {
            key: long_key.as_slice(),
            value: BTreeNil,
        }];
        let mut node = BTreeNodeBox::alloc(0, 1, &[], BTreeU64::INVALID_VALUE, &[], false);

        let err = pack_sibling_node(
            &mut node,
            PackedNodeParams {
                height: 0,
                ts: 1,
                lower_fence: &long_key,
                lower_fence_value: BTreeU64::INVALID_VALUE,
                min_slots: 1,
                hints_enabled: false,
            },
            &entries,
        )
        .unwrap_err();

        assert!(matches!(err, Error::InvalidArgument));
    }
}
