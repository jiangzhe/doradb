//! Shared B+Tree node packing helpers.
//!
//! This module is intentionally narrow: it owns reusable node/fence packing
//! logic only. It does not know about latches, buffer-pool allocation, CoW file
//! writes, checkpoint publication, or DiskTree batch semantics.

use crate::error::{Error, Result};
use crate::index::btree::{
    BTREE_NODE_USABLE_SIZE, BTreeNode, BTreeNodeBox, BTreeU64, BTreeValue, SpaceEstimation,
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
/// least one child slot whenever a following sibling remains.
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
    if node_fits(params, None, entries, rightmost_count) {
        return build_node(node, params, None, entries, rightmost_count);
    }

    if rightmost_count == 0 {
        return Err(Error::InvalidArgument);
    }

    let mut lo = params.min_slots;
    let mut hi = rightmost_count - 1;
    let mut best = None;
    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let upper_fence = packed_sibling_upper_fence(entries, mid);
        if node_fits(params, upper_fence, entries, mid) {
            best = Some(mid);
            lo = mid + 1;
        } else if mid == 0 {
            break;
        } else {
            hi = mid - 1;
        }
    }

    let packed = best.ok_or(Error::InvalidArgument)?;
    let upper_fence = packed_sibling_upper_fence(entries, packed);
    build_node(node, params, upper_fence, entries, packed)
}

fn node_fits<V>(
    params: PackedNodeParams<'_>,
    upper_fence: Option<&[u8]>,
    entries: &[PackedNodeEntry<'_, V>],
    count: usize,
) -> bool
where
    V: BTreeValue + Copy,
{
    let upper_fence = upper_fence.unwrap_or(&[]);
    if !fences_fit::<V>(params.lower_fence, upper_fence) {
        return false;
    }
    let mut node = BTreeNodeBox::alloc(
        params.height,
        params.ts,
        params.lower_fence,
        params.lower_fence_value,
        upper_fence,
        params.hints_enabled,
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
    use crate::index::btree::{BTREE_NODE_USABLE_SIZE, BTreeNil};
    use crate::index::util::Maskable;

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
