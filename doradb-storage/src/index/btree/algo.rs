//! Shared B+Tree node packing helpers.
//!
//! This module is intentionally narrow: it owns reusable node/fence packing
//! logic only. It does not know about latches, buffer-pool allocation, CoW file
//! writes, checkpoint publication, or DiskTree batch semantics.

use crate::error::{Error, Result};
use crate::index::btree::{
    BTREE_NODE_USABLE_SIZE, BTreeNode, BTreeNodeBox, BTreeU64, BTreeValue, PackedNodeSpace,
    SpaceEstimation,
};
use crate::trx::TrxID;
use std::ops::Range;

/// One sorted slot entry to be packed into a `BTreeNode`.
#[derive(Clone, Copy)]
pub(crate) struct PackedNodeEntry<'a, V> {
    /// Full encoded key bytes for the slot.
    pub(crate) key: &'a [u8],
    /// Value bytes associated with the slot.
    pub(crate) value: V,
}

/// Immutable parameters for planning one packed sibling node.
///
/// The planner only needs the lower fence and minimum slot count. Callers that
/// materialize the plan pass full node header fields through
/// `KnownFenceNodeParams` after the final upper fence is known.
#[derive(Clone, Copy)]
pub(crate) struct PackedNodePlanParams<'a> {
    /// Inclusive lower fence key for this node.
    pub(crate) lower_fence: &'a [u8],
    /// Minimum number of slot entries the node must consume.
    pub(crate) min_slots: usize,
}

/// Immutable parameters for rebuilding a node from a known source range.
///
/// MemTree split and compaction code already chooses separator keys, fences,
/// lower-fence values, and source slot ranges. This parameter object captures
/// those fixed decisions so shared helpers can own the mechanical
/// `init + extend_slots_from + update_hints` sequence without changing split or
/// merge policy.
#[derive(Clone, Copy)]
pub(crate) struct KnownFenceNodeParams<'a> {
    /// Height to write into the node header.
    pub(crate) height: u16,
    /// Timestamp to write into the node header.
    pub(crate) ts: TrxID,
    /// Inclusive lower fence key for this node.
    pub(crate) lower_fence: &'a [u8],
    /// Value stored beside the lower fence.
    pub(crate) lower_fence_value: BTreeU64,
    /// Exclusive upper fence, or `None` for an open-ended rightmost node.
    pub(crate) upper_fence: Option<&'a [u8]>,
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

/// Result of planning one packed sibling without mutating a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PackedNodePlan<'a> {
    /// Number of slot entries the planned node consumes.
    pub(crate) packed: usize,
    /// Exclusive upper fence chosen for the node, or `None` for rightmost.
    pub(crate) upper_fence: Option<&'a [u8]>,
}

/// Source slots copied into a rebuilt node.
#[derive(Clone)]
pub(crate) struct NodeSlotRange<'a> {
    /// Source node that owns the slot range.
    pub(crate) node: &'a BTreeNode,
    /// Half-open slot range within `node`.
    pub(crate) range: Range<usize>,
}

/// Conservative MemTree sibling-merge plan.
///
/// This mirrors the online compactor's historical `SpaceEstimation` behavior:
/// the left node is the anchor, the immediate right sibling is the donor, and
/// `target_effective_space` is the caller's high-space threshold. It does not
/// use the exact packed estimator because that would change MemTree split/merge
/// count selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemTreeSiblingMergePlan {
    /// The merged fence pair plus existing left slots cannot fit.
    FenceOutOfSpace,
    /// No right slot can be moved into the left anchor.
    NoProgress,
    /// The whole right sibling can be absorbed by the left anchor.
    Full,
    /// The left anchor can absorb this many right-side slots.
    Partial { right_count: usize },
}

/// Select the exclusive upper fence for a packed sibling.
///
/// `packed` is the number of slot entries assigned to the current node. When a
/// following entry remains, its key becomes this node's exclusive upper fence;
/// otherwise the node is rightmost and remains open-ended.
#[inline]
fn packed_sibling_upper_fence<'a, V>(
    entries: &'a [PackedNodeEntry<'a, V>],
    packed: usize,
) -> Option<&'a [u8]> {
    entries.get(packed).map(|entry| entry.key)
}

/// Plan the largest valid packed prefix of sorted sibling entries.
///
/// DiskTree rewrite uses this as a dry-run before allocating replacement blocks:
/// candidate right siblings are only accepted when the final packed block count
/// stays within the rewrite's write budget. The count and fence selection are
/// used later by `pack_fixed_entries`, so planning and materialization run
/// against the same lower/upper fence pair.
pub(crate) fn plan_sibling_node<'a, V>(
    params: PackedNodePlanParams<'a>,
    entries: &'a [PackedNodeEntry<'a, V>],
) -> Result<PackedNodePlan<'a>>
where
    V: BTreeValue + Copy,
{
    if params.min_slots > entries.len() {
        return Err(Error::InvalidArgument);
    }

    let rightmost_count = entries.len();
    if let Some(space) = packed_node_space::<V>(params.lower_fence, None, entries, rightmost_count)
        && space.total_space() <= BTREE_NODE_USABLE_SIZE
    {
        return Ok(PackedNodePlan {
            packed: rightmost_count,
            upper_fence: None,
        });
    }

    if rightmost_count == 0 {
        return Err(Error::InvalidArgument);
    }

    let packed = select_finite_packed_count::<V>(params.lower_fence, params.min_slots, entries)?;
    let upper_fence = packed_sibling_upper_fence(entries, packed);
    let space = packed_node_space::<V>(params.lower_fence, upper_fence, entries, packed)
        .ok_or(Error::InvalidArgument)?;
    if space.total_space() > BTREE_NODE_USABLE_SIZE {
        return Err(Error::InvalidArgument);
    }
    Ok(PackedNodePlan {
        packed,
        upper_fence,
    })
}

/// Rebuild `dst` from a fixed range of slots in `src`.
///
/// The caller owns separator/range selection. This helper is intentionally
/// mechanical: initialize `dst` with fixed fences, copy the selected slots by
/// calling `BTreeNode::extend_slots_from`, and refresh persisted hints.
pub(crate) fn pack_node_range_into<'a, V>(
    dst: &mut BTreeNode,
    src: &BTreeNode,
    params: KnownFenceNodeParams<'a>,
    range: Range<usize>,
) -> PackedNodeResult<'a>
where
    V: BTreeValue,
{
    dst.init(
        params.height,
        params.ts,
        params.lower_fence,
        params.lower_fence_value,
        params.upper_fence.unwrap_or(&[]),
        params.hints_enabled,
    );
    finish_node_range::<V>(dst, src, params.upper_fence, range)
}

/// Allocate a boxed node and rebuild it from a fixed range of slots in `src`.
///
/// Use this variant when the caller needs a temporary node to preserve in-place
/// mutation ordering, such as MemTree child splits that overwrite the original
/// child only after both halves have been constructed.
pub(crate) fn pack_node_range_box<'a, V>(
    src: &BTreeNode,
    params: KnownFenceNodeParams<'a>,
    range: Range<usize>,
) -> BTreeNodeBox
where
    V: BTreeValue,
{
    let mut dst = BTreeNodeBox::alloc(
        params.height,
        params.ts,
        params.lower_fence,
        params.lower_fence_value,
        params.upper_fence.unwrap_or(&[]),
        params.hints_enabled,
    );
    finish_node_range::<V>(&mut dst, src, params.upper_fence, range);
    dst
}

/// Initialize `node` with fixed fences and insert every supplied entry.
///
/// This is the materialization counterpart for callers that already ran a
/// sibling-packing dry run and stored the exact finite upper fence selected by
/// that plan.
pub(crate) fn pack_fixed_entries<'a, V>(
    node: &mut BTreeNode,
    params: KnownFenceNodeParams<'a>,
    entries: &[PackedNodeEntry<'_, V>],
) -> Result<PackedNodeResult<'a>>
where
    V: BTreeValue + Copy,
{
    if !fences_fit::<V>(params.lower_fence, params.upper_fence.unwrap_or(&[])) {
        return Err(Error::InvalidArgument);
    }
    node.init(
        params.height,
        params.ts,
        params.lower_fence,
        params.lower_fence_value,
        params.upper_fence.unwrap_or(&[]),
        params.hints_enabled,
    );
    for entry in entries {
        if !node.can_insert::<V>(entry.key) {
            return Err(Error::InvalidArgument);
        }
        let idx = node.count();
        node.insert_at::<V>(idx, entry.key, entry.value);
    }
    node.update_hints();
    Ok(PackedNodeResult {
        packed: entries.len(),
        effective_space: node.effective_space(),
        upper_fence: params.upper_fence,
    })
}

/// Allocate a boxed node and rebuild it from multiple fixed source ranges.
///
/// MemTree full and partial merges use this to keep the original online merge
/// policy while sharing the mechanical rebuild sequence. The caller owns merge
/// planning, separator selection, and page mutation ordering.
pub(crate) fn pack_node_ranges_box<'a, V>(
    params: KnownFenceNodeParams<'a>,
    ranges: &[NodeSlotRange<'_>],
) -> BTreeNodeBox
where
    V: BTreeValue,
{
    let mut dst = BTreeNodeBox::alloc(
        params.height,
        params.ts,
        params.lower_fence,
        params.lower_fence_value,
        params.upper_fence.unwrap_or(&[]),
        params.hints_enabled,
    );
    finish_node_ranges::<V>(&mut dst, params.upper_fence, ranges);
    dst
}

/// Plan how an online MemTree compactor should merge one right sibling.
///
/// The planner intentionally preserves the old conservative estimator and
/// threshold semantics. A full right sibling may still donate a prefix when the
/// left anchor is below the low-space trigger; unlike DiskTree rewrite, MemTree
/// does not require the donor itself to be underfilled.
pub(crate) fn plan_memtree_sibling_merge<V>(
    left: &BTreeNode,
    right: &BTreeNode,
    lower_fence: &[u8],
    upper_fence: &[u8],
    target_effective_space: usize,
) -> MemTreeSiblingMergePlan
where
    V: BTreeValue,
{
    let mut estimation = SpaceEstimation::with_fences(lower_fence, upper_fence, V::ENCODED_LEN);
    estimation.add_key_range(left, 0, left.count());
    if estimation.total_space() > BTREE_NODE_USABLE_SIZE {
        return MemTreeSiblingMergePlan::FenceOutOfSpace;
    }
    if right.count() == 0 {
        return MemTreeSiblingMergePlan::Full;
    }
    let right_count = estimation.grow_until_threshold(right, target_effective_space);
    if right_count == 0 {
        MemTreeSiblingMergePlan::NoProgress
    } else if right_count == right.count() {
        MemTreeSiblingMergePlan::Full
    } else {
        MemTreeSiblingMergePlan::Partial { right_count }
    }
}

fn finish_node_range<'a, V>(
    dst: &mut BTreeNode,
    src: &BTreeNode,
    upper_fence: Option<&'a [u8]>,
    range: Range<usize>,
) -> PackedNodeResult<'a>
where
    V: BTreeValue,
{
    debug_assert!(range.start <= range.end);
    debug_assert!(range.end <= src.count());
    let packed = range.end - range.start;
    dst.extend_slots_from::<V>(src, range.start, packed);
    dst.update_hints();
    PackedNodeResult {
        packed,
        effective_space: dst.effective_space(),
        upper_fence,
    }
}

fn finish_node_ranges<'a, V>(
    dst: &mut BTreeNode,
    upper_fence: Option<&'a [u8]>,
    ranges: &[NodeSlotRange<'_>],
) -> PackedNodeResult<'a>
where
    V: BTreeValue,
{
    let mut packed = 0usize;
    for source in ranges {
        debug_assert!(source.range.start <= source.range.end);
        debug_assert!(source.range.end <= source.node.count());
        let count = source.range.end - source.range.start;
        dst.extend_slots_from::<V>(source.node, source.range.start, count);
        packed += count;
    }
    dst.update_hints();
    PackedNodeResult {
        packed,
        effective_space: dst.effective_space(),
        upper_fence,
    }
}

#[cfg(test)]
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

fn packed_node_space<V>(
    lower_fence: &[u8],
    upper_fence: Option<&[u8]>,
    entries: &[PackedNodeEntry<'_, V>],
    count: usize,
) -> Option<PackedNodeSpace>
where
    V: BTreeValue + Copy,
{
    let upper_fence = upper_fence.unwrap_or(&[]);
    if !fences_fit::<V>(lower_fence, upper_fence) {
        return None;
    }
    estimate_packed_node_space::<V>(lower_fence, upper_fence, &entries[..count])
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
    fn test_plan_sibling_node_uses_finite_fence_common_prefix() {
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

        let result = plan_sibling_node(
            PackedNodePlanParams {
                lower_fence: b"prefix-0001",
                min_slots: 1,
            },
            &entries,
        )
        .unwrap();

        assert_eq!(result.packed, entries.len());
        assert_eq!(result.upper_fence, None);

        let mut finite_node =
            BTreeNodeBox::alloc(0, 9, b"prefix-0001", BTreeU64::INVALID_VALUE, &[], false);
        let result = pack_fixed_entries(
            &mut finite_node,
            KnownFenceNodeParams {
                height: 0,
                ts: 9,
                lower_fence: b"prefix-0001",
                lower_fence_value: BTreeU64::INVALID_VALUE,
                upper_fence: Some(b"prefix-0004"),
                hints_enabled: false,
            },
            &entries[..2],
        )
        .unwrap();

        assert_eq!(result.upper_fence, Some(b"prefix-0004".as_slice()));
        assert_eq!(finite_node.common_prefix(), b"prefix-000");
    }

    #[test]
    fn test_plan_sibling_node_accounts_for_upper_fence_capacity() {
        let long_key = vec![b'x'; BTREE_NODE_USABLE_SIZE];
        let entries = [PackedNodeEntry {
            key: long_key.as_slice(),
            value: BTreeNil,
        }];

        let err = plan_sibling_node(
            PackedNodePlanParams {
                lower_fence: &long_key,
                min_slots: 1,
            },
            &entries,
        )
        .unwrap_err();

        assert!(matches!(err, Error::InvalidArgument));
    }

    fn leaf_node(keys: &[&[u8]]) -> BTreeNodeBox {
        let mut node = BTreeNodeBox::alloc(
            0,
            1,
            keys.first().copied().unwrap_or(&[]),
            BTreeU64::INVALID_VALUE,
            b"zzzz",
            false,
        );
        for (idx, key) in keys.iter().enumerate() {
            node.insert_at::<BTreeU64>(idx, key, BTreeU64::from(idx as u64 + 1));
        }
        node
    }

    #[test]
    fn test_plan_memtree_sibling_merge_outcomes() {
        let left = leaf_node(&[b"aa01"]);
        let right = leaf_node(&[b"aa02", b"aa03", b"aa04"]);
        assert_eq!(
            plan_memtree_sibling_merge::<BTreeU64>(&left, &right, b"aa01", b"zzzz", 0),
            MemTreeSiblingMergePlan::NoProgress
        );

        let mut estimation = SpaceEstimation::with_fences(b"aa01", b"zzzz", BTreeU64::ENCODED_LEN);
        estimation.add_key_range(&left, 0, left.count());
        let threshold = estimation.add_key(right.key(0).len() as u16);
        assert_eq!(
            plan_memtree_sibling_merge::<BTreeU64>(&left, &right, b"aa01", b"zzzz", threshold),
            MemTreeSiblingMergePlan::Partial { right_count: 1 }
        );

        assert_eq!(
            plan_memtree_sibling_merge::<BTreeU64>(
                &left,
                &right,
                b"aa01",
                b"zzzz",
                BTREE_NODE_USABLE_SIZE,
            ),
            MemTreeSiblingMergePlan::Full
        );

        let empty_right = leaf_node(&[]);
        assert_eq!(
            plan_memtree_sibling_merge::<BTreeU64>(
                &left,
                &empty_right,
                b"aa01",
                b"zzzz",
                BTREE_NODE_USABLE_SIZE,
            ),
            MemTreeSiblingMergePlan::Full
        );
    }

    #[test]
    fn test_pack_node_ranges_matches_manual_rebuild() {
        let left = leaf_node(&[b"aa01", b"aa02"]);
        let right = leaf_node(&[b"aa03", b"aa04"]);
        let params = KnownFenceNodeParams {
            height: 0,
            ts: 7,
            lower_fence: b"aa01",
            lower_fence_value: BTreeU64::INVALID_VALUE,
            upper_fence: Some(b"zzzz"),
            hints_enabled: false,
        };
        let packed = pack_node_ranges_box::<BTreeU64>(
            params,
            &[
                NodeSlotRange {
                    node: &left,
                    range: 0..left.count(),
                },
                NodeSlotRange {
                    node: &right,
                    range: 0..right.count(),
                },
            ],
        );

        let mut manual =
            BTreeNodeBox::alloc(0, 7, b"aa01", BTreeU64::INVALID_VALUE, b"zzzz", false);
        manual.extend_slots_from::<BTreeU64>(&left, 0, left.count());
        manual.extend_slots_from::<BTreeU64>(&right, 0, right.count());
        manual.update_hints();

        assert_eq!(packed.count(), manual.count());
        assert_eq!(packed.effective_space(), manual.effective_space());
        for idx in 0..packed.count() {
            assert_eq!(packed.key(idx), manual.key(idx));
            assert_eq!(packed.value::<BTreeU64>(idx), manual.value::<BTreeU64>(idx));
        }
    }
}
