//! Bounded coordinate lookahead and fence-aware packing shared by tree consumers.
use super::merge::{HotEntryRef, execution_error};
use super::{BudgetedVec, MemoryBudget, SortedRuns};
use crate::error::{RuntimeError, RuntimeOrFatalResult};
use crate::id::{RowID, TrxID};
use crate::index::btree::algo::{
    KnownFenceNodeParams, PackedNodeEntry, PackedNodePlanParams, try_plan_sibling_node,
};
use crate::index::btree::{
    BTREE_NODE_USABLE_SIZE, BTreeSlot, BTreeU64, BTreeValue, PackedNodeSpace,
};
use crate::index::util::Maskable;
use error_stack::ResultExt;

const INITIAL_CANDIDATES: usize = 64;

/// One planned leaf prefix and its final fences in the retained sorted runs.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct LeafPlan {
    /// Number of entries to pack from the caller's candidate scratch.
    pub(super) count: usize,
    /// Inclusive lower fence, or `None` for the global leftmost MemIndex leaf.
    pub(super) lower: Option<HotEntryRef>,
    /// Exclusive upper fence, or `None` for the global rightmost leaf.
    pub(super) upper: Option<HotEntryRef>,
}

impl LeafPlan {
    /// Resolve the final fences for the shared node writer without changing the plan.
    #[inline]
    pub(super) fn node_params<'a>(
        &self,
        runs: &'a SortedRuns,
        ts: TrxID,
    ) -> KnownFenceNodeParams<'a> {
        KnownFenceNodeParams {
            height: 0,
            ts,
            lower_fence: self.lower.map_or(&[], |r| r.resolve(runs).key.as_bytes()),
            lower_fence_value: BTreeU64::INVALID_VALUE,
            upper_fence: self.upper.map(|r| r.resolve(runs).key.as_bytes()),
            hints_enabled: true,
        }
    }
}

/// Fixed-capacity coordinate lookahead shared by memory and disk consumers.
/// Consuming a prefix advances the head without moving initialized slots.
pub(super) struct LeafWindow {
    entries: BudgetedVec<HotEntryRef>,
    head: usize,
    len: usize,
    capacity: usize,
}

impl LeafWindow {
    /// Admit and allocate one fixed lookahead window.
    pub(super) fn new(budget: &MemoryBudget, capacity: usize) -> RuntimeOrFatalResult<Self> {
        let mut entries = BudgetedVec::new(budget);
        entries
            .ensure_capacity(capacity, "packing window")
            .change_context(RuntimeError::IndexAccess)?;
        Ok(Self {
            entries,
            head: 0,
            len: 0,
            capacity,
        })
    }

    /// Adopt already allocated lookahead storage without new admission.
    pub(super) fn admitted(entries: BudgetedVec<HotEntryRef>, capacity: usize) -> Self {
        Self {
            entries,
            head: 0,
            len: 0,
            capacity,
        }
    }

    /// Return the number of retained coordinates.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// Report whether all admitted lookahead slots are occupied.
    #[inline]
    pub(super) fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    /// Borrow one coordinate in logical window order.
    #[inline]
    pub(super) fn get(&self, index: usize) -> Option<HotEntryRef> {
        (index < self.len).then(|| self.entries[(self.head + index) % self.capacity])
    }

    /// Append a coordinate to an admitted slot.
    #[inline]
    pub(super) fn push(&mut self, entry: HotEntryRef) {
        assert!(
            self.len < self.capacity,
            "hot leaf window exceeds its admitted capacity"
        );
        let index = (self.head + self.len) % self.capacity;
        if index == self.entries.len() {
            self.entries.push_reserved(entry);
        } else {
            self.entries[index] = entry;
        }
        self.len += 1;
    }

    /// Discard a consumed prefix without moving the remaining coordinates.
    #[inline]
    pub(super) fn consume(&mut self, count: usize) {
        assert!(
            count <= self.len,
            "hot leaf window consumption exceeds retained entries"
        );
        if count != 0 {
            self.head = (self.head + count) % self.capacity;
            self.len -= count;
        }
    }

    /// Reset initialized slots while retaining allocation and admission.
    #[inline]
    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.head = 0;
        self.len = 0;
    }
}

/// Bound node slot count using mandatory slot/value bytes alone.
pub(super) const fn max_node_slots<V: BTreeValue>() -> usize {
    BTREE_NODE_USABLE_SIZE / (size_of::<BTreeSlot>() + V::ENCODED_LEN)
}

/// Bound retained leaf coordinates using three maximal pages plus one fence.
pub(super) const fn max_leaf_window_entries<V: BTreeValue>() -> usize {
    3 * max_node_slots::<V>() + 1
}

/// Plan one leaf using caller-owned scratch and the caller's lower-fence policy.
/// Keys remain borrowed from `runs`; consumers pack the returned prefix before
/// reusing `entries` and retain the same run owner through materialization.
pub(super) fn plan_leaf<'a, V: BTreeValue + Copy>(
    runs: &'a SortedRuns,
    window: &LeafWindow,
    lower: Option<HotEntryRef>,
    upper: Option<HotEntryRef>,
    value: fn(RowID) -> V,
    entries: &mut BudgetedVec<PackedNodeEntry<'a, V>>,
    purpose: &'static str,
) -> RuntimeOrFatalResult<LeafPlan> {
    // Both consumers call only when their window is full or has a final tail.
    assert!(window.len() > 0, "leaf planning requires a nonempty window");
    let count = plan_candidates(
        entries,
        window.len(),
        PackedNodePlanParams {
            lower_fence: lower.map_or(&[], |r| r.resolve(runs).key.as_bytes()),
            upper_fence: upper.map(|r| r.resolve(runs).key.as_bytes()),
            min_slots: 1,
        },
        purpose,
        |index| {
            let entry = window
                .get(index)
                .unwrap_or_else(|| unreachable!("bounded leaf candidate index"))
                .resolve(runs);
            PackedNodeEntry {
                key: entry.key.as_bytes(),
                value: value(entry.row_id),
            }
        },
    )?;
    let upper = window.get(count).or(upper);
    if let Some(upper) = upper
        && entries[count - 1].key >= upper.resolve(runs).key.as_bytes()
    {
        return Err(execution_error(
            "leaf contains a duplicate or excludes its last key",
        ));
    }
    Ok(LeafPlan {
        count,
        lower,
        upper,
    })
}

/// Plan a bounded candidate prefix under actual sibling fences.
pub(super) fn plan_candidates<'a, V: BTreeValue + Copy>(
    entries: &mut BudgetedVec<PackedNodeEntry<'a, V>>,
    available: usize,
    params: PackedNodePlanParams<'a>,
    purpose: &'static str,
    mut entry: impl FnMut(usize) -> PackedNodeEntry<'a, V>,
) -> RuntimeOrFatalResult<usize> {
    // One extra candidate supplies the upper fence even for a maximal node.
    let limit = available.min(max_node_slots::<V>() + 1);
    let mut count = limit.min(INITIAL_CANDIDATES);
    entries.clear();
    loop {
        entries
            .ensure_capacity(count, purpose)
            .change_context(RuntimeError::IndexAccess)?;
        for index in entries.len()..count {
            entries.push_reserved(entry(index));
        }
        let plan = try_plan_sibling_node(params, entries);
        if count < limit {
            let reaches_end = plan.is_some_and(|plan| plan.packed + 1 >= count);
            // The existing planner can stop at overflow once the prefix is inline.
            // An outlined prefix may still shrink and free space at a later fence.
            let prefix_can_shrink =
                PackedNodeSpace::with_fences(params.lower_fence, entries[count - 1].key)
                    .is_none_or(|space| !space.prefix_is_inline());
            if reaches_end || prefix_can_shrink {
                count = (count * 2).min(limit);
                continue;
            }
        }
        return plan
            .map(|plan| plan.packed)
            .ok_or_else(|| execution_error("hot node key/fences cannot fit a page"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ResourceError, RuntimeOrFatalError};
    use crate::index::BTreeKey;
    use crate::index::btree::algo::pack_fixed_entries;
    use crate::index::btree::{BTREE_BYTE_ZERO, BTreeNil, BTreeNodeBox};
    use crate::index::build::merge::test_runs;
    use crate::index::build::{DuplicateCheck, budget};
    use std::fmt::Debug;
    use std::sync::Arc;

    fn leaf_runs(count: usize, width: usize, prefix: usize) -> Arc<SortedRuns> {
        let keys = (0..count)
            .map(|index| {
                let mut bytes = vec![b'p'; prefix];
                bytes.extend_from_slice(&(index as u32).to_be_bytes());
                bytes.resize(prefix + width, 0);
                BTreeKey::from(bytes.as_slice())
            })
            .collect();
        test_runs(
            vec![keys],
            DuplicateCheck::Skip,
            MemoryBudget::new(usize::MAX),
        )
    }

    fn leaf_window(runs: &SortedRuns, count: usize, budget: &MemoryBudget) -> LeafWindow {
        let mut window = LeafWindow::new(budget, count).unwrap();
        for index in 0..count {
            window.push(HotEntryRef::new(runs, 0, index));
        }
        window
    }

    fn assert_leaf_plan<V: BTreeValue + Copy + Debug + Eq>(
        runs: &SortedRuns,
        count: usize,
        lower: Option<HotEntryRef>,
        upper: Option<HotEntryRef>,
        value: fn(RowID) -> V,
    ) -> usize {
        let budget = MemoryBudget::new(1024 * 1024);
        let window = leaf_window(runs, count, &budget);
        let mut entries = BudgetedVec::new(&budget);
        let source = &runs.runs()[0].entries()[..count];
        let full: Vec<_> = source
            .iter()
            .map(|entry| PackedNodeEntry {
                key: entry.key.as_bytes(),
                value: value(entry.row_id),
            })
            .collect();
        let lower_key = lower.map_or(&[][..], |r| r.resolve(runs).key.as_bytes());
        let expected = try_plan_sibling_node(
            PackedNodePlanParams {
                lower_fence: lower_key,
                upper_fence: upper.map(|r| r.resolve(runs).key.as_bytes()),
                min_slots: 1,
            },
            &full,
        )
        .unwrap();
        let plan = plan_leaf(
            runs,
            &window,
            lower,
            upper,
            value,
            &mut entries,
            "test leaf entries",
        )
        .unwrap();
        assert_eq!(plan.count, expected.packed);
        assert_eq!(plan.lower, lower);
        let params = plan.node_params(runs, TrxID::new(42));
        assert_eq!(params.upper_fence, expected.upper_fence);
        assert!(entries.capacity() <= max_node_slots::<V>() + 1);

        let mut node =
            BTreeNodeBox::alloc(0, TrxID::new(0), &[], BTreeU64::INVALID_VALUE, &[], false);
        pack_fixed_entries(&mut node, params, &entries[..plan.count]);
        assert!(node.validate_persisted_layout::<V>());
        assert_eq!(node.height(), 0);
        assert_eq!(node.ts(), TrxID::new(42));
        assert_eq!(node.count(), expected.packed);
        assert_eq!(node.lower_fence_key().as_bytes(), lower_key);
        assert_eq!(node.lower_fence_value(), BTreeU64::INVALID_VALUE);
        assert_eq!(node.has_no_upper_fence(), expected.upper_fence.is_none());
        assert_eq!(
            node.upper_fence_key().as_bytes(),
            expected.upper_fence.unwrap_or(&[])
        );
        assert!(node.header_hints_enabled());
        for (index, entry) in source[..expected.packed].iter().enumerate() {
            assert_eq!(node.key(index), entry.key);
            assert_eq!(node.value::<V>(index), value(entry.row_id));
            assert_eq!(node.search_key(entry.key.as_bytes()), Ok(index));
        }
        plan.count
    }

    fn assert_leaf_formats<V: BTreeValue + Copy + Debug + Eq>(value: fn(RowID) -> V) {
        for (count, width, prefix) in [(1, 8, 0), (65, 8, 0), (130, 2048, 0), (150, 8, 512)] {
            let runs = leaf_runs(count + 1, width, prefix);
            for lower in [None, Some(HotEntryRef::new(&runs, 0, 0))] {
                for upper in [None, Some(HotEntryRef::new(&runs, 0, count))] {
                    assert_leaf_plan(&runs, count, lower, upper, value);
                }
            }
        }
        // Restrict a known split to its first page plus one trailing entry.
        // The shared planner must preserve that split instead of redistributing.
        // Keep the partition fence finite so shortening the window does not
        // remove fence storage and let the entire prefix fit instead.
        let runs = leaf_runs(101, 2048, 0);
        let upper = Some(HotEntryRef::new(&runs, 0, 100));
        for lower in [None, Some(HotEntryRef::new(&runs, 0, 0))] {
            let count = assert_leaf_plan(&runs, 100, lower, upper, value);
            assert!(count > 2 && count + 1 < 100);
            assert_eq!(
                assert_leaf_plan(&runs, count + 1, lower, upper, value),
                count
            );
        }
    }

    /// Purpose: Share leaf planning and node filling across all value formats and fence policies.
    /// Expected: Full-window oracle splits, including singleton tails, preserve packed keys, values, fences, and headers.
    #[test]
    fn leaf_plans_pack_all_value_formats() {
        assert_leaf_formats(BTreeU64::from);
        assert_leaf_formats(|_| BTREE_BYTE_ZERO);
        assert_leaf_formats(|_| BTreeNil);
    }

    /// Purpose: Preserve caller-owned scratch admission for growing and preallocated leaf consumers.
    /// Expected: Replanning reuses the allocation and charge; rejected fresh admission retains its typed cause and existing scratch.
    #[test]
    fn leaf_planning_reuses_admitted_scratch() {
        let runs = leaf_runs(65, 8, 0);
        for preallocated in [false, true] {
            let budget = MemoryBudget::new(1024 * 1024);
            let window = leaf_window(&runs, 64, &budget);
            let mut entries = if preallocated {
                let capacity = max_node_slots::<BTreeU64>() + 1;
                let reservation = budget
                    .reserve(
                        capacity * size_of::<PackedNodeEntry<'_, BTreeU64>>(),
                        "test scratch",
                    )
                    .unwrap();
                BudgetedVec::from_reservation(reservation, capacity)
            } else {
                BudgetedVec::new(&budget)
            };
            let lower = Some(HotEntryRef::new(&runs, 0, 0));
            let upper = Some(HotEntryRef::new(&runs, 0, 64));
            let expected = plan_leaf(
                &runs,
                &window,
                lower,
                upper,
                BTreeU64::from,
                &mut entries,
                "test leaf entries",
            )
            .unwrap();
            let allocation = entries.as_ptr();
            let admitted = budget.used();
            budget::fail_at(&budget, "test leaf entries");
            let actual = plan_leaf(
                &runs,
                &window,
                lower,
                upper,
                BTreeU64::from,
                &mut entries,
                "test leaf entries",
            )
            .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(entries.as_ptr(), allocation);
            assert_eq!(budget.used(), admitted);

            let mut rejected = BudgetedVec::new(&budget);
            let error = plan_leaf(
                &runs,
                &window,
                lower,
                upper,
                BTreeU64::from,
                &mut rejected,
                "test leaf entries",
            )
            .unwrap_err();
            let RuntimeOrFatalError::Runtime(report) = error else {
                panic!("leaf scratch rejection must remain a runtime resource failure");
            };
            assert_eq!(*report.current_context(), RuntimeError::IndexAccess);
            assert_eq!(
                report.downcast_ref::<ResourceError>(),
                Some(&ResourceError::InsufficientMemory)
            );
            assert!(format!("{report:?}").contains("test leaf entries"));
            assert_eq!(rejected.capacity(), 0);
            assert_eq!(entries.as_ptr(), allocation);
            assert_eq!(budget.used(), admitted);
            drop(rejected);
            drop(entries);
            drop(window);
            assert_eq!(budget.used(), 0);
        }
    }

    /// Purpose: Reject a finite leaf upper fence that fails to exclude the packed final key.
    /// Expected: Equal and earlier upper boundaries return the index execution error before node filling.
    #[test]
    fn leaf_planning_rejects_invalid_upper_boundary() {
        let runs = leaf_runs(4, 8, 0);
        let budget = MemoryBudget::new(4096);
        let window = leaf_window(&runs, 4, &budget);
        let mut entries = BudgetedVec::new(&budget);
        for upper in [2, 3] {
            let error = plan_leaf(
                &runs,
                &window,
                None,
                Some(HotEntryRef::new(&runs, 0, upper)),
                BTreeU64::from,
                &mut entries,
                "test leaf entries",
            )
            .unwrap_err();
            let RuntimeOrFatalError::Runtime(report) = error else {
                panic!("invalid leaf boundary must remain an index execution failure");
            };
            assert_eq!(*report.current_context(), RuntimeError::IndexAccess);
            assert!(
                format!("{report:?}")
                    .contains("leaf contains a duplicate or excludes its last key")
            );
        }
    }

    /// Purpose: Protect retained leaf order and allocation reuse across circular-window wraparound and reset.
    /// Expected: Consumed and refilled windows match a queue oracle without moving or reallocating backing storage.
    #[test]
    fn packed_leaf_window_wraparound() {
        use std::collections::VecDeque;
        let runs = test_runs(
            (0..4)
                .map(|g| {
                    (0..25)
                        .map(|n| BTreeKey::from((g * 25 + n) as u32))
                        .collect()
                })
                .collect(),
            DuplicateCheck::Skip,
            MemoryBudget::new(usize::MAX),
        );
        for capacity in [0, 1, 7, 64] {
            let budget = MemoryBudget::new(4096);
            let mut window = LeafWindow::new(&budget, capacity).unwrap();
            let allocation = window.entries.as_ptr();
            let mut expected = VecDeque::new();
            for round in 0..100 {
                while !window.is_full() {
                    let entry = HotEntryRef::new(&runs, round % 4, (round + window.len()) % 25);
                    window.push(entry);
                    expected.push_back(entry);
                }
                for (index, &entry) in expected.iter().enumerate() {
                    assert_eq!(window.get(index), Some(entry));
                }
                assert_eq!(window.get(expected.len()), None);
                assert_eq!(window.entries.as_ptr(), allocation);
                assert_eq!(budget.used(), capacity * size_of::<HotEntryRef>());
                let count = (round % 5 + 1).min(expected.len());
                let retained = window.entries.to_vec();
                window.consume(count);
                assert_eq!(&*window.entries, retained.as_slice());
                expected.drain(..count);
                assert_eq!(window.len(), expected.len());
                if round % 11 == 10 {
                    window.clear();
                    expected.clear();
                }
            }
            drop(window);
            assert_eq!(budget.used(), 0);
        }
    }
}
