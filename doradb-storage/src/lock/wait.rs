use super::claim::PendingClaimToken;
use super::state::{FamilyLockState, LockScopeState};
use super::{
    LockGrant, LockManager, LockMode, LockOwner, PendingObservation, PendingStart,
    lock_waiter_released_err,
};
use crate::completion::Completion;
use crate::error::OperationResult;
use crate::id::ClaimNo;
use std::mem::replace;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WaitNodeID {
    pub(super) slot: usize,
    pub(super) generation: u64,
}

pub(super) struct WaitNodeSlot {
    generation: u64,
    entry: WaitNodeSlotEntry,
}

pub(super) enum WaitNodeSlotEntry {
    Occupied(WaitNode),
    Vacant { next_free: usize },
}

pub(super) struct WaitNode {
    pub(super) owner: LockOwner,
    pub(super) claim_no: ClaimNo,
    pub(super) target_mode: LockMode,
    pub(super) phase: WaitNodePhase,
    pub(super) completion: Arc<Completion<()>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WaitNodePhase {
    Queued {
        prev: Option<WaitNodeID>,
        next: Option<WaitNodeID>,
    },
    Provisional,
    Released,
}

pub(super) struct WaitNodeSlab {
    slots: Vec<WaitNodeSlot>,
    free_head: usize,
    live_count: usize,
}

impl WaitNodeSlab {
    #[inline]
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_head: 0,
            live_count: 0,
        }
    }

    #[inline]
    fn insert(&mut self, node: WaitNode) -> WaitNodeID {
        let slot = self.free_head;
        if slot < self.slots.len() {
            let next_free = match self.slots[slot].entry {
                WaitNodeSlotEntry::Vacant { next_free } => next_free,
                WaitNodeSlotEntry::Occupied(_) => {
                    panic!("waiter slab free head identifies an occupied slot: slot={slot}")
                }
            };
            assert!(
                next_free <= self.slots.len(),
                "waiter slab free link exceeds the direct end sentinel: \
                 slot={slot}, next_free={next_free}, slots_len={}",
                self.slots.len()
            );
            let generation = self.slots[slot].generation;
            self.slots[slot].entry = WaitNodeSlotEntry::Occupied(node);
            self.free_head = next_free;
            self.live_count += 1;
            return WaitNodeID { slot, generation };
        }

        assert!(
            slot == self.slots.len(),
            "waiter slab free head exceeds the direct end sentinel: \
             free_head={slot}, slots_len={}",
            self.slots.len()
        );
        self.slots.push(WaitNodeSlot {
            generation: 0,
            entry: WaitNodeSlotEntry::Occupied(node),
        });
        self.free_head = self.slots.len();
        self.live_count += 1;
        WaitNodeID {
            slot,
            generation: 0,
        }
    }

    #[inline]
    pub(super) fn get(&self, id: WaitNodeID) -> &WaitNode {
        let slot = self.slots.get(id.slot).unwrap_or_else(|| {
            panic!(
                "waiter node slot is out of bounds: id={id:?}, slots_len={}",
                self.slots.len()
            )
        });
        assert!(
            slot.generation == id.generation,
            "stale waiter node generation: id={id:?}, actual_generation={}",
            slot.generation
        );
        match &slot.entry {
            WaitNodeSlotEntry::Occupied(node) => node,
            WaitNodeSlotEntry::Vacant { .. } => {
                panic!("waiter node identifies a vacant slot: id={id:?}")
            }
        }
    }

    #[inline]
    fn get_mut(&mut self, id: WaitNodeID) -> &mut WaitNode {
        let slots_len = self.slots.len();
        let slot = self.slots.get_mut(id.slot).unwrap_or_else(|| {
            panic!("waiter node slot is out of bounds: id={id:?}, slots_len={slots_len}")
        });
        assert!(
            slot.generation == id.generation,
            "stale waiter node generation: id={id:?}, actual_generation={}",
            slot.generation
        );
        match &mut slot.entry {
            WaitNodeSlotEntry::Occupied(node) => node,
            WaitNodeSlotEntry::Vacant { .. } => {
                panic!("waiter node identifies a vacant slot: id={id:?}")
            }
        }
    }

    #[inline]
    fn reclaim(&mut self, id: WaitNodeID) -> WaitNode {
        let slots_len = self.slots.len();
        let slot = self.slots.get(id.slot).unwrap_or_else(|| {
            panic!("waiter reclaim slot is out of bounds: id={id:?}, slots_len={slots_len}")
        });
        assert!(
            slot.generation == id.generation,
            "stale waiter reclaim generation: id={id:?}, actual_generation={}",
            slot.generation
        );
        assert!(
            matches!(slot.entry, WaitNodeSlotEntry::Occupied(_)),
            "waiter reclaim requires an occupied slot: id={id:?}"
        );
        let next_generation = slot.generation.checked_add(1).unwrap_or_else(|| {
            panic!("waiter node generation exhausted before reclamation: id={id:?}")
        });

        let old_free_head = self.free_head;
        let slot = &mut self.slots[id.slot];
        let entry = replace(
            &mut slot.entry,
            WaitNodeSlotEntry::Vacant {
                next_free: old_free_head,
            },
        );
        slot.generation = next_generation;
        self.free_head = id.slot;
        self.live_count -= 1;
        match entry {
            WaitNodeSlotEntry::Occupied(node) => node,
            WaitNodeSlotEntry::Vacant { .. } => {
                panic!("validated waiter reclaim unexpectedly found a vacant slot: id={id:?}")
            }
        }
    }

    #[inline]
    pub(super) const fn live_count(&self) -> usize {
        self.live_count
    }

    #[inline]
    pub(super) fn occupied_ids(&self) -> Vec<WaitNodeID> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| {
                matches!(entry.entry, WaitNodeSlotEntry::Occupied(_)).then_some(WaitNodeID {
                    slot,
                    generation: entry.generation,
                })
            })
            .collect()
    }

    #[inline]
    fn any_occupied(&self, mut predicate: impl FnMut(&WaitNode) -> bool) -> bool {
        self.slots.iter().any(|slot| match &slot.entry {
            WaitNodeSlotEntry::Occupied(node) => predicate(node),
            WaitNodeSlotEntry::Vacant { .. } => false,
        })
    }
}

pub(super) struct WaitQueue {
    head: Option<WaitNodeID>,
    tail: Option<WaitNodeID>,
    nodes: WaitNodeSlab,
}

impl Default for WaitQueue {
    #[inline]
    fn default() -> Self {
        Self {
            head: None,
            tail: None,
            nodes: WaitNodeSlab::new(),
        }
    }
}

impl WaitQueue {
    #[inline]
    pub(super) fn append(
        &mut self,
        owner: LockOwner,
        claim_no: ClaimNo,
        target_mode: LockMode,
        completion: Arc<Completion<()>>,
    ) -> WaitNodeID {
        let previous_tail = self.tail;
        let id = self.nodes.insert(WaitNode {
            owner,
            claim_no,
            target_mode,
            phase: WaitNodePhase::Queued {
                prev: previous_tail,
                next: None,
            },
            completion,
        });
        if let Some(tail) = previous_tail {
            let tail_node = self.nodes.get_mut(tail);
            match &mut tail_node.phase {
                WaitNodePhase::Queued { next, .. } => {
                    assert!(
                        next.is_none(),
                        "waiter queue tail retains a successor: tail={tail:?}, next={next:?}"
                    );
                    *next = Some(id);
                }
                phase => panic!("waiter queue tail is not linked: tail={tail:?}, phase={phase:?}"),
            }
        } else {
            assert!(
                self.head.is_none(),
                "waiter queue with no tail retains a head: head={:?}",
                self.head
            );
            self.head = Some(id);
        }
        self.tail = Some(id);
        id
    }

    #[inline]
    pub(super) const fn head(&self) -> Option<WaitNodeID> {
        self.head
    }

    #[inline]
    pub(super) const fn is_linked_empty(&self) -> bool {
        self.head.is_none()
    }

    #[inline]
    pub(super) fn node(&self, id: WaitNodeID) -> &WaitNode {
        self.nodes.get(id)
    }

    #[inline]
    pub(super) fn assert_identity(
        &self,
        id: WaitNodeID,
        token: &PendingClaimToken,
        target_mode: LockMode,
    ) {
        let node = self.node(id);
        assert!(
            node.owner == token.owner
                && node.claim_no == token.claim_no
                && node.target_mode == target_mode,
            "waiter node identity mismatch: id={id:?}, resource={}, \
             expected_owner={}, expected_claim_no={:?}, expected_mode={}, \
             actual_owner={}, actual_claim_no={:?}, actual_mode={}",
            token.resource,
            token.owner,
            token.claim_no,
            target_mode,
            node.owner,
            node.claim_no,
            node.target_mode
        );
    }

    #[inline]
    pub(super) fn detach_to(&mut self, id: WaitNodeID, phase: WaitNodePhase) {
        assert!(
            matches!(phase, WaitNodePhase::Provisional | WaitNodePhase::Released),
            "waiter detach requires a detached phase: id={id:?}, phase={phase:?}"
        );
        let (prev, next) = match self.nodes.get(id).phase {
            WaitNodePhase::Queued { prev, next } => (prev, next),
            actual => {
                panic!("waiter detach requires a queued node: id={id:?}, actual_phase={actual:?}")
            }
        };

        // Validate both neighboring links and the head/tail boundary before
        // mutating any queue field. An invariant failure must leave the linked
        // structure unchanged.
        if let Some(prev_id) = prev {
            match self.nodes.get(prev_id).phase {
                WaitNodePhase::Queued {
                    next: prev_next, ..
                } => assert!(
                    prev_next == Some(id),
                    "waiter predecessor does not link to target: \
                     predecessor={prev_id:?}, target={id:?}, actual_next={prev_next:?}"
                ),
                actual => panic!(
                    "waiter predecessor is detached: predecessor={prev_id:?}, phase={actual:?}"
                ),
            }
        } else {
            assert!(
                self.head == Some(id),
                "headless waiter target is not the queue head: target={id:?}, head={:?}",
                self.head
            );
        }
        if let Some(next_id) = next {
            match self.nodes.get(next_id).phase {
                WaitNodePhase::Queued {
                    prev: next_prev, ..
                } => assert!(
                    next_prev == Some(id),
                    "waiter successor does not link to target: \
                     successor={next_id:?}, target={id:?}, actual_prev={next_prev:?}"
                ),
                actual => {
                    panic!("waiter successor is detached: successor={next_id:?}, phase={actual:?}")
                }
            }
        } else {
            assert!(
                self.tail == Some(id),
                "tailless waiter target is not the queue tail: target={id:?}, tail={:?}",
                self.tail
            );
        }

        if let Some(prev_id) = prev {
            let WaitNodePhase::Queued {
                next: prev_next, ..
            } = &mut self.nodes.get_mut(prev_id).phase
            else {
                unreachable!("validated waiter predecessor changed without intervening mutation")
            };
            *prev_next = next;
        } else {
            self.head = next;
        }
        if let Some(next_id) = next {
            let WaitNodePhase::Queued {
                prev: next_prev, ..
            } = &mut self.nodes.get_mut(next_id).phase
            else {
                unreachable!("validated waiter successor changed without intervening mutation")
            };
            *next_prev = prev;
        } else {
            self.tail = prev;
        }
        self.nodes.get_mut(id).phase = phase;
    }

    #[inline]
    pub(super) fn set_released(&mut self, id: WaitNodeID) {
        let node = self.nodes.get_mut(id);
        assert!(
            node.phase == WaitNodePhase::Provisional,
            "waiter release requires a provisional node: id={id:?}, phase={:?}",
            node.phase
        );
        node.phase = WaitNodePhase::Released;
    }

    #[inline]
    pub(super) fn consume(&mut self, id: WaitNodeID, expected: WaitNodePhase) -> WaitNode {
        assert!(
            matches!(
                expected,
                WaitNodePhase::Provisional | WaitNodePhase::Released
            ),
            "waiter consume requires a detached phase: id={id:?}, expected={expected:?}"
        );
        assert!(
            self.nodes.get(id).phase == expected,
            "waiter consume phase mismatch: id={id:?}, expected={expected:?}, actual={:?}",
            self.nodes.get(id).phase
        );
        self.nodes.reclaim(id)
    }

    #[inline]
    pub(super) fn occupied_ids(&self) -> Vec<WaitNodeID> {
        self.nodes.occupied_ids()
    }

    #[inline]
    pub(super) fn any_occupied(&self, predicate: impl FnMut(&WaitNode) -> bool) -> bool {
        self.nodes.any_occupied(predicate)
    }

    #[inline]
    pub(super) fn find_linked(
        &self,
        mut predicate: impl FnMut(&WaitNode) -> bool,
    ) -> Option<&WaitNode> {
        let mut next = self.head;
        while let Some(id) = next {
            let node = self.nodes.get(id);
            if predicate(node) {
                return Some(node);
            }
            next = match node.phase {
                WaitNodePhase::Queued { next, .. } => next,
                phase => {
                    panic!("linked waiter has a detached phase: id={id:?}, phase={phase:?}")
                }
            };
        }
        None
    }

    #[inline]
    pub(super) const fn live_count(&self) -> usize {
        self.nodes.live_count()
    }
}

pub(super) enum PendingGuardState {
    NotStarted,
    Waiting {
        node_id: WaitNodeID,
        completion: Arc<Completion<()>>,
    },
    FreshGranted,
    Disarmed,
}

pub(super) struct PendingClaimGuard<'a> {
    manager: &'a LockManager,
    family: &'a mut FamilyLockState,
    curr_scope: &'a mut LockScopeState,
    token: Option<PendingClaimToken>,
    requested_mode: LockMode,
    family_covered: bool,
    state: PendingGuardState,
    transfer_started: bool,
}

impl<'a> PendingClaimGuard<'a> {
    #[inline]
    pub(super) fn new(
        manager: &'a LockManager,
        family: &'a mut FamilyLockState,
        curr_scope: &'a mut LockScopeState,
        token: PendingClaimToken,
        requested_mode: LockMode,
        family_covered: bool,
    ) -> Self {
        Self {
            manager,
            family,
            curr_scope,
            token: Some(token),
            requested_mode,
            family_covered,
            state: PendingGuardState::NotStarted,
            transfer_started: false,
        }
    }

    #[inline]
    pub(super) async fn acquire(mut self) -> OperationResult<LockGrant> {
        let token = self.token.as_ref().unwrap_or_else(|| {
            panic!("pending claim guard must retain its token before manager entry")
        });
        self.state = match self.manager.start_pending(token, self.requested_mode)? {
            PendingStart::Immediate => PendingGuardState::FreshGranted,
            PendingStart::Waiting {
                node_id,
                completion,
            } => PendingGuardState::Waiting {
                node_id,
                completion,
            },
        };

        if let PendingGuardState::Waiting {
            node_id,
            completion,
        } = &self.state
        {
            let node_id = *node_id;
            let completion = Arc::clone(completion);
            let completion_result = completion.wait_take_result().await;
            assert!(
                completion_result.is_ok(),
                "lock waiter success-only completion carried an error"
            );
            let token = self.token.as_ref().unwrap_or_else(|| {
                panic!("waiting pending claim guard lost its token before observation")
            });
            match self
                .manager
                .observe_pending(token, self.requested_mode, node_id)
            {
                PendingObservation::Adopted => {
                    self.state = PendingGuardState::FreshGranted;
                }
                PendingObservation::Released => {
                    self.state = PendingGuardState::Disarmed;
                    let token = self.token.take().unwrap_or_else(|| {
                        panic!("released pending claim guard must retain its move-only token")
                    });
                    return Err(lock_waiter_released_err(token, self.requested_mode));
                }
            }
        }

        self.transfer_started = true;
        let token = self.token.as_ref().unwrap_or_else(|| {
            panic!("fresh-granted pending claim guard lost its token before publication")
        });
        self.family
            .publish_pending_family(token, self.requested_mode);
        self.curr_scope
            .publish_pending_scope(token, self.requested_mode);

        let accepted = self
            .token
            .take()
            .unwrap_or_else(|| panic!("accepted pending claim must retain its move-only token"))
            .accept();
        self.family
            .record_pending_accept(&accepted, self.family_covered);
        self.transfer_started = false;
        self.state = PendingGuardState::Disarmed;
        Ok(LockGrant::Fresh)
    }
}

impl Drop for PendingClaimGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let token = self.token.take();
        match &self.state {
            PendingGuardState::NotStarted => {
                assert!(
                    token.is_some(),
                    "unstarted pending claim guard must retain its move-only token"
                );
            }
            PendingGuardState::Disarmed => {
                assert!(
                    token.is_none(),
                    "disarmed pending claim guard must not retain a pending token"
                );
            }
            PendingGuardState::Waiting { node_id, .. } => {
                let token = token.unwrap_or_else(|| {
                    panic!("waiting pending claim guard must retain its move-only token")
                });
                self.manager
                    .cancel_waiting(token, self.requested_mode, *node_id);
            }
            PendingGuardState::FreshGranted => {
                let token = token.unwrap_or_else(|| {
                    panic!("fresh-granted pending claim guard must retain its move-only token")
                });
                if self.transfer_started {
                    self.family
                        .rollback_pending_publication(self.curr_scope, &token);
                }
                self.manager.cancel_fresh_grant(token, self.requested_mode);
            }
        }
    }
}

#[cfg(test)]
pub(in crate::lock) mod tests {
    use super::*;
    use crate::id::{SessionID, TableID, TrxID};
    use crate::lock::{FamilyLockAuthority, LockResource};
    use std::mem::size_of;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(in crate::lock) struct WaitSlabSnapshot {
        pub(in crate::lock) slots_len: usize,
        pub(in crate::lock) capacity: usize,
        pub(in crate::lock) live_count: usize,
        pub(in crate::lock) free_order: Vec<usize>,
        pub(in crate::lock) generations: Vec<u64>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(in crate::lock) struct WaitQueueSnapshot {
        pub(in crate::lock) head: Option<WaitNodeID>,
        pub(in crate::lock) tail: Option<WaitNodeID>,
        pub(in crate::lock) queue_order: Vec<WaitNodeID>,
        pub(in crate::lock) occupied: Vec<WaitNodeID>,
        pub(in crate::lock) slab: WaitSlabSnapshot,
    }

    #[inline]
    fn slab_snapshot(slab: &WaitNodeSlab) -> WaitSlabSnapshot {
        let mut free_order = Vec::new();
        let mut next = slab.free_head;
        while next < slab.slots.len() {
            free_order.push(next);
            next = match slab.slots[next].entry {
                WaitNodeSlotEntry::Vacant { next_free } => next_free,
                WaitNodeSlotEntry::Occupied(_) => {
                    panic!("waiter slab free list reaches an occupied slot: slot={next}")
                }
            };
        }
        assert!(
            next == slab.slots.len(),
            "waiter slab free list misses the direct end sentinel: \
             terminal={next}, slots_len={}",
            slab.slots.len()
        );
        WaitSlabSnapshot {
            slots_len: slab.slots.len(),
            capacity: slab.slots.capacity(),
            live_count: slab.live_count,
            free_order,
            generations: slab.slots.iter().map(|slot| slot.generation).collect(),
        }
    }

    #[inline]
    pub(in crate::lock) fn linked_ids(queue: &WaitQueue) -> Vec<WaitNodeID> {
        let mut ids = Vec::new();
        let mut next = queue.head;
        while let Some(id) = next {
            let node = queue.nodes.get(id);
            next = match node.phase {
                WaitNodePhase::Queued { next, .. } => next,
                phase => {
                    panic!("linked waiter has a detached phase: id={id:?}, phase={phase:?}")
                }
            };
            ids.push(id);
        }
        assert!(
            ids.last().copied() == queue.tail,
            "waiter queue traversal disagrees with tail: \
             traversed_tail={:?}, stored_tail={:?}",
            ids.last(),
            queue.tail
        );
        ids
    }

    #[inline]
    pub(in crate::lock) fn queue_snapshot(queue: &WaitQueue) -> WaitQueueSnapshot {
        let slab = slab_snapshot(&queue.nodes);
        let queue_order = linked_ids(queue);
        let occupied = queue.occupied_ids();
        let free_count = slab.free_order.len();
        assert_eq!(
            slab.live_count + free_count,
            slab.slots_len,
            "waiter slab live/free partition mismatch"
        );
        assert_eq!(
            occupied.len(),
            slab.live_count,
            "waiter slab occupied/live count mismatch"
        );
        WaitQueueSnapshot {
            head: queue.head,
            tail: queue.tail,
            queue_order,
            occupied,
            slab,
        }
    }

    fn owner(id: u64) -> LockOwner {
        LockOwner::transaction(SessionID::new(id), TrxID::new(id))
    }

    fn completion() -> Arc<Completion<()>> {
        Arc::new(Completion::new())
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn waiter_layout_is_recorded() {
        assert_eq!(size_of::<WaitNodeID>(), 16);
        assert_eq!(size_of::<WaitNode>(), 104);
        assert_eq!(size_of::<WaitNodeSlot>(), 112);
        assert_eq!(size_of::<WaitNodeSlotEntry>(), 104);
    }

    #[test]
    fn slab_reuses_slots_with_a_new_generation_and_retains_capacity() {
        let mut queue = WaitQueue::default();
        let empty = queue_snapshot(&queue);
        assert_eq!(empty.slab.slots_len, 0);
        assert_eq!(empty.slab.capacity, 0);
        assert_eq!(empty.slab.live_count, 0);
        assert!(empty.slab.free_order.is_empty());

        let first = queue.append(owner(1), ClaimNo::new(1), LockMode::Shared, completion());
        queue.detach_to(first, WaitNodePhase::Released);
        let _ = queue.consume(first, WaitNodePhase::Released);
        let reclaimed = queue_snapshot(&queue);
        assert_eq!(reclaimed.slab.slots_len, 1);
        assert_eq!(reclaimed.slab.free_order, vec![0]);
        let retained_capacity = reclaimed.slab.capacity;

        let reused = queue.append(owner(2), ClaimNo::new(2), LockMode::Shared, completion());
        assert_eq!(reused.slot, first.slot);
        assert_eq!(reused.generation, first.generation + 1);
        let snapshot = queue_snapshot(&queue);
        assert_eq!(snapshot.slab.slots_len, 1);
        assert_eq!(snapshot.slab.capacity, retained_capacity);
        assert!(snapshot.slab.free_order.is_empty());

        let appended = queue.append(owner(3), ClaimNo::new(3), LockMode::Shared, completion());
        assert_eq!(appended.slot, 1);
        assert_eq!(appended.generation, 0);
        assert_eq!(queue_snapshot(&queue).slab.slots_len, 2);
    }

    #[test]
    fn intrusive_unlink_updates_head_middle_and_tail() {
        let mut queue = WaitQueue::default();
        let ids = (1..=4)
            .map(|id| {
                queue.append(
                    owner(id),
                    ClaimNo::new(id),
                    LockMode::Exclusive,
                    completion(),
                )
            })
            .collect::<Vec<_>>();
        queue.detach_to(ids[1], WaitNodePhase::Released);
        assert_eq!(linked_ids(&queue), vec![ids[0], ids[2], ids[3]]);
        queue.detach_to(ids[0], WaitNodePhase::Released);
        assert_eq!(linked_ids(&queue), vec![ids[2], ids[3]]);
        queue.detach_to(ids[3], WaitNodePhase::Released);
        assert_eq!(linked_ids(&queue), vec![ids[2]]);
        queue.detach_to(ids[2], WaitNodePhase::Released);
        assert!(linked_ids(&queue).is_empty());
        assert!(queue.head().is_none());
        for id in ids {
            let _ = queue.consume(id, WaitNodePhase::Released);
        }
        assert_eq!(queue.live_count(), 0);
    }

    #[test]
    fn stale_id_panics_before_reused_node_mutation() {
        let mut queue = WaitQueue::default();
        let first = queue.append(owner(1), ClaimNo::new(1), LockMode::Shared, completion());
        queue.detach_to(first, WaitNodePhase::Released);
        let _ = queue.consume(first, WaitNodePhase::Released);
        let reused = queue.append(owner(2), ClaimNo::new(2), LockMode::Shared, completion());
        let before = queue_snapshot(&queue);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            queue.detach_to(first, WaitNodePhase::Released);
        }));
        assert!(panic.is_err());
        assert_eq!(queue_snapshot(&queue), before);
        assert_eq!(linked_ids(&queue), vec![reused]);
    }

    #[test]
    fn link_mismatch_panics_before_neighbor_mutation() {
        let mut queue = WaitQueue::default();
        let ids = (1..=3)
            .map(|id| queue.append(owner(id), ClaimNo::new(id), LockMode::Shared, completion()))
            .collect::<Vec<_>>();
        let WaitNodePhase::Queued { prev, .. } = &mut queue.nodes.get_mut(ids[2]).phase else {
            panic!("new waiter must be queued")
        };
        *prev = Some(ids[0]);

        let panic = catch_unwind(AssertUnwindSafe(|| {
            queue.detach_to(ids[1], WaitNodePhase::Released);
        }));

        assert!(panic.is_err());
        let WaitNodePhase::Queued { next, .. } = queue.node(ids[0]).phase else {
            panic!("predecessor must remain queued")
        };
        assert_eq!(next, Some(ids[1]));
        assert_eq!(queue.head(), Some(ids[0]));
        assert_eq!(queue.tail, Some(ids[2]));
        assert_eq!(queue.live_count(), 3);
    }

    #[test]
    fn generation_exhaustion_leaves_the_slot_occupied() {
        let mut queue = WaitQueue::default();
        let initial = queue.append(owner(1), ClaimNo::new(1), LockMode::Shared, completion());
        let exhausted = WaitNodeID {
            slot: initial.slot,
            generation: u64::MAX,
        };
        queue.nodes.slots[initial.slot].generation = u64::MAX;
        queue.head = Some(exhausted);
        queue.tail = Some(exhausted);
        queue.detach_to(exhausted, WaitNodePhase::Released);
        let before = queue_snapshot(&queue);

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = queue.consume(exhausted, WaitNodePhase::Released);
        }));

        assert!(panic.is_err());
        assert_eq!(queue_snapshot(&queue), before);
        assert_eq!(queue.live_count(), 1);
        assert_eq!(
            queue.node(exhausted).phase,
            WaitNodePhase::Released,
            "generation exhaustion must not expose the slot for reuse"
        );
    }

    #[test]
    fn direct_free_list_reuses_last_reclaimed_slot_first() {
        let mut queue = WaitQueue::default();
        let ids = (1..=3)
            .map(|id| queue.append(owner(id), ClaimNo::new(id), LockMode::Shared, completion()))
            .collect::<Vec<_>>();
        for id in [ids[1], ids[0], ids[2]] {
            queue.detach_to(id, WaitNodePhase::Released);
            let _ = queue.consume(id, WaitNodePhase::Released);
        }
        assert_eq!(queue_snapshot(&queue).slab.free_order, vec![2, 0, 1]);

        let reused = (4..=6)
            .map(|id| queue.append(owner(id), ClaimNo::new(id), LockMode::Shared, completion()))
            .collect::<Vec<_>>();
        assert_eq!(
            reused.iter().map(|id| id.slot).collect::<Vec<_>>(),
            vec![2, 0, 1]
        );
        assert!(queue_snapshot(&queue).slab.free_order.is_empty());
    }

    #[test]
    fn deterministic_queue_trace_matches_a_vector_and_free_list_model() {
        let mut queue = WaitQueue::default();
        let mut queue_model = Vec::<WaitNodeID>::new();
        let mut free_model = Vec::<usize>::new();
        let mut generations = Vec::<u64>::new();
        let mut random = 0x4d59_5df4_d0f3_3173_u64;
        let mut next_owner = 1_u64;

        for _ in 0..512 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            if queue_model.is_empty() || random & 3 != 0 {
                let expected_slot = if free_model.is_empty() {
                    let slot = generations.len();
                    generations.push(0);
                    slot
                } else {
                    free_model.remove(0)
                };
                let id = queue.append(
                    owner(next_owner),
                    ClaimNo::new(next_owner),
                    LockMode::Shared,
                    completion(),
                );
                next_owner += 1;
                assert_eq!(id.slot, expected_slot);
                assert_eq!(id.generation, generations[expected_slot]);
                queue_model.push(id);
            } else {
                let index = usize::try_from(random).unwrap() % queue_model.len();
                let id = queue_model.remove(index);
                queue.detach_to(id, WaitNodePhase::Released);
                let _ = queue.consume(id, WaitNodePhase::Released);
                generations[id.slot] += 1;
                free_model.insert(0, id.slot);
            }

            let snapshot = queue_snapshot(&queue);
            assert_eq!(snapshot.queue_order, queue_model);
            assert_eq!(snapshot.slab.free_order, free_model);
            assert_eq!(snapshot.slab.generations, generations);
            assert_eq!(snapshot.slab.live_count, queue_model.len());
        }
    }

    #[test]
    fn node_identity_includes_reserved_claim_number() {
        let mut queue = WaitQueue::default();
        let owner = owner(9);
        let resource = LockResource::TableMetadata(TableID::new(9));
        let token = PendingClaimToken {
            resource,
            owner,
            claim_no: ClaimNo::new(17),
        };
        let id = queue.append(owner, ClaimNo::new(17), LockMode::Exclusive, completion());
        queue.assert_identity(id, &token, LockMode::Exclusive);
    }

    #[test]
    fn guard_drop_rolls_back_partial_local_publication_and_fresh_grant() {
        for publish_scope in [false, true] {
            let manager = LockManager::new();
            let mut authority = FamilyLockAuthority::new(SessionID::new(90));
            let resource = LockResource::TableMetadata(TableID::new(90 + u64::from(publish_scope)));
            let (family, curr_scope) = authority.parts();
            let owner = curr_scope.owner();
            let guard_token = PendingClaimToken {
                resource,
                owner,
                claim_no: ClaimNo::new(7),
            };
            assert!(matches!(
                manager
                    .start_pending(&guard_token, LockMode::Shared)
                    .unwrap(),
                PendingStart::Immediate
            ));
            let publication_token = PendingClaimToken {
                resource,
                owner,
                claim_no: ClaimNo::new(7),
            };
            let mut guard = PendingClaimGuard::new(
                &manager,
                family,
                curr_scope,
                guard_token,
                LockMode::Shared,
                false,
            );
            guard.state = PendingGuardState::FreshGranted;
            guard.transfer_started = true;
            guard
                .family
                .publish_pending_family(&publication_token, LockMode::Shared);
            if publish_scope {
                guard
                    .curr_scope
                    .publish_pending_scope(&publication_token, LockMode::Shared);
            }

            drop(guard);

            family.assert_empty();
            curr_scope.assert_cleared();
            assert!(!manager.owner_holds(resource, owner, LockMode::Shared));
            assert!(manager.resources.get(&resource).is_none());
        }
    }

    #[test]
    fn adopted_exact_grant_pins_resource_until_guard_drop() {
        let manager = LockManager::new();
        let resource = LockResource::TableMetadata(TableID::new(92));
        let blocker = owner(91);
        let blocker_token = PendingClaimToken {
            resource,
            owner: blocker,
            claim_no: ClaimNo::new(1),
        };
        assert!(matches!(
            manager
                .start_pending(&blocker_token, LockMode::Exclusive)
                .unwrap(),
            PendingStart::Immediate
        ));

        let mut authority = FamilyLockAuthority::new(SessionID::new(92));
        let (family, curr_scope) = authority.parts();
        let pending = PendingClaimToken {
            resource,
            owner: curr_scope.owner(),
            claim_no: ClaimNo::new(1),
        };
        let PendingStart::Waiting { node_id, .. } =
            manager.start_pending(&pending, LockMode::Shared).unwrap()
        else {
            panic!("conflicting fresh request must wait")
        };
        assert_eq!(manager.release_owner(blocker), 1);
        assert!(matches!(
            manager.observe_pending(&pending, LockMode::Shared, node_id),
            PendingObservation::Adopted
        ));
        let resource_state = manager.resources.get(&resource).unwrap();
        assert_eq!(resource_state.wait_queue.live_count(), 0);
        assert_eq!(resource_state.granted.len(), 1);
        drop(resource_state);

        let mut guard = PendingClaimGuard::new(
            &manager,
            family,
            curr_scope,
            pending,
            LockMode::Shared,
            false,
        );
        guard.state = PendingGuardState::FreshGranted;
        drop(guard);

        family.assert_empty();
        curr_scope.assert_cleared();
        assert!(manager.resources.get(&resource).is_none());
    }
}
