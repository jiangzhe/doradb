//! Bounded, publication-before-check deadlock detection for logical locks.
//!
//! Resource guards protect dependency observation. Family authority keeps an
//! observed edge valid while both exact requests remain Pending. Election locks
//! only the witness decisions, in family/claim order, and never enters a resource
//! or the registry. Normal transitions hold one resource and at most one decision;
//! registry references are always dropped before taking a decision lock.

use super::wait::WaitNodeID;
use super::{LockFamily, LockMode, LockOwner, LockResource};
use crate::completion::Completion;
use crate::error::{OperationError, OperationResult};
use crate::id::ClaimNo;
use crate::map::{FastDashMap, FastHashMap};
#[cfg(feature = "profiling")]
use crate::profiling::{LockManagerStats, add, decrement_current, increment_current};
use dashmap::mapref::entry::Entry;
use error_stack::Report;
use parking_lot::Mutex;
use std::fmt::Write;
use std::iter::once;
use std::mem::replace;
use std::ops::Range;
use std::sync::Arc;

/// Resource-qualified generational queue identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(super) struct WaitKey {
    /// Requested logical resource.
    pub(super) resource: LockResource,
    /// Slot and generation within that resource.
    pub(super) node_id: WaitNodeID,
}

/// Monotonic decision for a single published acquisition attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WaitDecision {
    /// Published and eligible for traversal or promotion.
    Pending,
    /// This checker committed rejection; only cancellation may retire it.
    DeadlockChosen,
    /// Cancelled or promoted; this record can never participate again.
    Retired,
}

/// One allocation shared by the registry, queue, guard, and notification owner.
pub(super) struct PendingWait {
    /// Resource-qualified queue identity.
    pub(super) key: WaitKey,
    /// Exact acquiring scope.
    pub(super) owner: LockOwner,
    /// Checked family-local acquisition identity.
    pub(super) claim_no: ClaimNo,
    /// Immutable requested physical mode.
    pub(super) requested_mode: LockMode,
    /// Monotonic state protected during retirement and election.
    pub(super) decision: Mutex<WaitDecision>,
    /// Success-only notification for a provisional physical grant.
    pub(super) completion: Completion<()>,
}

impl PendingWait {
    /// Initializes the entire record before either queue or registry publication.
    #[inline]
    pub(super) fn new(
        key: WaitKey,
        owner: LockOwner,
        claim_no: ClaimNo,
        requested_mode: LockMode,
    ) -> Self {
        Self {
            key,
            owner,
            claim_no,
            requested_mode,
            decision: Mutex::new(WaitDecision::Pending),
            completion: Completion::new(),
        }
    }

    /// Returns the checked identity shared by resource inspection and traversal.
    #[inline]
    pub(super) fn identity(&self) -> (LockFamily, ClaimNo, WaitKey) {
        (self.owner.family(), self.claim_no, self.key)
    }

    /// Tests traversal eligibility without retaining a decision guard.
    #[inline]
    pub(super) fn is_pending(&self) -> bool {
        *self.decision.lock() == WaitDecision::Pending
    }
}

/// Authoritative resource relationship captured while its guard is held.
#[derive(Clone, Copy, Debug)]
pub(super) enum EdgeReason {
    /// Incompatible physical protection held by this family.
    Holder { family: LockFamily, mode: LockMode },
    /// An earlier live request in the same resource queue.
    Fifo,
}

/// Guarded physical holder with an exact pending acquisition.
pub(super) struct HolderSnapshot {
    /// Exact wait observed while the held-lock relationship is guarded.
    pub(super) target: Arc<PendingWait>,
    /// Counted physical mode at capture time.
    pub(super) mode: LockMode,
}

/// One guarded observation of a resource, consumed by a single search attempt.
pub(super) struct ResourceSnapshot {
    /// Inline first entry avoids a queue allocation for a single waiter.
    pub(super) head: Arc<PendingWait>,
    /// Remaining linked requests in FIFO order, including selected requests.
    pub(super) following: Vec<Arc<PendingWait>>,
    /// Counted holders whose families have pending acquisitions.
    pub(super) holders: Vec<HolderSnapshot>,
}

struct SearchWait {
    wait: Arc<PendingWait>,
    location: Option<(usize, usize)>,
    visited: bool,
}

struct IndexedHolder {
    target: usize,
    mode: LockMode,
}

struct IndexedSnapshot {
    queue_start: usize,
    holders: Range<usize>,
}

struct PrefixEntry {
    target: usize,
    visited: bool,
}

#[derive(Clone, Copy)]
enum SearchVertex {
    Wait(usize),
    Prefix { snapshot: usize, position: usize },
}

struct SearchFrame {
    vertex: SearchVertex,
    next: usize,
    incoming: Option<EdgeReason>,
}

struct WitnessStep<'a> {
    wait: &'a Arc<PendingWait>,
    incoming: Option<EdgeReason>,
}

/// A newly encountered request can postdate a cached resource snapshot.
enum PreparedWait {
    Ready,
    Terminal,
    Retry,
}

#[derive(Default)]
struct SearchState {
    waits: Vec<SearchWait>,
    identities: FastHashMap<(LockFamily, ClaimNo, WaitKey), usize>,
    resources: FastHashMap<LockResource, usize>,
    snapshots: Vec<IndexedSnapshot>,
    queue: Vec<PrefixEntry>,
    holders: Vec<IndexedHolder>,
}

impl SearchState {
    #[inline]
    fn intern(&mut self, wait: Arc<PendingWait>, budget: &mut SearchBudget) -> Result<usize, ()> {
        budget.charge(1)?;
        let identity = wait.identity();
        if let Some(&id) = self.identities.get(&identity) {
            assert!(
                Arc::ptr_eq(&self.waits[id].wait, &wait),
                "deadlock identity must name one shared record: identity={identity:?}"
            );
            return Ok(id);
        }
        let id = self.waits.len();
        self.identities.insert(identity, id);
        self.waits.push(SearchWait {
            wait,
            location: None,
            visited: false,
        });
        Ok(id)
    }

    fn insert_snapshot(
        &mut self,
        resource: LockResource,
        snapshot: ResourceSnapshot,
        budget: &mut SearchBudget,
    ) -> Result<(), ()> {
        budget.charge(1)?;
        let snapshot_id = self.snapshots.len();
        let queue_start = self.queue.len();
        let holders_start = self.holders.len();
        for wait in once(snapshot.head).chain(snapshot.following) {
            budget.charge(1)?;
            let id = self.intern(wait, budget)?;
            self.waits[id].location = Some((snapshot_id, self.queue.len()));
            self.queue.push(PrefixEntry {
                target: id,
                visited: false,
            });
        }
        for holder in snapshot.holders {
            budget.charge(1)?;
            let target = self.intern(holder.target, budget)?;
            self.holders.push(IndexedHolder {
                target,
                mode: holder.mode,
            });
        }
        self.resources.insert(resource, snapshot_id);
        self.snapshots.push(IndexedSnapshot {
            queue_start,
            holders: holders_start..self.holders.len(),
        });
        Ok(())
    }

    #[inline]
    fn prepare_wait(
        &mut self,
        id: usize,
        budget: &mut SearchBudget,
        inspect: &mut impl FnMut(
            &Arc<PendingWait>,
            &mut SearchBudget,
        ) -> Result<Option<ResourceSnapshot>, ()>,
    ) -> Result<PreparedWait, ()> {
        let wait = &self.waits[id];
        if !wait.wait.is_pending() {
            return Ok(PreparedWait::Terminal);
        }
        if wait.location.is_some() {
            return Ok(PreparedWait::Ready);
        }
        let resource = wait.wait.key.resource;
        if self.resources.contains_key(&resource) {
            // The source is still pending but was not in the earlier snapshot.
            // Never attach an old holder observation to this newer request.
            return Ok(PreparedWait::Retry);
        }
        let Some(snapshot) = inspect(&wait.wait, budget)? else {
            return Ok(PreparedWait::Terminal);
        };
        self.insert_snapshot(resource, snapshot, budget)?;
        assert!(
            self.waits[id].location.is_some(),
            "guarded deadlock snapshot must contain its exact requesting record: identity={:?}",
            self.waits[id].wait.identity()
        );
        Ok(PreparedWait::Ready)
    }

    #[inline]
    fn visit(&mut self, vertex: SearchVertex) -> bool {
        let visited = match vertex {
            SearchVertex::Wait(id) => &mut self.waits[id].visited,
            SearchVertex::Prefix { position, .. } => &mut self.queue[position].visited,
        };
        !replace(visited, true)
    }

    #[inline]
    fn next_edge(
        &self,
        frame: &mut SearchFrame,
        budget: &mut SearchBudget,
    ) -> Result<Option<(SearchVertex, Option<EdgeReason>)>, ()> {
        match frame.vertex {
            SearchVertex::Wait(id) => {
                let source = &self.waits[id];
                let (snapshot_id, position) = source.location.unwrap_or_else(|| {
                    unreachable!("only prepared deadlock requests enter the traversal stack")
                });
                let snapshot = &self.snapshots[snapshot_id];
                let holders = &self.holders[snapshot.holders.clone()];
                while let Some(holder) = holders.get(frame.next) {
                    budget.charge(1)?;
                    frame.next += 1;
                    if !super::modes_are_compatible(
                        source.wait.key.resource,
                        holder.mode,
                        source.wait.requested_mode,
                    ) {
                        return Ok(Some((
                            SearchVertex::Wait(holder.target),
                            Some(EdgeReason::Holder {
                                family: self.waits[holder.target].wait.owner.family(),
                                mode: holder.mode,
                            }),
                        )));
                    }
                }
                if frame.next == holders.len() {
                    frame.next += 1;
                    if position > snapshot.queue_start {
                        let position = position - 1;
                        return Ok(Some((
                            SearchVertex::Prefix {
                                snapshot: snapshot_id,
                                position,
                            },
                            None,
                        )));
                    }
                }
                Ok(None)
            }
            SearchVertex::Prefix { snapshot, position } => {
                frame.next += 1;
                match frame.next {
                    1 => Ok(Some((
                        SearchVertex::Wait(self.queue[position].target),
                        Some(EdgeReason::Fifo),
                    ))),
                    2 => Ok(position
                        .checked_sub(1)
                        .filter(|&position| position >= self.snapshots[snapshot].queue_start)
                        .map(|position| (SearchVertex::Prefix { snapshot, position }, None))),
                    _ => Ok(None),
                }
            }
        }
    }

    fn witness<'a>(
        &'a self,
        stack: &[SearchFrame],
        budget: &mut SearchBudget,
    ) -> Result<Vec<WitnessStep<'a>>, ()> {
        let mut path = Vec::new();
        for frame in stack {
            budget.charge(1)?;
            if let SearchVertex::Wait(id) = frame.vertex {
                path.push(WitnessStep {
                    wait: &self.waits[id].wait,
                    incoming: frame.incoming,
                });
            }
        }
        Ok(path)
    }
}

/// Every scan, expansion, and witness validation shares this one request budget.
pub(super) struct SearchBudget {
    limit: usize,
    work: usize,
}

impl SearchBudget {
    /// Charges before scanning or allocating; exhaustion rejects the entering request.
    #[inline]
    pub(super) fn charge(&mut self, work: usize) -> Result<(), ()> {
        if work > self.limit - self.work {
            self.work = self.limit;
            return Err(());
        }
        self.work += work;
        Ok(())
    }
}

/// Owns pending attempts and cycle election independently of physical resource storage.
///
/// Resource transitions call registration and retirement under their guards.
/// Checking borrows a synchronous inspector that captures exact dependencies
/// under those guards, charges scan work, and releases every guard before returning.
/// The detector retains no reference to the resource owner.
pub(super) struct DeadlockDetector {
    work_limit: usize,
    pending: FastDashMap<LockFamily, Arc<PendingWait>>,
    #[cfg(feature = "profiling")]
    stats: Arc<LockManagerStats>,
}

impl DeadlockDetector {
    /// Creates a registry with a validated startup allowance and optional shared statistics.
    #[inline]
    pub(super) fn new(
        work_limit: usize,
        #[cfg(feature = "profiling")] stats: Arc<LockManagerStats>,
    ) -> Self {
        Self {
            work_limit,
            pending: FastDashMap::default(),
            #[cfg(feature = "profiling")]
            stats,
        }
    }

    /// Captures a registration without retaining its map guard during decision locking.
    /// The caller must still hold the resource guard that established the holder edge.
    #[inline]
    pub(super) fn pending_for(&self, family: LockFamily) -> Option<Arc<PendingWait>> {
        self.pending
            .get(&family)
            .map(|entry| Arc::clone(entry.value()))
    }

    /// Publishes under the resource guard, before any detector can inspect the node.
    pub(super) fn register_pending(&self, pending: &Arc<PendingWait>) {
        match self.pending.entry(pending.owner.family()) {
            Entry::Vacant(entry) => {
                #[cfg(feature = "profiling")]
                increment_current(
                    &self.stats.current_pending_registrations,
                    &self.stats.peak_pending_registrations,
                );
                entry.insert(Arc::clone(pending));
            }
            Entry::Occupied(_) => panic!(
                "exclusive family authority permits only one pending lock: owner={}, claim={:?}",
                pending.owner, pending.claim_no
            ),
        }
    }

    /// Retires under the resource and this record's decision guard, before resumption.
    pub(super) fn retire_pending(&self, pending: &Arc<PendingWait>, decision: &mut WaitDecision) {
        assert_ne!(
            *decision,
            WaitDecision::Retired,
            "logical wait cannot retire twice: key={:?}, owner={}",
            pending.key,
            pending.owner
        );
        *decision = WaitDecision::Retired;
        let removed = self
            .pending
            .remove_if(&pending.owner.family(), |_, current| {
                Arc::ptr_eq(current, pending) && current.identity() == pending.identity()
            });
        assert!(
            removed.is_some(),
            "logical wait retirement lost its exact registration: key={:?}, owner={}",
            pending.key,
            pending.owner
        );
        #[cfg(feature = "profiling")]
        decrement_current(&self.stats.current_pending_registrations);
    }

    /// Checks the already published request with its rollback guard armed.
    /// The inspector charges physical entries, queue nodes, and snapshot storage
    /// before growing its output, and returns without resource or registry guards.
    pub(super) fn check(
        &self,
        root: &Arc<PendingWait>,
        mut inspect: impl FnMut(
            &Arc<PendingWait>,
            &mut SearchBudget,
        ) -> Result<Option<ResourceSnapshot>, ()>,
    ) -> OperationResult<()> {
        let limit = self.work_limit;
        #[cfg(feature = "profiling")]
        add(&self.stats.deadlock_initial_checks, 1);
        let mut budget = SearchBudget { limit, work: 0 };
        let result = self.search_deadlock(root, &mut budget, &mut inspect);
        #[cfg(feature = "profiling")]
        add(&self.stats.deadlock_examined_work, budget.work as u64);
        let (error, details) = match result {
            Ok(None) => return Ok(()),
            Ok(Some(witness)) => (OperationError::Deadlock, witness),
            Err(()) => {
                // Promotion can race scanning or budget exhaustion. Its ordinary
                // success-only completion owns the result once the root retires.
                if *root.decision.lock() == WaitDecision::Retired {
                    return Ok(());
                }
                #[cfg(feature = "profiling")]
                add(&self.stats.deadlock_check_limits, 1);
                (
                    OperationError::DeadlockCheckLimit,
                    format!("budget={limit}, work={}", budget.work),
                )
            }
        };
        Err(Report::new(error).attach(format!(
            "owner={}, resource={}, mode={}, claim={:?}, node={:?}; {details}",
            root.owner, root.key.resource, root.requested_mode, root.claim_no, root.key.node_id
        )))
    }

    fn search_deadlock(
        &self,
        root: &Arc<PendingWait>,
        budget: &mut SearchBudget,
        inspect: &mut impl FnMut(
            &Arc<PendingWait>,
            &mut SearchBudget,
        ) -> Result<Option<ResourceSnapshot>, ()>,
    ) -> Result<Option<String>, ()> {
        // Each attempt starts after root publication. All waits in a persistent
        // cycle completed by this publication are already visible to snapshots.
        // Later publications run their own initial checks. A captured relation
        // stays valid while both exact endpoints remain Pending, even if other
        // queue entries retire. Retry discards ALL snapshots and visited state.
        'retry: loop {
            if !root.is_pending() {
                return Ok(None);
            }
            let Some(snapshot) = inspect(root, budget)? else {
                return Ok(None);
            };
            // A head request with no pending holders has no outgoing edge.
            // Avoid allocating traversal state for this common first waiter.
            if snapshot.holders.is_empty() && Arc::ptr_eq(&snapshot.head, root) {
                return Ok(None);
            }
            let mut state = SearchState::default();
            let root_id = state.intern(Arc::clone(root), budget)?;
            state.insert_snapshot(root.key.resource, snapshot, budget)?;
            match state.prepare_wait(root_id, budget, inspect)? {
                PreparedWait::Terminal => return Ok(None),
                PreparedWait::Retry => {
                    unreachable!("guarded root snapshot must contain its exact request")
                }
                PreparedWait::Ready => {}
            }
            budget.charge(1)?;
            state.visit(SearchVertex::Wait(root_id));
            let mut stack = vec![SearchFrame {
                vertex: SearchVertex::Wait(root_id),
                next: 0,
                incoming: None,
            }];
            while let Some(frame) = stack.last_mut() {
                let Some((target, incoming)) = state.next_edge(frame, budget)? else {
                    stack.pop();
                    continue;
                };
                budget.charge(1)?;
                if matches!(target, SearchVertex::Wait(id) if id == root_id) {
                    #[cfg(feature = "profiling")]
                    add(&self.stats.deadlock_candidate_cycles, 1);
                    let path = state.witness(&stack, budget)?;
                    if self.elect_deadlock(&path, budget)? {
                        return Ok(Some(cycle_witness(
                            &path,
                            incoming.unwrap_or_else(|| {
                                unreachable!(
                                    "edges to actual waits always carry a physical or FIFO reason"
                                )
                            }),
                        )));
                    }
                    #[cfg(feature = "profiling")]
                    add(&self.stats.deadlock_invalidated_witnesses, 1);
                    continue 'retry;
                }
                if !state.visit(target) {
                    continue;
                }
                budget.charge(1)?;
                if let SearchVertex::Wait(id) = target {
                    match state.prepare_wait(id, budget, inspect)? {
                        PreparedWait::Terminal => continue,
                        PreparedWait::Retry => continue 'retry,
                        PreparedWait::Ready => {}
                    }
                }
                // Prefix traversal does not depend on the intermediate wait's
                // decision: selected/retired requests cannot hide earlier waits.
                stack.push(SearchFrame {
                    vertex: target,
                    next: 0,
                    incoming,
                });
            }
            return Ok(None);
        }
    }

    fn elect_deadlock(
        &self,
        path: &[WitnessStep<'_>],
        budget: &mut SearchBudget,
    ) -> Result<bool, ()> {
        // Charge the allocation, sort bound, and validation before taking locks.
        let sort_work = path.len() * (path.len().ilog2() as usize + 2);
        budget.charge(sort_work)?;
        // Traversal inserts each exact identity once, so the path is already unique.
        let mut records: Vec<_> = path.iter().map(|frame| frame.wait).collect();
        records.sort_unstable_by_key(|wait| (wait.owner.family(), wait.claim_no));
        #[cfg(test)]
        tests::before_election(path);
        let mut decisions: Vec<_> = records.iter().map(|wait| wait.decision.lock()).collect();
        if decisions
            .iter()
            .any(|decision| **decision != WaitDecision::Pending)
        {
            return Ok(false);
        }
        let root = path[0].wait;
        for (record, decision) in records.iter().zip(&mut decisions) {
            if Arc::ptr_eq(record, root) {
                **decision = WaitDecision::DeadlockChosen;
                break;
            }
        }
        drop(decisions);
        #[cfg(feature = "profiling")]
        add(&self.stats.deadlock_elections, 1);
        #[cfg(test)]
        tests::after_election();
        Ok(true)
    }
}

fn cycle_witness(path: &[WitnessStep<'_>], closing: EdgeReason) -> String {
    let mut witness = String::from("cycle:");
    for (idx, frame) in path.iter().enumerate() {
        let (target, reason) = if let Some(next) = path.get(idx + 1) {
            (
                next.wait,
                next.incoming
                    .unwrap_or_else(|| unreachable!("non-root frame has an incoming edge")),
            )
        } else {
            (path[0].wait, closing)
        };
        let _ = write!(
            witness,
            " [{} claim={:?} requests {} mode={} -> {} claim={:?} via ",
            frame.wait.owner,
            frame.wait.claim_no,
            frame.wait.key.resource,
            frame.wait.requested_mode,
            target.owner,
            target.claim_no
        );
        match reason {
            EdgeReason::Holder { family, mode } => {
                let _ = write!(witness, "physical holder family={family} mode={mode}]");
            }
            EdgeReason::Fifo => witness.push_str("FIFO predecessor]"),
        }
    }
    witness
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::catalog::storage::layout::BUILTIN_CATALOG_TABLE_IDS;
    use crate::conf::{DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT, LockManagerConfig};
    use crate::error::{DiscloseError, ErrorKind, OperationOrFatalError};
    use crate::id::{SessionID, TableID};
    use crate::lock::claim::PendingClaimToken;
    use crate::lock::tests::{TestLockOwner, debug_snapshot, inspect_resource};
    use crate::lock::wait::tests::{PendingClaimTestPhase, poison_pending_claim_at};
    use crate::lock::{FamilyLockAuthority, FreshClaimsGuard, LockManager, PendingStart};
    use crate::poison::EnginePoisoner;
    use futures::poll;
    use std::cell::{Cell, RefCell};
    use std::iter::repeat_n;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Barrier;
    use std::sync::mpsc::channel;
    use std::thread;

    /// Direct arbitration fixture for schedules paused between publication and check.
    /// All accepted holders are installed before their family starts waiting.
    #[derive(Default)]
    struct Fixture {
        manager: Arc<LockManager>,
        holders: Vec<(LockResource, LockOwner, ClaimNo, LockMode)>,
        waits: Vec<Arc<PendingWait>>,
        next_claim: u64,
    }

    impl Fixture {
        fn with_limit(limit: usize) -> Self {
            Self {
                manager: Arc::new(
                    LockManager::with_config(
                        LockManagerConfig::default().deadlock_search_work_limit(limit),
                    )
                    .unwrap(),
                ),
                holders: Vec::new(),
                waits: Vec::new(),
                next_claim: 0,
            }
        }

        fn hold(&mut self, family: u64, resource: LockResource, mode: LockMode) {
            self.next_claim += 1;
            let token = PendingClaimToken {
                resource,
                owner: owner(family),
                claim_no: ClaimNo::new(self.next_claim),
            };
            assert!(matches!(
                self.manager.start_pending(&token, mode).unwrap(),
                PendingStart::Immediate
            ));
            self.holders
                .push((resource, token.owner, token.claim_no, mode));
        }

        fn queue(
            &mut self,
            family: u64,
            resource: LockResource,
            mode: LockMode,
        ) -> Arc<PendingWait> {
            self.next_claim += 1;
            let token = PendingClaimToken {
                resource,
                owner: owner(family),
                claim_no: ClaimNo::new(self.next_claim),
            };
            let PendingStart::Waiting { pending, .. } =
                self.manager.start_pending(&token, mode).unwrap()
            else {
                panic!("fixture requires a blocked request");
            };
            self.waits.push(Arc::clone(&pending));
            pending
        }

        fn graph(edges: &[Vec<usize>]) -> Self {
            Self::graph_with_limit(edges, DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT)
        }

        fn graph_with_limit(edges: &[Vec<usize>], limit: usize) -> Self {
            let mut fixture = Self::with_limit(limit);
            for (source, targets) in edges.iter().enumerate() {
                // Idle shared holders keep even terminal vertices queued.
                fixture.hold(
                    1000 + source as u64,
                    resource(source as u64),
                    LockMode::Shared,
                );
                for &target in targets {
                    fixture.hold(1 + target as u64, resource(source as u64), LockMode::Shared);
                }
            }
            for source in 0..edges.len() {
                fixture.queue(
                    1 + source as u64,
                    resource(source as u64),
                    LockMode::Exclusive,
                );
            }
            fixture
        }

        fn fifo(width: usize) -> Self {
            let mut fixture = Self::default();
            fixture.hold(10_000, resource(0), LockMode::Exclusive);
            for family in 1..=width {
                fixture.queue(family as u64, resource(0), LockMode::Shared);
            }
            fixture
        }

        fn release(&self, family: u64, resource: LockResource) {
            let &(_, owner, claim_no, mode) = self
                .holders
                .iter()
                .find(|(r, o, _, _)| {
                    *r == resource && o.family() == super::LockFamily::new(SessionID::new(family))
                })
                .unwrap();
            self.manager.cancel_fresh_grant(
                PendingClaimToken {
                    resource,
                    owner,
                    claim_no,
                },
                mode,
            );
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            for wait in &self.waits {
                let live = inspect_resource(&self.manager, wait.key.resource, |state| {
                    state
                        .wait_queue
                        .try_node(wait.key.node_id)
                        .is_some_and(|node| Arc::ptr_eq(&node.pending, wait))
                })
                .unwrap_or(false);
                if live {
                    cancel(&self.manager, wait);
                }
                assert_eq!(*wait.decision.lock(), WaitDecision::Retired);
            }
            for &(resource, owner, claim_no, mode) in &self.holders {
                if inspect_resource(&self.manager, resource, |state| {
                    state.families.contains_key(&owner.family())
                })
                .unwrap_or(false)
                {
                    self.manager.cancel_fresh_grant(
                        PendingClaimToken {
                            resource,
                            owner,
                            claim_no,
                        },
                        mode,
                    );
                }
            }
            assert!(self.manager.deadlock.pending.is_empty());
            assert_eq!(debug_snapshot(&self.manager).entries, []);
            #[cfg(feature = "profiling")]
            assert_eq!(self.manager.stats().current_pending_registrations, 0);
        }
    }

    type ElectionHook = Box<dyn FnOnce(&[WitnessStep<'_>])>;

    thread_local! {
        static PANIC_AFTER_ELECTION: Cell<bool> = const { Cell::new(false) };
        static ELECTION_HOOK: RefCell<Option<ElectionHook>> = RefCell::new(None);
    }

    /// Inspects registry drain without exposing the detector's map to its owner.
    pub(in crate::lock) fn pending_count(detector: &DeadlockDetector) -> usize {
        detector.pending.len()
    }

    pub(super) fn after_election() {
        assert!(
            !PANIC_AFTER_ELECTION.take(),
            "detector unwind after election"
        );
    }

    pub(super) fn before_election(path: &[WitnessStep<'_>]) {
        let hook = ELECTION_HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook(path);
        }
    }

    fn owner(family: u64) -> LockOwner {
        LockOwner::session_explicit(SessionID::new(family))
    }

    fn resource(id: u64) -> LockResource {
        LockResource::TableData(TableID::new(100_000 + id))
    }

    fn token(wait: &PendingWait) -> PendingClaimToken {
        PendingClaimToken {
            resource: wait.key.resource,
            owner: wait.owner,
            claim_no: wait.claim_no,
        }
    }

    fn cancel(manager: &LockManager, wait: &PendingWait) {
        manager.cancel_waiting(token(wait), wait.requested_mode, wait.key.node_id);
    }

    fn assert_deadlock(manager: &LockManager, root: &Arc<PendingWait>) -> String {
        let error = manager.check_deadlock(root).unwrap_err();
        assert_eq!(*error.current_context(), OperationError::Deadlock);
        assert_eq!(*root.decision.lock(), WaitDecision::DeadlockChosen);
        let detail = format!("{error:?}");
        assert!(detail.contains("cycle:"));
        assert!(detail.contains("claim="));
        detail
    }

    /// Purpose: Measure detector checks separately from fixture construction and cleanup in release builds.
    /// Expected: Stable acyclic fixtures remain pending, cycles elect their entrant, and measurements distinguish budget rejections.
    #[cfg(feature = "profiling")]
    #[test]
    #[ignore = "manual release-mode performance comparison"]
    fn benchmark_deadlock_search() {
        use std::hint::black_box;
        use std::time::Instant;

        fn measure(name: &str, fixture: &Fixture, root: usize, samples: usize) {
            for _ in 0..32 {
                let _ = black_box(fixture.manager.check_deadlock(&fixture.waits[root]));
            }
            let before = fixture.manager.stats();
            let mut nanos = Vec::new();
            let mut limited = 0;
            for _ in 0..samples {
                let started = Instant::now();
                let result = black_box(fixture.manager.check_deadlock(&fixture.waits[root]));
                nanos.push(started.elapsed().as_nanos());
                if let Err(error) = result {
                    assert_eq!(*error.current_context(), OperationError::DeadlockCheckLimit);
                    limited += 1;
                }
            }
            nanos.sort_unstable();
            let after = fixture.manager.stats();
            assert!(fixture.waits.iter().all(|wait| wait.is_pending()));
            println!(
                "DEADLOCK_BENCH {name} median_ns={} p95_ns={} p99_ns={} work={} limited={limited}/{samples}",
                nanos[samples / 2],
                nanos[samples * 95 / 100],
                nanos[samples * 99 / 100],
                (after.deadlock_examined_work - before.deadlock_examined_work) / samples as u64,
            );
        }

        for width in [1, 8, 32, 64, 128, 256, 512] {
            let fixture = Fixture::fifo(width);
            measure(&format!("queue-{width}"), &fixture, width - 1, 1000);
        }
        let mut chain = vec![Vec::new(); 64];
        for (i, edges) in chain.iter_mut().enumerate().take(63) {
            edges.push(i + 1);
        }
        measure("chain-64", &Fixture::graph(&chain), 0, 1000);
        let mut wide = vec![Vec::new(); 64];
        wide[0].extend(1..64);
        measure("holders-63", &Fixture::graph(&wide), 0, 1000);
        let mut nanos = Vec::new();
        for _ in 0..1000 {
            let fixture = Fixture::graph(&[vec![1], vec![0]]);
            let started = Instant::now();
            let error = black_box(fixture.manager.check_deadlock(&fixture.waits[0])).unwrap_err();
            nanos.push(started.elapsed().as_nanos());
            assert_eq!(*error.current_context(), OperationError::Deadlock);
        }
        nanos.sort_unstable();
        println!(
            "DEADLOCK_BENCH cycle-2 median_ns={} p95_ns={} p99_ns={}",
            nanos[500], nanos[950], nanos[990]
        );
    }

    /// Purpose: Reuse one resource observation across the user's shared Y queue and its FIFO branches.
    /// Expected: F inspects only Y once, while A inspects X and Y once each without electing an acyclic waiter.
    #[test]
    fn shared_queue_uses_one_snapshot_per_resource() {
        let mut fixture = Fixture::default();
        fixture.hold(2, resource(0), LockMode::Exclusive);
        fixture.hold(3, resource(1), LockMode::Exclusive);
        fixture.queue(1, resource(0), LockMode::Shared);
        for family in [2, 4, 5, 6] {
            fixture.queue(family, resource(1), LockMode::Shared);
        }
        for (root, expected) in [(4, vec![resource(1)]), (0, vec![resource(0), resource(1)])] {
            let mut captured = Vec::new();
            let mut budget = SearchBudget {
                work: 0,
                limit: DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT,
            };
            let result = fixture
                .manager
                .deadlock
                .search_deadlock(&fixture.waits[root], &mut budget, &mut |source, budget| {
                    captured.push(source.key.resource);
                    let snapshot = fixture.manager.wait_snapshot(source, budget)?;
                    if source.key.resource == resource(1) {
                        let snapshot = snapshot.as_ref().unwrap();
                        assert_eq!(
                            once(&snapshot.head)
                                .chain(&snapshot.following)
                                .map(|wait| wait.owner)
                                .collect::<Vec<_>>(),
                            [owner(2), owner(4), owner(5), owner(6)]
                        );
                        assert!(
                            snapshot.holders.is_empty(),
                            "idle C has no pending detector vertex"
                        );
                    }
                    Ok(snapshot)
                })
                .unwrap();
            assert!(result.is_none());
            assert_eq!(captured, expected);
            assert!(fixture.waits.iter().all(|wait| wait.is_pending()));
        }
    }

    /// Purpose: Bound work for an acyclic queue whose explicit FIFO dependency count grows quadratically.
    /// Expected: Doubling queue width doubles charged work within a constant allowance, and 512 waiters fit the production budget.
    #[test]
    fn fifo_search_work_scales_linearly() {
        let mut previous = None;
        for width in [32, 64, 128, 256, 512] {
            let fixture = Fixture::fifo(width);
            let mut captures = 0;
            let mut budget = SearchBudget {
                work: 0,
                limit: DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT,
            };
            let result = fixture
                .manager
                .deadlock
                .search_deadlock(
                    fixture.waits.last().unwrap(),
                    &mut budget,
                    &mut |source, budget| {
                        captures += 1;
                        fixture.manager.wait_snapshot(source, budget)
                    },
                )
                .unwrap();
            assert!(result.is_none());
            assert_eq!(captures, 1, "width={width}");
            if let Some(previous) = previous {
                assert!(
                    budget.work <= 2 * previous + 32,
                    "width={width}, work={}, previous={previous}",
                    budget.work
                );
            }
            previous = Some(budget.work);
        }
    }

    /// Purpose: Reach a request published on an already snapshotted resource through a different holder branch.
    /// Expected: The cache miss restarts with fresh snapshots, exposes the entrant's cycle, and never replenishes the budget.
    #[test]
    fn newer_request_restarts_resource_snapshots_with_remaining_budget() {
        fn run(limit: usize) -> (OperationResult<()>, usize, usize) {
            let mut fixture = Fixture::with_limit(limit);
            fixture.hold(10, resource(0), LockMode::Shared);
            fixture.hold(2, resource(0), LockMode::Shared);
            fixture.hold(3, resource(1), LockMode::Shared);
            fixture.queue(2, resource(1), LockMode::Exclusive);
            let root = fixture.queue(1, resource(0), LockMode::Exclusive);
            let manager = Arc::clone(&fixture.manager);
            let mut captures = 0;
            let mut retry_work = 0;
            let result = manager.deadlock.check(&root, |source, budget| {
                if source.key.resource == resource(0) {
                    captures += 1;
                    if captures == 2 {
                        retry_work = budget.work;
                    }
                }
                let snapshot = manager.wait_snapshot(source, budget)?;
                if source.key.resource == resource(0) && captures == 1 {
                    fixture.queue(3, resource(0), LockMode::Exclusive);
                }
                Ok(snapshot)
            });
            (result, captures, retry_work)
        }
        let (result, captures, retry_work) = run(DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT);
        assert_eq!(
            *result.unwrap_err().current_context(),
            OperationError::Deadlock
        );
        assert_eq!(captures, 2);
        assert!(retry_work > 0);
        let (result, captures, limited_retry_work) = run(retry_work + 1);
        assert_eq!(
            *result.unwrap_err().current_context(),
            OperationError::DeadlockCheckLimit
        );
        assert_eq!(captures, 2);
        assert_eq!(limited_retry_work, retry_work);
    }

    /// Purpose: Remove an intermediate FIFO waiter after capture while an earlier predecessor still closes a cycle.
    /// Expected: Prefix traversal bypasses the retired record and the projected witness elects only live requests.
    #[test]
    fn retired_snapshot_predecessor_does_not_hide_earlier_cycle() {
        let mut fixture = Fixture::default();
        fixture.hold(1, resource(0), LockMode::Shared);
        fixture.hold(3, resource(1), LockMode::Shared);
        fixture.hold(4, resource(1), LockMode::Shared);
        fixture.queue(2, resource(0), LockMode::Exclusive);
        fixture.queue(1, resource(1), LockMode::Exclusive);
        let middle = fixture.queue(3, resource(0), LockMode::Shared);
        let root = fixture.queue(4, resource(0), LockMode::Shared);
        let mut retired = false;
        let error = fixture
            .manager
            .deadlock
            .check(&root, |source, budget| {
                let snapshot = fixture.manager.wait_snapshot(source, budget)?;
                if !retired && source.key.resource == resource(0) {
                    cancel(&fixture.manager, &middle);
                    retired = true;
                }
                Ok(snapshot)
            })
            .unwrap_err();
        assert_eq!(*error.current_context(), OperationError::Deadlock);
        assert_eq!(*middle.decision.lock(), WaitDecision::Retired);
        let detail = format!("{error:?}");
        assert!(detail.contains("FIFO predecessor"));
        assert!(!detail.contains("Prefix"));
    }

    /// Purpose: Publish an idle holder's wait after another acquisition has captured that holder's resource.
    /// Expected: The later entrant's fresh initial check detects the cycle even though the earlier snapshot omitted the idle holder.
    #[test]
    fn idle_holder_later_waits_and_checks_fresh_snapshots() {
        let mut fixture = Fixture::default();
        fixture.hold(1, resource(0), LockMode::Exclusive);
        fixture.hold(2, resource(1), LockMode::Exclusive);
        let root = fixture.queue(1, resource(1), LockMode::Exclusive);
        let manager = Arc::clone(&fixture.manager);
        let mut later = None;
        manager
            .deadlock
            .check(&root, |source, budget| {
                let snapshot = manager.wait_snapshot(source, budget)?;
                assert!(snapshot.as_ref().unwrap().holders.is_empty());
                let new = fixture.queue(2, resource(0), LockMode::Exclusive);
                assert_deadlock(&manager, &new);
                later = Some(new);
                Ok(snapshot)
            })
            .unwrap();
        assert!(root.is_pending());
        assert_eq!(
            *later.unwrap().decision.lock(),
            WaitDecision::DeadlockChosen
        );
    }

    /// Purpose: Recreate a snapshotted resource before another branch reaches its replacement queue.
    /// Expected: Reused node coordinates cannot rebind old observations; a fresh attempt finds the new live cycle.
    #[test]
    fn recreated_resource_refreshes_exact_snapshot_membership() {
        let mut fixture = Fixture::default();
        fixture.hold(2, resource(0), LockMode::Shared);
        fixture.hold(4, resource(0), LockMode::Shared);
        fixture.hold(10, resource(1), LockMode::Exclusive);
        fixture.hold(3, resource(2), LockMode::Exclusive);
        let old = fixture.queue(2, resource(1), LockMode::Exclusive);
        fixture.queue(4, resource(2), LockMode::Exclusive);
        let root = fixture.queue(1, resource(0), LockMode::Exclusive);
        let manager = Arc::clone(&fixture.manager);
        let mut replacement = None;
        let mut captures = 0;
        let error = manager
            .deadlock
            .check(&root, |source, budget| {
                let mut snapshot = manager.wait_snapshot(source, budget)?.unwrap();
                // Choose the branch that establishes the stale snapshot first.
                snapshot
                    .holders
                    .sort_unstable_by_key(|holder| holder.target.owner.family());
                if source.key.resource == resource(1) {
                    captures += 1;
                    if replacement.is_none() {
                        cancel(&manager, &old);
                        fixture.release(2, resource(0));
                        fixture.release(10, resource(1));
                        fixture.hold(5, resource(1), LockMode::Exclusive);
                        fixture.queue(5, resource(0), LockMode::Exclusive);
                        let new = fixture.queue(3, resource(1), LockMode::Exclusive);
                        assert_eq!(
                            new.key, old.key,
                            "whole-resource recreation reuses slab coordinates"
                        );
                        replacement = Some(new);
                    }
                }
                Ok(Some(snapshot))
            })
            .unwrap_err();
        assert_eq!(*error.current_context(), OperationError::Deadlock);
        assert_eq!(captures, 2);
        assert_eq!(*old.decision.lock(), WaitDecision::Retired);
        assert!(replacement.unwrap().is_pending());
    }

    /// Purpose: Break reverse acquisitions through the real family authority on fixed, dynamic, and mixed resources.
    /// Expected: Only the entering acquisition fails, older claims survive, and ordinary release lets the peer finish.
    #[test]
    fn reverse_pair_retains_claims_and_releases_peer() {
        for [first, second] in [
            [resource(1), resource(2)],
            [
                LockResource::TableMetadata(BUILTIN_CATALOG_TABLE_IDS[0]),
                LockResource::TableMetadata(BUILTIN_CATALOG_TABLE_IDS[1]),
            ],
            [
                LockResource::TableMetadata(BUILTIN_CATALOG_TABLE_IDS[0]),
                resource(2),
            ],
        ] {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut a = TestLockOwner::new(owner(1));
                let mut b = TestLockOwner::new(owner(2));
                a.acquire(&manager, first, LockMode::Exclusive)
                    .await
                    .unwrap();
                b.acquire(&manager, second, LockMode::Exclusive)
                    .await
                    .unwrap();
                let mut peer = Box::pin(a.acquire(&manager, second, LockMode::Exclusive));
                assert!(poll!(peer.as_mut()).is_pending());
                let error = b
                    .acquire(&manager, first, LockMode::Exclusive)
                    .await
                    .unwrap_err()
                    .disclose();
                assert_eq!(error.kind(), ErrorKind::Operation);
                assert_eq!(error.operation_error(), Some(OperationError::Deadlock));
                assert_eq!(manager.deadlock.pending.len(), 1);
                assert!(
                    poll!(peer.as_mut()).is_pending(),
                    "victim's older claim must remain held"
                );
                assert!(b.release(&manager, second));
                peer.await.unwrap();
                a.close(&manager);
                b.close(&manager);
                assert!(manager.deadlock.pending.is_empty());
                assert_eq!(debug_snapshot(&manager).entries, []);
            });
        }
    }

    /// Purpose: Search every incompatible holder and distinguish foreign cycles from cycles containing the entrant.
    /// Expected: Idle and repeated foreign branches cannot hide the entrant's own cycle, and an outsider is never elected.
    #[test]
    fn branching_long_cycles_and_foreign_cycles() {
        for (name, edges, root, expected) in [
            ("long cycle", vec![vec![1], vec![2], vec![0]], 2, true),
            ("idle branch", vec![vec![1, 2], vec![], vec![0]], 0, true),
            ("foreign cycle", vec![vec![1], vec![2], vec![1]], 0, false),
            (
                "foreign and own",
                vec![vec![1, 3], vec![2], vec![1], vec![0]],
                0,
                true,
            ),
            (
                "convergence",
                vec![vec![1, 2], vec![3], vec![3], vec![]],
                0,
                false,
            ),
        ] {
            let fixture = Fixture::graph(&edges);
            let result = fixture.manager.check_deadlock(&fixture.waits[root]);
            assert_eq!(result.is_err(), expected, "case={name}, result={result:?}");
            if let Err(error) = result {
                assert_eq!(
                    *error.current_context(),
                    OperationError::Deadlock,
                    "case={name}"
                );
            }
        }
    }

    /// Purpose: Include FIFO predecessors even when a later request is compatible with existing holders.
    /// Expected: The cycle witness names FIFO and a chosen intermediate predecessor cannot hide an earlier live cycle.
    #[test]
    fn fifo_and_selected_predecessor_preserve_complete_dependencies() {
        let mut fixture = Fixture::default();
        fixture.hold(1, resource(0), LockMode::Shared);
        fixture.hold(3, resource(1), LockMode::Shared);
        fixture.hold(4, resource(1), LockMode::Shared);
        fixture.queue(2, resource(0), LockMode::Exclusive);
        fixture.queue(1, resource(1), LockMode::Exclusive);
        let selected = fixture.queue(3, resource(0), LockMode::Shared);
        let detail = assert_deadlock(&fixture.manager, &selected);
        assert!(detail.contains("FIFO predecessor"));
        let root = fixture.queue(4, resource(0), LockMode::Shared);
        assert!(assert_deadlock(&fixture.manager, &root).contains("FIFO predecessor"));
        assert_eq!(*selected.decision.lock(), WaitDecision::DeadlockChosen);
        assert!(selected.completion.completed_result().is_none());
    }

    /// Purpose: Coordinate initial checks only after both reverse requests have been published.
    /// Expected: Exactly one checker wins the same cycle and the other succeeds without a second election.
    #[test]
    fn concurrent_publication_elects_exactly_one_breaker() {
        for _ in 0..32 {
            let fixture = Fixture::graph(&[vec![1], vec![0]]);
            let barrier = Arc::new(Barrier::new(2));
            let results = thread::scope(|scope| {
                let handles: Vec<_> = fixture
                    .waits
                    .iter()
                    .map(|root| {
                        let barrier = Arc::clone(&barrier);
                        let manager = &fixture.manager;
                        scope.spawn(move || {
                            ELECTION_HOOK.with(|hook| {
                                *hook.borrow_mut() = Some(Box::new(move |_| {
                                    barrier.wait();
                                }))
                            });
                            manager.check_deadlock(root)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect::<Vec<_>>()
            });
            assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
            assert_eq!(
                fixture
                    .waits
                    .iter()
                    .filter(|wait| *wait.decision.lock() == WaitDecision::DeadlockChosen)
                    .count(),
                1
            );
            for result in results {
                if let Err(error) = result {
                    assert_eq!(*error.current_context(), OperationError::Deadlock);
                }
            }
        }
    }

    /// Purpose: Retire a captured witness member before election while a different cycle remains live.
    /// Expected: The stale witness cannot win; retry shares the original budget and can still elect a surviving cycle.
    #[test]
    fn invalidated_witness_retries_without_hiding_other_branches() {
        for limit in [DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT, 37] {
            let edges = if limit == DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT {
                vec![vec![1, 2], vec![0], vec![0]]
            } else {
                vec![vec![1], vec![0]]
            };
            let fixture = Fixture::graph_with_limit(&edges, limit);
            let manager = Arc::clone(&fixture.manager);
            ELECTION_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |path| {
                    cancel(&manager, path[1].wait);
                }))
            });
            let result = fixture.manager.check_deadlock(&fixture.waits[0]);
            assert!(
                ELECTION_HOOK.with(|hook| hook.borrow().is_none()),
                "must reach validation before exhausting the budget"
            );
            let expected = if limit == DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT {
                OperationError::Deadlock
            } else {
                OperationError::DeadlockCheckLimit
            };
            assert_eq!(*result.unwrap_err().current_context(), expected);
            #[cfg(feature = "profiling")]
            assert_eq!(fixture.manager.stats().deadlock_invalidated_witnesses, 1);
        }
    }

    /// Purpose: Preserve monotonic identities across slab reuse and complete dynamic-resource recreation.
    /// Expected: Old observations find no dependency and cannot remove or select a replacement registration.
    #[test]
    fn stale_records_never_rebind_to_reused_nodes() {
        for recreate in [false, true] {
            let mut fixture = Fixture::default();
            fixture.hold(9, resource(0), LockMode::Exclusive);
            let old = fixture.queue(1, resource(0), LockMode::Exclusive);
            cancel(&fixture.manager, &old);
            if recreate {
                fixture.release(9, resource(0));
                fixture.hold(8, resource(0), LockMode::Exclusive);
            }
            let new = fixture.queue(1, resource(0), LockMode::Exclusive);
            assert_eq!(old.key.node_id.slot, new.key.node_id.slot);
            assert_eq!(
                old.key.node_id.generation == new.key.node_id.generation,
                recreate
            );
            let mut budget = SearchBudget {
                work: 0,
                limit: 100,
            };
            assert!(
                fixture
                    .manager
                    .wait_snapshot(&old, &mut budget)
                    .unwrap()
                    .is_none()
            );
            fixture.manager.check_deadlock(&old).unwrap();
            assert!(Arc::ptr_eq(
                fixture
                    .manager
                    .deadlock
                    .pending
                    .get(&new.owner.family())
                    .unwrap()
                    .value(),
                &new
            ));
            assert!(new.is_pending());
        }
    }

    /// Purpose: Serialize promotion against the decision of a queued deadlock breaker.
    /// Expected: Promotion retires before observation, while election-first blocks success until exact cancellation promotes the next waiter.
    #[test]
    fn promotion_and_election_are_mutually_exclusive() {
        let mut fixture = Fixture::graph(&[vec![1], vec![0]]);
        let root = Arc::clone(&fixture.waits[0]);
        assert_deadlock(&fixture.manager, &root);
        let follower = fixture.queue(8, resource(0), LockMode::Shared);
        cancel(&fixture.manager, &fixture.waits[1]);
        fixture.release(2, resource(0));
        fixture.release(1000, resource(0));
        assert_eq!(*root.decision.lock(), WaitDecision::DeadlockChosen);
        assert!(root.completion.completed_result().is_none());
        assert!(follower.is_pending(), "chosen head still enforces FIFO");
        cancel(&fixture.manager, &root);
        assert_eq!(*follower.decision.lock(), WaitDecision::Retired);
        assert!(follower.completion.completed_result().unwrap().is_ok());
        assert!(
            !fixture
                .manager
                .deadlock
                .pending
                .contains_key(&follower.owner.family())
        );
        fixture.manager.check_deadlock(&follower).unwrap();
        // Provisional cancellation must not touch another family's registry.
        cancel(&fixture.manager, &follower);
    }

    /// Purpose: Bound scans of long chains and wide holder/FIFO frontiers before accumulating unbounded scratch state.
    /// Expected: Exact scan and election budgets finish at the boundary; insufficient budgets reject without claiming a cycle.
    #[test]
    fn work_limits_cover_scans_expansion_and_election() {
        let mut edges = vec![Vec::new(); 8];
        for (idx, edge) in edges.iter_mut().enumerate().take(7) {
            edge.push(idx + 1);
        }
        let exact = {
            let fixture = Fixture::graph(&edges);
            let mut budget = SearchBudget {
                work: 0,
                limit: fixture.manager.deadlock.work_limit,
            };
            assert!(
                fixture
                    .manager
                    .deadlock
                    .search_deadlock(&fixture.waits[0], &mut budget, &mut |source, budget| {
                        fixture.manager.wait_snapshot(source, budget)
                    },)
                    .unwrap()
                    .is_none()
            );
            budget.work
        };
        for limit in [exact, exact - 1] {
            let fixture = Fixture::graph_with_limit(&edges, limit);
            let result = fixture.manager.check_deadlock(&fixture.waits[0]);
            if limit == exact {
                result.unwrap();
            } else {
                let error = result.unwrap_err();
                assert_eq!(*error.current_context(), OperationError::DeadlockCheckLimit);
                assert!(format!("{error:?}").contains(&format!("budget={limit}, work={limit}")));
            }
            assert!(fixture.waits[0].is_pending());
        }
        let wide = Fixture::graph_with_limit(
            &[(1..8).collect(), vec![]]
                .into_iter()
                .chain(repeat_n(vec![], 6))
                .collect::<Vec<_>>(),
            10,
        );
        let error = wide.manager.check_deadlock(&wide.waits[0]).unwrap_err();
        assert_eq!(*error.current_context(), OperationError::DeadlockCheckLimit);
        assert!(format!("{error:?}").contains("budget=10, work=10"));

        let mut fifo = Fixture::with_limit(12);
        fifo.hold(999, resource(0), LockMode::Exclusive);
        for family in 1..=8 {
            fifo.queue(family, resource(0), LockMode::Shared);
        }
        let error = fifo.manager.check_deadlock(&fifo.waits[7]).unwrap_err();
        assert_eq!(*error.current_context(), OperationError::DeadlockCheckLimit);
        assert!(format!("{error:?}").contains("budget=12, work=12"));
        assert!(fifo.waits.iter().all(|wait| wait.is_pending()));

        // Capture, indexing, traversal, and projection consume 31 units;
        // sorting and validating the two-record witness consume another six.
        for (limit, expected) in [
            (36, OperationError::DeadlockCheckLimit),
            (37, OperationError::Deadlock),
        ] {
            let cycle = Fixture::graph_with_limit(&[vec![1], vec![0]], limit);
            let error = cycle.manager.check_deadlock(&cycle.waits[0]).unwrap_err();
            assert_eq!(*error.current_context(), expected, "limit={limit}");
            assert_eq!(cycle.waits[0].is_pending(), limit == 36);
        }
    }

    /// Purpose: Recheck a promoted request under a budget too small to inspect a live wait.
    /// Expected: Retirement bypasses detection and preserves the successful provisional completion.
    #[test]
    fn promoted_request_bypasses_tiny_configured_budget() {
        let mut fixture = Fixture::with_limit(1);
        fixture.hold(2, resource(0), LockMode::Exclusive);
        let root = fixture.queue(1, resource(0), LockMode::Shared);
        fixture.release(2, resource(0));
        fixture.manager.check_deadlock(&root).unwrap();
        assert_eq!(*root.decision.lock(), WaitDecision::Retired);
        assert!(root.completion.completed_result().unwrap().is_ok());
        #[cfg(feature = "profiling")]
        assert_eq!(fixture.manager.stats().deadlock_examined_work, 0);
    }

    /// Purpose: Compare seeded logical-lock schedules with an independent transitive-closure graph oracle.
    /// Expected: A checker elects itself exactly when its own vertex lies on a cycle, including branching and converging graphs.
    #[test]
    fn seeded_graphs_match_transitive_closure_oracle() {
        let mut seed = 0x296d_713a_982b_5c04_u64;
        for case in 0..128 {
            let mut edges = vec![Vec::new(); 7];
            let mut reach = [[false; 7]; 7];
            for (source, targets) in edges.iter_mut().enumerate() {
                for (target, reachable) in reach[source].iter_mut().enumerate() {
                    seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    if source != target && seed >> 61 == 0 {
                        targets.push(target);
                        *reachable = true;
                    }
                }
            }
            for via in 0..7 {
                for from in 0..7 {
                    for to in 0..7 {
                        reach[from][to] |= reach[from][via] && reach[via][to];
                    }
                }
            }
            for (root, reachable) in reach.iter().enumerate() {
                let fixture = Fixture::graph(&edges);
                let result = fixture.manager.check_deadlock(&fixture.waits[root]);
                assert_eq!(
                    result.is_err(),
                    reachable[root],
                    "case={case}, seed={seed:#x}, root={root}, edges={edges:?}, result={result:?}"
                );
                if let Err(error) = result {
                    assert_eq!(*error.current_context(), OperationError::Deadlock);
                }
            }
        }
    }

    /// Purpose: Unwind a selected request or observe engine poison after detection with a fresh batch prefix already accepted.
    /// Expected: Exact pending and fresh-prefix cleanup run, Fatal takes precedence, and older accepted claims remain held.
    #[test]
    fn poison_and_unwind_preserve_older_claims_and_clean_fresh_prefix() {
        for case in ["deadlock", "limit", "poison", "unwind"] {
            smol::block_on(async {
                // Four units allow the peer's initial idle-holder wait, but
                // cannot finish the later acquisition's cycle check.
                let limit = if case == "limit" {
                    4
                } else {
                    DEFAULT_DEADLOCK_SEARCH_WORK_LIMIT
                };
                let manager = LockManager::with_config(
                    LockManagerConfig::default().deadlock_search_work_limit(limit),
                )
                .unwrap();
                let poisoner = EnginePoisoner::new();
                let mut authority = FamilyLockAuthority::new(SessionID::new(1));
                let (family, scope) = authority.parts();
                family
                    .acquire(scope, &manager, &poisoner, resource(0), LockMode::Exclusive)
                    .await
                    .unwrap();
                let mut peer = TestLockOwner::new(owner(2));
                peer.acquire(&manager, resource(1), LockMode::Exclusive)
                    .await
                    .unwrap();
                let mut peer_wait =
                    Box::pin(peer.acquire(&manager, resource(0), LockMode::Exclusive));
                assert!(poll!(peer_wait.as_mut()).is_pending());
                let mut fresh = FreshClaimsGuard::<2>::new(family, scope, &manager, &poisoner);
                fresh.acquire(resource(2), LockMode::Shared).await.unwrap();
                if case == "poison" {
                    let _hook = poison_pending_claim_at(PendingClaimTestPhase::DeadlockChecked);
                    let error = fresh
                        .acquire(resource(1), LockMode::Exclusive)
                        .await
                        .unwrap_err();
                    assert!(matches!(error, OperationOrFatalError::Fatal(_)));
                } else if case == "unwind" {
                    PANIC_AFTER_ELECTION.set(true);
                    let unwind = catch_unwind(AssertUnwindSafe(|| {
                        smol::block_on(fresh.acquire(resource(1), LockMode::Exclusive))
                    }));
                    assert!(unwind.is_err());
                } else {
                    let error = fresh
                        .acquire(resource(1), LockMode::Exclusive)
                        .await
                        .unwrap_err();
                    let OperationOrFatalError::Operation(report) = error else {
                        panic!("healthy rejection must remain Operation")
                    };
                    let expected = if case == "limit" {
                        OperationError::DeadlockCheckLimit
                    } else {
                        OperationError::Deadlock
                    };
                    assert_eq!(*report.current_context(), expected);
                }
                drop(fresh);
                assert!(scope.covers(resource(0), LockMode::Exclusive));
                assert!(!scope.covers(resource(2), LockMode::Shared));
                assert!(!scope.covers(resource(1), LockMode::Exclusive));
                assert_eq!(manager.deadlock.pending.len(), 1);
                family.close_scope(scope, &manager);
                peer_wait.await.unwrap();
                peer.close(&manager);
                assert!(manager.deadlock.pending.is_empty());
                assert_eq!(debug_snapshot(&manager).entries, []);
            });
        }
    }

    /// Purpose: Resolve overlapping and disjoint cycles without electing a selected record again.
    /// Expected: A surviving cycle may choose another breaker, a new wait is a new attempt, and unrelated checks run while one election pauses.
    #[test]
    fn elections_remain_local_and_new_attempts_are_distinct() {
        let mut fixture = Fixture::graph(&[vec![1], vec![0, 2], vec![1], vec![4], vec![3]]);
        let old = Arc::clone(&fixture.waits[0]);
        assert_deadlock(&fixture.manager, &old);
        fixture.manager.check_deadlock(&old).unwrap();
        assert_deadlock(&fixture.manager, &fixture.waits[2]);
        cancel(&fixture.manager, &old);
        let new = fixture.queue(1, resource(0), LockMode::Exclusive);
        assert_deadlock(&fixture.manager, &new);
        assert_ne!(old.identity(), new.identity());
        assert_eq!(*old.decision.lock(), WaitDecision::Retired);
        // Pause the disjoint cycle's checker before election; another independent
        // cycle can finish while that thread retains its search witness.
        let separate = Fixture::graph(&[vec![1], vec![0], vec![3], vec![2]]);
        let (ready_tx, ready_rx) = channel();
        let (resume_tx, resume_rx) = channel();
        thread::scope(|scope| {
            let manager = &separate.manager;
            let root = &separate.waits[0];
            let paused = scope.spawn(move || {
                ELECTION_HOOK.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move |_| {
                        ready_tx.send(()).unwrap();
                        resume_rx.recv().unwrap();
                    }))
                });
                manager.check_deadlock(root)
            });
            ready_rx.recv().unwrap();
            assert_deadlock(&separate.manager, &separate.waits[2]);
            resume_tx.send(()).unwrap();
            assert_eq!(
                *paused.join().unwrap().unwrap_err().current_context(),
                OperationError::Deadlock
            );
        });
    }

    /// Purpose: Retain the exact wait identity captured from a holder across cancellation and a replacement wait.
    /// Expected: Replacing a pending registration cannot rebind a previously observed holder edge into a fabricated cycle.
    #[test]
    fn observed_holder_edge_keeps_its_original_wait() {
        let mut fixture = Fixture::graph(&[vec![1], vec![]]);
        let root = Arc::clone(&fixture.waits[0]);
        let old = Arc::clone(&fixture.waits[1]);
        let mut budget = SearchBudget {
            work: 0,
            limit: 100,
        };
        let snapshot = fixture
            .manager
            .wait_snapshot(&root, &mut budget)
            .unwrap()
            .unwrap();
        let edges = snapshot.holders;
        assert_eq!(edges.len(), 1);
        assert!(Arc::ptr_eq(&edges[0].target, &old));
        cancel(&fixture.manager, &old);
        fixture.release(2, resource(0));
        let replacement = fixture.queue(2, resource(1), LockMode::Exclusive);
        let path = [
            WitnessStep {
                wait: &root,
                incoming: None,
            },
            WitnessStep {
                wait: &old,
                incoming: Some(EdgeReason::Holder {
                    family: old.owner.family(),
                    mode: edges[0].mode,
                }),
            },
        ];
        assert!(
            !fixture
                .manager
                .deadlock
                .elect_deadlock(&path, &mut budget)
                .unwrap()
        );
        assert!(replacement.is_pending());
        assert!(
            fixture
                .manager
                .wait_snapshot(&fixture.waits[0], &mut budget)
                .unwrap()
                .unwrap()
                .holders
                .is_empty()
        );
    }

    /// Purpose: Keep uncontended and covered acquisitions out of detector work and expose queued lifecycle counters.
    /// Expected: Only queued requests increment check and registration counters, and cancellation drains current registrations.
    #[cfg(feature = "profiling")]
    #[test]
    fn profiling_tracks_checks_without_charging_owner_local_paths() {
        smol::block_on(async {
            let manager = LockManager::new();
            let mut a = TestLockOwner::new(owner(1));
            let mut b = TestLockOwner::new(owner(2));
            a.acquire(&manager, resource(0), LockMode::Exclusive)
                .await
                .unwrap();
            a.acquire(&manager, resource(0), LockMode::Shared)
                .await
                .unwrap();
            assert_eq!(manager.stats().deadlock_initial_checks, 0);
            let mut wait = Box::pin(b.acquire(&manager, resource(0), LockMode::Shared));
            assert!(poll!(wait.as_mut()).is_pending());
            let stats = manager.stats();
            assert_eq!(stats.deadlock_initial_checks, 1);
            assert!(stats.deadlock_examined_work > 0);
            assert_eq!(stats.current_pending_registrations, 1);
            assert_eq!(stats.peak_pending_registrations, 1);
            assert_eq!(stats.deadlock_candidate_cycles, 0);
            drop(wait);
            assert_eq!(manager.stats().current_pending_registrations, 0);
            a.close(&manager);
            b.close(&manager);
        });
    }

    /// Purpose: Race a fully captured cycle against FIFO promotion before decision validation.
    /// Expected: Promotion retires the root, invalidates the witness, and preserves its successful provisional completion.
    #[test]
    fn promotion_before_election_invalidates_the_witness() {
        let fixture = Fixture::graph(&[vec![1], vec![0]]);
        let manager = Arc::clone(&fixture.manager);
        let peer = Arc::clone(&fixture.waits[1]);
        let blockers: Vec<_> = fixture
            .holders
            .iter()
            .copied()
            .filter(|(r, _, _, _)| *r == resource(0))
            .collect();
        ELECTION_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |_| {
                cancel(&manager, &peer);
                for (resource, owner, claim_no, mode) in blockers {
                    manager.cancel_fresh_grant(
                        PendingClaimToken {
                            resource,
                            owner,
                            claim_no,
                        },
                        mode,
                    );
                }
            }))
        });
        fixture.manager.check_deadlock(&fixture.waits[0]).unwrap();
        assert_eq!(*fixture.waits[0].decision.lock(), WaitDecision::Retired);
        assert!(
            fixture.waits[0]
                .completion
                .completed_result()
                .unwrap()
                .is_ok()
        );
        assert!(fixture.manager.deadlock.pending.is_empty());
    }

    /// Purpose: Derive holder edges for all kinds of incompatible table-data modes.
    /// Expected: Intention/shared and intention/exclusive conflicts participate in cycles just like exclusive/shared conflicts.
    #[test]
    fn intention_modes_participate_in_holder_cycles() {
        for (held, requested) in [
            (LockMode::IntentShared, LockMode::Exclusive),
            (LockMode::IntentExclusive, LockMode::Shared),
            (LockMode::Shared, LockMode::IntentExclusive),
            (LockMode::Exclusive, LockMode::IntentShared),
        ] {
            let mut fixture = Fixture::default();
            fixture.hold(1, resource(0), held);
            fixture.hold(2, resource(1), held);
            fixture.queue(1, resource(1), requested);
            let root = fixture.queue(2, resource(0), requested);
            assert!(assert_deadlock(&fixture.manager, &root).contains("physical holder"));
        }
    }

    /// Purpose: Check seeded shared-holder and FIFO schedules against an independent reachability oracle.
    /// Expected: All earlier FIFO requests and incompatible holders determine cycle membership, including compatible requests blocked only by queue order.
    #[test]
    fn seeded_fifo_schedules_match_independent_oracle() {
        let mut seed = 0x9813_7c2d_67b5_4a01_u64;
        for case in 0..64 {
            let mut held_resources = [0; 7];
            let mut requested_modes = [LockMode::Exclusive; 7];
            for family in 0..7 {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                held_resources[family] = (family % 3 + 1 + ((seed >> 32) as usize % 2)) % 3;
                if family >= 3 {
                    requested_modes[family] = [
                        LockMode::Shared,
                        LockMode::IntentExclusive,
                        LockMode::Exclusive,
                    ][(seed >> 48) as usize % 3];
                }
            }
            let mut reach = [[false; 7]; 7];
            for source in 0..7 {
                for (target, &held_resource) in held_resources.iter().enumerate() {
                    // Independent model: every holder is S; S requests conflict
                    // with none, IX/X with all. Earlier requests on the same
                    // resource always block regardless of requested mode.
                    reach[source][target] = (held_resource == source % 3
                        && requested_modes[source] != LockMode::Shared)
                        || (target < source && target % 3 == source % 3);
                }
            }
            for via in 0..7 {
                for from in 0..7 {
                    for to in 0..7 {
                        reach[from][to] |= reach[from][via] && reach[via][to];
                    }
                }
            }
            for (root, reachable) in reach.iter().enumerate() {
                let mut fixture = Fixture::default();
                for r in 0..3 {
                    fixture.hold(100 + r, resource(r), LockMode::Shared);
                }
                for (family, &held_resource) in held_resources.iter().enumerate() {
                    fixture.hold(
                        1 + family as u64,
                        resource(held_resource as u64),
                        LockMode::Shared,
                    );
                }
                for (family, &mode) in requested_modes.iter().enumerate() {
                    fixture.queue(1 + family as u64, resource((family % 3) as u64), mode);
                }
                let result = fixture.manager.check_deadlock(&fixture.waits[root]);
                assert_eq!(
                    result.is_err(),
                    reachable[root],
                    "case={case}, seed={seed:#x}, root={root}, held={held_resources:?}, requested={requested_modes:?}, result={result:?}"
                );
                if let Err(error) = result {
                    assert_eq!(*error.current_context(), OperationError::Deadlock);
                }
            }
        }
    }
}
