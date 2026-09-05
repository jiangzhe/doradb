//! Logical metadata and table-data lock manager primitives.
//!
//! This module is the standalone core for RFC-0016 logical locks. It tracks
//! table metadata and table data resources independently from the
//! engine/session/transaction lifecycle wiring that later phases will add.

mod claim;
mod state;
mod wait;

use self::claim::PendingClaimToken;
use self::wait::{WaitNodeID, WaitNodePhase, WaitQueue};
use crate::catalog::storage::layout::{BUILTIN_CATALOG_TABLE_COUNT, builtin_catalog_table_slot};
use crate::completion::Completion;
use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::error::{OperationError, OperationResult};
use crate::id::{OperationID, SessionID, SessionOperationKey, TableID, TrxID};
use crate::map::{FastDashMap, FastHashMap};
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use crate::stats::LogicalLockStats;
use crossbeam_utils::CachePadded;
use dashmap::mapref::entry::Entry;
use error_stack::Report;
use parking_lot::Mutex;
use std::array::from_fn;
use std::convert::Infallible;
use std::fmt;
use std::mem::take;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) use state::{
    FamilyLockAuthority, FamilyLockState, FreshClaimsGuard, LockScopeState, TransactionLockState,
};

const MODE_COUNT: usize = 4;
/// Maximum entry/slot capacity retained by each idle catalog-state container.
const CATALOG_STATE_RETAIN_CAPACITY: usize = 1024;
const LOCK_MODES: [LockMode; MODE_COUNT] = [
    LockMode::IntentShared,
    LockMode::IntentExclusive,
    LockMode::Shared,
    LockMode::Exclusive,
];

/// Logical resource protected by the lock manager.
///
/// Lock acquisition follows the resource order below to avoid deadlocks across
/// multi-resource operations:
///
/// ```text
/// TableMetadata(table_id ascending)
///   -> TableData(table_id ascending)
///   -> row undo/CDB ownership
/// ```
///
/// The derived [`Ord`] implementation encodes the represented portion of this
/// order. Row ownership is outside this logical lock manager and must be
/// acquired only after the relevant `TableData` lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum LockResource {
    /// Table definition and metadata for one table.
    TableMetadata(TableID),
    /// Multi-granularity table-data root above row ownership.
    TableData(TableID),
}

impl fmt::Display for LockResource {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockResource::TableMetadata(table_id) => {
                write!(f, "table_metadata({table_id})")
            }
            LockResource::TableData(table_id) => write!(f, "table_data({table_id})"),
        }
    }
}

/// Mode accepted by the public explicit table-lock APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableLockMode {
    /// Shared whole-table access.
    Shared,
    /// Exclusive whole-table access.
    Exclusive,
}

/// Logical lock mode used inside the lock manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LockMode {
    /// Intention shared mode (`IS`) for table-data resources.
    IntentShared,
    /// Intention exclusive mode (`IX`) for table-data resources.
    IntentExclusive,
    /// Shared mode (`S`).
    Shared,
    /// Exclusive mode (`X`).
    Exclusive,
}

impl LockMode {
    #[inline]
    fn label(self) -> &'static str {
        match self {
            LockMode::IntentShared => "intent_shared",
            LockMode::IntentExclusive => "intent_exclusive",
            LockMode::Shared => "shared",
            LockMode::Exclusive => "exclusive",
        }
    }

    /// Asserts that this mode can be used for `resource`.
    #[inline]
    fn assert_valid_for(self, resource: LockResource) {
        assert!(
            mode_is_valid(resource, self),
            "lock mode/resource invariant violated: resource={resource}, mode={self}"
        );
    }

    /// Returns whether this mode covers a request for `requested` on `resource`.
    ///
    /// Coverage is used for reentrant acquisitions and immediate conversion
    /// decisions. `TableData(S)` and `TableData(IX)` are intentionally
    /// incomparable because the first phase does not introduce `SIX`.
    #[inline]
    pub(crate) fn covers(self, resource: LockResource, requested: Self) -> bool {
        self.assert_valid_for(resource);
        requested.assert_valid_for(resource);
        mode_covers(resource, self, requested)
    }
}

impl From<TableLockMode> for LockMode {
    #[inline]
    fn from(mode: TableLockMode) -> Self {
        match mode {
            TableLockMode::Shared => LockMode::Shared,
            TableLockMode::Exclusive => LockMode::Exclusive,
        }
    }
}

impl fmt::Display for LockMode {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Canonical family shared by every logical lock owner from one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct LockFamily(SessionID);

impl LockFamily {
    /// Creates the lock family for one engine-local session.
    #[inline]
    pub(crate) const fn new(session_id: SessionID) -> Self {
        Self(session_id)
    }

    /// Returns the session identity represented by this family.
    #[inline]
    pub(crate) const fn session_id(self) -> SessionID {
        self.0
    }
}

impl fmt::Display for LockFamily {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "session(session_id={})", self.0)
    }
}

/// Exact lifetime scope of one logical lock owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum LockScope {
    /// Explicit locks retained until unlock or session teardown.
    SessionExplicit,
    /// Locks retained for one transaction.
    Transaction(TrxID),
    /// Locks retained for one enclosing DDL or maintenance operation.
    Operation(OperationID),
}

/// Canonical exact logical lock owner independent from Rust object lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct LockOwner {
    family: LockFamily,
    scope: LockScope,
}

impl LockOwner {
    #[inline]
    const fn from_parts(family: LockFamily, scope: LockScope) -> Self {
        Self { family, scope }
    }

    /// Creates the explicit-lock owner for one session.
    #[inline]
    pub(crate) const fn session_explicit(session_id: SessionID) -> Self {
        Self {
            family: LockFamily::new(session_id),
            scope: LockScope::SessionExplicit,
        }
    }

    /// Creates the transaction owner for one session.
    #[inline]
    pub(crate) const fn transaction(session_id: SessionID, trx_id: TrxID) -> Self {
        Self {
            family: LockFamily::new(session_id),
            scope: LockScope::Transaction(trx_id),
        }
    }

    /// Creates the exact lock owner for one enclosing session operation.
    #[inline]
    pub(crate) const fn operation(key: SessionOperationKey) -> Self {
        Self {
            family: LockFamily::new(key.session_id()),
            scope: LockScope::Operation(key.operation_id()),
        }
    }

    /// Returns the canonical family of this exact owner.
    #[inline]
    pub(crate) const fn family(self) -> LockFamily {
        self.family
    }

    /// Returns the exact lifetime scope of this owner.
    #[inline]
    pub(crate) const fn scope(self) -> LockScope {
        self.scope
    }
}

impl fmt::Display for LockOwner {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let session_id = self.family.session_id();
        match self.scope {
            LockScope::SessionExplicit => {
                write!(f, "session_explicit(session_id={session_id})")
            }
            LockScope::Transaction(trx_id) => {
                write!(f, "transaction(session_id={session_id},trx_id={trx_id})")
            }
            LockScope::Operation(operation_id) => write!(
                f,
                "operation(session_id={session_id},operation_id={operation_id})"
            ),
        }
    }
}

/// Whether an acquisition created a new exact logical claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockGrant {
    /// The acquire call created a fresh exact claim.
    Fresh,
    /// The requested mode was already represented by this owner or waiter.
    Existing,
}

/// Independently synchronized and padded metadata/data states for one built-in table.
#[derive(Default)]
struct CatalogTableLockSlot {
    metadata: CachePadded<Mutex<ResourceState>>,
    data: CachePadded<Mutex<ResourceState>>,
}

#[derive(Clone, Copy)]
enum ResourceAccess {
    Create,
    Existing,
}

/// Physical arbitration layer for logical lock families.
///
/// Exact scope claims and their lifetimes remain owner-side in
/// `FamilyLockState`. For each `(resource, family)` this manager stores only
/// one physical state in the family's covering mode. Compatibility therefore
/// scales with physical session families rather than with the number of
/// session, operation, and transaction claims they aggregate.
pub(crate) struct LockManager {
    catalog: [CatalogTableLockSlot; BUILTIN_CATALOG_TABLE_COUNT],
    /// Dynamic fallback for user tables and every unknown reserved catalog ID.
    user: FastDashMap<LockResource, ResourceState>,
    stats: LockManagerStats,
}

impl LockManager {
    /// Creates an empty lock manager.
    #[inline]
    pub(crate) fn new() -> Self {
        LockManager {
            catalog: from_fn(|_| CatalogTableLockSlot::default()),
            user: FastDashMap::default(),
            stats: LockManagerStats::default(),
        }
    }

    /// Returns a poison-tolerant logical-lock statistics snapshot.
    #[inline]
    pub(crate) fn stats(&self) -> LogicalLockStats {
        self.stats.snapshot()
    }

    #[inline]
    fn record_family_stats(&self, stats: state::FamilyLockStats) {
        self.stats.record_family(stats);
    }

    #[inline]
    fn start_pending(
        &self,
        token: &PendingClaimToken,
        mode: LockMode,
    ) -> OperationResult<PendingStart> {
        mode.assert_valid_for(token.resource);
        add(&self.stats.resource_transitions, 1);
        add(&self.stats.mode_slots_examined, MODE_COUNT as u64);
        self.with_resource(
            token.resource,
            ResourceAccess::Create,
            "start_pending",
            |resource_state| {
                let old_slots = resource_state.wait_queue.allocated_slots();
                let result = resource_state.start_pending(token.resource, token, mode);
                if let Ok(start) = &result {
                    increment_current(
                        &self.stats.current_physical_families,
                        &self.stats.peak_physical_families,
                    );
                    match start {
                        PendingStart::Immediate => {
                            add(&self.stats.immediate_physical_acquisitions, 1);
                        }
                        PendingStart::Waiting { .. } => {
                            add(&self.stats.enqueued_waiters, 1);
                            add(&self.stats.completion_allocations, 1);
                            add(&self.stats.queue_link_mutations, 1);
                            increment_current(
                                &self.stats.current_linked_waiters,
                                &self.stats.peak_linked_waiters,
                            );
                            increment_current(
                                &self.stats.current_live_waiter_nodes,
                                &self.stats.peak_live_waiter_nodes,
                            );
                            if resource_state.wait_queue.allocated_slots() > old_slots {
                                add(&self.stats.waiter_slab_growths, 1);
                            } else {
                                add(&self.stats.waiter_slab_reuses, 1);
                            }
                        }
                    }
                }
                result
            },
        )
    }

    #[inline]
    fn observe_pending(&self, token: &PendingClaimToken, mode: LockMode, node_id: WaitNodeID) {
        add(&self.stats.resource_transitions, 1);
        self.with_resource(
            token.resource,
            ResourceAccess::Existing,
            "observe_pending",
            |resource_state| {
                resource_state.observe_pending(token, mode, node_id);
            },
        );
        decrement_current(
            &self.stats.current_live_waiter_nodes,
            "current_live_waiter_nodes",
        );
        add(&self.stats.provisional_observations, 1);
    }

    #[inline]
    fn cancel_waiting(&self, token: PendingClaimToken, mode: LockMode, node_id: WaitNodeID) {
        add(&self.stats.resource_transitions, 1);
        let mut notify = DeferredNotifications::default();
        self.with_resource(
            token.resource,
            ResourceAccess::Existing,
            "cancel_waiting",
            |resource_state| {
                resource_state
                    .wait_queue
                    .assert_identity(node_id, &token, mode);
                match resource_state.wait_queue.node(node_id).phase {
                    WaitNodePhase::Queued { prev, next } => {
                        match (prev, next) {
                            (None, _) => add(&self.stats.cancelled_head_waiters, 1),
                            (Some(_), None) => add(&self.stats.cancelled_tail_waiters, 1),
                            (Some(_), Some(_)) => add(&self.stats.cancelled_middle_waiters, 1),
                        }
                        resource_state.assert_queued_family(&token, node_id);
                        let _ = resource_state.wait_queue.remove_queued(node_id);
                        let removed = resource_state.families.remove(&token.owner.family());
                        assert!(
                            removed.is_some(),
                            "queued pending cancellation lost its physical family entry: \
                         resource={}, family={}",
                            token.resource,
                            token.owner.family()
                        );
                        add(&self.stats.queue_link_mutations, 1);
                        decrement_current(
                            &self.stats.current_linked_waiters,
                            "current_linked_waiters",
                        );
                        decrement_current(
                            &self.stats.current_live_waiter_nodes,
                            "current_live_waiter_nodes",
                        );
                        decrement_current(
                            &self.stats.current_physical_families,
                            "current_physical_families",
                        );
                        resource_state.grant_waiters(token.resource, &mut notify);
                    }
                    WaitNodePhase::Provisional => {
                        resource_state.remove_provisional(&token, mode, node_id);
                        let _ = resource_state.wait_queue.consume_provisional(node_id);
                        decrement_current(
                            &self.stats.current_live_waiter_nodes,
                            "current_live_waiter_nodes",
                        );
                        decrement_current(
                            &self.stats.current_physical_families,
                            "current_physical_families",
                        );
                        resource_state.grant_waiters(token.resource, &mut notify);
                    }
                }
                self.record_promotions(&notify);
            },
        );
        notify.publish();
    }

    #[inline]
    fn cancel_fresh_grant(&self, token: PendingClaimToken, mode: LockMode) {
        add(&self.stats.resource_transitions, 1);
        let mut notify = DeferredNotifications::default();
        self.with_resource(
            token.resource,
            ResourceAccess::Existing,
            "cancel_fresh_grant",
            |resource_state| {
                resource_state.remove_fresh_grant(&token, mode);
                resource_state.grant_waiters(token.resource, &mut notify);
                decrement_current(
                    &self.stats.current_physical_families,
                    "current_physical_families",
                );
                self.record_promotions(&notify);
            },
        );
        notify.publish();
    }

    #[inline]
    fn convert_family(
        &self,
        resource: LockResource,
        family: LockFamily,
        old_mode: LockMode,
        new_mode: LockMode,
    ) -> OperationResult<()> {
        new_mode.assert_valid_for(resource);
        add(&self.stats.resource_transitions, 1);
        add(&self.stats.mode_slots_examined, MODE_COUNT as u64);
        self.with_resource(
            resource,
            ResourceAccess::Existing,
            "convert_family",
            |resource_state| {
                let result = resource_state.convert_family(resource, family, old_mode, new_mode);
                if result.is_ok() {
                    add(&self.stats.physical_upgrades, 1);
                }
                result
            },
        )
    }

    #[inline]
    fn remove_family(&self, resource: LockResource, family: LockFamily, old_mode: LockMode) {
        add(&self.stats.resource_transitions, 1);
        let mut notify = DeferredNotifications::default();
        self.with_resource(
            resource,
            ResourceAccess::Existing,
            "remove_family",
            |resource_state| {
                resource_state.remove_family(resource, family, old_mode);
                resource_state.grant_waiters(resource, &mut notify);
                decrement_current(
                    &self.stats.current_physical_families,
                    "current_physical_families",
                );
                self.record_promotions(&notify);
            },
        );
        notify.publish();
    }

    /// Runs one synchronous transition and finishes store-specific accounting/cleanup.
    /// No resource guard escapes this boundary or survives an await.
    #[inline]
    fn with_resource<R>(
        &self,
        resource: LockResource,
        access: ResourceAccess,
        transition: &'static str,
        apply: impl FnOnce(&mut ResourceState) -> R,
    ) -> R {
        if let Some(cell) = self.catalog_cell(resource) {
            let mut state = cell.lock();
            if state.is_empty() {
                assert!(
                    matches!(access, ResourceAccess::Create),
                    "lock manager requires active catalog state: resource={resource}, transition={transition}"
                );
                increment_current(
                    &self.stats.current_physical_resources,
                    &self.stats.peak_physical_resources,
                );
            }
            let result = apply(&mut state);
            // Activation and complete drain linearize under this same cell mutex.
            // Queued and provisional nodes pin the state until their exact cleanup.
            let discarded = if state.is_empty() {
                decrement_current(
                    &self.stats.current_physical_resources,
                    "current_physical_resources",
                );
                // Retain ordinary working capacity and detach oversized containers
                // independently. A retained slab keeps its free list and generations.
                let families = (state.families.capacity() > CATALOG_STATE_RETAIN_CAPACITY)
                    .then(|| take(&mut state.families));
                let wait_queue = (state.wait_queue.capacity() > CATALOG_STATE_RETAIN_CAPACITY)
                    .then(|| take(&mut state.wait_queue));
                (families, wait_queue)
            } else {
                (None, None)
            };
            drop(state);
            // Free detached allocations after releasing synchronization.
            drop(discarded);
            result
        } else {
            let (result, empty) = {
                let mut state = match access {
                    ResourceAccess::Create => match self.user.entry(resource) {
                        Entry::Occupied(entry) => entry.into_ref(),
                        Entry::Vacant(entry) => {
                            increment_current(&self.stats.current_physical_resources, &self.stats.peak_physical_resources);
                            entry.insert(ResourceState::default())
                        }
                    },
                    ResourceAccess::Existing => self.user.get_mut(&resource).unwrap_or_else(|| {
                        panic!("lock manager requires retained dynamic state: resource={resource}, transition={transition}")
                    }),
                };
                let result = apply(&mut state);
                (result, state.is_empty())
            };
            // A racing acquisition can refill this entry after the guard is released.
            if empty
                && self
                    .user
                    .remove_if(&resource, |_, state| state.is_empty())
                    .is_some()
            {
                decrement_current(
                    &self.stats.current_physical_resources,
                    "current_physical_resources",
                );
            }
            result
        }
    }

    #[inline]
    fn catalog_cell(&self, resource: LockResource) -> Option<&Mutex<ResourceState>> {
        match resource {
            LockResource::TableMetadata(table_id) => {
                builtin_catalog_table_slot(table_id).map(|slot| &*self.catalog[slot].metadata)
            }
            LockResource::TableData(table_id) => {
                builtin_catalog_table_slot(table_id).map(|slot| &*self.catalog[slot].data)
            }
        }
    }

    #[inline]
    fn record_promotions(&self, notifications: &DeferredNotifications) {
        let promoted = notifications.len();
        if promoted == 0 {
            return;
        }
        add(&self.stats.promoted_waiters, promoted);
        add(&self.stats.queue_link_mutations, promoted);
        add(
            &self.stats.mode_slots_examined,
            promoted * MODE_COUNT as u64,
        );
        for _ in 0..promoted {
            decrement_current(&self.stats.current_linked_waiters, "current_linked_waiters");
        }
    }
}

impl Default for LockManager {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Component for LockManager {
    type Config = ();
    type Owned = Self;
    type Access = QuiescentGuard<Self>;
    type Error = Infallible;

    const NAME: &'static str = "lock_manager";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> StdResult<(), Self::Error> {
        registry.register::<Self>(Self::new());
        Ok(())
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {
        // Panic safety: the engine session/operation drain removes every lock
        // manager user before this passive hook is dispatched.
    }
}

#[derive(Default)]
struct LockManagerStats {
    owner_local_exact_covered_hits: AtomicU64,
    owner_local_covered_publications: AtomicU64,
    owner_local_mode_preserving_conversions: AtomicU64,
    owner_local_mode_preserving_releases: AtomicU64,
    resource_transitions: AtomicU64,
    mode_slots_examined: AtomicU64,
    immediate_physical_acquisitions: AtomicU64,
    physical_upgrades: AtomicU64,
    enqueued_waiters: AtomicU64,
    queue_link_mutations: AtomicU64,
    cancelled_head_waiters: AtomicU64,
    cancelled_middle_waiters: AtomicU64,
    cancelled_tail_waiters: AtomicU64,
    provisional_observations: AtomicU64,
    promoted_waiters: AtomicU64,
    scope_close_claims_visited: AtomicU64,
    scope_close_physical_changes: AtomicU64,
    completion_allocations: AtomicU64,
    waiter_slab_growths: AtomicU64,
    waiter_slab_reuses: AtomicU64,
    current_physical_resources: AtomicU64,
    peak_physical_resources: AtomicU64,
    current_physical_families: AtomicU64,
    peak_physical_families: AtomicU64,
    current_linked_waiters: AtomicU64,
    peak_linked_waiters: AtomicU64,
    current_live_waiter_nodes: AtomicU64,
    peak_live_waiter_nodes: AtomicU64,
}

impl LockManagerStats {
    #[inline]
    fn snapshot(&self) -> LogicalLockStats {
        LogicalLockStats {
            owner_local_exact_covered_hits: load(&self.owner_local_exact_covered_hits),
            owner_local_covered_publications: load(&self.owner_local_covered_publications),
            owner_local_mode_preserving_conversions: load(
                &self.owner_local_mode_preserving_conversions,
            ),
            owner_local_mode_preserving_releases: load(&self.owner_local_mode_preserving_releases),
            resource_transitions: load(&self.resource_transitions),
            mode_slots_examined: load(&self.mode_slots_examined),
            immediate_physical_acquisitions: load(&self.immediate_physical_acquisitions),
            physical_upgrades: load(&self.physical_upgrades),
            enqueued_waiters: load(&self.enqueued_waiters),
            queue_link_mutations: load(&self.queue_link_mutations),
            cancelled_head_waiters: load(&self.cancelled_head_waiters),
            cancelled_middle_waiters: load(&self.cancelled_middle_waiters),
            cancelled_tail_waiters: load(&self.cancelled_tail_waiters),
            provisional_observations: load(&self.provisional_observations),
            promoted_waiters: load(&self.promoted_waiters),
            scope_close_claims_visited: load(&self.scope_close_claims_visited),
            scope_close_physical_changes: load(&self.scope_close_physical_changes),
            completion_allocations: load(&self.completion_allocations),
            waiter_slab_growths: load(&self.waiter_slab_growths),
            waiter_slab_reuses: load(&self.waiter_slab_reuses),
            current_physical_resources: load(&self.current_physical_resources),
            peak_physical_resources: load(&self.peak_physical_resources),
            current_physical_families: load(&self.current_physical_families),
            peak_physical_families: load(&self.peak_physical_families),
            current_linked_waiters: load(&self.current_linked_waiters),
            peak_linked_waiters: load(&self.peak_linked_waiters),
            current_live_waiter_nodes: load(&self.current_live_waiter_nodes),
            peak_live_waiter_nodes: load(&self.peak_live_waiter_nodes),
        }
    }

    #[inline]
    fn record_family(&self, family: state::FamilyLockStats) {
        add(
            &self.owner_local_exact_covered_hits,
            family.repeated_exact_covered,
        );
        add(
            &self.owner_local_covered_publications,
            family.family_covered_publications,
        );
        add(
            &self.owner_local_mode_preserving_conversions,
            family.physical_mode_preserving_conversions,
        );
        add(
            &self.owner_local_mode_preserving_releases,
            family.physical_mode_preserving_releases,
        );
        add(
            &self.scope_close_claims_visited,
            family.close_claims_visited,
        );
        add(
            &self.scope_close_physical_changes,
            family.scope_close_physical_changes,
        );
    }
}

/// Physical state for one lock resource across all session families.
///
/// `families` contains exactly one state per participating family.
/// `granted_counts` counts `Held` and `Provisional` families by their one
/// physical mode; `grant_mask` is the corresponding non-empty-mode summary.
/// `Queued` families are represented in `families` and `wait_queue` but do not
/// contribute to compatibility counts until promotion.
#[derive(Default)]
struct ResourceState {
    granted_counts: [u32; MODE_COUNT],
    grant_mask: claim::ModeMask,
    families: FastHashMap<LockFamily, PhysicalFamilyState>,
    wait_queue: WaitQueue,
}

impl ResourceState {
    /// Starts the first physical claim for a family on this resource.
    ///
    /// Covered claims from additional exact scopes bypass `ResourceState`
    /// entirely because the existing family holder already represents them.
    #[inline]
    fn start_pending(
        &mut self,
        resource: LockResource,
        token: &PendingClaimToken,
        mode: LockMode,
    ) -> OperationResult<PendingStart> {
        let family = token.owner.family();
        assert!(
            !self.families.contains_key(&family),
            "first physical claim duplicates a manager family entry: \
             resource={resource}, family={family}, owner={}, claim_no={:?}",
            token.owner,
            token.claim_no
        );
        if self.compatible_with_families(resource, mode, family)
            && self.wait_queue.is_linked_empty()
        {
            self.insert_held(family, mode);
            return Ok(PendingStart::Immediate);
        }
        let completion = Arc::new(Completion::new());
        let node_id =
            self.wait_queue
                .append(token.owner, token.claim_no, mode, Arc::clone(&completion));
        let previous = self
            .families
            .insert(family, PhysicalFamilyState::Queued { node_id });
        assert!(
            previous.is_none(),
            "queued first physical claim replaced a manager family entry: \
             resource={resource}, family={family}, owner={}, claim_no={:?}",
            token.owner,
            token.claim_no
        );
        Ok(PendingStart::Waiting {
            node_id,
            completion,
        })
    }

    #[inline]
    fn convert_family(
        &mut self,
        resource: LockResource,
        family: LockFamily,
        old_mode: LockMode,
        new_mode: LockMode,
    ) -> OperationResult<()> {
        // Conversion replaces the family's one counted mode; it never adds a
        // second physical holder for another exact scope.
        assert!(
            matches!(
                self.families.get(&family),
                Some(PhysicalFamilyState::Held { mode }) if *mode == old_mode
            ),
            "physical conversion expected a matching held family: \
             resource={resource}, family={family}, old_mode={old_mode}, \
             new_mode={new_mode}, actual={:?}",
            self.families.get(&family)
        );
        assert!(
            new_mode.covers(resource, old_mode),
            "physical conversion must strengthen the family mode: \
             resource={resource}, family={family}, old_mode={old_mode}, new_mode={new_mode}"
        );
        if !self.wait_queue.is_linked_empty()
            || !self.compatible_with_families(resource, new_mode, family)
        {
            // The current implementation cannot retain `old_mode` while
            // queueing `new_mode`, so conversion is deliberately immediate.
            return Err(upgrade_would_block_err(
                resource,
                old_mode,
                new_mode,
                LockOwner::from_parts(family, LockScope::SessionExplicit),
            ));
        }
        self.replace_holder_mode(family, old_mode, new_mode);
        Ok(())
    }

    #[inline]
    fn remove_family(&mut self, resource: LockResource, family: LockFamily, old_mode: LockMode) {
        // Owner-side aggregation has established that no exact claims remain.
        // Remove the family's one counted holder; the caller reruns FIFO
        // promotion afterward because this may unblock queued families.
        assert!(
            matches!(
                self.families.get(&family),
                Some(PhysicalFamilyState::Held { mode }) if *mode == old_mode
            ),
            "physical release expected a matching held family: \
             resource={resource}, family={family}, old_mode={old_mode}, actual={:?}",
            self.families.get(&family)
        );
        self.decrement_holder(old_mode);
        let removed = self.families.remove(&family);
        assert!(
            removed.is_some(),
            "physical family removal lost its family entry: \
             resource={resource}, family={family}"
        );
    }

    #[inline]
    fn compatible_with_families(
        &self,
        resource: LockResource,
        requested: LockMode,
        excluded_family: LockFamily,
    ) -> bool {
        // Excluding the requester's existing counted mode prevents a physical
        // family conversion from conflicting with itself. For a first claim,
        // the family has no counted mode and exclusion is a no-op.
        let excluded_mode = self
            .families
            .get(&excluded_family)
            .and_then(|state| state.counted_mode());
        LOCK_MODES.into_iter().enumerate().all(|(idx, held)| {
            let excluded = u32::from(excluded_mode == Some(held));
            self.granted_counts[idx] == excluded || modes_are_compatible(resource, held, requested)
        })
    }

    #[inline]
    fn grant_waiters(&mut self, resource: LockResource, notifications: &mut DeferredNotifications) {
        while let Some(node_id) = self.wait_queue.head() {
            let node = self.wait_queue.node(node_id);
            let owner = node.owner;
            let mode = node.target_mode;
            let family = owner.family();
            if !self.compatible_with_families(resource, mode, family) {
                break;
            }
            assert!(
                matches!(
                    self.families.get(&family),
                    Some(PhysicalFamilyState::Queued { node_id: family_node })
                        if *family_node == node_id
                ),
                "FIFO promotion requires the matching queued family: \
                 resource={resource}, family={family}, owner={owner}, node_id={node_id:?}, \
                 actual={:?}",
                self.families.get(&family)
            );
            let completion = Arc::clone(&node.completion);
            self.wait_queue.detach_to_provisional(node_id);
            self.increment_holder(mode);
            let previous = self
                .families
                .insert(family, PhysicalFamilyState::Provisional { mode, node_id });
            assert!(
                previous.is_some(),
                "FIFO promotion lost its queued physical family: \
                 resource={resource}, family={family}, node_id={node_id:?}"
            );
            notifications.push(completion);
        }
    }

    #[inline]
    fn observe_pending(&mut self, token: &PendingClaimToken, mode: LockMode, node_id: WaitNodeID) {
        self.wait_queue.assert_identity(node_id, token, mode);
        match self.wait_queue.node(node_id).phase {
            WaitNodePhase::Queued { .. } => panic!(
                "completed lock waiter remains queued at observation: \
                 resource={}, owner={}, claim_no={:?}, node_id={node_id:?}",
                token.resource, token.owner, token.claim_no
            ),
            WaitNodePhase::Provisional => {
                let family = token.owner.family();
                assert!(
                    matches!(
                        self.families.get(&family),
                        Some(PhysicalFamilyState::Provisional {
                            mode: actual_mode,
                            node_id: actual_node,
                        }) if *actual_mode == mode && *actual_node == node_id
                    ),
                    "provisional observation physical-family identity mismatch: \
                     resource={}, owner={}, claim_no={:?}, mode={}, node_id={node_id:?}, \
                     actual={:?}",
                    token.resource,
                    token.owner,
                    token.claim_no,
                    mode,
                    self.families.get(&family)
                );
                let previous = self
                    .families
                    .insert(family, PhysicalFamilyState::Held { mode });
                assert!(
                    previous.is_some(),
                    "provisional observation lost its physical family: \
                     resource={}, family={family}, node_id={node_id:?}",
                    token.resource
                );
                let _ = self.wait_queue.consume_provisional(node_id);
            }
        }
    }

    #[inline]
    fn assert_queued_family(&self, token: &PendingClaimToken, node_id: WaitNodeID) {
        assert!(
            matches!(
                self.families.get(&token.owner.family()),
                Some(PhysicalFamilyState::Queued {
                    node_id: family_node,
                }) if *family_node == node_id
            ),
            "queued waiter physical-family identity mismatch: \
             resource={}, owner={}, claim_no={:?}, node_id={node_id:?}, actual={:?}",
            token.resource,
            token.owner,
            token.claim_no,
            self.families.get(&token.owner.family())
        );
    }

    #[inline]
    fn remove_provisional(
        &mut self,
        token: &PendingClaimToken,
        mode: LockMode,
        node_id: WaitNodeID,
    ) {
        let family = token.owner.family();
        assert!(
            matches!(
                self.families.get(&family),
                Some(PhysicalFamilyState::Provisional {
                    mode: actual_mode,
                    node_id: actual_node,
                }) if *actual_mode == mode && *actual_node == node_id
            ),
            "provisional cancellation physical-family identity mismatch: \
             resource={}, owner={}, claim_no={:?}, mode={}, node_id={node_id:?}, \
             actual={:?}",
            token.resource,
            token.owner,
            token.claim_no,
            mode,
            self.families.get(&family)
        );
        self.decrement_holder(mode);
        let removed = self.families.remove(&family);
        assert!(
            removed.is_some(),
            "provisional cancellation lost its physical family: \
             resource={}, family={family}, node_id={node_id:?}",
            token.resource
        );
    }

    #[inline]
    fn remove_fresh_grant(&mut self, token: &PendingClaimToken, mode: LockMode) {
        let family = token.owner.family();
        assert!(
            matches!(
                self.families.get(&family),
                Some(PhysicalFamilyState::Held { mode: actual_mode }) if *actual_mode == mode
            ),
            "fresh pending rollback physical-family identity mismatch: \
             resource={}, owner={}, claim_no={:?}, mode={}, actual={:?}",
            token.resource,
            token.owner,
            token.claim_no,
            mode,
            self.families.get(&family)
        );
        self.decrement_holder(mode);
        let removed = self.families.remove(&family);
        assert!(
            removed.is_some(),
            "fresh pending rollback lost its physical family: \
             resource={}, family={family}",
            token.resource
        );
    }

    #[inline]
    fn insert_held(&mut self, family: LockFamily, mode: LockMode) {
        self.increment_holder(mode);
        let previous = self
            .families
            .insert(family, PhysicalFamilyState::Held { mode });
        assert!(
            previous.is_none(),
            "physical family insert replaced an existing state: \
             family={family}, mode={mode}, previous={previous:?}"
        );
    }

    #[inline]
    fn replace_holder_mode(&mut self, family: LockFamily, old_mode: LockMode, new_mode: LockMode) {
        self.decrement_holder(old_mode);
        self.increment_holder(new_mode);
        let previous = self
            .families
            .insert(family, PhysicalFamilyState::Held { mode: new_mode });
        assert!(
            previous.is_some(),
            "physical family conversion unexpectedly inserted a new state: \
             family={family}, old_mode={old_mode}, new_mode={new_mode}"
        );
    }

    #[inline]
    fn increment_holder(&mut self, mode: LockMode) {
        let idx = mode_index(mode);
        self.granted_counts[idx] = self.granted_counts[idx]
            .checked_add(1)
            .unwrap_or_else(|| panic!("physical lock holder count overflowed: mode={mode}"));
        self.grant_mask.insert(mode);
    }

    #[inline]
    fn decrement_holder(&mut self, mode: LockMode) {
        let idx = mode_index(mode);
        self.granted_counts[idx] = self.granted_counts[idx]
            .checked_sub(1)
            .unwrap_or_else(|| panic!("physical lock holder count underflowed: mode={mode}"));
        if self.granted_counts[idx] == 0 {
            self.grant_mask.remove(mode);
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.families.is_empty()
            && self.granted_counts == [0; MODE_COUNT]
            && self.grant_mask.is_empty()
            && self.wait_queue.is_linked_empty()
            && self.wait_queue.live_count() == 0
    }
}

/// One family's complete physical participation in a resource.
///
/// Exact logical claims are intentionally absent:
///
/// - `Held` is an accepted family aggregate.
/// - `Queued` is a first physical claim waiting for compatibility and FIFO.
/// - `Provisional` has been counted as granted for arbitration, but its unique
///   waiter has not yet observed and adopted the grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhysicalFamilyState {
    /// Accepted physical aggregate in its owner-side covering mode.
    Held { mode: LockMode },
    /// Uncounted first-family request linked in the FIFO queue.
    Queued { node_id: WaitNodeID },
    /// Counted promoted request awaiting observation by its unique waiter.
    Provisional { mode: LockMode, node_id: WaitNodeID },
}

impl PhysicalFamilyState {
    #[inline]
    const fn counted_mode(self) -> Option<LockMode> {
        match self {
            Self::Held { mode } | Self::Provisional { mode, .. } => Some(mode),
            Self::Queued { .. } => None,
        }
    }
}

enum PendingStart {
    Immediate,
    Waiting {
        node_id: WaitNodeID,
        completion: Arc<Completion<()>>,
    },
}

#[derive(Default)]
enum NotificationSet {
    #[default]
    None,
    One(Arc<Completion<()>>),
    Many(Vec<Arc<Completion<()>>>),
}

#[derive(Default)]
struct DeferredNotifications {
    notifications: NotificationSet,
}

impl DeferredNotifications {
    #[inline]
    fn len(&self) -> u64 {
        match &self.notifications {
            NotificationSet::None => 0,
            NotificationSet::One(_) => 1,
            NotificationSet::Many(completions) => completions.len() as u64,
        }
    }

    #[inline]
    fn push(&mut self, completion: Arc<Completion<()>>) {
        self.notifications = match take(&mut self.notifications) {
            NotificationSet::None => NotificationSet::One(completion),
            NotificationSet::One(first) => NotificationSet::Many(vec![first, completion]),
            NotificationSet::Many(mut completions) => {
                completions.push(completion);
                NotificationSet::Many(completions)
            }
        };
    }

    #[inline]
    fn publish(mut self) {
        self.publish_inner();
    }

    #[inline]
    fn publish_inner(&mut self) {
        let notifications = take(&mut self.notifications);
        match notifications {
            NotificationSet::None => {}
            NotificationSet::One(completion) => completion.complete(Ok(())),
            NotificationSet::Many(completions) => {
                for completion in completions {
                    completion.complete(Ok(()));
                }
            }
        }
    }
}

impl Drop for DeferredNotifications {
    #[inline]
    fn drop(&mut self) {
        self.publish_inner();
    }
}

#[inline]
fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

#[inline]
fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

#[inline]
fn increment_current(current: &AtomicU64, peak: &AtomicU64) {
    let value = current.fetch_add(1, Ordering::Relaxed) + 1;
    peak.fetch_max(value, Ordering::Relaxed);
}

#[inline]
fn decrement_current(current: &AtomicU64, label: &'static str) {
    let previous = current.fetch_sub(1, Ordering::Relaxed);
    assert!(
        previous > 0,
        "logical-lock current statistic underflowed: counter={label}"
    );
}

#[inline]
const fn mode_index(mode: LockMode) -> usize {
    match mode {
        LockMode::IntentShared => 0,
        LockMode::IntentExclusive => 1,
        LockMode::Shared => 2,
        LockMode::Exclusive => 3,
    }
}

#[inline]
fn mode_is_valid(resource: LockResource, mode: LockMode) -> bool {
    match resource {
        LockResource::TableMetadata(_) => {
            matches!(mode, LockMode::Shared | LockMode::Exclusive)
        }
        LockResource::TableData(_) => true,
    }
}

/// Returns whether two modes can be granted together on the same resource.
///
/// Compatibility is symmetric and is checked only after both modes have been
/// validated for the resource. Table-metadata resources use the ordinary
/// shared/exclusive matrix:
///
/// ```text
/// TableMetadata
///
///       | S | X
/// ------+---+---
/// S     | Y | N
/// X     | N | N
/// ```
///
/// Table-data resources use the RFC-0016 multi-granularity table-level matrix:
///
/// ```text
/// TableData
///
///       | IS | IX | S | X
/// ------+----+----+---+---
/// IS    | Y  | Y  | Y | N
/// IX    | Y  | Y  | N | N
/// S     | Y  | N  | Y | N
/// X     | N  | N  | N | N
/// ```
#[inline]
fn modes_are_compatible(resource: LockResource, left: LockMode, right: LockMode) -> bool {
    match resource {
        LockResource::TableMetadata(_) => {
            matches!((left, right), (LockMode::Shared, LockMode::Shared))
        }
        LockResource::TableData(_) => matches!(
            (left, right),
            (
                LockMode::IntentShared | LockMode::IntentExclusive | LockMode::Shared,
                LockMode::IntentShared
            ) | (
                LockMode::IntentShared | LockMode::IntentExclusive,
                LockMode::IntentExclusive
            ) | (LockMode::IntentShared | LockMode::Shared, LockMode::Shared)
        ),
    }
}

/// Returns whether `held` is strong enough to satisfy `requested`.
///
/// Coverage is directional: a mode can cover another mode even when the reverse
/// is not true. The lock manager uses this for reentrant acquisitions and
/// immediate same-owner conversions. Table-metadata resources use the ordinary
/// hierarchy where `X` covers every valid request and `S` covers only `S`.
///
/// ```text
/// TableMetadata
///
/// held \ requested | S | X
/// -----------------+---+---
/// S                | Y | N
/// X                | Y | Y
/// ```
///
/// Table-data resources use the RFC-0016 table-level coverage relation. `S`
/// and `IX` are intentionally incomparable because v1 does not introduce a
/// synthetic `SIX` mode.
///
/// ```text
/// TableData
///
/// held \ requested | IS | IX | S | X
/// -----------------+----+----+---+---
/// IS               | Y  | N  | N | N
/// IX               | Y  | Y  | N | N
/// S                | Y  | N  | Y | N
/// X                | Y  | Y  | Y | Y
/// ```
#[inline]
fn mode_covers(resource: LockResource, held: LockMode, requested: LockMode) -> bool {
    match resource {
        LockResource::TableMetadata(_) => held == LockMode::Exclusive || held == requested,
        LockResource::TableData(_) => match held {
            LockMode::IntentShared => requested == LockMode::IntentShared,
            LockMode::IntentExclusive => {
                matches!(
                    requested,
                    LockMode::IntentShared | LockMode::IntentExclusive
                )
            }
            LockMode::Shared => matches!(requested, LockMode::IntentShared | LockMode::Shared),
            LockMode::Exclusive => true,
        },
    }
}

#[inline]
fn upgrade_would_block_err(
    resource: LockResource,
    held: LockMode,
    requested: LockMode,
    owner: LockOwner,
) -> Report<OperationError> {
    Report::new(OperationError::LockUpgradeWouldBlock).attach(format!(
        "resource={resource}, owner={owner}, held={held}, requested={requested}"
    ))
}

#[inline]
fn conversion_not_supported_err(
    resource: LockResource,
    held: LockMode,
    requested: LockMode,
    owner: LockOwner,
) -> Report<OperationError> {
    Report::new(OperationError::LockConversionNotSupported).attach(format!(
        "resource={resource}, owner={owner}, held={held}, requested={requested}"
    ))
}

#[inline]
fn lock_family_conflict_err(
    resource: LockResource,
    held: LockMode,
    requested: LockMode,
    owner: LockOwner,
    held_owner: LockOwner,
) -> Report<OperationError> {
    Report::new(OperationError::LockFamilyConflict).attach(format!(
        "resource={resource}, owner={owner}, family={}, \
             held_owner={held_owner}, held={held}, requested={requested}",
        owner.family()
    ))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::wait::tests::{
        linked_ids as linked_waiter_ids, queue_snapshot, reserve_waiter_capacity,
    };
    use super::*;
    use crate::catalog::storage::layout::BUILTIN_CATALOG_TABLE_IDS;
    use crate::error::{OperationOrFatalError, OperationOrFatalResult};
    use crate::id::ClaimNo;
    use crate::poison::healthy_test_poisoner;
    use std::task::Wake;

    /// Debug snapshot of all physical families and queued waiters.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct LockDebugSnapshot {
        /// Physical family and pending waiter entries.
        pub(crate) entries: Vec<LockDebugEntry>,
        /// Per-resource waiter-slab diagnostics.
        pub(crate) resources: Vec<LockDebugResource>,
    }

    /// Waiter storage diagnostics for one manager resource.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct LockDebugResource {
        /// Resource containing this waiter storage.
        pub(crate) resource: LockResource,
        /// Number of allocated slab slots.
        pub(crate) waiter_slots: usize,
        /// Retained slab vector capacity.
        pub(crate) waiter_capacity: usize,
        /// Number of occupied waiter nodes in any phase.
        pub(crate) live_waiters: usize,
        /// Direct-index free-list order.
        pub(crate) free_slots: Vec<usize>,
        /// Generation of every allocated slot.
        pub(crate) generations: Vec<u64>,
        /// Fixed physical holder counts in IS, IX, S, X order.
        pub(crate) granted_counts: [u32; MODE_COUNT],
        /// Compact fixed-mode holder mask.
        pub(crate) grant_mask: claim::ModeMask,
    }

    /// One granted lock or queued waiter in a debug snapshot.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct LockDebugEntry {
        /// Resource for this entry.
        pub(crate) resource: LockResource,
        /// Requested or granted mode.
        pub(crate) mode: LockMode,
        /// Physical session family for this entry.
        pub(crate) family: LockFamily,
        /// Exact owner retained only while this entry has a pending waiter.
        pub(crate) pending_owner: Option<LockOwner>,
        /// Exact claim number retained only while the entry has a pending waiter.
        pub(crate) claim_no: Option<ClaimNo>,
        /// Whether the entry is granted or waiting.
        pub(crate) state: LockDebugEntryState,
        /// FIFO queue order for waiters; `None` for granted locks.
        pub(crate) queue_order: Option<usize>,
        /// Resource-local waiter slot for queued or provisional state.
        pub(crate) wait_slot: Option<usize>,
        /// Generation paired with `wait_slot`.
        pub(crate) wait_generation: Option<u64>,
    }

    /// Granted-or-waiting state for a debug snapshot entry.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum LockDebugEntryState {
        /// Lock is currently granted.
        Granted,
        /// Lock is waiting in the resource queue.
        Waiting,
        /// A physical family is installed but its waiter has not observed it.
        Provisional,
    }

    /// Test owner that exercises the production family/scope acquisition path.
    pub(crate) struct TestLockOwner {
        authority: Box<FamilyLockAuthority>,
        scope: LockScopeState,
    }

    impl TestLockOwner {
        /// Creates a new test fixture.
        #[inline]
        pub(crate) fn new(owner: LockOwner) -> Self {
            Self {
                authority: FamilyLockAuthority::new(owner.family().session_id()),
                scope: LockScopeState::new(owner),
            }
        }

        /// Acquires the test fixture resource.
        #[inline]
        pub(crate) async fn acquire(
            &mut self,
            manager: &LockManager,
            resource: LockResource,
            mode: LockMode,
        ) -> OperationResult<LockGrant> {
            self.authority
                .family_mut()
                .acquire(
                    &mut self.scope,
                    manager,
                    healthy_test_poisoner(),
                    resource,
                    mode,
                )
                .await
                .map_err(|error| match error {
                    OperationOrFatalError::Operation(report) => report,
                    OperationOrFatalError::Fatal(report) => {
                        panic!("shared healthy test poisoner returned Fatal: {report:?}")
                    }
                })
        }

        /// Releases the test fixture resource.
        #[inline]
        pub(crate) fn release(&mut self, manager: &LockManager, resource: LockResource) -> bool {
            self.authority
                .family_mut()
                .release(&mut self.scope, manager, resource)
        }

        /// Closes the test fixture.
        #[inline]
        pub(crate) fn close(mut self, manager: &LockManager) {
            self.authority
                .family_mut()
                .close_scope(&mut self.scope, manager);
            self.authority.close_session(manager);
        }
    }

    struct ReenterResourceOnWake {
        manager: Arc<LockManager>,
        resource: LockResource,
        wakes: AtomicU64,
    }

    impl Wake for ReenterResourceOnWake {
        fn wake(self: Arc<Self>) {
            let token = pending_token(self.resource, 90);
            start_immediate(&self.manager, &token, LockMode::Shared);
            self.manager.cancel_fresh_grant(token, LockMode::Shared);
            self.wakes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Captures the current lock table for tests.
    #[inline]
    pub(crate) fn debug_snapshot(manager: &LockManager) -> LockDebugSnapshot {
        let mut resources: Vec<_> = manager
            .user
            .iter()
            .map(|resource_state| *resource_state.key())
            .collect();
        for table_id in BUILTIN_CATALOG_TABLE_IDS {
            resources.extend([
                LockResource::TableMetadata(table_id),
                LockResource::TableData(table_id),
            ]);
        }
        resources.sort_unstable();
        let mut entries = Vec::new();
        let mut resource_diagnostics = Vec::new();
        for resource in resources {
            inspect_resource(manager, resource, |resource_state| {
                entries.extend(snapshot_entries(resource_state, resource));
                let queue = queue_snapshot(&resource_state.wait_queue);
                resource_diagnostics.push(LockDebugResource {
                    resource,
                    waiter_slots: queue.slab.slots_len,
                    waiter_capacity: queue.slab.capacity,
                    live_waiters: queue.slab.live_count,
                    free_slots: queue.slab.free_order,
                    generations: queue.slab.generations,
                    granted_counts: resource_state.granted_counts,
                    grant_mask: resource_state.grant_mask,
                });
            });
        }
        LockDebugSnapshot {
            entries,
            resources: resource_diagnostics,
        }
    }

    /// Inspects one active resource without exposing its synchronization guard.
    pub(super) fn inspect_resource<R>(
        manager: &LockManager,
        resource: LockResource,
        inspect: impl FnOnce(&ResourceState) -> R,
    ) -> Option<R> {
        if let Some(cell) = manager.catalog_cell(resource) {
            let state = cell.lock();
            (!state.is_empty()).then(|| inspect(&state))
        } else {
            manager
                .user
                .get(&resource)
                .filter(|state| !state.is_empty())
                .map(|state| inspect(&state))
        }
    }

    #[inline]
    fn snapshot_entries(
        resource_state: &ResourceState,
        resource: LockResource,
    ) -> Vec<LockDebugEntry> {
        let queue_ids = linked_waiter_ids(&resource_state.wait_queue);
        let mut entries = Vec::with_capacity(
            resource_state.families.len() + resource_state.wait_queue.live_count(),
        );
        entries.extend(resource_state.families.iter().map(|(&family, &physical)| {
            let (mode, owner, claim_no, state, node_id, queue_order) = match physical {
                PhysicalFamilyState::Held { mode } => {
                    (mode, None, None, LockDebugEntryState::Granted, None, None)
                }
                PhysicalFamilyState::Queued { node_id } => {
                    let waiter = resource_state.wait_queue.node(node_id);
                    (
                        waiter.target_mode,
                        Some(waiter.owner),
                        Some(waiter.claim_no),
                        LockDebugEntryState::Waiting,
                        Some(node_id),
                        queue_ids.iter().position(|&id| id == node_id),
                    )
                }
                PhysicalFamilyState::Provisional { mode, node_id } => {
                    let waiter = resource_state.wait_queue.node(node_id);
                    (
                        mode,
                        Some(waiter.owner),
                        Some(waiter.claim_no),
                        LockDebugEntryState::Provisional,
                        Some(node_id),
                        None,
                    )
                }
            };
            LockDebugEntry {
                resource,
                mode,
                family,
                pending_owner: owner,
                claim_no,
                state,
                queue_order,
                wait_slot: node_id.map(|id| id.slot),
                wait_generation: node_id.map(|id| id.generation),
            }
        }));
        entries
    }

    /// Representative dynamic and fixed identities preserving the resource kind.
    pub(super) fn resource_variants(resource: LockResource) -> [LockResource; 3] {
        use crate::catalog::storage::layout::{TABLE_ID_TABLE_BINDINGS, TABLE_ID_TABLES};
        let fixed = |id| match resource {
            LockResource::TableMetadata(_) => LockResource::TableMetadata(id),
            LockResource::TableData(_) => LockResource::TableData(id),
        };
        [
            resource,
            fixed(TABLE_ID_TABLES),
            fixed(TABLE_ID_TABLE_BINDINGS),
        ]
    }

    fn assert_drained(manager: &LockManager) {
        let stats = manager.stats();
        assert_eq!(stats.current_physical_resources, 0);
        assert_eq!(stats.current_physical_families, 0);
        assert_eq!(stats.current_linked_waiters, 0);
        assert_eq!(stats.current_live_waiter_nodes, 0);
        assert!(debug_snapshot(manager).resources.is_empty());
        assert!(manager.user.is_empty());
    }

    fn table_data(id: TableID) -> LockResource {
        LockResource::TableData(id)
    }

    fn table_metadata(id: TableID) -> LockResource {
        LockResource::TableMetadata(id)
    }

    fn trx(id: TrxID) -> LockOwner {
        LockOwner::transaction(SessionID::new(id.as_u64()), id)
    }

    fn session(id: SessionID) -> LockOwner {
        LockOwner::session_explicit(id)
    }

    fn assert_operation_err<T>(res: OperationResult<T>, expected: OperationError) {
        let err = res.err().unwrap();
        assert_eq!(*err.current_context(), expected);
    }

    fn operation_result<T>(result: OperationOrFatalResult<T>) -> OperationResult<T> {
        result.map_err(|error| match error {
            OperationOrFatalError::Operation(report) => report,
            OperationOrFatalError::Fatal(report) => {
                panic!("shared healthy test poisoner returned Fatal: {report:?}")
            }
        })
    }

    fn count_entries(
        snapshot: &LockDebugSnapshot,
        resource: LockResource,
        state: LockDebugEntryState,
    ) -> usize {
        snapshot
            .entries
            .iter()
            .filter(|entry| entry.resource == resource && entry.state == state)
            .count()
    }

    fn pending_token(resource: LockResource, session_id: u64) -> PendingClaimToken {
        PendingClaimToken {
            resource,
            owner: session(SessionID::new(session_id)),
            claim_no: ClaimNo::new(1),
        }
    }

    fn start_immediate(manager: &LockManager, token: &PendingClaimToken, mode: LockMode) {
        assert!(matches!(
            manager.start_pending(token, mode).unwrap(),
            PendingStart::Immediate
        ));
    }

    fn start_waiting(
        manager: &LockManager,
        token: &PendingClaimToken,
        mode: LockMode,
    ) -> (WaitNodeID, Arc<Completion<()>>) {
        let PendingStart::Waiting {
            node_id,
            completion,
        } = manager.start_pending(token, mode).unwrap()
        else {
            panic!("conflicting request must queue");
        };
        (node_id, completion)
    }

    #[test]
    fn lock_owner_identity_carries_family_and_exact_scope() {
        let session_id = SessionID::new(7);
        let trx_id = TrxID::new(11);
        let explicit = LockOwner::session_explicit(session_id);
        let trx_owner = LockOwner::transaction(session_id, trx_id);
        let ddl_owner =
            LockOwner::operation(SessionOperationKey::new(session_id, OperationID::new(5)));
        let maintenance_owner =
            LockOwner::operation(SessionOperationKey::new(session_id, OperationID::new(6)));

        for owner in [explicit, trx_owner, ddl_owner, maintenance_owner] {
            assert_eq!(owner.family(), LockFamily::new(session_id));
        }
        assert_ne!(explicit, trx_owner);
        assert_ne!(ddl_owner, maintenance_owner);
        assert_ne!(trx_owner, LockOwner::transaction(SessionID::new(8), trx_id));

        assert_eq!(explicit.to_string(), "session_explicit(session_id=7)");
        assert_eq!(trx_owner.to_string(), "transaction(session_id=7,trx_id=11)");
        assert_eq!(
            ddl_owner.to_string(),
            "operation(session_id=7,operation_id=5)"
        );
        assert_eq!(
            maintenance_owner.to_string(),
            "operation(session_id=7,operation_id=6)"
        );
    }

    #[test]
    fn table_data_compatibility_matrix_matches_rfc() {
        let resource = table_data(TableID::new(1));
        let modes = [
            LockMode::IntentShared,
            LockMode::IntentExclusive,
            LockMode::Shared,
            LockMode::Exclusive,
        ];
        let expected = [
            [true, true, true, false],
            [true, true, false, false],
            [true, false, true, false],
            [false, false, false, false],
        ];

        for (left_idx, left) in modes.iter().copied().enumerate() {
            for (right_idx, right) in modes.iter().copied().enumerate() {
                assert_eq!(
                    modes_are_compatible(resource, left, right),
                    expected[left_idx][right_idx],
                    "left={left:?}, right={right:?}"
                );
            }
        }
    }

    #[test]
    fn metadata_only_accepts_shared_and_exclusive() {
        let resource = table_metadata(TableID::new(1));
        assert!(modes_are_compatible(
            resource,
            LockMode::Shared,
            LockMode::Shared
        ));
        assert!(!modes_are_compatible(
            resource,
            LockMode::Shared,
            LockMode::Exclusive
        ));
        assert!(!modes_are_compatible(
            resource,
            LockMode::Exclusive,
            LockMode::Shared
        ));
    }

    #[test]
    fn multiple_compatible_holders_grant_together() {
        for resource in resource_variants(table_data(TableID::new(7))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut first = TestLockOwner::new(trx(TrxID::new(1)));
                let mut second = TestLockOwner::new(trx(TrxID::new(2)));
                let mut third = TestLockOwner::new(trx(TrxID::new(3)));
                first
                    .acquire(&manager, resource, LockMode::IntentShared)
                    .await
                    .unwrap();
                second
                    .acquire(&manager, resource, LockMode::IntentExclusive)
                    .await
                    .unwrap();
                third
                    .acquire(&manager, resource, LockMode::IntentShared)
                    .await
                    .unwrap();
                let snapshot = debug_snapshot(&manager);
                assert_eq!(
                    count_entries(&snapshot, resource, LockDebugEntryState::Granted),
                    3
                );
                first.close(&manager);
                second.close(&manager);
                third.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn newer_compatible_request_waits_behind_older_incompatible_waiter() {
        for resource in resource_variants(table_metadata(TableID::new(9))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut holder = TestLockOwner::new(trx(TrxID::new(1)));
                holder
                    .acquire(&manager, resource, LockMode::Shared)
                    .await
                    .unwrap();
                let mut exclusive = TestLockOwner::new(trx(TrxID::new(2)));
                let mut exclusive_acquire =
                    Box::pin(exclusive.acquire(&manager, resource, LockMode::Exclusive));
                assert!(matches!(
                    futures::poll!(exclusive_acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                let mut compatible = TestLockOwner::new(trx(TrxID::new(3)));
                let mut compatible_acquire =
                    Box::pin(compatible.acquire(&manager, resource, LockMode::Shared));
                assert!(matches!(
                    futures::poll!(compatible_acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                let snapshot = debug_snapshot(&manager);
                assert_eq!(
                    count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                    2
                );
                holder.close(&manager);
                exclusive_acquire.await.unwrap();
                exclusive.close(&manager);
                compatible_acquire.await.unwrap();
                compatible.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn release_grants_next_compatible_fifo_group() {
        for resource in resource_variants(table_data(TableID::new(11))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut holder = TestLockOwner::new(trx(TrxID::new(1)));
                holder
                    .acquire(&manager, resource, LockMode::Exclusive)
                    .await
                    .unwrap();
                let mut shared = TestLockOwner::new(trx(TrxID::new(2)));
                let mut intent_shared = TestLockOwner::new(trx(TrxID::new(3)));
                let mut intent_exclusive = TestLockOwner::new(trx(TrxID::new(4)));
                let mut shared_acquire =
                    Box::pin(shared.acquire(&manager, resource, LockMode::Shared));
                let mut intent_shared_acquire =
                    Box::pin(intent_shared.acquire(&manager, resource, LockMode::IntentShared));
                let mut intent_exclusive_acquire = Box::pin(intent_exclusive.acquire(
                    &manager,
                    resource,
                    LockMode::IntentExclusive,
                ));
                assert!(matches!(
                    futures::poll!(shared_acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                assert!(matches!(
                    futures::poll!(intent_shared_acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                assert!(matches!(
                    futures::poll!(intent_exclusive_acquire.as_mut()),
                    std::task::Poll::Pending
                ));

                holder.close(&manager);
                shared_acquire.await.unwrap();
                intent_shared_acquire.await.unwrap();

                let snapshot = debug_snapshot(&manager);
                assert_eq!(
                    snapshot
                        .entries
                        .iter()
                        .filter(|entry| {
                            entry.resource == resource
                                && entry.state == LockDebugEntryState::Waiting
                                && entry.pending_owner == Some(trx(TrxID::new(4)))
                        })
                        .count(),
                    1
                );
                shared.close(&manager);
                intent_shared.close(&manager);
                intent_exclusive_acquire.await.unwrap();
                intent_exclusive.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn release_one_resource_does_not_release_other_resources() {
        smol::block_on(async {
            let manager = LockManager::new();
            let first = table_data(TableID::new(1));
            let second = table_data(TableID::new(2));
            let mut owner = TestLockOwner::new(trx(TrxID::new(10)));
            owner
                .acquire(&manager, first, LockMode::IntentExclusive)
                .await
                .unwrap();
            owner
                .acquire(&manager, second, LockMode::IntentExclusive)
                .await
                .unwrap();
            assert!(owner.release(&manager, first));
            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, first, LockDebugEntryState::Granted),
                0
            );
            assert_eq!(
                count_entries(&snapshot, second, LockDebugEntryState::Granted),
                1
            );
            owner.close(&manager);
        });
    }

    #[test]
    fn same_owner_covered_requests_do_not_duplicate_entries() {
        for resource in resource_variants(table_data(TableID::new(5))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut owner = TestLockOwner::new(session(SessionID::new(1)));
                owner
                    .acquire(&manager, resource, LockMode::Exclusive)
                    .await
                    .unwrap();
                owner
                    .acquire(&manager, resource, LockMode::Shared)
                    .await
                    .unwrap();
                owner
                    .acquire(&manager, resource, LockMode::IntentExclusive)
                    .await
                    .unwrap();
                let snapshot = debug_snapshot(&manager);
                assert_eq!(
                    count_entries(&snapshot, resource, LockDebugEntryState::Granted),
                    1
                );
                assert_eq!(snapshot.entries[0].mode, LockMode::Exclusive);
                owner.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn same_family_covered_request_grants_without_waiting() {
        for resource in resource_variants(table_data(TableID::new(60))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut authority = FamilyLockAuthority::new(SessionID::new(1));
                let family_owner = LockOwner::transaction(SessionID::new(1), TrxID::new(3));
                let mut transaction_scope = LockScopeState::new(family_owner);
                let (family, session_scope) = authority.parts();
                family
                    .acquire(
                        session_scope,
                        &manager,
                        healthy_test_poisoner(),
                        resource,
                        LockMode::Exclusive,
                    )
                    .await
                    .unwrap();

                let mut external = TestLockOwner::new(trx(TrxID::new(2)));
                let mut external_acquire =
                    Box::pin(external.acquire(&manager, resource, LockMode::Shared));
                assert!(matches!(
                    futures::poll!(external_acquire.as_mut()),
                    std::task::Poll::Pending
                ));

                family
                    .acquire(
                        &mut transaction_scope,
                        &manager,
                        healthy_test_poisoner(),
                        resource,
                        LockMode::IntentExclusive,
                    )
                    .await
                    .unwrap();

                let snapshot = debug_snapshot(&manager);
                assert!(snapshot.entries.iter().any(|entry| {
                    entry.family == family_owner.family()
                        && entry.mode == LockMode::Exclusive
                        && entry.state == LockDebugEntryState::Granted
                }));
                assert!(snapshot.entries.iter().any(|entry| {
                    entry.pending_owner == Some(trx(TrxID::new(2)))
                        && entry.state == LockDebugEntryState::Waiting
                }));

                family.close_scope(&mut transaction_scope, &manager);
                authority.close_session(&manager);
                external_acquire.await.unwrap();
                external.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn same_family_noncovered_request_is_rejected_without_waiter() {
        for resource in resource_variants(table_data(TableID::new(61))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut authority = FamilyLockAuthority::new(SessionID::new(1));
                let family_owner = LockOwner::transaction(SessionID::new(1), TrxID::new(2));
                let mut transaction_scope = LockScopeState::new(family_owner);
                let (family, session_scope) = authority.parts();
                family
                    .acquire(
                        session_scope,
                        &manager,
                        healthy_test_poisoner(),
                        resource,
                        LockMode::Shared,
                    )
                    .await
                    .unwrap();
                assert_operation_err(
                    operation_result(
                        family
                            .acquire(
                                &mut transaction_scope,
                                &manager,
                                healthy_test_poisoner(),
                                resource,
                                LockMode::IntentExclusive,
                            )
                            .await,
                    ),
                    OperationError::LockFamilyConflict,
                );
                let snapshot = debug_snapshot(&manager);
                assert_eq!(
                    count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                    0
                );
                assert!(snapshot.entries.iter().any(|entry| {
                    entry.family == family_owner.family()
                        && entry.mode == LockMode::Shared
                        && entry.state == LockDebugEntryState::Granted
                }));
                authority.close_session(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn immediate_conversion_succeeds_only_when_it_will_not_wait() {
        for resource in resource_variants(table_data(TableID::new(6))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut first = TestLockOwner::new(trx(TrxID::new(1)));
                first
                    .acquire(&manager, resource, LockMode::IntentShared)
                    .await
                    .unwrap();
                first
                    .acquire(&manager, resource, LockMode::IntentExclusive)
                    .await
                    .unwrap();
                let snapshot = debug_snapshot(&manager);
                assert_eq!(snapshot.entries[0].mode, LockMode::IntentExclusive);

                let mut second = TestLockOwner::new(trx(TrxID::new(2)));
                second
                    .acquire(&manager, resource, LockMode::IntentShared)
                    .await
                    .unwrap();
                assert_operation_err(
                    first.acquire(&manager, resource, LockMode::Exclusive).await,
                    OperationError::LockUpgradeWouldBlock,
                );
                first.close(&manager);
                second.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn incomparable_same_owner_conversion_is_explicit_error() {
        for resource in resource_variants(table_data(TableID::new(8))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut owner = TestLockOwner::new(trx(TrxID::new(1)));
                owner
                    .acquire(&manager, resource, LockMode::IntentExclusive)
                    .await
                    .unwrap();
                assert_operation_err(
                    owner.acquire(&manager, resource, LockMode::Shared).await,
                    OperationError::LockConversionNotSupported,
                );
                owner.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn async_acquire_waits_behind_conflict_and_completes_after_release() {
        for resource in resource_variants(table_metadata(TableID::new(70))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut holder = TestLockOwner::new(trx(TrxID::new(1)));
                holder
                    .acquire(&manager, resource, LockMode::Exclusive)
                    .await
                    .unwrap();
                let mut waiter = TestLockOwner::new(trx(TrxID::new(2)));
                let mut acquire = Box::pin(waiter.acquire(&manager, resource, LockMode::Shared));
                assert!(matches!(
                    futures::poll!(acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                holder.close(&manager);
                acquire.await.unwrap();

                let snapshot = debug_snapshot(&manager);
                assert_eq!(
                    snapshot
                        .entries
                        .iter()
                        .filter(|entry| entry.family == trx(TrxID::new(2)).family()
                            && entry.state == LockDebugEntryState::Granted)
                        .count(),
                    1
                );
                waiter.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn cancelled_acquire_removes_queued_waiter() {
        for resource in resource_variants(table_metadata(TableID::new(72))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut holder = TestLockOwner::new(trx(TrxID::new(1)));
                holder
                    .acquire(&manager, resource, LockMode::Exclusive)
                    .await
                    .unwrap();
                let mut waiter = TestLockOwner::new(trx(TrxID::new(2)));
                let mut acquire = Box::pin(waiter.acquire(&manager, resource, LockMode::Shared));
                assert!(matches!(
                    futures::poll!(acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                drop(acquire);
                waiter.close(&manager);
                let snapshot = debug_snapshot(&manager);
                assert!(
                    !snapshot
                        .entries
                        .iter()
                        .any(|entry| entry.family == trx(TrxID::new(2)).family())
                );
                holder.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn cancelling_front_waiter_grants_later_compatible_waiter() {
        for resource in resource_variants(table_metadata(TableID::new(51))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut holder = TestLockOwner::new(trx(TrxID::new(1)));
                holder
                    .acquire(&manager, resource, LockMode::Shared)
                    .await
                    .unwrap();
                let mut front = TestLockOwner::new(trx(TrxID::new(2)));
                let mut front_acquire =
                    Box::pin(front.acquire(&manager, resource, LockMode::Exclusive));
                assert!(matches!(
                    futures::poll!(front_acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                let mut compatible = TestLockOwner::new(trx(TrxID::new(3)));
                let mut compatible_acquire =
                    Box::pin(compatible.acquire(&manager, resource, LockMode::Shared));
                assert!(matches!(
                    futures::poll!(compatible_acquire.as_mut()),
                    std::task::Poll::Pending
                ));

                drop(front_acquire);
                front.close(&manager);
                compatible_acquire.await.unwrap();

                let snapshot = debug_snapshot(&manager);
                assert!(snapshot.entries.iter().any(|entry| {
                    entry.family == trx(TrxID::new(3)).family()
                        && entry.state == LockDebugEntryState::Granted
                }));
                holder.close(&manager);
                compatible.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn pending_guard_removes_a_promoted_but_unobserved_grant() {
        for resource in resource_variants(table_metadata(TableID::new(52))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut blocker = TestLockOwner::new(trx(TrxID::new(1)));
                blocker
                    .acquire(&manager, resource, LockMode::Exclusive)
                    .await
                    .unwrap();
                let mut waiter = TestLockOwner::new(trx(TrxID::new(2)));
                let mut acquire = Box::pin(waiter.acquire(&manager, resource, LockMode::Shared));
                assert!(matches!(
                    futures::poll!(acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                blocker.close(&manager);
                assert_eq!(
                    count_entries(
                        &debug_snapshot(&manager),
                        resource,
                        LockDebugEntryState::Provisional
                    ),
                    1
                );

                drop(acquire);
                waiter.close(&manager);

                let snapshot = debug_snapshot(&manager);
                assert!(
                    !snapshot
                        .entries
                        .iter()
                        .any(|entry| entry.family == trx(TrxID::new(2)).family())
                );
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn grant_waiters_installs_a_provisional_grant_for_each_fifo_node() {
        let resource = table_data(TableID::new(54));
        let mut resource_state = ResourceState::default();
        let first = resource_state.wait_queue.append(
            trx(TrxID::new(2)),
            ClaimNo::new(2),
            LockMode::IntentShared,
            Arc::new(Completion::new()),
        );
        let second = resource_state.wait_queue.append(
            trx(TrxID::new(3)),
            ClaimNo::new(3),
            LockMode::IntentExclusive,
            Arc::new(Completion::new()),
        );
        resource_state.families.insert(
            trx(TrxID::new(2)).family(),
            PhysicalFamilyState::Queued { node_id: first },
        );
        resource_state.families.insert(
            trx(TrxID::new(3)).family(),
            PhysicalFamilyState::Queued { node_id: second },
        );

        let mut notifications = DeferredNotifications::default();
        resource_state.grant_waiters(resource, &mut notifications);

        assert_eq!(notifications.len(), 2);
        assert_eq!(resource_state.granted_counts, [1, 1, 0, 0]);
        assert_eq!(
            resource_state.wait_queue.node(first).phase,
            WaitNodePhase::Provisional
        );
        assert_eq!(
            resource_state.wait_queue.node(second).phase,
            WaitNodePhase::Provisional
        );
        assert_eq!(
            resource_state.families[&trx(TrxID::new(2)).family()],
            PhysicalFamilyState::Provisional {
                mode: LockMode::IntentShared,
                node_id: first,
            }
        );
        assert_eq!(
            resource_state.families[&trx(TrxID::new(3)).family()],
            PhysicalFamilyState::Provisional {
                mode: LockMode::IntentExclusive,
                node_id: second,
            }
        );
    }

    #[test]
    fn deferred_notifications_publish_on_drop() {
        let first = Arc::new(Completion::new());
        let second = Arc::new(Completion::new());
        {
            let mut notifications = DeferredNotifications::default();
            notifications.push(Arc::clone(&first));
            notifications.push(Arc::clone(&second));
            assert_eq!(notifications.len(), 2);
        }
        assert!(smol::block_on(first.wait_take_result()).is_ok());
        assert!(smol::block_on(second.wait_take_result()).is_ok());
    }

    #[test]
    fn immediate_fresh_grant_allocates_no_waiter_storage() {
        for resource in resource_variants(table_metadata(TableID::new(55))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let owner = trx(TrxID::new(55));
                let mut lock = TestLockOwner::new(owner);
                lock.acquire(&manager, resource, LockMode::Shared)
                    .await
                    .unwrap();

                let snapshot = debug_snapshot(&manager);
                let diagnostics = snapshot
                    .resources
                    .iter()
                    .find(|entry| entry.resource == resource)
                    .unwrap();
                assert_eq!(diagnostics.waiter_slots, 0);
                assert_eq!(diagnostics.waiter_capacity, 0);
                assert_eq!(diagnostics.live_waiters, 0);
                assert!(diagnostics.free_slots.is_empty());
                let grant = snapshot
                    .entries
                    .iter()
                    .find(|entry| entry.family == owner.family())
                    .unwrap();
                assert_eq!(grant.claim_no, None);
                assert_eq!(grant.state, LockDebugEntryState::Granted);
                lock.close(&manager);
                assert_drained(&manager);
            });
        }
    }

    #[test]
    fn debug_snapshot_reports_granted_waiting_and_queue_order() {
        for resource in resource_variants(table_metadata(TableID::new(42))) {
            smol::block_on(async {
                let manager = LockManager::new();
                let mut holder = TestLockOwner::new(trx(TrxID::new(1)));
                holder
                    .acquire(&manager, resource, LockMode::Shared)
                    .await
                    .unwrap();
                let mut first_waiter = TestLockOwner::new(trx(TrxID::new(2)));
                let mut first_acquire =
                    Box::pin(first_waiter.acquire(&manager, resource, LockMode::Exclusive));
                assert!(matches!(
                    futures::poll!(first_acquire.as_mut()),
                    std::task::Poll::Pending
                ));
                let mut second_waiter = TestLockOwner::new(trx(TrxID::new(3)));
                let mut second_acquire =
                    Box::pin(second_waiter.acquire(&manager, resource, LockMode::Exclusive));
                assert!(matches!(
                    futures::poll!(second_acquire.as_mut()),
                    std::task::Poll::Pending
                ));

                let snapshot = debug_snapshot(&manager);
                assert!(snapshot.entries.iter().any(|entry| {
                    entry.resource == resource
                        && entry.family == trx(TrxID::new(1)).family()
                        && entry.mode == LockMode::Shared
                        && entry.state == LockDebugEntryState::Granted
                        && entry.queue_order.is_none()
                }));
                assert!(snapshot.entries.iter().any(|entry| {
                    entry.resource == resource
                        && entry.pending_owner == Some(trx(TrxID::new(2)))
                        && entry.mode == LockMode::Exclusive
                        && entry.state == LockDebugEntryState::Waiting
                        && entry.queue_order == Some(0)
                }));
                assert!(snapshot.entries.iter().any(|entry| {
                    entry.resource == resource
                        && entry.pending_owner == Some(trx(TrxID::new(3)))
                        && entry.mode == LockMode::Exclusive
                        && entry.state == LockDebugEntryState::Waiting
                        && entry.queue_order == Some(1)
                }));

                drop(first_acquire);
                drop(second_acquire);
                first_waiter.close(&manager);
                second_waiter.close(&manager);
                holder.close(&manager);
                assert!(debug_snapshot(&manager).entries.is_empty());
                assert_drained(&manager);
            });
        }
    }
    #[test]
    fn catalog_slots_route_twelve_resources_and_preserve_dynamic_fallback() {
        use crate::catalog::storage::layout::BUILTIN_CATALOG_TABLE_IDS;
        let manager = LockManager::new();
        assert_drained(&manager);
        let mut tokens = Vec::new();
        for id in BUILTIN_CATALOG_TABLE_IDS {
            for resource in [table_metadata(id), table_data(id)] {
                let token = pending_token(resource, 1);
                start_immediate(&manager, &token, LockMode::Shared);
                tokens.push(token);
            }
        }
        assert!(manager.user.is_empty());
        assert_eq!(manager.stats().current_physical_resources, 12);
        for id in [
            0,
            (1_u64 << 63) + 6,
            (1_u64 << 63) + (1_u64 << 32),
            u64::MAX,
        ] {
            for resource in [
                table_metadata(TableID::new(id)),
                table_data(TableID::new(id)),
            ] {
                let token = pending_token(resource, 1);
                start_immediate(&manager, &token, LockMode::Shared);
                tokens.push(token);
            }
        }
        assert_eq!(manager.user.len(), 8);
        let snapshot = debug_snapshot(&manager);
        assert_eq!(snapshot.resources.len(), 20);
        assert!(
            snapshot
                .resources
                .windows(2)
                .all(|pair| pair[0].resource < pair[1].resource)
        );
        for (index, token) in tokens.into_iter().enumerate() {
            manager.cancel_fresh_grant(token, LockMode::Shared);
            assert_eq!(
                manager.stats().current_physical_resources,
                19 - index as u64
            );
        }
        assert_eq!(manager.stats().peak_physical_resources, 20);
        assert_drained(&manager);
    }

    #[test]
    fn catalog_cells_synchronize_and_drain_independently() {
        use crate::catalog::storage::layout::BUILTIN_CATALOG_TABLE_IDS;
        let manager = LockManager::new();
        let resource = table_metadata(BUILTIN_CATALOG_TABLE_IDS[0]);
        let token = pending_token(resource, 1);
        start_immediate(&manager, &token, LockMode::Shared);
        let guard = manager.catalog_cell(resource).unwrap().lock();
        for id in BUILTIN_CATALOG_TABLE_IDS {
            for other in [table_metadata(id), table_data(id)] {
                if other == resource {
                    continue;
                }
                // try_lock is the synchronization predicate; no timing assumption is needed.
                assert!(manager.catalog_cell(other).unwrap().try_lock().is_some());
                let other_token = pending_token(other, 2);
                start_immediate(&manager, &other_token, LockMode::Shared);
                manager.cancel_fresh_grant(other_token, LockMode::Shared);
                assert!(manager.catalog_cell(other).unwrap().lock().is_empty());
            }
        }
        assert_eq!(guard.families.len(), 1);
        drop(guard);
        manager.cancel_fresh_grant(token, LockMode::Shared);
        assert_drained(&manager);
    }

    #[test]
    fn catalog_full_drain_retains_working_capacity_and_provisional_nodes_pin_state() {
        use crate::catalog::storage::layout::TABLE_ID_TABLE_BINDINGS;
        let manager = LockManager::new();
        let resource = table_data(TABLE_ID_TABLE_BINDINGS);
        let mut previous_generations = Vec::new();
        for cycle in 0..2 {
            let before_stats = manager.stats();
            let blocker = pending_token(resource, 1);
            start_immediate(&manager, &blocker, LockMode::Exclusive);
            let mut waiters = (2..66)
                .map(|id| {
                    let token = pending_token(resource, id);
                    let (node, _) = start_waiting(&manager, &token, LockMode::Shared);
                    (token, node)
                })
                .collect::<Vec<_>>();
            let (cancelled, old_node) = waiters.pop().unwrap();
            manager.cancel_waiting(cancelled, LockMode::Shared, old_node);
            let reused = pending_token(resource, 66);
            let (new_node, _) = start_waiting(&manager, &reused, LockMode::Shared);
            assert_eq!(old_node.slot, new_node.slot);
            assert_ne!(old_node.generation, new_node.generation);
            waiters.push((reused, new_node));
            manager.cancel_fresh_grant(blocker, LockMode::Exclusive);
            {
                let state = manager.catalog_cell(resource).unwrap().lock();
                assert!(state.wait_queue.is_linked_empty());
                assert_eq!(state.wait_queue.live_count(), 64);
                assert!(!state.is_empty());
                assert!(state.families.capacity() >= 64);
                let generations = queue_snapshot(&state.wait_queue).slab.generations;
                if cycle != 0 {
                    assert!(
                        generations
                            .iter()
                            .zip(&previous_generations)
                            .all(|(current, previous)| current > previous)
                    );
                }
                previous_generations = generations;
            }
            for (index, (token, node)) in waiters.into_iter().enumerate() {
                if index % 2 == 0 {
                    manager.observe_pending(&token, LockMode::Shared, node);
                    manager.cancel_fresh_grant(token, LockMode::Shared);
                } else {
                    manager.cancel_waiting(token, LockMode::Shared, node);
                }
            }
            let state = manager.catalog_cell(resource).unwrap().lock();
            assert!(state.is_empty());
            assert!(state.families.capacity() > 0);
            assert!(state.families.capacity() <= CATALOG_STATE_RETAIN_CAPACITY);
            let queue = queue_snapshot(&state.wait_queue);
            assert!(queue.slab.capacity >= 64);
            assert!(queue.slab.capacity <= CATALOG_STATE_RETAIN_CAPACITY);
            assert_eq!(queue.slab.slots_len, 64);
            assert_eq!(queue.slab.free_order.len(), 64);
            assert_eq!(state.granted_counts, [0; MODE_COUNT]);
            assert!(state.grant_mask.is_empty());
            drop(state);
            assert_drained(&manager);
            if cycle != 0 {
                assert_eq!(
                    manager.stats().waiter_slab_growths,
                    before_stats.waiter_slab_growths
                );
                assert_eq!(
                    manager.stats().waiter_slab_reuses - before_stats.waiter_slab_reuses,
                    65
                );
            }
        }
        assert_eq!(manager.stats().waiter_slab_reuses, 66);
    }

    #[test]
    fn catalog_full_drain_reclaims_only_containers_above_capacity_limit() {
        // Map reserve requests are additional entries and can round upward.
        // Slab reservations exercise vector capacity separately from slot count.
        for (family_reserve, waiter_capacity, drop_families, drop_waiters) in [
            (16, 8, false, false),
            (16, 1024, false, false),
            (1024, 8, true, false),
            (16, 1025, false, true),
            (1024, 1025, true, true),
        ] {
            for resource in [
                table_metadata(BUILTIN_CATALOG_TABLE_IDS[0]),
                table_data(BUILTIN_CATALOG_TABLE_IDS[0]),
            ] {
                let manager = LockManager::new();
                let blocker = pending_token(resource, 1);
                start_immediate(&manager, &blocker, LockMode::Exclusive);
                let waiter = pending_token(resource, 2);
                let (node, _) = start_waiting(&manager, &waiter, LockMode::Shared);
                let capacities = {
                    let mut state = manager.catalog_cell(resource).unwrap().lock();
                    state.families.reserve(family_reserve);
                    reserve_waiter_capacity(&mut state.wait_queue, waiter_capacity);
                    assert_eq!(state.wait_queue.allocated_slots(), 1);
                    assert_eq!(state.wait_queue.capacity(), waiter_capacity);
                    (state.families.capacity(), state.wait_queue.capacity())
                };
                assert_eq!(capacities.0 > CATALOG_STATE_RETAIN_CAPACITY, drop_families);
                assert_eq!(capacities.1 > CATALOG_STATE_RETAIN_CAPACITY, drop_waiters);

                manager.cancel_fresh_grant(blocker, LockMode::Exclusive);
                {
                    let state = manager.catalog_cell(resource).unwrap().lock();
                    assert!(state.wait_queue.is_linked_empty());
                    assert_eq!(state.wait_queue.live_count(), 1);
                    // A provisional grant pins even oversized idle-looking storage.
                    assert_eq!(state.families.capacity(), capacities.0);
                    assert_eq!(state.wait_queue.capacity(), capacities.1);
                }
                manager.cancel_waiting(waiter, LockMode::Shared, node);
                {
                    let state = manager.catalog_cell(resource).unwrap().lock();
                    assert!(state.is_empty());
                    assert_eq!(
                        state.families.capacity(),
                        if drop_families { 0 } else { capacities.0 }
                    );
                    assert_eq!(
                        state.wait_queue.capacity(),
                        if drop_waiters { 0 } else { capacities.1 }
                    );
                    assert_eq!(
                        state.wait_queue.allocated_slots(),
                        usize::from(!drop_waiters)
                    );
                }
                assert_drained(&manager);

                let before_stats = manager.stats();
                let blocker = pending_token(resource, 3);
                start_immediate(&manager, &blocker, LockMode::Exclusive);
                let waiter = pending_token(resource, 4);
                let (next_node, _) = start_waiting(&manager, &waiter, LockMode::Shared);
                if !drop_waiters {
                    assert_eq!(next_node.slot, node.slot);
                    assert!(next_node.generation > node.generation);
                }
                manager.cancel_waiting(waiter, LockMode::Shared, next_node);
                manager.cancel_fresh_grant(blocker, LockMode::Exclusive);
                let after_stats = manager.stats();
                assert_eq!(
                    after_stats.waiter_slab_growths - before_stats.waiter_slab_growths,
                    u64::from(drop_waiters)
                );
                assert_eq!(
                    after_stats.waiter_slab_reuses - before_stats.waiter_slab_reuses,
                    u64::from(!drop_waiters)
                );
                assert_drained(&manager);
            }
        }
    }

    #[test]
    fn queued_cancellation_preserves_head_middle_tail_and_exact_nodes_on_both_stores() {
        for resource in resource_variants(table_data(TableID::new(80))) {
            for cancelled_index in 0..3 {
                let manager = LockManager::new();
                let blocker = pending_token(resource, 1);
                start_immediate(&manager, &blocker, LockMode::Exclusive);
                let mut waiters = (2..5)
                    .map(|id| {
                        let token = pending_token(resource, id);
                        let (node, _) = start_waiting(&manager, &token, LockMode::Shared);
                        Some((token, node))
                    })
                    .collect::<Vec<_>>();
                let (token, node) = waiters[cancelled_index].take().unwrap();
                manager.cancel_waiting(token, LockMode::Shared, node);
                let stats = manager.stats();
                assert_eq!(
                    [
                        stats.cancelled_head_waiters,
                        stats.cancelled_middle_waiters,
                        stats.cancelled_tail_waiters
                    ][cancelled_index],
                    1
                );
                manager.cancel_fresh_grant(blocker, LockMode::Exclusive);
                for (token, node) in waiters.into_iter().flatten() {
                    manager.cancel_waiting(token, LockMode::Shared, node);
                }
                assert_drained(&manager);
            }
        }
    }

    #[test]
    fn release_cancel_and_reacquire_races_preserve_resource_accounting() {
        use std::sync::Barrier;
        use std::thread;
        for resource in resource_variants(table_data(TableID::new(81))) {
            let manager = LockManager::new();
            let barrier = Barrier::new(2);
            thread::scope(|scope| {
                for session_id in 1..=2 {
                    let manager = &manager;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        for _ in 0..300 {
                            barrier.wait();
                            let token = pending_token(resource, session_id);
                            start_immediate(manager, &token, LockMode::Shared);
                            barrier.wait();
                            manager.cancel_fresh_grant(token, LockMode::Shared);
                            // This acquisition can race the peer's last release and dynamic eviction.
                            let token = pending_token(resource, session_id);
                            start_immediate(manager, &token, LockMode::Shared);
                            manager.cancel_fresh_grant(token, LockMode::Shared);
                        }
                    });
                }
            });
            assert_drained(&manager);
        }
    }

    #[test]
    fn promotion_wakes_can_reenter_resource_after_explicit_drop_and_unwind_publication() {
        use std::future::Future;
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::task::{Context, Waker};
        for resource in resource_variants(table_metadata(TableID::new(82))) {
            for publication in 0..3 {
                let manager = Arc::new(LockManager::new());
                let blocker = pending_token(resource, 1);
                start_immediate(&manager, &blocker, LockMode::Exclusive);
                let waiter = pending_token(resource, 2);
                let (node, completion) = start_waiting(&manager, &waiter, LockMode::Shared);
                let wake = Arc::new(ReenterResourceOnWake {
                    manager: Arc::clone(&manager),
                    resource,
                    wakes: AtomicU64::new(0),
                });
                let waker = Waker::from(Arc::clone(&wake));
                let mut context = Context::from_waker(&waker);
                let mut observe = Box::pin(completion.wait_take_result());
                assert!(observe.as_mut().poll(&mut context).is_pending());
                if publication == 0 {
                    manager.cancel_fresh_grant(blocker, LockMode::Exclusive);
                } else {
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        let mut notify = DeferredNotifications::default();
                        manager.with_resource(
                            resource,
                            ResourceAccess::Existing,
                            "test_deferred_release",
                            |state| {
                                state.remove_fresh_grant(&blocker, LockMode::Exclusive);
                                decrement_current(
                                    &manager.stats.current_physical_families,
                                    "current_physical_families",
                                );
                                state.grant_waiters(resource, &mut notify);
                                manager.record_promotions(&notify);
                                if publication == 2 {
                                    panic!("exercise committed promotion unwind");
                                }
                            },
                        );
                        // Exercise DeferredNotifications::drop rather than explicit publication.
                    }));
                    assert_eq!(result.is_err(), publication == 2);
                }
                assert_eq!(wake.wakes.load(Ordering::Relaxed), 1);
                assert!(observe.as_mut().poll(&mut context).is_ready());
                manager.cancel_waiting(waiter, LockMode::Shared, node);
                assert_drained(&manager);
            }
        }
    }
    #[test]
    fn mixed_snapshots_keep_queued_and_provisional_resources_until_independent_drain() {
        let manager = LockManager::new();
        let resources = [
            table_data(BUILTIN_CATALOG_TABLE_IDS[0]),
            table_metadata(TableID::new(3)),
            table_metadata(BUILTIN_CATALOG_TABLE_IDS[0]),
        ];
        let mut pending = Vec::new();
        for resource in resources {
            let blocker = pending_token(resource, 1);
            start_immediate(&manager, &blocker, LockMode::Exclusive);
            let shared = pending_token(resource, 2);
            let (shared_node, _) = start_waiting(&manager, &shared, LockMode::Shared);
            let exclusive = pending_token(resource, 3);
            let (exclusive_node, _) = start_waiting(&manager, &exclusive, LockMode::Exclusive);
            manager.cancel_fresh_grant(blocker, LockMode::Exclusive);
            pending.push((shared, shared_node, exclusive, exclusive_node));
        }
        let snapshot = debug_snapshot(&manager);
        assert_eq!(snapshot.resources.len(), 3);
        assert!(
            snapshot
                .resources
                .windows(2)
                .all(|pair| pair[0].resource < pair[1].resource)
        );
        for resource in resources {
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Provisional),
                1
            );
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                1
            );
        }
        for (index, (shared, shared_node, exclusive, exclusive_node)) in
            pending.into_iter().enumerate()
        {
            manager.cancel_waiting(exclusive, LockMode::Exclusive, exclusive_node);
            manager.cancel_waiting(shared, LockMode::Shared, shared_node);
            assert_eq!(debug_snapshot(&manager).resources.len(), 2 - index);
        }
        assert_drained(&manager);
    }

    #[test]
    fn queued_or_provisional_cancellation_races_release_and_reacquisition() {
        use std::sync::Barrier;
        use std::sync::mpsc::sync_channel;
        use std::thread;
        for resource in resource_variants(table_data(TableID::new(83))) {
            let manager = LockManager::new();
            let barrier = Barrier::new(2);
            thread::scope(|scope| {
                let (sender, receiver) = sync_channel(1);
                let manager = &manager;
                let barrier = &barrier;
                scope.spawn(move || {
                    for _ in 0..100 {
                        let (waiter, node) = receiver.recv().unwrap();
                        barrier.wait();
                        manager.cancel_waiting(waiter, LockMode::Shared, node);
                        let next = pending_token(resource, 2);
                        match manager.start_pending(&next, LockMode::Shared).unwrap() {
                            PendingStart::Immediate => {
                                manager.cancel_fresh_grant(next, LockMode::Shared)
                            }
                            PendingStart::Waiting { node_id, .. } => {
                                manager.cancel_waiting(next, LockMode::Shared, node_id)
                            }
                        }
                        barrier.wait();
                    }
                });
                for _ in 0..100 {
                    let blocker = pending_token(resource, 1);
                    start_immediate(manager, &blocker, LockMode::Exclusive);
                    let waiter = pending_token(resource, 2);
                    let (node, _) = start_waiting(manager, &waiter, LockMode::Shared);
                    sender.send((waiter, node)).unwrap();
                    barrier.wait();
                    manager.cancel_fresh_grant(blocker, LockMode::Exclusive);
                    barrier.wait();
                    assert_drained(manager);
                }
            });
        }
    }
}
