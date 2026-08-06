//! Logical metadata and table-data lock manager primitives.
//!
//! This module is the standalone core for RFC-0016 logical locks. It tracks
//! table metadata and table data resources independently from the
//! engine/session/transaction lifecycle wiring that later phases will add.

mod claim;
mod state;
mod wait;

use self::claim::{ClaimToken, PendingClaimToken};
use self::wait::{WaitNodeID, WaitNodePhase, WaitQueue};
use crate::completion::Completion;
use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::error::{OperationError, OperationResult};
use crate::id::{ClaimNo, OperationID, SessionID, SessionOperationKey, TableID, TrxID};
use crate::map::FastDashMap;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use error_stack::Report;
use std::convert::Infallible;
use std::fmt;
use std::result::Result as StdResult;
use std::sync::Arc;

pub(crate) use state::{
    FamilyLockAuthority, FamilyLockState, FreshClaimsGuard, LockScopeState, TransactionLockState,
};

/// Statement number for statement-owned logical locks.
pub(crate) type StmtNo = u64;

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
    /// Locks retained for one statement inside a transaction.
    Statement(TrxID, StmtNo),
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

    /// Derives a statement owner from this authoritative transaction owner.
    #[inline]
    pub(crate) fn statement(self, stmt_no: StmtNo) -> Self {
        let LockScope::Transaction(trx_id) = self.scope else {
            panic!(
                "statement lock owner requires a transaction source: source_owner={self}, stmt_no={stmt_no}"
            )
        };
        Self {
            family: self.family,
            scope: LockScope::Statement(trx_id, stmt_no),
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
            LockScope::Statement(trx_id, stmt_no) => write!(
                f,
                "statement(session_id={session_id},trx_id={trx_id},stmt_no={stmt_no})"
            ),
            LockScope::Operation(operation_id) => write!(
                f,
                "operation(session_id={session_id},operation_id={operation_id})"
            ),
        }
    }
}

/// Whether an acquisition created a new granted lock entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockGrant {
    /// The acquire call created a fresh granted entry.
    Fresh,
    /// The requested mode was already represented by this owner or waiter.
    Existing,
}

/// Standalone logical lock manager.
pub(crate) struct LockManager {
    resources: Arc<FastDashMap<LockResource, ResourceState>>,
}

impl LockManager {
    /// Creates an empty lock manager.
    #[inline]
    pub(crate) fn new() -> Self {
        LockManager {
            resources: Arc::new(FastDashMap::default()),
        }
    }

    /// Acquires raw manager state for tests without owner-side indexes.
    #[cfg(test)]
    #[inline]
    pub(crate) async fn acquire(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> OperationResult<()> {
        mode.assert_valid_for(resource);
        let token = PendingClaimToken {
            resource,
            owner,
            claim_no: ClaimNo::new(0),
        };
        if self.raw_existing_acquire(&token, mode)? {
            return Ok(());
        }
        let mut guard = RawPendingGuard::new(self, token, mode);
        guard.start()?;
        guard.wait_and_observe().await?;
        guard.disarm();
        Ok(())
    }

    /// Releases raw manager state for one owner/resource pair in tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn release(&self, resource: LockResource, owner: LockOwner) -> usize {
        self.release_owner_resource(resource, owner)
    }

    /// Releases every granted lock and queued request owned by `owner`.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "retained as an explicit migration and diagnostic defense"
        )
    )]
    #[inline]
    pub(crate) fn release_owner(&self, owner: LockOwner) -> usize {
        let mut removed = 0;
        let mut resources: Vec<_> = self
            .resources
            .iter()
            .map(|resource_state| *resource_state.key())
            .collect();
        resources.sort_unstable();
        for resource in resources {
            removed += self.release_owner_resource(resource, owner);
        }
        removed
    }

    /// Returns whether an exact manager grant covers `requested`.
    #[inline]
    pub(crate) fn owner_holds(
        &self,
        resource: LockResource,
        owner: LockOwner,
        requested: LockMode,
    ) -> bool {
        requested.assert_valid_for(resource);
        self.resources.get(&resource).is_some_and(|resource_state| {
            resource_state.granted.iter().any(|granted| {
                granted.owner == owner
                    && granted.provisional_node.is_none()
                    && mode_covers(resource, granted.mode, requested)
            })
        })
    }

    #[inline]
    fn start_pending(
        &self,
        token: &PendingClaimToken,
        mode: LockMode,
    ) -> OperationResult<PendingStart> {
        mode.assert_valid_for(token.resource);
        let (result, empty_after_error) = {
            let mut resource_state = self.resources.entry(token.resource).or_default();
            let result = resource_state.start_pending(token.resource, token, mode);
            let empty_after_error = result.is_err() && resource_state.is_empty();
            (result, empty_after_error)
        };
        if empty_after_error {
            self.remove_if_empty(token.resource);
        }
        result
    }

    #[inline]
    fn observe_pending(
        &self,
        token: &PendingClaimToken,
        mode: LockMode,
        node_id: WaitNodeID,
    ) -> PendingObservation {
        let (observation, empty) = {
            let mut resource_state = self.resources.get_mut(&token.resource).unwrap_or_else(|| {
                panic!(
                    "pending observation requires retained resource state: \
                         resource={}, owner={}, claim_no={:?}, node_id={node_id:?}",
                    token.resource, token.owner, token.claim_no
                )
            });
            let observation = resource_state.observe_pending(token, mode, node_id);
            (observation, resource_state.is_empty())
        };
        if empty {
            self.remove_if_empty(token.resource);
        }
        observation
    }

    #[inline]
    fn cancel_waiting(&self, token: PendingClaimToken, mode: LockMode, node_id: WaitNodeID) {
        let (notify, empty) = {
            let mut resource_state = self.resources.get_mut(&token.resource).unwrap_or_else(|| {
                panic!(
                    "pending cancellation requires retained resource state: \
                         resource={}, owner={}, claim_no={:?}, node_id={node_id:?}",
                    token.resource, token.owner, token.claim_no
                )
            });
            resource_state
                .wait_queue
                .assert_identity(node_id, &token, mode);
            let mut notify = Vec::new();
            match resource_state.wait_queue.node(node_id).phase {
                WaitNodePhase::Queued { .. } => {
                    resource_state
                        .wait_queue
                        .detach_to(node_id, WaitNodePhase::Released);
                    let _ = resource_state
                        .wait_queue
                        .consume(node_id, WaitNodePhase::Released);
                    notify.extend(resource_state.grant_waiters(token.resource));
                }
                WaitNodePhase::Provisional => {
                    resource_state.remove_provisional(&token, mode, node_id);
                    let _ = resource_state
                        .wait_queue
                        .consume(node_id, WaitNodePhase::Provisional);
                    notify.extend(resource_state.grant_waiters(token.resource));
                }
                WaitNodePhase::Released => {
                    let _ = resource_state
                        .wait_queue
                        .consume(node_id, WaitNodePhase::Released);
                }
            }
            (notify, resource_state.is_empty())
        };
        if empty {
            self.remove_if_empty(token.resource);
        }
        notify_completions(notify);
    }

    #[inline]
    fn cancel_fresh_grant(&self, token: PendingClaimToken, mode: LockMode) {
        let (notify, empty) = {
            let mut resource_state = self.resources.get_mut(&token.resource).unwrap_or_else(|| {
                panic!(
                    "fresh pending rollback requires retained resource state: \
                         resource={}, owner={}, claim_no={:?}",
                    token.resource, token.owner, token.claim_no
                )
            });
            resource_state.remove_fresh_grant(&token, mode);
            let notify = resource_state.grant_waiters(token.resource);
            (notify, resource_state.is_empty())
        };
        if empty {
            self.remove_if_empty(token.resource);
        }
        notify_completions(notify);
    }

    #[inline]
    fn convert_claim(&self, token: &ClaimToken, mode: LockMode) -> OperationResult<()> {
        mode.assert_valid_for(token.resource);
        let mut resource_state = self.resources.get_mut(&token.resource).unwrap_or_else(|| {
            panic!(
                "accepted conversion requires a manager mirror: \
                     resource={}, owner={}, claim_no={:?}",
                token.resource, token.owner, token.claim_no
            )
        });
        resource_state.convert_claim(token.resource, token, mode)
    }

    #[inline]
    fn release_claim(&self, token: &ClaimToken) -> usize {
        let (notify, empty) = {
            let mut resource_state = self.resources.get_mut(&token.resource).unwrap_or_else(|| {
                panic!(
                    "accepted release requires a manager mirror: \
                         resource={}, owner={}, claim_no={:?}",
                    token.resource, token.owner, token.claim_no
                )
            });
            resource_state.remove_accepted(token);
            let notify = resource_state.grant_waiters(token.resource);
            (notify, resource_state.is_empty())
        };
        if empty {
            self.remove_if_empty(token.resource);
        }
        notify_completions(notify);
        1
    }

    #[inline]
    fn release_owner_resource(&self, resource: LockResource, owner: LockOwner) -> usize {
        let Some(mut resource_state) = self.resources.get_mut(&resource) else {
            return 0;
        };
        let (removed, notify) = resource_state.release_owner(resource, owner);
        let empty = resource_state.is_empty();
        drop(resource_state);
        if empty {
            self.remove_if_empty(resource);
        }
        notify_completions(notify);
        removed
    }

    #[inline]
    fn remove_if_empty(&self, resource: LockResource) {
        self.resources
            .remove_if(&resource, |_resource, resource_state| {
                resource_state.is_empty()
            });
    }

    #[cfg(test)]
    #[inline]
    fn raw_existing_acquire(
        &self,
        token: &PendingClaimToken,
        mode: LockMode,
    ) -> OperationResult<bool> {
        let Some(mut resource_state) = self.resources.get_mut(&token.resource) else {
            return Ok(false);
        };
        let Some(idx) = resource_state.granted_idx(token.owner) else {
            assert!(
                !resource_state.has_waiter_owner(token.owner),
                "duplicate raw pending acquisition is forbidden: \
                 resource={}, owner={}",
                token.resource,
                token.owner
            );
            return Ok(false);
        };
        let accepted = ClaimToken {
            resource: token.resource,
            owner: token.owner,
            claim_no: token.claim_no,
        };
        resource_state.convert_claim(token.resource, &accepted, mode)?;
        Ok(idx < resource_state.granted.len())
    }

    #[cfg(test)]
    #[inline]
    fn restore_raw_claim(&self, token: &ClaimToken, mode: LockMode) {
        mode.assert_valid_for(token.resource);
        let mut resource_state = self.resources.entry(token.resource).or_default();
        assert!(
            resource_state.granted_idx(token.owner).is_none()
                && !resource_state.has_waiter_owner(token.owner),
            "raw exact-claim restoration requires an owner miss: \
             resource={}, owner={}, claim_no={:?}",
            token.resource,
            token.owner,
            token.claim_no
        );
        assert!(
            resource_state.compatible_with_granted(token.resource, mode, token.owner),
            "raw exact-claim restoration must be immediately compatible: \
             resource={}, owner={}, claim_no={:?}, mode={mode}",
            token.resource,
            token.owner,
            token.claim_no
        );
        resource_state.granted.push(GrantedLock {
            owner: token.owner,
            claim_no: token.claim_no,
            mode,
            provisional_node: None,
        });
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
struct ResourceState {
    granted: Vec<GrantedLock>,
    wait_queue: WaitQueue,
}

impl ResourceState {
    #[inline]
    fn start_pending(
        &mut self,
        resource: LockResource,
        token: &PendingClaimToken,
        mode: LockMode,
    ) -> OperationResult<PendingStart> {
        assert!(
            self.granted_idx(token.owner).is_none(),
            "fresh pending claim duplicates an exact manager grant: \
             resource={resource}, owner={}, claim_no={:?}",
            token.owner,
            token.claim_no
        );
        assert!(
            !self.has_waiter_owner(token.owner),
            "fresh pending claim duplicates an exact waiter: \
             resource={resource}, owner={}, claim_no={:?}",
            token.owner,
            token.claim_no
        );
        let family_covered = self.validate_family_coverage(resource, mode, token.owner)?;
        // Fresh compatible requests still wait behind an existing queue so
        // readers or intent holders cannot starve an older incompatible waiter,
        // unless an already-granted same-family lock covers this request.
        if self.compatible_with_granted(resource, mode, token.owner)
            && (family_covered || self.wait_queue.is_linked_empty())
        {
            self.granted.push(GrantedLock {
                owner: token.owner,
                claim_no: token.claim_no,
                mode,
                provisional_node: None,
            });
            return Ok(PendingStart::Immediate);
        }
        let completion = Arc::new(Completion::new());
        let node_id =
            self.wait_queue
                .append(token.owner, token.claim_no, mode, Arc::clone(&completion));
        Ok(PendingStart::Waiting {
            node_id,
            completion,
        })
    }

    #[inline]
    fn convert_claim(
        &mut self,
        resource: LockResource,
        token: &ClaimToken,
        mode: LockMode,
    ) -> OperationResult<()> {
        let idx = self.granted_idx(token.owner).unwrap_or_else(|| {
            panic!(
                "accepted conversion cannot find its exact manager grant: \
                 resource={resource}, owner={}, claim_no={:?}",
                token.owner, token.claim_no
            )
        });
        let granted = self.granted[idx];
        assert!(
            granted.claim_no == token.claim_no && granted.provisional_node.is_none(),
            "accepted conversion token does not identify a held exact grant: \
             resource={resource}, owner={}, token_claim_no={:?}, \
             actual_claim_no={:?}, provisional_node={:?}",
            token.owner,
            token.claim_no,
            granted.claim_no,
            granted.provisional_node
        );
        if mode_covers(resource, granted.mode, mode) {
            return Ok(());
        }
        if !mode_covers(resource, mode, granted.mode) {
            return Err(conversion_not_supported_err(
                resource,
                granted.mode,
                mode,
                token.owner,
            ));
        }
        self.validate_family_coverage(resource, mode, token.owner)?;
        if !self.wait_queue.is_linked_empty()
            || !self.compatible_with_granted(resource, mode, token.owner)
        {
            return Err(upgrade_would_block_err(
                resource,
                granted.mode,
                mode,
                token.owner,
            ));
        }
        self.granted[idx].mode = mode;
        Ok(())
    }

    #[inline]
    fn granted_idx(&self, owner: LockOwner) -> Option<usize> {
        self.granted
            .iter()
            .position(|granted| granted.owner == owner)
    }

    #[inline]
    fn has_waiter_owner(&self, owner: LockOwner) -> bool {
        self.wait_queue.any_occupied(|node| node.owner == owner)
    }

    #[inline]
    fn compatible_with_granted(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> bool {
        self.granted.iter().all(|granted| {
            if granted.owner == owner {
                return true;
            }
            if granted.owner.family() == owner.family() {
                return mode_covers(resource, granted.mode, mode);
            }
            modes_are_compatible(resource, granted.mode, mode)
        })
    }

    #[inline]
    fn validate_family_coverage(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> OperationResult<bool> {
        let mut covered = false;
        for granted in self.granted.iter() {
            if granted.owner == owner || granted.owner.family() != owner.family() {
                continue;
            }
            if !mode_covers(resource, granted.mode, mode) {
                return Err(lock_family_conflict_err(
                    resource,
                    granted.mode,
                    mode,
                    owner,
                    granted.owner,
                ));
            }
            covered = true;
        }
        if let Some(waiter) = self.wait_queue.find_linked(|waiter| {
            waiter.owner != owner
                && waiter.owner.family() == owner.family()
                && !mode_covers(resource, waiter.target_mode, mode)
        }) {
            return Err(lock_family_conflict_err(
                resource,
                waiter.target_mode,
                mode,
                owner,
                waiter.owner,
            ));
        }
        Ok(covered)
    }

    #[inline]
    fn grant_waiters(&mut self, resource: LockResource) -> Vec<Arc<Completion<()>>> {
        let mut notifications = Vec::new();
        while let Some(node_id) = self.wait_queue.head() {
            let node = self.wait_queue.node(node_id);
            let owner = node.owner;
            let claim_no = node.claim_no;
            let mode = node.target_mode;
            if !self.compatible_with_granted(resource, mode, owner) {
                break;
            }
            assert!(
                self.granted_idx(owner).is_none(),
                "FIFO promotion found a duplicate exact manager grant: \
                 resource={resource}, owner={owner}, claim_no={claim_no:?}"
            );
            let completion = Arc::clone(&node.completion);
            self.wait_queue
                .detach_to(node_id, WaitNodePhase::Provisional);
            self.granted.push(GrantedLock {
                owner,
                claim_no,
                mode,
                provisional_node: Some(node_id),
            });
            notifications.push(completion);
        }
        notifications
    }

    #[inline]
    fn observe_pending(
        &mut self,
        token: &PendingClaimToken,
        mode: LockMode,
        node_id: WaitNodeID,
    ) -> PendingObservation {
        self.wait_queue.assert_identity(node_id, token, mode);
        match self.wait_queue.node(node_id).phase {
            WaitNodePhase::Queued { .. } => panic!(
                "completed lock waiter remains queued at observation: \
                 resource={}, owner={}, claim_no={:?}, node_id={node_id:?}",
                token.resource, token.owner, token.claim_no
            ),
            WaitNodePhase::Provisional => {
                let idx = self.granted_idx(token.owner).unwrap_or_else(|| {
                    panic!(
                        "provisional waiter has no exact manager grant: \
                         resource={}, owner={}, claim_no={:?}, node_id={node_id:?}",
                        token.resource, token.owner, token.claim_no
                    )
                });
                let granted = self.granted[idx];
                assert!(
                    granted.claim_no == token.claim_no
                        && granted.mode == mode
                        && granted.provisional_node == Some(node_id),
                    "provisional observation grant identity mismatch: \
                     resource={}, owner={}, claim_no={:?}, mode={}, node_id={node_id:?}, \
                     actual_grant={granted:?}",
                    token.resource,
                    token.owner,
                    token.claim_no,
                    mode
                );
                self.granted[idx].provisional_node = None;
                let _ = self.wait_queue.consume(node_id, WaitNodePhase::Provisional);
                PendingObservation::Adopted
            }
            WaitNodePhase::Released => {
                let _ = self.wait_queue.consume(node_id, WaitNodePhase::Released);
                PendingObservation::Released
            }
        }
    }

    #[inline]
    fn remove_provisional(
        &mut self,
        token: &PendingClaimToken,
        mode: LockMode,
        node_id: WaitNodeID,
    ) {
        let idx = self.granted_idx(token.owner).unwrap_or_else(|| {
            panic!(
                "provisional cancellation cannot find its exact manager grant: \
                 resource={}, owner={}, claim_no={:?}, node_id={node_id:?}",
                token.resource, token.owner, token.claim_no
            )
        });
        let granted = self.granted[idx];
        assert!(
            granted.claim_no == token.claim_no
                && granted.mode == mode
                && granted.provisional_node == Some(node_id),
            "provisional cancellation grant identity mismatch: \
             resource={}, owner={}, claim_no={:?}, mode={}, node_id={node_id:?}, \
             actual_grant={granted:?}",
            token.resource,
            token.owner,
            token.claim_no,
            mode
        );
        self.granted.remove(idx);
    }

    #[inline]
    fn remove_fresh_grant(&mut self, token: &PendingClaimToken, mode: LockMode) {
        let idx = self.granted_idx(token.owner).unwrap_or_else(|| {
            panic!(
                "fresh pending rollback cannot find its exact manager grant: \
                 resource={}, owner={}, claim_no={:?}",
                token.resource, token.owner, token.claim_no
            )
        });
        let granted = self.granted[idx];
        assert!(
            granted.claim_no == token.claim_no
                && granted.mode == mode
                && granted.provisional_node.is_none(),
            "fresh pending rollback grant identity mismatch: \
             resource={}, owner={}, claim_no={:?}, mode={}, actual_grant={granted:?}",
            token.resource,
            token.owner,
            token.claim_no,
            mode
        );
        self.granted.remove(idx);
    }

    #[inline]
    fn remove_accepted(&mut self, token: &ClaimToken) {
        let idx = self.granted_idx(token.owner).unwrap_or_else(|| {
            panic!(
                "accepted release cannot find its exact manager grant: \
                 resource={}, owner={}, claim_no={:?}",
                token.resource, token.owner, token.claim_no
            )
        });
        let granted = self.granted[idx];
        assert!(
            granted.claim_no == token.claim_no && granted.provisional_node.is_none(),
            "accepted release token does not identify a held exact grant: \
             resource={}, owner={}, token_claim_no={:?}, actual_grant={granted:?}",
            token.resource,
            token.owner,
            token.claim_no
        );
        self.granted.remove(idx);
    }

    #[inline]
    fn release_owner(
        &mut self,
        resource: LockResource,
        owner: LockOwner,
    ) -> (usize, Vec<Arc<Completion<()>>>) {
        let mut removed = 0;
        let mut notifications = Vec::new();
        let node_ids = self
            .wait_queue
            .occupied_ids()
            .into_iter()
            .filter(|&id| self.wait_queue.node(id).owner == owner)
            .collect::<Vec<_>>();

        for node_id in node_ids {
            let completion = Arc::clone(&self.wait_queue.node(node_id).completion);
            match self.wait_queue.node(node_id).phase {
                WaitNodePhase::Queued { .. } => {
                    self.wait_queue.detach_to(node_id, WaitNodePhase::Released);
                    notifications.push(completion);
                    removed += 1;
                }
                WaitNodePhase::Provisional => {
                    let node = self.wait_queue.node(node_id);
                    let idx = self.granted_idx(owner).unwrap_or_else(|| {
                        panic!(
                            "migration cleanup found a provisional node without its grant: \
                             resource={resource}, owner={owner}, node_id={node_id:?}"
                        )
                    });
                    assert!(
                        self.granted[idx].claim_no == node.claim_no
                            && self.granted[idx].mode == node.target_mode
                            && self.granted[idx].provisional_node == Some(node_id),
                        "migration cleanup provisional grant/node mismatch: \
                         resource={resource}, owner={owner}, node_id={node_id:?}, \
                         node_claim_no={:?}, node_mode={}, actual_grant={:?}",
                        node.claim_no,
                        node.target_mode,
                        self.granted[idx],
                    );
                    self.granted.remove(idx);
                    self.wait_queue.set_released(node_id);
                    notifications.push(completion);
                    removed += 1;
                }
                WaitNodePhase::Released => {}
            }
        }

        let before = self.granted.len();
        self.granted.retain(|granted| granted.owner != owner);
        removed += before - self.granted.len();
        notifications.extend(self.grant_waiters(resource));
        (removed, notifications)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.granted.is_empty()
            && self.wait_queue.is_linked_empty()
            && self.wait_queue.live_count() == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrantedLock {
    owner: LockOwner,
    claim_no: ClaimNo,
    mode: LockMode,
    provisional_node: Option<WaitNodeID>,
}

enum PendingStart {
    Immediate,
    Waiting {
        node_id: WaitNodeID,
        completion: Arc<Completion<()>>,
    },
}

enum PendingObservation {
    Adopted,
    Released,
}

#[cfg(test)]
struct RawPendingGuard<'a> {
    manager: &'a LockManager,
    token: Option<PendingClaimToken>,
    mode: LockMode,
    state: wait::PendingGuardState,
}

#[cfg(test)]
impl<'a> RawPendingGuard<'a> {
    #[inline]
    fn new(manager: &'a LockManager, token: PendingClaimToken, mode: LockMode) -> Self {
        Self {
            manager,
            token: Some(token),
            mode,
            state: wait::PendingGuardState::NotStarted,
        }
    }

    #[inline]
    fn start(&mut self) -> OperationResult<()> {
        let token = self
            .token
            .as_ref()
            .unwrap_or_else(|| panic!("raw pending guard must retain its token before start"));
        self.state = match self.manager.start_pending(token, self.mode)? {
            PendingStart::Immediate => wait::PendingGuardState::FreshGranted,
            PendingStart::Waiting {
                node_id,
                completion,
            } => wait::PendingGuardState::Waiting {
                node_id,
                completion,
            },
        };
        Ok(())
    }

    #[inline]
    async fn wait_and_observe(&mut self) -> OperationResult<()> {
        let wait::PendingGuardState::Waiting {
            node_id,
            completion,
        } = &self.state
        else {
            return Ok(());
        };
        let node_id = *node_id;
        let completion = Arc::clone(completion);
        assert!(
            completion.wait_take_result().await.is_ok(),
            "raw lock waiter success-only completion carried an error"
        );
        let token = self
            .token
            .as_ref()
            .unwrap_or_else(|| panic!("raw pending guard lost its token before observation"));
        match self.manager.observe_pending(token, self.mode, node_id) {
            PendingObservation::Adopted => {
                self.state = wait::PendingGuardState::FreshGranted;
                Ok(())
            }
            PendingObservation::Released => {
                self.state = wait::PendingGuardState::Disarmed;
                let token = self.token.take().unwrap_or_else(|| {
                    panic!("released raw pending guard must retain its move-only token")
                });
                Err(lock_waiter_released_err(token, self.mode))
            }
        }
    }

    #[inline]
    fn disarm(&mut self) {
        self.state = wait::PendingGuardState::Disarmed;
        let _ = self.token.take();
    }
}

#[cfg(test)]
impl Drop for RawPendingGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let token = self.token.take();
        match &self.state {
            wait::PendingGuardState::NotStarted => {
                assert!(
                    token.is_some(),
                    "unstarted raw pending guard must retain its move-only token"
                );
            }
            wait::PendingGuardState::Disarmed => {
                assert!(
                    token.is_none(),
                    "disarmed raw pending guard must not retain a pending token"
                );
            }
            wait::PendingGuardState::Waiting { node_id, .. } => {
                let token = token.unwrap_or_else(|| {
                    panic!("waiting raw pending guard must retain its move-only token")
                });
                self.manager.cancel_waiting(token, self.mode, *node_id);
            }
            wait::PendingGuardState::FreshGranted => {
                let token = token.unwrap_or_else(|| {
                    panic!("fresh-granted raw pending guard must retain its move-only token")
                });
                self.manager.cancel_fresh_grant(token, self.mode);
            }
        }
    }
}

#[inline]
fn notify_completions(completions: Vec<Arc<Completion<()>>>) {
    for completion in completions {
        completion.complete(Ok(()));
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
fn lock_waiter_released_err(token: PendingClaimToken, mode: LockMode) -> Report<OperationError> {
    Report::new(OperationError::LockWaiterReleased).attach(format!(
        "resource={}, owner={}, claim_no={:?}, mode={mode}",
        token.resource, token.owner, token.claim_no
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
    use super::wait::tests::{linked_ids as linked_waiter_ids, queue_snapshot};
    use super::*;
    use smol::Timer;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::time::Duration;

    /// Debug snapshot of all granted locks and queued waiters.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct LockDebugSnapshot {
        /// Granted and waiting lock entries.
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
    }

    /// One granted lock or queued waiter in a debug snapshot.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct LockDebugEntry {
        /// Resource for this entry.
        pub(crate) resource: LockResource,
        /// Requested or granted mode.
        pub(crate) mode: LockMode,
        /// Owner for this entry.
        pub(crate) owner: LockOwner,
        /// Exact manager claim number.
        pub(crate) claim_no: ClaimNo,
        /// Whether the entry is granted or waiting.
        pub(crate) state: LockDebugEntryState,
        /// FIFO queue order for waiters; `None` for granted locks.
        pub(crate) queue_order: Option<usize>,
        /// Resource-local waiter slot for queued, provisional, or released state.
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
        /// An exact grant is installed but its waiter has not observed it.
        Provisional,
        /// Migration cleanup released the request before its observer resumed.
        Released,
    }

    /// Captures the current lock table for tests.
    #[inline]
    pub(crate) fn debug_snapshot(manager: &LockManager) -> LockDebugSnapshot {
        let mut resources: Vec<_> = manager
            .resources
            .iter()
            .map(|resource_state| *resource_state.key())
            .collect();
        resources.sort_unstable();
        let mut entries = Vec::new();
        let mut resource_diagnostics = Vec::new();
        for resource in resources {
            if let Some(resource_state) = manager.resources.get(&resource) {
                entries.extend(snapshot_entries(resource_state.value(), resource));
                let queue = queue_snapshot(&resource_state.wait_queue);
                resource_diagnostics.push(LockDebugResource {
                    resource,
                    waiter_slots: queue.slab.slots_len,
                    waiter_capacity: queue.slab.capacity,
                    live_waiters: queue.slab.live_count,
                    free_slots: queue.slab.free_order,
                    generations: queue.slab.generations,
                });
            }
        }
        LockDebugSnapshot {
            entries,
            resources: resource_diagnostics,
        }
    }

    /// Attempts to acquire a lock without waiting.
    #[inline]
    pub(crate) fn try_acquire(
        manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> OperationResult<bool> {
        mode.assert_valid_for(resource);
        let pending = PendingClaimToken {
            resource,
            owner,
            claim_no: ClaimNo::new(0),
        };
        if manager.raw_existing_acquire(&pending, mode)? {
            return Ok(true);
        }
        let (granted, empty) = {
            let mut resource_state = manager.resources.entry(resource).or_default();
            let family_covered = resource_state.validate_family_coverage(resource, mode, owner)?;
            let granted = resource_state.compatible_with_granted(resource, mode, owner)
                && (family_covered || resource_state.wait_queue.is_linked_empty());
            if granted {
                resource_state.granted.push(GrantedLock {
                    owner,
                    claim_no: pending.claim_no,
                    mode,
                    provisional_node: None,
                });
            }
            (granted, resource_state.is_empty())
        };
        if empty {
            manager.remove_if_empty(resource);
        }
        Ok(granted)
    }

    #[inline]
    fn snapshot_entries(
        resource_state: &ResourceState,
        resource: LockResource,
    ) -> Vec<LockDebugEntry> {
        let queue_ids = linked_waiter_ids(&resource_state.wait_queue);
        let mut entries = Vec::with_capacity(
            resource_state.granted.len() + resource_state.wait_queue.live_count(),
        );
        entries.extend(resource_state.granted.iter().map(|granted| LockDebugEntry {
            resource,
            mode: granted.mode,
            owner: granted.owner,
            claim_no: granted.claim_no,
            state: if granted.provisional_node.is_some() {
                LockDebugEntryState::Provisional
            } else {
                LockDebugEntryState::Granted
            },
            queue_order: None,
            wait_slot: granted.provisional_node.map(|id| id.slot),
            wait_generation: granted.provisional_node.map(|id| id.generation),
        }));
        entries.extend(
            resource_state
                .wait_queue
                .occupied_ids()
                .into_iter()
                .filter_map(|node_id| {
                    let waiter = resource_state.wait_queue.node(node_id);
                    let (state, queue_order) = match waiter.phase {
                        WaitNodePhase::Queued { .. } => (
                            LockDebugEntryState::Waiting,
                            queue_ids.iter().position(|&id| id == node_id),
                        ),
                        WaitNodePhase::Provisional => return None,
                        WaitNodePhase::Released => (LockDebugEntryState::Released, None),
                    };
                    Some(LockDebugEntry {
                        resource,
                        mode: waiter.target_mode,
                        owner: waiter.owner,
                        claim_no: waiter.claim_no,
                        state,
                        queue_order,
                        wait_slot: Some(node_id.slot),
                        wait_generation: Some(node_id.generation),
                    })
                }),
        );
        entries
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

    fn stmt(trx_id: TrxID, stmt_no: StmtNo) -> LockOwner {
        trx(trx_id).statement(stmt_no)
    }

    fn session(id: SessionID) -> LockOwner {
        LockOwner::session_explicit(id)
    }

    fn assert_operation_err<T>(res: OperationResult<T>, expected: OperationError) {
        let err = res.err().unwrap();
        assert_eq!(*err.current_context(), expected);
    }

    #[test]
    fn lock_owner_identity_carries_family_and_exact_scope() {
        let session_id = SessionID::new(7);
        let trx_id = TrxID::new(11);
        let explicit = LockOwner::session_explicit(session_id);
        let trx_owner = LockOwner::transaction(session_id, trx_id);
        let stmt_owner = trx_owner.statement(3);
        let ddl_owner =
            LockOwner::operation(SessionOperationKey::new(session_id, OperationID::new(5)));
        let maintenance_owner =
            LockOwner::operation(SessionOperationKey::new(session_id, OperationID::new(6)));

        for owner in [
            explicit,
            trx_owner,
            stmt_owner,
            ddl_owner,
            maintenance_owner,
        ] {
            assert_eq!(owner.family(), LockFamily::new(session_id));
        }
        assert_ne!(explicit, trx_owner);
        assert_ne!(trx_owner, stmt_owner);
        assert_ne!(ddl_owner, maintenance_owner);
        assert_ne!(trx_owner, LockOwner::transaction(SessionID::new(8), trx_id));
        assert_eq!(stmt_owner.scope(), LockScope::Statement(trx_id, 3));

        assert_eq!(explicit.to_string(), "session_explicit(session_id=7)");
        assert_eq!(trx_owner.to_string(), "transaction(session_id=7,trx_id=11)");
        assert_eq!(
            stmt_owner.to_string(),
            "statement(session_id=7,trx_id=11,stmt_no=3)"
        );
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
    #[should_panic(expected = "statement lock owner requires a transaction source")]
    fn statement_owner_requires_transaction_source() {
        let _ = LockOwner::session_explicit(SessionID::new(7)).statement(1);
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

    async fn wait_for_waiters(manager: &LockManager, resource: LockResource, expected: usize) {
        for _ in 0..100 {
            let snapshot = debug_snapshot(manager);
            if count_entries(&snapshot, resource, LockDebugEntryState::Waiting) == expected {
                return;
            }
            Timer::after(Duration::from_millis(1)).await;
        }
        panic!("waiter count did not reach {expected}");
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
        let manager = LockManager::new();
        let resource = table_data(TableID::new(7));
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentShared,
                trx(TrxID::new(1))
            )
            .unwrap()
        );
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentExclusive,
                trx(TrxID::new(2))
            )
            .unwrap()
        );
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentShared,
                trx(TrxID::new(3))
            )
            .unwrap()
        );
        let snapshot = debug_snapshot(&manager);
        assert_eq!(
            count_entries(&snapshot, resource, LockDebugEntryState::Granted),
            3
        );
    }

    #[test]
    fn newer_compatible_request_waits_behind_older_incompatible_waiter() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(TableID::new(9));
            assert!(try_acquire(&manager, resource, LockMode::Shared, trx(TrxID::new(1))).unwrap());

            let waiter_manager = Arc::clone(&manager);
            let waiter = smol::spawn(async move {
                waiter_manager
                    .acquire(resource, LockMode::Exclusive, trx(TrxID::new(2)))
                    .await
            });
            wait_for_waiters(&manager, resource, 1).await;

            assert!(
                !try_acquire(&manager, resource, LockMode::Shared, trx(TrxID::new(3))).unwrap()
            );
            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                1
            );
            assert_eq!(manager.release(resource, trx(TrxID::new(1))), 1);
            waiter.await.unwrap();
        });
    }

    #[test]
    fn release_grants_next_compatible_fifo_group() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_data(TableID::new(11));
            assert!(
                try_acquire(&manager, resource, LockMode::Exclusive, trx(TrxID::new(1))).unwrap()
            );

            let waiter_s = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::Shared, trx(TrxID::new(2)))
                        .await
                })
            };
            let waiter_is = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::IntentShared, trx(TrxID::new(3)))
                        .await
                })
            };
            let waiter_ix = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::IntentExclusive, trx(TrxID::new(4)))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 3).await;

            assert_eq!(manager.release(resource, trx(TrxID::new(1))), 1);
            waiter_s.await.unwrap();
            waiter_is.await.unwrap();

            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                snapshot
                    .entries
                    .iter()
                    .filter(|entry| {
                        entry.resource == resource
                            && entry.state == LockDebugEntryState::Waiting
                            && entry.owner == trx(TrxID::new(4))
                    })
                    .count(),
                1
            );
            assert_eq!(manager.release(resource, trx(TrxID::new(2))), 1);
            assert_eq!(manager.release(resource, trx(TrxID::new(3))), 1);
            waiter_ix.await.unwrap();
        });
    }

    #[test]
    fn release_one_resource_does_not_release_other_resources() {
        let manager = LockManager::new();
        let first = table_data(TableID::new(1));
        let second = table_data(TableID::new(2));
        assert!(
            try_acquire(
                &manager,
                first,
                LockMode::IntentExclusive,
                trx(TrxID::new(10))
            )
            .unwrap()
        );
        assert!(
            try_acquire(
                &manager,
                second,
                LockMode::IntentExclusive,
                trx(TrxID::new(10))
            )
            .unwrap()
        );

        assert_eq!(manager.release(first, trx(TrxID::new(10))), 1);
        let snapshot = debug_snapshot(&manager);
        assert_eq!(
            count_entries(&snapshot, first, LockDebugEntryState::Granted),
            0
        );
        assert_eq!(
            count_entries(&snapshot, second, LockDebugEntryState::Granted),
            1
        );
    }

    #[test]
    fn release_owner_removes_granted_locks_and_queued_waiters() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let first = table_data(TableID::new(1));
            let second = table_data(TableID::new(2));
            assert!(try_acquire(&manager, first, LockMode::Exclusive, trx(TrxID::new(1))).unwrap());
            assert!(try_acquire(&manager, second, LockMode::Shared, trx(TrxID::new(2))).unwrap());

            let waiting_owner = trx(TrxID::new(3));
            let waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(first, LockMode::IntentShared, waiting_owner)
                        .await
                })
            };
            wait_for_waiters(&manager, first, 1).await;

            assert_eq!(manager.release_owner(waiting_owner), 1);
            let err = waiter.await.unwrap_err();
            assert_eq!(*err.current_context(), OperationError::LockWaiterReleased);

            assert_eq!(manager.release_owner(trx(TrxID::new(2))), 1);
            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, second, LockDebugEntryState::Granted),
                0
            );
            assert_eq!(
                count_entries(&snapshot, first, LockDebugEntryState::Granted),
                1
            );
        });
    }

    #[test]
    fn statement_owner_cleanup_does_not_release_transaction_owner() {
        let manager = LockManager::new();
        let resource = table_data(TableID::new(3));
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentExclusive,
                trx(TrxID::new(20))
            )
            .unwrap()
        );
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentShared,
                stmt(TrxID::new(20), 1)
            )
            .unwrap()
        );

        assert_eq!(manager.release_owner(stmt(TrxID::new(20), 1)), 1);
        let snapshot = debug_snapshot(&manager);
        assert_eq!(
            snapshot
                .entries
                .iter()
                .filter(|entry| entry.owner == trx(TrxID::new(20))
                    && entry.state == LockDebugEntryState::Granted)
                .count(),
            1
        );
    }

    #[test]
    fn try_acquire_returns_false_for_fresh_blocking_request() {
        let manager = LockManager::new();
        let resource = table_data(TableID::new(4));
        assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(TrxID::new(1))).unwrap());
        assert!(
            !try_acquire(
                &manager,
                resource,
                LockMode::IntentShared,
                trx(TrxID::new(2))
            )
            .unwrap()
        );
    }

    #[test]
    fn same_owner_covered_requests_do_not_duplicate_entries() {
        let manager = LockManager::new();
        let resource = table_data(TableID::new(5));
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::Exclusive,
                session(SessionID::new(1))
            )
            .unwrap()
        );
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::Shared,
                session(SessionID::new(1))
            )
            .unwrap()
        );
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentExclusive,
                session(SessionID::new(1))
            )
            .unwrap()
        );
        let snapshot = debug_snapshot(&manager);
        assert_eq!(
            count_entries(&snapshot, resource, LockDebugEntryState::Granted),
            1
        );
        assert_eq!(snapshot.entries[0].mode, LockMode::Exclusive);
    }

    #[test]
    fn same_family_covered_request_grants_without_waiting() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_data(TableID::new(60));
            assert!(
                try_acquire(
                    &manager,
                    resource,
                    LockMode::Exclusive,
                    session(SessionID::new(1))
                )
                .unwrap()
            );

            let external_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::Shared, trx(TrxID::new(2)))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 1).await;

            let family_owner = LockOwner::transaction(SessionID::new(1), TrxID::new(3));
            assert!(
                try_acquire(&manager, resource, LockMode::IntentExclusive, family_owner,).unwrap()
            );

            let snapshot = debug_snapshot(&manager);
            assert!(snapshot.entries.iter().any(|entry| {
                entry.owner == family_owner
                    && entry.mode == LockMode::IntentExclusive
                    && entry.state == LockDebugEntryState::Granted
            }));
            assert!(snapshot.entries.iter().any(|entry| {
                entry.owner == trx(TrxID::new(2)) && entry.state == LockDebugEntryState::Waiting
            }));

            assert_eq!(manager.release(resource, family_owner), 1);
            assert_eq!(manager.release(resource, session(SessionID::new(1))), 1);
            external_waiter.await.unwrap();
        });
    }

    #[test]
    fn same_family_noncovered_request_errors_without_waiter() {
        let manager = LockManager::new();
        let resource = table_data(TableID::new(61));
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::Shared,
                session(SessionID::new(1))
            )
            .unwrap()
        );

        let family_owner = LockOwner::transaction(SessionID::new(1), TrxID::new(2));
        assert_operation_err(
            try_acquire(&manager, resource, LockMode::IntentExclusive, family_owner),
            OperationError::LockFamilyConflict,
        );

        let snapshot = debug_snapshot(&manager);
        assert_eq!(
            count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
            0
        );
        assert!(
            !snapshot
                .entries
                .iter()
                .any(|entry| entry.owner == family_owner)
        );
    }

    #[test]
    fn same_family_noncovered_request_does_not_queue_behind_same_family_waiter() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_data(TableID::new(62));
            assert!(
                try_acquire(&manager, resource, LockMode::Exclusive, trx(TrxID::new(99))).unwrap()
            );

            let session_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::Shared, session(SessionID::new(1)))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 1).await;

            let family_owner = LockOwner::transaction(SessionID::new(1), TrxID::new(2));
            assert_operation_err(
                try_acquire(&manager, resource, LockMode::IntentExclusive, family_owner),
                OperationError::LockFamilyConflict,
            );
            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                1
            );
            assert!(
                !snapshot
                    .entries
                    .iter()
                    .any(|entry| entry.owner == family_owner)
            );

            assert_eq!(manager.release(resource, trx(TrxID::new(99))), 1);
            session_waiter.await.unwrap();
        });
    }

    #[test]
    fn immediate_conversion_succeeds_only_when_it_will_not_wait() {
        let manager = LockManager::new();
        let resource = table_data(TableID::new(6));
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentShared,
                trx(TrxID::new(1))
            )
            .unwrap()
        );
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentExclusive,
                trx(TrxID::new(1))
            )
            .unwrap()
        );
        let snapshot = debug_snapshot(&manager);
        assert_eq!(snapshot.entries[0].mode, LockMode::IntentExclusive);

        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentShared,
                trx(TrxID::new(2))
            )
            .unwrap()
        );
        assert_operation_err(
            try_acquire(&manager, resource, LockMode::Exclusive, trx(TrxID::new(1))),
            OperationError::LockUpgradeWouldBlock,
        );
    }

    #[test]
    fn incomparable_same_owner_conversion_is_explicit_error() {
        let manager = LockManager::new();
        let resource = table_data(TableID::new(8));
        assert!(
            try_acquire(
                &manager,
                resource,
                LockMode::IntentExclusive,
                trx(TrxID::new(1))
            )
            .unwrap()
        );
        assert_operation_err(
            try_acquire(&manager, resource, LockMode::Shared, trx(TrxID::new(1))),
            OperationError::LockConversionNotSupported,
        );
    }

    #[test]
    fn async_acquire_waits_behind_conflict_and_completes_after_release() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(TableID::new(70));
            assert!(
                try_acquire(&manager, resource, LockMode::Exclusive, trx(TrxID::new(1))).unwrap()
            );

            let waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::Shared, trx(TrxID::new(2)))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 1).await;
            assert_eq!(manager.release(resource, trx(TrxID::new(1))), 1);
            waiter.await.unwrap();

            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                snapshot
                    .entries
                    .iter()
                    .filter(|entry| entry.owner == trx(TrxID::new(2))
                        && entry.state == LockDebugEntryState::Granted)
                    .count(),
                1
            );
        });
    }

    #[test]
    fn duplicate_pending_owner_is_detected_without_sharing_an_observer() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(TableID::new(71));
            let owner = trx(TrxID::new(2));
            assert!(
                try_acquire(&manager, resource, LockMode::Exclusive, trx(TrxID::new(1))).unwrap()
            );

            let first_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move { manager.acquire(resource, LockMode::Shared, owner).await })
            };
            wait_for_waiters(&manager, resource, 1).await;

            let panic = catch_unwind(AssertUnwindSafe(|| {
                let _ = try_acquire(&manager, resource, LockMode::Shared, owner);
            }));
            assert!(panic.is_err());
            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                1
            );
            assert!(first_waiter.cancel().await.is_none());
        });
    }

    #[test]
    fn cancelled_acquire_removes_queued_waiter() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(TableID::new(72));
            assert!(
                try_acquire(&manager, resource, LockMode::Exclusive, trx(TrxID::new(1))).unwrap()
            );

            let waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::Shared, trx(TrxID::new(2)))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 1).await;
            assert!(waiter.cancel().await.is_none());
            wait_for_waiters(&manager, resource, 0).await;

            assert_eq!(manager.release(resource, trx(TrxID::new(1))), 1);
            let snapshot = debug_snapshot(&manager);
            assert!(
                !snapshot
                    .entries
                    .iter()
                    .any(|entry| entry.owner == trx(TrxID::new(2)))
            );
        });
    }

    #[test]
    fn cancelling_front_waiter_grants_later_compatible_waiter() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(TableID::new(51));
            assert!(try_acquire(&manager, resource, LockMode::Shared, trx(TrxID::new(1))).unwrap());

            let front_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::Exclusive, trx(TrxID::new(2)))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 1).await;

            let compatible_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::Shared, trx(TrxID::new(3)))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 2).await;

            assert!(front_waiter.cancel().await.is_none());
            wait_for_waiters(&manager, resource, 0).await;
            compatible_waiter.await.unwrap();

            let snapshot = debug_snapshot(&manager);
            assert!(snapshot.entries.iter().any(|entry| {
                entry.owner == trx(TrxID::new(3)) && entry.state == LockDebugEntryState::Granted
            }));
        });
    }

    #[test]
    fn pending_guard_removes_a_promoted_but_unobserved_grant() {
        let manager = LockManager::new();
        let resource = table_metadata(TableID::new(52));
        let blocker = trx(TrxID::new(1));
        assert!(try_acquire(&manager, resource, LockMode::Exclusive, blocker).unwrap());
        let token = PendingClaimToken {
            resource,
            owner: trx(TrxID::new(2)),
            claim_no: ClaimNo::new(7),
        };
        let mut guard = RawPendingGuard::new(&manager, token, LockMode::Shared);
        guard.start().unwrap();
        assert_eq!(manager.release(resource, blocker), 1);
        assert_eq!(
            count_entries(
                &debug_snapshot(&manager),
                resource,
                LockDebugEntryState::Provisional
            ),
            1
        );

        drop(guard);

        let snapshot = debug_snapshot(&manager);
        assert!(
            !snapshot
                .entries
                .iter()
                .any(|entry| entry.owner == trx(TrxID::new(2)))
        );
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

        let notifications = resource_state.grant_waiters(resource);

        assert_eq!(notifications.len(), 2);
        assert_eq!(resource_state.granted.len(), 2);
        assert_eq!(
            resource_state.wait_queue.node(first).phase,
            WaitNodePhase::Provisional
        );
        assert_eq!(
            resource_state.wait_queue.node(second).phase,
            WaitNodePhase::Provisional
        );
        assert_eq!(resource_state.granted[0].provisional_node, Some(first));
        assert_eq!(resource_state.granted[1].provisional_node, Some(second));
    }

    #[test]
    fn immediate_fresh_grant_allocates_no_waiter_storage() {
        let manager = LockManager::new();
        let resource = table_metadata(TableID::new(55));
        let owner = trx(TrxID::new(55));

        assert!(try_acquire(&manager, resource, LockMode::Shared, owner).unwrap());

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
            .find(|entry| entry.owner == owner)
            .unwrap();
        assert_eq!(grant.claim_no, ClaimNo::new(0));
        assert_eq!(grant.state, LockDebugEntryState::Granted);
    }

    #[test]
    fn external_provisional_release_retains_node_until_observation() {
        smol::block_on(async {
            let manager = LockManager::new();
            let resource = table_metadata(TableID::new(56));
            let blocker = trx(TrxID::new(56));
            let waiting_owner = trx(TrxID::new(57));
            assert!(try_acquire(&manager, resource, LockMode::Exclusive, blocker).unwrap());
            let token = PendingClaimToken {
                resource,
                owner: waiting_owner,
                claim_no: ClaimNo::new(9),
            };
            let mut guard = RawPendingGuard::new(&manager, token, LockMode::Shared);
            guard.start().unwrap();

            assert_eq!(manager.release(resource, blocker), 1);
            assert_eq!(
                count_entries(
                    &debug_snapshot(&manager),
                    resource,
                    LockDebugEntryState::Provisional
                ),
                1
            );
            assert_eq!(manager.release_owner(waiting_owner), 1);
            let released = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&released, resource, LockDebugEntryState::Released),
                1
            );
            assert_eq!(released.resources[0].live_waiters, 1);
            assert!(manager.resources.get(&resource).is_some());

            let err = guard.wait_and_observe().await.unwrap_err();
            assert_eq!(*err.current_context(), OperationError::LockWaiterReleased);
            drop(guard);
            assert!(manager.resources.get(&resource).is_none());
        });
    }

    #[test]
    fn debug_snapshot_reports_granted_waiting_and_queue_order() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(TableID::new(42));
            assert!(try_acquire(&manager, resource, LockMode::Shared, trx(TrxID::new(1))).unwrap());
            let first_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::Exclusive, trx(TrxID::new(2)))
                        .await
                })
            };
            let second_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::Exclusive, trx(TrxID::new(3)))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 2).await;

            let snapshot = debug_snapshot(&manager);
            assert!(snapshot.entries.iter().any(|entry| {
                entry.resource == resource
                    && entry.owner == trx(TrxID::new(1))
                    && entry.mode == LockMode::Shared
                    && entry.state == LockDebugEntryState::Granted
                    && entry.queue_order.is_none()
            }));
            assert!(snapshot.entries.iter().any(|entry| {
                entry.resource == resource
                    && entry.owner == trx(TrxID::new(2))
                    && entry.mode == LockMode::Exclusive
                    && entry.state == LockDebugEntryState::Waiting
                    && entry.queue_order == Some(0)
            }));
            assert!(snapshot.entries.iter().any(|entry| {
                entry.resource == resource
                    && entry.owner == trx(TrxID::new(3))
                    && entry.mode == LockMode::Exclusive
                    && entry.state == LockDebugEntryState::Waiting
                    && entry.queue_order == Some(1)
            }));

            assert_eq!(manager.release_owner(trx(TrxID::new(2))), 1);
            assert_eq!(manager.release_owner(trx(TrxID::new(3))), 1);
            let released = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&released, resource, LockDebugEntryState::Released),
                2
            );
            assert_eq!(released.resources[0].live_waiters, 2);
            assert_eq!(
                *first_waiter.await.unwrap_err().current_context(),
                OperationError::LockWaiterReleased
            );
            assert_eq!(
                *second_waiter.await.unwrap_err().current_context(),
                OperationError::LockWaiterReleased
            );
        });
    }
}
