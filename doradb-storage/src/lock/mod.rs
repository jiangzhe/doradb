//! Logical metadata and table-data lock manager primitives.
//!
//! This module is the standalone core for RFC-0016 logical locks. It tracks
//! catalog, table metadata, and table data resources independently from the
//! engine/session/transaction lifecycle wiring that later phases will add.

use crate::catalog::TableID;
use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::error::{OperationError, Result};
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use crate::session::SessionID;
use crate::trx::TrxID;
use dashmap::DashMap;
use error_stack::Report;
use event_listener::Event;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

/// Statement number for statement-owned logical locks.
pub(crate) type StmtNo = u64;

/// Logical resource protected by the lock manager.
///
/// Lock acquisition follows the resource order below to avoid deadlocks across
/// multi-resource operations:
///
/// ```text
/// CatalogNamespace
///   -> TableMetadata(table_id ascending)
///   -> TableData(table_id ascending)
///   -> row undo/CDB ownership
/// ```
///
/// The derived [`Ord`] implementation encodes the represented portion of this
/// order. Row ownership is outside this logical lock manager and must be
/// acquired only after the relevant `TableData` lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum LockResource {
    /// Catalog-wide table identity or namespace invariants.
    CatalogNamespace,
    /// Table definition and metadata for one table.
    TableMetadata(TableID),
    /// Multi-granularity table-data root above row ownership.
    TableData(TableID),
}

/// Logical lock mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
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
    /// Validates that this mode can be used for `resource`.
    #[inline]
    pub(crate) fn validate_for(self, resource: LockResource) -> Result<()> {
        validate_mode(resource, self)
    }

    /// Returns whether this mode covers a request for `requested` on `resource`.
    ///
    /// Coverage is used for reentrant acquisitions and immediate conversion
    /// decisions. `TableData(S)` and `TableData(IX)` are intentionally
    /// incomparable because the first phase does not introduce `SIX`.
    #[inline]
    pub(crate) fn covers(self, resource: LockResource, requested: Self) -> Result<bool> {
        validate_mode(resource, self)?;
        validate_mode(resource, requested)?;
        Ok(mode_covers(resource, self, requested))
    }

    /// Validates that this mode is allowed for explicit table-lock APIs.
    #[inline]
    pub(crate) fn validate_explicit_table_lock(self) -> Result<()> {
        if matches!(self, LockMode::Shared | LockMode::Exclusive) {
            return Ok(());
        }
        Err(Report::new(OperationError::InvalidLockMode)
            .attach(format!("explicit table lock mode={self:?}"))
            .into())
    }
}

/// Logical lock owner independent from Rust object lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum LockOwner {
    /// Lock held for a session lifetime.
    Session(SessionID),
    /// Lock held for a transaction lifetime.
    Transaction(TrxID),
    /// Lock held for one statement inside a transaction.
    Statement(TrxID, StmtNo),
}

/// Logical owner group for locks created by the same client session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum LockOwnerGroup {
    /// Owners associated with one engine-local session.
    Session(SessionID),
}

/// Whether an acquisition created a new granted lock entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockGrant {
    /// The acquire call created a fresh granted entry.
    Fresh,
    /// The requested mode was already represented by this owner or waiter.
    Existing,
}

impl LockGrant {
    #[inline]
    fn is_fresh(self) -> bool {
        self == LockGrant::Fresh
    }
}

/// Releases a freshly acquired lock unless the caller completes and disarms it.
pub(crate) struct FreshLockGuard<'a> {
    lock_manager: &'a LockManager,
    resource: LockResource,
    owner: LockOwner,
    active: bool,
}

impl<'a> FreshLockGuard<'a> {
    #[inline]
    pub(crate) fn new(
        lock_manager: &'a LockManager,
        resource: LockResource,
        owner: LockOwner,
        grant: LockGrant,
    ) -> Option<Self> {
        grant.is_fresh().then(|| FreshLockGuard {
            lock_manager,
            resource,
            owner,
            active: true,
        })
    }

    #[inline]
    pub(crate) fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for FreshLockGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.active {
            self.lock_manager.release(self.resource, self.owner);
        }
    }
}

/// Standalone logical lock manager.
pub(crate) struct LockManager {
    resources: Arc<DashMap<LockResource, ResourceState>>,
}

impl LockManager {
    /// Creates an empty lock manager.
    #[inline]
    pub(crate) fn new() -> Self {
        LockManager {
            resources: Arc::new(DashMap::new()),
        }
    }

    /// Acquires a lock, waiting until a fresh conflicting request can be granted.
    ///
    /// Blocking conversion is not supported. If the same owner already holds an
    /// incomparable or non-immediate weaker mode, this method returns the same
    /// explicit operation error as the non-blocking acquisition path.
    #[inline]
    pub(crate) async fn acquire(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> Result<()> {
        self.acquire_with_group(resource, mode, owner, None)
            .await
            .map(|_| ())
    }

    /// Acquires a lock and reports whether this call created a grant.
    #[inline]
    pub(crate) async fn acquire_with_grant(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> Result<LockGrant> {
        self.acquire_with_group(resource, mode, owner, None).await
    }

    /// Acquires a lock for an owner inside a session owner group.
    #[inline]
    pub(crate) async fn acquire_grouped(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        owner_group: LockOwnerGroup,
    ) -> Result<()> {
        self.acquire_with_group(resource, mode, owner, Some(owner_group))
            .await
            .map(|_| ())
    }

    /// Acquires a grouped lock and reports whether this call created a grant.
    #[inline]
    pub(crate) async fn acquire_grouped_with_grant(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        owner_group: LockOwnerGroup,
    ) -> Result<LockGrant> {
        self.acquire_with_group(resource, mode, owner, Some(owner_group))
            .await
    }

    #[inline]
    async fn acquire_with_group(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        owner_group: Option<LockOwnerGroup>,
    ) -> Result<LockGrant> {
        validate_mode(resource, mode)?;
        let (waiter, waiter_guard, grant) = {
            // Reuse the non-blocking path first. If the request must wait,
            // enqueue the waiter while still holding the resource guard so a
            // concurrent release cannot miss this request.
            let mut resource_state = self.resources.entry(resource).or_default();
            match resource_state.try_acquire_immediate(resource, mode, owner, owner_group)? {
                AcquireImmediate::Granted(grant) => return Ok(grant),
                AcquireImmediate::WouldWait => {
                    let waiter = Arc::new(Waiter::new(owner, owner_group, mode));
                    resource_state.waiters.push_back(Arc::clone(&waiter));
                    // Keep the queued request cancellation-safe after the resource
                    // guard is released and before the grant is observed.
                    let waiter_guard =
                        WaiterGuard::new(&self.resources, resource, Arc::clone(&waiter));
                    (waiter, waiter_guard, LockGrant::Fresh)
                }
                AcquireImmediate::AlreadyWaiting(waiter) => {
                    let waiter_guard =
                        WaiterGuard::new(&self.resources, resource, Arc::clone(&waiter));
                    (waiter, waiter_guard, LockGrant::Existing)
                }
            }
        };
        // The resource guard is dropped before awaiting; grant notification
        // and cleanup paths can keep mutating the same resource while this task
        // is parked.
        self.wait_for_grant_with_guard(resource, mode, owner, waiter, waiter_guard, grant)
            .await
    }

    #[inline]
    async fn wait_for_grant_with_guard(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        waiter: Arc<Waiter>,
        mut waiter_guard: WaiterGuard,
        grant: LockGrant,
    ) -> Result<LockGrant> {
        let res = self.wait_for_grant(resource, mode, owner, waiter).await;
        if res.is_ok() {
            waiter_guard.disarm_after_grant_observed();
        }
        res.map(|_| grant)
    }

    /// Releases locks and waiters for one owner/resource pair.
    ///
    /// This method is also the cancellation path for lifecycle cleanup: a
    /// session, transaction, statement, rollback, or admin cleanup thread may
    /// call it while the original task is still blocked in [`Self::acquire`].
    /// In that case, any queued waiter for `owner` on `resource` is removed and
    /// the blocked acquisition wakes with `LockWaiterReleased`.
    ///
    /// The return value is the number of granted locks and queued requests
    /// removed. Waiters removed by this call wake with `LockWaiterReleased`.
    #[inline]
    pub(crate) fn release(&self, resource: LockResource, owner: LockOwner) -> usize {
        let mut notify = Vec::new();
        let mut removed = 0;
        let remove_resource = {
            if let Some(mut resource_state) = self.resources.get_mut(&resource) {
                removed += resource_state.remove_granted(owner);
                let released_waiters = resource_state.remove_waiters(owner);
                removed += released_waiters.len();
                mark_waiters(&released_waiters, WaitOutcome::Released);
                notify.extend(released_waiters);
                notify.extend(resource_state.grant_waiters(resource));
                resource_state.is_empty()
            } else {
                false
            }
        };
        if remove_resource {
            self.resources
                .remove_if(&resource, |_resource, resource_state| {
                    resource_state.is_empty()
                });
        }
        notify_waiters(notify);
        removed
    }

    /// Releases `owner` on `resource` and fails every queued waiter.
    ///
    /// This is for resource invalidation, not ordinary unlock. A successful
    /// `DROP TABLE` uses it so waiters queued behind the drop do not acquire
    /// locks for a table that has just left the runtime catalog.
    #[inline]
    pub(crate) fn release_and_fail_waiters(
        &self,
        resource: LockResource,
        owner: LockOwner,
        error: OperationError,
    ) -> usize {
        let mut notify = Vec::new();
        let mut removed = 0;
        let remove_resource = {
            if let Some(mut resource_state) = self.resources.get_mut(&resource) {
                removed += resource_state.remove_granted(owner);
                let failed_waiters = resource_state.drain_waiters();
                removed += failed_waiters.len();
                mark_waiters(&failed_waiters, WaitOutcome::Failed(error));
                notify.extend(failed_waiters);
                resource_state.is_empty()
            } else {
                false
            }
        };
        if remove_resource {
            self.resources
                .remove_if(&resource, |_resource, resource_state| {
                    resource_state.is_empty()
                });
        }
        notify_waiters(notify);
        removed
    }

    /// Releases every granted lock and queued request owned by `owner`.
    ///
    /// This is the authoritative cleanup path for later statement, transaction,
    /// session, rollback, and fatal cleanup integration.
    #[inline]
    pub(crate) fn release_owner(&self, owner: LockOwner) -> usize {
        let mut notify = Vec::new();
        let mut removed = 0;
        let mut resources: Vec<_> = self
            .resources
            .iter()
            .map(|resource_state| *resource_state.key())
            .collect();
        resources.sort_unstable();
        for resource in resources {
            let remove_resource = {
                if let Some(mut resource_state) = self.resources.get_mut(&resource) {
                    let removed_granted = resource_state.remove_granted(owner);
                    let released_waiters = resource_state.remove_waiters(owner);
                    let removed_waiters = released_waiters.len();
                    mark_waiters(&released_waiters, WaitOutcome::Released);
                    notify.extend(released_waiters);
                    notify.extend(resource_state.grant_waiters(resource));
                    removed += removed_granted + removed_waiters;
                    resource_state.is_empty()
                } else {
                    false
                }
            };
            if remove_resource {
                self.resources
                    .remove_if(&resource, |_resource, resource_state| {
                        resource_state.is_empty()
                    });
            }
        }
        notify_waiters(notify);
        removed
    }

    #[inline]
    async fn wait_for_grant(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        waiter: Arc<Waiter>,
    ) -> Result<()> {
        loop {
            // Register the listener before reading the outcome to avoid losing
            // a notification that races with this waiter going back to sleep.
            let listener = waiter.event.listen();
            match waiter.outcome() {
                WaitOutcome::Waiting => listener.await,
                WaitOutcome::Granted => {
                    // Owner cleanup can race with a granted waiter resuming.
                    // Confirm the lock is still held before reporting success.
                    if self.owner_holds(resource, owner, mode) {
                        return Ok(());
                    }
                    return Err(waiter_released_err(resource, mode, owner));
                }
                WaitOutcome::Released => return Err(waiter_released_err(resource, mode, owner)),
                WaitOutcome::Failed(error) => {
                    return Err(waiter_failed_err(resource, mode, owner, error));
                }
            }
        }
    }

    /// Returns whether `owner` currently holds a mode covering `requested`.
    #[inline]
    pub(crate) fn owner_holds(
        &self,
        resource: LockResource,
        owner: LockOwner,
        requested: LockMode,
    ) -> bool {
        self.resources
            .get(&resource)
            .and_then(|resource_state| resource_state.granted_mode(owner))
            .is_some_and(|held| mode_covers(resource, held, requested))
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

    const NAME: &'static str = "lock_manager";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        registry.register::<Self>(Self::new())
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {}
}

#[derive(Default)]
struct ResourceState {
    granted: Vec<GrantedLock>,
    waiters: VecDeque<Arc<Waiter>>,
}

impl ResourceState {
    #[inline]
    fn try_acquire_immediate(
        &mut self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        owner_group: Option<LockOwnerGroup>,
    ) -> Result<AcquireImmediate> {
        if let Some(idx) = self.granted_idx(owner) {
            // Reentrant requests that are already covered do not create
            // duplicate granted entries.
            let held = self.granted[idx].mode;
            if mode_covers(resource, held, mode) {
                return Ok(AcquireImmediate::Granted(LockGrant::Existing));
            }
            // Conversions are immediate-only in this phase. Incomparable modes
            // are rejected rather than synthesized into a combined mode such as
            // SIX, which RFC-0016 deliberately excludes from v1.
            if !mode_covers(resource, mode, held) {
                return Err(conversion_not_supported_err(resource, held, mode, owner));
            }
            // A stronger same-owner mode may replace the existing grant only
            // when it does not conflict with current holders and does not jump
            // ahead of any queued request.
            self.validate_owner_group_coverage(resource, mode, owner, owner_group)?;
            if !self.waiters.is_empty()
                || !self.compatible_with_granted(resource, mode, owner, owner_group)
            {
                return Err(upgrade_would_block_err(resource, held, mode, owner));
            }
            self.granted[idx].mode = mode;
            return Ok(AcquireImmediate::Granted(LockGrant::Existing));
        }
        if let Some(waiter) = self.waiter_by_owner(owner) {
            let waiting = waiter.mode;
            if mode_covers(resource, waiting, mode) {
                return Ok(AcquireImmediate::AlreadyWaiting(waiter));
            }
            if !mode_covers(resource, mode, waiting) {
                return Err(conversion_not_supported_err(resource, waiting, mode, owner));
            }
            return Err(upgrade_would_block_err(resource, waiting, mode, owner));
        }
        let owner_group_covered =
            self.validate_owner_group_coverage(resource, mode, owner, owner_group)?;
        // Fresh compatible requests still wait behind an existing queue so
        // readers or intent holders cannot starve an older incompatible waiter,
        // unless an already-granted same-session lock covers this request.
        if self.compatible_with_granted(resource, mode, owner, owner_group)
            && (owner_group_covered || self.waiters.is_empty())
        {
            self.granted.push(GrantedLock {
                owner,
                owner_group,
                mode,
            });
            return Ok(AcquireImmediate::Granted(LockGrant::Fresh));
        }
        Ok(AcquireImmediate::WouldWait)
    }

    #[inline]
    fn granted_idx(&self, owner: LockOwner) -> Option<usize> {
        self.granted
            .iter()
            .position(|granted| granted.owner == owner)
    }

    #[inline]
    fn granted_mode(&self, owner: LockOwner) -> Option<LockMode> {
        self.granted
            .iter()
            .find(|granted| granted.owner == owner)
            .map(|granted| granted.mode)
    }

    #[inline]
    fn waiter_by_owner(&self, owner: LockOwner) -> Option<Arc<Waiter>> {
        self.waiters
            .iter()
            .find(|waiter| waiter.owner == owner)
            .cloned()
    }

    #[inline]
    fn compatible_with_granted(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        owner_group: Option<LockOwnerGroup>,
    ) -> bool {
        self.granted.iter().all(|granted| {
            if granted.owner == owner {
                return true;
            }
            if owner_group.is_some() && granted.owner_group == owner_group {
                return mode_covers(resource, granted.mode, mode);
            }
            modes_are_compatible(resource, granted.mode, mode)
        })
    }

    #[inline]
    fn validate_owner_group_coverage(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        owner_group: Option<LockOwnerGroup>,
    ) -> Result<bool> {
        let Some(owner_group) = owner_group else {
            return Ok(false);
        };
        let mut covered = false;
        for granted in self.granted.iter() {
            if granted.owner == owner || granted.owner_group != Some(owner_group) {
                continue;
            }
            if !mode_covers(resource, granted.mode, mode) {
                return Err(owner_group_conflict_err(
                    resource,
                    granted.mode,
                    mode,
                    owner,
                    owner_group,
                    granted.owner,
                ));
            }
            covered = true;
        }
        for waiter in self.waiters.iter() {
            if waiter.owner == owner || waiter.owner_group != Some(owner_group) {
                continue;
            }
            if !mode_covers(resource, waiter.mode, mode) {
                return Err(owner_group_conflict_err(
                    resource,
                    waiter.mode,
                    mode,
                    owner,
                    owner_group,
                    waiter.owner,
                ));
            }
        }
        Ok(covered)
    }

    #[inline]
    fn remove_granted(&mut self, owner: LockOwner) -> usize {
        let before = self.granted.len();
        self.granted.retain(|granted| granted.owner != owner);
        before - self.granted.len()
    }

    #[inline]
    fn remove_waiters(&mut self, owner: LockOwner) -> Vec<Arc<Waiter>> {
        let mut retained = VecDeque::with_capacity(self.waiters.len());
        let mut removed = Vec::new();
        while let Some(waiter) = self.waiters.pop_front() {
            if waiter.owner == owner {
                removed.push(waiter);
            } else {
                retained.push_back(waiter);
            }
        }
        self.waiters = retained;
        removed
    }

    #[inline]
    fn drain_waiters(&mut self) -> Vec<Arc<Waiter>> {
        self.waiters.drain(..).collect()
    }

    #[inline]
    fn remove_waiter(&mut self, target: &Arc<Waiter>) -> Option<Arc<Waiter>> {
        let mut retained = VecDeque::with_capacity(self.waiters.len());
        let mut removed = None;
        while let Some(waiter) = self.waiters.pop_front() {
            if removed.is_none() && Arc::ptr_eq(&waiter, target) {
                removed = Some(waiter);
            } else {
                retained.push_back(waiter);
            }
        }
        self.waiters = retained;
        removed
    }

    #[inline]
    fn grant_waiters(&mut self, resource: LockResource) -> Vec<Arc<Waiter>> {
        let mut granted_waiters = Vec::new();
        while let Some((mode, owner, owner_group)) = self
            .waiters
            .front()
            .map(|waiter| (waiter.mode, waiter.owner, waiter.owner_group))
        {
            if !self.compatible_with_granted(resource, mode, owner, owner_group) {
                break;
            }
            let Some(waiter) = self.waiters.pop_front() else {
                break;
            };
            if let Some(idx) = self.granted_idx(waiter.owner) {
                let held = self.granted[idx].mode;
                if !mode_covers(resource, held, waiter.mode) {
                    if !mode_covers(resource, waiter.mode, held) {
                        self.waiters.push_front(waiter);
                        break;
                    }
                    self.granted[idx].mode = waiter.mode;
                }
            } else {
                self.granted.push(GrantedLock {
                    owner: waiter.owner,
                    owner_group: waiter.owner_group,
                    mode: waiter.mode,
                });
            }
            waiter.set_outcome(WaitOutcome::Granted);
            granted_waiters.push(waiter);
        }
        granted_waiters
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.granted.is_empty() && self.waiters.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrantedLock {
    owner: LockOwner,
    owner_group: Option<LockOwnerGroup>,
    mode: LockMode,
}

/// Cancellation guard for a queued acquisition.
///
/// If the acquire future is dropped while waiting, the guard removes the exact
/// queued waiter. If cancellation races with promotion, the guard removes the
/// unobserved grant before it can leak.
struct WaiterGuard {
    resources: Weak<DashMap<LockResource, ResourceState>>,
    resource: LockResource,
    waiter: Arc<Waiter>,
    active: bool,
}

impl WaiterGuard {
    #[inline]
    fn new(
        resources: &Arc<DashMap<LockResource, ResourceState>>,
        resource: LockResource,
        waiter: Arc<Waiter>,
    ) -> Self {
        waiter.add_guard();
        WaiterGuard {
            resources: Arc::downgrade(resources),
            resource,
            waiter,
            active: true,
        }
    }

    #[inline]
    fn disarm_after_grant_observed(&mut self) {
        if !self.active {
            return;
        }
        self.waiter.mark_grant_observed();
        self.waiter.remove_guard();
        self.active = false;
    }
}

impl Drop for WaiterGuard {
    #[inline]
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        if !self.waiter.remove_guard() || self.waiter.grant_observed() {
            return;
        }
        let Some(resources) = self.resources.upgrade() else {
            return;
        };
        let mut notify = Vec::new();
        let remove_resource = {
            if let Some(mut resource_state) = resources.get_mut(&self.resource) {
                if let Some(waiter) = resource_state.remove_waiter(&self.waiter) {
                    waiter.set_outcome(WaitOutcome::Released);
                    notify.extend(resource_state.grant_waiters(self.resource));
                } else if self.waiter.outcome() == WaitOutcome::Granted {
                    let removed = resource_state.remove_granted(self.waiter.owner);
                    if removed > 0 {
                        self.waiter.set_outcome(WaitOutcome::Released);
                        notify.extend(resource_state.grant_waiters(self.resource));
                    }
                }
                resource_state.is_empty()
            } else {
                false
            }
        };
        if remove_resource {
            resources.remove_if(&self.resource, |_resource, resource_state| {
                resource_state.is_empty()
            });
        }
        notify_waiters(notify);
    }
}

struct Waiter {
    owner: LockOwner,
    owner_group: Option<LockOwnerGroup>,
    mode: LockMode,
    outcome: Mutex<WaitOutcome>,
    event: Event,
    active_guards: AtomicUsize,
    grant_observed: AtomicBool,
}

impl Waiter {
    #[inline]
    fn new(owner: LockOwner, owner_group: Option<LockOwnerGroup>, mode: LockMode) -> Self {
        Waiter {
            owner,
            owner_group,
            mode,
            outcome: Mutex::new(WaitOutcome::Waiting),
            event: Event::new(),
            active_guards: AtomicUsize::new(0),
            grant_observed: AtomicBool::new(false),
        }
    }

    #[inline]
    fn outcome(&self) -> WaitOutcome {
        *self.outcome.lock()
    }

    #[inline]
    fn set_outcome(&self, outcome: WaitOutcome) {
        *self.outcome.lock() = outcome;
    }

    #[inline]
    fn add_guard(&self) {
        self.active_guards.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn remove_guard(&self) -> bool {
        self.active_guards.fetch_sub(1, Ordering::AcqRel) == 1
    }

    #[inline]
    fn mark_grant_observed(&self) {
        self.grant_observed.store(true, Ordering::Release);
    }

    #[inline]
    fn grant_observed(&self) -> bool {
        self.grant_observed.load(Ordering::Acquire)
    }

    #[inline]
    #[cfg(test)]
    fn active_guard_count(&self) -> usize {
        self.active_guards.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitOutcome {
    Waiting,
    Granted,
    Released,
    Failed(OperationError),
}

enum AcquireImmediate {
    Granted(LockGrant),
    WouldWait,
    AlreadyWaiting(Arc<Waiter>),
}

#[inline]
fn notify_waiters(waiters: Vec<Arc<Waiter>>) {
    for waiter in waiters {
        waiter.event.notify(usize::MAX);
    }
}

#[inline]
fn mark_waiters(waiters: &[Arc<Waiter>], outcome: WaitOutcome) {
    for waiter in waiters {
        waiter.set_outcome(outcome);
    }
}

#[inline]
fn validate_mode(resource: LockResource, mode: LockMode) -> Result<()> {
    if mode_is_valid(resource, mode) {
        return Ok(());
    }
    Err(Report::new(OperationError::InvalidLockMode)
        .attach(format!("resource={resource:?}, mode={mode:?}"))
        .into())
}

#[inline]
fn mode_is_valid(resource: LockResource, mode: LockMode) -> bool {
    match resource {
        LockResource::CatalogNamespace | LockResource::TableMetadata(_) => {
            matches!(mode, LockMode::Shared | LockMode::Exclusive)
        }
        LockResource::TableData(_) => true,
    }
}

/// Returns whether two modes can be granted together on the same resource.
///
/// Compatibility is symmetric and is checked only after both modes have been
/// validated for the resource. Catalog and table-metadata resources use the
/// ordinary shared/exclusive matrix:
///
/// ```text
/// CatalogNamespace and TableMetadata
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
        LockResource::CatalogNamespace | LockResource::TableMetadata(_) => {
            matches!((left, right), (LockMode::Shared, LockMode::Shared))
        }
        LockResource::TableData(_) => matches!(
            (left, right),
            (LockMode::IntentShared, LockMode::IntentShared)
                | (LockMode::IntentShared, LockMode::IntentExclusive)
                | (LockMode::IntentShared, LockMode::Shared)
                | (LockMode::IntentExclusive, LockMode::IntentShared)
                | (LockMode::IntentExclusive, LockMode::IntentExclusive)
                | (LockMode::Shared, LockMode::IntentShared)
                | (LockMode::Shared, LockMode::Shared)
        ),
    }
}

/// Returns whether `held` is strong enough to satisfy `requested`.
///
/// Coverage is directional: a mode can cover another mode even when the reverse
/// is not true. The lock manager uses this for reentrant acquisitions and
/// immediate same-owner conversions. Catalog and table-metadata resources use
/// the ordinary hierarchy where `X` covers every valid request and `S` covers
/// only `S`.
///
/// ```text
/// CatalogNamespace and TableMetadata
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
        LockResource::CatalogNamespace | LockResource::TableMetadata(_) => {
            held == LockMode::Exclusive || held == requested
        }
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
) -> crate::error::Error {
    Report::new(OperationError::LockUpgradeWouldBlock)
        .attach(format!(
            "resource={resource:?}, owner={owner:?}, held={held:?}, requested={requested:?}"
        ))
        .into()
}

#[inline]
fn conversion_not_supported_err(
    resource: LockResource,
    held: LockMode,
    requested: LockMode,
    owner: LockOwner,
) -> crate::error::Error {
    Report::new(OperationError::LockConversionNotSupported)
        .attach(format!(
            "resource={resource:?}, owner={owner:?}, held={held:?}, requested={requested:?}"
        ))
        .into()
}

#[inline]
fn owner_group_conflict_err(
    resource: LockResource,
    held: LockMode,
    requested: LockMode,
    owner: LockOwner,
    owner_group: LockOwnerGroup,
    held_owner: LockOwner,
) -> crate::error::Error {
    Report::new(OperationError::LockOwnerGroupConflict)
        .attach(format!(
            "resource={resource:?}, owner={owner:?}, owner_group={owner_group:?}, \
             held_owner={held_owner:?}, held={held:?}, requested={requested:?}"
        ))
        .into()
}

#[inline]
fn waiter_released_err(
    resource: LockResource,
    mode: LockMode,
    owner: LockOwner,
) -> crate::error::Error {
    Report::new(OperationError::LockWaiterReleased)
        .attach(format!(
            "resource={resource:?}, owner={owner:?}, mode={mode:?}"
        ))
        .into()
}

#[inline]
fn waiter_failed_err(
    resource: LockResource,
    mode: LockMode,
    owner: LockOwner,
    error: OperationError,
) -> crate::error::Error {
    Report::new(error)
        .attach(format!(
            "resource={resource:?}, owner={owner:?}, mode={mode:?}"
        ))
        .into()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::time::Duration;

    /// Debug snapshot of all granted locks and queued waiters.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct LockDebugSnapshot {
        /// Granted and waiting lock entries.
        pub(crate) entries: Vec<LockDebugEntry>,
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
        /// Owner group for this entry, when grouped acquisition was used.
        pub(crate) owner_group: Option<LockOwnerGroup>,
        /// Whether the entry is granted or waiting.
        pub(crate) state: LockDebugEntryState,
        /// FIFO queue order for waiters; `None` for granted locks.
        pub(crate) queue_order: Option<usize>,
    }

    /// Granted-or-waiting state for a debug snapshot entry.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum LockDebugEntryState {
        /// Lock is currently granted.
        Granted,
        /// Lock is waiting in the resource queue.
        Waiting,
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
        for resource in resources {
            if let Some(resource_state) = manager.resources.get(&resource) {
                entries.extend(snapshot_entries(resource_state.value(), resource));
            }
        }
        LockDebugSnapshot { entries }
    }

    /// Attempts to acquire a lock without waiting.
    #[inline]
    pub(crate) fn try_acquire(
        manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> Result<bool> {
        try_acquire_with_group(manager, resource, mode, owner, None)
    }

    /// Attempts to acquire a lock for an owner inside a session owner group.
    #[inline]
    pub(crate) fn try_acquire_grouped(
        manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        owner_group: LockOwnerGroup,
    ) -> Result<bool> {
        try_acquire_with_group(manager, resource, mode, owner, Some(owner_group))
    }

    #[inline]
    fn try_acquire_with_group(
        manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        owner_group: Option<LockOwnerGroup>,
    ) -> Result<bool> {
        validate_mode(resource, mode)?;
        let mut resource_state = manager.resources.entry(resource).or_default();
        match resource_state.try_acquire_immediate(resource, mode, owner, owner_group)? {
            AcquireImmediate::Granted(_) => Ok(true),
            AcquireImmediate::WouldWait | AcquireImmediate::AlreadyWaiting(_) => Ok(false),
        }
    }

    #[inline]
    fn snapshot_entries(
        resource_state: &ResourceState,
        resource: LockResource,
    ) -> Vec<LockDebugEntry> {
        let mut entries =
            Vec::with_capacity(resource_state.granted.len() + resource_state.waiters.len());
        entries.extend(resource_state.granted.iter().map(|granted| LockDebugEntry {
            resource,
            mode: granted.mode,
            owner: granted.owner,
            owner_group: granted.owner_group,
            state: LockDebugEntryState::Granted,
            queue_order: None,
        }));
        entries.extend(
            resource_state
                .waiters
                .iter()
                .enumerate()
                .map(|(queue_order, waiter)| LockDebugEntry {
                    resource,
                    mode: waiter.mode,
                    owner: waiter.owner,
                    owner_group: waiter.owner_group,
                    state: LockDebugEntryState::Waiting,
                    queue_order: Some(queue_order),
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
        LockOwner::Transaction(id)
    }

    fn stmt(trx_id: TrxID, stmt_no: StmtNo) -> LockOwner {
        LockOwner::Statement(trx_id, stmt_no)
    }

    fn session(id: SessionID) -> LockOwner {
        LockOwner::Session(id)
    }

    fn group(id: SessionID) -> LockOwnerGroup {
        LockOwnerGroup::Session(id)
    }

    fn assert_operation_err<T>(res: Result<T>, expected: OperationError) {
        let err = res.err().unwrap();
        assert_eq!(err.operation_error(), Some(expected));
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
            smol::Timer::after(Duration::from_millis(1)).await;
        }
        panic!("waiter count did not reach {expected}");
    }

    async fn wait_for_owner_guard_count(
        manager: &LockManager,
        resource: LockResource,
        owner: LockOwner,
        expected: usize,
    ) {
        for _ in 0..100 {
            let actual = manager
                .resources
                .get(&resource)
                .and_then(|resource_state| resource_state.waiter_by_owner(owner))
                .map_or(0, |waiter| waiter.active_guard_count());
            if actual == expected {
                return;
            }
            smol::Timer::after(Duration::from_millis(1)).await;
        }
        panic!("waiter guard count did not reach {expected}");
    }

    #[test]
    fn table_data_compatibility_matrix_matches_rfc() {
        let resource = table_data(1);
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
    fn metadata_and_catalog_only_accept_shared_and_exclusive() {
        for resource in [LockResource::CatalogNamespace, table_metadata(1)] {
            assert!(LockMode::Shared.validate_for(resource).is_ok());
            assert!(LockMode::Exclusive.validate_for(resource).is_ok());
            assert_operation_err(
                LockMode::IntentShared.validate_for(resource),
                OperationError::InvalidLockMode,
            );
            assert_operation_err(
                LockMode::IntentExclusive.validate_for(resource),
                OperationError::InvalidLockMode,
            );
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
    }

    #[test]
    fn multiple_compatible_holders_grant_together() {
        let manager = LockManager::new();
        let resource = table_data(7);
        assert!(try_acquire(&manager, resource, LockMode::IntentShared, trx(1)).unwrap());
        assert!(try_acquire(&manager, resource, LockMode::IntentExclusive, trx(2)).unwrap());
        assert!(try_acquire(&manager, resource, LockMode::IntentShared, trx(3)).unwrap());
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
            let resource = table_metadata(9);
            assert!(try_acquire(&manager, resource, LockMode::Shared, trx(1)).unwrap());

            let waiter_manager = Arc::clone(&manager);
            let waiter = smol::spawn(async move {
                waiter_manager
                    .acquire(resource, LockMode::Exclusive, trx(2))
                    .await
            });
            wait_for_waiters(&manager, resource, 1).await;

            assert!(!try_acquire(&manager, resource, LockMode::Shared, trx(3)).unwrap());
            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                1
            );
            assert_eq!(manager.release(resource, trx(1)), 1);
            waiter.await.unwrap();
        });
    }

    #[test]
    fn release_grants_next_compatible_fifo_group() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_data(11);
            assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(1)).unwrap());

            let waiter_s = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Shared, trx(2)).await },
                )
            };
            let waiter_is = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::IntentShared, trx(3))
                        .await
                })
            };
            let waiter_ix = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire(resource, LockMode::IntentExclusive, trx(4))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 3).await;

            assert_eq!(manager.release(resource, trx(1)), 1);
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
                            && entry.owner == trx(4)
                    })
                    .count(),
                1
            );
            assert_eq!(manager.release(resource, trx(2)), 1);
            assert_eq!(manager.release(resource, trx(3)), 1);
            waiter_ix.await.unwrap();
        });
    }

    #[test]
    fn release_one_resource_does_not_release_other_resources() {
        let manager = LockManager::new();
        let first = table_data(1);
        let second = table_data(2);
        assert!(try_acquire(&manager, first, LockMode::IntentExclusive, trx(10)).unwrap());
        assert!(try_acquire(&manager, second, LockMode::IntentExclusive, trx(10)).unwrap());

        assert_eq!(manager.release(first, trx(10)), 1);
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
    fn release_and_fail_waiters_does_not_grant_queue() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(41);
            assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(1)).unwrap());

            let first_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Shared, trx(2)).await },
                )
            };
            let second_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Shared, trx(3)).await },
                )
            };
            wait_for_waiters(&manager, resource, 2).await;

            assert_eq!(
                manager.release_and_fail_waiters(resource, trx(1), OperationError::TableNotFound,),
                3
            );
            for waiter in [first_waiter, second_waiter] {
                let err = waiter.await.unwrap_err();
                assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            }
            assert_eq!(
                count_entries(
                    &debug_snapshot(&manager),
                    resource,
                    LockDebugEntryState::Granted,
                ),
                0
            );
            assert_eq!(
                count_entries(
                    &debug_snapshot(&manager),
                    resource,
                    LockDebugEntryState::Waiting,
                ),
                0
            );
        });
    }

    #[test]
    fn release_owner_removes_granted_locks_and_queued_waiters() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let first = table_data(1);
            let second = table_data(2);
            assert!(try_acquire(&manager, first, LockMode::Exclusive, trx(1)).unwrap());
            assert!(try_acquire(&manager, second, LockMode::Shared, trx(2)).unwrap());

            let waiting_owner = trx(3);
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
            assert_eq!(
                err.operation_error(),
                Some(OperationError::LockWaiterReleased)
            );

            assert_eq!(manager.release_owner(trx(2)), 1);
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
        let resource = table_data(3);
        assert!(try_acquire(&manager, resource, LockMode::IntentExclusive, trx(20)).unwrap());
        assert!(try_acquire(&manager, resource, LockMode::IntentShared, stmt(20, 1)).unwrap());

        assert_eq!(manager.release_owner(stmt(20, 1)), 1);
        let snapshot = debug_snapshot(&manager);
        assert_eq!(
            snapshot
                .entries
                .iter()
                .filter(
                    |entry| entry.owner == trx(20) && entry.state == LockDebugEntryState::Granted
                )
                .count(),
            1
        );
    }

    #[test]
    fn try_acquire_returns_false_for_fresh_blocking_request() {
        let manager = LockManager::new();
        let resource = table_data(4);
        assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(1)).unwrap());
        assert!(!try_acquire(&manager, resource, LockMode::IntentShared, trx(2)).unwrap());
    }

    #[test]
    fn same_owner_covered_requests_do_not_duplicate_entries() {
        let manager = LockManager::new();
        let resource = table_data(5);
        assert!(try_acquire(&manager, resource, LockMode::Exclusive, session(1)).unwrap());
        assert!(try_acquire(&manager, resource, LockMode::Shared, session(1)).unwrap());
        assert!(try_acquire(&manager, resource, LockMode::IntentExclusive, session(1)).unwrap());
        let snapshot = debug_snapshot(&manager);
        assert_eq!(
            count_entries(&snapshot, resource, LockDebugEntryState::Granted),
            1
        );
        assert_eq!(snapshot.entries[0].mode, LockMode::Exclusive);
    }

    #[test]
    fn same_group_covered_request_grants_without_waiting() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_data(60);
            assert!(
                try_acquire_grouped(
                    &manager,
                    resource,
                    LockMode::Exclusive,
                    session(1),
                    group(1)
                )
                .unwrap()
            );

            let external_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Shared, trx(2)).await },
                )
            };
            wait_for_waiters(&manager, resource, 1).await;

            assert!(
                try_acquire_grouped(
                    &manager,
                    resource,
                    LockMode::IntentExclusive,
                    trx(3),
                    group(1),
                )
                .unwrap()
            );

            let snapshot = debug_snapshot(&manager);
            assert!(snapshot.entries.iter().any(|entry| {
                entry.owner == trx(3)
                    && entry.owner_group == Some(group(1))
                    && entry.mode == LockMode::IntentExclusive
                    && entry.state == LockDebugEntryState::Granted
            }));
            assert!(snapshot.entries.iter().any(|entry| {
                entry.owner == trx(2) && entry.state == LockDebugEntryState::Waiting
            }));

            assert_eq!(manager.release(resource, trx(3)), 1);
            assert_eq!(manager.release(resource, session(1)), 1);
            external_waiter.await.unwrap();
        });
    }

    #[test]
    fn same_group_noncovered_request_errors_without_waiter() {
        let manager = LockManager::new();
        let resource = table_data(61);
        assert!(
            try_acquire_grouped(&manager, resource, LockMode::Shared, session(1), group(1))
                .unwrap()
        );

        assert_operation_err(
            try_acquire_grouped(
                &manager,
                resource,
                LockMode::IntentExclusive,
                trx(2),
                group(1),
            ),
            OperationError::LockOwnerGroupConflict,
        );

        let snapshot = debug_snapshot(&manager);
        assert_eq!(
            count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
            0
        );
        assert!(!snapshot.entries.iter().any(|entry| entry.owner == trx(2)));
    }

    #[test]
    fn same_group_noncovered_request_does_not_queue_behind_same_group_waiter() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_data(62);
            assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(99)).unwrap());

            let session_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move {
                    manager
                        .acquire_grouped(resource, LockMode::Shared, session(1), group(1))
                        .await
                })
            };
            wait_for_waiters(&manager, resource, 1).await;

            assert_operation_err(
                try_acquire_grouped(
                    &manager,
                    resource,
                    LockMode::IntentExclusive,
                    trx(2),
                    group(1),
                ),
                OperationError::LockOwnerGroupConflict,
            );
            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                1
            );
            assert!(!snapshot.entries.iter().any(|entry| entry.owner == trx(2)));

            assert_eq!(manager.release(resource, trx(99)), 1);
            session_waiter.await.unwrap();
        });
    }

    #[test]
    fn immediate_conversion_succeeds_only_when_it_will_not_wait() {
        let manager = LockManager::new();
        let resource = table_data(6);
        assert!(try_acquire(&manager, resource, LockMode::IntentShared, trx(1)).unwrap());
        assert!(try_acquire(&manager, resource, LockMode::IntentExclusive, trx(1)).unwrap());
        let snapshot = debug_snapshot(&manager);
        assert_eq!(snapshot.entries[0].mode, LockMode::IntentExclusive);

        assert!(try_acquire(&manager, resource, LockMode::IntentShared, trx(2)).unwrap());
        assert_operation_err(
            try_acquire(&manager, resource, LockMode::Exclusive, trx(1)),
            OperationError::LockUpgradeWouldBlock,
        );
    }

    #[test]
    fn incomparable_same_owner_conversion_is_explicit_error() {
        let manager = LockManager::new();
        let resource = table_data(8);
        assert!(try_acquire(&manager, resource, LockMode::IntentExclusive, trx(1)).unwrap());
        assert_operation_err(
            try_acquire(&manager, resource, LockMode::Shared, trx(1)),
            OperationError::LockConversionNotSupported,
        );
    }

    #[test]
    fn async_acquire_waits_behind_conflict_and_completes_after_release() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = LockResource::CatalogNamespace;
            assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(1)).unwrap());

            let waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Shared, trx(2)).await },
                )
            };
            wait_for_waiters(&manager, resource, 1).await;
            assert_eq!(manager.release(resource, trx(1)), 1);
            waiter.await.unwrap();

            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                snapshot
                    .entries
                    .iter()
                    .filter(|entry| entry.owner == trx(2)
                        && entry.state == LockDebugEntryState::Granted)
                    .count(),
                1
            );
        });
    }

    #[test]
    fn duplicate_async_acquire_reuses_existing_waiter() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = LockResource::CatalogNamespace;
            let owner = trx(2);
            assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(1)).unwrap());

            let first_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move { manager.acquire(resource, LockMode::Shared, owner).await })
            };
            wait_for_waiters(&manager, resource, 1).await;

            let second_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move { manager.acquire(resource, LockMode::Shared, owner).await })
            };
            wait_for_owner_guard_count(&manager, resource, owner, 2).await;

            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                1
            );
            assert_eq!(manager.release(resource, trx(1)), 1);
            first_waiter.await.unwrap();
            second_waiter.await.unwrap();

            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                snapshot
                    .entries
                    .iter()
                    .filter(|entry| {
                        entry.owner == owner && entry.state == LockDebugEntryState::Granted
                    })
                    .count(),
                1
            );
        });
    }

    #[test]
    fn try_acquire_returns_false_for_existing_same_owner_waiter() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(50);
            let owner = trx(2);
            assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(1)).unwrap());

            let waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move { manager.acquire(resource, LockMode::Shared, owner).await })
            };
            wait_for_waiters(&manager, resource, 1).await;

            assert!(!try_acquire(&manager, resource, LockMode::Shared, owner).unwrap());
            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                1
            );

            assert!(waiter.cancel().await.is_none());
        });
    }

    #[test]
    fn cancelled_acquire_removes_queued_waiter() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = LockResource::CatalogNamespace;
            assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(1)).unwrap());

            let waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Shared, trx(2)).await },
                )
            };
            wait_for_waiters(&manager, resource, 1).await;
            assert!(waiter.cancel().await.is_none());
            wait_for_waiters(&manager, resource, 0).await;

            assert_eq!(manager.release(resource, trx(1)), 1);
            let snapshot = debug_snapshot(&manager);
            assert!(!snapshot.entries.iter().any(|entry| entry.owner == trx(2)));
        });
    }

    #[test]
    fn cancelling_duplicate_waiter_keeps_shared_waiter_queued() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(53);
            let owner = trx(2);
            assert!(try_acquire(&manager, resource, LockMode::Exclusive, trx(1)).unwrap());

            let first_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move { manager.acquire(resource, LockMode::Shared, owner).await })
            };
            wait_for_waiters(&manager, resource, 1).await;

            let duplicate_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(async move { manager.acquire(resource, LockMode::Shared, owner).await })
            };
            wait_for_owner_guard_count(&manager, resource, owner, 2).await;

            assert!(duplicate_waiter.cancel().await.is_none());
            wait_for_owner_guard_count(&manager, resource, owner, 1).await;
            let snapshot = debug_snapshot(&manager);
            assert_eq!(
                count_entries(&snapshot, resource, LockDebugEntryState::Waiting),
                1
            );

            assert_eq!(manager.release(resource, trx(1)), 1);
            first_waiter.await.unwrap();
            let snapshot = debug_snapshot(&manager);
            assert!(snapshot.entries.iter().any(|entry| {
                entry.owner == owner && entry.state == LockDebugEntryState::Granted
            }));
        });
    }

    #[test]
    fn cancelling_front_waiter_grants_later_compatible_waiter() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(51);
            assert!(try_acquire(&manager, resource, LockMode::Shared, trx(1)).unwrap());

            let front_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Exclusive, trx(2)).await },
                )
            };
            wait_for_waiters(&manager, resource, 1).await;

            let compatible_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Shared, trx(3)).await },
                )
            };
            wait_for_waiters(&manager, resource, 2).await;

            assert!(front_waiter.cancel().await.is_none());
            wait_for_waiters(&manager, resource, 0).await;
            compatible_waiter.await.unwrap();

            let snapshot = debug_snapshot(&manager);
            assert!(snapshot.entries.iter().any(|entry| {
                entry.owner == trx(3) && entry.state == LockDebugEntryState::Granted
            }));
        });
    }

    #[test]
    fn active_waiter_guard_removes_unobserved_grant() {
        let manager = LockManager::new();
        let resource = table_metadata(52);
        let waiter = Arc::new(Waiter::new(trx(2), None, LockMode::Shared));
        {
            let mut resource_state = manager.resources.entry(resource).or_default();
            resource_state.waiters.push_back(Arc::clone(&waiter));
        }
        let waiter_guard = WaiterGuard::new(&manager.resources, resource, Arc::clone(&waiter));
        {
            let mut resource_state = manager.resources.get_mut(&resource).unwrap();
            assert_eq!(resource_state.grant_waiters(resource).len(), 1);
        }

        drop(waiter_guard);

        let snapshot = debug_snapshot(&manager);
        assert!(!snapshot.entries.iter().any(|entry| entry.owner == trx(2)));
    }

    #[test]
    fn grant_waiters_deduplicates_existing_owner_grants() {
        let resource = table_data(54);
        let mut resource_state = ResourceState::default();
        resource_state.granted.push(GrantedLock {
            owner: trx(2),
            owner_group: None,
            mode: LockMode::IntentShared,
        });
        let covered_waiter = Arc::new(Waiter::new(trx(2), None, LockMode::IntentShared));
        let stronger_waiter = Arc::new(Waiter::new(trx(2), None, LockMode::IntentExclusive));
        resource_state
            .waiters
            .push_back(Arc::clone(&covered_waiter));
        resource_state
            .waiters
            .push_back(Arc::clone(&stronger_waiter));

        let granted_waiters = resource_state.grant_waiters(resource);

        assert_eq!(granted_waiters.len(), 2);
        assert_eq!(covered_waiter.outcome(), WaitOutcome::Granted);
        assert_eq!(stronger_waiter.outcome(), WaitOutcome::Granted);
        assert_eq!(resource_state.granted.len(), 1);
        assert_eq!(resource_state.granted[0].mode, LockMode::IntentExclusive);
    }

    #[test]
    fn debug_snapshot_reports_granted_waiting_and_queue_order() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(42);
            assert!(try_acquire(&manager, resource, LockMode::Shared, trx(1)).unwrap());
            let first_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Exclusive, trx(2)).await },
                )
            };
            let second_waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Exclusive, trx(3)).await },
                )
            };
            wait_for_waiters(&manager, resource, 2).await;

            let snapshot = debug_snapshot(&manager);
            assert!(snapshot.entries.iter().any(|entry| {
                entry.resource == resource
                    && entry.owner == trx(1)
                    && entry.mode == LockMode::Shared
                    && entry.state == LockDebugEntryState::Granted
                    && entry.queue_order.is_none()
            }));
            assert!(snapshot.entries.iter().any(|entry| {
                entry.resource == resource
                    && entry.owner == trx(2)
                    && entry.mode == LockMode::Exclusive
                    && entry.state == LockDebugEntryState::Waiting
                    && entry.queue_order == Some(0)
            }));
            assert!(snapshot.entries.iter().any(|entry| {
                entry.resource == resource
                    && entry.owner == trx(3)
                    && entry.mode == LockMode::Exclusive
                    && entry.state == LockDebugEntryState::Waiting
                    && entry.queue_order == Some(1)
            }));

            assert_eq!(manager.release_owner(trx(2)), 1);
            assert_eq!(manager.release_owner(trx(3)), 1);
            assert_eq!(
                first_waiter.await.unwrap_err().operation_error(),
                Some(OperationError::LockWaiterReleased)
            );
            assert_eq!(
                second_waiter.await.unwrap_err().operation_error(),
                Some(OperationError::LockWaiterReleased)
            );
        });
    }
}
