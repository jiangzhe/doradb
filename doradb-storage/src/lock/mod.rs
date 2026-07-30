//! Logical metadata and table-data lock manager primitives.
//!
//! This module is the standalone core for RFC-0016 logical locks. It tracks
//! table metadata and table data resources independently from the
//! engine/session/transaction lifecycle wiring that later phases will add.

mod state;

use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::error::{OperationError, OperationResult};
use crate::id::{OperationID, SessionID, SessionOperationKey, TableID, TrxID};
use crate::map::FastDashMap;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use error_stack::Report;
use event_listener::Event;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::fmt;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

pub(crate) use state::OwnerLockState;

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
    /// Creates a guard that releases a fresh grant on drop.
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

    /// Marks the guarded fresh lock as externally owned.
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

/// Scoped table DDL lock guard for metadata and data resources.
pub(crate) struct ScopedTableDdlLocks<'a> {
    lock_manager: &'a LockManager,
    table_id: TableID,
    owner: LockOwner,
    metadata_fresh: bool,
    data_fresh: bool,
}

impl Drop for ScopedTableDdlLocks<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.data_fresh {
            self.lock_manager
                .release(LockResource::TableData(self.table_id), self.owner);
        }
        if self.metadata_fresh {
            self.lock_manager
                .release(LockResource::TableMetadata(self.table_id), self.owner);
        }
    }
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
    ) -> OperationResult<()> {
        self.acquire_inner(resource, mode, owner).await.map(|_| ())
    }

    /// Acquires a lock and reports whether this call created a grant.
    #[inline]
    pub(crate) async fn acquire_with_grant(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> OperationResult<LockGrant> {
        self.acquire_inner(resource, mode, owner).await
    }

    /// Acquires the ordered metadata/data locks for one table operation.
    #[inline]
    pub(crate) async fn acquire_table_locks<'a>(
        &'a self,
        table_id: TableID,
        data_mode: LockMode,
        owner: LockOwner,
    ) -> OperationResult<(Option<FreshLockGuard<'a>>, Option<FreshLockGuard<'a>>)> {
        let metadata_resource = LockResource::TableMetadata(table_id);
        let metadata_grant = self
            .acquire_with_grant(metadata_resource, LockMode::Shared, owner)
            .await?;
        let metadata_guard = FreshLockGuard::new(self, metadata_resource, owner, metadata_grant);

        let data_resource = LockResource::TableData(table_id);
        let data_grant = self
            .acquire_with_grant(data_resource, data_mode, owner)
            .await?;
        let data_guard = FreshLockGuard::new(self, data_resource, owner, data_grant);

        Ok((metadata_guard, data_guard))
    }

    /// Acquires metadata-X for one freshly allocated CREATE TABLE id.
    #[inline]
    pub(crate) async fn acquire_create_table_metadata_lock<'a>(
        &'a self,
        table_id: TableID,
        owner: LockOwner,
    ) -> OperationResult<FreshLockGuard<'a>> {
        let resource = LockResource::TableMetadata(table_id);
        let grant = self
            .acquire_with_grant(resource, LockMode::Exclusive, owner)
            .await?;
        FreshLockGuard::new(self, resource, owner, grant).map_or_else(
            || {
                panic!(
                    "create-table metadata lock invariant violated: fresh table id reused an existing owner grant, table_id={table_id}, owner={owner:?}"
                )
            },
            Ok,
        )
    }

    /// Acquires scoped exclusive table DDL locks.
    #[inline]
    pub(crate) async fn acquire_table_ddl_locks<'a>(
        &'a self,
        table_id: TableID,
        owner: LockOwner,
    ) -> OperationResult<ScopedTableDdlLocks<'a>> {
        let metadata_resource = LockResource::TableMetadata(table_id);
        let metadata_grant = self
            .acquire_with_grant(metadata_resource, LockMode::Exclusive, owner)
            .await?;
        let mut metadata_guard =
            FreshLockGuard::new(self, metadata_resource, owner, metadata_grant);

        let data_resource = LockResource::TableData(table_id);
        let data_grant = self
            .acquire_with_grant(data_resource, LockMode::Exclusive, owner)
            .await?;
        if let Some(guard) = metadata_guard.as_mut() {
            guard.disarm();
        }

        Ok(ScopedTableDdlLocks {
            lock_manager: self,
            table_id,
            owner,
            metadata_fresh: metadata_grant == LockGrant::Fresh,
            data_fresh: data_grant == LockGrant::Fresh,
        })
    }

    /// Rejects table DDL when the session already holds explicit table locks.
    #[inline]
    pub(crate) fn reject_table_ddl_explicit_session_lock(
        &self,
        table_id: TableID,
        ddl_owner: LockOwner,
    ) -> OperationResult<()> {
        let explicit_owner = LockOwner::session_explicit(ddl_owner.family().session_id());
        for resource in [
            LockResource::TableMetadata(table_id),
            LockResource::TableData(table_id),
        ] {
            let held = self
                .resources
                .get(&resource)
                .and_then(|state| state.granted_mode(explicit_owner));
            if let Some(held) = held {
                return Err(lock_family_conflict_err(
                    resource,
                    held,
                    LockMode::Exclusive,
                    ddl_owner,
                    explicit_owner,
                )
                .attach(format!(
                    "table_id={table_id}, policy=reject_ddl_under_explicit_session_lock"
                )));
            }
        }
        Ok(())
    }

    #[inline]
    async fn acquire_inner(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> OperationResult<LockGrant> {
        mode.assert_valid_for(resource);
        let (waiter, waiter_guard, grant) = {
            // Reuse the non-blocking path first. If the request must wait,
            // enqueue the waiter while still holding the resource guard so a
            // concurrent release cannot miss this request.
            let mut resource_state = self.resources.entry(resource).or_default();
            match resource_state.try_acquire_immediate(resource, mode, owner)? {
                AcquireImmediate::Granted(grant) => return Ok(grant),
                AcquireImmediate::WouldWait => {
                    let waiter = Arc::new(Waiter::new(owner, mode));
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
    ) -> OperationResult<LockGrant> {
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
    ) -> OperationResult<()> {
        loop {
            // Register the listener before reading the outcome to avoid losing
            // a notification that races with this waiter going back to sleep.
            let listener = waiter.event.listen();
            match waiter.outcome() {
                WaitOutcome::Waiting => listener.await,
                // Owner cleanup can race with a granted waiter resuming.
                // Confirm the lock is still held before reporting success.
                WaitOutcome::Granted if self.owner_holds(resource, owner, mode) => return Ok(()),
                WaitOutcome::Granted | WaitOutcome::Released => {
                    return Err(Report::new(OperationError::LockWaiterReleased)
                        .attach(format!("resource={resource}, owner={owner}, mode={mode}")));
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
        requested.assert_valid_for(resource);
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
    ) -> OperationResult<AcquireImmediate> {
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
            self.validate_family_coverage(resource, mode, owner)?;
            if !self.waiters.is_empty() || !self.compatible_with_granted(resource, mode, owner) {
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
        let family_covered = self.validate_family_coverage(resource, mode, owner)?;
        // Fresh compatible requests still wait behind an existing queue so
        // readers or intent holders cannot starve an older incompatible waiter,
        // unless an already-granted same-family lock covers this request.
        if self.compatible_with_granted(resource, mode, owner)
            && (family_covered || self.waiters.is_empty())
        {
            self.granted.push(GrantedLock { owner, mode });
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
        for waiter in self.waiters.iter() {
            if waiter.owner == owner || waiter.owner.family() != owner.family() {
                continue;
            }
            if !mode_covers(resource, waiter.mode, mode) {
                return Err(lock_family_conflict_err(
                    resource,
                    waiter.mode,
                    mode,
                    owner,
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
        while let Some((mode, owner)) = self
            .waiters
            .front()
            .map(|waiter| (waiter.mode, waiter.owner))
        {
            if !self.compatible_with_granted(resource, mode, owner) {
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
    mode: LockMode,
}

/// Cancellation guard for a queued acquisition.
///
/// If the acquire future is dropped while waiting, the guard removes the exact
/// queued waiter. If cancellation races with promotion, the guard removes the
/// unobserved grant before it can leak.
struct WaiterGuard {
    resources: Weak<FastDashMap<LockResource, ResourceState>>,
    resource: LockResource,
    waiter: Arc<Waiter>,
    active: bool,
}

impl WaiterGuard {
    #[inline]
    fn new(
        resources: &Arc<FastDashMap<LockResource, ResourceState>>,
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
    mode: LockMode,
    outcome: Mutex<WaitOutcome>,
    event: Event,
    active_guards: AtomicUsize,
    grant_observed: AtomicBool,
}

impl Waiter {
    #[inline]
    fn new(owner: LockOwner, mode: LockMode) -> Self {
        Waiter {
            owner,
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
    use super::*;
    use smol::Timer;
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
    ) -> OperationResult<bool> {
        mode.assert_valid_for(resource);
        let mut resource_state = manager.resources.entry(resource).or_default();
        match resource_state.try_acquire_immediate(resource, mode, owner)? {
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

    #[test]
    fn create_table_metadata_guard_holds_only_fresh_metadata_x() {
        smol::block_on(async {
            let manager = LockManager::new();
            let table_id = TableID::new(42);
            let owner = LockOwner::operation(SessionOperationKey::new(
                SessionID::new(7),
                OperationID::new(1),
            ));
            let guard = manager
                .acquire_create_table_metadata_lock(table_id, owner)
                .await
                .unwrap();

            assert_eq!(
                debug_snapshot(&manager).entries,
                vec![LockDebugEntry {
                    resource: table_metadata(table_id),
                    mode: LockMode::Exclusive,
                    owner,
                    state: LockDebugEntryState::Granted,
                    queue_order: None,
                }]
            );

            drop(guard);
            assert!(debug_snapshot(&manager).entries.is_empty());
        });
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
            Timer::after(Duration::from_millis(1)).await;
        }
        panic!("waiter guard count did not reach {expected}");
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
    fn duplicate_async_acquire_reuses_existing_waiter() {
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
            assert_eq!(manager.release(resource, trx(TrxID::new(1))), 1);
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
            let resource = table_metadata(TableID::new(50));
            let owner = trx(TrxID::new(2));
            assert!(
                try_acquire(&manager, resource, LockMode::Exclusive, trx(TrxID::new(1))).unwrap()
            );

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
    fn cancelling_duplicate_waiter_keeps_shared_waiter_queued() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(TableID::new(53));
            let owner = trx(TrxID::new(2));
            assert!(
                try_acquire(&manager, resource, LockMode::Exclusive, trx(TrxID::new(1))).unwrap()
            );

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

            assert_eq!(manager.release(resource, trx(TrxID::new(1))), 1);
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
    fn active_waiter_guard_removes_unobserved_grant() {
        let manager = LockManager::new();
        let resource = table_metadata(TableID::new(52));
        let waiter = Arc::new(Waiter::new(trx(TrxID::new(2)), LockMode::Shared));
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
        assert!(
            !snapshot
                .entries
                .iter()
                .any(|entry| entry.owner == trx(TrxID::new(2)))
        );
    }

    #[test]
    fn grant_waiters_deduplicates_existing_owner_grants() {
        let resource = table_data(TableID::new(54));
        let mut resource_state = ResourceState::default();
        resource_state.granted.push(GrantedLock {
            owner: trx(TrxID::new(2)),
            mode: LockMode::IntentShared,
        });
        let covered_waiter = Arc::new(Waiter::new(trx(TrxID::new(2)), LockMode::IntentShared));
        let stronger_waiter = Arc::new(Waiter::new(trx(TrxID::new(2)), LockMode::IntentExclusive));
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
