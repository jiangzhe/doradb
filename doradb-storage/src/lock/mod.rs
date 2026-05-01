//! Logical metadata and table-data lock manager primitives.
//!
//! This module is the standalone core for RFC-0016 logical locks. It tracks
//! catalog, table metadata, and table data resources independently from the
//! engine/session/transaction lifecycle wiring that later phases will add.

use crate::catalog::TableID;
use crate::error::{OperationError, Result};
use crate::session::SessionID;
use crate::trx::TrxID;
use dashmap::DashMap;
use error_stack::Report;
use event_listener::Event;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

/// Statement sequence placeholder for statement-owned logical locks.
pub type StatementSeq = u64;

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
pub enum LockResource {
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
    /// Returns whether this mode is valid for `resource`.
    #[inline]
    pub fn is_valid_for(self, resource: LockResource) -> bool {
        mode_is_valid(resource, self)
    }

    /// Validates that this mode can be used for `resource`.
    #[inline]
    pub fn validate_for(self, resource: LockResource) -> Result<()> {
        validate_mode(resource, self)
    }

    /// Returns whether this mode is compatible with `other` on `resource`.
    #[inline]
    pub fn compatible_with(self, resource: LockResource, other: Self) -> Result<bool> {
        validate_mode(resource, self)?;
        validate_mode(resource, other)?;
        Ok(modes_are_compatible(resource, self, other))
    }

    /// Returns whether this mode covers a request for `requested` on `resource`.
    ///
    /// Coverage is used for reentrant acquisitions and immediate conversion
    /// decisions. `TableData(S)` and `TableData(IX)` are intentionally
    /// incomparable because the first phase does not introduce `SIX`.
    #[inline]
    pub fn covers(self, resource: LockResource, requested: Self) -> Result<bool> {
        validate_mode(resource, self)?;
        validate_mode(resource, requested)?;
        Ok(mode_covers(resource, self, requested))
    }
}

/// Logical lock owner independent from Rust object lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LockOwner {
    /// Lock held for a session lifetime.
    Session(SessionID),
    /// Lock held for a transaction lifetime.
    Transaction(TrxID),
    /// Lock held for one statement inside a transaction.
    Statement(TrxID, StatementSeq),
}

/// Intended lifetime category for a logical lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockLifetime {
    /// Statement-scoped lock lifetime.
    Statement,
    /// Transaction-scoped lock lifetime.
    Transaction,
    /// Session-scoped lock lifetime.
    Session,
}

/// Standalone logical lock manager.
pub struct LockManager {
    resources: DashMap<LockResource, ResourceState>,
}

impl LockManager {
    /// Creates an empty lock manager.
    #[inline]
    pub fn new() -> Self {
        LockManager {
            resources: DashMap::new(),
        }
    }

    /// Attempts to acquire a lock without waiting.
    ///
    /// Returns `Ok(true)` when granted immediately and `Ok(false)` when a fresh
    /// request would need to wait. Same-owner conversions never enqueue: they
    /// either complete immediately or return a lock conversion error.
    #[inline]
    pub fn try_acquire(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> Result<bool> {
        validate_mode(resource, mode)?;
        // The DashMap entry guard is the per-resource serialization boundary:
        // callers for different resources can proceed independently, while all
        // grant/queue decisions for this resource stay atomic.
        let mut resource_state = self.resources.entry(resource).or_default();
        match resource_state.try_acquire_immediate(resource, mode, owner)? {
            AcquireImmediate::Granted => Ok(true),
            AcquireImmediate::WouldWait => Ok(false),
        }
    }

    /// Acquires a lock, waiting until a fresh conflicting request can be granted.
    ///
    /// Blocking conversion is not supported. If the same owner already holds an
    /// incomparable or non-immediate weaker mode, this method returns the same
    /// explicit operation error as [`Self::try_acquire`].
    #[inline]
    pub async fn acquire(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> Result<()> {
        validate_mode(resource, mode)?;
        let waiter = {
            // Reuse the non-blocking path first. If the request must wait,
            // enqueue the waiter while still holding the resource guard so a
            // concurrent release cannot miss this request.
            let mut resource_state = self.resources.entry(resource).or_default();
            match resource_state.try_acquire_immediate(resource, mode, owner)? {
                AcquireImmediate::Granted => return Ok(()),
                AcquireImmediate::WouldWait => {}
            }
            let waiter = Arc::new(Waiter::new(owner, mode));
            resource_state.waiters.push_back(Arc::clone(&waiter));
            waiter
        };
        // The resource guard is dropped before awaiting; grant notification
        // and cleanup paths can keep mutating the same resource while this task
        // is parked.
        self.wait_for_grant(resource, mode, owner, waiter).await
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
    pub fn release(&self, resource: LockResource, owner: LockOwner) -> usize {
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
    pub fn release_owner(&self, owner: LockOwner) -> usize {
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

    /// Captures the current lock table for tests and internal diagnostics.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn debug_snapshot(&self) -> LockDebugSnapshot {
        let mut resources: Vec<_> = self
            .resources
            .iter()
            .map(|resource_state| *resource_state.key())
            .collect();
        resources.sort_unstable();
        let mut entries = Vec::new();
        for resource in resources {
            if let Some(resource_state) = self.resources.get(&resource) {
                entries.extend(resource_state.snapshot_entries(resource));
            }
        }
        LockDebugSnapshot { entries }
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
            }
        }
    }

    #[inline]
    fn owner_holds(&self, resource: LockResource, owner: LockOwner, requested: LockMode) -> bool {
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
    ) -> Result<AcquireImmediate> {
        if let Some(idx) = self.granted_idx(owner) {
            // Reentrant requests that are already covered do not create
            // duplicate granted entries.
            let held = self.granted[idx].mode;
            if mode_covers(resource, held, mode) {
                return Ok(AcquireImmediate::Granted);
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
            if !self.waiters.is_empty() || !self.compatible_with_granted(resource, mode, owner) {
                return Err(upgrade_would_block_err(resource, held, mode, owner));
            }
            self.granted[idx].mode = mode;
            return Ok(AcquireImmediate::Granted);
        }
        // Fresh compatible requests still wait behind an existing queue so
        // readers or intent holders cannot starve an older incompatible waiter.
        if self.waiters.is_empty() && self.compatible_with_granted(resource, mode, owner) {
            self.granted.push(GrantedLock { owner, mode });
            return Ok(AcquireImmediate::Granted);
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
    fn compatible_with_granted(
        &self,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
    ) -> bool {
        self.granted.iter().all(|granted| {
            granted.owner == owner || modes_are_compatible(resource, granted.mode, mode)
        })
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
            self.granted.push(GrantedLock {
                owner: waiter.owner,
                mode: waiter.mode,
            });
            waiter.set_outcome(WaitOutcome::Granted);
            granted_waiters.push(waiter);
        }
        granted_waiters
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.granted.is_empty() && self.waiters.is_empty()
    }

    #[inline]
    #[allow(dead_code)]
    fn snapshot_entries(&self, resource: LockResource) -> Vec<LockDebugEntry> {
        let mut entries = Vec::with_capacity(self.granted.len() + self.waiters.len());
        entries.extend(self.granted.iter().map(|granted| LockDebugEntry {
            resource,
            mode: granted.mode,
            owner: granted.owner,
            state: LockDebugEntryState::Granted,
            queue_order: None,
        }));
        entries.extend(
            self.waiters
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrantedLock {
    owner: LockOwner,
    mode: LockMode,
}

struct Waiter {
    owner: LockOwner,
    mode: LockMode,
    outcome: Mutex<WaitOutcome>,
    event: Event,
}

impl Waiter {
    #[inline]
    fn new(owner: LockOwner, mode: LockMode) -> Self {
        Waiter {
            owner,
            mode,
            outcome: Mutex::new(WaitOutcome::Waiting),
            event: Event::new(),
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitOutcome {
    Waiting,
    Granted,
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcquireImmediate {
    Granted,
    WouldWait,
}

/// Debug snapshot of all granted locks and queued waiters.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LockDebugSnapshot {
    /// Granted and waiting lock entries.
    pub(crate) entries: Vec<LockDebugEntry>,
}

/// One granted lock or queued waiter in a debug snapshot.
#[allow(dead_code)]
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
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockDebugEntryState {
    /// Lock is currently granted.
    Granted,
    /// Lock is waiting in the resource queue.
    Waiting,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn table_data(id: TableID) -> LockResource {
        LockResource::TableData(id)
    }

    fn table_metadata(id: TableID) -> LockResource {
        LockResource::TableMetadata(id)
    }

    fn trx(id: TrxID) -> LockOwner {
        LockOwner::Transaction(id)
    }

    fn stmt(trx_id: TrxID, seq: StatementSeq) -> LockOwner {
        LockOwner::Statement(trx_id, seq)
    }

    fn session(id: SessionID) -> LockOwner {
        LockOwner::Session(id)
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
            let snapshot = manager.debug_snapshot();
            if count_entries(&snapshot, resource, LockDebugEntryState::Waiting) == expected {
                return;
            }
            smol::Timer::after(Duration::from_millis(1)).await;
        }
        panic!("waiter count did not reach {expected}");
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
                    left.compatible_with(resource, right).unwrap(),
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
            assert!(
                LockMode::Shared
                    .compatible_with(resource, LockMode::Shared)
                    .unwrap()
            );
            assert!(
                !LockMode::Shared
                    .compatible_with(resource, LockMode::Exclusive)
                    .unwrap()
            );
            assert!(
                !LockMode::Exclusive
                    .compatible_with(resource, LockMode::Shared)
                    .unwrap()
            );
        }
    }

    #[test]
    fn multiple_compatible_holders_grant_together() {
        let manager = LockManager::new();
        let resource = table_data(7);
        assert!(
            manager
                .try_acquire(resource, LockMode::IntentShared, trx(1))
                .unwrap()
        );
        assert!(
            manager
                .try_acquire(resource, LockMode::IntentExclusive, trx(2))
                .unwrap()
        );
        assert!(
            manager
                .try_acquire(resource, LockMode::IntentShared, trx(3))
                .unwrap()
        );
        let snapshot = manager.debug_snapshot();
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
            assert!(
                manager
                    .try_acquire(resource, LockMode::Shared, trx(1))
                    .unwrap()
            );

            let waiter_manager = Arc::clone(&manager);
            let waiter = smol::spawn(async move {
                waiter_manager
                    .acquire(resource, LockMode::Exclusive, trx(2))
                    .await
            });
            wait_for_waiters(&manager, resource, 1).await;

            assert!(
                !manager
                    .try_acquire(resource, LockMode::Shared, trx(3))
                    .unwrap()
            );
            let snapshot = manager.debug_snapshot();
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
            assert!(
                manager
                    .try_acquire(resource, LockMode::Exclusive, trx(1))
                    .unwrap()
            );

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

            let snapshot = manager.debug_snapshot();
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
        assert!(
            manager
                .try_acquire(first, LockMode::IntentExclusive, trx(10))
                .unwrap()
        );
        assert!(
            manager
                .try_acquire(second, LockMode::IntentExclusive, trx(10))
                .unwrap()
        );

        assert_eq!(manager.release(first, trx(10)), 1);
        let snapshot = manager.debug_snapshot();
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
            let first = table_data(1);
            let second = table_data(2);
            assert!(
                manager
                    .try_acquire(first, LockMode::Exclusive, trx(1))
                    .unwrap()
            );
            assert!(
                manager
                    .try_acquire(second, LockMode::Shared, trx(2))
                    .unwrap()
            );

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
            let snapshot = manager.debug_snapshot();
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
        assert!(
            manager
                .try_acquire(resource, LockMode::IntentExclusive, trx(20))
                .unwrap()
        );
        assert!(
            manager
                .try_acquire(resource, LockMode::IntentShared, stmt(20, 1))
                .unwrap()
        );

        assert_eq!(manager.release_owner(stmt(20, 1)), 1);
        let snapshot = manager.debug_snapshot();
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
        assert!(
            manager
                .try_acquire(resource, LockMode::Exclusive, trx(1))
                .unwrap()
        );
        assert!(
            !manager
                .try_acquire(resource, LockMode::IntentShared, trx(2))
                .unwrap()
        );
    }

    #[test]
    fn same_owner_covered_requests_do_not_duplicate_entries() {
        let manager = LockManager::new();
        let resource = table_data(5);
        assert!(
            manager
                .try_acquire(resource, LockMode::Exclusive, session(1))
                .unwrap()
        );
        assert!(
            manager
                .try_acquire(resource, LockMode::Shared, session(1))
                .unwrap()
        );
        assert!(
            manager
                .try_acquire(resource, LockMode::IntentExclusive, session(1))
                .unwrap()
        );
        let snapshot = manager.debug_snapshot();
        assert_eq!(
            count_entries(&snapshot, resource, LockDebugEntryState::Granted),
            1
        );
        assert_eq!(snapshot.entries[0].mode, LockMode::Exclusive);
    }

    #[test]
    fn immediate_conversion_succeeds_only_when_it_will_not_wait() {
        let manager = LockManager::new();
        let resource = table_data(6);
        assert!(
            manager
                .try_acquire(resource, LockMode::IntentShared, trx(1))
                .unwrap()
        );
        assert!(
            manager
                .try_acquire(resource, LockMode::IntentExclusive, trx(1))
                .unwrap()
        );
        let snapshot = manager.debug_snapshot();
        assert_eq!(snapshot.entries[0].mode, LockMode::IntentExclusive);

        assert!(
            manager
                .try_acquire(resource, LockMode::IntentShared, trx(2))
                .unwrap()
        );
        assert_operation_err(
            manager.try_acquire(resource, LockMode::Exclusive, trx(1)),
            OperationError::LockUpgradeWouldBlock,
        );
    }

    #[test]
    fn incomparable_same_owner_conversion_is_explicit_error() {
        let manager = LockManager::new();
        let resource = table_data(8);
        assert!(
            manager
                .try_acquire(resource, LockMode::IntentExclusive, trx(1))
                .unwrap()
        );
        assert_operation_err(
            manager.try_acquire(resource, LockMode::Shared, trx(1)),
            OperationError::LockConversionNotSupported,
        );
    }

    #[test]
    fn async_acquire_waits_behind_conflict_and_completes_after_release() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = LockResource::CatalogNamespace;
            assert!(
                manager
                    .try_acquire(resource, LockMode::Exclusive, trx(1))
                    .unwrap()
            );

            let waiter = {
                let manager = Arc::clone(&manager);
                smol::spawn(
                    async move { manager.acquire(resource, LockMode::Shared, trx(2)).await },
                )
            };
            wait_for_waiters(&manager, resource, 1).await;
            assert_eq!(manager.release(resource, trx(1)), 1);
            waiter.await.unwrap();

            let snapshot = manager.debug_snapshot();
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
    fn debug_snapshot_reports_granted_waiting_and_queue_order() {
        smol::block_on(async {
            let manager = Arc::new(LockManager::new());
            let resource = table_metadata(42);
            assert!(
                manager
                    .try_acquire(resource, LockMode::Shared, trx(1))
                    .unwrap()
            );
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

            let snapshot = manager.debug_snapshot();
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
