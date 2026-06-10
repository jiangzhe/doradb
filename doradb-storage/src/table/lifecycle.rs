use crate::error::{Error, InternalError, OperationError, Result};
use crate::id::TableID;
use error_stack::Report;
use event_listener::{Event, listener};
use std::sync::atomic::{AtomicU32, Ordering};

/// Normal checkpoint cancellation reasons for user-table checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointCancelReason {
    /// Checkpoint reached publication after a table drop gate had started.
    TableDropping,
    /// Checkpoint reached publication after the table had already been dropped.
    TableDropped,
    /// Checkpoint was excluded by an active table metadata change.
    TableMetadataChanging,
    /// Checkpoint was excluded because another checkpoint is already active.
    CheckpointInProgress,
}

/// Volatile runtime lifecycle state for a user-table handle.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableLifecycleState {
    /// Foreground and checkpoint publication may proceed.
    Live = 0,
    /// Drop has crossed the irreversible runtime barrier.
    Dropping = 1,
    /// Drop has completed for this runtime handle.
    Dropped = 2,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishGateState {
    Open = 0,
    Publishing = 1,
    ClosingPublishing = 2,
    Closed = 3,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetadataChangePhase {
    Open = 0,
    Pending = 1,
    Active = 2,
}

/// Volatile lifecycle, checkpoint, and metadata-change gates for one user table.
///
/// This is one atomically packed state machine with three domains that must stay
/// coherent:
///
/// * table liveness: `Live -> Dropping -> Dropped`. This is terminal and never
///   moves back to a previous state.
/// * checkpoint publish admission: `Open` admits one publish lease, `Publishing`
///   means that lease is active, `ClosingPublishing` means drop has started and
///   is waiting for that lease to drain, and `Closed` means no publish lease may
///   be acquired again. Legal combinations are:
///   `Live + (Open | Publishing)`, `Dropping + (ClosingPublishing | Closed)`,
///   and `Dropped + Closed`.
/// * metadata/root exclusion: checkpoint root mutation and future index DDL are
///   mutually exclusive. Legal combinations are `(Open, no root)`, `(Open, root)`,
///   `(Pending, root)`, `(Pending, no root)`, and `(Active, no root)`. The
///   `Pending, no root` combination is the handoff window after a checkpoint
///   releases the root mutation lease and before the waiting metadata change
///   future wakes. `Active` is only legal while the table is still `Live`.
///
/// `Dropping` and `Dropped` may temporarily coexist with an active checkpoint
/// root-mutation lease or a pending metadata-change waiter that was admitted
/// before drop closed the terminal state. Those leases/waiters are unwinding;
/// no new checkpoint or metadata operation is admitted once the table is not
/// `Live`.
///
/// Transition summary:
///
/// * checkpoint root mutation: `Live/Open/no root -> Live/Open/root`, then
///   release returns to no root. If a metadata change is pending, release leaves
///   `Pending/no root` so that waiter can become `Active`.
/// * metadata change: `Open/no root -> Active -> Open`, or `Open/root ->
///   Pending -> Active -> Open` after the checkpoint root-mutation lease ends.
/// * checkpoint publish: while a root-mutation lease is active, `Live/Open ->
///   Live/Publishing -> Live/Open`. If drop starts during publish, it becomes
///   `Dropping/ClosingPublishing -> Dropping/Closed`.
/// * drop: `Live/Open -> Dropping/Closed` when no publish lease is active, or
///   `Live/Publishing -> Dropping/ClosingPublishing -> Dropping/Closed` when
///   drop must drain an active publish lease. Runtime removal completes with
///   `Dropping/Closed -> Dropped/Closed`.
pub(crate) struct TableLifecycle {
    state: AtomicU32,
    changed: Event,
}

const TERMINAL_MASK: u32 = 0b11;
const PUBLISH_SHIFT: u32 = 2;
const PUBLISH_MASK: u32 = 0b11 << PUBLISH_SHIFT;
const METADATA_CHANGE_SHIFT: u32 = 4;
const METADATA_CHANGE_MASK: u32 = 0b11 << METADATA_CHANGE_SHIFT;
const ROOT_MUTATION_ACTIVE_BIT: u32 = 1 << 6;
const STATE_MASK: u32 =
    TERMINAL_MASK | PUBLISH_MASK | METADATA_CHANGE_MASK | ROOT_MUTATION_ACTIVE_BIT;
const INITIAL_STATE: u32 = (TableLifecycleState::Live as u32)
    | ((PublishGateState::Open as u32) << PUBLISH_SHIFT)
    | ((MetadataChangePhase::Open as u32) << METADATA_CHANGE_SHIFT);

#[derive(Clone, Copy, Debug)]
struct TableLifecycleInner {
    /// Terminal liveness visible to foreground operations and DDL.
    terminal: TableLifecycleState,
    /// Admission/drain state for checkpoint root publication.
    publish: PublishGateState,
    /// Future index-DDL gate state relative to checkpoint root mutation.
    metadata_change: MetadataChangePhase,
    /// True while one checkpoint owns the table-root mutation section.
    root_mutation_active: bool,
}

impl TableLifecycleInner {
    #[inline]
    fn decode(raw: u32) -> Self {
        debug_assert_eq!(
            raw & !STATE_MASK,
            0,
            "table lifecycle atomic state contains unknown bits: {raw:#x}"
        );
        let terminal = match raw & TERMINAL_MASK {
            0 => TableLifecycleState::Live,
            1 => TableLifecycleState::Dropping,
            2 => TableLifecycleState::Dropped,
            _ => unreachable!("invalid table lifecycle terminal bits"),
        };
        let publish = match (raw & PUBLISH_MASK) >> PUBLISH_SHIFT {
            0 => PublishGateState::Open,
            1 => PublishGateState::Publishing,
            2 => PublishGateState::ClosingPublishing,
            3 => PublishGateState::Closed,
            _ => unreachable!("invalid table lifecycle publish bits"),
        };
        let metadata_change = match (raw & METADATA_CHANGE_MASK) >> METADATA_CHANGE_SHIFT {
            0 => MetadataChangePhase::Open,
            1 => MetadataChangePhase::Pending,
            2 => MetadataChangePhase::Active,
            _ => unreachable!("invalid table lifecycle metadata-change bits"),
        };
        let root_mutation_active = raw & ROOT_MUTATION_ACTIVE_BIT != 0;
        Self {
            terminal,
            publish,
            metadata_change,
            root_mutation_active,
        }
    }

    #[inline]
    fn encode(self) -> u32 {
        (self.terminal as u32)
            | ((self.publish as u32) << PUBLISH_SHIFT)
            | ((self.metadata_change as u32) << METADATA_CHANGE_SHIFT)
            | if self.root_mutation_active {
                ROOT_MUTATION_ACTIVE_BIT
            } else {
                0
            }
    }

    #[inline]
    fn debug_assert_valid(&self, operation: &'static str) {
        debug_assert!(
            self.valid_publish_domain() && self.valid_metadata_root_domain(),
            "invalid table lifecycle state during {operation}: {self:?}"
        );
    }

    #[inline]
    fn valid_publish_domain(&self) -> bool {
        match self.terminal {
            TableLifecycleState::Live => {
                matches!(
                    self.publish,
                    PublishGateState::Open | PublishGateState::Publishing
                )
            }
            TableLifecycleState::Dropping => matches!(
                self.publish,
                PublishGateState::ClosingPublishing | PublishGateState::Closed
            ),
            TableLifecycleState::Dropped => self.publish == PublishGateState::Closed,
        }
    }

    #[inline]
    fn valid_metadata_root_domain(&self) -> bool {
        if self.metadata_change == MetadataChangePhase::Active {
            self.terminal == TableLifecycleState::Live && !self.root_mutation_active
        } else {
            true
        }
    }
}

impl TableLifecycle {
    /// Create a live lifecycle gate.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU32::new(INITIAL_STATE),
            changed: Event::new(),
        }
    }

    #[inline]
    fn load_state(&self, operation: &'static str) -> (u32, TableLifecycleInner) {
        let raw = self.state.load(Ordering::Acquire);
        let state = TableLifecycleInner::decode(raw);
        state.debug_assert_valid(operation);
        (raw, state)
    }

    #[inline]
    fn compare_exchange_state(&self, current: u32, next: TableLifecycleInner) -> bool {
        self.state
            .compare_exchange(current, next.encode(), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Returns the current externally visible lifecycle state.
    #[inline]
    pub(crate) fn state(&self) -> TableLifecycleState {
        let (_, state) = self.load_state("read state");
        state.terminal
    }

    /// Ensures a foreground operation may proceed after logical locks are held.
    #[inline]
    pub(crate) fn check_foreground_live(
        &self,
        table_id: TableID,
        operation: &'static str,
    ) -> Result<()> {
        match self.state() {
            TableLifecycleState::Live => Ok(()),
            state => Err(foreground_not_live_err(table_id, operation, state)),
        }
    }

    /// Starts the irreversible drop gate after the caller has acquired DDL locks.
    ///
    /// This marks the handle as dropping, closes new checkpoint publication
    /// admission, and waits for any already-held publish lease to drain.
    pub(crate) async fn begin_drop(&self, table_id: TableID) -> Result<()> {
        loop {
            let (raw, state) = self.load_state("begin drop enter");
            if state.terminal != TableLifecycleState::Live {
                return Err(drop_not_live_err(table_id, "begin drop", state.terminal));
            }
            if state.metadata_change == MetadataChangePhase::Active {
                return Err(begin_drop_metadata_active_err(table_id));
            }

            let mut next = state;
            next.terminal = TableLifecycleState::Dropping;
            let wait_for_publish = match state.publish {
                PublishGateState::Open => {
                    next.publish = PublishGateState::Closed;
                    next.debug_assert_valid("begin drop close open publish gate");
                    false
                }
                PublishGateState::Publishing => {
                    next.publish = PublishGateState::ClosingPublishing;
                    next.debug_assert_valid("begin drop wait for active publish lease");
                    true
                }
                PublishGateState::ClosingPublishing | PublishGateState::Closed => {
                    next.publish = PublishGateState::Closed;
                    next.debug_assert_valid("begin drop close already closing publish gate");
                    false
                }
            };

            if self.compare_exchange_state(raw, next) {
                self.changed.notify(usize::MAX);
                if wait_for_publish {
                    self.wait_for_publish_gate_closed().await;
                }
                return Ok(());
            }
        }
    }

    /// Marks a dropping table handle as fully dropped.
    #[inline]
    pub(crate) fn mark_dropped(&self, table_id: TableID) -> Result<()> {
        loop {
            let (raw, state) = self.load_state("mark dropped enter");
            if state.terminal != TableLifecycleState::Dropping {
                return Err(mark_dropped_err(table_id, state.terminal));
            }
            if state.publish != PublishGateState::Closed {
                return Err(mark_dropped_publish_err(table_id, state.publish));
            }

            let mut next = state;
            next.terminal = TableLifecycleState::Dropped;
            next.debug_assert_valid("mark dropped exit");
            if self.compare_exchange_state(raw, next) {
                self.changed.notify(usize::MAX);
                return Ok(());
            }
        }
    }

    /// Attempts to enter the no-cancel checkpoint publish section.
    pub(crate) fn try_begin_checkpoint_publish(
        &self,
    ) -> std::result::Result<CheckpointPublishLease<'_>, CheckpointCancelReason> {
        loop {
            let (raw, state) = self.load_state("begin checkpoint publish enter");
            check_terminal_for_checkpoint(state.terminal)?;
            assert!(
                state.root_mutation_active,
                "checkpoint publish requires active root mutation lease"
            );
            match state.publish {
                PublishGateState::Open => {
                    let mut next = state;
                    next.publish = PublishGateState::Publishing;
                    next.debug_assert_valid("begin checkpoint publish exit");
                    if self.compare_exchange_state(raw, next) {
                        return Ok(CheckpointPublishLease { lifecycle: self });
                    }
                }
                PublishGateState::Publishing => {
                    debug_assert_ne!(
                        state.publish,
                        PublishGateState::Publishing,
                        "concurrent checkpoint publish lease is not expected"
                    );
                    return Err(CheckpointCancelReason::TableDropping);
                }
                PublishGateState::ClosingPublishing | PublishGateState::Closed => {
                    return Err(CheckpointCancelReason::TableDropping);
                }
            }
        }
    }

    /// Acquires the reversible table metadata-change gate for future index DDL.
    #[inline]
    pub(crate) async fn begin_metadata_change(
        &self,
        table_id: TableID,
    ) -> Result<TableMetadataChangeLease<'_>> {
        let mut pending = None;
        loop {
            let mut should_wait = true;
            {
                let (raw, state) = self.load_state("begin metadata change enter");
                if state.terminal != TableLifecycleState::Live {
                    return Err(drop_not_live_err(
                        table_id,
                        "begin metadata change",
                        state.terminal,
                    ));
                }

                match state.metadata_change {
                    MetadataChangePhase::Open if !state.root_mutation_active => {
                        let mut next = state;
                        next.metadata_change = MetadataChangePhase::Active;
                        next.debug_assert_valid("begin metadata change active");
                        if self.compare_exchange_state(raw, next) {
                            return Ok(TableMetadataChangeLease { lifecycle: self });
                        }
                        should_wait = false;
                    }
                    MetadataChangePhase::Open if pending.is_none() => {
                        let mut next = state;
                        next.metadata_change = MetadataChangePhase::Pending;
                        next.debug_assert_valid("begin metadata change pending");
                        if self.compare_exchange_state(raw, next) {
                            pending = Some(PendingMetadataChange::new(self));
                        }
                        should_wait = false;
                    }
                    MetadataChangePhase::Open => {}
                    MetadataChangePhase::Pending
                        if pending.is_some() && !state.root_mutation_active =>
                    {
                        let mut next = state;
                        next.metadata_change = MetadataChangePhase::Active;
                        next.debug_assert_valid("begin pending metadata change active");
                        if self.compare_exchange_state(raw, next) {
                            if let Some(pending) = &mut pending {
                                pending.disarm();
                            }
                            return Ok(TableMetadataChangeLease { lifecycle: self });
                        }
                        should_wait = false;
                    }
                    MetadataChangePhase::Pending | MetadataChangePhase::Active => {}
                }
            }

            if !should_wait {
                continue;
            }

            listener!(self.changed => listener);
            {
                let (_, state) = self.load_state("begin metadata change wait");
                if state.terminal != TableLifecycleState::Live
                    || state.metadata_change == MetadataChangePhase::Open
                    || (state.metadata_change == MetadataChangePhase::Pending
                        && pending.is_some()
                        && !state.root_mutation_active)
                {
                    continue;
                }
            }
            listener.await;
        }
    }

    /// Attempts to enter the checkpoint root-mutation section.
    #[inline]
    pub(crate) fn try_begin_checkpoint_root_mutation(
        &self,
    ) -> std::result::Result<TableCheckpointRootMutationLease<'_>, CheckpointCancelReason> {
        loop {
            let (raw, state) = self.load_state("begin checkpoint root mutation enter");
            check_terminal_for_checkpoint(state.terminal)?;
            if state.metadata_change != MetadataChangePhase::Open {
                return Err(CheckpointCancelReason::TableMetadataChanging);
            }
            if state.root_mutation_active {
                return Err(CheckpointCancelReason::CheckpointInProgress);
            }

            let mut next = state;
            next.root_mutation_active = true;
            next.debug_assert_valid("begin checkpoint root mutation exit");
            if self.compare_exchange_state(raw, next) {
                return Ok(TableCheckpointRootMutationLease { lifecycle: self });
            }
        }
    }

    async fn wait_for_publish_gate_closed(&self) {
        loop {
            {
                let (_, state) = self.load_state("wait for publish gate closed check");
                if state.publish == PublishGateState::Closed {
                    break;
                }
            }
            listener!(self.changed => listener);
            {
                let (_, state) = self.load_state("wait for publish gate closed listen check");
                if state.publish == PublishGateState::Closed {
                    break;
                }
            }
            listener.await;
        }
    }

    #[inline]
    fn release_publish_lease(&self) {
        loop {
            let (raw, state) = self.load_state("release checkpoint publish lease enter");
            // Normal checkpoint publication reopens the gate. If drop observed this
            // lease and moved the gate to ClosingPublishing, this release is the
            // handoff point that permanently closes the gate and wakes the dropper.
            let mut next = state;
            match state.publish {
                PublishGateState::Publishing => next.publish = PublishGateState::Open,
                PublishGateState::ClosingPublishing => next.publish = PublishGateState::Closed,
                publish => {
                    debug_assert!(
                        matches!(
                            publish,
                            PublishGateState::Publishing | PublishGateState::ClosingPublishing
                        ),
                        "publish lease release in unexpected gate state: {:?}",
                        publish
                    );
                    return;
                }
            }
            next.debug_assert_valid("release checkpoint publish lease exit");
            if self.compare_exchange_state(raw, next) {
                self.changed.notify(usize::MAX);
                return;
            }
        }
    }

    #[inline]
    fn release_metadata_change(&self) {
        loop {
            let (raw, state) = self.load_state("release metadata change enter");
            debug_assert_eq!(state.metadata_change, MetadataChangePhase::Active);
            if state.metadata_change != MetadataChangePhase::Active {
                return;
            }
            let mut next = state;
            next.metadata_change = MetadataChangePhase::Open;
            next.debug_assert_valid("release metadata change exit");
            if self.compare_exchange_state(raw, next) {
                self.changed.notify(usize::MAX);
                return;
            }
        }
    }

    #[inline]
    fn release_checkpoint_root_mutation(&self) {
        loop {
            let (raw, state) = self.load_state("release checkpoint root mutation enter");
            debug_assert!(state.root_mutation_active);
            if !state.root_mutation_active {
                return;
            }
            let mut next = state;
            next.root_mutation_active = false;
            next.debug_assert_valid("release checkpoint root mutation exit");
            if self.compare_exchange_state(raw, next) {
                self.changed.notify(usize::MAX);
                return;
            }
        }
    }

    #[inline]
    fn release_pending_metadata_change(&self) {
        loop {
            let (raw, state) = self.load_state("release pending metadata change enter");
            if state.metadata_change != MetadataChangePhase::Pending {
                return;
            }
            let mut next = state;
            next.metadata_change = MetadataChangePhase::Open;
            next.debug_assert_valid("release pending metadata change exit");
            if self.compare_exchange_state(raw, next) {
                self.changed.notify(usize::MAX);
                return;
            }
        }
    }
}

struct PendingMetadataChange<'a> {
    lifecycle: &'a TableLifecycle,
    armed: bool,
}

impl<'a> PendingMetadataChange<'a> {
    #[inline]
    fn new(lifecycle: &'a TableLifecycle) -> Self {
        Self {
            lifecycle,
            armed: true,
        }
    }

    #[inline]
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingMetadataChange<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.armed {
            self.lifecycle.release_pending_metadata_change();
        }
    }
}

/// RAII guard for the no-cancel checkpoint publish section.
pub(crate) struct CheckpointPublishLease<'a> {
    lifecycle: &'a TableLifecycle,
}

impl std::fmt::Debug for CheckpointPublishLease<'_> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointPublishLease")
            .finish_non_exhaustive()
    }
}

impl Drop for CheckpointPublishLease<'_> {
    #[inline]
    fn drop(&mut self) {
        self.lifecycle.release_publish_lease();
    }
}

/// RAII guard for a reversible table metadata-change section.
pub(crate) struct TableMetadataChangeLease<'a> {
    lifecycle: &'a TableLifecycle,
}

impl std::fmt::Debug for TableMetadataChangeLease<'_> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableMetadataChangeLease")
            .finish_non_exhaustive()
    }
}

impl Drop for TableMetadataChangeLease<'_> {
    #[inline]
    fn drop(&mut self) {
        self.lifecycle.release_metadata_change();
    }
}

/// RAII guard for checkpoint table-root mutation before root publication.
pub(crate) struct TableCheckpointRootMutationLease<'a> {
    lifecycle: &'a TableLifecycle,
}

impl std::fmt::Debug for TableCheckpointRootMutationLease<'_> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableCheckpointRootMutationLease")
            .finish_non_exhaustive()
    }
}

impl Drop for TableCheckpointRootMutationLease<'_> {
    #[inline]
    fn drop(&mut self) {
        self.lifecycle.release_checkpoint_root_mutation();
    }
}

#[inline]
fn check_terminal_for_checkpoint(
    state: TableLifecycleState,
) -> std::result::Result<(), CheckpointCancelReason> {
    match state {
        TableLifecycleState::Live => Ok(()),
        TableLifecycleState::Dropping => Err(CheckpointCancelReason::TableDropping),
        TableLifecycleState::Dropped => Err(CheckpointCancelReason::TableDropped),
    }
}

#[inline]
fn foreground_not_live_err(
    table_id: TableID,
    operation: &'static str,
    state: TableLifecycleState,
) -> Error {
    let reason = match state {
        TableLifecycleState::Live => unreachable!("live table should not fail foreground check"),
        TableLifecycleState::Dropping => OperationError::TableDropping,
        TableLifecycleState::Dropped => OperationError::TableNotFound,
    };
    Report::new(reason)
        .attach(format!(
            "foreground table access rejected: table_id={table_id}, operation={operation}, lifecycle_state={state:?}"
        ))
        .into()
}

#[inline]
fn drop_not_live_err(
    table_id: TableID,
    operation: &'static str,
    state: TableLifecycleState,
) -> Error {
    let reason = match state {
        TableLifecycleState::Live => unreachable!("live table should not fail drop gate"),
        TableLifecycleState::Dropping => OperationError::TableDropping,
        TableLifecycleState::Dropped => OperationError::TableNotFound,
    };
    Report::new(reason)
        .attach(format!(
            "table lifecycle operation rejected: table_id={table_id}, operation={operation}, lifecycle_state={state:?}"
        ))
        .into()
}

#[inline]
fn mark_dropped_err(table_id: TableID, state: TableLifecycleState) -> Error {
    Report::new(InternalError::Generic)
        .attach(format!(
            "mark dropped requires Dropping state: table_id={table_id}, lifecycle_state={state:?}"
        ))
        .into()
}

#[inline]
fn mark_dropped_publish_err(table_id: TableID, publish: PublishGateState) -> Error {
    Report::new(InternalError::Generic)
        .attach(format!(
            "mark dropped requires closed publish gate: table_id={table_id}, publish_state={publish:?}"
        ))
        .into()
}

#[inline]
fn begin_drop_metadata_active_err(table_id: TableID) -> Error {
    Report::new(InternalError::Generic)
        .attach(format!(
            "begin drop requires no active metadata change: table_id={table_id}"
        ))
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE_ID: TableID = TableID::new(42);

    #[test]
    fn test_lifecycle_foreground_errors_for_terminal_states() {
        smol::block_on(async {
            let lifecycle = TableLifecycle::new();
            lifecycle.begin_drop(TABLE_ID).await.unwrap();

            let err = lifecycle
                .check_foreground_live(TABLE_ID, "scan")
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableDropping));

            lifecycle.mark_dropped(TABLE_ID).unwrap();
            let err = lifecycle
                .check_foreground_live(TABLE_ID, "insert")
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        });
    }

    #[test]
    fn test_drop_gate_waits_for_active_publish_lease_before_completing() {
        smol::block_on(async {
            let lifecycle = std::sync::Arc::new(TableLifecycle::new());
            let root_lease = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
            let lease = lifecycle.try_begin_checkpoint_publish().unwrap();
            let mut drop_fut = Box::pin(lifecycle.begin_drop(TABLE_ID));

            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(lifecycle.state(), TableLifecycleState::Dropping);
            match lifecycle.try_begin_checkpoint_publish() {
                Ok(_lease) => panic!("publish lease should be blocked by drop gate"),
                Err(reason) => assert_eq!(reason, CheckpointCancelReason::TableDropping),
            }
            match lifecycle.try_begin_checkpoint_root_mutation() {
                Ok(_lease) => panic!("root mutation should be blocked by drop gate"),
                Err(reason) => assert_eq!(reason, CheckpointCancelReason::TableDropping),
            }
            let err = lifecycle.begin_metadata_change(TABLE_ID).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableDropping));

            drop(lease);
            drop_fut.await.unwrap();
            assert_eq!(lifecycle.state(), TableLifecycleState::Dropping);
            drop(root_lease);
        });
    }

    #[test]
    fn test_mark_dropped_requires_drained_publish_gate() {
        smol::block_on(async {
            let lifecycle = std::sync::Arc::new(TableLifecycle::new());
            let root_lease = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
            let publish_lease = lifecycle.try_begin_checkpoint_publish().unwrap();
            let mut drop_fut = Box::pin(lifecycle.begin_drop(TABLE_ID));

            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(lifecycle.mark_dropped(TABLE_ID).is_err());

            drop(publish_lease);
            drop_fut.await.unwrap();
            lifecycle.mark_dropped(TABLE_ID).unwrap();
            assert_eq!(lifecycle.state(), TableLifecycleState::Dropped);
            drop(root_lease);
        });
    }

    #[test]
    #[should_panic(expected = "checkpoint publish requires active root mutation lease")]
    fn test_checkpoint_publish_requires_root_mutation_lease() {
        let lifecycle = TableLifecycle::new();
        let _lease = lifecycle.try_begin_checkpoint_publish();
    }

    #[test]
    fn test_begin_drop_rejects_active_metadata_change() {
        smol::block_on(async {
            let lifecycle = TableLifecycle::new();
            let metadata_lease = lifecycle.begin_metadata_change(TABLE_ID).await.unwrap();

            assert!(lifecycle.begin_drop(TABLE_ID).await.is_err());
            assert_eq!(lifecycle.state(), TableLifecycleState::Live);

            drop(metadata_lease);
            lifecycle.begin_drop(TABLE_ID).await.unwrap();
            assert_eq!(lifecycle.state(), TableLifecycleState::Dropping);
        });
    }

    #[test]
    fn test_dropping_transition_has_no_live_restore_path() {
        smol::block_on(async {
            let lifecycle = TableLifecycle::new();
            lifecycle.begin_drop(TABLE_ID).await.unwrap();
            assert_eq!(lifecycle.state(), TableLifecycleState::Dropping);

            let err = lifecycle.begin_drop(TABLE_ID).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableDropping));

            lifecycle.mark_dropped(TABLE_ID).unwrap();
            assert_eq!(lifecycle.state(), TableLifecycleState::Dropped);
        });
    }

    #[test]
    fn test_metadata_change_blocks_checkpoint_root_mutation() {
        smol::block_on(async {
            let lifecycle = TableLifecycle::new();
            let metadata_lease = lifecycle.begin_metadata_change(TABLE_ID).await.unwrap();

            match lifecycle.try_begin_checkpoint_root_mutation() {
                Ok(_lease) => panic!("checkpoint root mutation should be cancelled"),
                Err(reason) => assert_eq!(reason, CheckpointCancelReason::TableMetadataChanging),
            }

            drop(metadata_lease);
            let _root_lease = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
        });
    }

    #[test]
    fn test_active_checkpoint_root_mutation_blocks_concurrent_checkpoint() {
        let lifecycle = TableLifecycle::new();
        let root_lease = lifecycle.try_begin_checkpoint_root_mutation().unwrap();

        match lifecycle.try_begin_checkpoint_root_mutation() {
            Ok(_lease) => panic!("concurrent checkpoint root mutation should be cancelled"),
            Err(reason) => assert_eq!(reason, CheckpointCancelReason::CheckpointInProgress),
        }

        drop(root_lease);
        let _root_lease = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
    }

    #[test]
    fn test_metadata_change_waits_for_active_checkpoint_root_mutation() {
        smol::block_on(async {
            let lifecycle = TableLifecycle::new();
            let root_lease = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
            let mut metadata_fut = Box::pin(lifecycle.begin_metadata_change(TABLE_ID));

            assert!(matches!(
                futures::poll!(metadata_fut.as_mut()),
                std::task::Poll::Pending
            ));
            match lifecycle.try_begin_checkpoint_root_mutation() {
                Ok(_lease) => panic!("pending metadata change should block new checkpoint roots"),
                Err(reason) => assert_eq!(reason, CheckpointCancelReason::TableMetadataChanging),
            }
            let publish_lease = lifecycle.try_begin_checkpoint_publish().unwrap();
            drop(publish_lease);

            drop(root_lease);
            let metadata_lease = metadata_fut.await.unwrap();
            match lifecycle.try_begin_checkpoint_root_mutation() {
                Ok(_lease) => panic!("active metadata change should block checkpoint roots"),
                Err(reason) => assert_eq!(reason, CheckpointCancelReason::TableMetadataChanging),
            }
            drop(metadata_lease);
            let _root_lease = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
        });
    }

    #[test]
    fn test_pending_metadata_change_cancellation_reopens_checkpoint_root_mutation() {
        smol::block_on(async {
            let lifecycle = TableLifecycle::new();
            let root_lease = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
            let mut metadata_fut = Box::pin(lifecycle.begin_metadata_change(TABLE_ID));

            assert!(matches!(
                futures::poll!(metadata_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(metadata_fut);
            drop(root_lease);
            let _root_lease = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
        });
    }
}
