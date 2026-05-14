use crate::catalog::TableID;
use crate::error::{Error, InternalError, OperationError, Result};
use error_stack::Report;
use event_listener::{Event, listener};
use parking_lot::Mutex;

/// Normal checkpoint cancellation reasons for user-table checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointCancelReason {
    /// Checkpoint reached publication after a table drop gate had started.
    TableDropping,
    /// Checkpoint reached publication after the table had already been dropped.
    TableDropped,
    /// Checkpoint was excluded by an active table metadata change.
    TableMetadataChanging,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishGateState {
    Open,
    Publishing,
    ClosingPublishing,
    Closed,
}

/// Volatile lifecycle, checkpoint, and metadata-change gates for one user table.
///
/// This is one mutex-protected state machine with three domains that must stay
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
    state: Mutex<TableLifecycleInner>,
    changed: Event,
}

#[derive(Debug)]
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

impl Default for TableLifecycleInner {
    #[inline]
    fn default() -> Self {
        Self {
            terminal: TableLifecycleState::Live,
            publish: PublishGateState::Open,
            metadata_change: MetadataChangePhase::Open,
            root_mutation_active: false,
        }
    }
}

impl TableLifecycleInner {
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
            state: Mutex::new(TableLifecycleInner::default()),
            changed: Event::new(),
        }
    }

    /// Returns the current externally visible lifecycle state.
    #[inline]
    pub(crate) fn state(&self) -> TableLifecycleState {
        let state = self.state.lock();
        state.debug_assert_valid("read state");
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
        {
            let mut state = self.state.lock();
            state.debug_assert_valid("begin drop enter");
            if state.terminal != TableLifecycleState::Live {
                return Err(drop_not_live_err(table_id, "begin drop", state.terminal));
            }
            if state.metadata_change == MetadataChangePhase::Active {
                return Err(begin_drop_metadata_active_err(table_id));
            }
            state.terminal = TableLifecycleState::Dropping;
            match state.publish {
                PublishGateState::Open => {
                    state.publish = PublishGateState::Closed;
                    state.debug_assert_valid("begin drop close open publish gate");
                    drop(state);
                    self.changed.notify(usize::MAX);
                    return Ok(());
                }
                PublishGateState::Publishing => {
                    state.publish = PublishGateState::ClosingPublishing;
                    state.debug_assert_valid("begin drop wait for active publish lease");
                }
                PublishGateState::ClosingPublishing | PublishGateState::Closed => {
                    state.publish = PublishGateState::Closed;
                    state.debug_assert_valid("begin drop close already closing publish gate");
                    drop(state);
                    self.changed.notify(usize::MAX);
                    return Ok(());
                }
            }
        }
        self.changed.notify(usize::MAX);
        self.wait_for_publish_gate_closed().await;
        Ok(())
    }

    /// Marks a dropping table handle as fully dropped.
    #[inline]
    pub(crate) fn mark_dropped(&self, table_id: TableID) -> Result<()> {
        let mut state = self.state.lock();
        state.debug_assert_valid("mark dropped enter");
        if state.terminal != TableLifecycleState::Dropping {
            return Err(mark_dropped_err(table_id, state.terminal));
        }
        if state.publish != PublishGateState::Closed {
            return Err(mark_dropped_publish_err(table_id, state.publish));
        }
        state.terminal = TableLifecycleState::Dropped;
        state.debug_assert_valid("mark dropped exit");
        drop(state);
        self.changed.notify(usize::MAX);
        Ok(())
    }

    /// Attempts to enter the no-cancel checkpoint publish section.
    pub(crate) fn try_begin_checkpoint_publish(
        &self,
    ) -> std::result::Result<CheckpointPublishLease<'_>, CheckpointCancelReason> {
        let mut state = self.state.lock();
        state.debug_assert_valid("begin checkpoint publish enter");
        check_terminal_for_checkpoint(state.terminal)?;
        assert!(
            state.root_mutation_active,
            "checkpoint publish requires active root mutation lease"
        );
        match state.publish {
            PublishGateState::Open => {
                state.publish = PublishGateState::Publishing;
                state.debug_assert_valid("begin checkpoint publish exit");
                Ok(CheckpointPublishLease { lifecycle: self })
            }
            PublishGateState::Publishing => {
                debug_assert_ne!(
                    state.publish,
                    PublishGateState::Publishing,
                    "concurrent checkpoint publish lease is not expected"
                );
                Err(CheckpointCancelReason::TableDropping)
            }
            PublishGateState::ClosingPublishing | PublishGateState::Closed => {
                Err(CheckpointCancelReason::TableDropping)
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
            {
                let mut state = self.state.lock();
                state.debug_assert_valid("begin metadata change enter");
                if state.terminal != TableLifecycleState::Live {
                    return Err(drop_not_live_err(
                        table_id,
                        "begin metadata change",
                        state.terminal,
                    ));
                }
                match state.metadata_change {
                    MetadataChangePhase::Open if !state.root_mutation_active => {
                        state.metadata_change = MetadataChangePhase::Active;
                        state.debug_assert_valid("begin metadata change active");
                        return Ok(TableMetadataChangeLease { lifecycle: self });
                    }
                    MetadataChangePhase::Open => {
                        state.metadata_change = MetadataChangePhase::Pending;
                        state.debug_assert_valid("begin metadata change pending");
                        pending = Some(PendingMetadataChange::new(self));
                    }
                    MetadataChangePhase::Pending
                        if pending.is_some() && !state.root_mutation_active =>
                    {
                        state.metadata_change = MetadataChangePhase::Active;
                        state.debug_assert_valid("begin pending metadata change active");
                        if let Some(pending) = &mut pending {
                            pending.disarm();
                        }
                        return Ok(TableMetadataChangeLease { lifecycle: self });
                    }
                    MetadataChangePhase::Pending | MetadataChangePhase::Active => {}
                }
            }
            listener!(self.changed => listener);
            {
                let state = self.state.lock();
                state.debug_assert_valid("begin metadata change wait");
                if state.terminal != TableLifecycleState::Live {
                    continue;
                }
                if state.metadata_change == MetadataChangePhase::Pending
                    && pending.is_some()
                    && !state.root_mutation_active
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
        let mut state = self.state.lock();
        state.debug_assert_valid("begin checkpoint root mutation enter");
        check_terminal_for_checkpoint(state.terminal)?;
        if state.metadata_change != MetadataChangePhase::Open {
            return Err(CheckpointCancelReason::TableMetadataChanging);
        }
        assert!(
            !state.root_mutation_active,
            "concurrent table checkpoint root mutation is not supported"
        );
        state.root_mutation_active = true;
        state.debug_assert_valid("begin checkpoint root mutation exit");
        Ok(TableCheckpointRootMutationLease { lifecycle: self })
    }

    async fn wait_for_publish_gate_closed(&self) {
        loop {
            {
                let state = self.state.lock();
                state.debug_assert_valid("wait for publish gate closed check");
                if state.publish == PublishGateState::Closed {
                    break;
                }
            }
            listener!(self.changed => listener);
            {
                let state = self.state.lock();
                state.debug_assert_valid("wait for publish gate closed listen check");
                if state.publish == PublishGateState::Closed {
                    break;
                }
            }
            listener.await;
        }
    }

    #[inline]
    fn release_publish_lease(&self) {
        let mut state = self.state.lock();
        state.debug_assert_valid("release checkpoint publish lease enter");
        // Normal checkpoint publication reopens the gate. If drop observed this
        // lease and moved the gate to ClosingPublishing, this release is the
        // handoff point that permanently closes the gate and wakes the dropper.
        match state.publish {
            PublishGateState::Publishing => state.publish = PublishGateState::Open,
            PublishGateState::ClosingPublishing => state.publish = PublishGateState::Closed,
            publish => {
                debug_assert!(
                    matches!(
                        publish,
                        PublishGateState::Publishing | PublishGateState::ClosingPublishing
                    ),
                    "publish lease release in unexpected gate state: {:?}",
                    publish
                );
            }
        }
        state.debug_assert_valid("release checkpoint publish lease exit");
        drop(state);
        self.changed.notify(usize::MAX);
    }

    #[inline]
    fn release_metadata_change(&self) {
        let mut state = self.state.lock();
        state.debug_assert_valid("release metadata change enter");
        debug_assert_eq!(state.metadata_change, MetadataChangePhase::Active);
        state.metadata_change = MetadataChangePhase::Open;
        state.debug_assert_valid("release metadata change exit");
        drop(state);
        self.changed.notify(usize::MAX);
    }

    #[inline]
    fn release_checkpoint_root_mutation(&self) {
        let mut state = self.state.lock();
        state.debug_assert_valid("release checkpoint root mutation enter");
        debug_assert!(state.root_mutation_active);
        state.root_mutation_active = false;
        state.debug_assert_valid("release checkpoint root mutation exit");
        drop(state);
        self.changed.notify(usize::MAX);
    }

    #[inline]
    fn release_pending_metadata_change(&self) {
        let mut state = self.state.lock();
        state.debug_assert_valid("release pending metadata change enter");
        if state.metadata_change == MetadataChangePhase::Pending {
            state.metadata_change = MetadataChangePhase::Open;
            state.debug_assert_valid("release pending metadata change exit");
            drop(state);
            self.changed.notify(usize::MAX);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum MetadataChangePhase {
    #[default]
    Open,
    Pending,
    Active,
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

    const TABLE_ID: TableID = 42;

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
