use crate::catalog::TableID;
use crate::error::{Error, InternalError, OperationError, Result};
use error_stack::Report;
use event_listener::{Event, listener};
use std::sync::atomic::{AtomicU8, Ordering};

/// Normal checkpoint cancellation reasons for user-table checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointCancelReason {
    /// Checkpoint reached publication after a table drop gate had started.
    TableDropping,
    /// Checkpoint reached publication after the table had already been dropped.
    TableDropped,
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

impl TableLifecycleState {
    #[inline]
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Live,
            1 => Self::Dropping,
            2 => Self::Dropped,
            _ => unreachable!("invalid table lifecycle state"),
        }
    }

    #[inline]
    const fn as_raw(self) -> u8 {
        self as u8
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishGateState {
    Open = 0,
    Publishing = 1,
    ClosingPublishing = 2,
    Closed = 3,
}

impl PublishGateState {
    #[inline]
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Open,
            1 => Self::Publishing,
            2 => Self::ClosingPublishing,
            3 => Self::Closed,
            _ => unreachable!("invalid checkpoint publish gate state"),
        }
    }

    #[inline]
    const fn as_raw(self) -> u8 {
        self as u8
    }
}

/// Volatile lifecycle and checkpoint-publish gate for one user table.
pub(crate) struct TableLifecycle {
    state: AtomicU8,
    publish_gate: AtomicU8,
    publish_drained: Event,
}

impl TableLifecycle {
    /// Create a live lifecycle gate.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(TableLifecycleState::Live.as_raw()),
            publish_gate: AtomicU8::new(PublishGateState::Open.as_raw()),
            publish_drained: Event::new(),
        }
    }

    /// Returns the current externally visible lifecycle state.
    #[inline]
    pub(crate) fn state(&self) -> TableLifecycleState {
        TableLifecycleState::from_raw(self.state.load(Ordering::Acquire))
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
    /// This closes the checkpoint publish gate first, waits for any already-held
    /// publish lease to drain, and only then makes `Dropping` visible.
    #[allow(dead_code)]
    pub(crate) async fn begin_drop(&self, table_id: TableID) -> Result<()> {
        match self.state() {
            TableLifecycleState::Live => {}
            state => return Err(drop_not_live_err(table_id, "begin drop", state)),
        }

        loop {
            match self.publish_gate_state() {
                PublishGateState::Open => {
                    if self
                        .publish_gate
                        .compare_exchange(
                            PublishGateState::Open.as_raw(),
                            PublishGateState::Closed.as_raw(),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                PublishGateState::Publishing => {
                    if self
                        .publish_gate
                        .compare_exchange(
                            PublishGateState::Publishing.as_raw(),
                            PublishGateState::ClosingPublishing.as_raw(),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.wait_for_publish_gate_closed().await;
                        break;
                    }
                }
                PublishGateState::ClosingPublishing | PublishGateState::Closed => {
                    return Err(drop_not_live_err(
                        table_id,
                        "begin drop",
                        TableLifecycleState::Dropping,
                    ));
                }
            }
        }

        self.state
            .compare_exchange(
                TableLifecycleState::Live.as_raw(),
                TableLifecycleState::Dropping.as_raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|raw| {
                drop_not_live_err(table_id, "begin drop", TableLifecycleState::from_raw(raw))
            })
    }

    /// Marks a dropping table handle as fully dropped.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn mark_dropped(&self, table_id: TableID) -> Result<()> {
        self.state
            .compare_exchange(
                TableLifecycleState::Dropping.as_raw(),
                TableLifecycleState::Dropped.as_raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|raw| mark_dropped_err(table_id, TableLifecycleState::from_raw(raw)))
    }

    /// Attempts to enter the no-cancel checkpoint publish section.
    pub(crate) fn try_begin_checkpoint_publish(
        &self,
    ) -> std::result::Result<CheckpointPublishLease<'_>, CheckpointCancelReason> {
        loop {
            self.check_lifecycle_for_checkpoint_publish()?;
            match self.publish_gate_state() {
                PublishGateState::Open => {
                    if self
                        .publish_gate
                        .compare_exchange(
                            PublishGateState::Open.as_raw(),
                            PublishGateState::Publishing.as_raw(),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    match self.check_lifecycle_for_checkpoint_publish() {
                        Ok(()) => return Ok(CheckpointPublishLease { lifecycle: self }),
                        Err(reason) => {
                            self.release_publish_lease();
                            return Err(reason);
                        }
                    }
                }
                PublishGateState::Publishing => {
                    debug_assert_ne!(
                        self.publish_gate_state(),
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

    #[inline]
    fn check_lifecycle_for_checkpoint_publish(
        &self,
    ) -> std::result::Result<(), CheckpointCancelReason> {
        match self.state() {
            TableLifecycleState::Live => Ok(()),
            TableLifecycleState::Dropping => Err(CheckpointCancelReason::TableDropping),
            TableLifecycleState::Dropped => Err(CheckpointCancelReason::TableDropped),
        }
    }

    #[inline]
    fn publish_gate_state(&self) -> PublishGateState {
        PublishGateState::from_raw(self.publish_gate.load(Ordering::Acquire))
    }

    async fn wait_for_publish_gate_closed(&self) {
        loop {
            if self.publish_gate_state() == PublishGateState::Closed {
                break;
            }
            listener!(self.publish_drained => listener);
            if self.publish_gate_state() == PublishGateState::Closed {
                break;
            }
            listener.await;
        }
    }

    #[inline]
    fn release_publish_lease(&self) {
        // Normal checkpoint publication reopens the gate. If drop observed this
        // lease and moved the gate to ClosingPublishing, this release is the
        // handoff point that permanently closes the gate and wakes the dropper.
        match self.publish_gate.compare_exchange(
            PublishGateState::Publishing.as_raw(),
            PublishGateState::Open.as_raw(),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(raw) if PublishGateState::from_raw(raw) == PublishGateState::ClosingPublishing => {
                let released = self
                    .publish_gate
                    .compare_exchange(
                        PublishGateState::ClosingPublishing.as_raw(),
                        PublishGateState::Closed.as_raw(),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok();
                debug_assert!(released, "publish lease gate changed while releasing");
                self.publish_drained.notify(usize::MAX);
            }
            Err(raw) => {
                let state = PublishGateState::from_raw(raw);
                debug_assert!(
                    matches!(
                        state,
                        PublishGateState::Publishing | PublishGateState::ClosingPublishing
                    ),
                    "publish lease release in unexpected gate state: {:?}",
                    state
                );
            }
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
#[allow(dead_code)]
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
#[allow(dead_code)]
fn mark_dropped_err(table_id: TableID, state: TableLifecycleState) -> Error {
    Report::new(InternalError::Generic)
        .attach(format!(
            "mark dropped requires Dropping state: table_id={table_id}, lifecycle_state={state:?}"
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
    fn test_drop_gate_waits_for_active_publish_lease_before_dropping() {
        smol::block_on(async {
            let lifecycle = std::sync::Arc::new(TableLifecycle::new());
            let lease = lifecycle.try_begin_checkpoint_publish().unwrap();
            let mut drop_fut = Box::pin(lifecycle.begin_drop(TABLE_ID));

            assert!(matches!(
                futures::poll!(drop_fut.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(lifecycle.state(), TableLifecycleState::Live);
            match lifecycle.try_begin_checkpoint_publish() {
                Ok(_lease) => panic!("publish lease should be blocked by drop gate"),
                Err(reason) => assert_eq!(reason, CheckpointCancelReason::TableDropping),
            }

            drop(lease);
            drop_fut.await.unwrap();
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
}
