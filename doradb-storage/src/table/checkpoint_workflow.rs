use super::CheckpointCancelReason;
use super::deletion_buffer::DeleteMarker;
use super::lifecycle::{CheckpointPublishLease, TableLifecycle, TableTerminal};
use crate::error::FatalError;
use crate::id::{PageID, RowID, TableID, TrxID};
use crate::trx::sys::TransactionSystem;
use parking_lot::Mutex;
use std::fmt;
use std::mem::replace;
use std::result::Result as StdResult;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FrozenPage {
    pub(super) page_id: PageID,
    pub(super) start_row_id: RowID,
    pub(super) end_row_id: RowID,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FrozenPageValidationState {
    Unchecked,
    Blocked { required_cutoff_ts: Option<TrxID> },
    Stable { required_cutoff_ts: Option<TrxID> },
}

#[derive(Clone)]
pub(super) struct PreparedTransitionPage {
    pub(super) page_id: PageID,
    pub(super) start_row_id: RowID,
    pub(super) end_row_id: RowID,
    pub(super) cutoff_ts: TrxID,
    pub(super) observed_version: u64,
    pub(super) required_cutoff_ts: Option<TrxID>,
    pub(super) del_bitmap: Vec<u64>,
    pub(super) overlay_markers: Vec<(RowID, DeleteMarker)>,
}

impl PreparedTransitionPage {
    #[inline]
    pub(super) fn matches(&self, page: FrozenPage, cutoff_ts: TrxID, version: u64) -> bool {
        self.page_id == page.page_id
            && self.start_row_id == page.start_row_id
            && self.end_row_id == page.end_row_id
            && self.cutoff_ts == cutoff_ts
            && self.observed_version == version
    }
}

impl fmt::Debug for PreparedTransitionPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedTransitionPage")
            .field("page_id", &self.page_id)
            .field("start_row_id", &self.start_row_id)
            .field("end_row_id", &self.end_row_id)
            .field("cutoff_ts", &self.cutoff_ts)
            .field("observed_version", &self.observed_version)
            .field("required_cutoff_ts", &self.required_cutoff_ts)
            .field("del_bitmap", &self.del_bitmap)
            .field("overlay_marker_count", &self.overlay_markers.len())
            .finish()
    }
}

#[derive(Debug)]
pub(super) struct FrozenPageBatch {
    pub(super) table_id: TableID,
    pub(super) frozen_ts: TrxID,
    pub(super) heap_redo_start_ts: Option<TrxID>,
    pub(super) approximate_rows: usize,
    pub(super) pages: Vec<FrozenPage>,
    pub(super) validation: Vec<FrozenPageValidationState>,
    pub(super) prepared: Vec<Option<PreparedTransitionPage>>,
}

impl FrozenPageBatch {
    #[inline]
    pub(super) fn new(
        table_id: TableID,
        frozen_ts: TrxID,
        heap_redo_start_ts: Option<TrxID>,
        approximate_rows: usize,
        pages: Vec<FrozenPage>,
    ) -> Self {
        let validation = vec![FrozenPageValidationState::Unchecked; pages.len()];
        let mut prepared = Vec::with_capacity(pages.len());
        prepared.resize_with(pages.len(), || None);
        Self {
            table_id,
            frozen_ts,
            heap_redo_start_ts,
            approximate_rows,
            pages,
            validation,
            prepared,
        }
    }

    #[inline]
    pub(super) fn info(&self) -> FrozenPageBatchInfo {
        FrozenPageBatchInfo {
            table_id: self.table_id,
            frozen_ts: self.frozen_ts,
            approximate_rows: self.approximate_rows,
            page_count: self.pages.len(),
            stable_page_count: self
                .validation
                .iter()
                .filter(|state| matches!(state, FrozenPageValidationState::Stable { .. }))
                .count(),
        }
    }
}

/// Read-only summary of the canonical frozen-page batch owned by a table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrozenPageBatchInfo {
    table_id: TableID,
    frozen_ts: TrxID,
    approximate_rows: usize,
    page_count: usize,
    stable_page_count: usize,
}

impl FrozenPageBatchInfo {
    /// Returns the table selected by this batch.
    #[inline]
    pub fn table_id(&self) -> TableID {
        self.table_id
    }

    /// Returns the timestamp fence allocated after the pages were frozen.
    #[inline]
    pub fn frozen_ts(&self) -> TrxID {
        self.frozen_ts
    }

    /// Returns the approximate number of non-deleted rows selected.
    #[inline]
    pub fn approximate_rows(&self) -> usize {
        self.approximate_rows
    }

    /// Returns the number of selected row pages.
    #[inline]
    pub fn page_count(&self) -> usize {
        self.page_count
    }

    /// Returns whether the canonical batch contains no row pages.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.page_count == 0
    }

    /// Returns the number of pages whose undo chains no longer need rescanning.
    #[inline]
    pub fn stable_page_count(&self) -> usize {
        self.stable_page_count
    }
}

/// Result of attempting to freeze one table's hot row-page prefix.
#[must_use = "freeze outcome must be checked for cancellation or an existing frozen batch"]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FreezeOutcome {
    /// This call created and installed the table's canonical frozen batch.
    Frozen {
        /// Read-only summary of the installed batch.
        batch: FrozenPageBatchInfo,
    },
    /// The table already owned a frozen batch, which was left unchanged.
    AlreadyFrozen {
        /// Read-only summary of the existing canonical batch.
        batch: FrozenPageBatchInfo,
    },
    /// Freeze admission lost a normal maintenance or terminal lifecycle race.
    Cancelled {
        /// Reason the freeze attempt was not admitted.
        reason: CheckpointCancelReason,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CheckpointSource {
    Idle,
    Frozen,
}

#[derive(Debug)]
enum TableCheckpointWorkflowState {
    Idle,
    Freezing,
    Frozen(FrozenPageBatch),
    Checkpointing { source: CheckpointSource },
    Publishing,
    Transition,
    Closed,
}

enum CheckpointPublicationState {
    /// No publication admission is held; `CheckpointAttempt` still owns
    /// reversible workflow restoration.
    Initial,
    /// Row-page transition admission is held and any failure must poison
    /// storage because a Transition prefix may already be visible.
    Transition { reason: FatalError },
    /// Late publication admission is held, but no irreversible mutation has
    /// occurred, so dropping the guard restores the workflow.
    Publishing,
    /// Late publication crossed the irreversible mutation boundary and any
    /// failure must poison storage with the current reason.
    IrreversiblePublishing { reason: FatalError },
    /// Workflow publication completed successfully; the lifecycle lease stays
    /// held until the guard itself is dropped.
    Finished,
}

/// Owns checkpoint workflow, lifecycle, and fatal-failure publication state.
pub(super) struct CheckpointPublicationGuard<'table, 'trx> {
    workflow: &'table TableCheckpointWorkflow,
    lifecycle: &'table TableLifecycle,
    trx_sys: &'trx TransactionSystem,
    publish_lease: Option<CheckpointPublishLease<'table>>,
    state: CheckpointPublicationState,
}

impl<'table, 'trx> CheckpointPublicationGuard<'table, 'trx> {
    #[inline]
    pub(super) fn new(
        workflow: &'table TableCheckpointWorkflow,
        lifecycle: &'table TableLifecycle,
        trx_sys: &'trx TransactionSystem,
    ) -> Self {
        Self {
            workflow,
            lifecycle,
            trx_sys,
            publish_lease: None,
            state: CheckpointPublicationState::Initial,
        }
    }

    pub(super) fn begin_transition(&mut self) -> StdResult<(), CheckpointCancelReason> {
        if !matches!(self.state, CheckpointPublicationState::Initial) {
            panic!("page transition requires an initial publication guard")
        }
        let lease = self.workflow.try_begin_transition(self.lifecycle)?;
        self.publish_lease = Some(lease);
        self.state = CheckpointPublicationState::Transition {
            reason: FatalError::CheckpointWrite,
        };
        Ok(())
    }

    /// Ensures final publication admission and reports whether this call
    /// acquired it. An existing Transition admission already owns the gate.
    pub(super) fn begin_publishing(
        &mut self,
        source: CheckpointSource,
    ) -> StdResult<bool, CheckpointCancelReason> {
        if matches!(self.state, CheckpointPublicationState::Transition { .. }) {
            return Ok(false);
        }
        if !matches!(self.state, CheckpointPublicationState::Initial) {
            panic!("final publication requires an initial or transition guard")
        }
        let lease = self.workflow.try_begin_publishing(self.lifecycle, source)?;
        self.publish_lease = Some(lease);
        self.state = CheckpointPublicationState::Publishing;
        Ok(true)
    }

    #[inline]
    pub(super) fn make_irreversible(&mut self, reason: FatalError) {
        if matches!(self.state, CheckpointPublicationState::Publishing) {
            self.state = CheckpointPublicationState::IrreversiblePublishing { reason };
            return;
        }
        match &mut self.state {
            CheckpointPublicationState::Transition {
                reason: current, ..
            } => *current = reason,
            CheckpointPublicationState::Initial => {
                panic!("initial checkpoint publication cannot become irreversible")
            }
            CheckpointPublicationState::Publishing => unreachable!(),
            CheckpointPublicationState::IrreversiblePublishing { .. } => {
                panic!("checkpoint publication is already irreversible")
            }
            CheckpointPublicationState::Finished => {
                panic!("finished checkpoint publication cannot become irreversible")
            }
        }
    }

    #[inline]
    pub(super) fn set_failure_reason(&mut self, reason: FatalError) {
        match &mut self.state {
            CheckpointPublicationState::Transition {
                reason: current, ..
            }
            | CheckpointPublicationState::IrreversiblePublishing {
                reason: current, ..
            } => *current = reason,
            CheckpointPublicationState::Initial
            | CheckpointPublicationState::Publishing
            | CheckpointPublicationState::Finished => {
                panic!("checkpoint publication failure reason requires irreversible state")
            }
        }
    }

    #[inline]
    pub(super) fn finish(&mut self) {
        match self.state {
            CheckpointPublicationState::Transition { .. }
            | CheckpointPublicationState::Publishing
            | CheckpointPublicationState::IrreversiblePublishing { .. } => {}
            CheckpointPublicationState::Initial => {
                panic!("initial checkpoint publication cannot finish")
            }
            CheckpointPublicationState::Finished => {
                panic!("checkpoint publication already finished")
            }
        }
        self.workflow.finish_publication();
        self.state = CheckpointPublicationState::Finished;
    }
}

impl Drop for CheckpointPublicationGuard<'_, '_> {
    #[inline]
    fn drop(&mut self) {
        debug_assert_eq!(
            self.publish_lease.is_some(),
            !matches!(self.state, CheckpointPublicationState::Initial)
        );
        match &self.state {
            CheckpointPublicationState::Initial | CheckpointPublicationState::Finished => {}
            CheckpointPublicationState::Publishing => self.workflow.finish_publication(),
            CheckpointPublicationState::Transition { reason }
            | CheckpointPublicationState::IrreversiblePublishing { reason } => {
                let _ = self.trx_sys.poison_engine(*reason);
            }
        }
    }
}

/// Volatile ownership state for one table's freeze-to-checkpoint workflow.
pub(super) struct TableCheckpointWorkflow {
    state: Mutex<TableCheckpointWorkflowState>,
}

impl TableCheckpointWorkflow {
    #[inline]
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(TableCheckpointWorkflowState::Idle),
        }
    }

    pub(super) fn begin_freeze<'a>(
        &'a self,
        lifecycle: &TableLifecycle,
    ) -> StdResult<FreezeAttempt<'a>, FreezeOutcome> {
        let mut state = self.state.lock();
        if let Err(reason) = checkpoint_lifecycle(lifecycle.inspect_terminal()) {
            return Err(FreezeOutcome::Cancelled { reason });
        }
        match &*state {
            TableCheckpointWorkflowState::Idle => {
                *state = TableCheckpointWorkflowState::Freezing;
                Ok(FreezeAttempt {
                    workflow: self,
                    restore_idle: true,
                })
            }
            TableCheckpointWorkflowState::Freezing => Err(FreezeOutcome::Cancelled {
                reason: CheckpointCancelReason::FreezeInProgress,
            }),
            TableCheckpointWorkflowState::Frozen(batch) => Err(FreezeOutcome::AlreadyFrozen {
                batch: batch.info(),
            }),
            TableCheckpointWorkflowState::Checkpointing { .. }
            | TableCheckpointWorkflowState::Publishing
            | TableCheckpointWorkflowState::Transition => Err(FreezeOutcome::Cancelled {
                reason: CheckpointCancelReason::CheckpointInProgress,
            }),
            TableCheckpointWorkflowState::Closed => Err(FreezeOutcome::Cancelled {
                reason: terminal_checkpoint_cancel(lifecycle.inspect_terminal()),
            }),
        }
    }

    pub(super) fn begin_checkpoint<'a>(
        &'a self,
        lifecycle: &TableLifecycle,
    ) -> StdResult<CheckpointAttempt<'a>, CheckpointCancelReason> {
        let mut state = self.state.lock();
        checkpoint_lifecycle(lifecycle.inspect_terminal())?;
        let (source, batch) = match &*state {
            TableCheckpointWorkflowState::Idle => (CheckpointSource::Idle, None),
            TableCheckpointWorkflowState::Freezing => {
                return Err(CheckpointCancelReason::FreezeInProgress);
            }
            TableCheckpointWorkflowState::Frozen(_) => {
                let TableCheckpointWorkflowState::Frozen(batch) =
                    replace(&mut *state, TableCheckpointWorkflowState::Idle)
                else {
                    unreachable!("frozen workflow state changed while locked")
                };
                (CheckpointSource::Frozen, Some(batch))
            }
            TableCheckpointWorkflowState::Checkpointing { .. }
            | TableCheckpointWorkflowState::Publishing
            | TableCheckpointWorkflowState::Transition => {
                return Err(CheckpointCancelReason::CheckpointInProgress);
            }
            TableCheckpointWorkflowState::Closed => {
                return Err(terminal_checkpoint_cancel(lifecycle.inspect_terminal()));
            }
        };
        *state = TableCheckpointWorkflowState::Checkpointing { source };
        Ok(CheckpointAttempt {
            workflow: self,
            source,
            batch,
        })
    }

    pub(super) fn try_begin_transition<'a>(
        &self,
        lifecycle: &'a TableLifecycle,
    ) -> StdResult<CheckpointPublishLease<'a>, CheckpointCancelReason> {
        let mut state = self.state.lock();
        match *state {
            TableCheckpointWorkflowState::Checkpointing {
                source: CheckpointSource::Frozen,
            } => {}
            TableCheckpointWorkflowState::Closed => {
                return Err(terminal_checkpoint_cancel(lifecycle.inspect_terminal()));
            }
            _ => panic!("row-page transition requires a frozen checkpoint attempt"),
        }
        let lease = lifecycle.try_begin_checkpoint_publish()?;
        *state = TableCheckpointWorkflowState::Transition;
        Ok(lease)
    }

    pub(super) fn try_begin_publishing<'a>(
        &self,
        lifecycle: &'a TableLifecycle,
        source: CheckpointSource,
    ) -> StdResult<CheckpointPublishLease<'a>, CheckpointCancelReason> {
        let mut state = self.state.lock();
        match *state {
            TableCheckpointWorkflowState::Checkpointing { source: current }
                if current == source => {}
            TableCheckpointWorkflowState::Closed => {
                return Err(terminal_checkpoint_cancel(lifecycle.inspect_terminal()));
            }
            _ => panic!("publication requires the admitted checkpoint attempt"),
        }
        let lease = lifecycle.try_begin_checkpoint_publish()?;
        *state = TableCheckpointWorkflowState::Publishing;
        Ok(lease)
    }

    #[inline]
    pub(super) fn finish_publication(&self) {
        let mut state = self.state.lock();
        match *state {
            TableCheckpointWorkflowState::Publishing | TableCheckpointWorkflowState::Transition => {
                *state = TableCheckpointWorkflowState::Idle;
            }
            TableCheckpointWorkflowState::Closed => {}
            _ => panic!("checkpoint publication finished from invalid workflow state"),
        }
    }

    #[inline]
    pub(super) fn close(&self) {
        *self.state.lock() = TableCheckpointWorkflowState::Closed;
    }

    #[inline]
    pub(super) fn close_offline(&self) {
        let mut state = self.state.lock();
        assert!(
            matches!(*state, TableCheckpointWorkflowState::Idle),
            "offline workflow closure requires idle state"
        );
        *state = TableCheckpointWorkflowState::Closed;
    }

    #[inline]
    pub(super) fn assert_closed(&self) {
        assert!(
            matches!(*self.state.lock(), TableCheckpointWorkflowState::Closed),
            "table runtime destruction requires closed checkpoint workflow"
        );
    }

    #[cfg(test)]
    pub(super) fn frozen_page_ids(&self) -> Option<Vec<PageID>> {
        match &*self.state.lock() {
            TableCheckpointWorkflowState::Frozen(batch) => {
                Some(batch.pages.iter().map(|page| page.page_id).collect())
            }
            TableCheckpointWorkflowState::Idle
            | TableCheckpointWorkflowState::Freezing
            | TableCheckpointWorkflowState::Checkpointing { .. }
            | TableCheckpointWorkflowState::Publishing
            | TableCheckpointWorkflowState::Transition
            | TableCheckpointWorkflowState::Closed => None,
        }
    }

    #[cfg(test)]
    pub(super) fn prepared_page_ids(&self) -> Option<Vec<PageID>> {
        match &*self.state.lock() {
            TableCheckpointWorkflowState::Frozen(batch) => Some(
                batch
                    .prepared
                    .iter()
                    .filter_map(|plan| plan.as_ref().map(|plan| plan.page_id))
                    .collect(),
            ),
            TableCheckpointWorkflowState::Idle
            | TableCheckpointWorkflowState::Freezing
            | TableCheckpointWorkflowState::Checkpointing { .. }
            | TableCheckpointWorkflowState::Publishing
            | TableCheckpointWorkflowState::Transition
            | TableCheckpointWorkflowState::Closed => None,
        }
    }

    #[cfg(test)]
    pub(super) fn frozen_page_validation(
        &self,
    ) -> Option<Vec<(PageID, FrozenPageValidationState)>> {
        match &*self.state.lock() {
            TableCheckpointWorkflowState::Frozen(batch) => Some(
                batch
                    .pages
                    .iter()
                    .zip(&batch.validation)
                    .map(|(page, validation)| (page.page_id, *validation))
                    .collect(),
            ),
            TableCheckpointWorkflowState::Idle
            | TableCheckpointWorkflowState::Freezing
            | TableCheckpointWorkflowState::Checkpointing { .. }
            | TableCheckpointWorkflowState::Publishing
            | TableCheckpointWorkflowState::Transition
            | TableCheckpointWorkflowState::Closed => None,
        }
    }

    #[cfg(test)]
    pub(super) fn state_name(&self) -> &'static str {
        match &*self.state.lock() {
            TableCheckpointWorkflowState::Idle => "Idle",
            TableCheckpointWorkflowState::Freezing => "Freezing",
            TableCheckpointWorkflowState::Frozen(_) => "Frozen",
            TableCheckpointWorkflowState::Checkpointing { .. } => "Checkpointing",
            TableCheckpointWorkflowState::Publishing => "Publishing",
            TableCheckpointWorkflowState::Transition => "Transition",
            TableCheckpointWorkflowState::Closed => "Closed",
        }
    }
}

pub(super) struct FreezeAttempt<'a> {
    workflow: &'a TableCheckpointWorkflow,
    restore_idle: bool,
}

impl FreezeAttempt<'_> {
    #[inline]
    pub(super) fn begin_page_publication(&mut self) -> bool {
        let state = self.workflow.state.lock();
        match *state {
            TableCheckpointWorkflowState::Freezing => {
                self.restore_idle = false;
                true
            }
            TableCheckpointWorkflowState::Closed => {
                self.restore_idle = false;
                false
            }
            _ => panic!("freeze publication requires Freezing workflow state"),
        }
    }

    #[inline]
    pub(super) fn cancelled(mut self, lifecycle: &TableLifecycle) -> FreezeOutcome {
        self.restore_idle = false;
        FreezeOutcome::Cancelled {
            reason: terminal_checkpoint_cancel(lifecycle.inspect_terminal()),
        }
    }

    pub(super) fn finish(
        mut self,
        batch: FrozenPageBatch,
        lifecycle: &TableLifecycle,
    ) -> FreezeOutcome {
        let info = batch.info();
        let mut state = self.workflow.state.lock();
        let outcome = if let Err(reason) = checkpoint_lifecycle(lifecycle.inspect_terminal()) {
            *state = TableCheckpointWorkflowState::Closed;
            FreezeOutcome::Cancelled { reason }
        } else {
            assert!(
                matches!(*state, TableCheckpointWorkflowState::Freezing),
                "freeze completion requires Freezing workflow state"
            );
            *state = TableCheckpointWorkflowState::Frozen(batch);
            FreezeOutcome::Frozen { batch: info }
        };
        self.restore_idle = false;
        outcome
    }
}

impl Drop for FreezeAttempt<'_> {
    #[inline]
    fn drop(&mut self) {
        if !self.restore_idle {
            return;
        }
        let mut state = self.workflow.state.lock();
        if matches!(*state, TableCheckpointWorkflowState::Freezing) {
            *state = TableCheckpointWorkflowState::Idle;
        }
    }
}

pub(super) struct CheckpointAttempt<'a> {
    workflow: &'a TableCheckpointWorkflow,
    source: CheckpointSource,
    batch: Option<FrozenPageBatch>,
}

impl CheckpointAttempt<'_> {
    #[inline]
    pub(super) fn source(&self) -> CheckpointSource {
        self.source
    }

    #[inline]
    pub(super) fn batch(&self) -> Option<&FrozenPageBatch> {
        self.batch.as_ref()
    }

    #[inline]
    pub(super) fn batch_mut(&mut self) -> Option<&mut FrozenPageBatch> {
        self.batch.as_mut()
    }
}

impl Drop for CheckpointAttempt<'_> {
    fn drop(&mut self) {
        let mut state = self.workflow.state.lock();
        if !matches!(
            *state,
            TableCheckpointWorkflowState::Checkpointing { source }
                if source == self.source
        ) {
            return;
        }
        match self.source {
            CheckpointSource::Idle => {
                debug_assert!(self.batch.is_none());
                *state = TableCheckpointWorkflowState::Idle;
            }
            CheckpointSource::Frozen => {
                let Some(batch) = self.batch.take() else {
                    panic!("frozen checkpoint attempt must retain its batch")
                };
                *state = TableCheckpointWorkflowState::Frozen(batch);
            }
        }
    }
}

#[inline]
fn checkpoint_lifecycle(terminal: TableTerminal) -> StdResult<(), CheckpointCancelReason> {
    match terminal {
        TableTerminal::Live => Ok(()),
        TableTerminal::Dropping => Err(CheckpointCancelReason::TableDropping),
        TableTerminal::Dropped => Err(CheckpointCancelReason::TableDropped),
    }
}

#[inline]
fn terminal_checkpoint_cancel(terminal: TableTerminal) -> CheckpointCancelReason {
    match terminal {
        TableTerminal::Live | TableTerminal::Dropping => CheckpointCancelReason::TableDropping,
        TableTerminal::Dropped => CheckpointCancelReason::TableDropped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE_ID: TableID = TableID::new(42);

    fn one_page_batch(frozen_ts: TrxID) -> FrozenPageBatch {
        FrozenPageBatch::new(
            TABLE_ID,
            frozen_ts,
            None,
            7,
            vec![FrozenPage {
                page_id: PageID::new(3),
                start_row_id: RowID::new(10),
                end_row_id: RowID::new(20),
            }],
        )
    }

    #[test]
    fn test_prepared_transition_page_matches_identity_cutoff_and_full_version() {
        let page = FrozenPage {
            page_id: PageID::new(3),
            start_row_id: RowID::new(10),
            end_row_id: RowID::new(20),
        };
        let plan = PreparedTransitionPage {
            page_id: page.page_id,
            start_row_id: page.start_row_id,
            end_row_id: page.end_row_id,
            cutoff_ts: TrxID::new(100),
            observed_version: 8,
            required_cutoff_ts: Some(TrxID::new(90)),
            del_bitmap: vec![],
            overlay_markers: vec![],
        };

        assert!(plan.matches(page, TrxID::new(100), 8));
        assert!(!plan.matches(page, TrxID::new(101), 8));
        assert!(!plan.matches(page, TrxID::new(100), 10));
        assert!(!plan.matches(
            FrozenPage {
                page_id: PageID::new(4),
                ..page
            },
            TrxID::new(100),
            8,
        ));
        assert!(!plan.matches(
            FrozenPage {
                end_row_id: RowID::new(21),
                ..page
            },
            TrxID::new(100),
            8,
        ));
    }

    #[test]
    fn test_repeated_freeze_keeps_original_batch_and_validation_cache() {
        let lifecycle = TableLifecycle::new();
        let workflow = TableCheckpointWorkflow::new();
        let attempt = workflow.begin_freeze(&lifecycle).unwrap();
        let frozen_ts = TrxID::new(101);
        let outcome = attempt.finish(one_page_batch(frozen_ts), &lifecycle);
        let FreezeOutcome::Frozen { batch } = outcome else {
            panic!("first freeze should install a batch: {outcome:?}");
        };
        assert_eq!(batch.table_id(), TABLE_ID);
        assert_eq!(batch.frozen_ts(), frozen_ts);
        assert_eq!(batch.approximate_rows(), 7);
        assert_eq!(batch.page_count(), 1);
        assert_eq!(batch.stable_page_count(), 0);

        let mut checkpoint = workflow.begin_checkpoint(&lifecycle).unwrap();
        checkpoint.batch_mut().unwrap().validation[0] = FrozenPageValidationState::Stable {
            required_cutoff_ts: Some(TrxID::new(88)),
        };
        drop(checkpoint);

        let repeated = workflow.begin_freeze(&lifecycle).err().unwrap();
        let FreezeOutcome::AlreadyFrozen { batch } = repeated else {
            panic!("repeated freeze should return the canonical batch: {repeated:?}");
        };
        assert_eq!(batch.frozen_ts(), frozen_ts);
        assert_eq!(batch.approximate_rows(), 7);
        assert_eq!(batch.page_count(), 1);
        assert_eq!(batch.stable_page_count(), 1);
        assert_eq!(workflow.state_name(), "Frozen");
    }

    #[test]
    fn test_reversible_attempt_guards_restore_admitted_state() {
        let lifecycle = TableLifecycle::new();
        let workflow = TableCheckpointWorkflow::new();

        let freeze = workflow.begin_freeze(&lifecycle).unwrap();
        assert_eq!(workflow.state_name(), "Freezing");
        assert_eq!(
            workflow.begin_checkpoint(&lifecycle).err().unwrap(),
            CheckpointCancelReason::FreezeInProgress
        );
        let repeated = workflow.begin_freeze(&lifecycle).err().unwrap();
        assert_eq!(
            repeated,
            FreezeOutcome::Cancelled {
                reason: CheckpointCancelReason::FreezeInProgress,
            }
        );
        drop(freeze);
        assert_eq!(workflow.state_name(), "Idle");

        let checkpoint = workflow.begin_checkpoint(&lifecycle).unwrap();
        let freeze = workflow.begin_freeze(&lifecycle).err().unwrap();
        assert_eq!(
            freeze,
            FreezeOutcome::Cancelled {
                reason: CheckpointCancelReason::CheckpointInProgress,
            }
        );
        drop(checkpoint);
        assert_eq!(workflow.state_name(), "Idle");
    }

    #[test]
    fn test_terminal_close_prevents_attempt_state_resurrection() {
        let lifecycle = TableLifecycle::new();
        let workflow = TableCheckpointWorkflow::new();
        let checkpoint = workflow.begin_checkpoint(&lifecycle).unwrap();

        let drain = lifecycle.start_drop(TABLE_ID).unwrap();
        workflow.close();
        smol::block_on(drain.wait());
        drop(checkpoint);
        assert_eq!(workflow.state_name(), "Closed");
        assert_eq!(
            workflow.begin_checkpoint(&lifecycle).err().unwrap(),
            CheckpointCancelReason::TableDropping
        );
        assert_eq!(
            workflow.begin_freeze(&lifecycle).err().unwrap(),
            FreezeOutcome::Cancelled {
                reason: CheckpointCancelReason::TableDropping,
            }
        );
    }

    #[test]
    fn test_publish_states_reject_concurrent_freeze_and_checkpoint() {
        fn assert_checkpoint_conflicts(
            workflow: &TableCheckpointWorkflow,
            lifecycle: &TableLifecycle,
        ) {
            assert_eq!(
                workflow.begin_checkpoint(lifecycle).err().unwrap(),
                CheckpointCancelReason::CheckpointInProgress
            );
            assert_eq!(
                workflow.begin_freeze(lifecycle).err().unwrap(),
                FreezeOutcome::Cancelled {
                    reason: CheckpointCancelReason::CheckpointInProgress,
                }
            );
        }

        let lifecycle = TableLifecycle::new();
        let workflow = TableCheckpointWorkflow::new();

        let publishing_attempt = workflow.begin_checkpoint(&lifecycle).unwrap();
        let publishing_root = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
        let publishing_lease = workflow
            .try_begin_publishing(&lifecycle, publishing_attempt.source())
            .unwrap();
        assert_eq!(workflow.state_name(), "Publishing");
        assert_checkpoint_conflicts(&workflow, &lifecycle);
        workflow.finish_publication();
        drop(publishing_lease);
        drop(publishing_root);
        drop(publishing_attempt);

        let freeze = workflow.begin_freeze(&lifecycle).unwrap();
        assert!(matches!(
            freeze.finish(one_page_batch(TrxID::new(102)), &lifecycle),
            FreezeOutcome::Frozen { .. }
        ));
        let transition_attempt = workflow.begin_checkpoint(&lifecycle).unwrap();
        let transition_root = lifecycle.try_begin_checkpoint_root_mutation().unwrap();
        let transition_lease = workflow.try_begin_transition(&lifecycle).unwrap();
        assert_eq!(workflow.state_name(), "Transition");
        assert_checkpoint_conflicts(&workflow, &lifecycle);
        workflow.finish_publication();
        drop(transition_lease);
        drop(transition_root);
        drop(transition_attempt);
        assert_eq!(workflow.state_name(), "Idle");
    }
}
