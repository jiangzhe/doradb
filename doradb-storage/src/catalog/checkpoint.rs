use crate::catalog::storage::tables::TABLE_ID_TABLES;
use crate::catalog::{
    Catalog, IndexDdlKind, IndexDdlRootProof, classify_index_ddl_root, is_catalog_table,
    is_user_table,
};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, FatalError, IoError, RuntimeError,
    RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeResult,
};
use crate::id::{TableID, TrxID};
use crate::log::discover_redo_log_files;
use crate::log::redo::{DDLRedo, RowRedoKind, TableDML};
use crate::obs;
use crate::recovery::stream::{CatalogSafeRedoSegment, RedoReplayPlanner};
use crate::trx::sys::{CatalogRedoRetentionProgress, TransactionSystem};
use error_stack::{Report, ResultExt};
use event_listener::{Event, listener};
use parking_lot::Mutex;
use std::collections::BTreeMap;

/// One catalog-row redo operation extracted from persisted logs.
pub(crate) struct CatalogRedoEntry {
    /// Catalog table that owns the row operation.
    pub(crate) table_id: TableID,
    /// Redo operation applied to the catalog row.
    pub(crate) kind: RowRedoKind,
}

/// Table DDL kinds that can block catalog checkpoint scan on ordering safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogCheckpointBlockingDDL {
    DropTable,
}

/// Stop reason for one catalog checkpoint scan batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogCheckpointScanStopReason {
    ReachedDurableUpper,
    BlockedByTableDDL {
        table_id: TableID,
        ddl: CatalogCheckpointBlockingDDL,
    },
}

/// Catalog checkpoint scan result consumed by apply phase.
pub(crate) struct CatalogCheckpointBatch {
    /// Catalog redo cursor at the start of this scan.
    pub(crate) replay_start_ts: TrxID,
    /// Highest commit timestamp safely covered by the batch.
    pub(crate) safe_cts: TrxID,
    /// First redo file sequence retained in the catalog marker for this scan.
    pub(crate) first_retained_file_seq: u32,
    /// Sealed redo segment summaries observed while planning the scan.
    pub(crate) sealed_redo_segments: Vec<CatalogSafeRedoSegment>,
    /// Catalog table row redo operations folded into the checkpoint.
    pub(crate) catalog_ops: Vec<CatalogRedoEntry>,
    /// Number of catalog DDL transactions included in the batch.
    pub(crate) catalog_ddl_txn_count: usize,
    /// Reason the scan stopped.
    pub(crate) stop_reason: CatalogCheckpointScanStopReason,
}

impl CatalogCheckpointBatch {
    /// Catalog replay boundary that would be published for this batch.
    #[inline]
    fn catalog_replay_start_ts_after_publish(&self) -> TrxID {
        self.safe_cts.saturating_add(1).max(self.replay_start_ts)
    }

    /// Build catalog-safe redo retention progress represented by this batch.
    #[inline]
    pub(crate) fn redo_retention_progress(&self) -> Option<CatalogRedoRetentionProgress> {
        // A scan that did not cover any durable record cannot prove a new
        // catalog replay boundary, so it must not refresh retention progress.
        if self.safe_cts < self.replay_start_ts {
            return None;
        }
        let catalog_replay_start_ts = self.catalog_replay_start_ts_after_publish();
        let segments = self
            .sealed_redo_segments
            .iter()
            .filter(|segment| match segment.redo_range {
                // Sealed empty files contain no catalog redo records, so they
                // are catalog-safe once the checkpoint publish succeeds.
                None => true,
                // Non-empty sealed files are catalog-safe only when their
                // entire redo CTS range is strictly below the published catalog
                // replay boundary. The file that contains the boundary remains
                // replay-relevant for catalog recovery.
                Some(range) => range.max_cts < catalog_replay_start_ts,
            })
            .cloned()
            .collect();
        Some(CatalogRedoRetentionProgress {
            // Keep the durable first-retained marker with the in-memory
            // summaries so later planning can verify the cache belongs to the
            // retained redo suffix it is reasoning about.
            first_retained_file_seq: self.first_retained_file_seq,
            catalog_replay_start_ts,
            segments,
        })
    }
}

/// Result of a catalog checkpoint maintenance operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogCheckpointOutcome {
    /// Catalog checkpoint metadata was durably published at this replay boundary.
    Published {
        /// New catalog replay start timestamp.
        catalog_replay_start_ts: TrxID,
    },
    /// The batch had no publishable work or was already superseded.
    Noop,
}

/// Configuration for scanning catalog checkpoint redo logs.
#[derive(Clone)]
pub(crate) struct CatalogCheckpointScanConfig {
    /// Redo log file prefix used to discover scan inputs.
    pub(crate) file_prefix: String,
    /// Maximum direct-IO read-ahead depth for redo scan input.
    pub(crate) read_ahead_depth: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum CatalogMetadataChangePhase {
    #[default]
    Open,
    Pending,
    Active,
}

#[derive(Debug, Default)]
struct CatalogCheckpointGateState {
    checkpoint_active: bool,
    metadata_change: CatalogMetadataChangePhase,
}

/// Catalog-local checkpoint, marker-publish, and metadata-DDL exclusion gate.
///
/// A catalog checkpoint scans redo and publishes catalog table roots that must
/// match a stable catalog metadata shape. Index DDL temporarily moves catalog
/// rows, user-table roots, and runtime layouts through different intermediate
/// states, so checkpoint scan/apply sections and catalog metadata-change
/// sections must not overlap.
///
/// Redo truncation also uses the checkpoint side of this gate while it plans
/// from the catalog snapshot and publishes the durable `first_redo_log_seq`
/// marker. The marker is bootstrap metadata in the `catalog.mtb` root, not an
/// ordinary catalog row: startup must read it before redo discovery so missing
/// prefix files below the marker can be accepted without replaying catalog redo
/// first.
///
/// Pending metadata changes reserve the next turn once an active checkpoint
/// drains. This prevents later checkpoints from repeatedly entering ahead of a
/// waiting DDL operation.
pub(crate) struct CatalogCheckpointGate {
    state: Mutex<CatalogCheckpointGateState>,
    changed: Event,
}

impl CatalogCheckpointGate {
    /// Create an open catalog checkpoint gate.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(CatalogCheckpointGateState::default()),
            changed: Event::new(),
        }
    }

    /// Acquire a catalog checkpoint/marker-publish lease.
    ///
    /// The lease waits until no catalog metadata change is active or pending,
    /// and also serializes overlapping catalog checkpoints or redo marker
    /// publishes. It does not protect the retained redo suffix itself; callers
    /// that scan retained redo, publish a marker, or unlink obsolete files must
    /// also hold the transaction-system redo-retention lease.
    pub(crate) async fn begin_checkpoint(&self) -> CatalogCheckpointLease<'_> {
        loop {
            {
                let mut state = self.state.lock();
                if state.metadata_change == CatalogMetadataChangePhase::Open
                    && !state.checkpoint_active
                {
                    state.checkpoint_active = true;
                    return CatalogCheckpointLease { gate: self };
                }
            }
            listener!(self.changed => listener);
            {
                let state = self.state.lock();
                if state.metadata_change == CatalogMetadataChangePhase::Open
                    && !state.checkpoint_active
                {
                    continue;
                }
            }
            listener.await;
        }
    }

    /// Acquire a catalog metadata-change lease for index DDL.
    ///
    /// When a checkpoint is active, the first metadata-change waiter records a
    /// pending reservation so it will run before subsequent checkpoints. Dropping
    /// a pending future before it becomes active reopens the gate.
    pub(crate) async fn begin_metadata_change(&self) -> CatalogMetadataChangeLease<'_> {
        let mut pending = None;
        loop {
            {
                let mut state = self.state.lock();
                match state.metadata_change {
                    CatalogMetadataChangePhase::Open if !state.checkpoint_active => {
                        // No checkpoint is running, so this metadata DDL can
                        // immediately exclude future catalog checkpoints.
                        state.metadata_change = CatalogMetadataChangePhase::Active;
                        return CatalogMetadataChangeLease { gate: self };
                    }
                    CatalogMetadataChangePhase::Open => {
                        // A checkpoint is already inside the gated section.
                        // Reserve the metadata change as pending so later
                        // checkpoints cannot enter ahead of it.
                        state.metadata_change = CatalogMetadataChangePhase::Pending;
                        pending = Some(PendingCatalogMetadataChange::new(self));
                    }
                    CatalogMetadataChangePhase::Pending
                        if pending.is_some() && !state.checkpoint_active =>
                    {
                        // This waiter owns the pending reservation, and the
                        // active checkpoint has drained. Promote the
                        // reservation into the exclusive metadata-change lease.
                        state.metadata_change = CatalogMetadataChangePhase::Active;
                        if let Some(pending) = &mut pending {
                            pending.disarm();
                        }
                        return CatalogMetadataChangeLease { gate: self };
                    }
                    CatalogMetadataChangePhase::Pending | CatalogMetadataChangePhase::Active => {
                        // Another metadata change is either already active or
                        // has the pending reservation; wait for the next state
                        // transition and retry.
                    }
                }
            }
            listener!(self.changed => listener);
            {
                let state = self.state.lock();
                let can_retry = match state.metadata_change {
                    CatalogMetadataChangePhase::Open => true,
                    CatalogMetadataChangePhase::Pending => {
                        pending.is_some() && !state.checkpoint_active
                    }
                    CatalogMetadataChangePhase::Active => false,
                };
                if can_retry {
                    continue;
                }
            }
            listener.await;
        }
    }

    #[inline]
    fn release_checkpoint(&self) {
        let mut state = self.state.lock();
        debug_assert!(state.checkpoint_active);
        state.checkpoint_active = false;
        drop(state);
        self.changed.notify(usize::MAX);
    }

    #[inline]
    fn release_metadata_change(&self) {
        let mut state = self.state.lock();
        debug_assert_eq!(state.metadata_change, CatalogMetadataChangePhase::Active);
        state.metadata_change = CatalogMetadataChangePhase::Open;
        drop(state);
        self.changed.notify(usize::MAX);
    }

    #[inline]
    fn release_pending_metadata_change(&self) {
        let mut state = self.state.lock();
        if state.metadata_change == CatalogMetadataChangePhase::Pending {
            state.metadata_change = CatalogMetadataChangePhase::Open;
            drop(state);
            self.changed.notify(usize::MAX);
        }
    }
}

struct PendingCatalogMetadataChange<'a> {
    gate: &'a CatalogCheckpointGate,
    armed: bool,
}

impl<'a> PendingCatalogMetadataChange<'a> {
    #[inline]
    fn new(gate: &'a CatalogCheckpointGate) -> Self {
        Self { gate, armed: true }
    }

    #[inline]
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingCatalogMetadataChange<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.armed {
            self.gate.release_pending_metadata_change();
        }
    }
}

/// RAII guard for one catalog checkpoint scan/apply section.
///
/// While held, other catalog checkpoints and catalog metadata DDL wait on the
/// catalog checkpoint gate.
pub(crate) struct CatalogCheckpointLease<'a> {
    gate: &'a CatalogCheckpointGate,
}

impl Drop for CatalogCheckpointLease<'_> {
    #[inline]
    fn drop(&mut self) {
        self.gate.release_checkpoint();
    }
}

/// RAII guard for one catalog metadata DDL section.
///
/// While held, catalog checkpoints wait so they cannot scan or publish against
/// partially updated catalog/table metadata.
pub(crate) struct CatalogMetadataChangeLease<'a> {
    gate: &'a CatalogCheckpointGate,
}

impl Drop for CatalogMetadataChangeLease<'_> {
    #[inline]
    fn drop(&mut self) {
        self.gate.release_metadata_change();
    }
}

impl Catalog {
    /// Trigger one ad-hoc catalog checkpoint publish.
    ///
    /// Normal overlap with another catalog checkpoint, catalog metadata DDL, or
    /// redo truncation waits through the catalog checkpoint gate and the
    /// transaction-system redo-retention gate.
    ///
    /// # Panics
    ///
    /// A panic indicates an internal invariant violation, such as bypassing the
    /// required gates and reaching the shared `CatalogStorage`/`MultiTableFile`
    /// with multiple mutable writers.
    #[inline]
    pub(crate) async fn checkpoint_now(
        &self,
        trx_sys: &TransactionSystem,
    ) -> RuntimeOrFatalResult<CatalogCheckpointOutcome> {
        obs::info!("event=checkpoint_publish component=catalog action=start result=ok");
        async {
            let _checkpoint_lease = self.checkpoint_gate.begin_checkpoint().await;
            let _redo_retention_lease = trx_sys.begin_redo_retention().await;
            let scan_cfg = trx_sys.catalog_checkpoint_scan_config()?;
            let batch = self
                .scan_checkpoint_batch(trx_sys.persisted_watermark_cts(), scan_cfg)
                .await?;
            let publishable_progress = batch.redo_retention_progress();
            match self.apply_checkpoint_batch(batch).await {
                Ok(CatalogCheckpointOutcome::Published {
                    catalog_replay_start_ts,
                }) => {
                    if let Some(progress) = publishable_progress {
                        debug_assert_eq!(progress.catalog_replay_start_ts, catalog_replay_start_ts);
                        trx_sys.record_catalog_redo_retention_progress(progress);
                    }
                    trx_sys.request_dropped_table_purge();
                    Ok(CatalogCheckpointOutcome::Published {
                        catalog_replay_start_ts,
                    })
                }
                Ok(CatalogCheckpointOutcome::Noop) => Ok(CatalogCheckpointOutcome::Noop),
                Err(err) => {
                    let has_io_source = match &err {
                        RuntimeOrFatalError::Runtime(report) => {
                            report.downcast_ref::<IoError>().is_some()
                        }
                        RuntimeOrFatalError::Fatal(report) => {
                            report.downcast_ref::<IoError>().is_some()
                        }
                    };
                    if !has_io_source {
                        return Err(err);
                    }
                    // Preserve the existing policy: any apply failure carrying
                    // an IO source is poisoned as a checkpoint-write failure.
                    // An already-Fatal source retains its original Fatal reason.
                    let report = err
                        .into_fatal_report(FatalError::CheckpointWrite)
                        .attach("catalog checkpoint publish IO failure");
                    Err(RuntimeOrFatalError::from(
                        self.poisoner.poison(report).into_report(),
                    ))
                }
            }
        }
        .await
        .inspect(|outcome| match outcome {
            CatalogCheckpointOutcome::Published {
                catalog_replay_start_ts,
            } => obs::info!(
                "event=checkpoint_publish component=catalog action=publish result=ok catalog_replay_start_ts={}",
                catalog_replay_start_ts
            ),
            CatalogCheckpointOutcome::Noop => obs::debug!(
                "event=checkpoint_publish component=catalog action=publish result=skipped reason=noop"
            ),
        })
        .inspect_err(|err| match err {
            RuntimeOrFatalError::Fatal(report) => obs::error!(
                "event=checkpoint_publish component=catalog action=poison result=error error={:?}",
                report
            ),
            RuntimeOrFatalError::Runtime(report) => obs::error!(
                "event=checkpoint_publish component=catalog action=publish result=error error={:?}",
                report
            ),
        })
    }

    /// Scan persisted redo logs and collect one safe catalog checkpoint batch.
    ///
    /// The caller owns selecting the durable upper CTS and base redo scan
    /// config. The retained redo marker and replay cursor come from the same
    /// catalog checkpoint snapshot.
    ///
    /// Scanned batches are intended for single-flight publish flow and must not
    /// be raced with other catalog checkpoint publishes against the same shared
    /// `CatalogStorage`/`MultiTableFile` writer.
    pub(crate) async fn scan_checkpoint_batch(
        &self,
        durable_upper_cts: TrxID,
        scan_cfg: CatalogCheckpointScanConfig,
    ) -> RuntimeResult<CatalogCheckpointBatch> {
        let snapshot = self.storage.checkpoint_snapshot();
        let first_retained_file_seq = snapshot.meta.first_redo_log_seq;
        let replay_start_ts = snapshot.catalog_replay_start_ts;
        // Start with an empty batch whose safe point is just before the
        // catalog replay cursor. The scan only advances `safe_cts` after a
        // redo record is proven safe to cover in this checkpoint.
        let mut batch = CatalogCheckpointBatch {
            replay_start_ts,
            safe_cts: replay_start_ts.saturating_sub(1),
            first_retained_file_seq,
            sealed_redo_segments: vec![],
            catalog_ops: vec![],
            catalog_ddl_txn_count: 0,
            stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
        };
        if durable_upper_cts < replay_start_ts {
            // Nothing durable has reached the catalog replay cursor yet, so
            // there are no log records this checkpoint can consume.
            return Ok(batch);
        }

        let logs = discover_redo_log_files(&scan_cfg.file_prefix, first_retained_file_seq, false)
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=scan_catalog_checkpoint, phase=discover_redo, replay_start_ts={replay_start_ts}"
                )
            })?;
        if logs.is_empty() {
            return Ok(batch);
        }
        let planner = RedoReplayPlanner::new(logs);
        let planned = planner
            .plan_catalog_scan(replay_start_ts, scan_cfg.read_ahead_depth)
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=scan_catalog_checkpoint, phase=plan_redo, replay_start_ts={replay_start_ts}"
                )
            })?;
        batch.sealed_redo_segments = planned.sealed_segments;
        let mut stream = planned.stream;

        while let Some(log) = stream
            .try_next()
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=scan_catalog_checkpoint, phase=read_redo, replay_start_ts={replay_start_ts}"
                )
            })?
        {
            let (header, redo) = log.into_inner();
            if header.cts < replay_start_ts {
                // Older records are already represented by the current
                // catalog storage checkpoint.
                continue;
            }
            if header.cts > durable_upper_cts {
                // The global durable watermark bounds this batch. Later
                // records may exist in logs but are not checkpoint-safe yet.
                break;
            }

            let Some(ddl) = redo.ddl.as_deref() else {
                // DML-only transactions do not participate in catalog
                // checkpoint state. Foreground commit enforces that catalog
                // table DML must carry a catalog metadata DDL marker.
                batch.safe_cts = header.cts;
                continue;
            };

            // Decide how this DDL transaction participates in the catalog
            // checkpoint before advancing the safe cursor. The decision covers
            // both table DDL ordering rules and index DDL root-proof rules.
            match self
                .catalog_checkpoint_txn_action(ddl, &redo.dml, header.cts)
                .change_context(RuntimeError::CatalogAccess)
                .attach_with(|| {
                    format!(
                        "operation=scan_catalog_checkpoint, phase=classify_ddl, commit_ts={}",
                        header.cts
                    )
                })?
            {
                CatalogCheckpointTxnAction::Include => {
                    // Reaching here means this transaction will never need to be
                    // replayed as catalog history before the next checkpoint cursor.
                    batch.safe_cts = header.cts;
                    batch.catalog_ddl_txn_count = batch.catalog_ddl_txn_count.saturating_add(1);

                    // Only catalog-table row redo is materialized into the catalog
                    // checkpoint; user-table row data remains owned by table
                    // files and is not part of catalog storage state.
                    for (table_id, table_dml) in redo.dml {
                        if !is_catalog_table(table_id) {
                            continue;
                        }
                        for row_redo in table_dml.rows.into_values() {
                            batch.catalog_ops.push(CatalogRedoEntry {
                                table_id,
                                kind: row_redo.kind,
                            });
                        }
                    }
                }
                CatalogCheckpointTxnAction::Skip => {
                    // Reaching here means this transaction will never need to be
                    // replayed as catalog history before the next checkpoint cursor.
                    batch.safe_cts = header.cts;
                }
                CatalogCheckpointTxnAction::Stop(reason) => {
                    batch.stop_reason = reason;
                    break;
                }
            }
        }
        Ok(batch)
    }

    fn catalog_checkpoint_txn_action(
        &self,
        ddl: &DDLRedo,
        dml: &BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> DataIntegrityResult<CatalogCheckpointTxnAction> {
        match ddl {
            DDLRedo::CreateTable(_) => Ok(CatalogCheckpointTxnAction::Include),
            DDLRedo::DropTable(table_id) if is_user_table(*table_id) => {
                if drop_table_has_catalog_table_delete(*table_id, dml)? {
                    return Ok(CatalogCheckpointTxnAction::Include);
                }
                // A user-table drop without the matching catalog-table delete
                // cannot be folded into the catalog checkpoint. Stop before
                // it so replay still sees the table DDL in log order.
                Ok(CatalogCheckpointTxnAction::Stop(
                    CatalogCheckpointScanStopReason::BlockedByTableDDL {
                        table_id: *table_id,
                        ddl: CatalogCheckpointBlockingDDL::DropTable,
                    },
                ))
            }
            DDLRedo::DropTable(_) => Ok(CatalogCheckpointTxnAction::Include),
            DDLRedo::CreateRowPage { .. } | DDLRedo::DataCheckpoint { .. } => {
                Ok(CatalogCheckpointTxnAction::Skip)
            }
            DDLRedo::TableReplaySilentWatermark { .. } => Ok(CatalogCheckpointTxnAction::Include),
            DDLRedo::CreateIndex { table_id, index_no } => self
                .catalog_checkpoint_index_ddl_action(
                    IndexDdlKind::Create,
                    *table_id,
                    *index_no,
                    cts,
                ),
            DDLRedo::DropIndex { table_id, index_no } => self.catalog_checkpoint_index_ddl_action(
                IndexDdlKind::Drop,
                *table_id,
                *index_no,
                cts,
            ),
        }
    }

    fn catalog_checkpoint_index_ddl_action(
        &self,
        kind: IndexDdlKind,
        table_id: TableID,
        index_no: u16,
        cts: TrxID,
    ) -> DataIntegrityResult<CatalogCheckpointTxnAction> {
        let table = self.get_table_now(table_id);
        let active_root = table
            .as_ref()
            .map(|table| table.file().active_root_unchecked());
        // Index DDL needs root proof because its catalog rows may be logged
        // before the table root is durably published. Proven index DDL is
        // folded into the checkpoint; provisional index DDL advances the safe
        // point but leaves catalog row redo for recovery to skip/replay from
        // the still-authoritative root.
        let proof = classify_index_ddl_root(kind, table_id, index_no, cts, active_root)?;
        match (kind, proof) {
            (
                IndexDdlKind::Create,
                IndexDdlRootProof::DurableFinalCreate | IndexDdlRootProof::DurableAllocationOnly,
            )
            | (IndexDdlKind::Drop, IndexDdlRootProof::DurableFinalDrop) => {
                Ok(CatalogCheckpointTxnAction::Include)
            }
            (_, IndexDdlRootProof::Provisional) => Ok(CatalogCheckpointTxnAction::Skip),
            (IndexDdlKind::Create, IndexDdlRootProof::DurableFinalDrop)
            | (
                IndexDdlKind::Drop,
                IndexDdlRootProof::DurableFinalCreate | IndexDdlRootProof::DurableAllocationOnly,
            ) => unreachable!("index DDL root proof kind mismatch"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogCheckpointTxnAction {
    Include,
    Skip,
    Stop(CatalogCheckpointScanStopReason),
}

fn drop_table_has_catalog_table_delete(
    table_id: TableID,
    dml: &BTreeMap<TableID, TableDML>,
) -> DataIntegrityResult<bool> {
    let Some(tables_dml) = dml.get(&TABLE_ID_TABLES) else {
        return Ok(false);
    };
    for row in tables_dml.rows.values() {
        let RowRedoKind::DeleteByPrimaryKey(key) = &row.kind else {
            continue;
        };
        if key.index_no != 0 {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "malformed drop-table redo: table_id={table_id}, index_no={}, expected_index_no=0",
                    key.index_no
                )),
            );
        }
        if key.vals.len() != 1 {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "malformed drop-table redo: table_id={table_id}, key_value_count={}, expected_count=1",
                    key.vals.len()
                )),
            );
        }
        let Some(deleted_table_id) = key.vals[0].as_u64() else {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "malformed drop-table redo: table_id={table_id}, key value is not u64"
                )),
            );
        };
        if deleted_table_id == table_id.as_u64() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{PageID, RowID};
    use crate::log::redo::RowRedo;
    use crate::recovery::stream::RedoSegmentCtsRange;
    use crate::row::ops::SelectKey;
    use crate::value::Val;
    use smol::Timer;
    use smol::future::or;
    use std::time::Duration;

    fn catalog_tables_delete_dml(key: SelectKey) -> BTreeMap<TableID, TableDML> {
        catalog_tables_dml(RowRedoKind::DeleteByPrimaryKey(key))
    }

    fn catalog_tables_dml(kind: RowRedoKind) -> BTreeMap<TableID, TableDML> {
        let row_id = RowID::new(1);
        let row = RowRedo {
            page_id: PageID::new(0),
            row_id,
            kind,
        };
        let mut rows = BTreeMap::new();
        rows.insert(row_id, row);
        let mut dml = BTreeMap::new();
        dml.insert(TABLE_ID_TABLES, TableDML { rows });
        dml
    }

    fn assert_malformed_drop_table_redo(err: Report<DataIntegrityError>, expected: &str) {
        assert_eq!(err.current_context(), &DataIntegrityError::InvalidPayload);
        let report = format!("{err:?}");
        assert!(report.contains("malformed drop-table redo"), "{report}");
        assert!(report.contains(expected), "{report}");
    }

    #[test]
    fn test_drop_table_has_catalog_table_delete_matches_table_key() {
        let table_id = TableID::new(42);
        let dml = catalog_tables_delete_dml(SelectKey::new(0, vec![Val::from(table_id.as_u64())]));

        assert!(drop_table_has_catalog_table_delete(table_id, &dml).unwrap());
    }

    #[test]
    fn test_drop_table_has_catalog_table_delete_missing_is_false() {
        let table_id = TableID::new(42);
        let dml = BTreeMap::new();

        assert!(!drop_table_has_catalog_table_delete(table_id, &dml).unwrap());
    }

    #[test]
    fn test_drop_table_has_catalog_table_delete_different_table_is_false() {
        let table_id = TableID::new(42);
        let dml = catalog_tables_delete_dml(SelectKey::new(
            0,
            vec![Val::from(TableID::new(43).as_u64())],
        ));

        assert!(!drop_table_has_catalog_table_delete(table_id, &dml).unwrap());
    }

    #[test]
    fn test_drop_table_has_catalog_table_delete_rejects_wrong_index_no() {
        let table_id = TableID::new(42);
        let dml = catalog_tables_delete_dml(SelectKey::new(1, vec![Val::from(table_id.as_u64())]));
        let err = drop_table_has_catalog_table_delete(table_id, &dml).unwrap_err();

        assert_malformed_drop_table_redo(err, "index_no=1");
    }

    #[test]
    fn test_drop_table_has_catalog_table_delete_rejects_value_count_mismatch() {
        let table_id = TableID::new(42);
        let dml = catalog_tables_delete_dml(SelectKey::new(
            0,
            vec![Val::from(table_id.as_u64()), Val::from(1u64)],
        ));
        let err = drop_table_has_catalog_table_delete(table_id, &dml).unwrap_err();

        assert_malformed_drop_table_redo(err, "key_value_count=2");
    }

    #[test]
    fn test_drop_table_has_catalog_table_delete_rejects_value_type_mismatch() {
        let table_id = TableID::new(42);
        let dml = catalog_tables_delete_dml(SelectKey::new(0, vec![Val::from(42u32)]));
        let err = drop_table_has_catalog_table_delete(table_id, &dml).unwrap_err();

        assert_malformed_drop_table_redo(err, "key value is not u64");
    }

    #[test]
    fn test_catalog_metadata_change_waits_for_active_checkpoint() {
        smol::block_on(async {
            let gate = CatalogCheckpointGate::new();
            let checkpoint_lease = gate.begin_checkpoint().await;
            let mut metadata_fut = Box::pin(gate.begin_metadata_change());

            assert!(matches!(
                futures::poll!(metadata_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(checkpoint_lease);
            let metadata_lease = metadata_fut.await;
            let mut checkpoint_fut = Box::pin(gate.begin_checkpoint());
            assert!(matches!(
                futures::poll!(checkpoint_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(metadata_lease);
            let _checkpoint_lease = checkpoint_fut.await;
        });
    }

    #[test]
    fn test_catalog_checkpoint_waits_for_active_metadata_change() {
        smol::block_on(async {
            let gate = CatalogCheckpointGate::new();
            let metadata_lease = gate.begin_metadata_change().await;
            let mut checkpoint_fut = Box::pin(gate.begin_checkpoint());

            assert!(matches!(
                futures::poll!(checkpoint_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(metadata_lease);
            let _checkpoint_lease = checkpoint_fut.await;
        });
    }

    #[test]
    fn test_catalog_checkpoint_waits_for_active_checkpoint() {
        smol::block_on(async {
            let gate = CatalogCheckpointGate::new();
            let checkpoint_lease = gate.begin_checkpoint().await;
            let mut checkpoint_fut = Box::pin(gate.begin_checkpoint());

            assert!(matches!(
                futures::poll!(checkpoint_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(checkpoint_lease);
            let _checkpoint_lease = checkpoint_fut.await;
        });
    }

    #[test]
    fn test_catalog_checkpoint_waits_behind_pending_metadata_change() {
        smol::block_on(async {
            let gate = CatalogCheckpointGate::new();
            let checkpoint_lease = gate.begin_checkpoint().await;
            let mut metadata_fut = Box::pin(gate.begin_metadata_change());
            assert!(matches!(
                futures::poll!(metadata_fut.as_mut()),
                std::task::Poll::Pending
            ));

            let mut checkpoint_fut = Box::pin(gate.begin_checkpoint());
            assert!(matches!(
                futures::poll!(checkpoint_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(checkpoint_lease);
            let metadata_lease = metadata_fut.await;
            assert!(matches!(
                futures::poll!(checkpoint_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(metadata_lease);
            let _checkpoint_lease = checkpoint_fut.await;
        });
    }

    #[test]
    fn test_catalog_pending_metadata_change_cancellation_reopens_checkpoint() {
        smol::block_on(async {
            let gate = CatalogCheckpointGate::new();
            let checkpoint_lease = gate.begin_checkpoint().await;
            let mut metadata_fut = Box::pin(gate.begin_metadata_change());

            assert!(matches!(
                futures::poll!(metadata_fut.as_mut()),
                std::task::Poll::Pending
            ));

            drop(metadata_fut);
            drop(checkpoint_lease);
            let _checkpoint_lease = gate.begin_checkpoint().await;
        });
    }

    #[test]
    fn test_catalog_checkpoint_batch_builds_catalog_safe_progress() {
        let replay_start_ts = TrxID::new(10);
        let published_catalog_replay_start_ts = TrxID::new(20);
        let batch = CatalogCheckpointBatch {
            replay_start_ts,
            safe_cts: TrxID::new(19),
            first_retained_file_seq: 7,
            sealed_redo_segments: vec![
                CatalogSafeRedoSegment {
                    file_seq: 7,
                    redo_range: None,
                },
                CatalogSafeRedoSegment {
                    file_seq: 8,
                    redo_range: Some(RedoSegmentCtsRange {
                        min_cts: TrxID::new(11),
                        max_cts: TrxID::new(19),
                    }),
                },
                CatalogSafeRedoSegment {
                    file_seq: 9,
                    redo_range: Some(RedoSegmentCtsRange {
                        min_cts: TrxID::new(20),
                        max_cts: TrxID::new(24),
                    }),
                },
            ],
            catalog_ops: Vec::new(),
            catalog_ddl_txn_count: 0,
            stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
        };

        let progress = batch.redo_retention_progress().unwrap();

        assert_eq!(progress.first_retained_file_seq, 7);
        assert_eq!(
            progress.catalog_replay_start_ts,
            published_catalog_replay_start_ts
        );
        assert_eq!(
            progress.segments,
            vec![
                CatalogSafeRedoSegment {
                    file_seq: 7,
                    redo_range: None,
                },
                CatalogSafeRedoSegment {
                    file_seq: 8,
                    redo_range: Some(RedoSegmentCtsRange {
                        min_cts: TrxID::new(11),
                        max_cts: TrxID::new(19),
                    }),
                },
            ]
        );
    }

    #[test]
    fn test_catalog_checkpoint_batch_without_durable_work_has_no_progress() {
        let replay_start_ts = TrxID::new(10);
        let batch = CatalogCheckpointBatch {
            replay_start_ts,
            safe_cts: TrxID::new(9),
            first_retained_file_seq: 7,
            sealed_redo_segments: vec![CatalogSafeRedoSegment {
                file_seq: 7,
                redo_range: None,
            }],
            catalog_ops: Vec::new(),
            catalog_ddl_txn_count: 0,
            stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
        };

        assert!(batch.redo_retention_progress().is_none());
    }

    #[test]
    fn test_catalog_second_metadata_change_completes_after_pending_waiter_cancelled() {
        smol::block_on(async {
            let gate = CatalogCheckpointGate::new();
            let checkpoint_lease = gate.begin_checkpoint().await;
            let mut pending_owner = Box::pin(gate.begin_metadata_change());
            let mut second_waiter = Box::pin(gate.begin_metadata_change());

            assert!(matches!(
                futures::poll!(pending_owner.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(matches!(
                futures::poll!(second_waiter.as_mut()),
                std::task::Poll::Pending
            ));

            drop(pending_owner);
            drop(checkpoint_lease);

            let waiter = async {
                let metadata_lease = second_waiter.await;
                drop(metadata_lease);
            };
            // Timer audit: hang watchdog for explicit metadata-latch coordination.
            or(waiter, async {
                Timer::after(Duration::from_secs(1)).await;
                panic!(
                    "second metadata-change waiter failed to acquire after pending cancellation"
                );
            })
            .await;
        });
    }
}
