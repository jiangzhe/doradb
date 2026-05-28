use crate::catalog::storage::tables::TABLE_ID_TABLES;
use crate::catalog::{
    Catalog, IndexDdlKind, IndexDdlRootProof, classify_index_ddl_root, is_catalog_obj_id,
    is_user_obj_id,
};
use crate::error::{ErrorKind, FatalError, Result};
use crate::id::{TableID, TrxID};
use crate::trx::log::{LogPartitionInitializer, list_log_files};
use crate::trx::log_replay::LogMerger;
use crate::trx::redo::{DDLRedo, RowRedoKind, TableDML};
use crate::trx::sys::TransactionSystem;
use event_listener::{Event, listener};
use parking_lot::Mutex;
use std::collections::BTreeMap;

/// One catalog-row redo operation extracted from persisted logs.
pub(crate) struct CatalogRedoEntry {
    pub(crate) table_id: TableID,
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
    pub(crate) replay_start_ts: TrxID,
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    #[cfg_attr(test, expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) durable_upper_cts: TrxID,
    pub(crate) safe_cts: TrxID,
    pub(crate) catalog_ops: Vec<CatalogRedoEntry>,
    pub(crate) catalog_ddl_txn_count: usize,
    pub(crate) stop_reason: CatalogCheckpointScanStopReason,
}

#[derive(Clone)]
pub(crate) struct CatalogCheckpointScanConfig {
    pub(crate) file_prefix: String,
    pub(crate) log_partitions: usize,
    pub(crate) io_depth_per_log: usize,
    pub(crate) log_file_max_size: usize,
    pub(crate) max_io_size: usize,
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

/// Reversible catalog metadata-change gate for catalog checkpoint exclusion.
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

    /// Acquires a catalog checkpoint lease, waiting for active metadata DDL.
    pub(crate) async fn begin_checkpoint(&self) -> CatalogCheckpointLease<'_> {
        loop {
            {
                let mut state = self.state.lock();
                if state.metadata_change == CatalogMetadataChangePhase::Open {
                    assert!(
                        !state.checkpoint_active,
                        "concurrent catalog checkpoint is not supported"
                    );
                    state.checkpoint_active = true;
                    return CatalogCheckpointLease { gate: self };
                }
            }
            listener!(self.changed => listener);
            {
                let state = self.state.lock();
                if state.metadata_change == CatalogMetadataChangePhase::Open {
                    continue;
                }
            }
            listener.await;
        }
    }

    /// Acquires a catalog metadata-change lease for future index DDL.
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

/// RAII guard for a catalog checkpoint scan/apply section.
pub(crate) struct CatalogCheckpointLease<'a> {
    gate: &'a CatalogCheckpointGate,
}

impl Drop for CatalogCheckpointLease<'_> {
    #[inline]
    fn drop(&mut self) {
        self.gate.release_checkpoint();
    }
}

/// RAII guard for future catalog metadata DDL sections.
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
    /// # Panics
    ///
    /// Panics if another checkpoint is already in progress on the same
    /// shared `CatalogStorage`/`MultiTableFile`. Concurrent checkpoint
    /// publishes are not supported by design; the underlying
    /// [`crate::file::cow_file::CowFile`] enforces a single mutable writer via
    /// an atomic claim and will panic on violation.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "missing catalog checkpoint invocation")
    )]
    pub(crate) async fn checkpoint_now(&self, trx_sys: &TransactionSystem) -> Result<()> {
        let _checkpoint_lease = self.checkpoint_gate.begin_checkpoint().await;
        let batch = self.scan_checkpoint_batch(trx_sys)?;
        match self.apply_checkpoint_batch(batch).await {
            Ok(()) => {
                trx_sys.request_dropped_table_purge();
                Ok(())
            }
            Err(err) if err.kind() == ErrorKind::Io => {
                Err(trx_sys.poison_storage(FatalError::CheckpointWrite).into())
            }
            Err(err) => Err(err),
        }
    }

    /// Scan persisted redo logs and collect one safe catalog checkpoint batch.
    ///
    /// This call satisfies the persisted-watermark precondition by using the
    /// global durable watermark across all log partitions.
    ///
    /// Scanned batches are intended for single-flight publish flow and must not
    /// be raced with other catalog checkpoint publishes against the same shared
    /// `CatalogStorage`/`MultiTableFile` writer.
    pub(crate) fn scan_checkpoint_batch(
        &self,
        trx_sys: &TransactionSystem,
    ) -> Result<CatalogCheckpointBatch> {
        let snapshot = self.storage.checkpoint_snapshot()?;
        let scan_cfg = trx_sys.catalog_checkpoint_scan_config()?;
        self.scan_checkpoint_batch_with_config(
            snapshot.catalog_replay_start_ts,
            trx_sys.persisted_watermark_cts(),
            &scan_cfg,
        )
    }

    pub(crate) fn scan_checkpoint_batch_with_config(
        &self,
        replay_start_ts: TrxID,
        durable_upper_cts: TrxID,
        scan_cfg: &CatalogCheckpointScanConfig,
    ) -> Result<CatalogCheckpointBatch> {
        // Start with an empty batch whose safe point is just before the
        // catalog replay cursor. The scan only advances `safe_cts` after a
        // redo record is proven safe to cover in this checkpoint.
        let mut batch = CatalogCheckpointBatch {
            replay_start_ts,
            durable_upper_cts,
            safe_cts: replay_start_ts.saturating_sub(1),
            catalog_ops: vec![],
            catalog_ddl_txn_count: 0,
            stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
        };
        if durable_upper_cts < replay_start_ts {
            // Nothing durable has reached the catalog replay cursor yet, so
            // there are no log records this checkpoint can consume.
            return Ok(batch);
        }

        // Build a merged CTS-ordered stream over all log partitions. Missing
        // partitions simply contribute no records to this checkpoint batch.
        let mut log_merger = LogMerger::default();
        for log_no in 0..scan_cfg.log_partitions {
            let logs = list_log_files(&scan_cfg.file_prefix, log_no, false)?;
            if logs.is_empty() {
                continue;
            }
            let stream = LogPartitionInitializer::recovery(
                scan_cfg.file_prefix.clone(),
                log_no,
                scan_cfg.io_depth_per_log,
                scan_cfg.log_file_max_size,
                scan_cfg.max_io_size,
                logs,
            )?
            .stream();
            log_merger.add_stream(stream)?;
        }

        while let Some(log) = log_merger.try_next()? {
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
            match self.catalog_checkpoint_txn_action(ddl, &redo.dml, header.cts)? {
                CatalogCheckpointTxnAction::Include => {
                    // Reaching here means this transaction will never need to be
                    // replayed as catalog history before the next checkpoint cursor.
                    batch.safe_cts = header.cts;
                    batch.catalog_ddl_txn_count = batch.catalog_ddl_txn_count.saturating_add(1);

                    // Only catalog-table row redo is materialized into the catalog
                    // checkpoint; user-table row data remains owned by table
                    // files and is not part of catalog storage state.
                    for (table_id, table_dml) in redo.dml {
                        if !is_catalog_obj_id(table_id) {
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
    ) -> Result<CatalogCheckpointTxnAction> {
        match ddl {
            DDLRedo::CreateTable(_) => Ok(CatalogCheckpointTxnAction::Include),
            DDLRedo::DropTable(table_id)
                if is_user_obj_id(*table_id)
                    && !drop_table_has_catalog_table_delete(*table_id, dml) =>
            {
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
    ) -> Result<CatalogCheckpointTxnAction> {
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
) -> bool {
    let Some(tables_dml) = dml.get(&TABLE_ID_TABLES) else {
        return false;
    };
    tables_dml.rows.values().any(|row| {
        let RowRedoKind::DeleteByUniqueKey(key) = &row.kind else {
            return false;
        };
        key.index_no == 0
            && key.vals.len() == 1
            && key.vals[0]
                .as_u64()
                .is_some_and(|id| id == table_id.as_u64())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
            smol::future::or(waiter, async {
                smol::Timer::after(std::time::Duration::from_secs(1)).await;
                panic!(
                    "second metadata-change waiter failed to acquire after pending cancellation"
                );
            })
            .await;
        });
    }
}
