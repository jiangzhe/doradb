use crate::catalog::{Catalog, TableID, is_catalog_obj_id, is_user_obj_id};
use crate::error::{ErrorKind, FatalError, Result};
use crate::trx::TrxID;
use crate::trx::log::{LogPartitionInitializer, list_log_files};
use crate::trx::log_replay::LogMerger;
use crate::trx::redo::{DDLRedo, RowRedoKind};
use crate::trx::sys::TransactionSystem;

/// One catalog-row redo operation extracted from persisted logs.
pub struct CatalogRedoEntry {
    pub table_id: TableID,
    pub kind: RowRedoKind,
}

/// Table DDL kinds that can block catalog checkpoint scan on ordering safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogCheckpointBlockingDDL {
    DropTable,
}

/// Stop reason for one catalog checkpoint scan batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogCheckpointScanStopReason {
    ReachedDurableUpper,
    BlockedByTableDDL {
        table_id: TableID,
        ddl: CatalogCheckpointBlockingDDL,
    },
}

/// Catalog checkpoint scan result consumed by apply phase.
pub struct CatalogCheckpointBatch {
    pub replay_start_ts: TrxID,
    pub durable_upper_cts: TrxID,
    pub safe_cts: TrxID,
    pub catalog_ops: Vec<CatalogRedoEntry>,
    pub catalog_ddl_txn_count: usize,
    pub stop_reason: CatalogCheckpointScanStopReason,
}

#[derive(Clone)]
pub(crate) struct CatalogCheckpointScanConfig {
    pub(crate) file_prefix: String,
    pub(crate) log_partitions: usize,
    pub(crate) io_depth_per_log: usize,
    pub(crate) log_file_max_size: usize,
    pub(crate) max_io_size: usize,
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
    pub async fn checkpoint_now(&self, trx_sys: &TransactionSystem) -> Result<()> {
        let batch = self.scan_checkpoint_batch(trx_sys)?;
        match self.apply_checkpoint_batch(batch).await {
            Ok(()) => Ok(()),
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
    pub fn scan_checkpoint_batch(
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
        let mut batch = CatalogCheckpointBatch {
            replay_start_ts,
            durable_upper_cts,
            safe_cts: replay_start_ts.saturating_sub(1),
            catalog_ops: vec![],
            catalog_ddl_txn_count: 0,
            stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
        };
        if durable_upper_cts < replay_start_ts {
            return Ok(batch);
        }

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
                continue;
            }
            if header.cts > durable_upper_cts {
                break;
            }

            if let Some(ddl) = redo.ddl.as_deref()
                && let Some((table_id, ddl_kind)) = blocking_table_ddl(ddl)
                && is_user_obj_id(table_id)
            {
                let Some(table_replay_start_ts) = self.loaded_table_replay_start_ts(table_id)
                else {
                    batch.stop_reason = CatalogCheckpointScanStopReason::BlockedByTableDDL {
                        table_id,
                        ddl: ddl_kind,
                    };
                    break;
                };
                if table_replay_start_ts <= header.cts {
                    batch.stop_reason = CatalogCheckpointScanStopReason::BlockedByTableDDL {
                        table_id,
                        ddl: ddl_kind,
                    };
                    break;
                }
            }

            batch.safe_cts = header.cts;

            if let Some(ddl) = redo.ddl.as_deref()
                && is_catalog_ddl(ddl)
            {
                batch.catalog_ddl_txn_count = batch.catalog_ddl_txn_count.saturating_add(1);
            }

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
        Ok(batch)
    }
}

#[inline]
fn blocking_table_ddl(ddl: &DDLRedo) -> Option<(TableID, CatalogCheckpointBlockingDDL)> {
    match ddl {
        DDLRedo::DropTable(table_id) => Some((*table_id, CatalogCheckpointBlockingDDL::DropTable)),
        _ => None,
    }
}

#[inline]
fn is_catalog_ddl(ddl: &DDLRedo) -> bool {
    matches!(ddl, DDLRedo::CreateTable(_) | DDLRedo::DropTable(_))
}
