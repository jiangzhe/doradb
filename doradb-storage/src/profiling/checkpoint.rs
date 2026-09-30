use super::clock::Instant;
use crate::catalog::storage::must_catalog_table_slot;
use crate::error::{CompletionResult, RuntimeResult};
use crate::file::cow_file::{COW_FILE_PAGE_SIZE, MutableCowFile};
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MutableMultiTableFile,
};
use crate::file::super_block::SUPER_BLOCK_SIZE;
use crate::id::{BlockID, TableID};
use crate::io::DirectBuf;
use crate::obs;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Successful catalog checkpoint measurement returned by the public operation.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogCheckpointReport {
    /// Number of catalog DDL transactions folded by this checkpoint.
    pub catalog_ddl_txn_count: usize,
    /// Changed logical catalog tables in increasing table-ID order.
    pub table_changes: Box<[CatalogTableCheckpointChange]>,
    /// Logical catalog tables with measured I/O in increasing table-ID order.
    pub table_io: Box<[CatalogTableCheckpointIoStats]>,
    /// Successfully written catalog metadata-page and super-root-slot bytes.
    pub metadata_bytes_written: usize,
}

/// Row-count change for one built-in logical catalog table.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogTableCheckpointChange {
    /// Built-in logical catalog table identity.
    pub table_id: TableID,
    /// Rows in the durable table image before the change.
    pub before_row_count: usize,
    /// Rows in the durable table image after the change.
    pub after_row_count: usize,
}

/// Checkpoint I/O for one built-in logical catalog table with measured activity.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogTableCheckpointIoStats {
    /// Built-in logical catalog table identity.
    pub table_id: TableID,
    /// Cache-independent bytes requested from compact table blocks.
    pub compact_bytes_read: usize,
    /// Bytes occupied by all compact blocks reachable from the final root.
    pub final_compact_bytes: usize,
    /// Successfully written replacement LWC block bytes.
    pub lwc_bytes_written: usize,
    /// Successfully written replacement column-index block bytes.
    pub index_bytes_written: usize,
}

/// Logical changes and physical I/O measurements for one catalog table.
pub(crate) struct CatalogTableCheckpointMeasurement {
    table_id: TableID,
    change: Option<CatalogTableCheckpointChange>,
    compact_blocks_read: AtomicUsize,
    final_compact_blocks: AtomicUsize,
    /// Successfully completed LWC-block writes.
    pub(crate) lwc_blocks_written: AtomicUsize,
    /// Successfully completed column-index block writes.
    pub(crate) index_blocks_written: AtomicUsize,
}

/// Measurement state retained by one catalog checkpoint operation.
pub(crate) struct CatalogCheckpointMeasurement {
    catalog_ddl_txn_count: usize,
    tables: Box<[CatalogTableCheckpointMeasurement]>,
}

impl CatalogCheckpointMeasurement {
    /// Allocate measurements for the captured catalog root set.
    pub(crate) fn new(
        roots: &[CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT],
        catalog_ddl_txn_count: usize,
    ) -> Self {
        Self {
            catalog_ddl_txn_count,
            tables: roots
                .iter()
                .map(|root| CatalogTableCheckpointMeasurement {
                    table_id: root.table_id,
                    change: None,
                    compact_blocks_read: AtomicUsize::new(0),
                    final_compact_blocks: AtomicUsize::new(0),
                    lwc_blocks_written: AtomicUsize::new(0),
                    index_blocks_written: AtomicUsize::new(0),
                })
                .collect(),
        }
    }

    /// Return the measurements for one built-in catalog table.
    #[inline]
    pub(crate) fn table(&self, table_id: TableID) -> &CatalogTableCheckpointMeasurement {
        &self.tables[must_catalog_table_slot(table_id)]
    }

    #[inline]
    fn table_mut(&mut self, table_id: TableID) -> &mut CatalogTableCheckpointMeasurement {
        &mut self.tables[must_catalog_table_slot(table_id)]
    }

    /// Borrow the logical compact-block read counter.
    #[inline]
    pub(crate) fn compact_read_counter(&self, table_id: TableID) -> &AtomicUsize {
        &self.table(table_id).compact_blocks_read
    }

    /// Record the before and after cardinality of one changed table.
    pub(crate) fn record_table_change(
        &mut self,
        table_id: TableID,
        before_row_count: usize,
        after_row_count: usize,
    ) {
        let previous = self
            .table_mut(table_id)
            .change
            .replace(CatalogTableCheckpointChange {
                table_id,
                before_row_count,
                after_row_count,
            });
        assert!(
            previous.is_none(),
            "catalog checkpoint measurement recorded table change twice: table_id={table_id}"
        );
    }

    /// Record the compact blocks reachable from the final root.
    pub(crate) fn set_final_compact_blocks(&self, table_id: TableID, block_count: usize) {
        self.table(table_id)
            .final_compact_blocks
            .store(block_count, Ordering::Relaxed);
    }

    /// Build the public report after successful root publication.
    pub(crate) fn finish(self) -> CatalogCheckpointReport {
        let mut table_changes = Vec::new();
        let mut table_io = Vec::new();
        for table in self.tables {
            if let Some(change) = table.change {
                table_changes.push(change);
            }
            let compact_bytes_read = table.compact_blocks_read.into_inner() * COW_FILE_PAGE_SIZE;
            let final_compact_bytes = table.final_compact_blocks.into_inner() * COW_FILE_PAGE_SIZE;
            let lwc_bytes_written = table.lwc_blocks_written.into_inner() * COW_FILE_PAGE_SIZE;
            let index_bytes_written = table.index_blocks_written.into_inner() * COW_FILE_PAGE_SIZE;
            if compact_bytes_read != 0 || lwc_bytes_written != 0 || index_bytes_written != 0 {
                table_io.push(CatalogTableCheckpointIoStats {
                    table_id: table.table_id,
                    compact_bytes_read,
                    final_compact_bytes,
                    lwc_bytes_written,
                    index_bytes_written,
                });
            }
        }
        CatalogCheckpointReport {
            catalog_ddl_txn_count: self.catalog_ddl_txn_count,
            table_changes: table_changes.into_boxed_slice(),
            table_io: table_io.into_boxed_slice(),
            metadata_bytes_written: COW_FILE_PAGE_SIZE + SUPER_BLOCK_SIZE,
        }
    }
}

/// CoW adapter counting only successfully completed index-block writes.
pub(crate) struct MeasurableMutableCowFile<'a> {
    /// Mutable catalog CoW file whose writes are measured.
    pub(crate) mutable: &'a mut MutableMultiTableFile,
    /// Counter incremented after each successful block write.
    pub(crate) successful_writes: &'a AtomicUsize,
}

impl MutableCowFile for MeasurableMutableCowFile<'_> {
    #[inline]
    fn allocate_block(&mut self) -> RuntimeResult<BlockID> {
        self.mutable.allocate_block()
    }

    #[inline]
    fn rollback_allocated_block(&mut self, block_id: BlockID) {
        self.mutable.rollback_allocated_block(block_id);
    }

    #[inline]
    async fn write_block(&self, block_id: BlockID, buf: DirectBuf) -> CompletionResult<()> {
        let result = self.mutable.write_block(block_id, buf).await;
        if result.is_ok() {
            self.successful_writes.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

/// Diagnostic timing and occupancy for one table checkpoint pipeline.
pub(crate) struct CheckpointLwcProfile {
    started_at: Instant,
    /// Last observed CPU encode completion.
    pub(crate) final_cpu_completion_at: Option<Instant>,
    /// Last write accepted by shared storage.
    pub(crate) final_write_acceptance_at: Option<Instant>,
    /// Number of submitted block encodes.
    pub(crate) produced_blocks: usize,
    /// Greatest observed number of unaccepted CPU-stage blocks.
    pub(crate) peak_cpu_occupancy: usize,
}

impl CheckpointLwcProfile {
    /// Start pipeline timing before the first block is submitted.
    pub(crate) fn new() -> Self {
        Self {
            started_at: Instant::now(),
            final_cpu_completion_at: None,
            final_write_acceptance_at: None,
            produced_blocks: 0,
            peak_cpu_occupancy: 0,
        }
    }

    /// Emit the completed pipeline timing and occupancy measurements.
    pub(crate) fn log_diagnostics(&self, result: &str, table_id: TableID, encode_limit: usize) {
        let production_nanos = self.final_cpu_completion_at.map_or(0, |finished| {
            finished.duration_since(self.started_at).as_nanos()
        });
        let cpu_to_accept_nanos = self
            .final_cpu_completion_at
            .zip(self.final_write_acceptance_at)
            .map_or(0, |(cpu, accepted)| {
                accepted.saturating_duration_since(cpu).as_nanos()
            });
        let write_drain_nanos = self
            .final_write_acceptance_at
            .map_or(0, |accepted| accepted.elapsed().as_nanos());
        obs::debug!(
            "event=checkpoint_lwc_pipeline component=table action=finish result={} table_id={} block_count={} cpu_stage_capacity={} peak_cpu_occupancy={} production_nanos={} final_cpu_to_final_write_acceptance_nanos={} data_write_drain_nanos={}",
            result,
            table_id,
            self.produced_blocks,
            encode_limit,
            self.peak_cpu_occupancy,
            production_nanos,
            cpu_to_accept_nanos,
            write_drain_nanos,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::catalog_table_id_from_slot;
    use crate::id::RowID;
    use std::array;

    /// Purpose: Distinguish logical catalog changes from physical checkpoint work.
    /// Expected: Unchanged cardinality does not hide edits, and reads alone do not imply table
    /// changes.
    #[test]
    fn report_splits_changed_tables_from_table_io() {
        let roots = array::from_fn(|idx| {
            CatalogTableRootDesc::published(
                catalog_table_id_from_slot(idx),
                BlockID::from(idx + 1),
                RowID::new(10_000 + idx as u64 * 17),
            )
        });
        let mut measurement = CatalogCheckpointMeasurement::new(&roots, 1);
        measurement.record_table_change(roots[0].table_id, 2, 2);
        measurement
            .table(roots[1].table_id)
            .compact_blocks_read
            .store(1, Ordering::Relaxed);
        measurement.set_final_compact_blocks(roots[2].table_id, 4);

        let report = measurement.finish();

        assert_eq!(report.table_changes.len(), 1);
        assert_eq!(report.table_changes[0].table_id, roots[0].table_id);
        assert_eq!(report.table_changes[0].before_row_count, 2);
        assert_eq!(report.table_changes[0].after_row_count, 2);
        assert_eq!(report.table_io.len(), 1);
        assert_eq!(report.table_io[0].table_id, roots[1].table_id);
        assert_eq!(report.table_io[0].compact_bytes_read, COW_FILE_PAGE_SIZE);
    }
}
