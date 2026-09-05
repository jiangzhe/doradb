use super::must_catalog_table_slot;
use crate::catalog::{
    CatalogCheckpointOutcome, CatalogCheckpointReport, CatalogTableCheckpointChange,
    CatalogTableCheckpointIoStats,
};
use crate::error::{CompletionResult, RuntimeResult};
use crate::file::cow_file::{COW_FILE_PAGE_SIZE, MutableCowFile};
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MutableMultiTableFile,
};
use crate::file::super_block::SUPER_BLOCK_SIZE;
use crate::id::{BlockID, TableID};
use crate::io::DirectBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct CatalogTableCheckpointMeasurement {
    table_id: TableID,
    change: Option<CatalogTableCheckpointChange>,
    compact_blocks_read: AtomicUsize,
    final_compact_blocks: AtomicUsize,
    pub(super) lwc_blocks_written: AtomicUsize,
    pub(super) index_blocks_written: AtomicUsize,
}

pub(super) struct CatalogCheckpointMeasurement {
    catalog_ddl_txn_count: usize,
    tables: Box<[CatalogTableCheckpointMeasurement]>,
}

impl CatalogCheckpointMeasurement {
    pub(super) fn new(
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

    #[inline]
    pub(super) fn table(&self, table_id: TableID) -> &CatalogTableCheckpointMeasurement {
        &self.tables[must_catalog_table_slot(table_id)]
    }

    #[inline]
    fn table_mut(&mut self, table_id: TableID) -> &mut CatalogTableCheckpointMeasurement {
        &mut self.tables[must_catalog_table_slot(table_id)]
    }

    #[inline]
    pub(super) fn compact_read_counter(&self, table_id: TableID) -> &AtomicUsize {
        &self.table(table_id).compact_blocks_read
    }

    pub(super) fn record_table_change(
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

    pub(super) fn set_final_compact_blocks(&self, table_id: TableID, block_count: usize) {
        self.table(table_id)
            .final_compact_blocks
            .store(block_count, Ordering::Relaxed);
    }

    pub(super) fn finish(self, outcome: CatalogCheckpointOutcome) -> CatalogCheckpointReport {
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
            outcome,
            catalog_ddl_txn_count: self.catalog_ddl_txn_count,
            table_changes: table_changes.into_boxed_slice(),
            table_io: table_io.into_boxed_slice(),
            metadata_bytes_written: COW_FILE_PAGE_SIZE + SUPER_BLOCK_SIZE,
        }
    }
}

pub(super) struct MeasurableMutableCowFile<'a> {
    pub(super) mutable: &'a mut MutableMultiTableFile,
    pub(super) successful_writes: &'a AtomicUsize,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::catalog_table_id_from_slot;
    use crate::id::{RowID, TrxID};
    use std::array;

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

        let report = measurement.finish(CatalogCheckpointOutcome::Published {
            catalog_replay_start_ts: TrxID::new(1),
        });

        assert_eq!(report.table_changes.len(), 1);
        assert_eq!(report.table_changes[0].table_id, roots[0].table_id);
        assert_eq!(report.table_changes[0].before_row_count, 2);
        assert_eq!(report.table_changes[0].after_row_count, 2);
        assert_eq!(report.table_io.len(), 1);
        assert_eq!(report.table_io[0].table_id, roots[1].table_id);
        assert_eq!(report.table_io[0].compact_bytes_read, COW_FILE_PAGE_SIZE);
    }
}
