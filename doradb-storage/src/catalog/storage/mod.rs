mod columns;
mod ddl;
mod indexes;
mod integrity;
pub(crate) mod layout;
mod measure;
mod merge;
mod object;
mod table_bindings;
mod table_descriptors;
mod table_replay_silent_watermarks;
pub(crate) mod tables;

use layout::BUILTIN_CATALOG_TABLE_IDS;
pub(crate) use layout::{
    TABLE_ID_COLUMNS, TABLE_ID_INDEXES, TABLE_ID_TABLE_BINDINGS, TABLE_ID_TABLE_DESCRIPTORS,
    TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS, TABLE_ID_TABLES,
};

use crate::buffer::{FixedBufferPool, PoolGuard, PoolGuards, ReadonlyBufferPool};
use crate::catalog::storage::columns::*;
use crate::catalog::storage::indexes::*;
use crate::catalog::storage::measure::{CatalogCheckpointMeasurement, MeasurableMutableCowFile};
use crate::catalog::storage::merge::{CatalogFoldedRows, CatalogMergeKeyBuilder};
pub(crate) use crate::catalog::storage::object::*;
pub(crate) use crate::catalog::storage::table_bindings::TableBindings;
use crate::catalog::storage::table_bindings::{
    catalog_definition_of_table_bindings, table_binding_object_from_vals,
};
pub(crate) use crate::catalog::storage::table_descriptors::{
    TableDescriptors, validate_table_descriptor_against_metadata,
};
use crate::catalog::storage::table_descriptors::{
    catalog_definition_of_table_descriptors, table_descriptor_object_from_vals,
};
use crate::catalog::storage::table_replay_silent_watermarks::*;
pub(crate) use crate::catalog::storage::tables::*;
use crate::catalog::{
    CatalogCheckpointBatch, CatalogCheckpointOutcome, CatalogCheckpointReport, CatalogRedoEntry,
    CatalogTable, TableMetadata, catalog_table_id_from_slot, catalog_table_slot,
};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, MultiDomainResultExt, RuntimeError,
    RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeOrFatalResultExt, RuntimeResult,
};
use crate::file::FileKind;
#[cfg(test)]
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::file::cow_file::{MutableCowFile, SUPER_BLOCK_ID};
use crate::file::fs::FileSystem;
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MultiTableActiveRoot, MultiTableFile,
    MultiTableFileSnapshot, MutableMultiTableFile,
};
#[cfg(test)]
use crate::file::super_block::SUPER_BLOCK_SIZE;
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::index::{BlockIndex, ColumnBlockEntryShape, ColumnBlockIndex, ColumnLeafEntry};
use crate::io::DirectBuf;
use crate::log::redo::RowRedoKind;
use crate::lwc::{LwcBuilder, PersistedLwcBlock};
use crate::map::{FastHashMap, FastHashSet};
use crate::quiescent::QuiescentGuard;
use crate::table::TableRedoReplayFloor;
use crate::value::Val;
use error_stack::{Report, ResultExt};
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[cfg(test)]
pub(crate) use tests::publish_first_redo_log_seq_for_test;

/// Runtime storage container for all catalog logical tables.
pub(crate) struct CatalogStorage {
    pub(super) meta_pool: QuiescentGuard<FixedBufferPool>,
    pub(super) table_fs: QuiescentGuard<FileSystem>,
    tables: Box<[Arc<CatalogTable>]>,
    next_table_id: TableID,
    pub(super) mtb: Arc<MultiTableFile>,
    pub(super) disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    /// Checkpoint-durable table replay watermark overlays.
    ///
    /// This cache is rebuilt only from checkpointed
    /// `catalog.table_replay_silent_watermarks` roots. A committed silent
    /// watermark transaction can make current catalog rows newer than this
    /// cache, but recovery and redo truncation must not treat those rows as
    /// durable proof until a catalog checkpoint folds them into `catalog.mtb`.
    /// Callers combine these overlays with user-table root floors by fieldwise
    /// maximum.
    checkpointed_silent_watermarks: Mutex<Arc<FastHashMap<TableID, TableRedoReplayFloor>>>,
}

impl CatalogStorage {
    /// Open or initialize catalog storage and bootstrap catalog table runtimes.
    #[inline]
    pub(crate) async fn new(
        meta_pool: QuiescentGuard<FixedBufferPool>,
        table_fs: QuiescentGuard<FileSystem>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
        bootstrap_guards: &PoolGuards,
    ) -> RuntimeResult<Self> {
        let mtb = table_fs
            .open_or_create_multi_table_file(disk_pool.clone(), bootstrap_guards.disk_guard())
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=open_catalog_storage")?;
        let mtb_snapshot = mtb.load_snapshot();

        let mut cat: Vec<Arc<CatalogTable>> = vec![];
        for CatalogDefinition { table_id, metadata } in [
            catalog_definition_of_tables(),
            catalog_definition_of_columns(),
            catalog_definition_of_indexes(),
            catalog_definition_of_table_descriptors(),
            catalog_definition_of_table_replay_silent_watermarks(),
            catalog_definition_of_table_bindings(),
        ] {
            // Make sure catalog table ids match their dense root slots.
            assert_eq!(
                BUILTIN_CATALOG_TABLE_IDS[cat.len()],
                *table_id,
                "catalog bootstrap definition order differs from durable layout"
            );
            let metadata = Arc::new(metadata.clone());
            let blk_idx = BlockIndex::new_catalog(meta_pool.clone(), bootstrap_guards.meta_guard())
                .await
                .change_context(RuntimeError::CatalogAccess)
                .attach_with(|| {
                    format!("operation=create_catalog_block_index, table_id={table_id}")
                })?;
            let table = Arc::new(
                CatalogTable::new(
                    meta_pool.clone(),
                    bootstrap_guards.meta_guard(),
                    *table_id,
                    blk_idx,
                    metadata,
                )
                .await?,
            );
            cat.push(table);
        }
        assert_eq!(
            cat.len(),
            CATALOG_TABLE_ROOT_DESC_COUNT,
            "catalog bootstrap definition count differs from durable layout"
        );
        Ok(CatalogStorage {
            meta_pool,
            table_fs,
            tables: cat.into_boxed_slice(),
            next_table_id: mtb_snapshot.meta.next_table_id,
            mtb,
            disk_pool,
            checkpointed_silent_watermarks: Mutex::new(Arc::new(FastHashMap::default())),
        })
    }

    /// Accessor of `catalog.tables`.
    #[inline]
    pub(crate) fn tables(&self) -> Tables<'_> {
        Tables {
            table: &self.tables[must_catalog_table_slot(TABLE_ID_TABLES)],
        }
    }

    /// Accessor of `catalog.columns`.
    #[inline]
    pub(crate) fn columns(&self) -> Columns<'_> {
        Columns {
            table: &self.tables[must_catalog_table_slot(TABLE_ID_COLUMNS)],
        }
    }

    /// Accessor of `catalog.indexes`.
    #[inline]
    pub(crate) fn indexes(&self) -> Indexes<'_> {
        Indexes {
            table: &self.tables[must_catalog_table_slot(TABLE_ID_INDEXES)],
        }
    }

    /// Accessor of `catalog.table_descriptors`.
    #[inline]
    pub(crate) fn table_descriptors(&self) -> TableDescriptors<'_> {
        TableDescriptors {
            table: &self.tables[must_catalog_table_slot(TABLE_ID_TABLE_DESCRIPTORS)],
        }
    }

    /// Accessor of `catalog.table_bindings`.
    #[inline]
    pub(crate) fn table_bindings(&self) -> TableBindings<'_> {
        TableBindings {
            table: &self.tables[must_catalog_table_slot(TABLE_ID_TABLE_BINDINGS)],
        }
    }

    /// Accessor of `catalog.table_replay_silent_watermarks`.
    #[inline]
    pub(crate) fn table_replay_silent_watermarks(&self) -> TableReplaySilentWatermarks<'_> {
        TableReplaySilentWatermarks {
            table: &self.tables[must_catalog_table_slot(TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS)],
        }
    }

    /// Clone the checkpoint-durable silent watermark overlay snapshot.
    ///
    /// The snapshot is loaded from checkpointed catalog roots only. It does not
    /// include silent watermark transactions committed after the latest catalog
    /// checkpoint, even though those rows are visible through
    /// `table_replay_silent_watermarks()`.
    #[inline]
    pub(crate) fn checkpointed_silent_watermarks(
        &self,
    ) -> Arc<FastHashMap<TableID, TableRedoReplayFloor>> {
        Arc::clone(&self.checkpointed_silent_watermarks.lock())
    }

    /// Return one catalog table runtime by table id.
    #[inline]
    pub(crate) fn get_catalog_table(&self, table_id: TableID) -> Option<Arc<CatalogTable>> {
        let slot = catalog_table_slot(table_id)?;
        self.tables.get(slot).map(Arc::clone)
    }

    /// Return current next table id persisted in catalog snapshot.
    #[inline]
    pub(crate) fn next_table_id(&self) -> TableID {
        self.next_table_id
    }

    /// Returns current persisted catalog checkpoint snapshot from `catalog.mtb`.
    #[inline]
    pub(crate) fn checkpoint_snapshot(&self) -> MultiTableFileSnapshot {
        self.mtb.load_snapshot()
    }

    /// Publish a durable first-retained redo marker without changing catalog table roots.
    ///
    /// The marker is stored in the `catalog.mtb` root instead of a catalog row
    /// because startup must read it before redo discovery and catalog redo
    /// replay. It tells recovery that missing prefix files below the marker
    /// were intentionally truncated; ordinary catalog-table state cannot prove
    /// that until after redo has already been selected for replay.
    pub(crate) async fn publish_first_redo_log_seq(
        &self,
        first_redo_log_seq: u32,
    ) -> RuntimeResult<u32> {
        let snapshot = self.mtb.load_snapshot();
        if first_redo_log_seq <= snapshot.meta.first_redo_log_seq {
            return Ok(snapshot.meta.first_redo_log_seq);
        }

        let background_writes = self.table_fs.background_writes();
        let mut mutable = MutableMultiTableFile::fork(&self.mtb, background_writes);
        let (current_first_redo_log_seq, displaced_meta_block_id) = {
            let root = mutable.root();
            (root.first_redo_log_seq, root.meta_block_id)
        };
        if first_redo_log_seq <= current_first_redo_log_seq {
            return Ok(current_first_redo_log_seq);
        }
        mutable.apply_first_redo_log_seq(first_redo_log_seq);
        mutable
            .reserve_publish_meta_block_reclaiming_displaced_meta(displaced_meta_block_id)
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=publish_first_redo_log_seq, phase=reserve_meta_block, first_redo_log_seq={first_redo_log_seq}"
                )
            })?;
        let (_, old_root) = mutable
            .commit_prepared()
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=publish_first_redo_log_seq, phase=commit_catalog_root, first_redo_log_seq={first_redo_log_seq}"
                )
            })?;
        drop(old_root);
        Ok(first_redo_log_seq)
    }

    /// Bootstrap in-memory catalog rows from the latest catalog checkpoint snapshot.
    pub(crate) async fn bootstrap_from_checkpoint(
        &self,
        snapshot: &MultiTableFileSnapshot,
        guards: &PoolGuards,
        disable_dml_validation: bool,
    ) -> RuntimeResult<()> {
        let measurement = CatalogCheckpointMeasurement::new(&snapshot.meta.table_roots, 0);
        for (idx, root) in snapshot.meta.table_roots.iter().copied().enumerate() {
            if idx >= self.tables.len() {
                break;
            }
            if catalog_table_slot(root.table_id) != Some(idx) {
                return Err(
                    Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "catalog root table id mismatch: root_table_id={}, slot_idx={idx}",
                        root.table_id
                    )),
                )
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=bootstrap_catalog, phase=validate_table_root");
            }
            if root.checkpoint_root_block_id().is_none() {
                continue;
            }
            let rows = self
                .load_rows_from_root(
                    self.tables[idx].metadata(),
                    guards.disk_guard(),
                    root,
                    &measurement,
                )
                .await?;
            for row in rows {
                self.tables[idx]
                    .insert_no_trx(guards, &row.vals, disable_dml_validation)
                    .await
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=bootstrap_catalog, phase=insert_row, table_id={}",
                            root.table_id
                        )
                    })?;
            }
        }
        let watermarks = self
            .load_checkpointed_table_replay_silent_watermark_map(
                guards.disk_guard(),
                snapshot.meta.table_roots
                    [must_catalog_table_slot(TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS)],
                &measurement,
            )
            .await?;
        self.install_checkpointed_silent_watermarks(Arc::new(watermarks));
        Ok(())
    }

    /// Prepare one scanned catalog checkpoint batch for catalog root publication.
    pub(crate) async fn prepare_checkpoint_batch(
        &self,
        batch: CatalogCheckpointBatch,
        next_table_id: TableID,
        disk_guard: &PoolGuard,
    ) -> RuntimeOrFatalResult<PreparedCatalogCheckpoint> {
        let CatalogCheckpointBatch {
            replay_start_ts,
            safe_cts,
            catalog_ops,
            catalog_ddl_txn_count,
            ..
        } = batch;
        let snapshot = self.mtb.load_snapshot();
        let current_catalog_replay_start_ts = snapshot.catalog_replay_start_ts;
        let next_catalog_replay_start_ts = safe_cts.saturating_add(1).max(replay_start_ts);

        // The batch is valid only for the catalog replay cursor it scanned
        // from. If another checkpoint already advanced far enough, this stale
        // batch is harmless; otherwise the cursor mismatch means applying it
        // would skip or duplicate catalog redo.
        if current_catalog_replay_start_ts != replay_start_ts {
            if current_catalog_replay_start_ts >= next_catalog_replay_start_ts {
                return Ok(PreparedCatalogCheckpoint::Noop {
                    catalog_replay_start_ts: current_catalog_replay_start_ts,
                    checkpointed_silent_watermarks: self.checkpointed_silent_watermarks(),
                    catalog_ddl_txn_count,
                });
            }
            return Err(RuntimeOrFatalError::from(
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach("catalog replay start does not match checkpoint batch")
                    .change_context(RuntimeError::CatalogAccess)
                    .attach(format!(
                    "operation=prepare_catalog_checkpoint, current_replay_start_ts={current_catalog_replay_start_ts}, expected_replay_start_ts={replay_start_ts}, next_replay_start_ts={next_catalog_replay_start_ts}"
                    )),
            ));
        }

        // A scan can legitimately find no durable record at or after the
        // catalog replay cursor. In that case there is no new checkpoint
        // boundary to publish.
        if safe_cts < replay_start_ts {
            return Ok(PreparedCatalogCheckpoint::Noop {
                catalog_replay_start_ts: current_catalog_replay_start_ts,
                checkpointed_silent_watermarks: self.checkpointed_silent_watermarks(),
                catalog_ddl_txn_count,
            });
        }
        let background_writes = self.table_fs.background_writes();

        let mut mutable = MutableMultiTableFile::fork(&self.mtb, background_writes);
        let mut new_roots = snapshot.meta.table_roots;
        let mut measurement = CatalogCheckpointMeasurement::new(&new_roots, catalog_ddl_txn_count);
        let mut catalog_blocks_changed = false;
        if !catalog_ops.is_empty() {
            // Replay only catalog-table row operations into catalog.mtb. User
            // table row data remains owned by table files and is not folded
            // into catalog checkpoint storage.
            let mut ops_by_table: Vec<Vec<RowRedoKind>> =
                (0..self.tables.len()).map(|_| Vec::new()).collect();
            for CatalogRedoEntry { table_id, kind } in catalog_ops {
                let table_idx = catalog_table_slot_checked(table_id, ops_by_table.len())
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=prepare_catalog_checkpoint, phase=group_catalog_redo, table_id={table_id}"
                        )
                    })?;
                ops_by_table[table_idx].push(kind);
            }

            for (idx, table) in self.tables.iter().enumerate() {
                if ops_by_table[idx].is_empty() {
                    continue;
                }
                let current_root = new_roots[idx];
                let (new_root, table_blocks_changed) = self
                    .apply_table_ops(
                        &mut mutable,
                        catalog_table_id_from_slot(idx),
                        table.metadata(),
                        current_root,
                        &ops_by_table[idx],
                        safe_cts,
                        disk_guard,
                        &mut measurement,
                    )
                    .await?;
                new_roots[idx] = new_root;
                catalog_blocks_changed |= table_blocks_changed;
            }
        }

        self.validate_projected_catalog_integrity(&new_roots, disk_guard, &measurement)
            .await?;

        // Publishing the metadata block advances the durable catalog replay
        // boundary even for metadata-only checkpoints, such as DML-only
        // heartbeat batches.
        mutable.apply_checkpoint_metadata(next_catalog_replay_start_ts, next_table_id, new_roots);
        if catalog_blocks_changed {
            // Rewriting catalog table roots can make arbitrary old catalog
            // blocks unreachable, so rebuild the allocation map from the new
            // root graph before publishing.
            self.rebuild_catalog_alloc_map(&mut mutable, disk_guard, &mut measurement)
                .await?;
        } else {
            // Metadata-only checkpoints do not change catalog table root
            // reachability. Reclaim the displaced metadata block directly and
            // avoid reading catalog indexes just to rebuild the same map.
            mutable
                .reserve_publish_meta_block_reclaiming_displaced_meta(snapshot.meta_block_id)
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=prepare_catalog_checkpoint, phase=reserve_meta_block")?;
        }
        // Load the silent replay watermark overlay from `new_roots`, not from
        // the currently durable cache. The prepared checkpoint has already
        // materialized catalog-table changes into blocks, but its metadata root
        // is not committed yet. Combined catalog checkpoint plus redo
        // truncation uses this projected overlay to plan against the same table
        // replay floors that will become checkpoint-durable if the prepared
        // root is committed.
        let checkpointed_silent_watermarks = self
            .load_checkpointed_table_replay_silent_watermark_map(
                disk_guard,
                new_roots[must_catalog_table_slot(TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS)],
                &measurement,
            )
            .await?;
        Ok(PreparedCatalogCheckpoint::Published(Box::new(
            PreparedCatalogPublish {
                mutable,
                catalog_replay_start_ts: next_catalog_replay_start_ts,
                checkpointed_silent_watermarks: Arc::new(checkpointed_silent_watermarks),
                measurement,
            },
        )))
    }

    #[inline]
    fn install_checkpointed_silent_watermarks(
        &self,
        watermarks: Arc<FastHashMap<TableID, TableRedoReplayFloor>>,
    ) {
        *self.checkpointed_silent_watermarks.lock() = watermarks;
    }

    async fn load_checkpointed_table_replay_silent_watermark_map(
        &self,
        disk_pool_guard: &PoolGuard,
        root: CatalogTableRootDesc,
        measurement: &CatalogCheckpointMeasurement,
    ) -> RuntimeResult<FastHashMap<TableID, TableRedoReplayFloor>> {
        let rows = self
            .load_rows_from_root(
                self.tables[must_catalog_table_slot(TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS)]
                    .metadata(),
                disk_pool_guard,
                root,
                measurement,
            )
            .await?;
        let mut watermarks = FastHashMap::default();
        for row in rows {
            let obj = table_replay_silent_watermark_object_from_vals(&row.vals)
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=load_checkpointed_silent_watermarks, phase=decode_row")?;
            watermarks.insert(
                obj.table_id,
                TableRedoReplayFloor {
                    heap_redo_start_ts: obj.heap_redo_start_ts,
                    deletion_cutoff_ts: obj.deletion_cutoff_ts,
                },
            );
        }
        Ok(watermarks)
    }

    async fn rebuild_catalog_alloc_map(
        &self,
        mutable: &mut MutableMultiTableFile,
        disk_guard: &PoolGuard,
        measurement: &mut CatalogCheckpointMeasurement,
    ) -> RuntimeResult<usize> {
        mutable
            .reserve_publish_meta_block()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=rebuild_catalog_alloc_map, phase=reserve_meta_block")?;
        let reachable = self
            .collect_catalog_reachable_blocks(mutable.root(), disk_guard, measurement)
            .await?;
        Ok(mutable.rebuild_alloc_map_from_reachable(&reachable))
    }

    async fn collect_catalog_reachable_blocks(
        &self,
        root: &MultiTableActiveRoot,
        disk_guard: &PoolGuard,
        measurement: &CatalogCheckpointMeasurement,
    ) -> RuntimeResult<BTreeSet<BlockID>> {
        let mut reachable = BTreeSet::new();
        reachable.insert(SUPER_BLOCK_ID);
        reachable.insert(root.meta_block_id);

        for (idx, table_root) in root.table_roots.iter().enumerate() {
            if catalog_table_slot(table_root.table_id) != Some(idx) {
                return Err(
                    Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                        "file={}, root_ts={}, table_id={}, slot_idx={idx}",
                        FileKind::CatalogMultiTableFile,
                        root.root_ts,
                        table_root.table_id
                    )),
                )
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=collect_catalog_reachable_blocks, phase=validate_table_root");
            }
            let Some(root_block_id) = table_root.checkpoint_root_block_id() else {
                measurement.set_final_compact_blocks(table_root.table_id, 0);
                continue;
            };
            validate_catalog_reachable_block(root, root_block_id)
                .change_context(RuntimeError::CatalogAccess)
                .attach_with(|| {
                    format!(
                        "operation=collect_catalog_reachable_blocks, phase=validate_index_root, table_id={}, block_id={root_block_id}",
                        table_root.table_id
                    )
                })?;
            let reachable_before = reachable.len();
            let column_index = ColumnBlockIndex::new(
                root_block_id,
                table_root.pivot_row_id(),
                self.mtb.file_kind(),
                self.mtb.sparse_file(),
                &self.disk_pool,
                disk_guard,
            )
            .with_logical_read_counter(measurement.compact_read_counter(table_root.table_id));
            column_index
                .collect_reachable_blocks(&mut reachable)
                .await
                .change_context(RuntimeError::CatalogAccess)
                .attach_with(|| {
                    format!(
                        "operation=collect_catalog_reachable_blocks, phase=walk_column_index, table_id={}",
                        table_root.table_id
                    )
                })?;
            measurement
                .set_final_compact_blocks(table_root.table_id, reachable.len() - reachable_before);
        }

        for block_id in reachable.iter().copied() {
            validate_catalog_reachable_block(root, block_id)
                .change_context(RuntimeError::CatalogAccess)
                .attach_with(|| {
                    format!(
                        "operation=collect_catalog_reachable_blocks, phase=validate_reachable_block, block_id={block_id}"
                    )
                })?;
        }
        Ok(reachable)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "catalog checkpoint folding keeps the root, mutation batch, and caller guard explicit"
    )]
    async fn apply_table_ops(
        &self,
        mutable: &mut MutableMultiTableFile,
        table_id: TableID,
        metadata: &TableMetadata,
        root: CatalogTableRootDesc,
        table_ops: &[RowRedoKind],
        checkpoint_cts: TrxID,
        disk_guard: &PoolGuard,
        measurement: &mut CatalogCheckpointMeasurement,
    ) -> RuntimeOrFatalResult<(CatalogTableRootDesc, bool)> {
        let base_rows = self
            .load_rows_from_root(metadata, disk_guard, root, measurement)
            .await?;
        let before_row_count = base_rows.len();
        let mut folded = CatalogFoldedRows::from_base_rows(metadata, base_rows)
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=apply_catalog_table_ops, table_id={table_id}"))?;

        for kind in table_ops {
            match kind {
                RowRedoKind::Insert(_, vals) => folded
                    .fold_insert(metadata, vals.clone())
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=apply_catalog_table_ops, phase=fold_insert, table_id={table_id}"
                        )
                    })?,
                RowRedoKind::DeleteByPrimaryKey(key) => folded
                    .fold_delete(key)
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=apply_catalog_table_ops, phase=fold_delete, table_id={table_id}"
                        )
                    })?,
                RowRedoKind::UpdateByPrimaryKey(key, update) => {
                    folded
                        .fold_update(metadata, key, update)
                        .change_context(RuntimeError::CatalogAccess)
                        .attach_with(|| {
                            format!(
                                "operation=apply_catalog_table_ops, phase=fold_update, table_id={table_id}"
                            )
                        })?;
                }
                RowRedoKind::Delete(_) | RowRedoKind::Update(..) => {
                    return Err(RuntimeOrFatalError::from(
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach(
                                "catalog checkpoint table op must be insert, delete-by-primary-key, or update-by-primary-key",
                            )
                            .change_context(RuntimeError::CatalogAccess)
                            .attach(format!(
                                "operation=apply_catalog_table_ops, table_id={table_id}"
                            )),
                    ));
                }
            }
        }

        if !folded.should_rewrite() {
            return Ok((root, false));
        }

        let output_vals = folded.materialize_output_rows();
        measurement.record_table_change(table_id, before_row_count, output_vals.len());
        if output_vals.is_empty() {
            return Ok((
                CatalogTableRootDesc::empty(table_id),
                root.checkpoint_root_block_id().is_some(),
            ));
        }

        let output_rows = output_vals
            .into_iter()
            .enumerate()
            .map(|(idx, vals)| RowRecord {
                row_id: RowID::new(idx as u64),
                vals,
            })
            .collect::<Vec<_>>();
        let new_pages =
            build_lwc_blocks_from_row_records(metadata, &output_rows).attach_with(|| {
                format!(
                    "operation=apply_catalog_table_ops, phase=build_lwc_blocks, table_id={table_id}"
                )
            })?;
        let mut new_entries = Vec::with_capacity(new_pages.len());
        for page in new_pages {
            let block_id = mutable
                .allocate_block()
                .change_context(RuntimeError::CatalogAccess)
                .attach_with(|| {
                    format!(
                        "operation=apply_catalog_table_ops, phase=allocate_lwc_block, table_id={table_id}"
                    )
                })?;
            mutable
                .write_block(block_id, page.buf)
                .await
                .map_err(|bridge| bridge.into_runtime_or_fatal(RuntimeError::CatalogAccess))
                .attach_with(|| {
                    format!(
                        "operation=apply_catalog_table_ops, phase=write_lwc_block, table_id={table_id}, block_id={block_id}, persist catalog LWC block"
                    )
                })?;
            measurement
                .table(table_id)
                .lwc_blocks_written
                .fetch_add(1, Ordering::Relaxed);
            new_entries.push(page.shape.with_block_id(block_id));
        }
        let pivot_row_id = RowID::new(output_rows.len() as u64);
        let column_index = ColumnBlockIndex::new(
            SUPER_BLOCK_ID,
            RowID::new(0),
            self.mtb.file_kind(),
            self.mtb.sparse_file(),
            &self.disk_pool,
            disk_guard,
        );
        let mut index_writer = MeasurableMutableCowFile {
            mutable,
            successful_writes: &measurement.table(table_id).index_blocks_written,
        };
        let root_block_id = column_index
            .batch_insert(
                &mut index_writer,
                &new_entries,
                pivot_row_id,
                checkpoint_cts,
            )
            .await
            .change_runtime_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=apply_catalog_table_ops, phase=build_column_index, table_id={table_id}"
                )
            })?;
        Ok((
            CatalogTableRootDesc::published(table_id, root_block_id, pivot_row_id),
            true,
        ))
    }

    async fn collect_index_entries(
        &self,
        disk_pool_guard: &PoolGuard,
        root_block_id: BlockID,
        table_id: TableID,
        measurement: &CatalogCheckpointMeasurement,
    ) -> RuntimeResult<Vec<CatalogIndexEntry>> {
        assert_ne!(
            root_block_id, SUPER_BLOCK_ID,
            "root_block_id must not reference the reserved super block",
        );
        let index = ColumnBlockIndex::new(
            root_block_id,
            RowID::MAX,
            self.mtb.file_kind(),
            self.mtb.sparse_file(),
            &self.disk_pool,
            disk_pool_guard,
        );
        let index = index.with_logical_read_counter(measurement.compact_read_counter(table_id));
        index
            .collect_leaf_entries()
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!("operation=collect_catalog_index_entries, root_block_id={root_block_id}")
            })
    }

    /// Load rows from one catalog table root.
    ///
    /// This is a disk-root reader, not a runtime catalog-table scan. The `root`
    /// must come from a checkpointed `catalog.mtb` root descriptor or from a
    /// newly prepared descriptor that is about to be published. It returns only
    /// rows encoded in that root's persisted LWC blocks.
    ///
    /// Checkpoint roots are compact catalog snapshots and must not contain
    /// delete deltas. This helper does not apply MVCC visibility rules, does not
    /// read in-memory catalog rows, and does not validate that the descriptor's
    /// `table_id` matches the supplied metadata; callers that iterate descriptor
    /// slots must enforce that outer invariant.
    ///
    /// Empty roots return no rows. Published rows are decoded with `metadata`;
    /// malformed row ids, delete deltas, or LWC payloads are surfaced as
    /// catalog payload errors.
    async fn load_rows_from_root(
        &self,
        metadata: &TableMetadata,
        disk_pool_guard: &PoolGuard,
        root: CatalogTableRootDesc,
        measurement: &CatalogCheckpointMeasurement,
    ) -> RuntimeResult<Vec<RowRecord>> {
        if root.checkpoint_root_block_id().is_none() {
            return Ok(Vec::new());
        }
        let root_block_id = root
            .checkpoint_root_block_id()
            .expect("root_block_id checked above");
        let index_reads_before = measurement
            .compact_read_counter(root.table_id)
            .load(Ordering::Relaxed);
        let entries = self
            .collect_index_entries(disk_pool_guard, root_block_id, root.table_id, measurement)
            .await?;
        let index_blocks = measurement
            .compact_read_counter(root.table_id)
            .load(Ordering::Relaxed)
            .saturating_sub(index_reads_before);
        measurement.set_final_compact_blocks(root.table_id, index_blocks + entries.len());
        let column_index = ColumnBlockIndex::new(
            root_block_id,
            root.pivot_row_id(),
            self.mtb.file_kind(),
            self.mtb.sparse_file(),
            &self.disk_pool,
            disk_pool_guard,
        );
        let column_index =
            column_index.with_logical_read_counter(measurement.compact_read_counter(root.table_id));
        let key_builder = CatalogMergeKeyBuilder::new(metadata)
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=load_catalog_rows_from_root, phase=build_primary_key, table_id={}",
                    root.table_id
                )
            })?;
        let mut rows = Vec::new();
        let mut primary_keys = FastHashSet::default();
        for entry in entries {
            let page_rows = self
                .decode_lwc_page_rows(
                    metadata,
                    disk_pool_guard,
                    &column_index,
                    &entry,
                    measurement,
                    root.table_id,
                )
                .await?;
            for row in page_rows {
                let delta = row
                    .row_id
                    .checked_sub(entry.start_row_id)
                    .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))
                    .attach_with(|| {
                        format!(
                            "catalog root row id precedes block start: row_id={}, start_row_id={}",
                            row.row_id, entry.start_row_id
                        )
                    })
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_catalog_rows_from_root, table_id={}, block_id={}",
                            root.table_id,
                            entry.block_id()
                        )
                    })?;
                if delta > u32::MAX as u64 {
                    return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                                "catalog root row delta exceeds u32: delta={delta}, row_id={}, start_row_id={}",
                                row.row_id, entry.start_row_id
                            )))
                        .change_context(RuntimeError::CatalogAccess)
                        .attach(format!(
                                "operation=load_catalog_rows_from_root, table_id={}, block_id={}",
                                root.table_id,
                                entry.block_id()
                            ));
                }
                validate_catalog_row(metadata, &row.vals, "catalog checkpoint root row")
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_catalog_rows_from_root, phase=validate_row, table_id={}, row_id={}",
                            root.table_id, row.row_id
                        )
                    })?;
                let primary_key = key_builder
                    .key_from_row(&row.vals)
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_catalog_rows_from_root, phase=decode_primary_key, table_id={}, row_id={}",
                            root.table_id, row.row_id
                        )
                    })?;
                if primary_keys.contains(&primary_key) {
                    return Err(
                        Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                            "catalog root contains duplicate primary key: key={primary_key:?}"
                        )),
                    )
                    .change_context(RuntimeError::CatalogAccess)
                    .attach(format!(
                        "operation=load_catalog_rows_from_root, table_id={}, row_id={}",
                        root.table_id, row.row_id
                    ));
                }
                primary_keys.insert(primary_key);
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn decode_lwc_page_rows(
        &self,
        metadata: &TableMetadata,
        disk_pool_guard: &PoolGuard,
        column_index: &ColumnBlockIndex<'_>,
        entry: &CatalogIndexEntry,
        measurement: &CatalogCheckpointMeasurement,
        table_id: TableID,
    ) -> RuntimeResult<Vec<RowRecord>> {
        let file_kind = self.mtb.file_kind();
        let block_id = entry.block_id();
        let persisted = PersistedLwcBlock::load(
            file_kind,
            self.mtb.sparse_file(),
            &self.disk_pool,
            disk_pool_guard,
            block_id,
        )
        .await
        .change_context(RuntimeError::CatalogAccess)
        .attach_with(|| {
            format!(
                "operation=decode_catalog_lwc_page_rows, phase=load_lwc_block, block_id={block_id}"
            )
        })?;
        measurement
            .compact_read_counter(table_id)
            .fetch_add(1, Ordering::Relaxed);
        let lwc_block = persisted.block();
        let row_count = lwc_block.row_count();
        let (delete_deltas, row_ids) = column_index
            .load_delete_deltas_and_row_ids(entry)
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=decode_catalog_lwc_page_rows, phase=load_row_shape, block_id={block_id}"
                )
            })?;
        if !delete_deltas.is_empty() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                "catalog root contains delete deltas: table_id={table_id}, block_id={block_id}, delete_count={}",
                delete_deltas.len()
            )))
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=decode_catalog_lwc_page_rows, phase=validate_delete_deltas");
        }
        if row_count != row_ids.len() {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "file={file_kind}, block=lwc_block, block_id={block_id}, \
                         row_count={row_count}, index_row_id_count={}",
                    row_ids.len()
                )),
            )
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=decode_catalog_lwc_page_rows, phase=validate_row_count");
        }
        let mut rows = Vec::with_capacity(row_count);
        for (row_idx, row_id) in row_ids.into_iter().enumerate() {
            let vals = lwc_block
                .decode_full_row_values(&metadata.col, row_idx)
                .attach_with(|| format!("file={file_kind}, block=lwc_block, block_id={block_id}"))
                .change_context(RuntimeError::CatalogAccess)
                .attach_with(|| {
                    format!(
                        "operation=decode_catalog_lwc_page_rows, phase=decode_row, block_id={block_id}, row_idx={row_idx}"
                    )
                })?;
            rows.push(RowRecord { row_id, vals });
        }
        Ok(rows)
    }

    /// Visits one selected column from every row in a projected catalog root
    /// without materializing full row payloads.
    async fn visit_projected_catalog_column<F>(
        &self,
        root: CatalogTableRootDesc,
        expected_table_id: TableID,
        column_no: usize,
        disk_pool_guard: &PoolGuard,
        measurement: &CatalogCheckpointMeasurement,
        mut visitor: F,
    ) -> RuntimeResult<()>
    where
        F: FnMut(Val) -> DataIntegrityResult<()>,
    {
        let Some(slot) = catalog_table_slot(expected_table_id) else {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "projected catalog inventory has non-catalog table id: expected_table_id={expected_table_id}"
                ))
                .change_context(RuntimeError::CatalogAccess));
        };
        if slot >= self.tables.len() || root.table_id != expected_table_id {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "projected catalog root table identity mismatch: root_table_id={}, slot={slot}, expected_table_id={expected_table_id}",
                    root.table_id
                ))
                .change_context(RuntimeError::CatalogAccess));
        }
        let metadata = self.tables[slot].metadata();
        if column_no >= metadata.col.col_count() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "projected catalog parent column is out of range: table_id={}, column_no={column_no}, column_count={}",
                    root.table_id,
                    metadata.col.col_count()
                ))
                .change_context(RuntimeError::CatalogAccess));
        }
        let Some(root_block_id) = root.checkpoint_root_block_id() else {
            return Ok(());
        };
        let index_reads_before = measurement
            .compact_read_counter(root.table_id)
            .load(Ordering::Relaxed);
        let entries = self
            .collect_index_entries(disk_pool_guard, root_block_id, root.table_id, measurement)
            .await?;
        let index_blocks = measurement
            .compact_read_counter(root.table_id)
            .load(Ordering::Relaxed)
            .saturating_sub(index_reads_before);
        measurement.set_final_compact_blocks(root.table_id, index_blocks + entries.len());
        let column_index = ColumnBlockIndex::new(
            root_block_id,
            root.pivot_row_id(),
            self.mtb.file_kind(),
            self.mtb.sparse_file(),
            &self.disk_pool,
            disk_pool_guard,
        );
        let column_index =
            column_index.with_logical_read_counter(measurement.compact_read_counter(root.table_id));
        for entry in entries {
            let block_id = entry.block_id();
            let (delete_deltas, row_ids) = column_index
                .load_delete_deltas_and_row_ids(&entry)
                .await
                .change_context(RuntimeError::CatalogAccess)
                .attach_with(|| {
                    format!(
                        "operation=validate_projected_catalog_integrity, phase=load_row_shape, table_id={}, block_id={block_id}",
                        root.table_id
                    )
                })?;
            if !delete_deltas.is_empty() {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach(format!(
                        "projected catalog root contains delete deltas: table_id={}, block_id={block_id}, delete_count={}",
                        root.table_id,
                        delete_deltas.len()
                    ))
                    .change_context(RuntimeError::CatalogAccess));
            }
            let persisted = PersistedLwcBlock::load(
                self.mtb.file_kind(),
                self.mtb.sparse_file(),
                &self.disk_pool,
                disk_pool_guard,
                block_id,
            )
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=validate_projected_catalog_integrity, phase=load_lwc_block, table_id={}, block_id={block_id}",
                    root.table_id
                )
            })?;
            measurement
                .compact_read_counter(root.table_id)
                .fetch_add(1, Ordering::Relaxed);
            let block = persisted.block();
            let block_row_count = block.row_count();
            let entry_row_count = usize::from(entry.row_count());
            if block_row_count != entry_row_count || block_row_count != row_ids.len() {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach(format!(
                        "projected catalog row count disagreement: table_id={}, block_id={block_id}, lwc_row_count={block_row_count}, entry_row_count={entry_row_count}, row_id_count={}",
                        root.table_id,
                        row_ids.len()
                    ))
                    .change_context(RuntimeError::CatalogAccess));
            }
            for row_idx in 0..block_row_count {
                let val = block
                    .decode_value(metadata.col.as_ref(), row_idx, column_no)
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=validate_projected_catalog_integrity, phase=decode_parent, table_id={}, block_id={block_id}, row_idx={row_idx}, column_no={column_no}",
                            root.table_id
                        )
                    })?;
                visitor(val)
                    .change_context(RuntimeError::CatalogAccess)
                    .attach_with(|| {
                        format!(
                            "operation=validate_projected_catalog_integrity, table_id={}, block_id={block_id}, row_idx={row_idx}",
                            root.table_id
                        )
                })?;
            }
        }
        Ok(())
    }
}

/// Static definition used to bootstrap one catalog logical table.
pub(crate) struct CatalogDefinition {
    /// Reserved catalog table id.
    pub(crate) table_id: TableID,
    /// Static metadata for the catalog table.
    pub(crate) metadata: TableMetadata,
}

struct PendingLwcBlock {
    shape: ColumnBlockEntryShape,
    buf: DirectBuf,
}

type CatalogIndexEntry = ColumnLeafEntry;

#[derive(Debug)]
struct RowRecord {
    row_id: RowID,
    vals: Vec<Val>,
}

/// Prepared result of planning a catalog checkpoint root publication.
pub(crate) enum PreparedCatalogCheckpoint {
    /// A new catalog root is prepared and can still accept marker metadata.
    Published(Box<PreparedCatalogPublish>),
    /// No catalog checkpoint metadata needs publication.
    Noop {
        /// Current durable catalog replay boundary.
        catalog_replay_start_ts: TrxID,
        /// Current checkpoint-durable silent watermark overlay.
        checkpointed_silent_watermarks: Arc<FastHashMap<TableID, TableRedoReplayFloor>>,
        /// Catalog DDL transactions represented by the superseded or empty batch.
        catalog_ddl_txn_count: usize,
    },
}

impl PreparedCatalogCheckpoint {
    /// Returns whether committing this prepared result will publish a catalog root.
    #[inline]
    pub(crate) fn will_publish(&self) -> bool {
        matches!(self, PreparedCatalogCheckpoint::Published(_))
    }

    /// Catalog replay boundary projected after this prepared result commits.
    #[inline]
    pub(crate) fn catalog_replay_start_ts(&self) -> TrxID {
        match self {
            PreparedCatalogCheckpoint::Published(publish) => publish.catalog_replay_start_ts,
            PreparedCatalogCheckpoint::Noop {
                catalog_replay_start_ts,
                ..
            } => *catalog_replay_start_ts,
        }
    }

    /// Checkpoint-durable silent watermark overlay projected after commit.
    #[inline]
    pub(crate) fn checkpointed_silent_watermarks(
        &self,
    ) -> Arc<FastHashMap<TableID, TableRedoReplayFloor>> {
        match self {
            PreparedCatalogCheckpoint::Published(publish) => {
                Arc::clone(&publish.checkpointed_silent_watermarks)
            }
            PreparedCatalogCheckpoint::Noop {
                checkpointed_silent_watermarks,
                ..
            } => Arc::clone(checkpointed_silent_watermarks),
        }
    }

    /// Add a monotonic first-retained redo marker to the prepared catalog root.
    #[inline]
    pub(crate) fn apply_first_redo_log_seq(&mut self, first_redo_log_seq: u32) -> bool {
        match self {
            PreparedCatalogCheckpoint::Published(publish) => {
                publish.mutable.apply_first_redo_log_seq(first_redo_log_seq)
            }
            PreparedCatalogCheckpoint::Noop { .. } => false,
        }
    }

    /// Commit the prepared catalog root, installing projected caches after success.
    pub(crate) async fn commit(
        self,
        storage: &CatalogStorage,
    ) -> RuntimeResult<CatalogCheckpointReport> {
        match self {
            PreparedCatalogCheckpoint::Published(publish) => {
                let PreparedCatalogPublish {
                    mutable,
                    catalog_replay_start_ts,
                    checkpointed_silent_watermarks,
                    measurement,
                } = *publish;
                let (_, old_root) = mutable
                    .commit_prepared()
                    .await
                    .change_context(RuntimeError::CatalogAccess)
                    .attach("operation=commit_catalog_checkpoint")?;
                drop(old_root);
                storage.install_checkpointed_silent_watermarks(checkpointed_silent_watermarks);
                Ok(measurement.finish(CatalogCheckpointOutcome::Published {
                    catalog_replay_start_ts,
                }))
            }
            PreparedCatalogCheckpoint::Noop {
                catalog_ddl_txn_count,
                ..
            } => Ok(CatalogCheckpointReport {
                outcome: CatalogCheckpointOutcome::Noop,
                catalog_ddl_txn_count,
                table_changes: Box::new([]),
                table_io: Box::new([]),
                metadata_bytes_written: 0,
            }),
        }
    }
}

/// Mutable catalog root prepared for a checkpoint publication.
pub(crate) struct PreparedCatalogPublish {
    mutable: MutableMultiTableFile,
    catalog_replay_start_ts: TrxID,
    checkpointed_silent_watermarks: Arc<FastHashMap<TableID, TableRedoReplayFloor>>,
    measurement: CatalogCheckpointMeasurement,
}

#[inline]
fn must_catalog_table_slot(table_id: TableID) -> usize {
    catalog_table_slot(table_id).expect("built-in catalog table id must be in catalog range")
}

#[inline]
fn catalog_table_slot_checked(
    table_id: TableID,
    catalog_table_count: usize,
) -> DataIntegrityResult<usize> {
    let Some(slot) = catalog_table_slot(table_id) else {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog checkpoint redo table id is not in catalog range: table_id={table_id}"
            )),
        );
    };
    if slot >= catalog_table_count {
        return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog checkpoint redo table id out of range: table_id={table_id}, slot={slot}, catalog_table_count={catalog_table_count}"
            )));
    }
    Ok(slot)
}

fn build_lwc_blocks_from_row_records(
    metadata: &TableMetadata,
    rows: &[RowRecord],
) -> RuntimeResult<Vec<PendingLwcBlock>> {
    for row in rows {
        validate_catalog_row(metadata, &row.vals, "catalog checkpoint LWC row")
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=build_catalog_lwc_blocks, phase=validate_row, row_id={}",
                    row.row_id
                )
            })?;
    }
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut lwc_blocks = Vec::new();
    let mut builder = LwcBuilder::new(Arc::clone(&metadata.col));
    let mut builder_start = None;
    let mut builder_end = RowID::new(0);

    for row in rows {
        if builder.is_empty() {
            builder_start = Some(row.row_id);
        }
        if !builder.append_row_values(row.row_id, &row.vals) {
            if builder.is_empty() {
                return Err(
                    Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "single catalog row does not fit in LWC block: row_id={}",
                        row.row_id
                    )),
                )
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=build_catalog_lwc_blocks, phase=append_row");
            }
            let start_row_id = builder_start
                .take()
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("catalog LWC builder missing start row id")
                })
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=build_catalog_lwc_blocks, phase=finish_block")?;
            if builder_end <= start_row_id {
                return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                            "catalog LWC builder end does not advance: start_row_id={start_row_id}, end_row_id={builder_end}"
                        )))
                    .change_context(RuntimeError::CatalogAccess)
                    .attach("operation=build_catalog_lwc_blocks, phase=finish_block");
            }
            let shape = ColumnBlockEntryShape::new(
                start_row_id,
                builder_end,
                builder.row_ids().to_vec(),
                Vec::new(),
            );
            let buf = builder
                .build(shape.row_shape_fingerprint())
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=build_catalog_lwc_blocks, phase=encode_block")?;
            lwc_blocks.push(PendingLwcBlock { shape, buf });

            builder = LwcBuilder::new(Arc::clone(&metadata.col));
            builder_start = Some(row.row_id);
            if !builder.append_row_values(row.row_id, &row.vals) {
                return Err(
                    Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "single catalog row does not fit in LWC block: row_id={}",
                        row.row_id
                    )),
                )
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=build_catalog_lwc_blocks, phase=append_row");
            }
        }
        builder_end = row.row_id.saturating_add(1);
    }

    if !builder.is_empty() {
        let start_row_id = builder_start
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach("catalog LWC builder missing final start row id")
            })
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=build_catalog_lwc_blocks, phase=finish_final_block")?;
        if builder_end <= start_row_id {
            return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "final catalog LWC builder end does not advance: start_row_id={start_row_id}, end_row_id={builder_end}"
                    )))
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=build_catalog_lwc_blocks, phase=finish_final_block");
        }
        let shape = ColumnBlockEntryShape::new(
            start_row_id,
            builder_end,
            builder.row_ids().to_vec(),
            Vec::new(),
        );
        let buf = builder
            .build(shape.row_shape_fingerprint())
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=build_catalog_lwc_blocks, phase=encode_final_block")?;
        lwc_blocks.push(PendingLwcBlock { shape, buf });
    }
    Ok(lwc_blocks)
}

fn validate_catalog_reachable_block(
    root: &MultiTableActiveRoot,
    block_id: BlockID,
) -> DataIntegrityResult<()> {
    let idx = usize::from(block_id);
    if idx >= root.alloc_map.len() {
        return Err(
            Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                "file={}, root_ts={}, block_id={block_id}, alloc_map_len={}",
                FileKind::CatalogMultiTableFile,
                root.root_ts,
                root.alloc_map.len()
            )),
        );
    }
    if !root.alloc_map.is_allocated(idx) {
        return Err(
            Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                "file={}, root_ts={}, block_id={block_id}, allocation bit is not set",
                FileKind::CatalogMultiTableFile,
                root.root_ts
            )),
        );
    }
    Ok(())
}

fn validate_catalog_row(
    metadata: &TableMetadata,
    row: &[Val],
    context: &'static str,
) -> DataIntegrityResult<()> {
    if row.len() != metadata.col.col_count() {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "{context} value count {} does not match column count {}",
                row.len(),
                metadata.col.col_count()
            )),
        );
    }
    for (idx, val) in row.iter().enumerate() {
        if !metadata.col.col_type_match(idx, val) {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("{context} column type mismatch: column_no={idx}")));
        }
    }
    if metadata == &catalog_definition_of_tables().metadata {
        table_object_from_vals(row)?;
    } else if metadata == &catalog_definition_of_columns().metadata {
        column_object_from_vals(row)?;
    } else if metadata == &catalog_definition_of_indexes().metadata {
        index_object_from_vals(row)?;
    } else if metadata == &catalog_definition_of_table_descriptors().metadata {
        table_descriptor_object_from_vals(row)?;
    } else if metadata == &catalog_definition_of_table_replay_silent_watermarks().metadata {
        table_replay_silent_watermark_object_from_vals(row)?;
    } else if metadata == &catalog_definition_of_table_bindings().metadata {
        table_binding_object_from_vals(row)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::{BufferPool, PoolGuards, PoolRole};
    use crate::catalog::tests::{open_catalog_test_engine, table1, table2};
    use crate::catalog::{Catalog, USER_TABLE_ID_START};
    use crate::catalog::{
        CatalogCheckpointBatch, CatalogCheckpointScanStopReason, CreateIndexDefinition,
        CreateTableDefinition, DescriptorUpdate, DropIndexDefinition, IndexID,
        ManagedCreateTableDefinition, ManagedTableInterpreter, StorageColumnFlags,
        StorageColumnSpec, StorageTableDefinition, StorageTableSpec, TableBinding,
    };
    use crate::catalog::{CatalogSelectKey, catalog_key_from_active_ordinal};
    use crate::error::{
        DataIntegrityError, DiscloseResultExt, Result, RuntimeError, RuntimeOrFatalError,
    };
    use crate::file::BlockKey;
    use crate::file::multi_table_file::publish_first_redo_log_seq_for_test as publish_mtb_first_redo_log_seq_for_test;
    use crate::file::multi_table_file::{
        CATALOG_MTB_FILE_ID, CatalogTableRootState, MutableMultiTableFile,
    };
    use crate::id::{BlockID, PageID};
    use crate::index::{ColumnBlockIndex, ColumnDeleteDeltaPatch};
    use crate::lock::{LockMode, LockResource};
    use crate::log::redo::{DDLRedo, RowRedoKind};
    use crate::row::ops::UpdateCol;
    use crate::session::tests::begin_test_mandatory_private_trx;
    use crate::session::{ManagedTableOps, MandatoryOperationGuard, Session};
    use crate::trx::PrivateTransaction;
    use crate::value::{Val, ValKind};
    use std::convert::Infallible;
    use std::result::Result as StdResult;
    use tempfile::TempDir;

    struct ProjectedIntegrityInterpreter;

    impl ManagedTableInterpreter for ProjectedIntegrityInterpreter {
        type Error = Infallible;

        fn create_table(
            &mut self,
            _source: &[u8],
        ) -> StdResult<ManagedCreateTableDefinition, Self::Error> {
            Ok(ManagedCreateTableDefinition::new(
                CreateTableDefinition::new(
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::I32,
                        StorageColumnFlags::empty(),
                    )]),
                    Vec::new(),
                ),
                vec![0x29],
                Box::<[TableBinding]>::default(),
            ))
        }

        fn create_index(
            &mut self,
            _source: &[u8],
            _previous_descriptor: &[u8],
            _current_schema: &StorageTableDefinition,
            _proposed_index_id: IndexID,
        ) -> StdResult<DescriptorUpdate<CreateIndexDefinition>, Self::Error> {
            unreachable!("projected-integrity test does not create an index")
        }

        fn drop_index(
            &mut self,
            _source: &[u8],
            _previous_descriptor: &[u8],
            _current_schema: &StorageTableDefinition,
        ) -> StdResult<DescriptorUpdate<DropIndexDefinition>, Self::Error> {
            unreachable!("projected-integrity test does not drop an index")
        }
    }

    /// Focused mandatory/private ownership harness for catalog accessor tests.
    pub(crate) struct CatalogTestTransaction {
        operation: MandatoryOperationGuard,
        trx: Option<PrivateTransaction>,
    }

    impl CatalogTestTransaction {
        /// Return the active private transaction.
        pub(crate) fn trx(&mut self) -> &mut PrivateTransaction {
            self.trx
                .as_mut()
                .expect("catalog test transaction must remain active")
        }

        /// Commit catalog changes and finish the mandatory test operation.
        pub(crate) async fn commit(mut self, ddl: DDLRedo) -> TrxID {
            let mut trx = self
                .trx
                .take()
                .expect("catalog test transaction must remain active");
            trx.install_ddl_redo(ddl);
            let cts = trx
                .commit_catalog_ddl()
                .await
                .expect("catalog test transaction must commit");
            self.operation.assert_finish_ready();
            self.operation.finish();
            cts
        }

        /// Roll back catalog changes and finish the mandatory test operation.
        pub(crate) async fn rollback(mut self) {
            self.trx
                .take()
                .expect("catalog test transaction must remain active")
                .rollback_catalog_ddl()
                .await
                .expect("catalog test transaction must roll back");
            self.operation.assert_finish_ready();
            self.operation.finish();
        }
    }

    /// Begin one focused catalog accessor transaction.
    pub(crate) fn begin_catalog_test_trx(session: &Session) -> CatalogTestTransaction {
        let (operation, mut trx) = begin_test_mandatory_private_trx(session);
        for slot in 0..CATALOG_TABLE_ROOT_DESC_COUNT {
            let table_id = catalog_table_id_from_slot(slot);
            trx.acquire_lock_immediate_for_test(
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
            );
            trx.acquire_lock_immediate_for_test(
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
            );
        }
        CatalogTestTransaction {
            operation,
            trx: Some(trx),
        }
    }

    /// Publish a metadata-only catalog root with a test-controlled redo retention marker.
    pub(crate) async fn publish_first_redo_log_seq_for_test(
        storage: &CatalogStorage,
        first_redo_log_seq: u32,
    ) -> Result<()> {
        publish_mtb_first_redo_log_seq_for_test(
            &storage.mtb,
            storage.table_fs.background_writes(),
            first_redo_log_seq,
        )
        .await
    }

    fn expect_runtime_report(error: RuntimeOrFatalError) -> Report<RuntimeError> {
        match error {
            RuntimeOrFatalError::Runtime(report) => report,
            RuntimeOrFatalError::Fatal(report) => {
                panic!("expected Runtime catalog failure, got Fatal: {report:?}")
            }
        }
    }
    fn metadata_only_batch(replay_start_ts: TrxID) -> CatalogCheckpointBatch {
        CatalogCheckpointBatch {
            replay_start_ts,
            safe_cts: replay_start_ts,
            first_retained_file_seq: 0,
            sealed_redo_segments: Vec::new(),
            catalog_ops: Vec::new(),
            catalog_ddl_txn_count: 0,
            stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
        }
    }

    async fn apply_metadata_only_checkpoint(catalog: &Catalog) -> Result<CatalogCheckpointReport> {
        let storage = &catalog.storage;
        let replay_start_ts = storage.checkpoint_snapshot().catalog_replay_start_ts;
        let disk_guard = storage.disk_pool.create_base_guard();
        catalog
            .apply_checkpoint_batch(metadata_only_batch(replay_start_ts), &disk_guard)
            .await
            .disclose()
    }

    fn checkpoint_batch_with_ops(
        storage: &CatalogStorage,
        catalog_ops: Vec<CatalogRedoEntry>,
    ) -> CatalogCheckpointBatch {
        let replay_start_ts = storage.checkpoint_snapshot().catalog_replay_start_ts;
        CatalogCheckpointBatch {
            replay_start_ts,
            safe_cts: replay_start_ts,
            first_retained_file_seq: 0,
            sealed_redo_segments: Vec::new(),
            catalog_ops,
            catalog_ddl_txn_count: 0,
            stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
        }
    }

    fn catalog_column_insert(
        table_id: TableID,
        column_no: u16,
        _name_len: usize,
    ) -> CatalogRedoEntry {
        CatalogRedoEntry {
            table_id: TABLE_ID_COLUMNS,
            kind: RowRedoKind::Insert(
                PageID::new(0),
                vec![
                    Val::from(table_id),
                    Val::from(u32::from(column_no)),
                    Val::from(column_no),
                    Val::from(ValKind::U64 as u32),
                    Val::from(0u32),
                ],
            ),
        }
    }

    fn catalog_column_row_record(
        row_id: RowID,
        table_id: TableID,
        column_no: u16,
        _name_len: usize,
    ) -> RowRecord {
        RowRecord {
            row_id,
            vals: vec![
                Val::from(table_id),
                Val::from(u32::from(column_no)),
                Val::from(column_no),
                Val::from(ValKind::U64 as u32),
                Val::from(0u32),
            ],
        }
    }

    fn catalog_index_row_record(
        row_id: RowID,
        table_id: TableID,
        index_id: u32,
        key_spec_len: usize,
    ) -> RowRecord {
        let key_count = key_spec_len.saturating_sub(3) / 5;
        let mut key_spec = Vec::with_capacity(3 + key_count * 5);
        key_spec.push(1);
        key_spec.extend_from_slice(&(key_count as u16).to_le_bytes());
        for column_id in 0..key_count as u32 {
            key_spec.extend_from_slice(&column_id.to_le_bytes());
            key_spec.push(0);
        }
        RowRecord {
            row_id,
            vals: vec![
                Val::from(table_id),
                Val::from(index_id),
                Val::from(index_id as u16),
                Val::from(0u32),
                Val::from(key_spec),
            ],
        }
    }

    fn catalog_table_vals(table_id: TableID, index_slot_count: u16) -> Vec<Val> {
        vec![
            Val::from(table_id),
            Val::from(0u64),
            Val::from(0u64),
            Val::from(0u64),
            Val::from(u32::from(index_slot_count)),
        ]
    }

    fn catalog_measurement(storage: &CatalogStorage) -> CatalogCheckpointMeasurement {
        CatalogCheckpointMeasurement::new(&storage.checkpoint_snapshot().meta.table_roots, 0)
    }

    async fn catalog_root_rows(storage: &CatalogStorage, table_id: TableID) -> Vec<RowRecord> {
        let root =
            storage.checkpoint_snapshot().meta.table_roots[must_catalog_table_slot(table_id)];
        let table = storage.get_catalog_table(table_id).unwrap();
        let disk_pool_guard = storage.disk_pool.create_base_guard();
        let measurement = catalog_measurement(storage);
        storage
            .load_rows_from_root(table.metadata(), &disk_pool_guard, root, &measurement)
            .await
            .unwrap()
    }

    async fn assert_compact_catalog_root(
        storage: &CatalogStorage,
        table_id: TableID,
    ) -> Vec<RowRecord> {
        let root =
            storage.checkpoint_snapshot().meta.table_roots[must_catalog_table_slot(table_id)];
        let rows = catalog_root_rows(storage, table_id).await;
        for (idx, row) in rows.iter().enumerate() {
            assert_eq!(row.row_id, RowID::new(idx as u64));
        }
        if root.checkpoint_root_block_id().is_none() {
            assert_eq!(root.pivot_row_id(), RowID::new(0));
            assert!(rows.is_empty());
            return rows;
        }
        assert_eq!(root.pivot_row_id(), RowID::new(rows.len() as u64));
        let root_block_id = root.checkpoint_root_block_id().unwrap();
        let disk_pool_guard = storage.disk_pool.create_base_guard();
        let measurement = catalog_measurement(storage);
        let entries = storage
            .collect_index_entries(&disk_pool_guard, root_block_id, table_id, &measurement)
            .await
            .unwrap();
        assert!(!entries.is_empty());
        assert_eq!(entries[0].start_row_id, RowID::new(0));
        for pair in entries.windows(2) {
            assert_eq!(pair[1].start_row_id, pair[0].end_row_id());
        }
        assert_eq!(entries.last().unwrap().end_row_id(), root.pivot_row_id());
        let index = ColumnBlockIndex::new(
            root_block_id,
            root.pivot_row_id(),
            storage.mtb.file_kind(),
            storage.mtb.sparse_file(),
            &storage.disk_pool,
            &disk_pool_guard,
        );
        for entry in entries {
            let (delete_deltas, _) = index.load_delete_deltas_and_row_ids(&entry).await.unwrap();
            assert!(delete_deltas.is_empty());
        }
        rows
    }

    async fn build_unpublished_catalog_root(
        storage: &CatalogStorage,
        table_id: TableID,
        metadata: &TableMetadata,
        rows: &[RowRecord],
        delete_deltas: Option<&[u32]>,
    ) -> CatalogTableRootDesc {
        let mut mutable =
            MutableMultiTableFile::fork(&storage.mtb, storage.table_fs.background_writes());
        let pages = build_lwc_blocks_from_row_records(metadata, rows).unwrap();
        let mut entries = Vec::with_capacity(pages.len());
        for page in pages {
            let block_id = mutable.allocate_block().unwrap();
            mutable.write_block(block_id, page.buf).await.unwrap();
            entries.push(page.shape.with_block_id(block_id));
        }
        let disk_pool_guard = storage.disk_pool.create_base_guard();
        let pivot_row_id = RowID::new(rows.len() as u64);
        let column_index = ColumnBlockIndex::new(
            SUPER_BLOCK_ID,
            RowID::new(0),
            storage.mtb.file_kind(),
            storage.mtb.sparse_file(),
            &storage.disk_pool,
            &disk_pool_guard,
        );
        let mut root_block_id = column_index
            .batch_insert(&mut mutable, &entries, pivot_row_id, TrxID::new(77))
            .await
            .unwrap();
        if let Some(delete_deltas) = delete_deltas {
            let column_index = ColumnBlockIndex::new(
                root_block_id,
                pivot_row_id,
                storage.mtb.file_kind(),
                storage.mtb.sparse_file(),
                &storage.disk_pool,
                &disk_pool_guard,
            );
            root_block_id = column_index
                .batch_replace_delete_deltas(
                    &mut mutable,
                    &[ColumnDeleteDeltaPatch {
                        start_row_id: RowID::new(0),
                        delete_deltas,
                    }],
                    TrxID::new(78),
                )
                .await
                .unwrap();
        }
        CatalogTableRootDesc::published(table_id, root_block_id, pivot_row_id)
    }

    async fn assert_checkpoint_rejects_delete_key(
        engine_name: &str,
        key: CatalogSelectKey,
        expected_message: &str,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = open_catalog_test_engine(main_dir, Some(engine_name)).await;

        let storage = &engine.inner().core.catalog().storage;
        let replay_start_ts = storage.checkpoint_snapshot().catalog_replay_start_ts;
        let batch = CatalogCheckpointBatch {
            replay_start_ts,
            safe_cts: replay_start_ts,
            first_retained_file_seq: 0,
            sealed_redo_segments: Vec::new(),
            catalog_ops: vec![CatalogRedoEntry {
                table_id: TABLE_ID_TABLES,
                kind: RowRedoKind::DeleteByPrimaryKey(key),
            }],
            catalog_ddl_txn_count: 0,
            stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
        };

        let err = expect_runtime_report(
            engine
                .inner()
                .core
                .catalog()
                .apply_checkpoint_batch(batch, engine.inner().core.pools.pool_guards().disk_guard())
                .await
                .unwrap_err(),
        );

        assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
        assert_eq!(
            err.downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidPayload)
        );
        let report = format!("{err:?}");
        assert!(report.contains(expected_message), "{report}");
        let current_replay_start_ts = storage.checkpoint_snapshot().catalog_replay_start_ts;
        assert_eq!(current_replay_start_ts, replay_start_ts);
    }

    #[test]
    fn test_static_catalog_definitions_expose_one_primary_key() {
        for CatalogDefinition { table_id, metadata } in [
            catalog_definition_of_tables(),
            catalog_definition_of_columns(),
            catalog_definition_of_indexes(),
            catalog_definition_of_table_replay_silent_watermarks(),
        ] {
            let primary_keys = metadata
                .idx
                .active_indexes()
                .filter(|(_, index_spec)| index_spec.primary_key())
                .collect::<Vec<_>>();
            assert_eq!(
                primary_keys.len(),
                1,
                "catalog table {table_id} must expose exactly one primary key"
            );
            for key in &primary_keys[0].1.keys {
                assert!(
                    !metadata.col.nullable(usize::from(key.column_ordinal)),
                    "catalog table {table_id} primary key column {} must be non-null",
                    key.column_ordinal
                );
            }
        }
    }

    #[test]
    fn test_catalog_leaf_validators_return_data_integrity_reports() {
        let err = catalog_table_slot_checked(TABLE_ID_TABLES, 0).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);

        let metadata = &catalog_definition_of_tables().metadata;
        let err = validate_catalog_row(metadata, &[], "catalog test row").unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
        let report = format!("{err:?}");
        assert!(report.contains("catalog test row"), "{report}");

        let err = table_replay_silent_watermark_object_from_vals(&[Val::from(1u32)]).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
        let report = format!("{err:?}");
        assert!(report.contains("table_id"), "{report}");
        assert!(report.contains("index 0"), "{report}");

        let err = table_replay_silent_watermark_object_from_vals(&[Val::from(1u64)]).unwrap_err();
        assert_eq!(*err.current_context(), DataIntegrityError::InvalidPayload);
        let report = format!("{err:?}");
        assert!(report.contains("heap_redo_start_ts"), "{report}");
        assert!(report.contains("index 1"), "{report}");
    }

    #[test]
    fn test_bootstrap_rejects_empty_catalog_root_table_id_mismatch() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-empty-root-mismatch")).await;

            let storage = &engine.inner().core.catalog().storage;
            let mut snapshot = storage.checkpoint_snapshot();
            let root = &mut snapshot.meta.table_roots[0];
            assert_eq!(root.state, CatalogTableRootState::Empty);
            root.table_id = TABLE_ID_COLUMNS;

            let guards = PoolGuards::builder()
                .push(PoolRole::Meta, storage.meta_pool.create_base_guard())
                .push(PoolRole::Disk, storage.disk_pool.create_base_guard())
                .build();
            let err = storage
                .bootstrap_from_checkpoint(&snapshot, &guards, false)
                .await
                .unwrap_err();

            assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("catalog root table id mismatch"),
                "{report}"
            );
            assert!(
                report.contains(&format!("root_table_id={TABLE_ID_COLUMNS}")),
                "{report}"
            );
            assert!(report.contains("slot_idx=0"), "{report}");
            assert!(
                report.contains("operation=bootstrap_catalog, phase=validate_table_root"),
                "{report}"
            );
        });
    }

    #[test]
    fn test_catalog_checkpoint_rejects_out_of_range_redo_table_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, Some("catalog-redo-table-range")).await;

            let storage = &engine.inner().core.catalog().storage;
            let replay_start_ts = storage.checkpoint_snapshot().catalog_replay_start_ts;
            let invalid_table_id = catalog_table_id_from_slot(CATALOG_TABLE_ROOT_DESC_COUNT);
            let batch = CatalogCheckpointBatch {
                replay_start_ts,
                safe_cts: replay_start_ts,
                first_retained_file_seq: 0,
                sealed_redo_segments: Vec::new(),
                catalog_ops: vec![CatalogRedoEntry {
                    table_id: invalid_table_id,
                    kind: RowRedoKind::Insert(PageID::new(0), Vec::new()),
                }],
                catalog_ddl_txn_count: 0,
                stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
            };

            let err = expect_runtime_report(
                engine
                    .inner()
                    .core
                    .catalog()
                    .apply_checkpoint_batch(
                        batch,
                        engine.inner().core.pools.pool_guards().disk_guard(),
                    )
                    .await
                    .unwrap_err(),
            );

            assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("catalog checkpoint redo table id out of range"),
                "{report}"
            );
            assert!(
                report.contains(&format!("table_id={invalid_table_id}")),
                "{report}"
            );
            assert!(
                report.contains(&format!("slot={}", CATALOG_TABLE_ROOT_DESC_COUNT)),
                "{report}"
            );
            assert!(
                report.contains(&format!(
                    "catalog_table_count={}",
                    CATALOG_TABLE_ROOT_DESC_COUNT
                )),
                "{report}"
            );
            let current_replay_start_ts = storage.checkpoint_snapshot().catalog_replay_start_ts;
            assert_eq!(current_replay_start_ts, replay_start_ts);
        });
    }

    #[test]
    fn test_catalog_checkpoint_rejects_projected_orphan_before_publication() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(
                temp_dir.path().to_path_buf(),
                Some("catalog-projected-parent-orphan"),
            )
            .await;
            let storage = &engine.inner().core.catalog().storage;
            let before = storage.checkpoint_snapshot();
            let before_watermarks = storage.checkpointed_silent_watermarks();
            let orphan = USER_TABLE_ID_START + 77;
            let batch =
                checkpoint_batch_with_ops(storage, vec![catalog_column_insert(orphan, 0, 0)]);

            let prepared = storage
                .prepare_checkpoint_batch(
                    batch,
                    engine.inner().core.catalog().curr_next_table_id(),
                    engine.inner().core.pools.pool_guards().disk_guard(),
                )
                .await;
            let err = match prepared {
                Ok(_) => panic!("projected catalog orphan must fail checkpoint preparation"),
                Err(err) => expect_runtime_report(err),
            };
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            let report = format!("{err:?}");
            assert!(report.contains("view=projected"), "{report}");
            assert!(report.contains("catalog.columns"), "{report}");
            assert!(report.contains(&format!("table_id={orphan}")), "{report}");

            let after = storage.checkpoint_snapshot();
            assert_eq!(after.meta_block_id, before.meta_block_id);
            assert_eq!(
                after.catalog_replay_start_ts,
                before.catalog_replay_start_ts
            );
            assert_eq!(after.meta.table_roots, before.meta.table_roots);
            assert!(Arc::ptr_eq(
                &storage.checkpointed_silent_watermarks(),
                &before_watermarks
            ));
        });
    }

    #[test]
    fn test_catalog_checkpoint_rejects_delete_key_non_primary_key() {
        smol::block_on(async {
            assert_checkpoint_rejects_delete_key(
                "catalog-delete-key-non-primary",
                catalog_key_from_active_ordinal(1, vec![Val::from(USER_TABLE_ID_START)]),
                "catalog checkpoint delete key is not primary key",
            )
            .await;
        });
    }

    #[test]
    fn test_catalog_checkpoint_rejects_delete_key_value_count_mismatch() {
        smol::block_on(async {
            assert_checkpoint_rejects_delete_key(
                    "catalog-delete-key-value-count",
                    catalog_key_from_active_ordinal(
                        0,
                        vec![Val::from(USER_TABLE_ID_START), Val::from(0u16)],
                    ),
                    "catalog checkpoint delete key value count 2 does not match primary key column count 1",
                )
                .await;
        });
    }

    #[test]
    fn test_catalog_lwc_direct_building_does_not_allocate_meta_pages() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, Some("catalog-lwc-direct-build")).await;

            let storage = &engine.inner().core.catalog().storage;
            let catalog_table = storage.get_catalog_table(TABLE_ID_COLUMNS).unwrap();
            let metadata = catalog_table.metadata();
            let table_id = USER_TABLE_ID_START + 101;

            let allocated_before = storage.meta_pool.allocated();
            let rows = vec![
                catalog_column_row_record(RowID::new(0), table_id, 0, 16),
                catalog_column_row_record(RowID::new(1), table_id, 1, 24),
            ];
            let blocks = build_lwc_blocks_from_row_records(metadata, &rows)
                .expect("small rows should build directly");
            assert!(!blocks.is_empty());
            assert_eq!(storage.meta_pool.allocated(), allocated_before);

            let index_catalog_table = storage.get_catalog_table(TABLE_ID_INDEXES).unwrap();
            let index_metadata = index_catalog_table.metadata();
            let oversized_row =
                catalog_index_row_record(RowID::new(0), table_id, 0, u16::MAX as usize);
            let result = build_lwc_blocks_from_row_records(index_metadata, &[oversized_row]);
            let err = match result {
                Ok(_) => panic!("oversized row should fail LWC block build"),
                Err(err) => err,
            };
            assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("single catalog row does not fit in LWC block: row_id=0"),
                "{report}"
            );
            assert!(
                report.contains("operation=build_catalog_lwc_blocks, phase=append_row"),
                "{report}"
            );
            assert_eq!(storage.meta_pool.allocated(), allocated_before);

            assert_eq!(storage.meta_pool.allocated(), allocated_before);
        });
    }

    #[test]
    fn test_catalog_lwc_rows_are_validated_before_trusted_builder() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U8,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .expect("valid table metadata");

        for (case, value) in [
            ("wrong kind", Val::I16(7)),
            ("invalid nullability", Val::Null),
        ] {
            let rows = [RowRecord {
                row_id: RowID::new(0),
                vals: vec![value],
            }];
            let err = match build_lwc_blocks_from_row_records(&metadata, &rows) {
                Ok(_) => panic!("{case} must fail catalog row validation"),
                Err(err) => err,
            };
            assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload),
                "case={case}"
            );
            let report = format!("{err:?}");
            assert!(report.contains("catalog checkpoint LWC row"), "{report}");
            assert!(report.contains("column_no=0"), "{report}");
        }
    }

    #[test]
    fn test_catalog_root_loader_rejects_delete_deltas() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-root-delete-deltas")).await;

            let storage = &engine.inner().core.catalog().storage;
            let table = storage.get_catalog_table(TABLE_ID_TABLES).unwrap();
            let table_id = USER_TABLE_ID_START + 111;
            let rows = vec![RowRecord {
                row_id: RowID::new(0),
                vals: catalog_table_vals(table_id, 0),
            }];
            let root = build_unpublished_catalog_root(
                storage,
                TABLE_ID_TABLES,
                table.metadata(),
                &rows,
                Some(&[0]),
            )
            .await;

            let disk_pool_guard = storage.disk_pool.create_base_guard();
            let measurement = catalog_measurement(storage);
            let err = storage
                .load_rows_from_root(table.metadata(), &disk_pool_guard, root, &measurement)
                .await
                .unwrap_err();
            assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("catalog root contains delete deltas"),
                "{report}"
            );
            assert!(
                report.contains(&format!("table_id={TABLE_ID_TABLES}")),
                "{report}"
            );
            assert!(report.contains("delete_count=1"), "{report}");
        });
    }

    #[test]
    fn test_catalog_root_loader_rejects_duplicate_primary_keys() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-root-duplicate-pk")).await;

            let storage = &engine.inner().core.catalog().storage;
            let table = storage.get_catalog_table(TABLE_ID_TABLES).unwrap();
            let table_id = USER_TABLE_ID_START + 112;
            let rows = vec![
                RowRecord {
                    row_id: RowID::new(0),
                    vals: catalog_table_vals(table_id, 0),
                },
                RowRecord {
                    row_id: RowID::new(1),
                    vals: catalog_table_vals(table_id, 1),
                },
            ];
            let root = build_unpublished_catalog_root(
                storage,
                TABLE_ID_TABLES,
                table.metadata(),
                &rows,
                None,
            )
            .await;

            let disk_pool_guard = storage.disk_pool.create_base_guard();
            let measurement = catalog_measurement(storage);
            let err = storage
                .load_rows_from_root(table.metadata(), &disk_pool_guard, root, &measurement)
                .await
                .unwrap_err();

            assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(report.contains("duplicate primary key"), "{report}");
        });
    }

    #[test]
    fn test_catalog_metadata_only_checkpoint_reclaims_displaced_meta_block() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir.clone(), Some("catalog-meta-reclaim")).await;

            let storage = &engine.inner().core.catalog().storage;
            let before_root = storage.mtb.active_root_unchecked();
            let old_meta_block_id = before_root.meta_block_id;
            let before_allocated = before_root.alloc_map.allocated();

            apply_metadata_only_checkpoint(engine.inner().core.catalog())
                .await
                .unwrap();

            let after_root = storage.mtb.active_root_unchecked();
            assert_ne!(after_root.meta_block_id, old_meta_block_id);
            assert!(
                after_root
                    .alloc_map
                    .is_allocated(usize::from(SUPER_BLOCK_ID))
            );
            assert!(
                after_root
                    .alloc_map
                    .is_allocated(usize::from(after_root.meta_block_id))
            );
            assert!(
                !after_root
                    .alloc_map
                    .is_allocated(usize::from(old_meta_block_id))
            );
            assert_eq!(after_root.alloc_map.allocated(), before_allocated);
            let expected_replay_start_ts = after_root.root_ts;
            drop(engine);

            let engine = open_catalog_test_engine(main_dir, Some("catalog-meta-reclaim")).await;
            let snap = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert_eq!(snap.catalog_replay_start_ts, expected_replay_start_ts);
            assert_eq!(snap.meta.next_table_id, USER_TABLE_ID_START);
        });
    }

    #[test]
    fn test_catalog_publish_first_redo_log_seq_preserves_checkpoint_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-redo-marker-preserve")).await;

            let _ = table1(&engine).await;
            let report1 = engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            assert!(matches!(
                report1.outcome,
                CatalogCheckpointOutcome::Published { .. }
            ));
            assert_eq!(report1.catalog_ddl_txn_count, 1);
            assert!(
                report1
                    .table_changes
                    .windows(2)
                    .all(|pair| pair[0].table_id < pair[1].table_id)
            );
            assert!(
                report1
                    .table_io
                    .windows(2)
                    .all(|pair| pair[0].table_id < pair[1].table_id)
            );
            assert_eq!(
                report1.metadata_bytes_written,
                COW_FILE_PAGE_SIZE + SUPER_BLOCK_SIZE
            );

            let noop = engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            assert_eq!(noop.outcome, CatalogCheckpointOutcome::Noop);
            assert_eq!(noop.catalog_ddl_txn_count, 0);
            assert!(noop.table_changes.is_empty());
            assert!(noop.table_io.is_empty());
            assert_eq!(noop.metadata_bytes_written, 0);

            let storage = &engine.inner().core.catalog().storage;
            let before = storage.checkpoint_snapshot();

            let marker = storage.publish_first_redo_log_seq(3).await.unwrap();

            assert_eq!(marker, 3);
            let after = storage.checkpoint_snapshot();
            assert_ne!(after.meta_block_id, before.meta_block_id);
            assert_eq!(
                after.catalog_replay_start_ts,
                before.catalog_replay_start_ts
            );
            assert_eq!(after.meta.next_table_id, before.meta.next_table_id);
            assert_eq!(after.meta.table_roots, before.meta.table_roots);
            assert_eq!(after.meta.first_redo_log_seq, 3);

            let marker = storage.publish_first_redo_log_seq(2).await.unwrap();

            assert_eq!(marker, 3);
            assert_eq!(storage.checkpoint_snapshot(), after);
        });
    }

    #[test]
    fn test_catalog_metadata_only_checkpoint_validates_roots_without_rewrite() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, Some("catalog-meta-fast-path")).await;

            let _ = table1(&engine).await;
            let initial_report = engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let tables_change = initial_report
                .table_changes
                .iter()
                .find(|change| change.table_id == TABLE_ID_TABLES)
                .unwrap();
            assert_eq!(tables_change.before_row_count, 0);
            assert_eq!(tables_change.after_row_count, 1);
            let tables_io = initial_report
                .table_io
                .iter()
                .find(|stats| stats.table_id == TABLE_ID_TABLES)
                .unwrap();
            assert!(tables_io.compact_bytes_read >= COW_FILE_PAGE_SIZE);
            assert!(tables_io.final_compact_bytes >= 2 * COW_FILE_PAGE_SIZE);
            assert!(tables_io.lwc_bytes_written >= COW_FILE_PAGE_SIZE);
            assert!(tables_io.index_bytes_written >= COW_FILE_PAGE_SIZE);
            assert_eq!(tables_io.lwc_bytes_written % COW_FILE_PAGE_SIZE, 0);
            assert_eq!(tables_io.index_bytes_written % COW_FILE_PAGE_SIZE, 0);

            let storage = &engine.inner().core.catalog().storage;
            let stats_session = engine.new_session().unwrap();
            let warm_io_before = stats_session.storage_io_stats().unwrap();
            let warm_report = apply_metadata_only_checkpoint(engine.inner().core.catalog())
                .await
                .unwrap();
            let warm_io_after = stats_session.storage_io_stats().unwrap();
            let snap = storage.checkpoint_snapshot();
            let table_roots = snap.meta.table_roots;
            let allocated_before = storage.mtb.active_root_unchecked().alloc_map.allocated();
            let disk_pool_guard = storage.disk_pool.create_base_guard();
            let measurement = CatalogCheckpointMeasurement::new(&table_roots, 0);
            let mut catalog_index_blocks = BTreeSet::new();
            for root in snap.meta.table_roots {
                let Some(root_block_id) = root.checkpoint_root_block_id() else {
                    continue;
                };
                catalog_index_blocks.insert(root_block_id);
                let entries = storage
                    .collect_index_entries(
                        &disk_pool_guard,
                        root_block_id,
                        root.table_id,
                        &measurement,
                    )
                    .await
                    .unwrap();
                for entry in entries {
                    catalog_index_blocks.insert(entry.leaf_block_id);
                }
            }
            assert!(!catalog_index_blocks.is_empty());
            for block_id in &catalog_index_blocks {
                let _ = engine.inner().pools.disk.invalidate_block(
                    &disk_pool_guard,
                    CATALOG_MTB_FILE_ID,
                    *block_id,
                );
                let key = BlockKey::new(CATALOG_MTB_FILE_ID, *block_id);
                assert!(engine.inner().pools.disk.try_get_frame_id(&key).is_none());
            }
            let cached_before = engine.inner().pools.disk.allocated();

            let cold_io_before = stats_session.storage_io_stats().unwrap();
            let metadata_only_report =
                apply_metadata_only_checkpoint(engine.inner().core.catalog())
                    .await
                    .unwrap();
            let cold_io_after = stats_session.storage_io_stats().unwrap();

            assert!(matches!(
                metadata_only_report.outcome,
                CatalogCheckpointOutcome::Published { .. }
            ));
            assert!(metadata_only_report.table_changes.is_empty());
            assert_eq!(
                metadata_only_report
                    .table_io
                    .iter()
                    .map(|stats| stats.compact_bytes_read)
                    .collect::<Vec<_>>(),
                warm_report
                    .table_io
                    .iter()
                    .map(|stats| stats.compact_bytes_read)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                metadata_only_report
                    .table_io
                    .iter()
                    .map(|stats| stats.final_compact_bytes)
                    .collect::<Vec<_>>(),
                initial_report
                    .table_io
                    .iter()
                    .map(|stats| stats.final_compact_bytes)
                    .collect::<Vec<_>>()
            );
            assert!(
                metadata_only_report.table_io.iter().all(|stats| {
                    stats.lwc_bytes_written == 0 && stats.index_bytes_written == 0
                })
            );
            assert!(
                cold_io_after.table_read_requests - cold_io_before.table_read_requests
                    > warm_io_after.table_read_requests - warm_io_before.table_read_requests
            );
            assert!(engine.inner().pools.disk.allocated() > cached_before);
            let mut validated_index_blocks = 0usize;
            for block_id in catalog_index_blocks {
                let key = BlockKey::new(CATALOG_MTB_FILE_ID, block_id);
                validated_index_blocks +=
                    usize::from(engine.inner().pools.disk.try_get_frame_id(&key).is_some());
            }
            assert!(validated_index_blocks > 0);
            let after = storage.checkpoint_snapshot();
            assert_eq!(after.meta.table_roots, table_roots);
            assert_eq!(
                storage.mtb.active_root_unchecked().alloc_map.allocated(),
                allocated_before
            );
        });
    }

    #[test]
    fn test_catalog_checkpoint_decodes_projected_shared_schema_roots_once() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = open_catalog_test_engine(
                temp_dir.path().to_path_buf(),
                Some("catalog-projected-integrity-single-pass"),
            )
            .await;
            let mut session = engine.new_session().unwrap();
            session
                .create_managed_table(&[], &mut ProjectedIntegrityInterpreter)
                .await
                .unwrap();
            session.checkpoint_catalog().await.unwrap();

            let report = apply_metadata_only_checkpoint(engine.inner().core.catalog())
                .await
                .unwrap();
            for table_id in [
                TABLE_ID_TABLES,
                TABLE_ID_COLUMNS,
                TABLE_ID_TABLE_DESCRIPTORS,
            ] {
                let stats = report
                    .table_io
                    .iter()
                    .find(|stats| stats.table_id == table_id)
                    .unwrap();
                // One single-block root costs its root read, LWC read, and one
                // combined delete-delta/row-ID read. A second projected
                // validation pass would double this value.
                assert_eq!(stats.compact_bytes_read, 3 * COW_FILE_PAGE_SIZE);
                assert_eq!(stats.final_compact_bytes, 2 * COW_FILE_PAGE_SIZE);
            }
        });
    }

    #[test]
    fn test_catalog_checkpoint_canceled_ops_use_meta_only_reclamation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-canceled-fast-path")).await;

            let storage = &engine.inner().core.catalog().storage;
            let before_root = storage.mtb.active_root_unchecked();
            let old_meta_block_id = before_root.meta_block_id;
            let before_allocated = before_root.alloc_map.allocated();
            let replay_start_ts = storage.checkpoint_snapshot().catalog_replay_start_ts;
            let table_id = USER_TABLE_ID_START + 4242;
            let batch = CatalogCheckpointBatch {
                replay_start_ts,
                safe_cts: replay_start_ts,
                first_retained_file_seq: 0,
                sealed_redo_segments: Vec::new(),
                catalog_ops: vec![
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::Insert(PageID::new(0), catalog_table_vals(table_id, 0)),
                    },
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::DeleteByPrimaryKey(catalog_key_from_active_ordinal(
                            0,
                            vec![Val::from(table_id)],
                        )),
                    },
                ],
                catalog_ddl_txn_count: 0,
                stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
            };

            engine
                .inner()
                .core
                .catalog()
                .apply_checkpoint_batch(batch, engine.inner().core.pools.pool_guards().disk_guard())
                .await
                .unwrap();

            let after_root = storage.mtb.active_root_unchecked();
            assert_ne!(after_root.meta_block_id, old_meta_block_id);
            assert!(
                !after_root
                    .alloc_map
                    .is_allocated(usize::from(old_meta_block_id))
            );
            assert_eq!(after_root.alloc_map.allocated(), before_allocated);
            assert_eq!(
                after_root.table_roots[0].state,
                CatalogTableRootState::Empty
            );
        });
    }

    #[test]
    fn test_catalog_checkpoint_update_by_primary_key_updates_same_batch_insert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-update-key-same-batch-insert"))
                    .await;

            let storage = &engine.inner().core.catalog().storage;
            let table_id = USER_TABLE_ID_START + 5252;
            let batch = checkpoint_batch_with_ops(
                storage,
                vec![
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::Insert(PageID::new(0), catalog_table_vals(table_id, 0)),
                    },
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::UpdateByPrimaryKey(
                            catalog_key_from_active_ordinal(0, vec![Val::from(table_id)]),
                            vec![UpdateCol {
                                idx: 4,
                                val: Val::from(7u32),
                            }],
                        ),
                    },
                ],
            );

            engine
                .inner()
                .core
                .catalog()
                .apply_checkpoint_batch(batch, engine.inner().core.pools.pool_guards().disk_guard())
                .await
                .unwrap();

            let rows = catalog_root_rows(storage, TABLE_ID_TABLES).await;
            let matching_rows = rows
                .iter()
                .filter(|row| row.vals[0] == Val::from(table_id))
                .collect::<Vec<_>>();
            assert_eq!(matching_rows.len(), 1);
            assert_eq!(matching_rows[0].vals[4], Val::from(7u32));
            assert_compact_catalog_root(storage, TABLE_ID_TABLES).await;
        });
    }

    #[test]
    fn test_catalog_checkpoint_update_by_primary_key_rejects_primary_key_column() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, Some("catalog-update-pk-column")).await;

            let storage = &engine.inner().core.catalog().storage;
            let table_id = USER_TABLE_ID_START + 6262;
            let batch = checkpoint_batch_with_ops(
                storage,
                vec![
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::Insert(PageID::new(0), catalog_table_vals(table_id, 0)),
                    },
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::UpdateByPrimaryKey(
                            catalog_key_from_active_ordinal(0, vec![Val::from(table_id)]),
                            vec![UpdateCol {
                                idx: 0,
                                val: Val::from(table_id),
                            }],
                        ),
                    },
                ],
            );

            let err = expect_runtime_report(
                engine
                    .inner()
                    .core
                    .catalog()
                    .apply_checkpoint_batch(
                        batch,
                        engine.inner().core.pools.pool_guards().disk_guard(),
                    )
                    .await
                    .unwrap_err(),
            );

            assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("catalog checkpoint update cannot change primary key column"),
                "{report}"
            );
        });
    }

    #[test]
    fn test_catalog_checkpoint_update_by_primary_key_replaces_existing_row() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-update-key-existing-row")).await;

            let table_id = table1(&engine).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let storage = &engine.inner().core.catalog().storage;
            let batch = checkpoint_batch_with_ops(
                storage,
                vec![CatalogRedoEntry {
                    table_id: TABLE_ID_TABLES,
                    kind: RowRedoKind::UpdateByPrimaryKey(
                        catalog_key_from_active_ordinal(0, vec![Val::from(table_id)]),
                        vec![UpdateCol {
                            idx: 4,
                            val: Val::from(9u32),
                        }],
                    ),
                }],
            );

            engine
                .inner()
                .core
                .catalog()
                .apply_checkpoint_batch(batch, engine.inner().core.pools.pool_guards().disk_guard())
                .await
                .unwrap();

            let rows = catalog_root_rows(storage, TABLE_ID_TABLES).await;
            let matching_rows = rows
                .iter()
                .filter(|row| row.vals[0] == Val::from(table_id))
                .collect::<Vec<_>>();
            assert_eq!(matching_rows.len(), 1);
            assert_eq!(matching_rows[0].vals[4], Val::from(9u32));
            assert_compact_catalog_root(storage, TABLE_ID_TABLES).await;
        });
    }

    #[test]
    fn test_catalog_reclamation_rejects_unallocated_root_descriptor_before_publish() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-reclaim-invalid-root")).await;

            let storage = &engine.inner().core.catalog().storage;
            let active_before = storage.mtb.active_root_unchecked();
            let active_meta_before = active_before.meta_block_id;
            let active_root_ts_before = active_before.root_ts;
            let bogus_root_block_id = (1..active_before.alloc_map.len())
                .rev()
                .map(BlockID::from)
                .find(|block_id| !active_before.alloc_map.is_allocated(usize::from(*block_id)))
                .unwrap();

            let mut roots = storage.checkpoint_snapshot().meta.table_roots;
            roots[0] = CatalogTableRootDesc::published(
                roots[0].table_id,
                bogus_root_block_id,
                RowID::new(1),
            );

            let mut mutable =
                MutableMultiTableFile::fork(&storage.mtb, storage.table_fs.background_writes());
            mutable.apply_checkpoint_metadata(
                active_root_ts_before.saturating_add(1),
                engine.inner().core.catalog().curr_next_table_id(),
                roots,
            );
            let mut measurement = CatalogCheckpointMeasurement::new(&roots, 0);
            let err = storage
                .rebuild_catalog_alloc_map(
                    &mut mutable,
                    engine.inner().core.pools.pool_guards().disk_guard(),
                    &mut measurement,
                )
                .await
                .unwrap_err();

            assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            let active_after = storage.mtb.active_root_unchecked();
            assert_eq!(active_after.meta_block_id, active_meta_before);
            assert_eq!(active_after.root_ts, active_root_ts_before);
        });
    }

    #[test]
    fn test_catalog_checkpoint_apply_table_ops_keeps_empty_root_for_canceled_insert_batch() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-checkpoint-canceled-empty-root"))
                    .await;

            let storage = &engine.inner().core.catalog().storage;
            let table = storage.get_catalog_table(TABLE_ID_TABLES).unwrap();
            let root = CatalogTableRootDesc::empty(TABLE_ID_TABLES);
            let table_id = USER_TABLE_ID_START + 42;
            let table_ops = vec![
                RowRedoKind::Insert(PageID::new(0), catalog_table_vals(table_id, 0)),
                RowRedoKind::DeleteByPrimaryKey(catalog_key_from_active_ordinal(
                    0,
                    vec![Val::from(table_id)],
                )),
            ];
            let mut mutable =
                MutableMultiTableFile::fork(&storage.mtb, storage.table_fs.background_writes());
            let mut measurement = CatalogCheckpointMeasurement::new(
                &storage.checkpoint_snapshot().meta.table_roots,
                0,
            );

            let (next_root, blocks_changed) = storage
                .apply_table_ops(
                    &mut mutable,
                    TABLE_ID_TABLES,
                    table.metadata(),
                    root,
                    &table_ops,
                    TrxID::new(7),
                    engine.inner().core.pools.pool_guards().disk_guard(),
                    &mut measurement,
                )
                .await
                .unwrap();

            assert_eq!(next_root.state, CatalogTableRootState::Empty);
            assert!(!blocks_changed);
        });
    }

    #[test]
    fn test_catalog_checkpoint_apply_table_ops_keeps_existing_root_for_canceled_insert_batch() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(
                main_dir,
                Some("catalog-checkpoint-canceled-existing-root"),
            )
            .await;

            let _ = table1(&engine).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let storage = &engine.inner().core.catalog().storage;
            let table = storage.get_catalog_table(TABLE_ID_TABLES).unwrap();
            let root = storage.checkpoint_snapshot().meta.table_roots[0];
            assert!(root.checkpoint_root_block_id().is_some());

            let table_id = USER_TABLE_ID_START + 4242;
            let table_ops = vec![
                RowRedoKind::Insert(PageID::new(0), catalog_table_vals(table_id, 0)),
                RowRedoKind::DeleteByPrimaryKey(catalog_key_from_active_ordinal(
                    0,
                    vec![Val::from(table_id)],
                )),
            ];
            let mut mutable =
                MutableMultiTableFile::fork(&storage.mtb, storage.table_fs.background_writes());
            let mut measurement = CatalogCheckpointMeasurement::new(
                &storage.checkpoint_snapshot().meta.table_roots,
                0,
            );

            let (next_root, blocks_changed) = storage
                .apply_table_ops(
                    &mut mutable,
                    TABLE_ID_TABLES,
                    table.metadata(),
                    root,
                    &table_ops,
                    TrxID::new(8),
                    engine.inner().core.pools.pool_guards().disk_guard(),
                    &mut measurement,
                )
                .await
                .unwrap();

            assert_eq!(next_root.state, root.state);
            assert!(!blocks_changed);
        });
    }

    #[test]
    fn test_catalog_checkpoint_collect_index_entries_uses_readonly_cache() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-checkpoint-readonly-cache")).await;

            let _ = table1(&engine).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let snap = engine.inner().core.catalog().storage.checkpoint_snapshot();
            let tables_root = snap.meta.table_roots[0];
            let root_block_id = tables_root.checkpoint_root_block_id().unwrap();
            let measurement = CatalogCheckpointMeasurement::new(&snap.meta.table_roots, 0);
            let disk_pool_guard = engine
                .inner()
                .core
                .catalog()
                .storage
                .disk_pool
                .create_base_guard();

            let cached_before_first = engine.inner().pools.disk.allocated();

            let entries1 = engine
                .inner()
                .core
                .catalog()
                .storage
                .collect_index_entries(
                    &disk_pool_guard,
                    root_block_id,
                    TABLE_ID_TABLES,
                    &measurement,
                )
                .await
                .unwrap();
            assert!(!entries1.is_empty());

            let cached_after_first = engine.inner().pools.disk.allocated();
            assert!(cached_after_first >= cached_before_first);
            let root_key = BlockKey::new(CATALOG_MTB_FILE_ID, root_block_id);
            assert!(
                engine
                    .inner()
                    .pools
                    .disk
                    .try_get_frame_id(&root_key)
                    .is_some()
            );

            let entries2 = engine
                .inner()
                .core
                .catalog()
                .storage
                .collect_index_entries(
                    &disk_pool_guard,
                    root_block_id,
                    TABLE_ID_TABLES,
                    &measurement,
                )
                .await
                .unwrap();
            assert_eq!(entries2.len(), entries1.len());
            assert_eq!(engine.inner().pools.disk.allocated(), cached_after_first);
        });
    }

    #[test]
    fn test_catalog_checkpoint_rewrites_changed_table_as_compact_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(
                main_dir.clone(),
                Some("catalog-checkpoint-compact-rewrite"),
            )
            .await;

            let table1_id = table1(&engine).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let snap1 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            let tables_root1 = snap1.meta.table_roots[0];
            assert!(tables_root1.checkpoint_root_block_id().is_some());
            assert_compact_catalog_root(&engine.inner().core.catalog().storage, TABLE_ID_TABLES)
                .await;

            let table2_id = table2(&engine).await;
            let report2 = engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let tables_change = report2
                .table_changes
                .iter()
                .find(|change| change.table_id == TABLE_ID_TABLES)
                .unwrap();
            assert_eq!(tables_change.before_row_count, 1);
            assert_eq!(tables_change.after_row_count, 2);
            let tables_io = report2
                .table_io
                .iter()
                .find(|stats| stats.table_id == TABLE_ID_TABLES)
                .unwrap();
            assert!(tables_io.compact_bytes_read >= COW_FILE_PAGE_SIZE);
            assert!(tables_io.final_compact_bytes >= 2 * COW_FILE_PAGE_SIZE);
            assert!(tables_io.lwc_bytes_written >= COW_FILE_PAGE_SIZE);
            assert!(tables_io.index_bytes_written >= COW_FILE_PAGE_SIZE);

            let snap2 = engine.inner().core.catalog().storage.checkpoint_snapshot();
            let tables_root2 = snap2.meta.table_roots[0];
            let rows = assert_compact_catalog_root(
                &engine.inner().core.catalog().storage,
                TABLE_ID_TABLES,
            )
            .await;

            assert_ne!(tables_root2.state, tables_root1.state);
            assert_eq!(tables_root2.pivot_row_id(), RowID::new(rows.len() as u64));
            let active_root = engine
                .inner()
                .core
                .catalog()
                .storage
                .mtb
                .active_root_unchecked();
            let root_block_id1 = tables_root1.checkpoint_root_block_id().unwrap();
            let root_block_id2 = tables_root2.checkpoint_root_block_id().unwrap();
            assert!(
                !active_root
                    .alloc_map
                    .is_allocated(usize::from(root_block_id1))
            );
            assert!(
                active_root
                    .alloc_map
                    .is_allocated(usize::from(root_block_id2))
            );
            drop(engine);

            let recovered =
                open_catalog_test_engine(main_dir, Some("catalog-checkpoint-compact-rewrite"))
                    .await;
            let mut recovered_table_ids =
                recovered.inner().core.catalog().list_user_table_ids_now();
            recovered_table_ids.sort();
            let mut expected_table_ids = vec![table1_id, table2_id];
            expected_table_ids.sort();
            assert_eq!(recovered_table_ids, expected_table_ids);
            assert_eq!(
                assert_compact_catalog_root(
                    &recovered.inner().core.catalog().storage,
                    TABLE_ID_TABLES
                )
                .await
                .len(),
                rows.len()
            );
        });
    }

    #[test]
    fn test_catalog_checkpoint_compact_rewrite_uses_dense_row_ids_after_large_append() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-compact-large-append")).await;

            let storage = &engine.inner().core.catalog().storage;
            let table_id = USER_TABLE_ID_START + 9000;
            engine
                .inner()
                .core
                .catalog()
                .apply_checkpoint_batch(
                    checkpoint_batch_with_ops(
                        storage,
                        vec![
                            CatalogRedoEntry {
                                table_id: TABLE_ID_TABLES,
                                kind: RowRedoKind::Insert(
                                    PageID::new(0),
                                    catalog_table_vals(table_id, 0),
                                ),
                            },
                            catalog_column_insert(table_id, 0, 30_000),
                        ],
                    ),
                    engine.inner().core.pools.pool_guards().disk_guard(),
                )
                .await
                .unwrap();

            let disk_pool_guard = storage.disk_pool.create_base_guard();
            let snap1 = storage.checkpoint_snapshot();
            let columns_root1 = snap1.meta.table_roots[1];
            let measurement1 = CatalogCheckpointMeasurement::new(&snap1.meta.table_roots, 0);
            assert_eq!(columns_root1.pivot_row_id(), RowID::new(1));
            let entries1 = storage
                .collect_index_entries(
                    &disk_pool_guard,
                    columns_root1.checkpoint_root_block_id().unwrap(),
                    TABLE_ID_COLUMNS,
                    &measurement1,
                )
                .await
                .unwrap();
            assert_eq!(entries1.len(), 1);

            let second_batch = (1..=4)
                .map(|column_no| catalog_column_insert(table_id, column_no, 15_000))
                .collect();
            engine
                .inner()
                .core
                .catalog()
                .apply_checkpoint_batch(
                    checkpoint_batch_with_ops(storage, second_batch),
                    engine.inner().core.pools.pool_guards().disk_guard(),
                )
                .await
                .unwrap();

            let snap2 = storage.checkpoint_snapshot();
            let columns_root2 = snap2.meta.table_roots[1];
            let measurement2 = CatalogCheckpointMeasurement::new(&snap2.meta.table_roots, 0);
            let rows = assert_compact_catalog_root(storage, TABLE_ID_COLUMNS).await;
            assert_eq!(rows.len(), 5);
            assert_eq!(columns_root2.pivot_row_id(), RowID::new(5));
            let entries2 = storage
                .collect_index_entries(
                    &disk_pool_guard,
                    columns_root2.checkpoint_root_block_id().unwrap(),
                    TABLE_ID_COLUMNS,
                    &measurement2,
                )
                .await
                .unwrap();

            assert!(
                columns_root2.state != columns_root1.state,
                "changed catalog tables should publish a rewritten compact root"
            );
            assert_eq!(entries2[0].start_row_id, RowID::new(0));
            for pair in entries2.windows(2) {
                assert_eq!(pair[1].start_row_id, pair[0].end_row_id());
            }
            assert_eq!(
                entries2.last().unwrap().end_row_id(),
                columns_root2.pivot_row_id()
            );
        });
    }
}
