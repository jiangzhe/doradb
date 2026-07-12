mod columns;
mod indexes;
mod merge;
mod object;
mod table_replay_silent_watermarks;
pub(crate) mod tables;

use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard, PoolGuards, ReadonlyBufferPool};
use crate::catalog::storage::columns::*;
use crate::catalog::storage::indexes::*;
use crate::catalog::storage::merge::{CatalogFoldedRows, CatalogMergeKeyBuilder};
pub(crate) use crate::catalog::storage::object::*;
use crate::catalog::storage::table_replay_silent_watermarks::*;
use crate::catalog::storage::tables::*;
use crate::catalog::table::TableMetadata;
use crate::catalog::{
    CatalogCheckpointBatch, CatalogCheckpointOutcome, CatalogRedoEntry, CatalogTable,
    catalog_table_id_from_slot, catalog_table_slot,
};
use crate::error::{DataIntegrityError, Error, FileKind, Result};
use crate::file::cow_file::{MutableCowFile, SUPER_BLOCK_ID};
use crate::file::fs::FileSystem;
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MultiTableActiveRoot, MultiTableFile,
    MultiTableFileSnapshot, MutableMultiTableFile,
};
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::index::{BlockIndex, ColumnBlockEntryShape, ColumnBlockIndex, ColumnLeafEntry};
use crate::io::DirectBuf;
use crate::log::redo::RowRedoKind;
use crate::lwc::{LwcBuilder, PersistedLwcBlock};
use crate::map::{FastHashMap, FastHashSet};
use crate::quiescent::QuiescentGuard;
use crate::table::TableRedoReplayFloor;
use crate::value::Val;
use error_stack::Report;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;

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
    ) -> Result<Self> {
        let meta_pool_guard = meta_pool.pool_guard();
        let mtb = table_fs
            .open_or_create_multi_table_file(disk_pool.clone())
            .await?;
        let mtb_snapshot = mtb.load_snapshot()?;

        let mut cat: Vec<Arc<CatalogTable>> = vec![];
        for CatalogDefinition { table_id, metadata } in [
            catalog_definition_of_tables(),
            catalog_definition_of_columns(),
            catalog_definition_of_indexes(),
            catalog_definition_of_index_columns(),
            catalog_definition_of_table_replay_silent_watermarks(),
        ] {
            // Make sure catalog table ids match their dense root slots.
            assert_eq!(cat.len(), must_catalog_table_slot(*table_id));
            let metadata = Arc::new(metadata.clone());
            let blk_idx = BlockIndex::new_catalog(meta_pool.clone(), &meta_pool_guard).await?;
            let table = Arc::new(
                CatalogTable::new(
                    meta_pool.clone(),
                    &meta_pool_guard,
                    *table_id,
                    blk_idx,
                    metadata,
                )
                .await?,
            );
            cat.push(table);
        }
        debug_assert_eq!(cat.len(), CATALOG_TABLE_ROOT_DESC_COUNT);
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

    /// Accessor of `catalog.index_columns`.
    #[inline]
    pub(crate) fn index_columns(&self) -> IndexColumns<'_> {
        IndexColumns {
            table: &self.tables[must_catalog_table_slot(TABLE_ID_INDEX_COLUMNS)],
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
    pub(crate) fn checkpoint_snapshot(&self) -> Result<MultiTableFileSnapshot> {
        self.mtb.load_snapshot()
    }

    /// Publish a durable first-retained redo marker without changing catalog table roots.
    ///
    /// The marker is stored in the `catalog.mtb` root instead of a catalog row
    /// because startup must read it before redo discovery and catalog redo
    /// replay. It tells recovery that missing prefix files below the marker
    /// were intentionally truncated; ordinary catalog-table state cannot prove
    /// that until after redo has already been selected for replay.
    pub(crate) async fn publish_first_redo_log_seq(&self, first_redo_log_seq: u32) -> Result<u32> {
        let snapshot = self.mtb.load_snapshot()?;
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
        mutable.apply_first_redo_log_seq(first_redo_log_seq)?;
        mutable.reserve_publish_meta_block_reclaiming_displaced_meta(displaced_meta_block_id)?;
        let (_, old_root) = mutable.commit_prepared().await?;
        drop(old_root);
        Ok(first_redo_log_seq)
    }

    /// Bootstrap in-memory catalog rows from the latest catalog checkpoint snapshot.
    pub(crate) async fn bootstrap_from_checkpoint(
        &self,
        snapshot: &MultiTableFileSnapshot,
        guards: &PoolGuards,
        disable_dml_validation: bool,
    ) -> Result<()> {
        for (idx, root) in snapshot.meta.table_roots.iter().copied().enumerate() {
            if idx >= self.tables.len() {
                break;
            }
            if catalog_table_slot(root.table_id) != Some(idx) {
                return Err(invalid_catalog_payload(format!(
                    "catalog root table id mismatch: root_table_id={}, slot_idx={idx}",
                    root.table_id
                )));
            }
            if root.root_block_id.is_none() {
                if root.pivot_row_id != RowID::new(0) {
                    return Err(invalid_catalog_payload(format!(
                        "empty catalog root has nonzero pivot_row_id={}",
                        root.pivot_row_id
                    )));
                }
                continue;
            }
            let rows = self
                .load_rows_from_root(self.tables[idx].metadata(), guards.disk_guard(), root)
                .await?;
            for row in rows {
                self.tables[idx]
                    .insert_no_trx(guards, &row.vals, disable_dml_validation)
                    .await?;
            }
        }
        let watermarks = self
            .load_checkpointed_table_replay_silent_watermark_map(
                guards.disk_guard(),
                snapshot.meta.table_roots
                    [must_catalog_table_slot(TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS)],
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
    ) -> Result<PreparedCatalogCheckpoint> {
        let CatalogCheckpointBatch {
            replay_start_ts,
            safe_cts,
            catalog_ops,
            ..
        } = batch;
        let snapshot = self.mtb.load_snapshot()?;
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
                });
            }
            return Err(invalid_catalog_payload(format!(
                "catalog replay start mismatch: current={current_catalog_replay_start_ts}, expected={replay_start_ts}, next={next_catalog_replay_start_ts}"
            )));
        }

        // A scan can legitimately find no durable record at or after the
        // catalog replay cursor. In that case there is no new checkpoint
        // boundary to publish.
        if safe_cts < replay_start_ts {
            return Ok(PreparedCatalogCheckpoint::Noop {
                catalog_replay_start_ts: current_catalog_replay_start_ts,
                checkpointed_silent_watermarks: self.checkpointed_silent_watermarks(),
            });
        }
        let background_writes = self.table_fs.background_writes();

        let mut mutable = MutableMultiTableFile::fork(&self.mtb, background_writes);
        let mut new_roots = snapshot.meta.table_roots;
        let mut catalog_blocks_changed = false;
        if !catalog_ops.is_empty() {
            // Replay only catalog-table row operations into catalog.mtb. User
            // table row data remains owned by table files and is not folded
            // into catalog checkpoint storage.
            let mut ops_by_table: Vec<Vec<RowRedoKind>> =
                (0..self.tables.len()).map(|_| Vec::new()).collect();
            for CatalogRedoEntry { table_id, kind } in catalog_ops {
                let table_idx = catalog_table_slot_checked(table_id, ops_by_table.len())?;
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
                    )
                    .await?;
                new_roots[idx] = new_root;
                catalog_blocks_changed |= table_blocks_changed;
            }
        }

        // Publishing the metadata block advances the durable catalog replay
        // boundary even for metadata-only checkpoints, such as DML-only
        // heartbeat batches.
        mutable.apply_checkpoint_metadata(
            next_catalog_replay_start_ts,
            next_table_id,
            new_roots,
        )?;
        if catalog_blocks_changed {
            // Rewriting catalog table roots can make arbitrary old catalog
            // blocks unreachable, so rebuild the allocation map from the new
            // root graph before publishing.
            self.rebuild_catalog_alloc_map(&mut mutable).await?;
        } else {
            // Metadata-only checkpoints do not change catalog table root
            // reachability. Reclaim the displaced metadata block directly and
            // avoid reading catalog indexes just to rebuild the same map.
            mutable.reserve_publish_meta_block_reclaiming_displaced_meta(snapshot.meta_block_id)?;
        }
        let disk_pool_guard = self.disk_pool.pool_guard();
        // Load the silent replay watermark overlay from `new_roots`, not from
        // the currently durable cache. The prepared checkpoint has already
        // materialized catalog-table changes into blocks, but its metadata root
        // is not committed yet. Combined catalog checkpoint plus redo
        // truncation uses this projected overlay to plan against the same table
        // replay floors that will become checkpoint-durable if the prepared
        // root is committed.
        let checkpointed_silent_watermarks = self
            .load_checkpointed_table_replay_silent_watermark_map(
                &disk_pool_guard,
                new_roots[must_catalog_table_slot(TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS)],
            )
            .await?;
        Ok(PreparedCatalogCheckpoint::Published(Box::new(
            PreparedCatalogPublish {
                mutable,
                catalog_replay_start_ts: next_catalog_replay_start_ts,
                checkpointed_silent_watermarks: Arc::new(checkpointed_silent_watermarks),
            },
        )))
    }

    /// Apply one scanned catalog checkpoint batch into catalog storage.
    pub(crate) async fn apply_checkpoint_batch(
        &self,
        batch: CatalogCheckpointBatch,
        next_table_id: TableID,
    ) -> Result<CatalogCheckpointOutcome> {
        let prepared = self.prepare_checkpoint_batch(batch, next_table_id).await?;
        prepared.commit(self).await
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
    ) -> Result<FastHashMap<TableID, TableRedoReplayFloor>> {
        let rows = self
            .load_rows_from_root(
                self.tables[must_catalog_table_slot(TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS)]
                    .metadata(),
                disk_pool_guard,
                root,
            )
            .await?;
        let mut watermarks = FastHashMap::default();
        for row in rows {
            let obj = table_replay_silent_watermark_object_from_vals(&row.vals)?;
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
    ) -> Result<usize> {
        mutable.reserve_publish_meta_block()?;
        let reachable = self
            .collect_catalog_reachable_blocks(mutable.root())
            .await?;
        mutable.rebuild_alloc_map_from_reachable(&reachable)
    }

    async fn collect_catalog_reachable_blocks(
        &self,
        root: &MultiTableActiveRoot,
    ) -> Result<BTreeSet<BlockID>> {
        let mut reachable = BTreeSet::new();
        reachable.insert(SUPER_BLOCK_ID);
        reachable.insert(root.meta_block_id);

        let disk_pool_guard = self.disk_pool.pool_guard();
        for (idx, table_root) in root.table_roots.iter().enumerate() {
            if catalog_table_slot(table_root.table_id) != Some(idx) {
                return Err(invalid_catalog_root_invariant(
                    root.root_ts,
                    format!(
                        "catalog table root table-id mismatch: table_id={}, slot_idx={idx}",
                        table_root.table_id
                    ),
                ));
            }
            let Some(root_block_id) = table_root.checkpoint_root_block_id() else {
                if table_root.pivot_row_id != RowID::new(0) {
                    return Err(invalid_catalog_root_invariant(
                        root.root_ts,
                        format!(
                            "empty catalog root has nonzero pivot_row_id={}",
                            table_root.pivot_row_id
                        ),
                    ));
                }
                continue;
            };
            validate_catalog_reachable_block(root, root_block_id)?;
            let column_index = ColumnBlockIndex::new(
                root_block_id,
                table_root.pivot_row_id,
                self.mtb.file_kind(),
                self.mtb.sparse_file(),
                &self.disk_pool,
                &disk_pool_guard,
            );
            column_index
                .collect_reachable_blocks(&mut reachable)
                .await?;
        }

        for block_id in reachable.iter().copied() {
            validate_catalog_reachable_block(root, block_id)?;
        }
        Ok(reachable)
    }

    async fn apply_table_ops(
        &self,
        mutable: &mut MutableMultiTableFile,
        table_id: TableID,
        metadata: &TableMetadata,
        root: CatalogTableRootDesc,
        table_ops: &[RowRedoKind],
        checkpoint_cts: TrxID,
    ) -> Result<(CatalogTableRootDesc, bool)> {
        let disk_pool_guard = self.disk_pool.pool_guard();
        let base_rows = self
            .load_rows_from_root(metadata, &disk_pool_guard, root)
            .await?;
        let mut folded = CatalogFoldedRows::from_base_rows(metadata, base_rows)?;

        for kind in table_ops {
            match kind {
                RowRedoKind::Insert(vals) => folded.fold_insert(metadata, vals.clone())?,
                RowRedoKind::DeleteByPrimaryKey(key) => folded.fold_delete(key)?,
                RowRedoKind::UpdateByPrimaryKey(key, update) => {
                    folded.fold_update(metadata, key, update)?;
                }
                RowRedoKind::Delete | RowRedoKind::Update(_) => {
                    return Err(invalid_catalog_payload(
                        "catalog checkpoint table op must be insert, delete-by-primary-key, or update-by-primary-key",
                    ));
                }
            }
        }

        if !folded.should_rewrite() {
            return Ok((root, false));
        }

        let output_vals = folded.materialize_output_rows()?;
        if output_vals.is_empty() {
            return Ok((
                CatalogTableRootDesc {
                    table_id,
                    root_block_id: None,
                    pivot_row_id: RowID::new(0),
                },
                root.root_block_id.is_some() || root.pivot_row_id != RowID::new(0),
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
        let new_pages = build_lwc_blocks_from_row_records(metadata, &output_rows)?;
        let mut new_entries = Vec::with_capacity(new_pages.len());
        for page in new_pages {
            let block_id = mutable.allocate_block()?;
            mutable.write_block(block_id, page.buf).await?;
            new_entries.push(page.shape.with_block_id(block_id));
        }
        let pivot_row_id = RowID::new(output_rows.len() as u64);
        let column_index = ColumnBlockIndex::new(
            SUPER_BLOCK_ID,
            RowID::new(0),
            self.mtb.file_kind(),
            self.mtb.sparse_file(),
            &self.disk_pool,
            &disk_pool_guard,
        );
        let root_block_id = column_index
            .batch_insert(mutable, &new_entries, pivot_row_id, checkpoint_cts)
            .await?;
        let root_block_id = NonZeroU64::new(root_block_id.into())
            .ok_or_else(|| invalid_catalog_payload("catalog root block id resolved to zero"))?;
        Ok((
            CatalogTableRootDesc {
                table_id,
                root_block_id: Some(root_block_id),
                pivot_row_id,
            },
            true,
        ))
    }

    async fn collect_index_entries(
        &self,
        disk_pool_guard: &PoolGuard,
        root_block_id: BlockID,
    ) -> Result<Vec<CatalogIndexEntry>> {
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
        index.collect_leaf_entries().await
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
    /// Empty roots are represented by `root_block_id == None` and
    /// `pivot_row_id == 0`. Any other empty-root shape is treated as catalog
    /// payload corruption. Returned rows are decoded with `metadata`; malformed
    /// row ids, delete deltas, or LWC payloads are surfaced as catalog payload
    /// errors.
    async fn load_rows_from_root(
        &self,
        metadata: &TableMetadata,
        disk_pool_guard: &PoolGuard,
        root: CatalogTableRootDesc,
    ) -> Result<Vec<RowRecord>> {
        if root.root_block_id.is_none() {
            if root.pivot_row_id != RowID::new(0) {
                return Err(invalid_catalog_payload(format!(
                    "empty catalog root has nonzero pivot_row_id={}",
                    root.pivot_row_id
                )));
            }
            return Ok(Vec::new());
        }
        let root_block_id = root
            .checkpoint_root_block_id()
            .expect("root_block_id checked above");
        let entries = self
            .collect_index_entries(disk_pool_guard, root_block_id)
            .await?;
        let column_index = ColumnBlockIndex::new(
            root_block_id,
            root.pivot_row_id,
            self.mtb.file_kind(),
            self.mtb.sparse_file(),
            &self.disk_pool,
            disk_pool_guard,
        );
        let key_builder = CatalogMergeKeyBuilder::new(metadata)?;
        let mut rows = Vec::new();
        let mut primary_keys = FastHashSet::default();
        for entry in entries {
            debug_assert!(
                {
                    let delete_deltas = column_index.load_delete_deltas(&entry).await?;
                    delete_deltas.is_empty()
                },
                "catalog root should not contain delete deltas: start_row_id={}",
                entry.start_row_id
            );
            let page_rows = self
                .decode_lwc_page_rows(metadata, disk_pool_guard, &column_index, &entry)
                .await?;
            for row in page_rows {
                let delta = row.row_id.checked_sub(entry.start_row_id).ok_or_else(|| {
                    invalid_catalog_payload(format!(
                        "catalog root row id precedes block start: row_id={}, start_row_id={}",
                        row.row_id, entry.start_row_id
                    ))
                })?;
                if delta > u32::MAX as u64 {
                    return Err(invalid_catalog_payload(format!(
                        "catalog root row delta exceeds u32: delta={delta}, row_id={}, start_row_id={}",
                        row.row_id, entry.start_row_id
                    )));
                }
                validate_catalog_row(metadata, &row.vals, "catalog checkpoint root row")?;
                let primary_key = key_builder.key_from_row(&row.vals)?;
                if primary_keys.contains(&primary_key) {
                    return Err(invalid_catalog_payload(format!(
                        "catalog root contains duplicate primary key: key={primary_key:?}"
                    )));
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
    ) -> Result<Vec<RowRecord>> {
        let lwc_block = PersistedLwcBlock::load(
            self.mtb.file_kind(),
            self.mtb.sparse_file(),
            &self.disk_pool,
            disk_pool_guard,
            entry.block_id(),
        )
        .await?;
        let row_count = lwc_block.row_count();
        let row_ids = column_index.load_entry_row_ids(entry).await?;
        if row_count != row_ids.len() {
            return Err(invalid_lwc_payload(
                self.mtb.file_kind(),
                entry.block_id(),
                format!(
                    "LWC row count {row_count} does not match index row id count {}",
                    row_ids.len()
                ),
            ));
        }
        let mut rows = Vec::with_capacity(row_count);
        for (row_idx, row_id) in row_ids.into_iter().enumerate() {
            let vals = lwc_block.decode_full_row_values(&metadata.col, row_idx)?;
            rows.push(RowRecord { row_id, vals });
        }
        Ok(rows)
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
    pub(crate) fn apply_first_redo_log_seq(&mut self, first_redo_log_seq: u32) -> Result<bool> {
        match self {
            PreparedCatalogCheckpoint::Published(publish) => {
                publish.mutable.apply_first_redo_log_seq(first_redo_log_seq)
            }
            PreparedCatalogCheckpoint::Noop { .. } => Ok(false),
        }
    }

    /// Commit the prepared catalog root, installing projected caches after success.
    pub(crate) async fn commit(self, storage: &CatalogStorage) -> Result<CatalogCheckpointOutcome> {
        match self {
            PreparedCatalogCheckpoint::Published(publish) => {
                let PreparedCatalogPublish {
                    mutable,
                    catalog_replay_start_ts,
                    checkpointed_silent_watermarks,
                } = *publish;
                let (_, old_root) = mutable.commit_prepared().await?;
                drop(old_root);
                storage.install_checkpointed_silent_watermarks(checkpointed_silent_watermarks);
                Ok(CatalogCheckpointOutcome::Published {
                    catalog_replay_start_ts,
                })
            }
            PreparedCatalogCheckpoint::Noop { .. } => Ok(CatalogCheckpointOutcome::Noop),
        }
    }
}

/// Mutable catalog root prepared for a checkpoint publication.
pub(crate) struct PreparedCatalogPublish {
    mutable: MutableMultiTableFile,
    catalog_replay_start_ts: TrxID,
    checkpointed_silent_watermarks: Arc<FastHashMap<TableID, TableRedoReplayFloor>>,
}

#[inline]
fn must_catalog_table_slot(table_id: TableID) -> usize {
    catalog_table_slot(table_id).expect("built-in catalog table id must be in catalog range")
}

#[inline]
fn catalog_table_slot_checked(table_id: TableID, catalog_table_count: usize) -> Result<usize> {
    let Some(slot) = catalog_table_slot(table_id) else {
        return Err(invalid_catalog_payload(format!(
            "catalog checkpoint redo table id is not in catalog range: table_id={table_id}"
        )));
    };
    if slot >= catalog_table_count {
        return Err(invalid_catalog_payload(format!(
            "catalog checkpoint redo table id out of range: table_id={table_id}, slot={slot}, catalog_table_count={catalog_table_count}"
        )));
    }
    Ok(slot)
}

fn build_lwc_blocks_from_row_records(
    metadata: &TableMetadata,
    rows: &[RowRecord],
) -> Result<Vec<PendingLwcBlock>> {
    for row in rows {
        if row.vals.len() != metadata.col.col_count() {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint row value count {} does not match column count {}",
                row.vals.len(),
                metadata.col.col_count()
            )));
        }
    }
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut lwc_blocks = Vec::new();
    let mut builder = LwcBuilder::new(&metadata.col);
    let mut builder_start = None;
    let mut builder_end = RowID::new(0);

    for row in rows {
        if builder.is_empty() {
            builder_start = Some(row.row_id);
        }
        if !builder.append_row_values(row.row_id, &row.vals)? {
            let start_row_id = builder_start.take().ok_or_else(|| {
                invalid_catalog_payload("catalog LWC builder missing start row id")
            })?;
            if builder_end <= start_row_id {
                return Err(invalid_catalog_payload(format!(
                    "catalog LWC builder end does not advance: start_row_id={start_row_id}, end_row_id={builder_end}"
                )));
            }
            let shape = ColumnBlockEntryShape::new(
                start_row_id,
                builder_end,
                builder.row_ids().to_vec(),
                Vec::new(),
            )?;
            let buf = builder.build(shape.row_shape_fingerprint())?;
            lwc_blocks.push(PendingLwcBlock { shape, buf });

            builder = LwcBuilder::new(&metadata.col);
            builder_start = Some(row.row_id);
            if !builder.append_row_values(row.row_id, &row.vals)? {
                return Err(invalid_catalog_payload(format!(
                    "single catalog row does not fit in LWC block: row_id={}",
                    row.row_id
                )));
            }
        }
        builder_end = row.row_id.saturating_add(1);
    }

    if !builder.is_empty() {
        let start_row_id = builder_start.ok_or_else(|| {
            invalid_catalog_payload("catalog LWC builder missing final start row id")
        })?;
        if builder_end <= start_row_id {
            return Err(invalid_catalog_payload(format!(
                "final catalog LWC builder end does not advance: start_row_id={start_row_id}, end_row_id={builder_end}"
            )));
        }
        let shape = ColumnBlockEntryShape::new(
            start_row_id,
            builder_end,
            builder.row_ids().to_vec(),
            Vec::new(),
        )?;
        let buf = builder.build(shape.row_shape_fingerprint())?;
        lwc_blocks.push(PendingLwcBlock { shape, buf });
    }
    Ok(lwc_blocks)
}

#[inline]
fn invalid_catalog_payload(message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(message.into())
        .into()
}

#[inline]
fn invalid_lwc_payload(
    file_kind: FileKind,
    block_id: BlockID,
    message: impl Into<String>,
) -> Error {
    let message = message.into();
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(format!(
            "file={file_kind}, block=lwc-block, block_id={block_id}, {message}"
        ))
        .into()
}

#[inline]
fn invalid_catalog_root_invariant(root_ts: TrxID, message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidRootInvariant)
        .attach(format!(
            "file={}, root_ts={root_ts}, {}",
            FileKind::CatalogMultiTableFile,
            message.into()
        ))
        .into()
}

#[inline]
fn invalid_catalog_reachable_block(
    root_ts: TrxID,
    block_id: BlockID,
    message: impl Into<String>,
) -> Error {
    Report::new(DataIntegrityError::InvalidRootInvariant)
        .attach(format!(
            "file={}, root_ts={root_ts}, block_id={block_id}, {}",
            FileKind::CatalogMultiTableFile,
            message.into()
        ))
        .into()
}

fn validate_catalog_reachable_block(root: &MultiTableActiveRoot, block_id: BlockID) -> Result<()> {
    let idx = usize::from(block_id);
    if idx >= root.alloc_map.len() {
        return Err(invalid_catalog_reachable_block(
            root.root_ts,
            block_id,
            format!("alloc_map_len={}", root.alloc_map.len()),
        ));
    }
    if !root.alloc_map.is_allocated(idx) {
        return Err(invalid_catalog_reachable_block(
            root.root_ts,
            block_id,
            "allocation bit is not set",
        ));
    }
    Ok(())
}

fn validate_catalog_row(
    metadata: &TableMetadata,
    row: &[Val],
    context: &'static str,
) -> Result<()> {
    if row.len() != metadata.col.col_count() {
        return Err(invalid_catalog_payload(format!(
            "{context} value count {} does not match column count {}",
            row.len(),
            metadata.col.col_count()
        )));
    }
    for (idx, val) in row.iter().enumerate() {
        if !metadata.col.col_type_match(idx, val) {
            return Err(invalid_catalog_payload(format!(
                "{context} column type mismatch: column_no={idx}",
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::{PoolGuards, PoolRole};
    use crate::catalog::USER_TABLE_ID_START;
    use crate::catalog::tests::{open_catalog_test_engine, table1, table2};
    use crate::catalog::{CatalogCheckpointBatch, CatalogCheckpointScanStopReason};
    use crate::error::DataIntegrityError;
    use crate::file::BlockKey;
    use crate::file::multi_table_file::publish_first_redo_log_seq_for_test as publish_mtb_first_redo_log_seq_for_test;
    use crate::file::multi_table_file::{CATALOG_MTB_FILE_ID, MutableMultiTableFile};
    use crate::id::BlockID;
    use crate::index::{ColumnBlockIndex, ColumnDeleteDeltaPatch};
    use crate::log::redo::{DDLRedo, RowRedoKind};
    use crate::row::ops::{SelectKey, UpdateCol};
    use crate::trx::stmt::Statement;
    use crate::value::{Val, ValKind};
    use tempfile::TempDir;

    /// Attach one catalog DDL marker to the current test statement.
    pub(crate) fn mark_catalog_ddl(stmt: &mut Statement<'_>, ddl: DDLRedo) {
        let old = stmt.effects_mut().set_ddl_redo(ddl);
        debug_assert!(old.is_none());
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

    #[test]
    fn test_static_catalog_definitions_expose_one_primary_key() {
        for CatalogDefinition { table_id, metadata } in [
            catalog_definition_of_tables(),
            catalog_definition_of_columns(),
            catalog_definition_of_indexes(),
            catalog_definition_of_index_columns(),
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
            for key in &primary_keys[0].1.cols {
                assert!(
                    !metadata.col.nullable(usize::from(key.col_no)),
                    "catalog table {table_id} primary key column {} must be non-null",
                    key.col_no
                );
            }
        }
    }

    async fn apply_metadata_only_checkpoint(
        storage: &CatalogStorage,
        next_table_id: TableID,
    ) -> Result<()> {
        let replay_start_ts = storage
            .checkpoint_snapshot()
            .unwrap()
            .catalog_replay_start_ts;
        storage
            .apply_checkpoint_batch(metadata_only_batch(replay_start_ts), next_table_id)
            .await
            .map(|_| ())
    }

    fn checkpoint_batch_with_ops(
        storage: &CatalogStorage,
        catalog_ops: Vec<CatalogRedoEntry>,
    ) -> CatalogCheckpointBatch {
        let replay_start_ts = storage
            .checkpoint_snapshot()
            .unwrap()
            .catalog_replay_start_ts;
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
        name_len: usize,
    ) -> CatalogRedoEntry {
        let mut name = vec![b'x'; name_len];
        name[0] = b'a' + (column_no % 26) as u8;
        CatalogRedoEntry {
            table_id: TABLE_ID_COLUMNS,
            kind: RowRedoKind::Insert(vec![
                Val::from(table_id),
                Val::from(column_no),
                Val::from(name),
                Val::from(ValKind::U64 as u32),
                Val::from(0u32),
            ]),
        }
    }

    fn catalog_column_row_record(
        row_id: RowID,
        table_id: TableID,
        column_no: u16,
        name_len: usize,
    ) -> RowRecord {
        let mut name = vec![b'x'; name_len];
        if let Some(first) = name.first_mut() {
            *first = b'a' + (column_no % 26) as u8;
        }
        RowRecord {
            row_id,
            vals: vec![
                Val::from(table_id),
                Val::from(column_no),
                Val::from(name),
                Val::from(ValKind::U64 as u32),
                Val::from(0u32),
            ],
        }
    }

    fn catalog_table_vals(table_id: TableID, next_index_no: u16) -> Vec<Val> {
        vec![Val::from(table_id), Val::from(next_index_no)]
    }

    async fn catalog_root_rows(storage: &CatalogStorage, table_id: TableID) -> Vec<RowRecord> {
        let root = storage.checkpoint_snapshot().unwrap().meta.table_roots
            [must_catalog_table_slot(table_id)];
        let table = storage.get_catalog_table(table_id).unwrap();
        let disk_pool_guard = storage.disk_pool.pool_guard();
        storage
            .load_rows_from_root(table.metadata(), &disk_pool_guard, root)
            .await
            .unwrap()
    }

    async fn assert_compact_catalog_root(
        storage: &CatalogStorage,
        table_id: TableID,
    ) -> Vec<RowRecord> {
        let root = storage.checkpoint_snapshot().unwrap().meta.table_roots
            [must_catalog_table_slot(table_id)];
        let rows = catalog_root_rows(storage, table_id).await;
        for (idx, row) in rows.iter().enumerate() {
            assert_eq!(row.row_id, RowID::new(idx as u64));
        }
        if root.root_block_id.is_none() {
            assert_eq!(root.pivot_row_id, RowID::new(0));
            assert!(rows.is_empty());
            return rows;
        }
        assert_eq!(root.pivot_row_id, RowID::new(rows.len() as u64));
        let root_block_id = BlockID::from(root.root_block_id.unwrap().get());
        let disk_pool_guard = storage.disk_pool.pool_guard();
        let entries = storage
            .collect_index_entries(&disk_pool_guard, root_block_id)
            .await
            .unwrap();
        assert!(!entries.is_empty());
        assert_eq!(entries[0].start_row_id, RowID::new(0));
        for pair in entries.windows(2) {
            assert_eq!(pair[1].start_row_id, pair[0].end_row_id());
        }
        assert_eq!(entries.last().unwrap().end_row_id(), root.pivot_row_id);
        let index = ColumnBlockIndex::new(
            root_block_id,
            root.pivot_row_id,
            storage.mtb.file_kind(),
            storage.mtb.sparse_file(),
            &storage.disk_pool,
            &disk_pool_guard,
        );
        for entry in entries {
            assert!(index.load_delete_deltas(&entry).await.unwrap().is_empty());
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
        let disk_pool_guard = storage.disk_pool.pool_guard();
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
        CatalogTableRootDesc {
            table_id,
            root_block_id: NonZeroU64::new(root_block_id.into()),
            pivot_row_id,
        }
    }

    async fn assert_checkpoint_rejects_delete_key(
        engine_name: &str,
        key: SelectKey,
        expected_message: &str,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = open_catalog_test_engine(main_dir, Some(engine_name)).await;

        let storage = &engine.catalog().storage;
        let replay_start_ts = storage
            .checkpoint_snapshot()
            .unwrap()
            .catalog_replay_start_ts;
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

        let err = storage
            .apply_checkpoint_batch(batch, engine.catalog().curr_next_table_id())
            .await
            .unwrap_err();

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
        let report = format!("{err:?}");
        assert!(report.contains(expected_message), "{report}");
        let current_replay_start_ts = storage
            .checkpoint_snapshot()
            .unwrap()
            .catalog_replay_start_ts;
        assert_eq!(current_replay_start_ts, replay_start_ts);
    }

    #[test]
    fn test_bootstrap_rejects_empty_catalog_root_table_id_mismatch() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-empty-root-mismatch")).await;

            let storage = &engine.catalog().storage;
            let mut snapshot = storage.checkpoint_snapshot().unwrap();
            let root = &mut snapshot.meta.table_roots[0];
            assert_eq!(root.root_block_id, None);
            assert_eq!(root.pivot_row_id, RowID::new(0));
            root.table_id = TABLE_ID_COLUMNS;

            let guards = PoolGuards::builder()
                .push(PoolRole::Meta, storage.meta_pool.pool_guard())
                .push(PoolRole::Disk, storage.disk_pool.pool_guard())
                .build();
            let err = storage
                .bootstrap_from_checkpoint(&snapshot, &guards, false)
                .await
                .unwrap_err();

            assert_eq!(
                err.data_integrity_error(),
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
        });
    }

    #[test]
    fn test_catalog_checkpoint_rejects_out_of_range_redo_table_id() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, Some("catalog-redo-table-range")).await;

            let storage = &engine.catalog().storage;
            let replay_start_ts = storage
                .checkpoint_snapshot()
                .unwrap()
                .catalog_replay_start_ts;
            let invalid_table_id = catalog_table_id_from_slot(CATALOG_TABLE_ROOT_DESC_COUNT);
            let batch = CatalogCheckpointBatch {
                replay_start_ts,
                safe_cts: replay_start_ts,
                first_retained_file_seq: 0,
                sealed_redo_segments: Vec::new(),
                catalog_ops: vec![CatalogRedoEntry {
                    table_id: invalid_table_id,
                    kind: RowRedoKind::Insert(Vec::new()),
                }],
                catalog_ddl_txn_count: 0,
                stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
            };

            let err = storage
                .apply_checkpoint_batch(batch, engine.catalog().curr_next_table_id())
                .await
                .unwrap_err();

            assert_eq!(
                err.data_integrity_error(),
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
            let current_replay_start_ts = storage
                .checkpoint_snapshot()
                .unwrap()
                .catalog_replay_start_ts;
            assert_eq!(current_replay_start_ts, replay_start_ts);
        });
    }

    #[test]
    fn test_catalog_checkpoint_rejects_delete_key_non_primary_key() {
        smol::block_on(async {
            assert_checkpoint_rejects_delete_key(
                "catalog-delete-key-non-primary",
                SelectKey::new(1, vec![Val::from(USER_TABLE_ID_START)]),
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
                    SelectKey::new(
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

            let storage = &engine.catalog().storage;
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

            let oversized_row =
                catalog_column_row_record(RowID::new(0), table_id, 0, u16::MAX as usize);
            let result = build_lwc_blocks_from_row_records(metadata, &[oversized_row]);
            let err = match result {
                Ok(_) => panic!("oversized row should fail LWC block build"),
                Err(err) => err,
            };
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );
            assert_eq!(storage.meta_pool.allocated(), allocated_before);

            assert_eq!(storage.meta_pool.allocated(), allocated_before);
        });
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "catalog root should not contain delete deltas")]
    fn test_catalog_root_loader_debug_asserts_delete_deltas() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-root-delete-deltas")).await;

            let storage = &engine.catalog().storage;
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

            let disk_pool_guard = storage.disk_pool.pool_guard();
            storage
                .load_rows_from_root(table.metadata(), &disk_pool_guard, root)
                .await
                .unwrap();
        });
    }

    #[test]
    fn test_catalog_root_loader_rejects_duplicate_primary_keys() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                open_catalog_test_engine(main_dir, Some("catalog-root-duplicate-pk")).await;

            let storage = &engine.catalog().storage;
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

            let disk_pool_guard = storage.disk_pool.pool_guard();
            let err = storage
                .load_rows_from_root(table.metadata(), &disk_pool_guard, root)
                .await
                .unwrap_err();

            assert_eq!(
                err.data_integrity_error(),
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

            let storage = &engine.catalog().storage;
            let before_root = storage.mtb.active_root_unchecked();
            let old_meta_block_id = before_root.meta_block_id;
            let before_allocated = before_root.alloc_map.allocated();

            apply_metadata_only_checkpoint(storage, engine.catalog().curr_next_table_id())
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
            let snap = engine.catalog().storage.checkpoint_snapshot().unwrap();
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
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let storage = &engine.catalog().storage;
            let before = storage.checkpoint_snapshot().unwrap();

            let marker = storage.publish_first_redo_log_seq(3).await.unwrap();

            assert_eq!(marker, 3);
            let after = storage.checkpoint_snapshot().unwrap();
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
            assert_eq!(storage.checkpoint_snapshot().unwrap(), after);
        });
    }

    #[test]
    fn test_catalog_metadata_only_checkpoint_skips_reachability_reads() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, Some("catalog-meta-fast-path")).await;

            let _ = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let storage = &engine.catalog().storage;
            let snap = storage.checkpoint_snapshot().unwrap();
            let disk_pool_guard = storage.disk_pool.pool_guard();
            let mut catalog_index_blocks = BTreeSet::new();
            for root in snap.meta.table_roots {
                let Some(root_block_id) = root.checkpoint_root_block_id() else {
                    continue;
                };
                catalog_index_blocks.insert(root_block_id);
                let entries = storage
                    .collect_index_entries(&disk_pool_guard, root_block_id)
                    .await
                    .unwrap();
                for entry in entries {
                    catalog_index_blocks.insert(entry.leaf_block_id);
                }
            }
            assert!(!catalog_index_blocks.is_empty());
            for block_id in &catalog_index_blocks {
                let _ = engine
                    .inner()
                    .disk_pool
                    .invalidate_block(CATALOG_MTB_FILE_ID, *block_id);
                let key = BlockKey::new(CATALOG_MTB_FILE_ID, *block_id);
                assert!(engine.inner().disk_pool.try_get_frame_id(&key).is_none());
            }
            let cached_before = engine.inner().disk_pool.allocated();

            apply_metadata_only_checkpoint(storage, engine.catalog().curr_next_table_id())
                .await
                .unwrap();

            assert_eq!(engine.inner().disk_pool.allocated(), cached_before);
            for block_id in catalog_index_blocks {
                let key = BlockKey::new(CATALOG_MTB_FILE_ID, block_id);
                assert!(engine.inner().disk_pool.try_get_frame_id(&key).is_none());
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

            let storage = &engine.catalog().storage;
            let before_root = storage.mtb.active_root_unchecked();
            let old_meta_block_id = before_root.meta_block_id;
            let before_allocated = before_root.alloc_map.allocated();
            let replay_start_ts = storage
                .checkpoint_snapshot()
                .unwrap()
                .catalog_replay_start_ts;
            let table_id = USER_TABLE_ID_START + 4242;
            let batch = CatalogCheckpointBatch {
                replay_start_ts,
                safe_cts: replay_start_ts,
                first_retained_file_seq: 0,
                sealed_redo_segments: Vec::new(),
                catalog_ops: vec![
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::Insert(vec![Val::from(table_id), Val::from(0u16)]),
                    },
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::DeleteByPrimaryKey(SelectKey::new(
                            0,
                            vec![Val::from(table_id)],
                        )),
                    },
                ],
                catalog_ddl_txn_count: 0,
                stop_reason: CatalogCheckpointScanStopReason::ReachedDurableUpper,
            };

            storage
                .apply_checkpoint_batch(batch, engine.catalog().curr_next_table_id())
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
            assert_eq!(after_root.table_roots[0].root_block_id, None);
            assert_eq!(after_root.table_roots[0].pivot_row_id, RowID::new(0));
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

            let storage = &engine.catalog().storage;
            let table_id = USER_TABLE_ID_START + 5252;
            let batch = checkpoint_batch_with_ops(
                storage,
                vec![
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::Insert(vec![Val::from(table_id), Val::from(0u16)]),
                    },
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::UpdateByPrimaryKey(
                            SelectKey::new(0, vec![Val::from(table_id)]),
                            vec![UpdateCol {
                                idx: 1,
                                val: Val::from(7u16),
                            }],
                        ),
                    },
                ],
            );

            storage
                .apply_checkpoint_batch(batch, engine.catalog().curr_next_table_id())
                .await
                .unwrap();

            let rows = catalog_root_rows(storage, TABLE_ID_TABLES).await;
            let matching_rows = rows
                .iter()
                .filter(|row| row.vals[0] == Val::from(table_id))
                .collect::<Vec<_>>();
            assert_eq!(matching_rows.len(), 1);
            assert_eq!(matching_rows[0].vals[1], Val::from(7u16));
            assert_compact_catalog_root(storage, TABLE_ID_TABLES).await;
        });
    }

    #[test]
    fn test_catalog_checkpoint_update_by_primary_key_rejects_primary_key_column() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = open_catalog_test_engine(main_dir, Some("catalog-update-pk-column")).await;

            let storage = &engine.catalog().storage;
            let table_id = USER_TABLE_ID_START + 6262;
            let batch = checkpoint_batch_with_ops(
                storage,
                vec![
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::Insert(vec![Val::from(table_id), Val::from(0u16)]),
                    },
                    CatalogRedoEntry {
                        table_id: TABLE_ID_TABLES,
                        kind: RowRedoKind::UpdateByPrimaryKey(
                            SelectKey::new(0, vec![Val::from(table_id)]),
                            vec![UpdateCol {
                                idx: 0,
                                val: Val::from(table_id),
                            }],
                        ),
                    },
                ],
            );

            let err = storage
                .apply_checkpoint_batch(batch, engine.catalog().curr_next_table_id())
                .await
                .unwrap_err();

            assert_eq!(
                err.data_integrity_error(),
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
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let storage = &engine.catalog().storage;
            let batch = checkpoint_batch_with_ops(
                storage,
                vec![CatalogRedoEntry {
                    table_id: TABLE_ID_TABLES,
                    kind: RowRedoKind::UpdateByPrimaryKey(
                        SelectKey::new(0, vec![Val::from(table_id)]),
                        vec![UpdateCol {
                            idx: 1,
                            val: Val::from(9u16),
                        }],
                    ),
                }],
            );

            storage
                .apply_checkpoint_batch(batch, engine.catalog().curr_next_table_id())
                .await
                .unwrap();

            let rows = catalog_root_rows(storage, TABLE_ID_TABLES).await;
            let matching_rows = rows
                .iter()
                .filter(|row| row.vals[0] == Val::from(table_id))
                .collect::<Vec<_>>();
            assert_eq!(matching_rows.len(), 1);
            assert_eq!(matching_rows[0].vals[1], Val::from(9u16));
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

            let storage = &engine.catalog().storage;
            let active_before = storage.mtb.active_root_unchecked();
            let active_meta_before = active_before.meta_block_id;
            let active_root_ts_before = active_before.root_ts;
            let bogus_root_block_id = (1..active_before.alloc_map.len())
                .rev()
                .map(BlockID::from)
                .find(|block_id| !active_before.alloc_map.is_allocated(usize::from(*block_id)))
                .unwrap();

            let mut roots = storage.checkpoint_snapshot().unwrap().meta.table_roots;
            roots[0].root_block_id = NonZeroU64::new(u64::from(bogus_root_block_id));
            roots[0].pivot_row_id = RowID::new(1);

            let mut mutable =
                MutableMultiTableFile::fork(&storage.mtb, storage.table_fs.background_writes());
            mutable
                .apply_checkpoint_metadata(
                    active_root_ts_before.saturating_add(1),
                    engine.catalog().curr_next_table_id(),
                    roots,
                )
                .unwrap();
            let err = storage
                .rebuild_catalog_alloc_map(&mut mutable)
                .await
                .unwrap_err();

            assert_eq!(
                err.data_integrity_error(),
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

            let storage = &engine.catalog().storage;
            let table = storage.get_catalog_table(TABLE_ID_TABLES).unwrap();
            let root = CatalogTableRootDesc {
                table_id: TABLE_ID_TABLES,
                root_block_id: None,
                pivot_row_id: RowID::new(0),
            };
            let table_id = USER_TABLE_ID_START + 42;
            let table_ops = vec![
                RowRedoKind::Insert(vec![Val::from(table_id), Val::from(0u16)]),
                RowRedoKind::DeleteByPrimaryKey(SelectKey::new(0, vec![Val::from(table_id)])),
            ];
            let mut mutable =
                MutableMultiTableFile::fork(&storage.mtb, storage.table_fs.background_writes());

            let (next_root, blocks_changed) = storage
                .apply_table_ops(
                    &mut mutable,
                    TABLE_ID_TABLES,
                    table.metadata(),
                    root,
                    &table_ops,
                    TrxID::new(7),
                )
                .await
                .unwrap();

            assert_eq!(next_root.root_block_id, None);
            assert_eq!(next_root.pivot_row_id, RowID::new(0));
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
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let storage = &engine.catalog().storage;
            let table = storage.get_catalog_table(TABLE_ID_TABLES).unwrap();
            let root = storage.checkpoint_snapshot().unwrap().meta.table_roots[0];
            assert!(root.root_block_id.is_some());

            let table_id = USER_TABLE_ID_START + 4242;
            let table_ops = vec![
                RowRedoKind::Insert(vec![Val::from(table_id), Val::from(0u16)]),
                RowRedoKind::DeleteByPrimaryKey(SelectKey::new(0, vec![Val::from(table_id)])),
            ];
            let mut mutable =
                MutableMultiTableFile::fork(&storage.mtb, storage.table_fs.background_writes());

            let (next_root, blocks_changed) = storage
                .apply_table_ops(
                    &mut mutable,
                    TABLE_ID_TABLES,
                    table.metadata(),
                    root,
                    &table_ops,
                    TrxID::new(8),
                )
                .await
                .unwrap();

            assert_eq!(next_root.root_block_id, root.root_block_id);
            assert_eq!(next_root.pivot_row_id, root.pivot_row_id);
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
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let snap = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let tables_root = snap.meta.table_roots[0];
            let root_block_id = BlockID::from(tables_root.root_block_id.unwrap().get());
            let disk_pool_guard = engine.catalog().storage.disk_pool.pool_guard();

            let cached_before_first = engine.inner().disk_pool.allocated();

            let entries1 = engine
                .catalog()
                .storage
                .collect_index_entries(&disk_pool_guard, root_block_id)
                .await
                .unwrap();
            assert!(!entries1.is_empty());

            let cached_after_first = engine.inner().disk_pool.allocated();
            assert!(cached_after_first >= cached_before_first);
            let root_key = BlockKey::new(CATALOG_MTB_FILE_ID, root_block_id);
            assert!(
                engine
                    .inner()
                    .disk_pool
                    .try_get_frame_id(&root_key)
                    .is_some()
            );

            let entries2 = engine
                .catalog()
                .storage
                .collect_index_entries(&disk_pool_guard, root_block_id)
                .await
                .unwrap();
            assert_eq!(entries2.len(), entries1.len());
            assert_eq!(engine.inner().disk_pool.allocated(), cached_after_first);
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
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let snap1 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let tables_root1 = snap1.meta.table_roots[0];
            assert!(tables_root1.root_block_id.is_some());
            assert_compact_catalog_root(&engine.catalog().storage, TABLE_ID_TABLES).await;

            let table2_id = table2(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let snap2 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let tables_root2 = snap2.meta.table_roots[0];
            let rows =
                assert_compact_catalog_root(&engine.catalog().storage, TABLE_ID_TABLES).await;

            assert!(tables_root2.root_block_id != tables_root1.root_block_id);
            assert_eq!(tables_root2.pivot_row_id, RowID::new(rows.len() as u64));
            let active_root = engine.catalog().storage.mtb.active_root_unchecked();
            let root_block_id1 = BlockID::from(tables_root1.root_block_id.unwrap().get());
            let root_block_id2 = BlockID::from(tables_root2.root_block_id.unwrap().get());
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
            let mut recovered_table_ids = recovered.catalog().list_user_table_ids_now();
            recovered_table_ids.sort();
            let mut expected_table_ids = vec![table1_id, table2_id];
            expected_table_ids.sort();
            assert_eq!(recovered_table_ids, expected_table_ids);
            assert_eq!(
                assert_compact_catalog_root(&recovered.catalog().storage, TABLE_ID_TABLES)
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

            let storage = &engine.catalog().storage;
            let table_id = USER_TABLE_ID_START + 9000;
            storage
                .apply_checkpoint_batch(
                    checkpoint_batch_with_ops(
                        storage,
                        vec![catalog_column_insert(table_id, 0, 30_000)],
                    ),
                    engine.catalog().curr_next_table_id(),
                )
                .await
                .unwrap();

            let disk_pool_guard = storage.disk_pool.pool_guard();
            let snap1 = storage.checkpoint_snapshot().unwrap();
            let columns_root1 = snap1.meta.table_roots[1];
            assert_eq!(columns_root1.pivot_row_id, RowID::new(1));
            let entries1 = storage
                .collect_index_entries(
                    &disk_pool_guard,
                    BlockID::from(columns_root1.root_block_id.unwrap().get()),
                )
                .await
                .unwrap();
            assert_eq!(entries1.len(), 1);

            let second_batch = (1..=4)
                .map(|column_no| catalog_column_insert(table_id, column_no, 15_000))
                .collect();
            storage
                .apply_checkpoint_batch(
                    checkpoint_batch_with_ops(storage, second_batch),
                    engine.catalog().curr_next_table_id(),
                )
                .await
                .unwrap();

            let snap2 = storage.checkpoint_snapshot().unwrap();
            let columns_root2 = snap2.meta.table_roots[1];
            let rows = assert_compact_catalog_root(storage, TABLE_ID_COLUMNS).await;
            assert_eq!(rows.len(), 5);
            assert_eq!(columns_root2.pivot_row_id, RowID::new(5));
            let entries2 = storage
                .collect_index_entries(
                    &disk_pool_guard,
                    BlockID::from(columns_root2.root_block_id.unwrap().get()),
                )
                .await
                .unwrap();

            assert!(
                columns_root2.root_block_id != columns_root1.root_block_id,
                "changed catalog tables should publish a rewritten compact root"
            );
            assert_eq!(entries2[0].start_row_id, RowID::new(0));
            for pair in entries2.windows(2) {
                assert_eq!(pair[1].start_row_id, pair[0].end_row_id());
            }
            assert_eq!(
                entries2.last().unwrap().end_row_id(),
                columns_root2.pivot_row_id
            );
        });
    }
}
