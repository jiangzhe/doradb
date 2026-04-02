use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::page::BufferPage;
use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard, PoolGuards};
use crate::catalog::storage::CatalogStorage;
use crate::catalog::table::TableMetadata;
use crate::catalog::{CatalogCheckpointBatch, CatalogRedoEntry};
use crate::catalog::{ObjID, TableID, USER_OBJ_ID_START};
use crate::error::{BlockCorruptionCause, BlockKind, Error, Result};
use crate::file::cow_file::{BlockID, SUPER_BLOCK_ID};
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MultiTableFileSnapshot,
    MutableMultiTableFile,
};
use crate::index::{
    ColumnBlockEntryPatch, ColumnBlockEntryShape, ColumnBlockIndex, ColumnDeleteDeltaPatch,
    ColumnDeleteDomain, ColumnLeafEntry, load_entry_deletion_deltas,
};
use crate::io::DirectBuf;
use crate::lwc::{LwcBuilder, PersistedLwcBlock};
use crate::row::ops::SelectKey;
use crate::row::{InsertRow, RowID, RowPage};
use crate::trx::redo::RowRedoKind;
use crate::value::Val;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

struct PendingLwcBlock {
    shape: ColumnBlockEntryShape,
    buf: DirectBuf,
}

type CatalogIndexEntry = ColumnLeafEntry;

struct RowRecord {
    row_id: RowID,
    vals: Vec<Val>,
}

struct ExistingVisibleRow {
    row_id: RowID,
    start_row_id: RowID,
    vals: Vec<Val>,
    deleted: bool,
}

struct PendingInsertRow {
    row_id: RowID,
    vals: Vec<Val>,
    deleted: bool,
}

impl CatalogStorage {
    pub(crate) async fn bootstrap_from_checkpoint(
        &self,
        snapshot: &MultiTableFileSnapshot,
        guards: &PoolGuards,
    ) -> Result<()> {
        for (idx, root) in snapshot.meta.table_roots.iter().copied().enumerate() {
            if idx >= self.tables.len() {
                break;
            }
            if root.root_block_id.is_none() {
                if root.pivot_row_id != 0 {
                    return Err(Error::InvalidState);
                }
                continue;
            }
            if root.table_id as usize != idx {
                return Err(Error::InvalidState);
            }
            let rows = self
                .load_visible_rows_from_root(self.tables[idx].metadata(), guards.disk_guard(), root)
                .await?;
            for row in rows {
                self.tables[idx].insert_no_trx(guards, &row.vals).await?;
            }
        }
        Ok(())
    }

    pub async fn apply_checkpoint_batch(
        &self,
        batch: CatalogCheckpointBatch,
        next_user_obj_id: ObjID,
    ) -> Result<()> {
        let CatalogCheckpointBatch {
            replay_start_ts,
            safe_cts,
            catalog_ops,
            ..
        } = batch;
        let snapshot = self.mtb.load_snapshot()?;
        let current_catalog_replay_start_ts = snapshot.catalog_replay_start_ts;
        let next_catalog_replay_start_ts = safe_cts.saturating_add(1).max(replay_start_ts);
        if current_catalog_replay_start_ts != replay_start_ts {
            if current_catalog_replay_start_ts >= next_catalog_replay_start_ts {
                return Ok(());
            }
            return Err(Error::InvalidState);
        }
        if safe_cts < replay_start_ts {
            return Ok(());
        }

        if catalog_ops.is_empty() {
            let mut mutable = MutableMultiTableFile::fork(&self.mtb);
            mutable.apply_checkpoint_metadata(
                next_catalog_replay_start_ts,
                next_user_obj_id.max(USER_OBJ_ID_START),
                snapshot.meta.table_roots,
            )?;
            let (_, old_root) = mutable.commit().await?;
            drop(old_root);
            return Ok(());
        }

        let mut ops_by_table: Vec<Vec<RowRedoKind>> =
            (0..self.tables.len()).map(|_| Vec::new()).collect();
        for CatalogRedoEntry { table_id, kind } in catalog_ops {
            let table_idx = table_id as usize;
            if table_idx >= ops_by_table.len() {
                continue;
            }
            ops_by_table[table_idx].push(kind);
        }

        let mut mutable = MutableMultiTableFile::fork(&self.mtb);
        let mut new_roots = snapshot.meta.table_roots;
        for (idx, table) in self.tables.iter().enumerate() {
            if idx >= CATALOG_TABLE_ROOT_DESC_COUNT {
                break;
            }
            if ops_by_table[idx].is_empty() {
                continue;
            }
            let current_root = new_roots[idx];
            let new_root = self
                .apply_table_ops(
                    &mut mutable,
                    idx as TableID,
                    table.metadata(),
                    current_root,
                    &ops_by_table[idx],
                    safe_cts,
                )
                .await?;
            new_roots[idx] = new_root;
        }

        mutable.apply_checkpoint_metadata(
            next_catalog_replay_start_ts,
            next_user_obj_id.max(USER_OBJ_ID_START),
            new_roots,
        )?;
        let (_, old_root) = mutable.commit().await?;
        drop(old_root);
        Ok(())
    }

    async fn apply_table_ops(
        &self,
        mutable: &mut MutableMultiTableFile,
        table_id: TableID,
        metadata: &TableMetadata,
        root: CatalogTableRootDesc,
        table_ops: &[RowRedoKind],
        checkpoint_cts: u64,
    ) -> Result<CatalogTableRootDesc> {
        let disk_pool_guard = self.disk_pool.pool_guard();
        // Step 1: Validate root invariants and construct a base index snapshot for reads.
        if root.root_block_id.is_none() && root.pivot_row_id != 0 {
            return Err(Error::InvalidState);
        }
        let root_block_id = root.checkpoint_root_block_id();
        let entries = if let Some(root_block_id) = root_block_id {
            self.collect_index_entries(&disk_pool_guard, root_block_id)
                .await?
        } else {
            Vec::new()
        };
        let base_index = ColumnBlockIndex::new(
            root_block_id.unwrap_or(SUPER_BLOCK_ID),
            root.pivot_row_id,
            &self.disk_pool,
            &disk_pool_guard,
        );
        let mut next_row_id = root.pivot_row_id;

        // Step 2: Preload existing visible rows only when delete-by-key appears.
        let need_delete_lookup = table_ops
            .iter()
            .any(|kind| matches!(kind, RowRedoKind::DeleteByUniqueKey(_)));
        let mut existing_rows = if need_delete_lookup {
            self.load_visible_rows_for_delete_lookup(
                metadata,
                &disk_pool_guard,
                &entries,
                &base_index,
            )
            .await?
        } else {
            Vec::new()
        };
        let mut pending_rows = Vec::new();
        let mut delete_deltas: BTreeMap<RowID, BTreeSet<u32>> = BTreeMap::new();

        // Step 3: Replay table ops into in-memory pending rows and deletion deltas.
        for kind in table_ops {
            match kind {
                RowRedoKind::Insert(vals) => {
                    if vals.len() != metadata.col_count() {
                        return Err(Error::InvalidFormat);
                    }
                    pending_rows.push(PendingInsertRow {
                        row_id: next_row_id,
                        vals: vals.clone(),
                        deleted: false,
                    });
                    next_row_id = next_row_id.saturating_add(1);
                }
                RowRedoKind::DeleteByUniqueKey(key) => {
                    if let Some(row) = pending_rows
                        .iter_mut()
                        .rev()
                        .find(|row| !row.deleted && row_matches_key(metadata, &row.vals, key))
                    {
                        row.deleted = true;
                        continue;
                    }
                    if let Some(row) = existing_rows
                        .iter_mut()
                        .find(|row| !row.deleted && row_matches_key(metadata, &row.vals, key))
                    {
                        row.deleted = true;
                        let delta = row
                            .row_id
                            .checked_sub(row.start_row_id)
                            .ok_or(Error::InvalidState)?;
                        if delta > u32::MAX as u64 {
                            return Err(Error::InvalidState);
                        }
                        delete_deltas
                            .entry(row.start_row_id)
                            .or_default()
                            .insert(delta as u32);
                    }
                }
                RowRedoKind::Delete | RowRedoKind::Update(_) => {
                    return Err(Error::InvalidState);
                }
            }
        }

        // Step 4: Build the live-insert batch after canceling same-batch insert+delete rows.
        let mut current_root_block_id = root_block_id.unwrap_or(SUPER_BLOCK_ID);
        let mut current_end_row_id = root.pivot_row_id;
        let mut entries_changed = false;
        let mut live_inserts = Vec::new();
        for row in pending_rows {
            if !row.deleted {
                live_inserts.push(RowRecord {
                    row_id: row.row_id,
                    vals: row.vals,
                });
            }
        }

        // Step 5: Try CoW-merging inserts into the right-most existing LWC block first.
        if !live_inserts.is_empty()
            && let Some(last_entry) = entries.last().copied()
        {
            let existing_tail_rows = self
                .decode_lwc_page_rows(metadata, &disk_pool_guard, &base_index, &last_entry)
                .await?;
            if !existing_tail_rows.is_empty()
                && let Some((merged_tail_buf, merged_row_ids, consumed)) = self
                    .build_merged_tail_lwc_block(
                        metadata,
                        last_entry.start_row_id,
                        &existing_tail_rows,
                        &live_inserts,
                    )
                    .await?
            {
                let new_tail_page_id = mutable.allocate_block_id()?;
                mutable
                    .write_block(new_tail_page_id, merged_tail_buf)
                    .await?;
                mutable.record_gc_block(last_entry.block_id());
                let merged_end_row_id = live_inserts
                    .get(consumed.saturating_sub(1))
                    .map(|row| row.row_id.saturating_add(1))
                    .unwrap_or(current_end_row_id);
                let existing_deletes = load_entry_deletion_deltas(&base_index, &last_entry).await?;
                let replacement = ColumnBlockEntryShape::new(
                    last_entry.start_row_id,
                    merged_end_row_id,
                    merged_row_ids,
                    existing_deletes.into_iter().collect(),
                )?
                .with_delete_domain(ColumnDeleteDomain::Ordinal)
                .with_block_id(new_tail_page_id);
                let patches = [ColumnBlockEntryPatch {
                    start_row_id: last_entry.start_row_id,
                    entry: replacement,
                }];
                let column_index = ColumnBlockIndex::new(
                    current_root_block_id,
                    current_end_row_id,
                    &self.disk_pool,
                    &disk_pool_guard,
                );
                current_root_block_id = column_index
                    .batch_replace_entries(mutable, &patches, checkpoint_cts)
                    .await?;
                entries_changed = true;
                live_inserts.drain(0..consumed);
                if live_inserts.is_empty() {
                    current_end_row_id = next_row_id.max(root.pivot_row_id);
                }
            }
        }

        // Step 6: Persist any remaining inserts as new LWC blocks and append index entries.
        if !live_inserts.is_empty() {
            let new_pages = self
                .build_lwc_blocks_from_row_records(&self.meta_pool, metadata, &live_inserts)
                .await?;
            let mut new_entries = Vec::with_capacity(new_pages.len());
            for page in new_pages {
                let page_id = mutable.allocate_block_id()?;
                mutable.write_block(page_id, page.buf).await?;
                new_entries.push(page.shape.with_block_id(page_id));
            }
            if !new_entries.is_empty() {
                let new_end_row_id = next_row_id.max(root.pivot_row_id);
                let column_index = ColumnBlockIndex::new(
                    current_root_block_id,
                    current_end_row_id,
                    &self.disk_pool,
                    &disk_pool_guard,
                );
                current_root_block_id = column_index
                    .batch_insert(mutable, &new_entries, new_end_row_id, checkpoint_cts)
                    .await?;
                current_end_row_id = new_end_row_id;
                entries_changed = true;
            }
        }

        // Step 7: Materialize typed delete rewrites keyed by leaf start-row-id.
        let mut patch_storage: Vec<(RowID, Vec<u32>)> = Vec::new();
        for (start_row_id, pending) in &delete_deltas {
            let idx = entries
                .binary_search_by_key(start_row_id, |entry| entry.start_row_id)
                .map_err(|_| Error::InvalidState)?;
            let entry = entries[idx];
            let mut base = load_entry_deletion_deltas(&base_index, &entry).await?;
            let old_len = base.len();
            base.extend(pending.iter().copied());
            if base.len() == old_len {
                continue;
            }
            patch_storage.push((*start_row_id, base.into_iter().collect()));
        }

        // Step 8: Apply typed delete rewrites on the current root.
        if !patch_storage.is_empty() {
            let patches: Vec<ColumnDeleteDeltaPatch<'_>> = patch_storage
                .iter()
                .map(|(start_row_id, delete_deltas)| ColumnDeleteDeltaPatch {
                    start_row_id: *start_row_id,
                    delete_deltas,
                })
                .collect();
            let column_index = ColumnBlockIndex::new(
                current_root_block_id,
                current_end_row_id,
                &self.disk_pool,
                &disk_pool_guard,
            );
            current_root_block_id = column_index
                .batch_replace_delete_deltas(mutable, &patches, checkpoint_cts)
                .await?;
            entries_changed = true;
        }

        // Step 9: Publish final per-table root descriptor for this checkpoint batch.
        let next_pivot_row_id = next_row_id.max(root.pivot_row_id);
        let (root_block_id, pivot_row_id) = if current_root_block_id == SUPER_BLOCK_ID {
            // Same-batch insert/delete on an empty root can advance `next_row_id`
            // without materializing any persisted blocks. Keep the published
            // descriptor empty instead of emitting `None` with a nonzero pivot.
            (root.root_block_id, root.pivot_row_id)
        } else {
            let root_block_id = if entries_changed || next_pivot_row_id > root.pivot_row_id {
                Some(NonZeroU64::new(current_root_block_id.into()).ok_or(Error::InvalidState)?)
            } else {
                root.root_block_id
            };
            (root_block_id, next_pivot_row_id)
        };
        Ok(CatalogTableRootDesc {
            table_id,
            root_block_id,
            pivot_row_id,
        })
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
        let index =
            ColumnBlockIndex::new(root_block_id, RowID::MAX, &self.disk_pool, disk_pool_guard);
        index.collect_leaf_entries().await
    }

    async fn load_visible_rows_from_root(
        &self,
        metadata: &TableMetadata,
        disk_pool_guard: &PoolGuard,
        root: CatalogTableRootDesc,
    ) -> Result<Vec<RowRecord>> {
        if root.root_block_id.is_none() {
            if root.pivot_row_id != 0 {
                return Err(Error::InvalidState);
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
            &self.disk_pool,
            disk_pool_guard,
        );
        let mut rows = Vec::new();
        for entry in entries {
            let deleted = load_entry_deletion_deltas(&column_index, &entry).await?;
            let page_rows = self
                .decode_lwc_page_rows(metadata, disk_pool_guard, &column_index, &entry)
                .await?;
            for row in page_rows {
                let delta = row
                    .row_id
                    .checked_sub(entry.start_row_id)
                    .ok_or(Error::InvalidState)?;
                if delta > u32::MAX as u64 {
                    return Err(Error::InvalidState);
                }
                if deleted.contains(&(delta as u32)) {
                    continue;
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn load_visible_rows_for_delete_lookup(
        &self,
        metadata: &TableMetadata,
        disk_pool_guard: &PoolGuard,
        entries: &[CatalogIndexEntry],
        column_index: &ColumnBlockIndex<'_>,
    ) -> Result<Vec<ExistingVisibleRow>> {
        let mut rows = Vec::new();
        for entry in entries {
            let deleted = load_entry_deletion_deltas(column_index, entry).await?;
            let page_rows = self
                .decode_lwc_page_rows(metadata, disk_pool_guard, column_index, entry)
                .await?;
            for row in page_rows {
                let delta = row
                    .row_id
                    .checked_sub(entry.start_row_id)
                    .ok_or(Error::InvalidState)?;
                if delta > u32::MAX as u64 {
                    return Err(Error::InvalidState);
                }
                if deleted.contains(&(delta as u32)) {
                    continue;
                }
                rows.push(ExistingVisibleRow {
                    row_id: row.row_id,
                    start_row_id: entry.start_row_id,
                    vals: row.vals,
                    deleted: false,
                });
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
        let lwc_block =
            PersistedLwcBlock::load(&self.disk_pool, disk_pool_guard, entry.block_id()).await?;
        let row_count = lwc_block.row_count();
        let row_ids = column_index.load_entry_row_ids(entry).await?;
        if row_count != row_ids.len() {
            return Err(Error::block_corrupted(
                self.disk_pool.file_kind(),
                BlockKind::LwcBlock,
                entry.block_id(),
                BlockCorruptionCause::InvalidPayload,
            ));
        }
        let mut rows = Vec::with_capacity(row_count);
        for (row_idx, row_id) in row_ids.into_iter().enumerate() {
            let vals = lwc_block.decode_full_row_values(metadata, row_idx)?;
            rows.push(RowRecord { row_id, vals });
        }
        Ok(rows)
    }

    async fn build_lwc_blocks_from_row_records(
        &self,
        meta_pool: &FixedBufferPool,
        metadata: &TableMetadata,
        rows: &[RowRecord],
    ) -> Result<Vec<PendingLwcBlock>> {
        for row in rows {
            if row.vals.len() != metadata.col_count() {
                return Err(Error::InvalidFormat);
            }
        }
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let mut lwc_blocks = Vec::new();
        let mut builder = LwcBuilder::new(metadata);
        let mut builder_start = None;
        let mut builder_end = 0u64;
        let meta_guard = meta_pool.pool_guard();
        let mut temp_page = meta_pool.allocate_page::<RowPage>(&meta_guard).await;

        for row in rows {
            if builder.is_empty() {
                builder_start = Some(row.row_id);
            }
            if !append_single_row_to_builder(metadata, &mut temp_page, &mut builder, row)? {
                let start_row_id = builder_start.take().ok_or(Error::InvalidState)?;
                if builder_end <= start_row_id {
                    return Err(Error::InvalidState);
                }
                let shape = ColumnBlockEntryShape::new(
                    start_row_id,
                    builder_end,
                    builder.row_ids().to_vec(),
                    Vec::new(),
                )?;
                let buf = builder.build(shape.row_shape_fingerprint())?;
                lwc_blocks.push(PendingLwcBlock { shape, buf });

                builder = LwcBuilder::new(metadata);
                builder_start = Some(row.row_id);
                if !append_single_row_to_builder(metadata, &mut temp_page, &mut builder, row)? {
                    return Err(Error::InvalidState);
                }
            }
            builder_end = row.row_id.saturating_add(1);
        }

        meta_pool.deallocate_page(temp_page);

        if !builder.is_empty() {
            let start_row_id = builder_start.ok_or(Error::InvalidState)?;
            if builder_end <= start_row_id {
                return Err(Error::InvalidState);
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

    async fn build_merged_tail_lwc_block(
        &self,
        metadata: &TableMetadata,
        start_row_id: RowID,
        existing_tail_rows: &[RowRecord],
        inserts: &[RowRecord],
    ) -> Result<Option<(DirectBuf, Vec<RowID>, usize)>> {
        if existing_tail_rows.is_empty() || inserts.is_empty() {
            return Ok(None);
        }
        for row in existing_tail_rows {
            if row.vals.len() != metadata.col_count() {
                return Err(Error::InvalidFormat);
            }
        }
        for row in inserts {
            if row.vals.len() != metadata.col_count() {
                return Err(Error::InvalidFormat);
            }
        }

        let mut builder = LwcBuilder::new(metadata);
        let meta_guard = self.meta_pool.pool_guard();
        let mut temp_page = self.meta_pool.allocate_page::<RowPage>(&meta_guard).await;

        for row in existing_tail_rows {
            if !append_single_row_to_builder(metadata, &mut temp_page, &mut builder, row)? {
                self.meta_pool.deallocate_page(temp_page);
                return Err(Error::InvalidState);
            }
        }

        let mut consumed = 0usize;
        for row in inserts {
            if !append_single_row_to_builder(metadata, &mut temp_page, &mut builder, row)? {
                break;
            }
            consumed += 1;
        }

        self.meta_pool.deallocate_page(temp_page);

        if consumed == 0 {
            return Ok(None);
        }
        let row_ids = builder.row_ids().to_vec();
        let end_row_id = row_ids
            .last()
            .copied()
            .ok_or(Error::InvalidState)?
            .saturating_add(1);
        let fingerprint =
            ColumnBlockEntryShape::new(start_row_id, end_row_id, row_ids.clone(), Vec::new())?
                .row_shape_fingerprint();
        let buf = builder.build(fingerprint)?;
        Ok(Some((buf, row_ids, consumed)))
    }
}

fn append_single_row_to_builder(
    metadata: &TableMetadata,
    temp_page: &mut PageExclusiveGuard<RowPage>,
    builder: &mut LwcBuilder<'_>,
    row: &RowRecord,
) -> Result<bool> {
    {
        let page = temp_page.page_mut();
        page.zero();
        page.init(row.row_id, 1, metadata);
        let insert = page.insert(metadata, &row.vals);
        if !matches!(insert, InsertRow::Ok(_)) {
            return Err(Error::InvalidState);
        }
    }
    builder.append_row_page(temp_page.page())
}

fn row_matches_key(metadata: &TableMetadata, row: &[Val], key: &SelectKey) -> bool {
    let Some(index_spec) = metadata.index_specs.get(key.index_no) else {
        return false;
    };
    if index_spec.index_cols.len() != key.vals.len() {
        return false;
    }
    for (index_key, key_val) in index_spec.index_cols.iter().zip(&key.vals) {
        let col_idx = index_key.col_no as usize;
        if row.get(col_idx) != Some(key_val) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BlockKey;
    use crate::catalog::USER_OBJ_ID_START;
    use crate::catalog::tests::{table1, table2};
    use crate::conf::{EngineConfig, TrxSysConfig};
    use crate::file::BlockID;
    use crate::file::multi_table_file::{CATALOG_MTB_PERSISTED_FILE_ID, MutableMultiTableFile};
    use crate::index::{ColumnBlockIndex, ColumnDeleteDomain, load_entry_deletion_deltas};
    use crate::row::ops::SelectKey;
    use crate::trx::redo::RowRedoKind;
    use crate::value::Val;
    use tempfile::TempDir;

    #[test]
    fn test_catalog_checkpoint_apply_table_ops_keeps_empty_root_for_canceled_insert_batch() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("catalog-checkpoint-canceled-empty-root")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let storage = &engine.catalog().storage;
            let table = storage.get_catalog_table(0).unwrap();
            let root = CatalogTableRootDesc {
                table_id: 0,
                root_block_id: None,
                pivot_row_id: 0,
            };
            let table_id = USER_OBJ_ID_START + 42;
            let table_ops = vec![
                RowRedoKind::Insert(vec![Val::from(table_id)]),
                RowRedoKind::DeleteByUniqueKey(SelectKey::new(0, vec![Val::from(table_id)])),
            ];
            let mut mutable = MutableMultiTableFile::fork(&storage.mtb);

            let next_root = storage
                .apply_table_ops(&mut mutable, 0, table.metadata(), root, &table_ops, 7)
                .await
                .unwrap();

            assert_eq!(next_root.root_block_id, None);
            assert_eq!(next_root.pivot_row_id, 0);
        });
    }

    #[test]
    fn test_catalog_checkpoint_apply_table_ops_advances_existing_root_for_canceled_insert_batch() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("catalog-checkpoint-canceled-existing-root")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let _ = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let storage = &engine.catalog().storage;
            let table = storage.get_catalog_table(0).unwrap();
            let root = storage.checkpoint_snapshot().unwrap().meta.table_roots[0];
            assert!(root.root_block_id.is_some());

            let table_id = USER_OBJ_ID_START + 4242;
            let table_ops = vec![
                RowRedoKind::Insert(vec![Val::from(table_id)]),
                RowRedoKind::DeleteByUniqueKey(SelectKey::new(0, vec![Val::from(table_id)])),
            ];
            let mut mutable = MutableMultiTableFile::fork(&storage.mtb);

            let next_root = storage
                .apply_table_ops(&mut mutable, 0, table.metadata(), root, &table_ops, 8)
                .await
                .unwrap();

            assert_eq!(next_root.root_block_id, root.root_block_id);
            assert_eq!(next_root.pivot_row_id, root.pivot_row_id + 1);
        });
    }

    #[test]
    fn test_catalog_checkpoint_collect_index_entries_uses_readonly_cache() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("catalog-checkpoint-readonly-cache")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let _ = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let snap = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let tables_root = snap.meta.table_roots[0];
            let root_block_id = BlockID::from(tables_root.root_block_id.unwrap().get());
            let disk_pool_guard = engine.catalog().storage.disk_pool.pool_guard();

            assert_eq!(engine.disk_pool.allocated(), 0);

            let entries1 = engine
                .catalog()
                .storage
                .collect_index_entries(&disk_pool_guard, root_block_id)
                .await
                .unwrap();
            assert!(!entries1.is_empty());

            let cached_after_first = engine.disk_pool.allocated();
            assert!(cached_after_first >= 1);
            let root_key = BlockKey::new(CATALOG_MTB_PERSISTED_FILE_ID, root_block_id);
            assert!(engine.disk_pool.try_get_frame_id(&root_key).is_some());

            let entries2 = engine
                .catalog()
                .storage
                .collect_index_entries(&disk_pool_guard, root_block_id)
                .await
                .unwrap();
            assert_eq!(entries2.len(), entries1.len());
            assert_eq!(engine.disk_pool.allocated(), cached_after_first);
        });
    }

    #[test]
    fn test_catalog_checkpoint_tail_merge_rewrites_last_payload_without_new_entry() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("catalog-checkpoint-tail-merge")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let _ = table1(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let snap1 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let tables_root1 = snap1.meta.table_roots[0];
            assert!(tables_root1.root_block_id.is_some());
            let disk_pool_guard = engine.catalog().storage.disk_pool.pool_guard();
            let entries1 = engine
                .catalog()
                .storage
                .collect_index_entries(
                    &disk_pool_guard,
                    BlockID::from(tables_root1.root_block_id.unwrap().get()),
                )
                .await
                .unwrap();
            assert!(!entries1.is_empty());

            let _ = table2(&engine).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let snap2 = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let tables_root2 = snap2.meta.table_roots[0];
            let entries2 = engine
                .catalog()
                .storage
                .collect_index_entries(
                    &disk_pool_guard,
                    BlockID::from(tables_root2.root_block_id.unwrap().get()),
                )
                .await
                .unwrap();

            assert!(tables_root2.pivot_row_id > tables_root1.pivot_row_id);
            assert!(tables_root2.root_block_id != tables_root1.root_block_id);
            assert_eq!(
                entries2.len(),
                entries1.len(),
                "tail-page merge should reuse the existing last index entry when capacity allows"
            );

            let last1 = entries1.last().copied().unwrap();
            let last2 = entries2.last().copied().unwrap();
            let index1 = ColumnBlockIndex::new(
                BlockID::from(tables_root1.root_block_id.unwrap().get()),
                tables_root1.pivot_row_id,
                &engine.catalog().storage.disk_pool,
                &disk_pool_guard,
            );
            let index2 = ColumnBlockIndex::new(
                BlockID::from(tables_root2.root_block_id.unwrap().get()),
                tables_root2.pivot_row_id,
                &engine.catalog().storage.disk_pool,
                &disk_pool_guard,
            );
            assert_eq!(last2.start_row_id, last1.start_row_id);
            assert_ne!(last2.block_id(), last1.block_id());
            assert_eq!(last2.delete_domain(), ColumnDeleteDomain::Ordinal);
            assert_eq!(
                load_entry_deletion_deltas(&index2, &last2).await.unwrap(),
                load_entry_deletion_deltas(&index1, &last1).await.unwrap()
            );
        });
    }
}
