use crate::buffer::PageID;
use crate::error::{Error, Result, StoragePoisonSource};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::table_file::MutableTableFile;
use crate::index::{
    ColumnBlockIndex, ColumnDeleteDeltaPatch, ColumnLeafEntry, load_entry_deletion_deltas,
};
use crate::session::Session;
use crate::table::Table;
use crate::trx::TrxID;
use crate::trx::redo::DDLRedo;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

pub trait TablePersistence {
    /// Freeze row pages and return approximate non-deleted rows visited.
    fn freeze(&self, session: &Session, max_rows: usize) -> impl Future<Output = usize>;

    /// Persist eligible row-store and cold-delete state in one checkpoint run.
    fn checkpoint(&self, session: &mut Session) -> impl Future<Output = Result<()>>;
}

#[derive(Clone, Copy)]
struct BlockPatchSeed {
    entry: ColumnLeafEntry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeletionCheckpointOutcome {
    NoOp,
    MetadataOnly,
    PayloadChanged,
}

impl DeletionCheckpointOutcome {
    #[inline]
    fn changed(self) -> bool {
        !matches!(self, Self::NoOp)
    }
}

impl Table {
    async fn run_checkpoint(
        &self,
        session: &mut Session,
        include_new_data: bool,
        include_deletion: bool,
    ) -> Result<()> {
        let table_file = self.file();
        let disk_pool = self.disk_pool();
        // Step 1: snapshot current table root and initialize checkpoint boundaries.
        let trx_sys = session.engine().trx_sys.clone();
        let active_root = table_file.active_root();
        let pivot_row_id = active_root.pivot_row_id;

        // Step 3: open a checkpoint transaction and prepare per-phase state.
        let mut trx = session
            .try_begin_trx()?
            .ok_or(Error::NotSupported("checkpoint requires idle session"))?;
        let checkpoint_ts = trx.sts;
        let mut new_pivot_row_id = pivot_row_id;
        let mut lwc_blocks = Vec::new();
        let mut heap_redo_start_ts = active_root.heap_redo_start_ts;

        // Step 2: for new-data checkpoint, collect frozen pages and move them into
        // transition state under a refreshed cutoff timestamp.
        let mut cutoff_ts = trx_sys.calc_min_active_sts_for_gc();
        if include_new_data {
            let pool_guards = session.pool_guards();
            let (frozen_pages, next_heap_redo_start_ts) =
                self.collect_frozen_pages(pool_guards).await;
            if !frozen_pages.is_empty() {
                self.wait_for_frozen_pages_stable(pool_guards, &trx_sys, &frozen_pages)
                    .await?;
                cutoff_ts = trx_sys.calc_min_active_sts_for_gc();
                self.set_frozen_pages_to_transition(pool_guards, &frozen_pages, cutoff_ts)
                    .await?;
            }

            // Step 4: build LWC blocks from transition pages using the cutoff snapshot.
            new_pivot_row_id = frozen_pages
                .last()
                .map(|page| page.end_row_id)
                .unwrap_or(pivot_row_id);

            match self
                .build_lwc_blocks(session.pool_guards(), cutoff_ts, &frozen_pages)
                .await
            {
                Ok(pages) => {
                    lwc_blocks = pages;
                    heap_redo_start_ts = next_heap_redo_start_ts.unwrap_or(checkpoint_ts);
                }
                Err(err) => {
                    trx_sys.rollback(trx).await?;
                    return Err(err);
                }
            }

            if let Some(last) = lwc_blocks.last_mut()
                && last.shape.end_row_id() < new_pivot_row_id
            {
                last.shape.set_end_row_id(new_pivot_row_id)?;
            }

            let gc_pages: Vec<PageID> = frozen_pages.iter().map(|page| page.page_id).collect();
            trx.extend_gc_row_pages(gc_pages);
        }
        // Step 5: emit one checkpoint redo marker for recovery.
        trx.redo.ddl = Some(Box::new(DDLRedo::DataCheckpoint {
            table_id: self.table_id(),
            pivor_row_id: new_pivot_row_id,
            sts: checkpoint_ts,
        }));

        // Step 6: fork mutable table-file state and apply selected phases.
        let mut mutable_file =
            MutableTableFile::fork(table_file, session.engine().table_fs.background_writes());
        let mut table_file_changed = false;
        if include_new_data {
            table_file_changed = true;
            if !lwc_blocks.is_empty() {
                mutable_file
                    .apply_lwc_blocks(lwc_blocks, heap_redo_start_ts, checkpoint_ts, disk_pool)
                    .await?;
            } else {
                mutable_file.apply_checkpoint_metadata(new_pivot_row_id, heap_redo_start_ts)?;
            }
        }

        // Step 7: merge committed cold-row deletions into column index payloads
        // and publish the durable cold-delete replay watermark.
        if include_deletion {
            let outcome = match self
                .apply_deletion_checkpoint(&mut mutable_file, cutoff_ts, checkpoint_ts)
                .await
            {
                Ok(outcome) => outcome,
                Err(err) => {
                    trx_sys.rollback(trx).await?;
                    return Err(err);
                }
            };
            table_file_changed |= outcome.changed();
        }

        // Step 8: rollback no-op checkpoint transactions to avoid empty commits.
        if !table_file_changed {
            trx_sys.rollback(trx).await?;
            return Ok(());
        }

        // Step 9: publish new table-file root and then commit checkpoint transaction.
        let (table_file, old_root) = match mutable_file.commit(checkpoint_ts, false).await {
            Ok(res) => res,
            Err(err) if err.is_storage_io_failure() || matches!(err, Error::SendError) => {
                let _ = trx_sys.rollback(trx).await;
                let poison = trx_sys.poison_storage(StoragePoisonSource::CheckpointWrite);
                return Err(poison);
            }
            Err(err) => {
                trx_sys.rollback(trx).await?;
                return Err(err);
            }
        };
        let active_root = table_file.active_root();
        self.blk_idx()
            .update_column_root(
                active_root.pivot_row_id,
                active_root.column_block_index_root,
            )
            .await;
        drop(old_root);

        let _cts = trx_sys.commit(trx).await?;
        Ok(())
    }

    async fn apply_deletion_checkpoint(
        &self,
        mutable_file: &mut MutableTableFile,
        cutoff_ts: TrxID,
        checkpoint_ts: TrxID,
    ) -> Result<DeletionCheckpointOutcome> {
        let disk_pool = self.disk_pool();
        let disk_pool_guard = disk_pool.pool_guard();
        let (column_block_index_root, pivot_row_id, deletion_cutoff_ts) = {
            let root = mutable_file.root();
            (
                root.column_block_index_root,
                root.pivot_row_id,
                root.deletion_cutoff_ts,
            )
        };
        let cutoff_advanced = cutoff_ts > deletion_cutoff_ts;
        // Step 1: ensure there is a persisted column index to patch.
        if column_block_index_root == SUPER_BLOCK_ID || pivot_row_id == 0 {
            if cutoff_advanced {
                mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
                return Ok(DeletionCheckpointOutcome::MetadataOnly);
            }
            return Ok(DeletionCheckpointOutcome::NoOp);
        }

        // Step 2: pick committed deletion markers that are visible at cutoff.
        // This is currently a simple scan + in-place filter + sort/dedup path.
        // For very large deletion buffers, we can optimize this step later by
        // introducing parallel marker selection and parallel sort/merge while
        // preserving deterministic ordering before patch application.
        let mut selected_row_ids = self.deletion_buffer().collect_committed_before(cutoff_ts);
        selected_row_ids.retain(|row_id| *row_id < pivot_row_id);
        if selected_row_ids.is_empty() {
            if cutoff_advanced {
                mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
                return Ok(DeletionCheckpointOutcome::MetadataOnly);
            }
            return Ok(DeletionCheckpointOutcome::NoOp);
        }
        selected_row_ids.sort_unstable();
        selected_row_ids.dedup();

        // Step 3: resolve each row-id to its persisted block payload.
        // Future improvement:
        // 1) resolve disjoint row-id ranges in parallel for higher throughput;
        // 2) when roaring bitmap encoding is introduced, also fetch the target LWC block
        //    row-id array to map each input row-id to the correct offset-based bit index.
        let column_index = ColumnBlockIndex::new(
            column_block_index_root,
            pivot_row_id,
            self.file().file_kind(),
            self.file().sparse_file(),
            disk_pool,
            &disk_pool_guard,
        );

        let mut grouped: BTreeMap<u64, (BlockPatchSeed, BTreeSet<u32>)> = BTreeMap::new();
        for row_id in selected_row_ids {
            let Some(entry) = column_index.locate_block(row_id).await? else {
                continue;
            };
            let delta_u64 = row_id
                .checked_sub(entry.start_row_id)
                .ok_or(Error::InvalidState)?;
            if delta_u64 > u32::MAX as u64 {
                return Err(Error::InvalidState);
            }
            let delta = delta_u64 as u32;
            let entry = grouped
                .entry(entry.start_row_id)
                .or_insert((BlockPatchSeed { entry }, BTreeSet::new()));
            entry.1.insert(delta);
        }
        if grouped.is_empty() {
            if cutoff_advanced {
                mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
                return Ok(DeletionCheckpointOutcome::MetadataOnly);
            }
            return Ok(DeletionCheckpointOutcome::NoOp);
        }

        // Step 4: load authoritative persisted deltas and merge pending row-id deltas.
        let mut patch_storage: Vec<(u64, Vec<u32>)> = Vec::new();
        for (start_row_id, (seed, pending)) in grouped {
            let mut base = load_entry_deletion_deltas(&column_index, &seed.entry).await?;
            let old_len = base.len();
            base.extend(pending);
            if base.len() == old_len {
                continue;
            }
            patch_storage.push((start_row_id, base.into_iter().collect()));
        }
        if patch_storage.is_empty() {
            if cutoff_advanced {
                mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
                return Ok(DeletionCheckpointOutcome::MetadataOnly);
            }
            return Ok(DeletionCheckpointOutcome::NoOp);
        }

        // Step 5: apply typed delete rewrites and advance the index root in the mutable file.
        let patches: Vec<ColumnDeleteDeltaPatch<'_>> = patch_storage
            .iter()
            .map(|(start_row_id, delete_deltas)| ColumnDeleteDeltaPatch {
                start_row_id: *start_row_id,
                delete_deltas,
            })
            .collect();
        let new_root = column_index
            .batch_replace_delete_deltas(mutable_file, &patches, checkpoint_ts)
            .await?;
        mutable_file.set_column_block_index_root(new_root);
        mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
        Ok(DeletionCheckpointOutcome::PayloadChanged)
    }
}

impl TablePersistence for Table {
    async fn freeze(&self, session: &Session, max_rows: usize) -> usize {
        let mut rows = 0usize;
        self.mem_scan(session.pool_guards(), |page_guard| {
            let (ctx, page) = page_guard.ctx_and_page();
            let vmap = ctx.row_ver().unwrap();
            rows += page.header.approx_non_deleted();
            if vmap.is_frozen() {
                return rows < max_rows;
            }
            vmap.set_frozen();
            rows < max_rows
        })
        .await;
        rows
    }

    async fn checkpoint(&self, session: &mut Session) -> Result<()> {
        // Run both new-data and deletion phases in one combined checkpoint.
        self.run_checkpoint(session, true, true).await
    }
}
