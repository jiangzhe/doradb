use crate::buffer::page::PageID;
use crate::error::{Error, Result};
use crate::file::table_file::MutableTableFile;
use crate::index::{ColumnBlockIndex, OffloadedBitmapPatch};
use crate::session::Session;
use crate::table::Table;
use crate::trx::TrxID;
use crate::trx::redo::DDLRedo;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

pub trait TablePersistence {
    /// Freeze row pages and return approximate non-deleted rows visited.
    fn freeze(&self, max_rows: usize) -> impl Future<Output = usize>;

    /// Convert frozen row pages to LWC pages and persist to table file.
    fn checkpoint_for_new_data(&self, session: &mut Session) -> impl Future<Output = Result<()>>;

    /// Persist committed cold-row deletions from memory to column block index.
    fn checkpoint_for_deletion(&self, session: &mut Session) -> impl Future<Output = Result<()>>;

    /// Combined checkpoint that persists both new data and deletions in one run.
    fn data_checkpoint(&self, session: &mut Session) -> impl Future<Output = Result<()>>;
}

#[derive(Clone, Copy)]
struct BlockPatchSeed {
    payload: crate::index::ColumnPagePayload,
}

impl Table {
    async fn run_checkpoint(
        &self,
        session: &mut Session,
        include_new_data: bool,
        include_deletion: bool,
    ) -> Result<()> {
        // Step 1: snapshot current table root and initialize checkpoint boundaries.
        let trx_sys = session.engine().trx_sys;
        let active_root = self.file.active_root();
        let pivot_row_id = active_root.pivot_row_id;
        let mut next_heap_redo_start_ts = None;

        // Step 2: for new-data checkpoint, collect frozen pages and move them into
        // transition state under a refreshed cutoff timestamp.
        let mut cutoff_ts = trx_sys.calc_min_active_sts_for_gc();
        let mut frozen_pages = Vec::new();
        if include_new_data {
            let (pages, candidate_heap_redo_start_ts) =
                self.collect_frozen_pages(pivot_row_id).await;
            frozen_pages = pages;
            if !frozen_pages.is_empty() {
                self.wait_for_frozen_pages_stable(trx_sys, &frozen_pages)
                    .await;
                cutoff_ts = trx_sys.calc_min_active_sts_for_gc();
                self.set_frozen_pages_to_transition(&frozen_pages, cutoff_ts)
                    .await;
            }
            next_heap_redo_start_ts = candidate_heap_redo_start_ts;
        }

        // Step 3: open a checkpoint transaction and prepare per-phase state.
        let mut trx = session
            .begin_trx()
            .ok_or(Error::NotSupported("checkpoint requires idle session"))?;
        let checkpoint_ts = trx.sts;
        let mut new_pivot_row_id = pivot_row_id;
        let mut lwc_pages = Vec::new();
        let mut heap_redo_start_ts = active_root.heap_redo_start_ts;
        if include_new_data {
            // Step 4: build LWC pages from transition pages using the cutoff snapshot.
            new_pivot_row_id = frozen_pages
                .last()
                .map(|page| page.end_row_id)
                .unwrap_or(pivot_row_id);

            match self.build_lwc_pages(cutoff_ts, &frozen_pages).await {
                Ok(pages) => {
                    lwc_pages = pages;
                    heap_redo_start_ts = next_heap_redo_start_ts.unwrap_or(checkpoint_ts);
                }
                Err(err) => {
                    trx_sys.rollback(trx).await;
                    return Err(err);
                }
            }

            if let Some(last) = lwc_pages.last_mut()
                && last.end_row_id < new_pivot_row_id
            {
                last.end_row_id = new_pivot_row_id;
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
        let mut mutable_file = MutableTableFile::fork(&self.file);
        let mut table_file_changed = false;
        if include_new_data {
            table_file_changed = true;
            if !lwc_pages.is_empty() {
                mutable_file
                    .apply_lwc_pages(
                        lwc_pages,
                        heap_redo_start_ts,
                        checkpoint_ts,
                        &self.disk_pool,
                    )
                    .await?;
            } else {
                mutable_file.apply_checkpoint_metadata(new_pivot_row_id, heap_redo_start_ts)?;
            }
        }

        // Step 7: merge committed cold-row deletions into column index payloads.
        if include_deletion {
            let changed = match self
                .apply_deletion_checkpoint(&mut mutable_file, cutoff_ts, checkpoint_ts)
                .await
            {
                Ok(changed) => changed,
                Err(err) => {
                    trx_sys.rollback(trx).await;
                    return Err(err);
                }
            };
            table_file_changed |= changed;
        }

        // Step 8: rollback no-op checkpoint transactions to avoid empty commits.
        if !table_file_changed {
            trx_sys.rollback(trx).await;
            return Ok(());
        }

        // Step 9: publish new table-file root and then commit checkpoint transaction.
        let (table_file, old_root) = mutable_file.commit(checkpoint_ts, false).await?;
        let active_root = table_file.active_root();
        self.blk_idx
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
    ) -> Result<bool> {
        // Step 1: ensure there is a persisted column index to patch.
        let mutable_root = mutable_file.root();
        if mutable_root.column_block_index_root == 0 || mutable_root.pivot_row_id == 0 {
            return Ok(false);
        }

        // Step 2: pick committed deletion markers that are visible at cutoff.
        // This is currently a simple scan + in-place filter + sort/dedup path.
        // For very large deletion buffers, we can optimize this step later by
        // introducing parallel marker selection and parallel sort/merge while
        // preserving deterministic ordering before patch application.
        let mut selected_row_ids = self.deletion_buffer.collect_committed_before(cutoff_ts);
        selected_row_ids.retain(|row_id| *row_id < mutable_root.pivot_row_id);
        if selected_row_ids.is_empty() {
            return Ok(false);
        }
        selected_row_ids.sort_unstable();
        selected_row_ids.dedup();

        // Step 3: resolve each row-id to its persisted block payload.
        // Future improvement:
        // 1) resolve disjoint row-id ranges in parallel for higher throughput;
        // 2) when roaring bitmap encoding is introduced, also fetch the target LWC page
        //    row-id array to map each input row-id to the correct offset-based bit index.
        let column_index = ColumnBlockIndex::new(
            mutable_root.column_block_index_root,
            mutable_root.pivot_row_id,
            &self.disk_pool,
        );

        let mut grouped: BTreeMap<u64, (BlockPatchSeed, BTreeSet<u32>)> = BTreeMap::new();
        for row_id in selected_row_ids {
            let Some((start_row_id, payload)) = column_index.find_entry(row_id).await? else {
                continue;
            };
            let delta_u64 = row_id
                .checked_sub(start_row_id)
                .ok_or(Error::InvalidState)?;
            if delta_u64 > u32::MAX as u64 {
                return Err(Error::InvalidState);
            }
            let delta = delta_u64 as u32;
            let entry = grouped
                .entry(start_row_id)
                .or_insert((BlockPatchSeed { payload }, BTreeSet::new()));
            entry.1.insert(delta);
        }
        if grouped.is_empty() {
            return Ok(false);
        }

        // Step 4: load persisted deltas, merge pending deltas, and build patch bytes.
        let mut patch_storage: Vec<(u64, Vec<u8>)> = Vec::new();
        for (start_row_id, (seed, pending)) in grouped {
            let mut base = self
                .load_persisted_deletion_deltas(&column_index, start_row_id, seed.payload)
                .await?;
            let old_len = base.len();
            base.extend(pending);
            if base.len() == old_len {
                continue;
            }
            patch_storage.push((start_row_id, encode_deltas_to_bytes(&base)));
        }
        if patch_storage.is_empty() {
            return Ok(false);
        }

        // Step 5: apply offloaded bitmap patches and advance index root in mutable file.
        let patches: Vec<OffloadedBitmapPatch<'_>> = patch_storage
            .iter()
            .map(|(start_row_id, bytes)| OffloadedBitmapPatch {
                start_row_id: *start_row_id,
                bitmap_bytes: bytes,
            })
            .collect();
        let new_root = column_index
            .batch_update_offloaded_bitmaps(mutable_file, &patches, checkpoint_ts)
            .await?;
        mutable_file.set_column_block_index_root(new_root);
        Ok(true)
    }

    async fn load_persisted_deletion_deltas(
        &self,
        column_index: &ColumnBlockIndex<'_>,
        _start_row_id: u64,
        mut payload: crate::index::ColumnPagePayload,
    ) -> Result<BTreeSet<u32>> {
        let mut res = BTreeSet::new();
        if let Some(bytes) = column_index.read_offloaded_bitmap_bytes(&payload).await? {
            for delta in decode_deltas_from_bytes(&bytes)? {
                res.insert(delta);
            }
            return Ok(res);
        }
        let list = payload.deletion_list();
        res.extend(list.iter());
        Ok(res)
    }
}

impl TablePersistence for Table {
    async fn freeze(&self, max_rows: usize) -> usize {
        let mut rows = 0usize;
        self.mem_scan(0, |page_guard| {
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

    async fn checkpoint_for_new_data(&self, session: &mut Session) -> Result<()> {
        // Run only the new-data phase.
        self.run_checkpoint(session, true, false).await
    }

    async fn checkpoint_for_deletion(&self, session: &mut Session) -> Result<()> {
        // Run only the deletion phase.
        self.run_checkpoint(session, false, true).await
    }

    async fn data_checkpoint(&self, session: &mut Session) -> Result<()> {
        // Run both new-data and deletion phases in one combined checkpoint.
        self.run_checkpoint(session, true, true).await
    }
}

fn encode_deltas_to_bytes(deltas: &BTreeSet<u32>) -> Vec<u8> {
    // Current on-disk blob encoding is a sorted little-endian u32 delta array.
    // Future improvement: switch to roaring bitmap encoding for better space and merge efficiency.
    let mut out = Vec::with_capacity(deltas.len() * std::mem::size_of::<u32>());
    for delta in deltas {
        out.extend_from_slice(&delta.to_le_bytes());
    }
    out
}

fn decode_deltas_from_bytes(bytes: &[u8]) -> Result<Vec<u32>> {
    // Must match `encode_deltas_to_bytes` format until roaring bitmap encoding is introduced.
    if bytes.is_empty() || !bytes.len().is_multiple_of(std::mem::size_of::<u32>()) {
        return Err(Error::InvalidFormat);
    }
    let mut res = Vec::with_capacity(bytes.len() / std::mem::size_of::<u32>());
    for chunk in bytes.chunks_exact(std::mem::size_of::<u32>()) {
        res.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    res.sort_unstable();
    res.dedup();
    Ok(res)
}
