use crate::buffer::PageID;
use crate::catalog::{IndexSpec, TableMetadata};
use crate::error::{Error, Result, StoragePoisonSource};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::table_file::{ActiveRoot, MutableTableFile};
use crate::index::BTreeKeyEncoder;
use crate::index::disk_tree::{
    NonUniqueDiskTreeEncodedExact, UniqueDiskTreeEncodedDelete, UniqueDiskTreeEncodedPut,
};
use crate::index::{ColumnBlockIndex, ColumnDeleteDeltaPatch, ColumnLeafEntry};
use crate::lwc::PersistedLwcBlock;
use crate::row::RowID;
use crate::session::Session;
use crate::table::Table;
use crate::trx::TrxID;
use crate::trx::redo::DDLRedo;
use crate::value::{Val, ValKind, ValType};
use std::collections::BTreeSet;
use std::future::Future;

const CHECKPOINT_REQUIRES_IDLE_SESSION: &str = "checkpoint requires idle session";

pub trait TablePersistence {
    /// Freeze row pages and return approximate non-deleted rows visited.
    fn freeze(&self, session: &Session, max_rows: usize) -> impl Future<Output = usize>;

    /// Report whether a user-table checkpoint can safely publish now.
    fn checkpoint_readiness(&self, session: &Session) -> CheckpointReadiness;

    /// Persist eligible row-store and cold-delete state in one checkpoint run.
    fn checkpoint(&self, session: &mut Session) -> impl Future<Output = Result<CheckpointOutcome>>;
}

/// Cheap checkpoint scheduling decision for one user-table root snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointReadiness {
    /// The active root is older than the current GC horizon.
    Ready,
    /// Checkpoint should be retried after the GC horizon advances.
    Delayed {
        /// Diagnostic details explaining why checkpoint should wait.
        reason: CheckpointDelayReason,
    },
}

/// User-table checkpoint execution result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointOutcome {
    /// A new checkpoint root was durably published.
    Published {
        /// Commit timestamp of the publishing checkpoint transaction.
        checkpoint_ts: TrxID,
    },
    /// No checkpoint work was published because the active root is still live.
    Delayed {
        /// Diagnostic details explaining why checkpoint waited.
        reason: CheckpointDelayReason,
    },
}

/// Diagnostic payload for normal checkpoint scheduling delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointDelayReason {
    /// Checkpoint timestamp of the active table-file root.
    pub root_cts: TrxID,
    /// Current global minimum active snapshot timestamp used by GC.
    pub min_active_sts: TrxID,
}

impl CheckpointReadiness {
    #[inline]
    fn for_root(active_root: &ActiveRoot, min_active_sts: TrxID) -> Self {
        if active_root.trx_id < min_active_sts {
            Self::Ready
        } else {
            Self::Delayed {
                reason: CheckpointDelayReason {
                    root_cts: active_root.trx_id,
                    min_active_sts,
                },
            }
        }
    }
}

#[derive(Clone, Copy)]
struct BlockPatchGroup {
    entry: ColumnLeafEntry,
    pending_start: usize,
    pending_end: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EncodedRowEntry {
    key: Vec<u8>,
    row_id: RowID,
}

// Checkpoint-local accumulator for one secondary index. Batches are appended
// while scanning checkpoint inputs, then sorted and coalesced once before the
// DiskTree writer runs.
enum SecondaryIndexSidecar {
    Unique {
        encoder: BTreeKeyEncoder,
        puts: Vec<EncodedRowEntry>,
        deletes: Vec<EncodedRowEntry>,
    },
    NonUnique {
        encoder: BTreeKeyEncoder,
        inserts: Vec<Vec<u8>>,
        deletes: Vec<Vec<u8>>,
    },
}

impl SecondaryIndexSidecar {
    #[inline]
    fn new(metadata: &TableMetadata, index_spec: &IndexSpec) -> Result<Self> {
        if index_spec.unique() {
            Ok(Self::Unique {
                encoder: secondary_disk_tree_encoder(metadata, index_spec, false)?,
                puts: Vec::new(),
                deletes: Vec::new(),
            })
        } else {
            Ok(Self::NonUnique {
                encoder: secondary_disk_tree_encoder(metadata, index_spec, true)?,
                inserts: Vec::new(),
                deletes: Vec::new(),
            })
        }
    }

    #[inline]
    fn has_work(&self) -> bool {
        match self {
            Self::Unique { puts, deletes, .. } => !puts.is_empty() || !deletes.is_empty(),
            Self::NonUnique {
                inserts, deletes, ..
            } => !inserts.is_empty() || !deletes.is_empty(),
        }
    }

    fn add_data(&mut self, key: Vec<Val>, row_id: RowID) {
        match self {
            Self::Unique { encoder, puts, .. } => {
                let encoded = encoder.encode(&key).as_bytes().to_vec();
                puts.push(EncodedRowEntry {
                    key: encoded,
                    row_id,
                });
                // Same-run delete suppression is applied once, after all data
                // and deletion sidecar inputs are sorted.
            }
            Self::NonUnique {
                encoder, inserts, ..
            } => {
                // Non-unique DiskTree stores exact (logical_key, row_id)
                // membership; there is no durable delete-mask value.
                let encoded = encoder
                    .encode_pair(&key, Val::from(row_id))
                    .as_bytes()
                    .to_vec();
                inserts.push(encoded);
            }
        }
    }

    fn add_delete(&mut self, key: Vec<Val>, row_id: RowID) {
        match self {
            Self::Unique {
                encoder, deletes, ..
            } => {
                let encoded = encoder.encode(&key).as_bytes().to_vec();
                // Unique deletes are conditional on the old owner. They are
                // suppressed during normalization if this checkpoint also
                // publishes a new owner for the same key.
                deletes.push(EncodedRowEntry {
                    key: encoded,
                    row_id,
                });
            }
            Self::NonUnique {
                encoder, deletes, ..
            } => {
                let encoded = encoder
                    .encode_pair(&key, Val::from(row_id))
                    .as_bytes()
                    .to_vec();
                deletes.push(encoded);
            }
        }
    }

    fn normalize(&mut self) {
        match self {
            Self::Unique { puts, deletes, .. } => {
                normalize_unique_puts(puts);
                normalize_unique_deletes(deletes, puts);
            }
            Self::NonUnique {
                inserts, deletes, ..
            } => {
                normalize_encoded_keys(inserts);
                normalize_encoded_keys(deletes);
                inserts.retain(|key| deletes.binary_search(key).is_err());
            }
        }
    }
}

fn normalize_unique_puts(puts: &mut Vec<EncodedRowEntry>) {
    puts.sort_by(|left, right| left.key.cmp(&right.key));
    let mut normalized: Vec<EncodedRowEntry> = Vec::with_capacity(puts.len());
    for entry in puts.drain(..) {
        if let Some(last) = normalized.last_mut()
            && last.key == entry.key
        {
            last.row_id = entry.row_id;
            continue;
        }
        normalized.push(entry);
    }
    *puts = normalized;
}

fn normalize_unique_deletes(deletes: &mut Vec<EncodedRowEntry>, puts: &[EncodedRowEntry]) {
    deletes.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then(left.row_id.cmp(&right.row_id))
    });
    deletes.dedup();
    deletes.retain(|delete| {
        puts.binary_search_by(|put| put.key.as_slice().cmp(delete.key.as_slice()))
            .is_err()
    });
}

fn normalize_encoded_keys(keys: &mut Vec<Vec<u8>>) {
    keys.sort();
    keys.dedup();
}

struct SecondaryCheckpointSidecar {
    indexes: Vec<SecondaryIndexSidecar>,
}

impl SecondaryCheckpointSidecar {
    fn new(metadata: &TableMetadata) -> Result<Self> {
        let indexes = metadata
            .index_specs
            .iter()
            .map(|index_spec| SecondaryIndexSidecar::new(metadata, index_spec))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { indexes })
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.indexes.iter().all(|index| !index.has_work())
    }

    fn add_data_row(
        &mut self,
        metadata: &TableMetadata,
        page: &crate::row::RowPage,
        row_idx: usize,
        row_id: RowID,
    ) -> Result<()> {
        if self.indexes.len() != metadata.index_specs.len() {
            return Err(Error::InvalidState);
        }
        // Data checkpoint feeds committed-visible transition rows here, once
        // per row selected for persistence.
        for (index_spec, sidecar) in metadata.index_specs.iter().zip(self.indexes.iter_mut()) {
            let key = index_spec
                .index_cols
                .iter()
                .map(|index_key| page.val(metadata, row_idx, index_key.col_no as usize))
                .collect();
            sidecar.add_data(key, row_id);
        }
        Ok(())
    }

    fn add_deleted_key(&mut self, index_no: usize, row_id: RowID, key: Vec<Val>) -> Result<()> {
        let sidecar = self.indexes.get_mut(index_no).ok_or(Error::InvalidState)?;
        sidecar.add_delete(key, row_id);
        Ok(())
    }
}

fn secondary_disk_tree_encoder(
    metadata: &TableMetadata,
    index_spec: &IndexSpec,
    append_row_id: bool,
) -> Result<BTreeKeyEncoder> {
    if index_spec.index_cols.is_empty() {
        return Err(Error::InvalidArgument);
    }
    let mut types = Vec::with_capacity(index_spec.index_cols.len() + usize::from(append_row_id));
    for key in &index_spec.index_cols {
        let col_no = key.col_no as usize;
        let ty = metadata
            .col_types()
            .get(col_no)
            .copied()
            .ok_or(Error::InvalidArgument)?;
        types.push(ty);
    }
    if append_row_id {
        types.push(ValType::new(ValKind::U64, false));
    }
    Ok(BTreeKeyEncoder::new(types))
}

impl Table {
    async fn apply_deletion_checkpoint(
        &self,
        mutable_file: &mut MutableTableFile,
        secondary_sidecar: &mut SecondaryCheckpointSidecar,
        cutoff_ts: TrxID,
        checkpoint_ts: TrxID,
    ) -> Result<()> {
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

        // Step 1: pick committed deletion markers in this checkpoint's durable
        // replay range: [previous_cutoff, current_cutoff). Markers below
        // deletion_cutoff_ts were already covered by an earlier checkpoint,
        // even if the in-memory deletion buffer has not been GC'ed yet. Markers
        // at or above cutoff_ts are intentionally left for a later checkpoint
        // because they may have just been moved from transition row pages.
        // This is currently a simple scan + in-place filter + sort/dedup path.
        // For very large deletion buffers, we can optimize this step later by
        // introducing parallel marker selection and parallel sort/merge while
        // preserving deterministic ordering before patch application.
        let mut selected_row_ids = self
            .deletion_buffer()
            .collect_committed_in_range(deletion_cutoff_ts, cutoff_ts);
        selected_row_ids.retain(|row_id| *row_id < pivot_row_id);
        if selected_row_ids.is_empty() {
            // No eligible cold delete marker remains for this checkpoint range,
            // so advancing only metadata is safe even when there is no persisted
            // column_block_index_root yet, e.g. all frozen rows were already
            // deleted and no LWC block was produced.
            if cutoff_advanced {
                mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
                return Ok(());
            }
            return Ok(());
        }
        selected_row_ids.sort_unstable();
        selected_row_ids.dedup();

        // Step 2: fail closed when eligible cold delete markers exist but the
        // persisted column index needed to resolve them is absent. Advancing
        // deletion_cutoff_ts here would make recovery skip delete redo that was
        // never reflected in column payloads.
        if column_block_index_root == SUPER_BLOCK_ID || pivot_row_id == 0 {
            return Err(Error::InvalidState);
        }

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

        let mut groups: Vec<BlockPatchGroup> = Vec::new();
        let mut pending_deltas = Vec::new();
        let mut cached_entry: Option<ColumnLeafEntry> = None;
        for row_id in selected_row_ids {
            let entry = if let Some(entry) = cached_entry
                && row_id >= entry.start_row_id
                && row_id < entry.end_row_id()
            {
                entry
            } else {
                let Some(entry) = column_index.locate_block(row_id).await? else {
                    // The marker is in [previous_cutoff, current_cutoff), so it is
                    // eligible now. A missing locate_block result means we cannot
                    // prove the delete is already durable; do not advance the cutoff.
                    return Err(Error::InvalidState);
                };
                cached_entry = Some(entry);
                entry
            };
            let delta_u64 = row_id
                .checked_sub(entry.start_row_id)
                .ok_or(Error::InvalidState)?;
            if delta_u64 > u32::MAX as u64 {
                return Err(Error::InvalidState);
            }
            let delta = delta_u64 as u32;
            if let Some(group) = groups
                .last_mut()
                .filter(|group| group.entry.start_row_id == entry.start_row_id)
            {
                pending_deltas.push(delta);
                group.pending_end = pending_deltas.len();
            } else {
                let pending_start = pending_deltas.len();
                pending_deltas.push(delta);
                groups.push(BlockPatchGroup {
                    entry,
                    pending_start,
                    pending_end: pending_deltas.len(),
                });
            }
        }
        if groups.is_empty() {
            // Defensive guard: selected markers should either resolve into at
            // least one patch group or fail above.
            return Err(Error::InvalidState);
        }

        // Step 4: load authoritative persisted deltas and merge pending row-id deltas.
        let mut patch_storage: Vec<(u64, Vec<u32>)> = Vec::new();
        for group in groups {
            let pending = &pending_deltas[group.pending_start..group.pending_end];
            let (base_deltas, row_ids) = column_index
                .load_delete_deltas_and_row_ids(&group.entry)
                .await?;
            let mut base = base_deltas.into_iter().collect::<BTreeSet<_>>();
            let new_deltas = pending
                .iter()
                .copied()
                .filter(|delta| !base.contains(delta))
                .collect::<Vec<_>>();
            if new_deltas.is_empty() {
                continue;
            }

            self.collect_deleted_secondary_sidecar(
                &group.entry,
                &row_ids,
                &new_deltas,
                secondary_sidecar,
            )
            .await?;

            base.extend(new_deltas);
            patch_storage.push((group.entry.start_row_id, base.into_iter().collect()));
        }
        if patch_storage.is_empty() {
            if cutoff_advanced {
                mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
                return Ok(());
            }
            return Ok(());
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
        Ok(())
    }

    async fn apply_secondary_checkpoint_sidecar(
        &self,
        mutable_file: &mut MutableTableFile,
        sidecar: &mut SecondaryCheckpointSidecar,
        checkpoint_ts: TrxID,
    ) -> Result<()> {
        if sidecar.is_empty() {
            return Ok(());
        }
        // Apply secondary-index checkpoint work against the same mutable table
        // file fork as LWC and delete metadata. A later error abandons the
        // fork, so no secondary root can be published on its own.
        #[cfg(test)]
        {
            if super::tests::test_force_secondary_sidecar_error_enabled() {
                return Err(Error::InvalidState);
            }
        }

        let metadata = self.metadata();
        if mutable_file.secondary_index_roots().len() != metadata.index_specs.len()
            || sidecar.indexes.len() != metadata.index_specs.len()
        {
            return Err(Error::InvalidState);
        }

        let disk_pool = self.disk_pool();
        let disk_pool_guard = disk_pool.pool_guard();
        for (index_no, index_sidecar) in sidecar.indexes.iter_mut().enumerate() {
            index_sidecar.normalize();
            if !index_sidecar.has_work() {
                continue;
            }
            let old_root = mutable_file.secondary_index_root(index_no)?;
            let runtime = self.storage.secondary_index_runtime(index_no)?;
            let new_root = match index_sidecar {
                SecondaryIndexSidecar::Unique { puts, deletes, .. } => {
                    // Use one writer per affected index so same-run puts and
                    // conditional deletes produce a single new DiskTree root.
                    let tree = runtime.open_unique_at(old_root, &disk_pool_guard)?;
                    let mut writer = tree.batch_writer(mutable_file, checkpoint_ts);
                    let put_entries = puts
                        .iter()
                        .map(|entry| UniqueDiskTreeEncodedPut {
                            key: &entry.key,
                            row_id: entry.row_id,
                        })
                        .collect::<Vec<_>>();
                    writer.batch_put_encoded(&put_entries)?;
                    // Deletes for keys with same-run puts were filtered during
                    // accumulation; remaining deletes must still match the old
                    // checkpointed owner before removing the key.
                    let delete_entries = deletes
                        .iter()
                        .map(|entry| UniqueDiskTreeEncodedDelete {
                            key: &entry.key,
                            expected_old_row_id: entry.row_id,
                        })
                        .collect::<Vec<_>>();
                    writer.batch_conditional_delete_encoded(&delete_entries)?;
                    writer.finish().await?
                }
                SecondaryIndexSidecar::NonUnique {
                    inserts, deletes, ..
                } => {
                    // Non-unique roots are exact-entry sets. Inserts and
                    // deletes are independent facts keyed by (key, row_id).
                    let tree = runtime.open_non_unique_at(old_root, &disk_pool_guard)?;
                    let mut writer = tree.batch_writer(mutable_file, checkpoint_ts);
                    let insert_entries = inserts
                        .iter()
                        .map(|key| NonUniqueDiskTreeEncodedExact { key })
                        .collect::<Vec<_>>();
                    let delete_entries = deletes
                        .iter()
                        .map(|key| NonUniqueDiskTreeEncodedExact { key })
                        .collect::<Vec<_>>();
                    writer.batch_insert_encoded(&insert_entries)?;
                    writer.batch_exact_delete_encoded(&delete_entries)?;
                    writer.finish().await?
                }
            };
            if new_root != old_root {
                // The root update stays private to mutable_file until the
                // final table-file commit publishes every checkpoint change.
                mutable_file.set_secondary_index_root(index_no, new_root)?;
            }
        }
        Ok(())
    }

    async fn collect_deleted_secondary_sidecar(
        &self,
        entry: &ColumnLeafEntry,
        row_ids: &[RowID],
        delete_deltas: &[u32],
        secondary_sidecar: &mut SecondaryCheckpointSidecar,
    ) -> Result<()> {
        if self.metadata().index_specs.is_empty() || delete_deltas.is_empty() {
            return Ok(());
        }

        // ColumnBlockIndex supplies the authoritative row-id ordering for this
        // persisted block, which maps deletion deltas back to row indexes.
        if row_ids.len() != entry.row_count() as usize
            || row_ids.windows(2).any(|window| window[0] >= window[1])
        {
            return Err(Error::InvalidState);
        }
        let dense_row_ids = row_ids.len() == entry.row_id_span() as usize
            && row_ids
                .iter()
                .enumerate()
                .all(|(idx, row_id)| *row_id == entry.start_row_id + idx as RowID);

        let disk_pool = self.disk_pool();
        let disk_pool_guard = disk_pool.pool_guard();
        // Decode the persisted LWC block once for this block group, then derive
        // all secondary delete keys from the selected row indexes.
        let block = PersistedLwcBlock::load(
            self.file().file_kind(),
            self.file().sparse_file(),
            disk_pool,
            &disk_pool_guard,
            entry.block_id(),
        )
        .await?;
        if block.row_count() != row_ids.len()
            || block.row_shape_fingerprint() != entry.row_shape_fingerprint()
        {
            return Err(Error::InvalidState);
        }

        let metadata = self.metadata();
        let index_read_sets = metadata
            .index_specs
            .iter()
            .map(|index_spec| {
                index_spec
                    .index_cols
                    .iter()
                    .map(|index_key| index_key.col_no as usize)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut sparse_row_idx = 0usize;
        for delta in delete_deltas {
            let row_id = entry
                .start_row_id
                .checked_add(RowID::from(*delta))
                .ok_or(Error::InvalidState)?;
            let row_idx = if dense_row_ids {
                usize::try_from(*delta).map_err(|_| Error::InvalidState)?
            } else {
                while row_ids
                    .get(sparse_row_idx)
                    .is_some_and(|current| *current < row_id)
                {
                    sparse_row_idx += 1;
                }
                if row_ids.get(sparse_row_idx) != Some(&row_id) {
                    return Err(Error::InvalidState);
                }
                sparse_row_idx
            };
            if row_idx >= row_ids.len() {
                return Err(Error::InvalidState);
            }
            for (index_no, read_set) in index_read_sets.iter().enumerate() {
                let key = block.decode_row_values(metadata, row_idx, read_set)?;
                secondary_sidecar.add_deleted_key(index_no, row_id, key)?;
            }
        }
        Ok(())
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

    fn checkpoint_readiness(&self, session: &Session) -> CheckpointReadiness {
        let trx_sys = session.engine().trx_sys.clone();
        let active_root = self.file().active_root();
        CheckpointReadiness::for_root(active_root, trx_sys.calc_min_active_sts_for_gc())
    }

    async fn checkpoint(&self, session: &mut Session) -> Result<CheckpointOutcome> {
        if session.in_trx() {
            return Err(Error::NotSupported(CHECKPOINT_REQUIRES_IDLE_SESSION));
        }

        let table_file = self.file();
        let disk_pool = self.disk_pool();
        let trx_sys = session.engine().trx_sys.clone();
        if let CheckpointReadiness::Delayed { reason } = self.checkpoint_readiness(session) {
            return Ok(CheckpointOutcome::Delayed { reason });
        }

        // Step 1: claim one mutable root snapshot and initialize checkpoint boundaries.
        let mut mutable_file =
            MutableTableFile::fork(table_file, session.engine().table_fs.background_writes());
        let pivot_row_id = mutable_file.root().pivot_row_id;
        let mut secondary_sidecar = SecondaryCheckpointSidecar::new(self.metadata())?;

        // Step 2: collect frozen pages and refresh checkpoint cutoff after any
        // stabilization wait. The entry readiness check is enough for root
        // liveness because the GC horizon used by the check only moves forward.
        let pool_guards = session.pool_guards().clone();
        let (frozen_pages, next_heap_redo_start_ts) = self.collect_frozen_pages(&pool_guards).await;
        if !frozen_pages.is_empty() {
            self.wait_for_frozen_pages_stable(&pool_guards, &trx_sys, &frozen_pages)
                .await?;
        }
        let cutoff_ts = trx_sys.calc_min_active_sts_for_gc();

        // Step 3: open a checkpoint transaction, then move frozen pages into
        // transition state under the refreshed cutoff timestamp.
        let mut trx = session
            .try_begin_trx()?
            .ok_or(Error::NotSupported(CHECKPOINT_REQUIRES_IDLE_SESSION))?;
        let checkpoint_ts = trx.sts;
        if !frozen_pages.is_empty() {
            self.set_frozen_pages_to_transition(&pool_guards, &frozen_pages, cutoff_ts)
                .await?;
        }

        // Step 4: build LWC blocks from transition pages using the cutoff
        // snapshot. The sidecar callback observes the same committed-visible
        // rows accepted by the LWC builder, independent of later block splits.
        let new_pivot_row_id = frozen_pages
            .last()
            .map(|page| page.end_row_id)
            .unwrap_or(pivot_row_id);
        let mut collect_secondary_row = |page: &crate::row::RowPage, row_idx, row_id| {
            secondary_sidecar.add_data_row(self.metadata(), page, row_idx, row_id)
        };
        let collect_visible_row = (!self.metadata().index_specs.is_empty()).then_some(
            &mut collect_secondary_row
                as &mut dyn FnMut(&crate::row::RowPage, usize, RowID) -> Result<()>,
        );
        let (mut lwc_blocks, heap_redo_start_ts) = match self
            .build_lwc_blocks(
                session.pool_guards(),
                cutoff_ts,
                &frozen_pages,
                collect_visible_row,
            )
            .await
        {
            Ok(pages) => (pages, next_heap_redo_start_ts.unwrap_or(checkpoint_ts)),
            Err(err) => {
                trx_sys.rollback(trx).await?;
                return Err(err);
            }
        };

        if let Some(last) = lwc_blocks.last_mut()
            && last.shape.end_row_id() < new_pivot_row_id
        {
            last.shape.set_end_row_id(new_pivot_row_id)?;
        }

        let gc_pages: Vec<PageID> = frozen_pages.iter().map(|page| page.page_id).collect();
        trx.extend_gc_row_pages(gc_pages);

        // Step 5: emit one checkpoint redo marker for recovery.
        trx.redo.ddl = Some(Box::new(DDLRedo::DataCheckpoint {
            table_id: self.table_id(),
            pivor_row_id: new_pivot_row_id,
            sts: checkpoint_ts,
        }));

        // Step 6: apply checkpoint changes to the already-checked mutable root.
        if !lwc_blocks.is_empty() {
            if let Err(err) = mutable_file
                .apply_lwc_blocks(lwc_blocks, heap_redo_start_ts, checkpoint_ts, disk_pool)
                .await
            {
                trx_sys.rollback(trx).await?;
                return Err(err);
            }
        } else if let Err(err) =
            mutable_file.apply_checkpoint_metadata(new_pivot_row_id, heap_redo_start_ts)
        {
            trx_sys.rollback(trx).await?;
            return Err(err);
        }

        // Step 7: merge committed cold-row deletions into column index payloads,
        // collect matching secondary-index delete sidecar work, and publish the
        // durable cold-delete replay watermark.
        if let Err(err) = self
            .apply_deletion_checkpoint(
                &mut mutable_file,
                &mut secondary_sidecar,
                cutoff_ts,
                checkpoint_ts,
            )
            .await
        {
            trx_sys.rollback(trx).await?;
            return Err(err);
        }

        // Step 8: apply accumulated secondary-index sidecar work to DiskTree
        // roots on the same mutable file fork. Root publication remains atomic
        // with the table checkpoint commit below.
        if let Err(err) = self
            .apply_secondary_checkpoint_sidecar(
                &mut mutable_file,
                &mut secondary_sidecar,
                checkpoint_ts,
            )
            .await
        {
            trx_sys.rollback(trx).await?;
            return Err(err);
        }

        // Step 9: publish a new table-file root and then commit the checkpoint
        // transaction. This intentionally happens even when no row data,
        // deletion payload, or secondary index root changed: the root trx_id
        // acts as a checkpoint heartbeat for future redo-log truncation.
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
        if let Some(old_root) = old_root
            && let Err(err) = trx.retain_old_table_root(old_root)
        {
            trx_sys.rollback(trx).await?;
            return Err(err);
        }

        let _cts = trx_sys.commit(trx).await?;
        Ok(CheckpointOutcome::Published { checkpoint_ts })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_sidecar_normalize_keeps_latest_put_and_suppresses_delete() {
        let mut puts = vec![
            EncodedRowEntry {
                key: b"b".to_vec(),
                row_id: 20,
            },
            EncodedRowEntry {
                key: b"a".to_vec(),
                row_id: 10,
            },
            EncodedRowEntry {
                key: b"a".to_vec(),
                row_id: 11,
            },
        ];
        let mut deletes = vec![
            EncodedRowEntry {
                key: b"a".to_vec(),
                row_id: 9,
            },
            EncodedRowEntry {
                key: b"c".to_vec(),
                row_id: 30,
            },
            EncodedRowEntry {
                key: b"c".to_vec(),
                row_id: 30,
            },
        ];

        normalize_unique_puts(&mut puts);
        normalize_unique_deletes(&mut deletes, &puts);

        assert_eq!(
            puts,
            vec![
                EncodedRowEntry {
                    key: b"a".to_vec(),
                    row_id: 11,
                },
                EncodedRowEntry {
                    key: b"b".to_vec(),
                    row_id: 20,
                },
            ]
        );
        assert_eq!(
            deletes,
            vec![EncodedRowEntry {
                key: b"c".to_vec(),
                row_id: 30,
            }]
        );
    }

    #[test]
    fn test_non_unique_sidecar_normalize_delete_wins_exact_key() {
        let mut sidecar = SecondaryIndexSidecar::NonUnique {
            encoder: BTreeKeyEncoder::new(vec![ValType::new(ValKind::U8, false)]),
            inserts: vec![b"b".to_vec(), b"a".to_vec(), b"b".to_vec()],
            deletes: vec![b"c".to_vec(), b"b".to_vec(), b"b".to_vec()],
        };

        sidecar.normalize();

        let SecondaryIndexSidecar::NonUnique {
            inserts, deletes, ..
        } = sidecar
        else {
            panic!("expected non-unique sidecar");
        };
        assert_eq!(inserts, vec![b"a".to_vec()]);
        assert_eq!(deletes, vec![b"b".to_vec(), b"c".to_vec()]);
    }
}
