use super::lifecycle::TableLifecycleState;
use crate::catalog::{IndexSpec, SilentWatermarkObject, TableColumnLayout, TableMetadata};
use crate::error::{
    ConfigError, DataIntegrityError, Error, ErrorKind, FatalError, InternalError, OperationError,
    Result,
};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::table_file::{ActiveRoot, MutableTableFile};
use crate::id::{BlockID, PageID, RowID, TrxID};
use crate::index::BTreeKeyEncoder;
use crate::index::disk_tree::{
    NonUniqueDiskTreeEncodedExact, UniqueDiskTreeEncodedDelete, UniqueDiskTreeEncodedPut,
};
use crate::index::{ColumnBlockIndex, ColumnDeleteDeltaPatch, ColumnLeafEntry};
use crate::log::redo::DDLRedo;
use crate::lwc::PersistedLwcBlock;
use crate::obs;
use crate::row::RowPage;
use crate::session::SessionPin;
use crate::table::{CheckpointCancelReason, Table, TableRedoReplayFloor, TableRuntimeLayout};
use crate::trx::sys::TransactionSystem;
#[cfg(test)]
use crate::trx::tests::discard_transaction_after_fatal_rollback;
use crate::value::{Val, ValKind, ValType};
use error_stack::Report;
use std::collections::BTreeSet;

#[cfg(test)]
pub(crate) use tests::test_hooks;

const CHECKPOINT_REQUIRES_IDLE_SESSION: &str = "checkpoint requires idle session";

struct TransitionRoutePublicationGuard<'a> {
    trx_sys: &'a TransactionSystem,
    armed: bool,
}

impl<'a> TransitionRoutePublicationGuard<'a> {
    #[inline]
    fn arm(trx_sys: &'a TransactionSystem) -> Self {
        Self {
            trx_sys,
            armed: true,
        }
    }

    #[inline]
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TransitionRoutePublicationGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.armed {
            // Once row pages enter TRANSITION, foreground hot writers cannot
            // safely continue on those pages. Failure before cold-route
            // publication must poison storage rather than leave route waiters
            // blocked forever.
            let _ = self.trx_sys.poison_storage(FatalError::CheckpointWrite);
        }
    }
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
    /// The table is already in its drop lifecycle and should not be checkpointed.
    TableDropping,
    /// The table runtime is absent or already dropped.
    TableNotFound,
}

impl CheckpointReadiness {
    #[inline]
    fn for_root(active_root: &ActiveRoot, min_active_sts: TrxID) -> Self {
        let effective_ts = active_root.effective_ts();
        if effective_ts < min_active_sts {
            Self::Ready
        } else {
            Self::Delayed {
                reason: CheckpointDelayReason {
                    effective_ts,
                    min_active_sts,
                },
            }
        }
    }
}

/// User-table checkpoint execution result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointOutcome {
    /// A checkpoint transaction was durably published.
    Published {
        /// Commit timestamp of the publishing checkpoint transaction.
        checkpoint_ts: TrxID,
        /// Whether publication used only a catalog silent replay watermark.
        ///
        /// `true` means no user-table root was published. `false` means the
        /// checkpoint published a user-table root.
        silent: bool,
    },
    /// No checkpoint work was published because the active root is still live.
    Delayed {
        /// Diagnostic details explaining why checkpoint waited.
        reason: CheckpointDelayReason,
    },
    /// No checkpoint work was published because checkpoint publication was cancelled.
    Cancelled {
        /// Diagnostic details explaining why checkpoint publication was cancelled.
        reason: CheckpointCancelReason,
    },
}

/// Diagnostic payload for normal checkpoint scheduling delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointDelayReason {
    /// Runtime post-publish root-observation boundary used by reclamation.
    pub effective_ts: TrxID,
    /// Current global minimum active snapshot timestamp used by GC.
    pub min_active_sts: TrxID,
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

struct ActiveSecondaryIndexSidecar {
    index_no: usize,
    key_cols: Box<[usize]>,
    sidecar: SecondaryIndexSidecar,
}

struct SecondaryCheckpointSidecar {
    indexes: Vec<ActiveSecondaryIndexSidecar>,
}

impl SecondaryCheckpointSidecar {
    fn new(metadata: &TableMetadata) -> Result<Self> {
        let indexes = metadata
            .idx
            .active_indexes()
            .map(|(index_no, index_spec)| {
                Ok(ActiveSecondaryIndexSidecar {
                    index_no,
                    key_cols: index_spec
                        .cols
                        .iter()
                        .map(|index_key| index_key.col_no as usize)
                        .collect(),
                    sidecar: SecondaryIndexSidecar::new(metadata, index_spec)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { indexes })
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.indexes.iter().all(|index| !index.sidecar.has_work())
    }

    fn add_data_row(
        &mut self,
        col_layout: &TableColumnLayout,
        page: &RowPage,
        row_idx: usize,
        row_id: RowID,
    ) -> Result<()> {
        // Data checkpoint feeds committed-visible transition rows here, once
        // per row selected for persistence.
        for active in &mut self.indexes {
            let key = active
                .key_cols
                .iter()
                .map(|col_idx| page.val(col_layout, row_idx, *col_idx))
                .collect();
            active.sidecar.add_data(key, row_id);
        }
        Ok(())
    }

    fn add_deleted_key_at(
        &mut self,
        sidecar_pos: usize,
        index_no: usize,
        row_id: RowID,
        key: Vec<Val>,
    ) -> Result<()> {
        let active = self.indexes.get_mut(sidecar_pos).ok_or_else(|| {
            Error::from(
                Report::new(InternalError::IndexKeyMissing)
                    .attach(format!("index_no={index_no}, sidecar_pos={sidecar_pos}")),
            )
        })?;
        if active.index_no != index_no {
            return Err(Report::new(InternalError::IndexKeyMissing)
                .attach(format!(
                    "secondary sidecar index mismatch: sidecar_pos={sidecar_pos}, expected_index_no={index_no}, actual_index_no={}",
                    active.index_no
                ))
                .into());
        }
        active.sidecar.add_delete(key, row_id);
        Ok(())
    }
}

/// Builds the durable secondary DiskTree key encoder for one index spec.
pub(crate) fn secondary_disk_tree_encoder(
    metadata: &TableMetadata,
    index_spec: &IndexSpec,
    append_row_id: bool,
) -> Result<BTreeKeyEncoder> {
    if index_spec.cols.is_empty() {
        return Err(invalid_index_spec("index has no key columns"));
    }
    let mut types = Vec::with_capacity(index_spec.cols.len() + usize::from(append_row_id));
    for key in &index_spec.cols {
        let col_no = key.col_no as usize;
        let ty = metadata
            .col
            .col_types()
            .get(col_no)
            .copied()
            .ok_or_else(|| invalid_index_spec(format!("index column {col_no} is out of range")))?;
        types.push(ty);
    }
    if append_row_id {
        types.push(ValType::new(ValKind::U64, false));
    }
    Ok(BTreeKeyEncoder::new(types))
}

#[inline]
fn invalid_index_spec(message: impl Into<String>) -> Error {
    Report::new(ConfigError::InvalidIndexSpec)
        .attach(message.into())
        .into()
}

#[inline]
fn checkpoint_outcome_from_readiness(readiness: CheckpointReadiness) -> Option<CheckpointOutcome> {
    match readiness {
        CheckpointReadiness::Ready => None,
        CheckpointReadiness::Delayed { reason } => Some(CheckpointOutcome::Delayed { reason }),
        CheckpointReadiness::TableDropping => Some(CheckpointOutcome::Cancelled {
            reason: CheckpointCancelReason::TableDropping,
        }),
        CheckpointReadiness::TableNotFound => Some(CheckpointOutcome::Cancelled {
            reason: CheckpointCancelReason::TableDropped,
        }),
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

fn invalid_reachable_block(root_ts: TrxID, block_id: BlockID, message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidRootInvariant)
        .attach(format!(
            "invalid table-root reachable block: root_ts={root_ts}, block_id={block_id}, {}",
            message.into()
        ))
        .into()
}

fn validate_reachable_block(root: &ActiveRoot, block_id: BlockID) -> Result<()> {
    let idx = usize::from(block_id);
    if idx >= root.alloc_map.len() {
        return Err(invalid_reachable_block(
            root.root_ts,
            block_id,
            format!("alloc_map_len={}", root.alloc_map.len()),
        ));
    }
    if !root.alloc_map.is_allocated(idx) {
        return Err(invalid_reachable_block(
            root.root_ts,
            block_id,
            "allocation bit is not set",
        ));
    }
    Ok(())
}

impl Table {
    async fn collect_root_reachable_blocks(
        &self,
        root: &ActiveRoot,
        layout: &TableRuntimeLayout,
        reachable: &mut BTreeSet<BlockID>,
    ) -> Result<()> {
        if root.secondary_index_roots.len() != layout.index_slot_count() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "secondary root count mismatch: root_count={}, index_slot_count={}",
                    root.secondary_index_roots.len(),
                    layout.index_slot_count()
                ))
                .into());
        }

        let mut root_reachable = BTreeSet::new();
        root_reachable.insert(SUPER_BLOCK_ID);
        root_reachable.insert(root.meta_block_id);

        if root.column_block_index_root != SUPER_BLOCK_ID {
            let disk_pool = self.disk_pool();
            let disk_pool_guard = disk_pool.pool_guard();
            let column_index = ColumnBlockIndex::new(
                root.column_block_index_root,
                root.pivot_row_id,
                self.file().file_kind(),
                self.file().sparse_file(),
                disk_pool,
                &disk_pool_guard,
            );
            column_index
                .collect_reachable_blocks(&mut root_reachable)
                .await?;
        }

        for (index_no, index_slot) in layout.secondary_indexes().iter().enumerate() {
            let root_block_id = root.secondary_index_roots[index_no];
            let Some(index) = index_slot.as_ref() else {
                if root_block_id != SUPER_BLOCK_ID {
                    return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                        .attach(format!(
                            "inactive secondary index slot has root: index_no={index_no}, root={root_block_id}"
                        ))
                        .into());
                }
                continue;
            };
            if root_block_id == SUPER_BLOCK_ID {
                continue;
            }
            let runtime = index.disk_runtime();
            let disk_pool_guard = runtime.disk_pool_guard();
            runtime
                .collect_reachable_blocks(root_block_id, &disk_pool_guard, &mut root_reachable)
                .await?;
        }

        for block_id in root_reachable {
            validate_reachable_block(root, block_id)?;
            reachable.insert(block_id);
        }
        Ok(())
    }

    async fn rebuild_reachable_alloc_map(
        &self,
        mutable_file: &mut MutableTableFile,
        layout: &TableRuntimeLayout,
    ) -> Result<usize> {
        let mut reachable = BTreeSet::new();
        self.collect_root_reachable_blocks(
            self.file().active_root_unchecked(),
            layout,
            &mut reachable,
        )
        .await?;
        self.collect_root_reachable_blocks(mutable_file.root(), layout, &mut reachable)
            .await?;
        mutable_file.rebuild_alloc_map_from_reachable(&reachable)
    }

    async fn apply_deletion_checkpoint(
        &self,
        mutable_file: &mut MutableTableFile,
        metadata: &TableMetadata,
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
        if column_block_index_root == SUPER_BLOCK_ID || pivot_row_id == RowID::new(0) {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "eligible delete markers require column index: column_block_index_root={column_block_index_root}, pivot_row_id={pivot_row_id}"
                ))
                .into());
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
                    return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                        .attach(format!(
                            "eligible delete marker cannot be located: row_id={row_id}"
                        ))
                        .into());
                };
                cached_entry = Some(entry);
                entry
            };
            let delta_u64 = row_id.checked_sub(entry.start_row_id).ok_or_else(|| {
                Error::from(
                    Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                        "delete marker precedes block start: row_id={row_id}, start_row_id={}",
                        entry.start_row_id
                    )),
                )
            })?;
            if delta_u64 > u32::MAX as u64 {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach(format!(
                        "delete marker delta exceeds u32: delta={delta_u64}, row_id={row_id}, start_row_id={}",
                        entry.start_row_id
                    ))
                    .into());
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
            return Err(Report::new(InternalError::ColumnIndexRewriteMiss)
                .attach("delete marker selection produced no patch groups")
                .into());
        }

        // Step 4: load authoritative persisted deltas and merge pending row-id deltas.
        let mut patch_storage: Vec<(RowID, Vec<u32>)> = Vec::new();
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
                metadata,
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
        layout: &TableRuntimeLayout,
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
            if test_hooks::test_force_secondary_sidecar_error_enabled() {
                return Err(Report::new(InternalError::InjectedTestFailure).into());
            }
        }

        let metadata = layout.metadata();
        if mutable_file.secondary_index_roots().len() != metadata.idx.index_slot_count() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "secondary root count mismatch: root_count={}, index_slot_count={}",
                    mutable_file.secondary_index_roots().len(),
                    metadata.idx.index_slot_count()
                ))
                .into());
        }

        let disk_pool = self.disk_pool();
        let disk_pool_guard = disk_pool.pool_guard();
        for active in &mut sidecar.indexes {
            let index_no = active.index_no;
            if metadata.idx.index_spec(index_no).is_none() {
                return Err(Report::new(InternalError::IndexKeyMissing)
                    .attach(format!("index_no={index_no}"))
                    .into());
            }
            let index_sidecar = &mut active.sidecar;
            index_sidecar.normalize();
            if !index_sidecar.has_work() {
                continue;
            }
            let old_root = mutable_file.secondary_index_root(index_no)?;
            let runtime = layout.secondary_index(index_no)?.disk_runtime();
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
        metadata: &TableMetadata,
        secondary_sidecar: &mut SecondaryCheckpointSidecar,
    ) -> Result<()> {
        if secondary_sidecar.indexes.is_empty() || delete_deltas.is_empty() {
            return Ok(());
        }

        // ColumnBlockIndex supplies the authoritative row-id ordering for this
        // persisted block, which maps deletion deltas back to row indexes.
        if row_ids.len() != entry.row_count() as usize
            || row_ids.windows(2).any(|window| window[0] >= window[1])
        {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "invalid persisted row id set: block_id={}, row_count={}, row_id_count={}",
                    entry.block_id(),
                    entry.row_count(),
                    row_ids.len()
                ))
                .into());
        }
        let dense_row_ids = row_ids.len() == entry.row_id_span() as usize
            && row_ids
                .iter()
                .enumerate()
                .all(|(idx, row_id)| *row_id == entry.start_row_id + idx as u64);

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
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "LWC block metadata mismatch: block_id={}, block_row_count={}, expected_row_count={}, block_fingerprint={}, expected_fingerprint={}",
                    entry.block_id(),
                    block.row_count(),
                    row_ids.len(),
                    block.row_shape_fingerprint(),
                    entry.row_shape_fingerprint()
                ))
                .into());
        }

        let mut sparse_row_idx = 0usize;
        for delta in delete_deltas {
            let row_id = entry
                .start_row_id
                .checked_add(u64::from(*delta))
                .ok_or_else(|| {
                    Error::from(
                        Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                            "delete delta overflows row id: start_row_id={}, delta={delta}",
                            entry.start_row_id
                        )),
                    )
                })?;
            let row_idx = if dense_row_ids {
                usize::try_from(*delta).map_err(|_| {
                    Error::from(
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach(format!("delete delta does not fit usize: delta={delta}")),
                    )
                })?
            } else {
                while row_ids
                    .get(sparse_row_idx)
                    .is_some_and(|current| *current < row_id)
                {
                    sparse_row_idx += 1;
                }
                if row_ids.get(sparse_row_idx) != Some(&row_id) {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!(
                            "delete delta does not map to row id: row_id={row_id}, delta={delta}"
                        ))
                        .into());
                }
                sparse_row_idx
            };
            if row_idx >= row_ids.len() {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "delete delta row index out of bounds: row_idx={row_idx}, row_count={}",
                        row_ids.len()
                    ))
                    .into());
            }
            for sidecar_pos in 0..secondary_sidecar.indexes.len() {
                let (index_no, key) = {
                    let active = &secondary_sidecar.indexes[sidecar_pos];
                    let key = block.decode_row_values(
                        metadata.col.as_ref(),
                        row_idx,
                        active.key_cols.as_ref(),
                    )?;
                    (active.index_no, key)
                };
                secondary_sidecar.add_deleted_key_at(sidecar_pos, index_no, row_id, key)?;
            }
        }
        Ok(())
    }
}

impl Table {
    #[inline]
    fn try_checkpoint_readiness_for_session(
        &self,
        session: &SessionPin,
    ) -> Result<CheckpointReadiness> {
        let active_root = self.file().active_root_unchecked();
        Ok(CheckpointReadiness::for_root(
            active_root,
            session.engine.trx_sys.calc_min_active_sts_for_gc(),
        ))
    }
}

impl Table {
    /// Mark hot row pages frozen up to the requested row budget.
    pub(crate) async fn freeze(&self, session: SessionPin, max_rows: usize) -> Result<usize> {
        let guards = session.pool_guards();
        let mut rows = 0usize;
        self.mem_scan(&guards, |page_guard| {
            let (ctx, page) = page_guard.ctx_and_page();
            let vmap = ctx.row_ver().unwrap();
            rows += page.header.approx_non_deleted();
            if vmap.is_frozen() {
                return rows < max_rows;
            }
            vmap.set_frozen();
            rows < max_rows
        })
        .await?;
        Ok(rows)
    }

    /// Return whether this table is ready for a checkpoint in the session.
    pub(crate) fn checkpoint_readiness(&self, session: &SessionPin) -> Result<CheckpointReadiness> {
        match self.lifecycle.state() {
            TableLifecycleState::Live => self.try_checkpoint_readiness_for_session(session),
            TableLifecycleState::Dropping => Ok(CheckpointReadiness::TableDropping),
            TableLifecycleState::Dropped => Ok(CheckpointReadiness::TableNotFound),
        }
    }

    /// Execute one user-table checkpoint attempt.
    pub(crate) async fn checkpoint(&self, session: SessionPin) -> Result<CheckpointOutcome> {
        let table_id = self.table_id();
        self.checkpoint_inner(session)
            .await
            .inspect(|outcome| match outcome {
                CheckpointOutcome::Published {
                    checkpoint_ts,
                    silent,
                } => obs::info!(
                    "event=checkpoint_publish component=table table_id={} action=publish result=ok checkpoint_ts={} silent={}",
                    table_id,
                    checkpoint_ts,
                    silent
                ),
                CheckpointOutcome::Delayed { reason } => obs::warn!(
                    "event=checkpoint_publish component=table table_id={} action=delay result=delayed effective_ts={} min_active_sts={}",
                    table_id,
                    reason.effective_ts,
                    reason.min_active_sts
                ),
                CheckpointOutcome::Cancelled { reason } => obs::warn!(
                    "event=checkpoint_publish component=table table_id={} action=cancel result=cancelled reason={:?}",
                    table_id,
                    reason
                ),
            })
            .inspect_err(|err| {
                if err.kind() == ErrorKind::Fatal {
                    obs::error!(
                        "event=checkpoint_publish component=table table_id={} action=publish result=error error={}",
                        table_id,
                        err
                    );
                }
            })
    }

    async fn checkpoint_inner(&self, session: SessionPin) -> Result<CheckpointOutcome> {
        if session.in_trx(CHECKPOINT_REQUIRES_IDLE_SESSION)? {
            return Err(Report::new(OperationError::NotSupported)
                .attach(CHECKPOINT_REQUIRES_IDLE_SESSION)
                .into());
        }

        if let Some(outcome) =
            checkpoint_outcome_from_readiness(self.checkpoint_readiness(&session)?)
        {
            return Ok(outcome);
        }

        let table_file = self.file();
        let disk_pool = self.disk_pool();
        let trx_sys = session.engine.trx_sys.clone();
        let table_writes = session.engine.table_fs.background_writes().clone();
        let pool_guards = session.pool_guards();
        #[cfg(test)]
        test_hooks::run_test_checkpoint_after_readiness_hook().await;
        let root_mutation_lease = match self.try_begin_checkpoint_root_mutation() {
            Ok(lease) => lease,
            Err(reason) => return Ok(CheckpointOutcome::Cancelled { reason }),
        };
        if let Some(outcome) =
            checkpoint_outcome_from_readiness(self.checkpoint_readiness(&session)?)
        {
            return Ok(outcome);
        }
        let layout = self.layout_snapshot();
        let metadata = layout.metadata();

        // Step 1: claim one mutable root snapshot and initialize checkpoint
        // boundaries. This is checkpoint-internal current-root access after the
        // post-lease liveness check above.
        let mut mutable_file = MutableTableFile::fork(table_file, &table_writes, disk_pool.clone());
        let pivot_row_id = mutable_file.root().pivot_row_id;
        let mut secondary_sidecar = SecondaryCheckpointSidecar::new(metadata)?;

        // Step 2: collect frozen pages and refresh checkpoint cutoff after any
        // stabilization wait. The post-lease readiness check is enough for root
        // liveness because the GC horizon used by the check only moves forward.
        let (frozen_pages, next_heap_redo_start_ts) =
            self.collect_frozen_pages(&pool_guards).await?;
        if !frozen_pages.is_empty() {
            self.wait_for_frozen_pages_stable(&pool_guards, &trx_sys, &frozen_pages)
                .await?;
        }
        let cutoff_ts = trx_sys.calc_min_active_sts_for_gc();

        // Step 3: open a checkpoint transaction, then move frozen pages into
        // transition state under the refreshed cutoff timestamp.
        let mut trx = session.begin_trx("checkpoint table")?;
        let checkpoint_ts = trx.sts();
        #[cfg(test)]
        test_hooks::run_test_checkpoint_after_trx_start_hook().await;
        let mut transition_publication_guard = None;
        if !frozen_pages.is_empty() {
            let transition_pages = self
                .load_frozen_pages_for_transition(&pool_guards, &frozen_pages)
                .await?;
            let guard = TransitionRoutePublicationGuard::arm(&trx_sys);
            self.set_loaded_frozen_pages_to_transition(&transition_pages, cutoff_ts);
            drop(transition_pages);
            transition_publication_guard = Some(guard);
        }

        // Step 4: build LWC blocks from transition pages using the cutoff
        // snapshot. The sidecar callback observes the same committed-visible
        // rows accepted by the LWC builder, independent of later block splits.
        let new_pivot_row_id = frozen_pages
            .last()
            .map(|page| page.end_row_id)
            .unwrap_or(pivot_row_id);
        let collect_visible_row = if secondary_sidecar.indexes.is_empty() {
            None
        } else {
            let col_layout = metadata.col.as_ref();
            Some(|page: &RowPage, row_idx: usize, row_id: RowID| {
                secondary_sidecar.add_data_row(col_layout, page, row_idx, row_id)
            })
        };
        let (mut lwc_blocks, heap_redo_start_ts) = match self
            .build_lwc_blocks(
                metadata,
                &pool_guards,
                cutoff_ts,
                &frozen_pages,
                collect_visible_row,
            )
            .await
        {
            // A heartbeat checkpoint still advances the heap replay floor:
            // its transaction STS comes from the global timestamp sequence.
            Ok(pages) => (pages, next_heap_redo_start_ts.unwrap_or(checkpoint_ts)),
            Err(err) => {
                trx.rollback().await?;
                return Err(err);
            }
        };

        if let Some(last) = lwc_blocks.last_mut()
            && last.shape.end_row_id() < new_pivot_row_id
        {
            last.shape.set_end_row_id(new_pivot_row_id)?;
        }

        let gc_pages: Vec<PageID> = frozen_pages.iter().map(|page| page.page_id).collect();
        trx.extend_gc_row_pages(gc_pages)?;

        // Step 5: emit one checkpoint redo marker for recovery.
        let old = trx.set_ddl_redo(DDLRedo::DataCheckpoint {
            table_id: self.table_id(),
            pivor_row_id: new_pivot_row_id,
            sts: checkpoint_ts,
        })?;
        debug_assert!(old.is_none());

        // Step 6: apply checkpoint changes to the already-checked mutable root.
        if !lwc_blocks.is_empty() {
            if let Err(err) = mutable_file
                .apply_lwc_blocks(lwc_blocks, heap_redo_start_ts, checkpoint_ts, disk_pool)
                .await
            {
                trx.rollback().await?;
                return Err(err);
            }
        } else if let Err(err) =
            mutable_file.apply_checkpoint_metadata(new_pivot_row_id, heap_redo_start_ts)
        {
            trx.rollback().await?;
            return Err(err);
        }

        // Step 7: merge committed cold-row deletions into column index payloads,
        // collect matching secondary-index delete sidecar work, and publish the
        // durable cold-delete replay watermark.
        if let Err(err) = self
            .apply_deletion_checkpoint(
                &mut mutable_file,
                metadata,
                &mut secondary_sidecar,
                cutoff_ts,
                checkpoint_ts,
            )
            .await
        {
            trx.rollback().await?;
            return Err(err);
        }

        // Step 8: apply accumulated secondary-index sidecar work to DiskTree
        // roots on the same mutable file fork. Root publication remains atomic
        // with the table checkpoint commit below.
        if let Err(err) = self
            .apply_secondary_checkpoint_sidecar(
                &mut mutable_file,
                &layout,
                &mut secondary_sidecar,
                checkpoint_ts,
            )
            .await
        {
            trx.rollback().await?;
            return Err(err);
        }

        // Step 9: after all checkpoint CoW writes are represented in the
        // mutable root, rebuild its allocation map from the current active root
        // and the mutable root that will be published.
        if let Err(err) = self
            .rebuild_reachable_alloc_map(&mut mutable_file, &layout)
            .await
        {
            trx.rollback().await?;
            return Err(err);
        }

        if let Some(requested_floor) =
            silent_watermark_floor(table_file.active_root_unchecked(), mutable_file.root())
        {
            drop(mutable_file);
            let old = trx.set_ddl_redo(DDLRedo::TableReplaySilentWatermark {
                table_id: self.table_id(),
            })?;
            debug_assert!(matches!(
                old.as_deref(),
                Some(DDLRedo::DataCheckpoint { .. })
            ));
            let watermark = SilentWatermarkObject {
                table_id: self.table_id(),
                heap_redo_start_ts: requested_floor.heap_redo_start_ts,
                deletion_cutoff_ts: requested_floor.deletion_cutoff_ts,
            };
            if let Err(err) = trx
                .exec(async |stmt| {
                    session
                        .engine
                        .catalog()
                        .storage
                        .table_replay_silent_watermarks()
                        .upsert(stmt, &watermark)
                        .await?;
                    Ok(())
                })
                .await
            {
                trx.rollback().await?;
                return Err(err);
            }
            let checkpoint_ts = trx.commit().await?;
            drop(root_mutation_lease);
            return Ok(CheckpointOutcome::Published {
                checkpoint_ts,
                silent: true,
            });
        }

        // Step 10: enter the no-cancel publication section, publish a new
        // table-file root, and then commit the checkpoint transaction. This
        // happens only when table-file state beyond replay-bound fields
        // changed. Replay-bound-only checkpoints are published as catalog
        // silent watermark rows above.
        let publish_lease = match self.try_begin_checkpoint_publish() {
            Ok(lease) => lease,
            Err(reason) => {
                trx.rollback().await?;
                return Ok(CheckpointOutcome::Cancelled { reason });
            }
        };
        let published_root = mutable_file.root();
        let published_pivot_row_id = published_root.pivot_row_id;
        let published_column_root = published_root.column_block_index_root;
        match trx_sys
            .publish_table_file_root(mutable_file, checkpoint_ts, false)
            .await
        {
            Ok(res) => res,
            Err(err) if err.kind() == ErrorKind::Io => {
                let _ = trx.rollback().await;
                let poison = trx_sys.poison_storage(FatalError::CheckpointWrite);
                return Err(poison.into());
            }
            Err(err) => {
                trx.rollback().await?;
                return Err(err);
            }
        };
        self.mem
            .blk_idx()
            .update_column_root(published_pivot_row_id, published_column_root)
            .await;
        if let Some(guard) = transition_publication_guard.as_mut() {
            guard.disarm();
        }
        #[cfg(test)]
        if test_hooks::test_force_post_publish_checkpoint_error_enabled() {
            let poison = trx_sys.poison_storage(FatalError::CheckpointWrite);
            discard_transaction_after_fatal_rollback(&mut trx);
            return Err(poison.into());
        }

        if trx.commit().await.is_err() {
            let poison = trx_sys.poison_storage(FatalError::CheckpointWrite);
            return Err(poison.into());
        }
        drop(publish_lease);
        drop(root_mutation_lease);
        Ok(CheckpointOutcome::Published {
            checkpoint_ts,
            silent: false,
        })
    }
}

#[inline]
fn silent_watermark_floor(
    active_root: &ActiveRoot,
    mutable_root: &ActiveRoot,
) -> Option<TableRedoReplayFloor> {
    // Any checkpointed table-file data/state change must update one of these
    // logical root fields before this decision:
    // - pivot_row_id for persisted row coverage,
    // - column_block_index_root for LWC inserts or cold-delete rewrites,
    // - secondary_index_roots for secondary DiskTree checkpoint work,
    // - metadata for table/index shape changes,
    // - alloc_map for CoW reachability/allocation changes.
    // Replay bounds are intentionally excluded because those are the catalog
    // watermark payload. `meta_block_id` is also excluded because the normal
    // table checkpoint path reserves the publish meta block only after this
    // silent-vs-root-publication decision.
    let table_file_work = active_root.pivot_row_id != mutable_root.pivot_row_id
        || active_root.column_block_index_root != mutable_root.column_block_index_root
        || active_root.secondary_index_roots != mutable_root.secondary_index_roots
        || active_root.metadata != mutable_root.metadata
        || active_root.alloc_map != mutable_root.alloc_map;
    if table_file_work {
        return None;
    }
    // Root fields can be unchanged while checkpoint STS or delete cutoff
    // progress advances replay bounds; publish that through a catalog watermark.
    Some(TableRedoReplayFloor {
        heap_redo_start_ts: mutable_root.heap_redo_start_ts,
        deletion_cutoff_ts: mutable_root.deletion_cutoff_ts,
    })
}

#[cfg(test)]
mod tests {
    pub(crate) mod test_hooks {
        use std::cell::{Cell, RefCell};
        use std::future::Future;
        use std::pin::Pin;

        type TableHook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + 'static>> + 'static>;

        thread_local! {
            static TEST_FORCE_SECONDARY_SIDECAR_ERROR: Cell<bool> = const { Cell::new(false) };
            static TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR: Cell<bool> = const { Cell::new(false) };
            static TEST_CHECKPOINT_AFTER_READINESS_HOOK: RefCell<Option<TableHook>> =
                const { RefCell::new(None) };
            static TEST_CHECKPOINT_AFTER_TRX_START_HOOK: RefCell<Option<TableHook>> =
                const { RefCell::new(None) };
        }

        pub(crate) fn set_test_force_secondary_sidecar_error(enabled: bool) {
            TEST_FORCE_SECONDARY_SIDECAR_ERROR.with(|flag| flag.set(enabled));
        }

        pub(crate) fn test_force_secondary_sidecar_error_enabled() -> bool {
            TEST_FORCE_SECONDARY_SIDECAR_ERROR.with(|flag| flag.get())
        }

        pub(crate) fn set_test_force_post_publish_checkpoint_error(enabled: bool) {
            TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR.with(|flag| flag.set(enabled));
        }

        pub(crate) struct ForcePostPublishCheckpointErrorGuard;

        impl ForcePostPublishCheckpointErrorGuard {
            pub(crate) fn new() -> Self {
                set_test_force_post_publish_checkpoint_error(true);
                Self
            }
        }

        impl Drop for ForcePostPublishCheckpointErrorGuard {
            fn drop(&mut self) {
                set_test_force_post_publish_checkpoint_error(false);
            }
        }

        pub(crate) fn test_force_post_publish_checkpoint_error_enabled() -> bool {
            TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR.with(|flag| flag.get())
        }

        pub(crate) fn set_test_checkpoint_after_readiness_hook<F, Fut>(hook: F)
        where
            F: FnOnce() -> Fut + 'static,
            Fut: Future<Output = ()> + 'static,
        {
            TEST_CHECKPOINT_AFTER_READINESS_HOOK.with(|slot| {
                let old = slot
                    .borrow_mut()
                    .replace(Box::new(move || Box::pin(hook())));
                assert!(old.is_none(), "checkpoint readiness hook already installed");
            });
        }

        pub(crate) fn set_test_checkpoint_after_trx_start_hook<F, Fut>(hook: F)
        where
            F: FnOnce() -> Fut + 'static,
            Fut: Future<Output = ()> + 'static,
        {
            TEST_CHECKPOINT_AFTER_TRX_START_HOOK.with(|slot| {
                let old = slot
                    .borrow_mut()
                    .replace(Box::new(move || Box::pin(hook())));
                assert!(
                    old.is_none(),
                    "checkpoint transaction-start hook already installed"
                );
            });
        }

        pub(crate) async fn run_test_checkpoint_after_readiness_hook() {
            let hook = TEST_CHECKPOINT_AFTER_READINESS_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook().await;
            }
        }

        pub(crate) async fn run_test_checkpoint_after_trx_start_hook() {
            let hook = TEST_CHECKPOINT_AFTER_TRX_START_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook().await;
            }
        }
    }

    use super::*;
    use crate::buffer::BufferPool;
    use crate::file::cow_file::tests::old_root_drop_count;
    use crate::index::{RowLocation, UniqueIndex};
    use crate::io::install_storage_backend_test_hook;
    use crate::row::ops::{SelectKey, SelectMvcc};
    use crate::session::{Session, tests::SessionTestExt};
    use crate::table::DeleteMarker;
    use crate::table::persistence::test_hooks::{
        ForcePostPublishCheckpointErrorGuard, set_test_checkpoint_after_readiness_hook,
        set_test_checkpoint_after_trx_start_hook, set_test_force_secondary_sidecar_error,
    };
    use crate::table::test_hooks::set_test_force_lwc_build_error;
    use crate::table::tests::*;
    use crate::trx::Transaction;
    use crate::trx::stmt::tests as stmt_tests;
    use crate::trx::ver_map::RowPageState;
    use futures::FutureExt;
    use smol::Timer;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_unique_sidecar_normalize_keeps_latest_put_and_suppresses_delete() {
        let mut puts = vec![
            EncodedRowEntry {
                key: b"b".to_vec(),
                row_id: RowID::new(20),
            },
            EncodedRowEntry {
                key: b"a".to_vec(),
                row_id: RowID::new(10),
            },
            EncodedRowEntry {
                key: b"a".to_vec(),
                row_id: RowID::new(11),
            },
        ];
        let mut deletes = vec![
            EncodedRowEntry {
                key: b"a".to_vec(),
                row_id: RowID::new(9),
            },
            EncodedRowEntry {
                key: b"c".to_vec(),
                row_id: RowID::new(30),
            },
            EncodedRowEntry {
                key: b"c".to_vec(),
                row_id: RowID::new(30),
            },
        ];

        normalize_unique_puts(&mut puts);
        normalize_unique_deletes(&mut deletes, &puts);

        assert_eq!(
            puts,
            vec![
                EncodedRowEntry {
                    key: b"a".to_vec(),
                    row_id: RowID::new(11),
                },
                EncodedRowEntry {
                    key: b"b".to_vec(),
                    row_id: RowID::new(20),
                },
            ]
        );
        assert_eq!(
            deletes,
            vec![EncodedRowEntry {
                key: b"c".to_vec(),
                row_id: RowID::new(30),
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

    #[test]
    fn test_checkpoint_persists_committed_cold_delete_markers() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let key = single_key(6i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            expect_delete_committed(table_id, &mut session, &key).await;
            let marker = table.deletion_buffer().get(row_id).unwrap();
            let marker_ts = delete_marker_ts(marker);
            wait_gc_cutoff_after(&session, marker_ts).await;
            let pool_guards = session.pool_guards();
            let snapshot_before = column_block_index_snapshot(&engine, table_id);
            let index_before = snapshot_before.index(pool_guards.disk_guard());
            let entry_before = index_before
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("persisted entry should exist before delete checkpoint");

            checkpoint_published(table_id, &mut session).await;

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let active_root = &snapshot.active_root;
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("persisted entry should exist");
            assert!(active_root.deletion_cutoff_ts > marker_ts);
            assert_eq!(entry.block_id(), entry_before.block_id());
            assert_eq!(entry.end_row_id(), entry_before.end_row_id());
            assert_eq!(entry.row_id_span(), entry_before.row_id_span());
            assert_eq!(entry.row_count(), entry_before.row_count());
            let deltas = index.load_delete_deltas(&entry).await.unwrap();
            let expected_delta = (row_id - entry.start_row_id) as u32;
            assert!(deltas.contains(&expected_delta));
        });
    }

    #[test]
    fn test_checkpoint_publishes_unique_secondary_disk_tree_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "name").await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let active_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_ne!(active_root.secondary_index_roots[0], SUPER_BLOCK_ID);
            let reader = session.begin_trx().unwrap();
            for key_value in 0..3 {
                let key = single_key(key_value);
                let row_id = assert_row_in_lwc(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &key,
                    reader.sts(),
                )
                .await;
                assert_eq!(
                    unique_disk_tree_lookup(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &key
                    )
                    .await,
                    Some(row_id)
                );
            }
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_trx_read_proof_root_snapshot_captures_active_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            for id in 0..8 {
                trx = expect_trx_insert(
                    table_id,
                    trx,
                    vec![Val::from(id), Val::from(format!("v{id}").as_str())],
                )
                .await;
            }
            trx.commit().await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                let proof = rt.read_proof();
                let snapshot = table_for_internal_assertion(&engine, table_id)
                    .root_snapshot(&proof)
                    .unwrap();
                let _effects_addr = effects as *mut _;
                table_for_internal_assertion(&engine, table_id).with_active_root(
                    &proof,
                    |active_root| {
                        assert_eq!(snapshot.root_ts(), active_root.root_ts);
                        assert_eq!(snapshot.pivot_row_id(), active_root.pivot_row_id);
                        assert_eq!(
                            snapshot.column_block_index_root(),
                            active_root.column_block_index_root
                        );
                        assert_eq!(
                            snapshot.deletion_cutoff_ts(),
                            active_root.deletion_cutoff_ts
                        );
                        assert_eq!(
                            snapshot.secondary_index_root(0).unwrap(),
                            active_root.secondary_index_roots[0]
                        );
                        assert_eq!(
                            snapshot.root_is_visible_to(rt.sts()),
                            active_root.effective_ts() < rt.sts()
                        );
                    },
                );
                Ok(())
            })
            .await
            .unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_checkpoint_publishes_non_unique_secondary_disk_tree_entries_across_lwc_splits() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "split-name-".repeat(120);
            let row_count = 80;
            insert_rows(table_id, &mut session, 0, row_count, &name).await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let name_key = name_key(&name);
            let table = table_for_internal_assertion(&engine, table_id);
            let row_ids =
                non_unique_disk_tree_prefix_scan(&table, &session.pool_guards(), &name_key).await;
            assert_eq!(row_ids.len(), row_count as usize);

            let first_key = single_key(0i32);
            let last_key = single_key(row_count - 1);
            let first_row_id = unique_disk_tree_lookup(&table, &session.pool_guards(), &first_key)
                .await
                .unwrap();
            let last_row_id = unique_disk_tree_lookup(&table, &session.pool_guards(), &last_key)
                .await
                .unwrap();
            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let column_index = snapshot.index(pool_guards.disk_guard());
            let first_entry = column_index
                .locate_block(first_row_id)
                .await
                .unwrap()
                .unwrap();
            let last_entry = column_index
                .locate_block(last_row_id)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(first_entry.block_id(), last_entry.block_id());
        });
    }

    #[test]
    fn test_deletion_checkpoint_updates_secondary_disk_tree_roots() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 2, "same-name").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let delete_key = single_key(0i32);
            let keep_key = single_key(1i32);
            let deleted_row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &delete_key,
            )
            .await
            .unwrap();
            let kept_row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &keep_key,
            )
            .await
            .unwrap();

            expect_delete_committed(table_id, &mut session, &delete_key).await;
            let marker_ts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(deleted_row_id)
                    .unwrap(),
            );
            wait_gc_cutoff_after(&session, marker_ts).await;
            checkpoint_published(table_id, &mut session).await;

            assert_eq!(
                unique_disk_tree_lookup(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &delete_key
                )
                .await,
                None
            );
            assert_eq!(
                unique_disk_tree_lookup(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &keep_key
                )
                .await,
                Some(kept_row_id)
            );
            let exact_rows = non_unique_disk_tree_prefix_scan(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &name_key("same-name"),
            )
            .await;
            assert_eq!(exact_rows, vec![kept_row_id]);
        });
    }

    #[test]
    fn test_unique_checkpoint_overlap_keeps_new_disk_tree_owner() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let old_row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from("old")],
            )
            .await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let key = single_key(1i32);
            expect_delete_committed(table_id, &mut session, &key).await;
            let delete_ts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(old_row_id)
                    .unwrap(),
            );
            let new_row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from("new")],
            )
            .await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let readiness_ts = delete_ts.max(
                table_for_internal_assertion(&engine, table_id)
                    .file()
                    .active_root_unchecked()
                    .root_ts,
            );
            wait_gc_cutoff_after(&session, readiness_ts).await;
            checkpoint_published(table_id, &mut session).await;

            assert_eq!(
                unique_disk_tree_lookup(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &key
                )
                .await,
                Some(new_row_id)
            );
        });
    }

    #[test]
    fn test_secondary_sidecar_failure_keeps_checkpoint_root_atomic() {
        struct ResetSidecarHook;

        impl Drop for ResetSidecarHook {
            fn drop(&mut self) {
                set_test_force_secondary_sidecar_error(false);
            }
        }

        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 2, "name").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let key = single_key(0i32);
            let row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
            )
            .await
            .unwrap();
            expect_delete_committed(table_id, &mut session, &key).await;
            let marker_ts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(row_id)
                    .unwrap(),
            );
            wait_gc_cutoff_after(&session, marker_ts).await;
            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();

            set_test_force_secondary_sidecar_error(true);
            let _reset = ResetSidecarHook;
            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::InjectedTestFailure)
            );

            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
            assert_eq!(
                root_after.column_block_index_root,
                root_before.column_block_index_root
            );
            assert_eq!(
                root_after.secondary_index_roots,
                root_before.secondary_index_roots
            );
        });
    }

    #[test]
    fn test_checkpoint_all_deleted_row_page_advances_without_column_index() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            delete_key_range_and_wait_gc_cutoff(table_id, &mut session, 0, 10).await;

            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert!(root_after.pivot_row_id > root_before.pivot_row_id);
            assert_eq!(root_after.column_block_index_root, SUPER_BLOCK_ID);
            assert!(root_after.deletion_cutoff_ts > root_before.deletion_cutoff_ts);
            for i in 0..10 {
                expect_select_not_found_committed(table_id, &mut session, &single_key(i)).await;
            }
        });
    }

    #[test]
    fn test_checkpoint_transition_delete_marker_waits_for_next_cutoff_range() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;

            let key = single_key(0i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let index = bound_unique_index_no(&table, key.index_no);
            let (row_id, _) = index
                .lookup(session.pool_guards().index_guard(), &key.vals, reader.sts())
                .await
                .unwrap()
                .expect("row should exist before delete");
            assert!(matches!(
                table
                    .find_row(&session.pool_guards(), row_id)
                    .await
                    .unwrap(),
                RowLocation::RowPage(_)
            ));
            reader.commit().await.unwrap();

            let mut hold_session = engine.new_session().unwrap();
            let hold_trx = hold_session.begin_trx().unwrap();
            let hold_sts = hold_trx.sts();

            let mut writer_session = engine.new_session().unwrap();
            expect_delete_committed(table_id, &mut writer_session, &key).await;
            assert!(table.deletion_buffer().get(row_id).is_none());

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(table_id, &mut checkpoint_session).await;

            let marker = table.deletion_buffer().get(row_id).unwrap();
            let delete_cts = delete_marker_ts(marker);
            assert!(delete_cts >= hold_sts);

            let snapshot_after_first = column_block_index_snapshot(&engine, table_id);
            let root_after_first = &snapshot_after_first.active_root;
            let pool_guards = session.pool_guards();
            let index_after_first = snapshot_after_first.index(pool_guards.disk_guard());
            let entry_after_first = index_after_first
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("transition snapshot should persist the row into LWC");
            assert!(root_after_first.deletion_cutoff_ts <= delete_cts);
            assert!(
                index_after_first
                    .load_delete_deltas(&entry_after_first)
                    .await
                    .unwrap()
                    .is_empty()
            );

            hold_trx.rollback().await.unwrap();
            wait_gc_cutoff_after(&checkpoint_session, delete_cts).await;
            checkpoint_published(table_id, &mut checkpoint_session).await;

            let snapshot_after_second = column_block_index_snapshot(&engine, table_id);
            let root_after_second = &snapshot_after_second.active_root;
            let pool_guards = session.pool_guards();
            let index_after_second = snapshot_after_second.index(pool_guards.disk_guard());
            let entry_after_second = index_after_second
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("persisted entry should still exist");
            let deltas = index_after_second
                .load_delete_deltas(&entry_after_second)
                .await
                .unwrap();
            let expected_delta = (row_id - entry_after_second.start_row_id) as u32;
            assert!(root_after_second.deletion_cutoff_ts > delete_cts);
            assert!(deltas.contains(&expected_delta));
        });
    }

    #[test]
    fn test_checkpoint_fails_when_eligible_delete_marker_has_no_column_index() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "name").await;
            delete_key_range_and_wait_gc_cutoff(table_id, &mut session, 0, 4).await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert!(root_before.pivot_row_id > RowID::new(0));
            assert_eq!(root_before.column_block_index_root, SUPER_BLOCK_ID);
            let marker_ts = root_before.deletion_cutoff_ts;
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(RowID::new(0), marker_ts)
                .unwrap();
            wait_gc_cutoff_after(&session, marker_ts).await;

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
            assert_eq!(
                root_after.column_block_index_root,
                root_before.column_block_index_root
            );
        });
    }

    #[test]
    fn test_checkpoint_fails_when_eligible_delete_marker_cannot_be_located() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let key = single_key(0i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            let snapshot_before = column_block_index_snapshot(&engine, table_id);
            let root_before = &snapshot_before.active_root;
            assert_ne!(root_before.column_block_index_root, SUPER_BLOCK_ID);
            let missing_row_id = row_id + 1;
            assert!(missing_row_id < root_before.pivot_row_id);
            let pool_guards = session.pool_guards();
            let index = snapshot_before.index(pool_guards.disk_guard());
            assert!(index.locate_block(missing_row_id).await.unwrap().is_none());

            let marker_ts = root_before.deletion_cutoff_ts;
            table
                .deletion_buffer()
                .put_committed(missing_row_id, marker_ts)
                .unwrap();
            wait_gc_cutoff_after(&session, marker_ts).await;

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            assert_eq!(
                table.file().active_root_unchecked().deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
        });
    }

    #[test]
    fn test_checkpoint_ignores_missing_old_delete_marker_below_previous_cutoff() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let key = single_key(0i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            let root_before = table.file().active_root_unchecked().clone();
            assert!(root_before.deletion_cutoff_ts > TrxID::new(0));
            let missing_row_id = row_id + 1;
            assert!(missing_row_id < root_before.pivot_row_id);
            let old_marker_ts = root_before.deletion_cutoff_ts.saturating_sub(1);
            table
                .deletion_buffer()
                .put_committed(missing_row_id, old_marker_ts)
                .unwrap();
            wait_gc_cutoff_after(&session, root_before.deletion_cutoff_ts).await;

            checkpoint_published(table_id, &mut session).await;
            let root_after = table.file().active_root_unchecked().clone();
            if root_after.deletion_cutoff_ts <= root_before.deletion_cutoff_ts {
                let watermark = engine
                    .catalog()
                    .storage
                    .table_replay_silent_watermarks()
                    .find_uncommitted_by_table_id(&session.pool_guards(), table_id)
                    .await
                    .unwrap()
                    .expect("silent checkpoint should write a watermark");
                assert!(watermark.deletion_cutoff_ts > root_before.deletion_cutoff_ts);
                session.checkpoint_catalog().await.unwrap();
            }
            let effective = engine.catalog().effective_user_table_redo_replay_floor(
                table_id,
                table.redo_replay_floor_snapshot(),
            );
            assert!(
                effective.deletion_cutoff_ts > root_before.deletion_cutoff_ts,
                "{effective:?}"
            );
        });
    }

    #[test]
    fn test_checkpoint_skips_cold_delete_markers_at_or_after_cutoff() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let key = single_key(7i32);
            let reader = session.begin_trx().unwrap();
            let row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                reader.sts(),
            )
            .await;
            reader.commit().await.unwrap();

            let mut hold_session = engine.new_session().unwrap();
            let hold_trx = hold_session.begin_trx().unwrap();
            let hold_sts = hold_trx.sts();

            let mut writer_session = engine.new_session().unwrap();
            expect_delete_committed(table_id, &mut writer_session, &key).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let marker = table.deletion_buffer().get(row_id).unwrap();
            let delete_cts = match marker {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            assert!(delete_cts >= hold_sts);

            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(table_id, &mut checkpoint_session).await;

            let snapshot = column_block_index_snapshot(&engine, table_id);
            let active_root = &snapshot.active_root;
            let pool_guards = session.pool_guards();
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("persisted entry should exist");
            assert!(active_root.deletion_cutoff_ts <= delete_cts);
            assert!(index.load_delete_deltas(&entry).await.unwrap().is_empty());

            hold_trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_checkpoint_fails_on_invalid_v2_delete_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let key1 = single_key(6i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id1 =
                assert_row_in_lwc(&table, &session.pool_guards(), &key1, reader.sts()).await;
            reader.commit().await.unwrap();

            expect_delete_committed(table_id, &mut session, &key1).await;
            let marker1 = table.deletion_buffer().get(row_id1).unwrap();
            let marker1_ts = delete_marker_ts(marker1);
            wait_gc_cutoff_after(&session, marker1_ts).await;
            checkpoint_published(table_id, &mut session).await;

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index
                .locate_block(row_id1)
                .await
                .unwrap()
                .expect("persisted entry should exist");

            let key2 = single_key(7i32);
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let row_id2 =
                assert_row_in_lwc(&table, &reader_session.pool_guards(), &key2, reader.sts()).await;
            reader.commit().await.unwrap();
            let entry2 = index
                .locate_block(row_id2)
                .await
                .unwrap()
                .expect("second persisted entry should exist");
            assert_eq!(entry2.leaf_block_id, entry.leaf_block_id);
            drop(reader_session);

            expect_delete_committed(table_id, &mut session, &key2).await;
            let marker2 = table.deletion_buffer().get(row_id2).unwrap();
            let marker2_ts = delete_marker_ts(marker2);
            wait_gc_cutoff_after(&session, marker2_ts).await;

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_leaf_delete_codec(table_file_path, entry.leaf_block_id, 0);
            let _ = table
                .disk_pool()
                .invalidate_block(table.file().sparse_file().file_id(), entry.leaf_block_id);

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_table_data_integrity(
                err,
                "column-block-index",
                entry.leaf_block_id,
                DataIntegrityError::InvalidPayload,
            );
        });
    }

    #[test]
    fn test_checkpoint_fails_on_short_v2_delete_section_header() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;

            let key1 = single_key(6i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id1 =
                assert_row_in_lwc(&table, &session.pool_guards(), &key1, reader.sts()).await;
            reader.commit().await.unwrap();

            expect_delete_committed(table_id, &mut session, &key1).await;
            let marker1 = table.deletion_buffer().get(row_id1).unwrap();
            let marker1_ts = delete_marker_ts(marker1);
            wait_gc_cutoff_after(&session, marker1_ts).await;
            checkpoint_published(table_id, &mut session).await;

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index
                .locate_block(row_id1)
                .await
                .unwrap()
                .expect("persisted entry should exist");

            let key2 = single_key(7i32);
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let row_id2 =
                assert_row_in_lwc(&table, &reader_session.pool_guards(), &key2, reader.sts()).await;
            reader.commit().await.unwrap();
            let entry2 = index
                .locate_block(row_id2)
                .await
                .unwrap()
                .expect("second persisted entry should exist");
            assert_eq!(entry2.leaf_block_id, entry.leaf_block_id);
            drop(reader_session);

            expect_delete_committed(table_id, &mut session, &key2).await;
            let marker2 = table.deletion_buffer().get(row_id2).unwrap();
            let marker2_ts = delete_marker_ts(marker2);
            wait_gc_cutoff_after(&session, marker2_ts).await;

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_leaf_short_delete_section_header(table_file_path, entry.leaf_block_id, 0);
            let _ = table
                .disk_pool()
                .invalidate_block(table.file().sparse_file().file_id(), entry.leaf_block_id);

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_table_data_integrity(
                err,
                "column-block-index",
                entry.leaf_block_id,
                DataIntegrityError::InvalidPayload,
            );
        });
    }

    #[test]
    fn test_checkpoint_basic_flow() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "x".repeat(1024);
            insert_rows(table_id, &mut session, 0, 200, &name).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let old_root = table.file().active_root_unchecked().clone();
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let (frozen_pages, _) = table
                .collect_frozen_pages(&session.pool_guards())
                .await
                .unwrap();
            assert!(!frozen_pages.is_empty());

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            assert!(
                matches!(outcome, CheckpointOutcome::Published { silent: false, .. }),
                "{outcome:?}"
            );
            assert!(engine.inner().trx_sys.storage_poison_error().is_none());

            let new_root = table.file().active_root_unchecked().clone();
            assert!(new_root.pivot_row_id > old_root.pivot_row_id);
            assert_ne!(new_root.column_block_index_root, SUPER_BLOCK_ID);
            assert!(new_root.deletion_cutoff_ts > old_root.deletion_cutoff_ts);
        });
    }

    #[test]
    fn test_checkpoint_publish_write_failure_poisons_storage() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "publish-write-fail").await;
            delete_key_range_and_wait_gc_cutoff(table_id, &mut session, 0, 4).await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let root_before = table.file().active_root_unchecked().clone();
            wait_checkpoint_ready(table_id, &session).await;
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let hook = Arc::new(FailingFirstWriteHook::new(table_file_path));
            let _install = install_storage_backend_test_hook(hook.clone());

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_checkpoint_write_poisoned(&err, &engine);
            assert!(hook.call_count() > 0);
            assert_root_metadata_unchanged(
                &root_before,
                &table_for_internal_assertion(&engine, table_id),
            );
            assert!(!session.in_trx().unwrap());
        });
    }

    #[test]
    fn test_checkpoint_post_publication_failure_poisons_storage() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "post-publish-fail").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let root_before = table.file().active_root_unchecked().clone();
            wait_checkpoint_ready(table_id, &session).await;

            let res = {
                let _guard = ForcePostPublishCheckpointErrorGuard::new();
                session.checkpoint_table(table_id).await
            };

            let err = res.unwrap_err();
            assert_checkpoint_write_poisoned(&err, &engine);
            assert!(table.file().active_root_unchecked().root_ts > root_before.root_ts);
            assert!(!session.in_trx().unwrap());
        });
    }

    #[test]
    fn test_checkpoint_readiness_ready_when_effective_ts_crossed_gc_horizon() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let session = engine.new_session().unwrap();
            let root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let effective_ts = root.effective_ts();
            wait_gc_cutoff_after(&session, effective_ts).await;
            assert!(matches!(
                session.table_checkpoint_readiness(table_id).unwrap(),
                CheckpointReadiness::Ready
            ));
        });
    }

    #[test]
    fn test_checkpoint_readiness_returns_error_after_session_close() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            session.close().await.unwrap();

            let err = session.table_checkpoint_readiness(table_id).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Operation);
            assert_eq!(err.operation_error(), Some(OperationError::NotSupported));
        });
    }

    #[test]
    fn test_checkpoint_readiness_reports_missing_and_non_live_tables() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let session = engine.new_session().unwrap();

            assert_eq!(
                session.table_checkpoint_readiness(table_id + 1000).unwrap(),
                CheckpointReadiness::TableNotFound
            );

            let _ = session.table_checkpoint_readiness(table_id).unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            table.begin_drop_lifecycle().await.unwrap();

            assert_eq!(
                session.table_checkpoint_readiness(table_id).unwrap(),
                CheckpointReadiness::TableDropping
            );
            let uncached_session = engine.new_session().unwrap();
            assert_eq!(
                uncached_session
                    .table_checkpoint_readiness(table_id)
                    .unwrap(),
                CheckpointReadiness::TableDropping
            );

            table.mark_dropped_lifecycle().unwrap();

            assert_eq!(
                session.table_checkpoint_readiness(table_id).unwrap(),
                CheckpointReadiness::TableNotFound
            );
            let uncached_session = engine.new_session().unwrap();
            assert_eq!(
                uncached_session
                    .table_checkpoint_readiness(table_id)
                    .unwrap(),
                CheckpointReadiness::TableNotFound
            );
        });
    }

    #[test]
    fn test_checkpoint_readiness_delayed_reports_effective_ts_and_horizon() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "readiness-delay").await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            checkpoint_published(table_id, &mut session).await;

            let active_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let active_root_effective_ts = active_root.effective_ts();
            let readiness = session.table_checkpoint_readiness(table_id).unwrap();
            let CheckpointReadiness::Delayed { reason } = readiness else {
                panic!("expected delayed checkpoint readiness, got {readiness:?}");
            };
            assert_eq!(reason.effective_ts, active_root_effective_ts);
            assert_eq!(reason.min_active_sts, reader.sts());
            assert!(reason.effective_ts >= reason.min_active_sts);

            reader.commit().await.unwrap();
            wait_gc_cutoff_after(&session, active_root_effective_ts).await;
            assert!(matches!(
                session.table_checkpoint_readiness(table_id).unwrap(),
                CheckpointReadiness::Ready
            ));
        });
    }

    #[test]
    fn test_checkpoint_readiness_uses_root_effective_ts_not_checkpoint_start_ts() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "effective-delay").await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let reader_holder: Rc<RefCell<Option<(Session, Transaction)>>> =
                Rc::new(RefCell::new(None));
            let reader_sts = Rc::new(Cell::new(TrxID::new(0)));
            let hook_reader_holder = Rc::clone(&reader_holder);
            let hook_reader_sts = Rc::clone(&reader_sts);
            let hook_engine = engine.new_ref().unwrap();
            set_test_checkpoint_after_trx_start_hook(move || async move {
                let mut reader_session = hook_engine.new_session().unwrap();
                let reader = reader_session.begin_trx().unwrap();
                hook_reader_sts.set(reader.sts());
                *hook_reader_holder.borrow_mut() = Some((reader_session, reader));
            });

            let checkpoint_ts = checkpoint_published(table_id, &mut session).await;
            let active_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let effective_ts = active_root.effective_ts();
            assert!(checkpoint_ts < reader_sts.get());
            assert!(effective_ts > reader_sts.get());

            let readiness = session.table_checkpoint_readiness(table_id).unwrap();
            let CheckpointReadiness::Delayed { reason } = readiness else {
                panic!("expected effective timestamp delay, got {readiness:?}");
            };
            assert_eq!(reason.effective_ts, effective_ts);
            assert!(reason.min_active_sts <= reader_sts.get());
            assert!(reason.effective_ts >= reason.min_active_sts);

            let (_, mut reader) = reader_holder
                .borrow_mut()
                .take()
                .expect("reader hook should install an active transaction");
            reader
                .exec(async |stmt| {
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    let proof = rt.read_proof();
                    let snapshot = table_for_internal_assertion(&engine, table_id)
                        .root_snapshot(&proof)
                        .unwrap();
                    let _effects_addr = effects as *mut _;
                    assert!(snapshot.root_ts() < rt.sts());
                    assert_eq!(snapshot.effective_ts(), effective_ts);
                    assert!(!snapshot.root_is_visible_to(rt.sts()));
                    Ok(())
                })
                .await
                .unwrap();
            reader.commit().await.unwrap();
            wait_gc_cutoff_after(&session, effective_ts).await;
            assert!(matches!(
                session.table_checkpoint_readiness(table_id).unwrap(),
                CheckpointReadiness::Ready
            ));
        });
    }

    #[test]
    fn test_checkpoint_requires_idle_session_before_delayed_outcome() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "idle-before-delay").await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let first_checkpoint_ts = checkpoint_published(table_id, &mut session).await;
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .file()
                    .active_root_unchecked()
                    .root_ts,
                first_checkpoint_ts
            );

            let checkpoint_trx = session.begin_trx().unwrap();
            assert!(session.in_trx().unwrap());
            assert!(matches!(
                session.table_checkpoint_readiness(table_id).unwrap(),
                CheckpointReadiness::Delayed { .. }
            ));

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::NotSupported));
            assert!(format!("{err:?}").contains("checkpoint requires idle session"));
            assert!(session.in_trx().unwrap());

            checkpoint_trx.rollback().await.unwrap();
            assert!(!session.in_trx().unwrap());
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_checkpoint_delayed_preserves_root_and_frozen_pages_until_ready() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "delayed-root").await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            checkpoint_published(table_id, &mut session).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let root_after_first = table.file().active_root_unchecked().clone();
            let effective_ts_protected_by_reader = root_after_first.effective_ts();

            insert_rows(table_id, &mut session, 1_000, 80, "delayed-frozen").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let (frozen_pages, _) = table
                .collect_frozen_pages(&session.pool_guards())
                .await
                .unwrap();
            assert!(!frozen_pages.is_empty());
            let first_frozen_page = frozen_pages[0].page_id;
            let root_before_delay = table.file().active_root_unchecked().clone();

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("expected delayed checkpoint, got {outcome:?}");
            };
            assert_eq!(reason.effective_ts, effective_ts_protected_by_reader);
            assert_eq!(reason.min_active_sts, reader.sts());
            assert_root_metadata_unchanged(&root_before_delay, &table);

            let page_guard = table
                .mem
                .must_get_row_page_shared(&session.pool_guards(), first_frozen_page)
                .await
                .unwrap();
            let (ctx, _) = page_guard.ctx_and_page();
            assert_eq!(ctx.row_ver().unwrap().state(), RowPageState::Frozen);
            drop(page_guard);
            let (still_frozen_pages, _) = table
                .collect_frozen_pages(&session.pool_guards())
                .await
                .unwrap();
            assert_eq!(still_frozen_pages[0].page_id, first_frozen_page);

            reader.commit().await.unwrap();
            wait_gc_cutoff_after(&session, effective_ts_protected_by_reader).await;
            let checkpoint_ts = checkpoint_published(table_id, &mut session).await;
            let root_after_publish = table.file().active_root_unchecked().clone();
            assert_eq!(root_after_publish.root_ts, checkpoint_ts);
            assert!(root_after_publish.pivot_row_id > root_before_delay.pivot_row_id);
        });
    }

    #[test]
    fn test_second_checkpoint_waits_for_previous_root_horizon() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "second-delay").await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let first_checkpoint_ts = checkpoint_published(table_id, &mut session).await;
            let first_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let first_effective_ts = first_root.effective_ts();
            assert_eq!(first_root.root_ts, first_checkpoint_ts);

            let root_before_second = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let outcome = session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("expected second checkpoint to wait, got {outcome:?}");
            };
            assert_eq!(reason.effective_ts, first_effective_ts);
            assert_eq!(reason.min_active_sts, reader.sts());
            assert_root_metadata_unchanged(
                &root_before_second,
                &table_for_internal_assertion(&engine, table_id),
            );

            reader.commit().await.unwrap();
            wait_gc_cutoff_after(&session, first_effective_ts).await;
            let second_checkpoint_ts = checkpoint_published(table_id, &mut session).await;
            assert!(second_checkpoint_ts > first_checkpoint_ts);
        });
    }

    #[test]
    fn test_checkpoint_reachability_reclaims_obsolete_column_index_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "reachability-first").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;
            let first_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let first_column_root = first_root.column_block_index_root;
            assert_ne!(first_column_root, SUPER_BLOCK_ID);

            insert_rows(table_id, &mut session, 1_000, 120, "reachability-second").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;
            let second_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_ne!(second_root.column_block_index_root, first_column_root);
            assert!(
                second_root
                    .alloc_map
                    .is_allocated(usize::from(first_column_root)),
                "the old column-index root stays allocated while the displaced root is protected"
            );

            wait_gc_cutoff_after(&session, second_root.effective_ts()).await;
            checkpoint_published(table_id, &mut session).await;
            let reclaimed_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_ne!(reclaimed_root.column_block_index_root, first_column_root);
            if reclaimed_root
                .alloc_map
                .is_allocated(usize::from(first_column_root))
            {
                assert_eq!(
                    reclaimed_root.meta_block_id, first_column_root,
                    "the freed obsolete column-index root may be immediately reused for the new meta block"
                );
            }
            assert!(
                reclaimed_root
                    .alloc_map
                    .is_allocated(usize::from(second_root.column_block_index_root)),
                "current column-index root must remain allocated"
            );
        });
    }

    #[test]
    fn test_checkpoint_rechecks_readiness_after_root_mutation_lease() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut checkpoint_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut checkpoint_session, 0, 4, "readiness-recheck").await;
            checkpoint_session
                .freeze_table(table_id, usize::MAX)
                .await
                .unwrap();
            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            wait_gc_cutoff_after(&checkpoint_session, root_before.effective_ts()).await;

            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            assert!(matches!(
                checkpoint_session
                    .table_checkpoint_readiness(table_id)
                    .unwrap(),
                CheckpointReadiness::Ready
            ));

            let hook_table = table_for_internal_assertion(&engine, table_id);
            let hook_engine = engine.new_ref().unwrap();
            let root_before_trx = root_before.root_ts;
            set_test_checkpoint_after_readiness_hook(move || async move {
                let mut competing_session = hook_engine.new_session().unwrap();
                let checkpoint_ts =
                    checkpoint_published(hook_table.table_id(), &mut competing_session).await;
                assert!(checkpoint_ts > root_before_trx);
            });

            let outcome = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("expected post-lease readiness delay, got {outcome:?}");
            };
            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert!(root_after.root_ts > root_before.root_ts);
            assert_eq!(reason.effective_ts, root_after.effective_ts());
            assert_eq!(reason.min_active_sts, reader.sts());

            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_concurrent_checkpoint_table_returns_in_progress_cancellation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut first_session = engine.new_session().unwrap();
            wait_checkpoint_ready(table_id, &first_session).await;

            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            set_test_checkpoint_after_trx_start_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
            });

            let first_outcome = {
                let first_checkpoint = first_session.checkpoint_table(table_id).fuse();
                futures::pin_mut!(first_checkpoint);
                let checkpoint_entered = entered_rx.recv_async().fuse();
                futures::pin_mut!(checkpoint_entered);
                futures::select! {
                    res = first_checkpoint => {
                        panic!("first checkpoint completed before the concurrency hook: {res:?}");
                    }
                    res = checkpoint_entered => {
                        res.unwrap();
                    }
                }

                let mut second_session = engine.new_session().unwrap();
                let outcome = second_session.checkpoint_table(table_id).await.unwrap();
                assert_eq!(
                    outcome,
                    CheckpointOutcome::Cancelled {
                        reason: CheckpointCancelReason::CheckpointInProgress,
                    }
                );
                assert!(!second_session.in_trx().unwrap());

                release_tx.send_async(()).await.unwrap();
                first_checkpoint.await.unwrap()
            };
            assert!(
                matches!(first_outcome, CheckpointOutcome::Published { .. }),
                "first checkpoint should publish after the competing checkpoint cancels: {first_outcome:?}"
            );
            assert!(!first_session.in_trx().unwrap());
        });
    }

    #[test]
    fn test_checkpoint_snapshot_consistency() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "y".repeat(256);
            insert_rows(table_id, &mut session, 0, 120, &name).await;

            session.freeze_table(table_id, 1).await.unwrap();

            let mut read_trx = session.begin_trx().unwrap();
            {
                let key = SelectKey::new(0, vec![Val::from(1)]);
                let res = trx_select_row_mvcc_by_id(&mut read_trx, table_id, &key, &[0, 1]).await;
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));
            }

            let mut write_session = engine.new_session().unwrap();
            let mut write_trx = write_session.begin_trx().unwrap();
            {
                let insert = vec![Val::from(10_000i32), Val::from("new")];
                let res = trx_insert_row_by_id(&mut write_trx, table_id, insert).await;
                assert!(res.is_ok());
            }

            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(table_id, &mut checkpoint_session).await;

            {
                let key = SelectKey::new(0, vec![Val::from(10_000i32)]);
                let res = trx_select_row_mvcc_by_id(&mut read_trx, table_id, &key, &[0, 1]).await;
                assert!(matches!(res, Ok(SelectMvcc::NotFound)));
            }

            write_trx.rollback().await.unwrap();
            read_trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_checkpoint_old_root_released_after_active_reader_purged() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "retained-root".repeat(64);
            insert_rows(table_id, &mut session, 0, 120, &name).await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let retained_root_ptr = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked() as *const _ as usize;
            let drop_count_before = old_root_drop_count(retained_root_ptr);

            let mut read_session = engine.new_session().unwrap();
            let read_trx = read_session.begin_trx().unwrap();

            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(table_id, &mut checkpoint_session).await;

            assert_eq!(
                old_root_drop_count(retained_root_ptr),
                drop_count_before,
                "old root must stay retained while a pre-checkpoint transaction is active"
            );

            read_trx.commit().await.unwrap();
            expect_insert_committed(
                table_id,
                &mut session,
                vec![Val::from(50_000i32), Val::from("after-retention-reader")],
            )
            .await;

            for _ in 0..50 {
                if old_root_drop_count(retained_root_ptr) > drop_count_before {
                    break;
                }
                Timer::after(Duration::from_millis(20)).await;
            }
            assert!(
                old_root_drop_count(retained_root_ptr) > drop_count_before,
                "old root should be released after transaction GC crosses the checkpoint"
            );
        });
    }

    #[test]
    fn test_checkpoint_persistence_recovery() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = engine
                .catalog()
                .get_table_now(table_id)
                .expect("test table should exist");
            let mut session = engine.new_session().unwrap();
            let name = "z".repeat(512);
            insert_rows_direct(table_id, &mut session, 0, 150, &name).await;

            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            checkpoint_published(table.table_id(), &mut session).await;

            let root_before = table.file().active_root_unchecked().clone();
            drop(table);

            let table_file = engine
                .inner()
                .table_fs
                .open_table_file(table_id, engine.inner().disk_pool.clone_inner())
                .await
                .unwrap();
            let root_after = table_file.active_root_unchecked();
            assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
            assert_eq!(
                root_after.heap_redo_start_ts,
                root_before.heap_redo_start_ts
            );
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
        });
    }

    #[test]
    fn test_checkpoint_heartbeat() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "h".repeat(128);
            insert_rows(table_id, &mut session, 0, 40, &name).await;

            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let outcome = session.checkpoint_table(table_id).await.unwrap();
            assert!(
                matches!(outcome, CheckpointOutcome::Published { silent: true, .. }),
                "{outcome:?}"
            );
            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();

            assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
            assert_eq!(
                root_after.heap_redo_start_ts,
                root_before.heap_redo_start_ts
            );
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
            assert_eq!(
                root_after.column_block_index_root,
                root_before.column_block_index_root
            );

            let guards = session.pool_guards();
            let watermark = engine
                .catalog()
                .storage
                .table_replay_silent_watermarks()
                .find_uncommitted_by_table_id(&guards, table_id)
                .await
                .unwrap()
                .expect("silent checkpoint should write a catalog row");
            assert!(watermark.heap_redo_start_ts > root_before.heap_redo_start_ts);
            assert!(watermark.deletion_cutoff_ts > root_before.deletion_cutoff_ts);
            assert!(
                engine
                    .catalog()
                    .storage
                    .checkpointed_silent_watermarks()
                    .get(&table_id)
                    .is_none(),
                "uncheckpointed watermark rows must not update durable cache"
            );

            let snapshot = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let (live_before_catalog_checkpoint, _) = engine
                .catalog()
                .snapshot_user_table_redo_floors(snapshot.catalog_replay_start_ts);
            assert_eq!(live_before_catalog_checkpoint.len(), 1);
            assert_eq!(
                live_before_catalog_checkpoint[0].floor,
                table_for_internal_assertion(&engine, table_id).redo_replay_floor_snapshot()
            );

            session.checkpoint_catalog().await.unwrap();
            let checkpointed = engine.catalog().storage.checkpointed_silent_watermarks();
            let checkpointed_floor = checkpointed
                .get(&table_id)
                .copied()
                .expect("catalog checkpoint should rebuild durable watermark cache");
            assert_eq!(
                checkpointed_floor.heap_redo_start_ts,
                watermark.heap_redo_start_ts
            );
            assert_eq!(
                checkpointed_floor.deletion_cutoff_ts,
                watermark.deletion_cutoff_ts
            );
            let snapshot = engine.catalog().storage.checkpoint_snapshot().unwrap();
            let (live_after_catalog_checkpoint, _) = engine
                .catalog()
                .snapshot_user_table_redo_floors(snapshot.catalog_replay_start_ts);
            assert_eq!(live_after_catalog_checkpoint.len(), 1);
            assert_eq!(live_after_catalog_checkpoint[0].floor, checkpointed_floor);
        });
    }

    #[test]
    fn test_checkpoint_gc_verification() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "g".repeat(1024);
            insert_rows(table_id, &mut session, 0, 200, &name).await;

            let allocated_before = engine.inner().mem_pool.allocated();
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;
            let allocated_after = engine.inner().mem_pool.allocated();
            let mut reclaimed = allocated_after < allocated_before;
            for _ in 0..20 {
                Timer::after(Duration::from_millis(200)).await;
                let allocated_now = engine.inner().mem_pool.allocated();
                if allocated_now < allocated_before {
                    reclaimed = true;
                    break;
                }
            }
            assert!(reclaimed, "row pages should be reclaimed after purge");
        });
    }

    #[test]
    fn test_wait_for_frozen_pages_stable_wakes_on_storage_poison() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut writer = engine.new_session().unwrap();
            let mut trx = writer.begin_trx().unwrap();
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(1i32), Val::from("blocked")],
            )
            .await
            .unwrap();

            let checkpoint_session = engine.new_session().unwrap();
            checkpoint_session
                .freeze_table(table_id, usize::MAX)
                .await
                .unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let pool_guards = checkpoint_session.pool_guards();
            let (frozen_pages, _) = table.collect_frozen_pages(&pool_guards).await.unwrap();
            assert_eq!(frozen_pages.len(), 1);

            let trx_sys = engine.inner().trx_sys.clone();
            let page_guard = table
                .mem
                .must_get_row_page_shared(&pool_guards, frozen_pages[0].page_id)
                .await
                .unwrap();
            let (ctx, _) = page_guard.ctx_and_page();
            assert!(
                ctx.row_ver().unwrap().max_ins_sts() >= trx_sys.published_gc_horizon(),
                "test setup must block on the published GC horizon"
            );
            drop(page_guard);

            let wait = table
                .wait_for_frozen_pages_stable(&pool_guards, &trx_sys, &frozen_pages)
                .fuse();
            let poison_trx_sys = trx_sys.clone();
            let poison = async move {
                Timer::after(Duration::from_millis(20)).await;
                let poison = poison_trx_sys.poison_storage(FatalError::CheckpointWrite);
                assert_eq!(*poison.current_context(), FatalError::CheckpointWrite);
            }
            .fuse();
            let timeout = Timer::after(Duration::from_secs(2)).fuse();
            futures::pin_mut!(wait);
            futures::pin_mut!(poison);
            futures::pin_mut!(timeout);

            let mut poison_done = false;
            let res = loop {
                futures::select! {
                    res = wait => {
                        assert!(poison_done, "wait returned before storage poison");
                        break res;
                    }
                    () = poison => {
                        poison_done = true;
                    }
                    _ = timeout => {
                        panic!("frozen-page stability wait did not wake on storage poison");
                    }
                }
            };
            let err = res.expect_err("storage poison should abort the wait");
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::CheckpointWrite)
            );
            discard_transaction_after_fatal_rollback(&mut trx);
        });
    }

    #[test]
    fn test_checkpoint_error_rollback() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "e".repeat(256);
            insert_rows(table_id, &mut session, 0, 80, &name).await;

            session.freeze_table(table_id, usize::MAX).await.unwrap();
            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();

            set_test_force_lwc_build_error(true);
            let res = session.checkpoint_table(table_id).await;
            set_test_force_lwc_build_error(false);
            assert!(res.is_err());
            let poison = engine
                .inner()
                .trx_sys
                .storage_poison_error()
                .expect("transition publication guard should poison storage");
            assert_eq!(*poison.current_context(), FatalError::CheckpointWrite);

            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
            assert_eq!(
                root_after.heap_redo_start_ts,
                root_before.heap_redo_start_ts
            );
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
            assert_eq!(
                root_after.column_block_index_root,
                root_before.column_block_index_root
            );
        });
    }

    #[test]
    fn test_checkpoint_cancelled_while_table_metadata_change_active() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut checkpoint_session = engine.new_session().unwrap();

            wait_checkpoint_ready(table_id, &checkpoint_session).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let _metadata_lease = table.begin_metadata_change().await.unwrap();

            let outcome = checkpoint_session
                .checkpoint_table(table_id)
                .await
                .expect("checkpoint should return a normal cancellation");

            assert_eq!(
                outcome,
                CheckpointOutcome::Cancelled {
                    reason: CheckpointCancelReason::TableMetadataChanging,
                }
            );
        })
    }

    #[test]
    fn test_checkpoint_reachability_reclaims_dropped_secondary_disk_tree_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 64, "drop-index-reclaim").await;
            session.freeze_table(table_id, usize::MAX).await.unwrap();
            checkpoint_published(table_id, &mut session).await;
            let indexed_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let dropped_disk_root = indexed_root.secondary_index_roots[0];
            assert_ne!(dropped_disk_root, SUPER_BLOCK_ID);
            assert!(
                indexed_root
                    .alloc_map
                    .is_allocated(usize::from(dropped_disk_root))
            );

            session.drop_index(table_id, 0).await.unwrap();
            let after_drop_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_eq!(after_drop_root.secondary_index_roots[0], SUPER_BLOCK_ID);
            assert!(
                after_drop_root
                    .alloc_map
                    .is_allocated(usize::from(dropped_disk_root)),
                "DROP INDEX detaches the root but leaves page reclamation to checkpoint reachability"
            );

            wait_gc_cutoff_after(&session, after_drop_root.effective_ts()).await;
            checkpoint_published(table_id, &mut session).await;
            let after_reclaim = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert!(
                !after_reclaim
                    .alloc_map
                    .is_allocated(usize::from(dropped_disk_root)),
                "checkpoint reachability should reclaim detached DiskTree pages"
            );
        })
    }
}
