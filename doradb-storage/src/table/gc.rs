#![cfg_attr(not(test), allow(dead_code))]

use super::Table;
use crate::buffer::{EvictableBufferPool, PoolGuard, PoolGuards};
use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind, Result};
use crate::file::BlockID;
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::index::{
    ColumnBlockIndex, NonUniqueMemIndexEntry, NonUniqueSecondaryIndex, ResolvedColumnRow,
    SecondaryIndex, UniqueMemIndexEntry, UniqueSecondaryIndex,
};
use crate::lwc::PersistedLwcBlock;
use crate::row::RowID;
use crate::session::Session;
use crate::trx::TrxID;
use crate::value::Val;

/// Aggregate result for a full-scan user-table secondary MemIndex cleanup pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SecondaryMemIndexCleanupStats {
    /// One row per secondary index scanned by this pass.
    pub(crate) indexes: Vec<SecondaryMemIndexCleanupIndexStats>,
}

/// Policy options for user-table secondary MemIndex cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SecondaryMemIndexCleanupOptions {
    clean_live_entries: bool,
}

impl SecondaryMemIndexCleanupOptions {
    /// Return options that retain live MemIndex cache entries.
    #[inline]
    pub(crate) const fn retain_live_entries(mut self) -> Self {
        self.clean_live_entries = false;
        self
    }

    #[inline]
    const fn clean_live_entries(self) -> bool {
        self.clean_live_entries
    }
}

impl Default for SecondaryMemIndexCleanupOptions {
    #[inline]
    fn default() -> Self {
        Self {
            clean_live_entries: true,
        }
    }
}

/// Cleanup result for one secondary MemIndex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecondaryMemIndexCleanupIndexStats {
    /// Table-local secondary-index number.
    pub(crate) index_no: usize,
    /// Whether the scanned index is unique.
    pub(crate) unique: bool,
    /// Number of MemIndex entries processed as cleanup candidates.
    pub(crate) scanned: usize,
    /// Number of MemIndex entries physically removed.
    pub(crate) removed: usize,
    /// Number of MemIndex entries intentionally retained.
    pub(crate) retained: usize,
    /// Number of live MemIndex entries skipped before key materialization.
    pub(crate) skipped_live: usize,
    /// Number of hot delete overlays skipped before key materialization.
    pub(crate) skipped_hot_deleted: usize,
}

struct MemIndexCleanupSnapshot {
    table_root_ts: TrxID,
    pivot_row_id: RowID,
    column_block_index_root: BlockID,
    deletion_cutoff_ts: TrxID,
    secondary_index_roots: Vec<BlockID>,
    min_active_sts: TrxID,
}

impl MemIndexCleanupSnapshot {
    #[inline]
    fn is_visible_to(&self, cleanup_sts: TrxID) -> bool {
        self.table_root_ts < cleanup_sts
    }
}

struct MemIndexCleanupContext<'a> {
    snapshot: &'a MemIndexCleanupSnapshot,
    options: SecondaryMemIndexCleanupOptions,
    column_index: Option<&'a ColumnBlockIndex<'a>>,
    index_pool_guard: &'a PoolGuard,
    disk_pool_guard: &'a PoolGuard,
}

enum CleanupDecision {
    Remove,
    Retain,
}

enum DeleteOverlayProof {
    NotProven,
    Obsolete,
    ColdRowValues(Vec<Val>),
}

impl SecondaryMemIndexCleanupIndexStats {
    #[inline]
    fn new(index_no: usize, unique: bool) -> Self {
        Self {
            index_no,
            unique,
            scanned: 0,
            removed: 0,
            retained: 0,
            skipped_live: 0,
            skipped_hot_deleted: 0,
        }
    }

    #[inline]
    fn record(&mut self, decision: CleanupDecision) {
        self.scanned += 1;
        match decision {
            CleanupDecision::Remove => self.removed += 1,
            CleanupDecision::Retain => self.retained += 1,
        }
    }

    #[inline]
    fn record_skipped_live(&mut self, count: usize) {
        self.skipped_live += count;
    }

    #[inline]
    fn record_skipped_hot_deleted(&mut self, count: usize) {
        self.skipped_hot_deleted += count;
    }
}

impl Table {
    /// Full-scan cleanup for user-table secondary MemIndex entries.
    ///
    /// This pass removes only entries proven redundant or obsolete against one
    /// captured table-file root. It never mutates DiskTree state, and it treats
    /// missing delete proof as a retention decision for delete overlays.
    pub(crate) async fn cleanup_secondary_mem_indexes(
        &self,
        session: &mut Session,
    ) -> Result<SecondaryMemIndexCleanupStats> {
        self.cleanup_secondary_mem_indexes_with_options(
            session,
            SecondaryMemIndexCleanupOptions::default(),
        )
        .await
    }

    /// Full-scan cleanup with caller-selected live-entry cleanup policy.
    pub(crate) async fn cleanup_secondary_mem_indexes_with_options(
        &self,
        session: &mut Session,
        options: SecondaryMemIndexCleanupOptions,
    ) -> Result<SecondaryMemIndexCleanupStats> {
        let trx_sys = session.engine().trx_sys.clone();
        loop {
            let trx = session.try_begin_trx()?.ok_or(Error::NotSupported(
                "secondary MemIndex cleanup requires idle session",
            ))?;
            let cleanup_sts = trx.sts();
            let min_active_sts = trx_sys.calc_min_active_sts_for_gc();
            let snapshot = self.capture_mem_index_cleanup_snapshot(min_active_sts);
            if !snapshot.is_visible_to(cleanup_sts) {
                trx_sys.rollback(trx).await?;
                continue;
            }

            let cleanup_res = self
                .cleanup_secondary_mem_indexes_at_snapshot(
                    session.pool_guards(),
                    &snapshot,
                    options,
                )
                .await;
            let rollback_res = trx_sys.rollback(trx).await;
            return match (cleanup_res, rollback_res) {
                (Ok(stats), Ok(())) => Ok(stats),
                (Err(err), Ok(())) => Err(err),
                (_, Err(err)) => Err(err),
            };
        }
    }

    #[inline]
    fn capture_mem_index_cleanup_snapshot(&self, min_active_sts: TrxID) -> MemIndexCleanupSnapshot {
        let active_root = self.file().active_root();
        // `gc_captured_snapshot`: keep this as an owned field snapshot until
        // RFC-0015 can replace it with a proof-gated `TableRootSnapshot`.
        // Cleanup predicates still use the explicit GC horizon below.
        MemIndexCleanupSnapshot {
            table_root_ts: active_root.trx_id,
            pivot_row_id: active_root.pivot_row_id,
            column_block_index_root: active_root.column_block_index_root,
            deletion_cutoff_ts: active_root.deletion_cutoff_ts,
            secondary_index_roots: active_root.secondary_index_roots.clone(),
            min_active_sts,
        }
    }

    #[inline]
    async fn cleanup_secondary_mem_indexes_at_snapshot(
        &self,
        guards: &PoolGuards,
        snapshot: &MemIndexCleanupSnapshot,
        options: SecondaryMemIndexCleanupOptions,
    ) -> Result<SecondaryMemIndexCleanupStats> {
        debug_assert!(snapshot.deletion_cutoff_ts <= snapshot.table_root_ts);

        let column_index = self.cleanup_column_index(guards, snapshot);
        let index_pool_guard = self.index_pool_guard(guards);
        let disk_pool_guard = guards.disk_guard();
        let cleanup_context = MemIndexCleanupContext {
            snapshot,
            options,
            column_index: column_index.as_ref(),
            index_pool_guard,
            disk_pool_guard,
        };
        let mut stats = SecondaryMemIndexCleanupStats {
            indexes: Vec::with_capacity(self.sec_idx().len()),
        };

        for index in self.sec_idx() {
            let index_no = index.index_no();
            let secondary_root = snapshot
                .secondary_index_roots
                .get(index_no)
                .copied()
                .ok_or(Error::InvalidState)?;
            let mut index_stats =
                SecondaryMemIndexCleanupIndexStats::new(index_no, index.is_unique());
            match index {
                SecondaryIndex::Unique(unique) => {
                    self.cleanup_unique_secondary_mem_index(
                        &cleanup_context,
                        index_no,
                        unique,
                        secondary_root,
                        &mut index_stats,
                    )
                    .await?;
                }
                SecondaryIndex::NonUnique(non_unique) => {
                    self.cleanup_non_unique_secondary_mem_index(
                        &cleanup_context,
                        index_no,
                        non_unique,
                        secondary_root,
                        &mut index_stats,
                    )
                    .await?;
                }
            }
            stats.indexes.push(index_stats);
        }

        Ok(stats)
    }

    #[inline]
    async fn cleanup_unique_secondary_mem_index(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_>,
        index_no: usize,
        index: &UniqueSecondaryIndex<EvictableBufferPool>,
        secondary_root: BlockID,
        stats: &mut SecondaryMemIndexCleanupIndexStats,
    ) -> Result<()> {
        let disk = index
            .disk()
            .open_unique_at(secondary_root, cleanup_context.disk_pool_guard)?;
        let mut scan = index.cleanup_mem_scan(
            cleanup_context.index_pool_guard,
            cleanup_context.snapshot.pivot_row_id,
            cleanup_context.options.clean_live_entries(),
        );
        while let Some(batch) = scan.next_batch().await? {
            stats.record_skipped_live(batch.skipped_live);
            stats.record_skipped_hot_deleted(batch.skipped_hot_deleted);
            for entry in batch.entries {
                let decision = if entry.deleted {
                    // Delete-shadows are removable only after we prove the overlay
                    // is obsolete. Whole-row deletion is one proof; for cold rows,
                    // an immutable LWC row whose current unique key encodes
                    // differently proves this scanned shadow no longer protects a
                    // visible owner. Hot row-page version checks stay in
                    // transaction index GC, where undo visibility is available.
                    if self
                        .cleanup_unique_delete_overlay_is_obsolete(
                            cleanup_context,
                            index_no,
                            index,
                            &entry,
                        )
                        .await?
                    {
                        compare_delete_unique_cleanup_entry(
                            index,
                            cleanup_context.index_pool_guard,
                            &entry,
                            cleanup_context.snapshot.min_active_sts,
                        )
                        .await?
                    } else {
                        CleanupDecision::Retain
                    }
                } else {
                    debug_assert!(cleanup_context.options.clean_live_entries());
                    debug_assert!(entry.row_id < cleanup_context.snapshot.pivot_row_id);
                    // Live entries need a matching cold mapping before cleanup can
                    // treat the MemIndex copy as redundant.
                    match disk.lookup_encoded(&entry.encoded_key).await {
                        Ok(Some(row_id)) if row_id == entry.row_id => {
                            compare_delete_unique_cleanup_entry(
                                index,
                                cleanup_context.index_pool_guard,
                                &entry,
                                cleanup_context.snapshot.min_active_sts,
                            )
                            .await?
                        }
                        Ok(_) => CleanupDecision::Retain,
                        Err(err) => return Err(err),
                    }
                };
                stats.record(decision);
            }
        }
        Ok(())
    }

    #[inline]
    async fn cleanup_non_unique_secondary_mem_index(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_>,
        index_no: usize,
        index: &NonUniqueSecondaryIndex<EvictableBufferPool>,
        secondary_root: BlockID,
        stats: &mut SecondaryMemIndexCleanupIndexStats,
    ) -> Result<()> {
        let disk = index
            .disk()
            .open_non_unique_at(secondary_root, cleanup_context.disk_pool_guard)?;
        let mut scan = index.cleanup_mem_scan(
            cleanup_context.index_pool_guard,
            cleanup_context.snapshot.pivot_row_id,
            cleanup_context.options.clean_live_entries(),
        );
        while let Some(batch) = scan.next_batch().await? {
            stats.record_skipped_live(batch.skipped_live);
            stats.record_skipped_hot_deleted(batch.skipped_hot_deleted);
            for entry in batch.entries {
                let decision = if entry.deleted {
                    // Non-unique delete marks use the same cold-row-only proof as
                    // unique shadows. If the persisted row exists but its current
                    // exact key no longer matches this encoded key+row-id pair,
                    // the old delete mark is obsolete. We deliberately avoid hot
                    // row-page key proof here; transaction index GC owns that path.
                    if self
                        .cleanup_non_unique_delete_overlay_is_obsolete(
                            cleanup_context,
                            index_no,
                            index,
                            &entry,
                        )
                        .await?
                    {
                        compare_delete_non_unique_cleanup_entry(
                            index,
                            cleanup_context.index_pool_guard,
                            &entry,
                            cleanup_context.snapshot.min_active_sts,
                        )
                        .await?
                    } else {
                        CleanupDecision::Retain
                    }
                } else {
                    debug_assert!(cleanup_context.options.clean_live_entries());
                    debug_assert!(entry.row_id < cleanup_context.snapshot.pivot_row_id);
                    // Live exact entries are redundant only when the same exact
                    // key is already present in the captured cold root.
                    match disk.contains_exact_encoded(&entry.encoded_key).await {
                        Ok(true) => {
                            compare_delete_non_unique_cleanup_entry(
                                index,
                                cleanup_context.index_pool_guard,
                                &entry,
                                cleanup_context.snapshot.min_active_sts,
                            )
                            .await?
                        }
                        Ok(false) => CleanupDecision::Retain,
                        Err(err) => return Err(err),
                    }
                };
                stats.record(decision);
            }
        }
        Ok(())
    }

    #[inline]
    fn cleanup_column_index<'a>(
        &'a self,
        guards: &'a PoolGuards,
        snapshot: &MemIndexCleanupSnapshot,
    ) -> Option<ColumnBlockIndex<'a>> {
        if snapshot.column_block_index_root == SUPER_BLOCK_ID
            || snapshot.table_root_ts >= snapshot.min_active_sts
        {
            return None;
        }
        Some(ColumnBlockIndex::new(
            snapshot.column_block_index_root,
            snapshot.pivot_row_id,
            self.file().file_kind(),
            self.file().sparse_file(),
            self.disk_pool(),
            guards.disk_guard(),
        ))
    }

    #[inline]
    async fn cleanup_unique_delete_overlay_is_obsolete(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_>,
        index_no: usize,
        index: &UniqueSecondaryIndex<EvictableBufferPool>,
        entry: &UniqueMemIndexEntry,
    ) -> Result<bool> {
        match self
            .cleanup_delete_overlay_proof(cleanup_context, index_no, entry.row_id)
            .await?
        {
            DeleteOverlayProof::NotProven => Ok(false),
            DeleteOverlayProof::Obsolete => Ok(true),
            DeleteOverlayProof::ColdRowValues(values) => {
                Ok(!index.mem_encoded_key_matches(&values, &entry.encoded_key))
            }
        }
    }

    #[inline]
    async fn cleanup_non_unique_delete_overlay_is_obsolete(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_>,
        index_no: usize,
        index: &NonUniqueSecondaryIndex<EvictableBufferPool>,
        entry: &NonUniqueMemIndexEntry,
    ) -> Result<bool> {
        match self
            .cleanup_delete_overlay_proof(cleanup_context, index_no, entry.row_id)
            .await?
        {
            DeleteOverlayProof::NotProven => Ok(false),
            DeleteOverlayProof::Obsolete => Ok(true),
            DeleteOverlayProof::ColdRowValues(values) => {
                Ok(!index.mem_encoded_exact_key_matches(&values, entry.row_id, &entry.encoded_key))
            }
        }
    }

    #[inline]
    async fn cleanup_delete_overlay_proof(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_>,
        index_no: usize,
        row_id: RowID,
    ) -> Result<DeleteOverlayProof> {
        let snapshot = cleanup_context.snapshot;
        // A globally purgeable row tombstone proves the delete overlay is no
        // longer protecting any transaction-visible row, independent of where
        // the row currently falls relative to the cold/hot pivot.
        if self
            .deletion_buffer()
            .delete_marker_is_globally_purgeable(row_id, snapshot.min_active_sts)
        {
            return Ok(DeleteOverlayProof::Obsolete);
        }
        // Full-scan cleanup only proves key obsolescence for persisted LWC
        // rows. Hot row pages require undo-chain checks, which transaction
        // index GC already performs while holding the row-page context.
        if row_id >= snapshot.pivot_row_id {
            return Ok(DeleteOverlayProof::NotProven);
        }
        // The captured column root can prove durable row absence or a cold key
        // mismatch only after it is older than every active snapshot. Otherwise
        // removing the overlay could expose this newer cold-root fact to a
        // transaction that still depends on the MemIndex delete marker.
        if snapshot.table_root_ts >= snapshot.min_active_sts {
            return Ok(DeleteOverlayProof::NotProven);
        }
        let Some(column_index) = cleanup_context.column_index else {
            return Ok(DeleteOverlayProof::NotProven);
        };
        let Some(row) = column_index.locate_and_resolve_row(row_id).await? else {
            return Ok(DeleteOverlayProof::Obsolete);
        };
        let values = self
            .cleanup_read_cold_index_values(cleanup_context, index_no, row)
            .await?;
        Ok(DeleteOverlayProof::ColdRowValues(values))
    }

    #[inline]
    async fn cleanup_read_cold_index_values(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_>,
        index_no: usize,
        row: ResolvedColumnRow,
    ) -> Result<Vec<Val>> {
        let index_spec = self
            .metadata()
            .index_specs
            .get(index_no)
            .ok_or(Error::InvalidState)?;
        let read_set = index_spec
            .index_cols
            .iter()
            .map(|key| key.col_no as usize)
            .collect::<Vec<_>>();
        let block = PersistedLwcBlock::load(
            self.file().file_kind(),
            self.file().sparse_file(),
            self.disk_pool(),
            cleanup_context.disk_pool_guard,
            row.block_id(),
        )
        .await?;
        if block.row_shape_fingerprint() != row.row_shape_fingerprint() {
            return Err(Error::block_corrupted(
                FileKind::TableFile,
                BlockKind::LwcBlock,
                row.block_id(),
                BlockCorruptionCause::InvalidPayload,
            ));
        }
        block.decode_row_values(self.metadata(), row.row_idx(), &read_set)
    }
}

#[inline]
async fn compare_delete_unique_cleanup_entry<P: crate::buffer::BufferPool>(
    index: &crate::index::UniqueSecondaryIndex<P>,
    index_pool_guard: &crate::buffer::PoolGuard,
    entry: &crate::index::UniqueMemIndexEntry,
    min_active_sts: TrxID,
) -> Result<CleanupDecision> {
    if index
        .compare_delete_mem_encoded_entry(
            index_pool_guard,
            &entry.encoded_key,
            entry.row_id,
            entry.deleted,
            min_active_sts,
        )
        .await?
    {
        Ok(CleanupDecision::Remove)
    } else {
        Ok(CleanupDecision::Retain)
    }
}

#[inline]
async fn compare_delete_non_unique_cleanup_entry<P: crate::buffer::BufferPool>(
    index: &crate::index::NonUniqueSecondaryIndex<P>,
    index_pool_guard: &crate::buffer::PoolGuard,
    entry: &crate::index::NonUniqueMemIndexEntry,
    min_active_sts: TrxID,
) -> Result<CleanupDecision> {
    if index
        .compare_delete_mem_encoded_entry(
            index_pool_guard,
            &entry.encoded_key,
            entry.deleted,
            min_active_sts,
        )
        .await?
    {
        Ok(CleanupDecision::Remove)
    } else {
        Ok(CleanupDecision::Retain)
    }
}
