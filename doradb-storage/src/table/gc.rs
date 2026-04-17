#![cfg_attr(not(test), allow(dead_code))]

use super::Table;
use crate::buffer::{EvictableBufferPool, PoolGuard, PoolGuards};
use crate::error::{Error, Result};
use crate::file::BlockID;
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::index::{
    ColumnBlockIndex, NonUniqueSecondaryIndex, SecondaryIndex, UniqueSecondaryIndex,
};
use crate::row::RowID;
use crate::session::Session;
use crate::trx::TrxID;

/// Aggregate result for a full-scan user-table secondary MemIndex cleanup pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SecondaryMemIndexCleanupStats {
    /// One row per secondary index scanned by this pass.
    pub(crate) indexes: Vec<SecondaryMemIndexCleanupIndexStats>,
}

/// Cleanup result for one secondary MemIndex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecondaryMemIndexCleanupIndexStats {
    /// Table-local secondary-index number.
    pub(crate) index_no: usize,
    /// Whether the scanned index is unique.
    pub(crate) unique: bool,
    /// Number of MemIndex entries scanned.
    pub(crate) scanned: usize,
    /// Number of MemIndex entries physically removed.
    pub(crate) removed: usize,
    /// Number of MemIndex entries intentionally retained.
    pub(crate) retained: usize,
    /// Number of per-entry cleanup errors that caused retention.
    pub(crate) errors: usize,
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
    column_index: Option<&'a ColumnBlockIndex<'a>>,
    index_pool_guard: &'a PoolGuard,
    disk_pool_guard: &'a PoolGuard,
}

enum CleanupDecision {
    Remove,
    Retain,
    Error,
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
            errors: 0,
        }
    }

    #[inline]
    fn record(&mut self, decision: CleanupDecision) {
        self.scanned += 1;
        match decision {
            CleanupDecision::Remove => self.removed += 1,
            CleanupDecision::Retain => self.retained += 1,
            CleanupDecision::Error => {
                self.retained += 1;
                self.errors += 1;
            }
        }
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
        let trx_sys = session.engine().trx_sys.clone();
        loop {
            let trx = session.try_begin_trx()?.ok_or(Error::NotSupported(
                "secondary MemIndex cleanup requires idle session",
            ))?;
            let cleanup_sts = trx.sts;
            let min_active_sts = trx_sys.calc_min_active_sts_for_gc();
            let snapshot = self.capture_mem_index_cleanup_snapshot(min_active_sts);
            if !snapshot.is_visible_to(cleanup_sts) {
                trx_sys.rollback(trx).await?;
                continue;
            }

            let cleanup_res = self
                .cleanup_secondary_mem_indexes_at_snapshot(session.pool_guards(), &snapshot)
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
        // Keep this as an owned field snapshot until checkpoint old-root
        // retention is wired through transaction GC. The current checkpoint
        // path can drop the swapped `OldRoot` immediately after publication,
        // so holding `&ActiveRoot` across async cleanup is not lifetime-safe.
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
    ) -> Result<SecondaryMemIndexCleanupStats> {
        debug_assert!(snapshot.deletion_cutoff_ts <= snapshot.table_root_ts);

        let column_index = self.cleanup_column_index(guards, snapshot);
        let index_pool_guard = self.index_pool_guard(guards);
        let disk_pool_guard = guards.disk_guard();
        let cleanup_context = MemIndexCleanupContext {
            snapshot,
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
                        unique,
                        secondary_root,
                        &mut index_stats,
                    )
                    .await?;
                }
                SecondaryIndex::NonUnique(non_unique) => {
                    self.cleanup_non_unique_secondary_mem_index(
                        &cleanup_context,
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
        index: &UniqueSecondaryIndex<EvictableBufferPool>,
        secondary_root: BlockID,
        stats: &mut SecondaryMemIndexCleanupIndexStats,
    ) -> Result<()> {
        let disk = index
            .disk()
            .open_unique_at(secondary_root, cleanup_context.disk_pool_guard)?;
        for entry in index
            .scan_mem_entries(cleanup_context.index_pool_guard)
            .await?
        {
            let decision = if entry.deleted {
                match disk.lookup_encoded(&entry.encoded_key).await {
                    Ok(Some(row_id)) if row_id == entry.row_id => CleanupDecision::Retain,
                    Ok(_) => {
                        if self
                            .cleanup_delete_overlay_is_proven(
                                cleanup_context.snapshot,
                                cleanup_context.column_index,
                                entry.row_id,
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
                    }
                    Err(_) => CleanupDecision::Error,
                }
            } else if entry.row_id >= cleanup_context.snapshot.pivot_row_id {
                CleanupDecision::Retain
            } else {
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
                    Err(_) => CleanupDecision::Error,
                }
            };
            stats.record(decision);
        }
        Ok(())
    }

    #[inline]
    async fn cleanup_non_unique_secondary_mem_index(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_>,
        index: &NonUniqueSecondaryIndex<EvictableBufferPool>,
        secondary_root: BlockID,
        stats: &mut SecondaryMemIndexCleanupIndexStats,
    ) -> Result<()> {
        let disk = index
            .disk()
            .open_non_unique_at(secondary_root, cleanup_context.disk_pool_guard)?;
        for entry in index
            .scan_mem_entries(cleanup_context.index_pool_guard)
            .await?
        {
            let decision = if entry.deleted {
                match disk.contains_exact_encoded(&entry.encoded_key).await {
                    Ok(true) => CleanupDecision::Retain,
                    Ok(false) => {
                        if self
                            .cleanup_delete_overlay_is_proven(
                                cleanup_context.snapshot,
                                cleanup_context.column_index,
                                entry.row_id,
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
                    }
                    Err(_) => CleanupDecision::Error,
                }
            } else if entry.row_id >= cleanup_context.snapshot.pivot_row_id {
                CleanupDecision::Retain
            } else {
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
                    Err(_) => CleanupDecision::Error,
                }
            };
            stats.record(decision);
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
    async fn cleanup_delete_overlay_is_proven(
        &self,
        snapshot: &MemIndexCleanupSnapshot,
        column_index: Option<&ColumnBlockIndex<'_>>,
        row_id: RowID,
    ) -> Result<bool> {
        if self
            .deletion_buffer()
            .delete_marker_is_globally_purgeable(row_id, snapshot.min_active_sts)
        {
            return Ok(true);
        }
        self.cleanup_durable_row_absence_is_proven(snapshot, column_index, row_id)
            .await
    }

    #[inline]
    async fn cleanup_durable_row_absence_is_proven(
        &self,
        snapshot: &MemIndexCleanupSnapshot,
        column_index: Option<&ColumnBlockIndex<'_>>,
        row_id: RowID,
    ) -> Result<bool> {
        if row_id >= snapshot.pivot_row_id || snapshot.table_root_ts >= snapshot.min_active_sts {
            return Ok(false);
        }
        let Some(column_index) = column_index else {
            return Ok(false);
        };
        Ok(column_index.locate_and_resolve_row(row_id).await?.is_none())
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
