use crate::error::{DataIntegrityError, ErrorKind, FatalError, Result};
use crate::id::{TableID, TrxID};
use crate::log::{
    discover_redo_log_files, next_redo_file_seq, obsolete_redo_log_files_below_marker,
};
use crate::recovery::stream::{
    RedoReplayPlanner, RedoRetentionSegment, RedoRetentionSegmentState, RedoSegmentCtsRange,
};
use crate::session::{RedoTruncationBlockerInfo, RedoTruncationOutcome};
use crate::table::{LiveTableRedoReplayFloor, TableRedoReplayFloor};
use crate::trx::sys::{CatalogRedoRetentionProgress, TransactionSystem};
use error_stack::Report;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind as IoErrorKind;

/// Dry-run redo truncation plan for the currently retained redo suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedoTruncationPlan {
    /// Durable first retained redo file sequence from `catalog.mtb`.
    pub(crate) first_retained_file_seq: u32,
    /// Catalog replay boundary from the current checkpoint snapshot.
    pub(crate) catalog_replay_start_ts: TrxID,
    /// Earliest replay boundary across catalog, live tables, and pending drops.
    pub(crate) global_floor: TrxID,
    /// Sealed retained prefix files eligible for future physical unlink.
    pub(crate) candidates: Vec<RedoTruncationCandidate>,
    /// First reasons that prevent candidate growth.
    pub(crate) blockers: Vec<RedoTruncationBlocker>,
}

/// One retained redo segment that is eligible for future truncation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RedoTruncationCandidate {
    /// Redo file sequence.
    pub(crate) file_seq: u32,
    /// Real redo CTS range, or `None` for a sealed empty file.
    pub(crate) redo_range: Option<RedoSegmentCtsRange>,
}

/// Reason a retained redo segment cannot yet be added to the truncation prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RedoTruncationBlocker {
    /// Catalog replay still needs a segment at or above this boundary.
    CatalogFloor {
        /// Catalog replay boundary.
        catalog_replay_start_ts: TrxID,
    },
    /// A resident live table still needs redo at this boundary.
    LiveTableFloor {
        /// User table id.
        table_id: TableID,
        /// Heap replay boundary copied from the table active root.
        heap_redo_start_ts: TrxID,
        /// Cold-delete replay boundary copied from the table active root.
        deletion_cutoff_ts: TrxID,
    },
    /// A pending dropped table still needs redo until catalog absence is durable.
    PendingDroppedTableFloor {
        /// Dropped user table id.
        table_id: TableID,
        /// Commit timestamp of the logical `DROP TABLE`.
        drop_cts: TrxID,
        /// Heap replay boundary copied before runtime destruction.
        heap_redo_start_ts: TrxID,
        /// Cold-delete replay boundary copied before runtime destruction.
        deletion_cutoff_ts: TrxID,
    },
    /// The retained prefix reached an unsealed file.
    UnsealedFile {
        /// Redo file sequence.
        file_seq: u32,
    },
}

/// Replay floor retained for a logically dropped table until catalog absence is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingDroppedTableRedoFloor {
    /// Dropped user table id.
    pub(crate) table_id: TableID,
    /// Commit timestamp of the logical `DROP TABLE`.
    pub(crate) drop_cts: TrxID,
    /// Heap replay boundary copied before runtime destruction.
    pub(crate) heap_redo_start_ts: TrxID,
    /// Cold-delete replay boundary copied before runtime destruction.
    pub(crate) deletion_cutoff_ts: TrxID,
}

impl PendingDroppedTableRedoFloor {
    /// Build a pending dropped-table replay floor from a copied table floor.
    #[inline]
    pub(crate) fn new(table_id: TableID, drop_cts: TrxID, floor: TableRedoReplayFloor) -> Self {
        Self {
            table_id,
            drop_cts,
            heap_redo_start_ts: floor.heap_redo_start_ts,
            deletion_cutoff_ts: floor.deletion_cutoff_ts,
        }
    }

    /// Earliest redo timestamp that may still affect the dropped table.
    #[inline]
    pub(crate) fn replay_start_ts(self) -> TrxID {
        self.heap_redo_start_ts.min(self.deletion_cutoff_ts)
    }
}

impl TransactionSystem {
    /// Compute a side-effect-free redo truncation plan.
    #[inline]
    pub(crate) fn plan_redo_truncation(&self) -> Result<RedoTruncationPlan> {
        let snapshot = self.catalog.storage.checkpoint_snapshot()?;
        let first_retained_file_seq = snapshot.meta.first_redo_log_seq;
        let catalog_replay_start_ts = snapshot.catalog_replay_start_ts;

        let live_table_floors = self.catalog.snapshot_live_table_redo_floors();
        let pending_dropped_table_floors = self
            .dropped_tables
            .lock()
            .snapshot_pending_redo_floors(catalog_replay_start_ts);

        let file_prefix = self.config.file_prefix()?;
        let discovered = discover_redo_log_files(&file_prefix, first_retained_file_seq, false)?;
        let segments = RedoReplayPlanner::new(discovered).plan_retention_segments()?;
        let catalog_safe_segments = catalog_safe_segments_for_plan(
            self.catalog_redo_retention_progress(),
            first_retained_file_seq,
            catalog_replay_start_ts,
            &segments,
        );

        build_redo_truncation_plan(
            first_retained_file_seq,
            catalog_replay_start_ts,
            live_table_floors,
            pending_dropped_table_floors,
            segments,
            catalog_safe_segments,
        )
    }

    /// Advance the durable redo retention marker and unlink obsolete redo files.
    pub(crate) async fn truncate_redo_log(&self) -> Result<RedoTruncationOutcome> {
        let _redo_retention_lease = self.begin_redo_retention().await;
        let plan = self.plan_redo_truncation()?;
        let previous_first_retained_file_seq = plan.first_retained_file_seq;
        let target_marker = match plan.candidates.last() {
            Some(candidate) => next_redo_file_seq(candidate.file_seq)?,
            None => previous_first_retained_file_seq,
        };
        let advanced_files = plan.candidates.len();
        let new_first_retained_file_seq = if target_marker > previous_first_retained_file_seq {
            match self
                .catalog
                .storage
                .publish_first_redo_log_seq(target_marker)
                .await
            {
                Ok(marker) => marker,
                Err(err) if err.kind() == ErrorKind::Io => {
                    return Err(self.poison_storage(FatalError::CheckpointWrite).into());
                }
                Err(err) => return Err(err),
            }
        } else {
            previous_first_retained_file_seq
        };

        let file_prefix = self.config.file_prefix()?;
        let cleanup = cleanup_obsolete_redo_files(&file_prefix, new_first_retained_file_seq)?;

        Ok(RedoTruncationOutcome {
            previous_first_retained_file_seq,
            new_first_retained_file_seq,
            advanced_files,
            removed_files: cleanup.removed_files,
            already_missing_files: cleanup.already_missing_files,
            failed_unlink_files: cleanup.failed_unlink_files,
            blockers: plan
                .blockers
                .into_iter()
                .map(public_redo_truncation_blocker)
                .collect(),
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RedoCleanupCounts {
    removed_files: usize,
    already_missing_files: usize,
    failed_unlink_files: usize,
}

fn cleanup_obsolete_redo_files(
    file_prefix: &str,
    first_retained_file_seq: u32,
) -> Result<RedoCleanupCounts> {
    let mut counts = RedoCleanupCounts::default();
    for descriptor in obsolete_redo_log_files_below_marker(file_prefix, first_retained_file_seq)? {
        debug_assert!(descriptor.file_seq < first_retained_file_seq);
        #[cfg(test)]
        tests::run_redo_cleanup_before_unlink_hook(descriptor.file_seq, &descriptor.path);
        match fs::remove_file(&descriptor.path) {
            Ok(()) => counts.removed_files = counts.removed_files.saturating_add(1),
            Err(err) if err.kind() == IoErrorKind::NotFound => {
                counts.already_missing_files = counts.already_missing_files.saturating_add(1);
            }
            Err(_) => {
                counts.failed_unlink_files = counts.failed_unlink_files.saturating_add(1);
            }
        }
    }
    Ok(counts)
}

#[inline]
fn public_redo_truncation_blocker(blocker: RedoTruncationBlocker) -> RedoTruncationBlockerInfo {
    match blocker {
        RedoTruncationBlocker::CatalogFloor {
            catalog_replay_start_ts,
        } => RedoTruncationBlockerInfo::CatalogFloor {
            catalog_replay_start_ts,
        },
        RedoTruncationBlocker::LiveTableFloor {
            table_id,
            heap_redo_start_ts,
            deletion_cutoff_ts,
        } => RedoTruncationBlockerInfo::LiveTableFloor {
            table_id,
            heap_redo_start_ts,
            deletion_cutoff_ts,
        },
        RedoTruncationBlocker::PendingDroppedTableFloor {
            table_id,
            drop_cts,
            heap_redo_start_ts,
            deletion_cutoff_ts,
        } => RedoTruncationBlockerInfo::PendingDroppedTableFloor {
            table_id,
            drop_cts,
            heap_redo_start_ts,
            deletion_cutoff_ts,
        },
        RedoTruncationBlocker::UnsealedFile { file_seq } => {
            RedoTruncationBlockerInfo::UnsealedFile { file_seq }
        }
    }
}

#[inline]
fn build_redo_truncation_plan(
    first_retained_file_seq: u32,
    catalog_replay_start_ts: TrxID,
    live_table_floors: Vec<LiveTableRedoReplayFloor>,
    pending_dropped_table_floors: Vec<PendingDroppedTableRedoFloor>,
    segments: Vec<RedoRetentionSegment>,
    catalog_safe_segments: BTreeMap<u32, Option<RedoSegmentCtsRange>>,
) -> Result<RedoTruncationPlan> {
    let global_floor = global_floor(
        catalog_replay_start_ts,
        &live_table_floors,
        &pending_dropped_table_floors,
    );
    let mut candidates = Vec::new();
    let mut blockers = Vec::new();
    let mut expected_file_seq = Some(first_retained_file_seq);

    for segment in segments {
        if expected_file_seq != Some(segment.file_seq) {
            return Err(Report::new(DataIntegrityError::RedoLogSequenceGap)
                .attach(format!(
                    "retention planner observed non-contiguous retained redo sequence: expected={:?}, actual={:08x}",
                    expected_file_seq.map(|seq| format!("{seq:08x}")),
                    segment.file_seq
                ))
                .into());
        }
        expected_file_seq = segment.file_seq.checked_add(1);

        match segment.state {
            RedoRetentionSegmentState::Unsealed => {
                blockers.push(RedoTruncationBlocker::UnsealedFile {
                    file_seq: segment.file_seq,
                });
                break;
            }
            RedoRetentionSegmentState::SealedEmpty => {
                candidates.push(RedoTruncationCandidate {
                    file_seq: segment.file_seq,
                    redo_range: None,
                });
            }
            RedoRetentionSegmentState::SealedNonEmpty(redo_range) => {
                if catalog_safe_segments.get(&segment.file_seq) != Some(&Some(redo_range)) {
                    blockers.push(RedoTruncationBlocker::CatalogFloor {
                        catalog_replay_start_ts,
                    });
                    break;
                }
                if redo_range.max_cts < global_floor {
                    candidates.push(RedoTruncationCandidate {
                        file_seq: segment.file_seq,
                        redo_range: Some(redo_range),
                    });
                    continue;
                }
                push_floor_blockers(
                    &mut blockers,
                    catalog_replay_start_ts,
                    global_floor,
                    &live_table_floors,
                    &pending_dropped_table_floors,
                );
                break;
            }
        }
    }

    Ok(RedoTruncationPlan {
        first_retained_file_seq,
        catalog_replay_start_ts,
        global_floor,
        candidates,
        blockers,
    })
}

#[inline]
fn catalog_safe_segments_for_plan(
    cached: Option<CatalogRedoRetentionProgress>,
    first_retained_file_seq: u32,
    catalog_replay_start_ts: TrxID,
    segments: &[RedoRetentionSegment],
) -> BTreeMap<u32, Option<RedoSegmentCtsRange>> {
    if let Some(progress) = cached
        && progress.first_retained_file_seq == first_retained_file_seq
        && progress.catalog_replay_start_ts == catalog_replay_start_ts
    {
        let mut safe = progress
            .segments
            .into_iter()
            .map(|segment| (segment.file_seq, segment.redo_range))
            .collect::<BTreeMap<_, _>>();
        add_sealed_empty_segments(&mut safe, segments);
        return safe;
    }
    fallback_catalog_safe_segments(catalog_replay_start_ts, segments)
}

#[inline]
fn fallback_catalog_safe_segments(
    catalog_replay_start_ts: TrxID,
    segments: &[RedoRetentionSegment],
) -> BTreeMap<u32, Option<RedoSegmentCtsRange>> {
    let mut safe = BTreeMap::new();
    for segment in segments {
        match segment.state {
            RedoRetentionSegmentState::SealedEmpty => {
                safe.insert(segment.file_seq, None);
            }
            RedoRetentionSegmentState::SealedNonEmpty(range)
                if range.max_cts < catalog_replay_start_ts =>
            {
                safe.insert(segment.file_seq, Some(range));
            }
            RedoRetentionSegmentState::SealedNonEmpty(_) | RedoRetentionSegmentState::Unsealed => {}
        }
    }
    safe
}

#[inline]
fn add_sealed_empty_segments(
    safe: &mut BTreeMap<u32, Option<RedoSegmentCtsRange>>,
    segments: &[RedoRetentionSegment],
) {
    for segment in segments {
        if segment.state == RedoRetentionSegmentState::SealedEmpty {
            safe.insert(segment.file_seq, None);
        }
    }
}

#[inline]
fn global_floor(
    catalog_replay_start_ts: TrxID,
    live_table_floors: &[LiveTableRedoReplayFloor],
    pending_dropped_table_floors: &[PendingDroppedTableRedoFloor],
) -> TrxID {
    let mut floor = catalog_replay_start_ts;
    for table_floor in live_table_floors {
        floor = floor.min(table_floor.floor.replay_start_ts());
    }
    for dropped_floor in pending_dropped_table_floors {
        floor = floor.min(dropped_floor.replay_start_ts());
    }
    floor
}

fn push_floor_blockers(
    blockers: &mut Vec<RedoTruncationBlocker>,
    catalog_replay_start_ts: TrxID,
    global_floor: TrxID,
    live_table_floors: &[LiveTableRedoReplayFloor],
    pending_dropped_table_floors: &[PendingDroppedTableRedoFloor],
) {
    if catalog_replay_start_ts == global_floor {
        blockers.push(RedoTruncationBlocker::CatalogFloor {
            catalog_replay_start_ts,
        });
    }

    let mut live_seen = BTreeSet::new();
    for table_floor in live_table_floors {
        if table_floor.floor.replay_start_ts() == global_floor
            && live_seen.insert(table_floor.table_id)
        {
            blockers.push(RedoTruncationBlocker::LiveTableFloor {
                table_id: table_floor.table_id,
                heap_redo_start_ts: table_floor.floor.heap_redo_start_ts,
                deletion_cutoff_ts: table_floor.floor.deletion_cutoff_ts,
            });
        }
    }

    let mut dropped_seen = BTreeSet::new();
    for dropped_floor in pending_dropped_table_floors {
        let key = (dropped_floor.table_id, dropped_floor.drop_cts);
        if dropped_floor.replay_start_ts() == global_floor && dropped_seen.insert(key) {
            blockers.push(RedoTruncationBlocker::PendingDroppedTableFloor {
                table_id: dropped_floor.table_id,
                drop_cts: dropped_floor.drop_cts,
                heap_redo_start_ts: dropped_floor.heap_redo_start_ts,
                deletion_cutoff_ts: dropped_floor.deletion_cutoff_ts,
            });
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        PendingDroppedTableRedoFloor, RedoTruncationBlocker, RedoTruncationCandidate,
        build_redo_truncation_plan, catalog_safe_segments_for_plan,
    };
    use crate::id::{TableID, TrxID};
    use crate::recovery::stream::{
        CatalogSafeRedoSegment, RedoRetentionSegment, RedoRetentionSegmentState,
        RedoSegmentCtsRange,
    };
    use crate::table::{LiveTableRedoReplayFloor, TableRedoReplayFloor};
    use crate::trx::sys::CatalogRedoRetentionProgress;
    use parking_lot::Mutex;
    use std::path::Path;
    use std::sync::{Arc, OnceLock};

    type BeforeUnlinkHook = Arc<dyn Fn(u32, &Path) + Send + Sync + 'static>;

    fn before_unlink_hook_slot() -> &'static Mutex<Option<BeforeUnlinkHook>> {
        static HOOK: OnceLock<Mutex<Option<BeforeUnlinkHook>>> = OnceLock::new();
        HOOK.get_or_init(|| Mutex::new(None))
    }

    /// Guard that restores the previous redo cleanup before-unlink hook on drop.
    pub(crate) struct RedoCleanupBeforeUnlinkHookGuard {
        previous: Option<BeforeUnlinkHook>,
    }

    impl Drop for RedoCleanupBeforeUnlinkHookGuard {
        #[inline]
        fn drop(&mut self) {
            *before_unlink_hook_slot().lock() = self.previous.take();
        }
    }

    /// Install a hook invoked after obsolete redo discovery and before unlink.
    #[inline]
    pub(crate) fn install_redo_cleanup_before_unlink_hook(
        hook: BeforeUnlinkHook,
    ) -> RedoCleanupBeforeUnlinkHookGuard {
        let mut slot = before_unlink_hook_slot().lock();
        let previous = slot.replace(hook);
        RedoCleanupBeforeUnlinkHookGuard { previous }
    }

    #[inline]
    pub(crate) fn run_redo_cleanup_before_unlink_hook(file_seq: u32, path: &Path) {
        let hook = before_unlink_hook_slot().lock().clone();
        if let Some(hook) = hook {
            hook(file_seq, path);
        }
    }

    #[inline]
    fn catalog_safe_segment(
        file_seq: u32,
        range: Option<(TrxID, TrxID)>,
    ) -> CatalogSafeRedoSegment {
        CatalogSafeRedoSegment {
            file_seq,
            redo_range: range.map(|(min_cts, max_cts)| RedoSegmentCtsRange { min_cts, max_cts }),
        }
    }

    fn table_floor(
        table_id: u64,
        heap_redo_start_ts: u64,
        deletion_cutoff_ts: u64,
    ) -> LiveTableRedoReplayFloor {
        LiveTableRedoReplayFloor {
            table_id: TableID::new(table_id),
            floor: TableRedoReplayFloor {
                heap_redo_start_ts: TrxID::new(heap_redo_start_ts),
                deletion_cutoff_ts: TrxID::new(deletion_cutoff_ts),
            },
        }
    }

    fn dropped_floor(
        table_id: u64,
        drop_cts: u64,
        heap_redo_start_ts: u64,
        deletion_cutoff_ts: u64,
    ) -> PendingDroppedTableRedoFloor {
        PendingDroppedTableRedoFloor {
            table_id: TableID::new(table_id),
            drop_cts: TrxID::new(drop_cts),
            heap_redo_start_ts: TrxID::new(heap_redo_start_ts),
            deletion_cutoff_ts: TrxID::new(deletion_cutoff_ts),
        }
    }

    fn sealed_empty(file_seq: u32) -> RedoRetentionSegment {
        RedoRetentionSegment {
            file_seq,
            state: RedoRetentionSegmentState::SealedEmpty,
        }
    }

    fn sealed_non_empty(file_seq: u32, min_cts: u64, max_cts: u64) -> RedoRetentionSegment {
        RedoRetentionSegment {
            file_seq,
            state: RedoRetentionSegmentState::SealedNonEmpty(RedoSegmentCtsRange {
                min_cts: TrxID::new(min_cts),
                max_cts: TrxID::new(max_cts),
            }),
        }
    }

    fn unsealed(file_seq: u32) -> RedoRetentionSegment {
        RedoRetentionSegment {
            file_seq,
            state: RedoRetentionSegmentState::Unsealed,
        }
    }

    fn plan_with_fallback(
        catalog_replay_start_ts: u64,
        live_table_floors: Vec<LiveTableRedoReplayFloor>,
        pending_dropped_table_floors: Vec<PendingDroppedTableRedoFloor>,
        segments: Vec<RedoRetentionSegment>,
    ) -> super::RedoTruncationPlan {
        let catalog_replay_start_ts = TrxID::new(catalog_replay_start_ts);
        let safe = catalog_safe_segments_for_plan(None, 0, catalog_replay_start_ts, &segments);
        build_redo_truncation_plan(
            0,
            catalog_replay_start_ts,
            live_table_floors,
            pending_dropped_table_floors,
            segments,
            safe,
        )
        .unwrap()
    }

    #[test]
    fn sealed_empty_prefix_is_candidate() {
        let plan = plan_with_fallback(10, vec![], vec![], vec![sealed_empty(0), unsealed(1)]);

        assert_eq!(
            plan.candidates,
            vec![RedoTruncationCandidate {
                file_seq: 0,
                redo_range: None
            }]
        );
        assert_eq!(
            plan.blockers,
            vec![RedoTruncationBlocker::UnsealedFile { file_seq: 1 }]
        );
    }

    #[test]
    fn sealed_non_empty_below_global_floor_is_candidate() {
        let plan = plan_with_fallback(
            20,
            vec![table_floor(100, 15, 18)],
            vec![],
            vec![sealed_non_empty(0, 2, 14), unsealed(1)],
        );

        assert_eq!(
            plan.candidates,
            vec![RedoTruncationCandidate {
                file_seq: 0,
                redo_range: Some(RedoSegmentCtsRange {
                    min_cts: TrxID::new(2),
                    max_cts: TrxID::new(14)
                })
            }]
        );
    }

    #[test]
    fn sealed_non_empty_at_global_floor_reports_floor_blocker() {
        let plan = plan_with_fallback(
            20,
            vec![table_floor(100, 15, 18)],
            vec![],
            vec![sealed_non_empty(0, 2, 15)],
        );

        assert!(plan.candidates.is_empty());
        assert_eq!(
            plan.blockers,
            vec![RedoTruncationBlocker::LiveTableFloor {
                table_id: TableID::new(100),
                heap_redo_start_ts: TrxID::new(15),
                deletion_cutoff_ts: TrxID::new(18)
            }]
        );
    }

    #[test]
    fn sealed_non_empty_above_global_floor_reports_all_tied_floor_blockers() {
        let plan = plan_with_fallback(
            12,
            vec![table_floor(100, 7, 20), table_floor(101, 9, 7)],
            vec![dropped_floor(200, 30, 7, 11)],
            vec![sealed_non_empty(0, 2, 9)],
        );

        assert_eq!(plan.global_floor, TrxID::new(7));
        assert_eq!(
            plan.blockers,
            vec![
                RedoTruncationBlocker::LiveTableFloor {
                    table_id: TableID::new(100),
                    heap_redo_start_ts: TrxID::new(7),
                    deletion_cutoff_ts: TrxID::new(20)
                },
                RedoTruncationBlocker::LiveTableFloor {
                    table_id: TableID::new(101),
                    heap_redo_start_ts: TrxID::new(9),
                    deletion_cutoff_ts: TrxID::new(7)
                },
                RedoTruncationBlocker::PendingDroppedTableFloor {
                    table_id: TableID::new(200),
                    drop_cts: TrxID::new(30),
                    heap_redo_start_ts: TrxID::new(7),
                    deletion_cutoff_ts: TrxID::new(11)
                },
            ]
        );
    }

    #[test]
    fn catalog_floor_blocks_catalog_unsafe_segment() {
        let plan = plan_with_fallback(10, vec![], vec![], vec![sealed_non_empty(0, 8, 12)]);

        assert!(plan.candidates.is_empty());
        assert_eq!(
            plan.blockers,
            vec![RedoTruncationBlocker::CatalogFloor {
                catalog_replay_start_ts: TrxID::new(10)
            }]
        );
    }

    #[test]
    fn valid_cached_catalog_progress_is_used() {
        let segments = vec![sealed_non_empty(0, 8, 12)];
        let safe = catalog_safe_segments_for_plan(
            Some(CatalogRedoRetentionProgress {
                first_retained_file_seq: 0,
                catalog_replay_start_ts: TrxID::new(10),
                segments: vec![catalog_safe_segment(
                    0,
                    Some((TrxID::new(8), TrxID::new(12))),
                )],
            }),
            0,
            TrxID::new(10),
            &segments,
        );

        let plan = build_redo_truncation_plan(
            0,
            TrxID::new(10),
            vec![table_floor(100, 9, 20)],
            vec![],
            segments,
            safe,
        )
        .unwrap();

        assert_eq!(
            plan.blockers,
            vec![RedoTruncationBlocker::LiveTableFloor {
                table_id: TableID::new(100),
                heap_redo_start_ts: TrxID::new(9),
                deletion_cutoff_ts: TrxID::new(20)
            }]
        );
    }

    #[test]
    fn stale_cached_catalog_progress_is_ignored() {
        let segments = vec![sealed_non_empty(0, 8, 12)];
        let old_marker_safe = catalog_safe_segments_for_plan(
            Some(CatalogRedoRetentionProgress {
                first_retained_file_seq: 1,
                catalog_replay_start_ts: TrxID::new(10),
                segments: vec![catalog_safe_segment(
                    0,
                    Some((TrxID::new(8), TrxID::new(12))),
                )],
            }),
            0,
            TrxID::new(10),
            &segments,
        );
        let old_boundary_safe = catalog_safe_segments_for_plan(
            Some(CatalogRedoRetentionProgress {
                first_retained_file_seq: 0,
                catalog_replay_start_ts: TrxID::new(11),
                segments: vec![catalog_safe_segment(
                    0,
                    Some((TrxID::new(8), TrxID::new(12))),
                )],
            }),
            0,
            TrxID::new(10),
            &segments,
        );

        assert!(old_marker_safe.is_empty());
        assert!(old_boundary_safe.is_empty());
    }

    #[test]
    fn unsealed_segment_blocks_prefix_growth() {
        let plan = plan_with_fallback(10, vec![], vec![], vec![unsealed(0)]);

        assert!(plan.candidates.is_empty());
        assert_eq!(
            plan.blockers,
            vec![RedoTruncationBlocker::UnsealedFile { file_seq: 0 }]
        );
    }
}
