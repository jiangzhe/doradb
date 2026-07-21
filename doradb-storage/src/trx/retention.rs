use crate::catalog::CatalogCheckpointOutcome;
use crate::error::{DataIntegrityError, FatalError, IoError, Result};
use crate::id::{TableID, TrxID};
use crate::log::{
    discover_redo_log_files, next_redo_file_seq, obsolete_redo_log_files_below_marker,
};
use crate::obs;
use crate::recovery::stream::{
    RedoReplayPlanner, RedoRetentionSegment, RedoRetentionSegmentState, RedoSegmentCtsRange,
};
use crate::session::{
    CatalogRedoMaintenanceOutcome, RedoTruncationBlockerInfo, RedoTruncationOutcome,
};
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
        let snapshot = self.catalog.storage.checkpoint_snapshot();
        let first_retained_file_seq = snapshot.meta.first_redo_log_seq;
        let catalog_replay_start_ts = snapshot.catalog_replay_start_ts;

        let (live_table_floors, pending_dropped_table_floors) = self
            .catalog
            .snapshot_user_table_redo_floors(catalog_replay_start_ts);

        self.plan_redo_truncation_with_floors(
            first_retained_file_seq,
            catalog_replay_start_ts,
            live_table_floors,
            pending_dropped_table_floors,
            self.catalog_redo_retention_progress(),
        )
    }

    fn plan_redo_truncation_with_floors(
        &self,
        first_retained_file_seq: u32,
        catalog_replay_start_ts: TrxID,
        live_table_floors: Vec<LiveTableRedoReplayFloor>,
        pending_dropped_table_floors: Vec<PendingDroppedTableRedoFloor>,
        catalog_redo_retention_progress: Option<CatalogRedoRetentionProgress>,
    ) -> Result<RedoTruncationPlan> {
        let file_prefix = self.config.file_prefix()?;
        let discovered = discover_redo_log_files(&file_prefix, first_retained_file_seq, false)?;
        let segments = RedoReplayPlanner::new(discovered).plan_retention_segments()?;
        let catalog_safe_segments = catalog_safe_segments_for_plan(
            catalog_redo_retention_progress,
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
    ///
    /// Lock ordering matches catalog checkpoint: acquire the catalog checkpoint
    /// lease before the redo-retention lease. The catalog lease protects the
    /// `catalog.mtb` root fork used to publish `first_redo_log_seq`, while the
    /// redo-retention lease protects the retained redo suffix, catalog-safe
    /// progress cache, and cleanup below the marker. They are separate because
    /// the marker is catalog bootstrap metadata, but unlink races are about the
    /// redo file family rather than catalog metadata shape.
    pub(crate) async fn truncate_redo_log(&self) -> Result<RedoTruncationOutcome> {
        let catalog_checkpoint_lease = self.catalog.begin_checkpoint().await;
        let _redo_retention_lease = self.begin_redo_retention().await;
        // Session admission happens before the async gate waits above. Recheck
        // here so poison published while truncation was queued prevents marker
        // publication and physical redo cleanup.
        self.poisoner.ensure_healthy()?;
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
                Err(err) if err.downcast_ref::<IoError>().is_some() => {
                    let report = err
                        .change_context(FatalError::CheckpointWrite)
                        .attach("publish redo retention marker IO failure");
                    obs::error!(
                        "event=engine_poison component=redo_retention action=poison result=error error={:?}",
                        report
                    );
                    return Err(self.poisoner.poison(report).into_report().into());
                }
                Err(err) => return Err(err.into()),
            }
        } else {
            previous_first_retained_file_seq
        };

        drop(catalog_checkpoint_lease);
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

    /// Run catalog checkpoint and redo truncation from one gated retention observation.
    pub(crate) async fn checkpoint_catalog_and_truncate_redo_log(
        &self,
    ) -> Result<CatalogRedoMaintenanceOutcome> {
        // 1. Acquire gates in the same order as standalone truncation. The
        // catalog gate protects the `catalog.mtb` root writer, while the redo
        // retention gate stays held until obsolete-file cleanup is finished.
        let catalog_checkpoint_lease = self.catalog.begin_checkpoint().await;
        let _redo_retention_lease = self.begin_redo_retention().await;
        // Session admission happened before these async waits. Recheck after
        // the gates so queued storage poison cannot publish metadata or unlink
        // redo files.
        self.poisoner.ensure_healthy()?;

        // 2. Scan exactly one catalog checkpoint batch and prepare, but do not
        // commit, the projected catalog root. Keeping the root mutable lets the
        // same publication also carry an advanced first-retained redo marker.
        let scan_cfg = self.catalog_checkpoint_scan_config()?;
        let batch = self
            .catalog
            .scan_checkpoint_batch(self.persisted_watermark_cts(), scan_cfg)
            .await?;
        let checkpoint_progress = batch.redo_retention_progress();
        let mut prepared = self.catalog.prepare_checkpoint_batch(batch).await?;
        let checkpoint_will_publish = prepared.will_publish();

        // 3. Build truncation inputs from the projected post-checkpoint state.
        // This is the combined command's main difference from standalone
        // truncation: newly checkpointed silent watermark rows may participate
        // in planning before the root has been committed.
        //
        // Dropped-table floors are filtered by the same projected catalog
        // replay boundary. If this checkpoint would make a table's catalog
        // absence durable (`projected_catalog_replay_start_ts > drop_cts`),
        // that table no longer protects old redo for this combined plan even
        // though purge may remove the retained dropped-floor entry later. If
        // the projected boundary is still at or before `drop_cts`, the dropped
        // table remains in `pending_dropped_table_floors` and can conservatively
        // block marker advancement.
        let projected_catalog_replay_start_ts = prepared.catalog_replay_start_ts();
        debug_assert!(
            checkpoint_progress.as_ref().is_none_or(|progress| {
                progress.catalog_replay_start_ts == projected_catalog_replay_start_ts
            }),
            "catalog checkpoint progress must match projected catalog replay boundary"
        );
        // Use the silent watermark overlay projected by the prepared catalog
        // checkpoint. For a publishable checkpoint this is loaded from the
        // not-yet-committed catalog roots; for a no-op it is the current
        // checkpoint-durable cache. Planning with this overlay makes the redo
        // truncation decision match the table replay floors that will be true
        // after this combined maintenance command commits.
        let projected_silent_watermarks = prepared.checkpointed_silent_watermarks();
        let (live_table_floors, pending_dropped_table_floors) = self
            .catalog
            .snapshot_user_table_redo_floors_with_silent_watermarks(
                projected_catalog_replay_start_ts,
                &projected_silent_watermarks,
            );
        let first_retained_file_seq = self
            .catalog
            .storage
            .checkpoint_snapshot()
            .meta
            .first_redo_log_seq;
        let projected_catalog_progress = projected_catalog_redo_retention_progress(
            checkpoint_progress.clone(),
            self.catalog_redo_retention_progress(),
            first_retained_file_seq,
            projected_catalog_replay_start_ts,
        );
        // 4. Plan truncation against the projected catalog boundary, projected
        // live-table floors, pending dropped-table floors, and the best
        // catalog-safe segment proof available from this scan plus cache.
        let plan = self.plan_redo_truncation_with_floors(
            first_retained_file_seq,
            projected_catalog_replay_start_ts,
            live_table_floors,
            pending_dropped_table_floors,
            projected_catalog_progress,
        )?;
        let previous_first_retained_file_seq = plan.first_retained_file_seq;
        let target_marker = match plan.candidates.last() {
            Some(candidate) => next_redo_file_seq(candidate.file_seq)?,
            None => previous_first_retained_file_seq,
        };
        let advanced_files = plan.candidates.len();

        let mut new_first_retained_file_seq = previous_first_retained_file_seq;
        // 5. Publish the durable metadata before unlink. If checkpoint metadata
        // is already prepared, fold marker advancement into that same
        // `catalog.mtb` root; otherwise use the marker-only publication path.
        let checkpoint_outcome = if checkpoint_will_publish {
            if target_marker > previous_first_retained_file_seq {
                let applied = prepared.apply_first_redo_log_seq(target_marker);
                debug_assert!(
                    applied,
                    "marker must monotonically advance when target_marker ({target_marker}) \
                     exceeds the durable marker ({previous_first_retained_file_seq})"
                );
                new_first_retained_file_seq = target_marker;
            }
            match prepared.commit(&self.catalog.storage).await {
                Ok(outcome) => outcome,
                Err(err) if err.downcast_ref::<IoError>().is_some() => {
                    let report = err
                        .change_context(FatalError::CheckpointWrite)
                        .attach("commit combined catalog checkpoint IO failure");
                    obs::error!(
                        "event=engine_poison component=redo_retention action=poison result=error error={:?}",
                        report
                    );
                    return Err(self.poisoner.poison(report).into_report().into());
                }
                Err(err) => return Err(err.into()),
            }
        } else {
            if target_marker > previous_first_retained_file_seq {
                new_first_retained_file_seq = match self
                    .catalog
                    .storage
                    .publish_first_redo_log_seq(target_marker)
                    .await
                {
                    Ok(marker) => marker,
                    Err(err) if err.downcast_ref::<IoError>().is_some() => {
                        let report = err
                            .change_context(FatalError::CheckpointWrite)
                            .attach("publish combined redo retention marker IO failure");
                        obs::error!(
                            "event=engine_poison component=redo_retention action=poison result=error error={:?}",
                            report
                        );
                        return Err(self.poisoner.poison(report).into_report().into());
                    }
                    Err(err) => return Err(err.into()),
                };
            }
            CatalogCheckpointOutcome::Noop
        };

        // 6. After a successful checkpoint publish, refresh in-memory
        // catalog-safe segment progress using the final marker. This prevents
        // later truncation planning from reusing segment proof below the new
        // retained suffix.
        if let CatalogCheckpointOutcome::Published {
            catalog_replay_start_ts,
        } = checkpoint_outcome
        {
            if let Some(progress) = catalog_progress_for_final_marker(
                checkpoint_progress,
                new_first_retained_file_seq,
                catalog_replay_start_ts,
            ) {
                self.record_catalog_redo_retention_progress(progress);
            }
            self.request_dropped_table_purge();
        }

        // 7. Release only the catalog gate before filesystem cleanup. The redo
        // retention lease remains held through cleanup so checkpoint scans
        // cannot race disappearing retained redo files.
        drop(catalog_checkpoint_lease);
        let file_prefix = self.config.file_prefix()?;
        let cleanup = cleanup_obsolete_redo_files(&file_prefix, new_first_retained_file_seq)?;

        // 8. Return both halves of the maintenance result. The redo outcome
        // reports marker advancement, retryable cleanup counts, and the
        // blockers from the projected plan.
        Ok(CatalogRedoMaintenanceOutcome {
            catalog_checkpoint: checkpoint_outcome,
            redo_truncation: RedoTruncationOutcome {
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
            },
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RedoCleanupCounts {
    removed_files: usize,
    already_missing_files: usize,
    failed_unlink_files: usize,
}

fn projected_catalog_redo_retention_progress(
    scanned: Option<CatalogRedoRetentionProgress>,
    cached: Option<CatalogRedoRetentionProgress>,
    first_retained_file_seq: u32,
    projected_catalog_replay_start_ts: TrxID,
) -> Option<CatalogRedoRetentionProgress> {
    // Merge catalog-safe segment proof into the exact view used by the
    // projected truncation plan. The map keeps file order deterministic and
    // lets freshly scanned proof replace cached proof for the same file.
    let mut segments = BTreeMap::new();
    // Cached progress is reusable only when it belongs to the same retained
    // redo suffix and was proven at or before the projected catalog boundary.
    // A future-boundary cache would be proof for a state this plan has not
    // published, so it must be ignored.
    if let Some(cached) = cached
        && cached.first_retained_file_seq == first_retained_file_seq
        && cached.catalog_replay_start_ts <= projected_catalog_replay_start_ts
    {
        for segment in cached.segments {
            // Never carry proof for files below the durable marker into the
            // retained suffix. Those files are already obsolete by marker
            // contract and should not affect planning for retained files.
            if segment.file_seq >= first_retained_file_seq {
                segments.insert(segment.file_seq, segment);
            }
        }
    }
    // The current scan is usable only when it exactly matches the projected
    // checkpoint boundary. If the checkpoint batch was a no-op or did not cover
    // this boundary, its segment proof cannot justify this projected plan.
    if let Some(scanned) = scanned
        && scanned.first_retained_file_seq == first_retained_file_seq
        && scanned.catalog_replay_start_ts == projected_catalog_replay_start_ts
    {
        for segment in scanned.segments {
            // Apply the same retained-suffix filter to scanned proof. This also
            // keeps the helper correct if a later caller supplies progress
            // collected before a marker advance.
            if segment.file_seq >= first_retained_file_seq {
                segments.insert(segment.file_seq, segment);
            }
        }
    }
    if segments.is_empty() {
        return None;
    }
    Some(CatalogRedoRetentionProgress {
        first_retained_file_seq,
        catalog_replay_start_ts: projected_catalog_replay_start_ts,
        segments: segments.into_values().collect(),
    })
}

fn catalog_progress_for_final_marker(
    progress: Option<CatalogRedoRetentionProgress>,
    first_retained_file_seq: u32,
    catalog_replay_start_ts: TrxID,
) -> Option<CatalogRedoRetentionProgress> {
    let mut progress = progress?;
    progress.first_retained_file_seq = first_retained_file_seq;
    progress.catalog_replay_start_ts = catalog_replay_start_ts;
    progress
        .segments
        .retain(|segment| segment.file_seq >= first_retained_file_seq);
    Some(progress)
}

fn cleanup_obsolete_redo_files(
    file_prefix: &str,
    first_retained_file_seq: u32,
) -> Result<RedoCleanupCounts> {
    let mut counts = RedoCleanupCounts::default();
    for descriptor in obsolete_redo_log_files_below_marker(file_prefix, first_retained_file_seq)? {
        debug_assert!(descriptor.seq < first_retained_file_seq);
        #[cfg(test)]
        tests::run_redo_cleanup_before_unlink_hook(descriptor.seq, &descriptor.path);
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
        // Seed from current sealed segment CTS ranges before overlaying cached
        // checkpoint-scan proof. Sealed non-empty ranges are immutable, so a
        // file whose max CTS is already below `catalog_replay_start_ts` is
        // catalog-safe even if the last checkpoint scan did not record it.
        // Cached proof still wins for matching file sequences below.
        let mut safe = fallback_catalog_safe_segments(catalog_replay_start_ts, segments);
        safe.extend(
            progress
                .segments
                .into_iter()
                .map(|segment| (segment.file_seq, segment.redo_range)),
        );
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
    use parking_lot::{Mutex, MutexGuard};
    use std::path::Path;
    use std::sync::{Arc, OnceLock, mpsc};
    use std::thread;
    use std::time::Duration;

    type BeforeUnlinkHook = Arc<dyn Fn(u32, &Path) + Send + Sync + 'static>;

    fn before_unlink_hook_slot() -> &'static Mutex<Option<BeforeUnlinkHook>> {
        static HOOK: OnceLock<Mutex<Option<BeforeUnlinkHook>>> = OnceLock::new();
        HOOK.get_or_init(|| Mutex::new(None))
    }

    fn before_unlink_hook_install_lock() -> &'static Mutex<()> {
        static INSTALL_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        INSTALL_LOCK.get_or_init(|| Mutex::new(()))
    }

    /// Guard that restores the previous redo cleanup before-unlink hook on drop.
    ///
    /// The process-wide install lock is held for the guard lifetime so parallel
    /// tests cannot overwrite each other's global hook state.
    pub(crate) struct RedoCleanupBeforeUnlinkHookGuard {
        previous: Option<BeforeUnlinkHook>,
        _install_guard: MutexGuard<'static, ()>,
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
        let install_guard = before_unlink_hook_install_lock().lock();
        let mut slot = before_unlink_hook_slot().lock();
        let previous = slot.replace(hook);
        RedoCleanupBeforeUnlinkHookGuard {
            previous,
            _install_guard: install_guard,
        }
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
    fn redo_cleanup_before_unlink_hook_installation_is_exclusive() {
        let first = install_redo_cleanup_before_unlink_hook(Arc::new(|_, _| {}));
        let (started_tx, started_rx) = mpsc::channel();
        let (installed_tx, installed_rx) = mpsc::channel();

        let installer = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _second = install_redo_cleanup_before_unlink_hook(Arc::new(|_, _| {}));
            installed_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            installed_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "second hook installer should wait for the first guard"
        );

        drop(first);
        installed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        installer.join().unwrap();
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
