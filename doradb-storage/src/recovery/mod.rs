//! Recovery is the process to recover all committed metadata and
//! data after database reboots.
//!
//! We need to recover:
//! 1. Catalog: table definition, index definition, etc.
//! 2. User data: rows in each table.
//! 3. Index.
//!
//! Catalog and user data are persisted via checkpoint and logging.
//! Index is recovered from user data.
//!
//! We separate all transactions into two kinds:
//! 1. DDL involved transactions
//! 2. DML-only transactions
mod decode;
mod dispatch;
mod packed;
mod resources;
mod row_state;
pub(crate) mod stream;
mod timeline;

use self::dispatch::ReplayDispatcher;
pub(crate) use self::dispatch::RowReplayCounts;
use crate::buffer::guard::PageGuard;
use crate::catalog::TableIndexMetadata;
use crate::catalog::{
    CatalogTable, IndexDdlKind, IndexDdlRootProof, IndexRef, ReplayVisibleIndexDdl,
    classify_index_ddl_root,
};
use crate::conf::RecoveryConfig;
use crate::error::{
    DataIntegrityError, DataIntegrityResult, IoResult, MultiDomainResultExt, RuntimeError,
    RuntimeOrFatalResult, RuntimeOrFatalResultExt, RuntimeResult,
};
use crate::id::{PageID, RowID, TableID, TrxID};
use crate::index::build::HotBuildSource;
use crate::log::redo::{DDLRedo, RowRedo, RowRedoKind, TableDML};
use crate::log::{RedoLogCreateMode, RedoLogFinalizer, next_redo_file_seq};
use crate::map::FastHashSet;
use crate::obs;
use crate::recovery::stream::{PlannedRedoRecovery, RecoveryLogStream};
use crate::stats::{RecoveryReport, recovery_add_count};
use crate::table::RowPageDescriptor;
use crate::table::{Table, TableRedoReplayFloor};
use crate::trx::MIN_SNAPSHOT_TS;
use decode::{DecodedGroup, DecodedRow, DecodedRowKind, DecodedTable, DecodedTrx, DecodedTrxKind};
#[cfg(test)]
pub(crate) use packed::{OwnedReplayOp, pack_test_ops};
pub(crate) use packed::{PackedPageBatch, ReplayKind, ReplayOp};
use std::time::Instant;
use stream::{RedoRecoveryRepairPolicy, RedoReplayPlanner, UnsealedSegmentTerminal};

use error_stack::{Report, ResultExt};
pub(crate) use resources::RecoveryResources;
pub(crate) use row_state::RowReplayState;
use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;
pub(crate) use timeline::{RecoveryTimeline, TableReplayBounds};

#[cfg(test)]
pub(crate) use tests::capture_hot_build_test_source;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UserTableRedoAction {
    Replay,
    SkipCheckpointCoveredUnknownTable,
}

/// Value-only outcome of successful recovery, before writable redo construction.
pub(crate) struct RecoveryOutcome {
    /// Highest recovered commit timestamp.
    pub(crate) max_recovered_cts: TrxID,
    /// Value-only writable redo startup policy.
    pub(crate) finalizer: RedoLogFinalizer,
    /// Completed coordinator measurements.
    pub(crate) report: RecoveryReport,
}

/// Recovery coordinator for checkpoint bootstrap, redo replay, final repair, and redo startup.
pub(crate) struct RecoveryCoordinator<'a> {
    report: RecoveryReport,
    /// Catalog, table files, and buffer-pool resources used by recovery.
    resources: RecoveryResources<'a>,
    /// Planner for the ordered redo-log stream.
    redo_planner: RedoReplayPlanner,
    /// Direct-IO read-ahead depth used by startup redo recovery.
    redo_read_depth: usize,
    /// Whether recovery/no-trx DML payload validation is disabled.
    recovery_disable_dml_validation: bool,
    /// Value-only finalizer for the writable redo log created after recovery.
    finalizer: RedoLogFinalizer,
    /// Replay cursors, per-table bounds, and recovered CTS watermark.
    timeline: RecoveryTimeline,
    /// Tables loaded from table-file metadata while catalog index DDL redo is pending.
    pending_index_ddl_reconciliations: FastHashSet<TableID>,
    /// Bounded page replay and retained insertion history.
    dispatcher: ReplayDispatcher,
}

impl<'a> RecoveryCoordinator<'a> {
    /// Create a recovery coordinator from prepared resources and redo startup state.
    #[inline]
    pub(crate) fn new(
        resources: RecoveryResources<'a>,
        redo_planner: RedoReplayPlanner,
        config: &RecoveryConfig,
        finalizer: RedoLogFinalizer,
    ) -> Self {
        let dispatcher = ReplayDispatcher::new(
            resources.thread_pool.clone(),
            resources.pool_guards.clone(),
            config,
        );
        RecoveryCoordinator {
            report: RecoveryReport::default(),
            resources,
            redo_planner,
            redo_read_depth: config.io_depth,
            recovery_disable_dml_validation: config.disable_dml_validation,
            finalizer,
            timeline: RecoveryTimeline::new(MIN_SNAPSHOT_TS),
            pending_index_ddl_reconciliations: FastHashSet::default(),
            dispatcher,
        }
    }

    /// Replay redo, rebuild indexes, and return recovery outcomes plus redo startup.
    #[inline]
    pub(crate) async fn recover_all(self) -> RuntimeOrFatalResult<RecoveryOutcome> {
        obs::info!("event=recovery_lifecycle component=recovery action=start result=ok");
        self.recover_all_inner()
            .await
            .inspect(|outcome| {
                obs::info!(
                    "event=recovery_lifecycle component=recovery action=finish result=ok max_recovered_cts={}",
                    outcome.max_recovered_cts
                );
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=recovery_lifecycle component=recovery action=finish result=error error={}",
                    err
                );
            })
    }

    async fn recover_all_inner(mut self) -> RuntimeOrFatalResult<RecoveryOutcome> {
        let started = Instant::now();
        obs::info!(
            "event=recovery_phase component=recovery phase=checkpoint_bootstrap action=start result=ok"
        );
        self.bootstrap_checkpointed_user_tables()
            .await
            .inspect(|_| {
                obs::info!("event=recovery_phase component=recovery phase=checkpoint_bootstrap action=finish result=ok");
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=recovery_phase component=recovery phase=checkpoint_bootstrap action=finish result=error error={}",
                    err
                );
            })?;

        self.report.phases.user_table_bootstrap_elapsed = started.elapsed();
        let started = Instant::now();
        obs::info!(
            "event=recovery_phase component=recovery phase=redo_planning action=start result=ok"
        );
        let PlannedRedoRecovery {
            skipped_max_recovered_cts,
            mut stream,
            repair_policy,
            segments_discovered,
            segments_selected,
        } = self
            .redo_planner
            .plan_recovery(self.timeline.replay_floor, self.redo_read_depth)
            .inspect(|_| {
                obs::info!(
                    "event=recovery_phase component=recovery phase=redo_planning action=finish result=ok"
                );
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=recovery_phase component=recovery phase=redo_planning action=finish result=error error={}",
                    err
                );
            })?;
        self.report.work.redo_segments_discovered = segments_discovered;
        self.report.work.redo_segments_selected = segments_selected;
        if let Some(skipped_max_cts) = skipped_max_recovered_cts {
            self.timeline.max_recovered_cts = self.timeline.max_recovered_cts.max(skipped_max_cts);
        }
        // 1. replay DDL and DML into catalog metadata, hot RowStore pages, and
        //    cold delete markers.
        self.report.phases.redo_planning_elapsed = started.elapsed();
        let started = Instant::now();
        obs::info!(
            "event=recovery_phase component=recovery phase=redo_replay action=start result=ok"
        );
        // Keep the count here so failure logs retain partial replay progress.
        let mut replayed_logs = 0usize;
        // All fallible replay exits share settlement. Accepted jobs must release
        // table handles and pool guards before bootstrap reports an error.
        let replay_result = Box::pin(self.replay_stream(&mut stream, &mut replayed_logs)).await;
        if let Err(error) = replay_result {
            let error = self.dispatcher.settle(error).await;
            obs::error!(
                "event=recovery_phase component=recovery phase=redo_replay action=finish result=error replayed_logs={} error={}",
                replayed_logs,
                error
            );
            return Err(error);
        }
        self.dispatcher.merge_counts(&mut self.report);
        obs::info!(
            "event=recovery_phase component=recovery phase=redo_replay action=finish result=ok replayed_logs={}",
            replayed_logs
        );
        self.report.phases.redo_replay_elapsed = started.elapsed();
        let (metrics, saturated) = stream.recovery_metrics();
        self.report.redo = metrics;
        self.report.saturated |= saturated;
        let started = Instant::now();
        let unsealed_terminals = stream.take_unsealed_terminals();
        // 2. Validate every final catalog satellite against catalog.tables.
        obs::info!(
            "event=recovery_phase component=recovery phase=catalog_parent_validation action=start result=ok"
        );
        self.resources
            .catalog
            .storage
            .validate_live_catalog_parent_integrity(&self.resources.pool_guards)
            .await
            .change_runtime_context(RuntimeError::Recovery)
            .inspect(|_| {
                obs::info!("event=recovery_phase component=recovery phase=catalog_parent_validation action=finish result=ok");
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=recovery_phase component=recovery phase=catalog_parent_validation action=finish result=error error={}",
                    err
                );
            })?;
        // Descriptor bytes remain opaque, but their storage-owned envelope must
        // describe the same current numeric schema reconstructed after replay.
        let descriptors = self
            .resources
            .catalog
            .validate_live_table_descriptors(&self.resources.pool_guards)
            .await
            .change_runtime_context(RuntimeError::Recovery)
            .attach("operation=recovery, phase=managed_descriptor_validation")?;
        // 3. Ensure catalog metadata caught up with table-file roots.
        obs::info!(
            "event=recovery_phase component=recovery phase=metadata_validation action=start result=ok"
        );
        self.validate_loaded_table_metadata()
            .await
            .inspect(|_| {
                obs::info!("event=recovery_phase component=recovery phase=metadata_validation action=finish result=ok");
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=recovery_phase component=recovery phase=metadata_validation action=finish result=error error={}",
                    err
                );
            })?;
        self.resources
            .catalog
            .hydrate_recovered_managed_definitions(descriptors)
            .change_context(RuntimeError::Recovery)
            .attach("operation=recovery, phase=hydrate_managed_definitions")?;
        // 4. Remove create-table provisional files whose catalog redo never
        //    became durable.
        self.report.phases.validation_elapsed = started.elapsed();
        let started = Instant::now();
        obs::info!(
            "event=recovery_phase component=recovery phase=absent_file_cleanup action=start result=ok"
        );
        self.cleanup_post_replay_absent_user_table_files()
            .inspect(|_| {
                obs::info!("event=recovery_phase component=recovery phase=absent_file_cleanup action=finish result=ok");
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=recovery_phase component=recovery phase=absent_file_cleanup action=finish result=error error={}",
                    err
                );
            })
            .change_context(RuntimeError::Recovery)?;
        // 5. Rebuild hot secondary-index state from recovered RowStore pages.
        self.report.phases.absent_file_cleanup_elapsed = started.elapsed();
        let started = Instant::now();
        obs::info!(
            "event=recovery_phase component=recovery phase=index_rebuild action=start result=ok"
        );
        self.rebuild_hot_indexes()
            .await
            .inspect(|_| {
                obs::info!("event=recovery_phase component=recovery phase=index_rebuild action=finish result=ok");
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=recovery_phase component=recovery phase=index_rebuild action=finish result=error error={}",
                    err
                );
            })?;
        // 5. Repair accepted unsealed redo prefixes and select the runtime
        //    active file only after replay has succeeded.
        self.report.phases.hot_index_rebuild_elapsed = started.elapsed();
        let started = Instant::now();
        obs::info!(
            "event=recovery_phase component=recovery phase=redo_repair_startup action=start result=ok"
        );
        self.repair_redo_and_prepare_startup(repair_policy, unsealed_terminals)
            .inspect(|_| {
                obs::info!("event=recovery_phase component=recovery phase=redo_repair_startup action=finish result=ok");
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=recovery_phase component=recovery phase=redo_repair_startup action=finish result=error error={}",
                    err
                );
            })
            .change_context(RuntimeError::Recovery)?;

        self.report.phases.redo_repair_planning_elapsed = started.elapsed();
        Ok(RecoveryOutcome {
            max_recovered_cts: self.timeline.max_recovered_cts,
            finalizer: self.finalizer,
            report: self.report,
        })
    }

    #[inline]
    fn repair_redo_and_prepare_startup(
        &mut self,
        repair_policy: RedoRecoveryRepairPolicy,
        unsealed_terminals: Vec<UnsealedSegmentTerminal>,
    ) -> DataIntegrityResult<()> {
        match repair_policy {
            RedoRecoveryRepairPolicy::CreateNext { file_seq } => {
                if !unsealed_terminals.is_empty() {
                    return Err(unexpected_unsealed_terminal_error());
                }
                self.finalizer
                    .set_startup_file(file_seq, RedoLogCreateMode::CreateOrFail);
            }
            RedoRecoveryRepairPolicy::SingleFinalUnsealed { file_seq } => {
                let terminal = take_unsealed_terminal(unsealed_terminals, file_seq)?;
                if terminal.redo_range.is_some() {
                    self.queue_recovered_seal(&terminal);
                    self.finalizer.set_startup_file(
                        next_redo_file_seq(file_seq)?,
                        RedoLogCreateMode::CreateOrFail,
                    );
                } else {
                    self.finalizer
                        .set_startup_file(file_seq, RedoLogCreateMode::CreateOrTrunc);
                }
            }
            RedoRecoveryRepairPolicy::FinalTwoUnsealed {
                older_file_seq,
                newest_file_seq,
            } => {
                let terminal = take_unsealed_terminal(unsealed_terminals, older_file_seq)?;
                self.queue_recovered_seal(&terminal);
                self.finalizer
                    .set_startup_file(newest_file_seq, RedoLogCreateMode::CreateOrTrunc);
            }
        }
        Ok(())
    }

    #[inline]
    fn queue_recovered_seal(&mut self, terminal: &UnsealedSegmentTerminal) {
        self.finalizer.set_recovered_seal(
            terminal.path.clone(),
            terminal.super_block.clone(),
            terminal.accepted_end_offset,
            terminal.redo_range,
        );
    }

    async fn bootstrap_checkpointed_user_tables(&mut self) -> RuntimeOrFatalResult<()> {
        let snapshot = self.resources.catalog.storage.checkpoint_snapshot();
        self.timeline
            .seed_catalog_checkpoint(snapshot.catalog_replay_start_ts);

        let checkpointed_tables = self
            .resources
            .catalog
            .storage
            .tables()
            .list_uncommitted(&self.resources.pool_guards)
            .await?;
        let checkpointed_user_table_ids = checkpointed_tables
            .iter()
            .filter(|table| table.table_id.is_user())
            .map(|table| table.table_id)
            .collect::<FastHashSet<_>>();
        self.resources
            .table_fs
            .cleanup_checkpoint_absent_user_table_files(
                snapshot.meta.next_table_id,
                &checkpointed_user_table_ids,
            )
            .change_context(RuntimeError::Recovery)?;

        for table in checkpointed_tables {
            if !table.table_id.is_user() {
                continue;
            }
            // Checkpoint bootstrap can see table-file metadata that already includes
            // index-DDL roots whose catalog rows are replayed later. Such a
            // narrow mismatch is temporary and must reconcile by final validation.
            let metadata_matched = self
                .resources
                .catalog
                .reload_create_table(
                    self.resources.pools.mem.clone(),
                    self.resources.pools.index.clone(),
                    &self.resources.table_fs,
                    self.resources.pools.disk.clone(),
                    &self.resources.pool_guards,
                    table.table_id,
                )
                .await?;
            let state = self
                .track_loaded_table(table.table_id)
                .change_context(RuntimeError::Recovery)?;
            let pending_index_ddl_reconciliation = !metadata_matched;
            if pending_index_ddl_reconciliation {
                // The catalog checkpoint cursor has already skipped older catalog
                // redo. A pending mismatch is recoverable only when the file root
                // is at or beyond that cursor, so later replay can still supply
                // the catalog index-DDL rows needed to match the root metadata.
                self.validate_checkpoint_pending_reconciliation_cts(table.table_id, state)
                    .change_context(RuntimeError::Recovery)?;
                self.pending_index_ddl_reconciliations
                    .insert(table.table_id);
            }
            self.timeline.seed_table_bounds(state);
            recovery_add_count(
                &mut self.report.work.checkpoint_user_tables,
                1,
                &mut self.report.saturated,
            );
        }
        Ok(())
    }

    #[inline]
    fn should_replay_catalog(&self, cts: TrxID) -> bool {
        cts >= self.timeline.catalog_replay_start_ts
    }

    #[inline]
    fn classify_user_table_redo(
        &self,
        table_id: TableID,
        cts: TrxID,
        context: &'static str,
    ) -> DataIntegrityResult<UserTableRedoAction> {
        if self.timeline.table_bounds.contains_key(&table_id) {
            return Ok(UserTableRedoAction::Replay);
        }
        if cts < self.timeline.catalog_replay_start_ts {
            return Ok(UserTableRedoAction::SkipCheckpointCoveredUnknownTable);
        }
        Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "invalid recovery ordering: {context}: unknown user table redo at or after catalog replay boundary: table_id={table_id}, cts={cts}, catalog_replay_start_ts={}",
                self.timeline.catalog_replay_start_ts
            )))
    }

    fn track_loaded_table(&mut self, table_id: TableID) -> DataIntegrityResult<TableReplayBounds> {
        let table = self.resources.catalog.get_table(table_id).ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("track loaded table runtime: table_id={table_id}"))
        })?;
        // Recovery seeds per-table replay bounds before normal transactions
        // run. The loaded table root supplies the physical root timestamp and
        // the root-local replay floors, but checkpointed silent watermark rows
        // may prove a stronger durable replay floor without a newer table root.
        // Use the catalog helper so the timeline records the fieldwise maximum
        // of root floors and checkpointed silent overlays; uncheckpointed
        // watermark rows replayed later must not affect these bounds.
        let active_root = table.file().active_root_unchecked();
        let effective_floor = self
            .resources
            .catalog
            .effective_user_table_redo_replay_floor(
                table_id,
                TableRedoReplayFloor {
                    heap_redo_start_ts: active_root.heap_redo_start_ts,
                    deletion_cutoff_ts: active_root.deletion_cutoff_ts,
                },
            );
        let state = TableReplayBounds {
            root_ts: active_root.root_ts,
            heap_redo_start_ts: effective_floor.heap_redo_start_ts,
            deletion_cutoff_ts: effective_floor.deletion_cutoff_ts,
        };
        let old = self.timeline.table_bounds.insert(table_id, state);
        if old.is_some() {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("track loaded table state: table_id={table_id}")));
        }
        Ok(state)
    }

    #[inline]
    fn validate_checkpoint_pending_reconciliation_cts(
        &self,
        table_id: TableID,
        state: TableReplayBounds,
    ) -> DataIntegrityResult<()> {
        // No upper bound is checked here: a table root may legitimately be newer
        // than the catalog checkpoint. The invalid state is the opposite case,
        // where a pre-cursor root would require catalog redo that checkpoint
        // bootstrap has already declared unnecessary.
        if state.root_ts < self.timeline.catalog_replay_start_ts {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "checkpointed table pending index-DDL reconciliation has root before catalog replay boundary: table_id={table_id}, root_ts={}, catalog_replay_start_ts={}",
                    state.root_ts,
                    self.timeline.catalog_replay_start_ts
                )));
        }
        Ok(())
    }

    #[inline]
    fn table_heap_redo_start_ts(&self, table_id: TableID) -> DataIntegrityResult<TrxID> {
        self.timeline
            .table_bounds
            .get(&table_id)
            .map(|state| state.heap_redo_start_ts)
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "lookup heap redo start timestamp: table_id={table_id}"
                ))
            })
    }

    #[inline]
    fn table_deletion_cutoff_ts(&self, table_id: TableID) -> DataIntegrityResult<TrxID> {
        self.timeline
            .table_bounds
            .get(&table_id)
            .map(|state| state.deletion_cutoff_ts)
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "lookup deletion cutoff timestamp: table_id={table_id}"
                ))
            })
    }

    #[inline]
    fn table_replay_start_ts(&self, table_id: TableID) -> DataIntegrityResult<TrxID> {
        self.timeline
            .table_bounds
            .get(&table_id)
            .map(|state| state.replay_start_ts())
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "lookup replay start timestamp: table_id={table_id}"
                ))
            })
    }

    /// Consume redo in order and drain replay at EOF, leaving error settlement to the caller.
    async fn replay_stream(
        &mut self,
        stream: &mut RecoveryLogStream,
        replayed_logs: &mut usize,
    ) -> RuntimeOrFatalResult<()> {
        loop {
            let Some(mut group) = self.dispatcher.read_next(stream.try_next()).await? else {
                break;
            };
            // Move only the routing directory; borrowed group storage remains
            // coordinator-owned across admission waits and is released here.
            for trx in mem::take(&mut group.transactions) {
                self.replay_transaction(trx, &group).await?;
                self.dispatcher.progress()?;
                *replayed_logs += 1;
            }
        }
        self.dispatcher.drain_all().await
    }

    async fn replay_transaction(
        &mut self,
        trx: DecodedTrx,
        group: &DecodedGroup,
    ) -> RuntimeOrFatalResult<()> {
        let DecodedTrx { header, kind } = trx;
        match &kind {
            DecodedTrxKind::Ddl(_, dml) => {
                for (id, table) in dml {
                    self.count_seen(*id, table.rows.len());
                }
            }
            DecodedTrxKind::Dml(dml) => {
                for (id, table) in dml {
                    self.count_seen(*id, table.len());
                }
            }
        }
        self.timeline.max_recovered_cts = self.timeline.max_recovered_cts.max(header.cts);
        if header.cts < self.timeline.replay_floor {
            return Ok(());
        }
        match kind {
            DecodedTrxKind::Ddl(ddl, dml) => self.replay_ddl(ddl, dml, header.cts).await,
            DecodedTrxKind::Dml(dml) => self.replay_decoded_dml(dml, header.cts, group).await,
        }
    }

    fn count_seen(&mut self, table_id: TableID, rows: usize) {
        let count = if table_id.is_catalog() {
            &mut self.report.work.catalog_row_ops_seen
        } else {
            &mut self.report.work.user_row_ops_seen
        };
        recovery_add_count(count, rows as u64, &mut self.report.saturated);
    }

    /// Consume one finalized replay registry after global drain and metadata reconciliation.
    /// The bootstrap owner must retain this source/scope until accepted work settles.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "RFC 0032 phase 4 integrates the production caller"
        )
    )]
    async fn capture_hot_index_build(
        &mut self,
        table: Arc<Table>,
        spec: &TableIndexMetadata,
    ) -> RuntimeOrFatalResult<HotBuildSource> {
        use crate::index::build::{DuplicateCheck, HotBuildCapture};
        self.dispatcher.drain_all().await?;
        let policy = self.resources.hot_build_policy;
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let layout = table.layout_snapshot();
        let pivot = table.row_store.blk_idx().pivot_row_id();
        let mut source = HotBuildSource::new(
            HotBuildCapture {
                table: table.clone(),
                layout,
                guards: self.resources.pool_guards.clone(),
                pivot,
                ddl: None,
                #[cfg(feature = "profiling")]
                profiler: self.resources.hot_build_profiler.clone(),
            },
            spec,
            MIN_SNAPSHOT_TS,
            DuplicateCheck::Skip,
            policy,
        );
        if let Some(pages) = self.dispatcher.page_history.remove(&table.table_id()) {
            for replay in pages.into_values() {
                source.push_page(replay.into_descriptor()).attach_with(|| format!("operation=hot_index_build, phase=capture_recovery_pages, table_id={}, index={}", table.table_id(), spec.index))?;
            }
        }
        source.finish_capture().attach_with(|| {
            format!(
                "operation=hot_index_build, phase=validate_recovery_pages, table_id={}, index={}",
                table.table_id(),
                spec.index
            )
        })?;
        #[cfg(feature = "profiling")]
        {
            source.capture_elapsed_nanos = started.elapsed().as_nanos() as u64;
        }
        Ok(source)
    }

    async fn rebuild_hot_indexes(&mut self) -> RuntimeOrFatalResult<()> {
        // Checkpointed cold indexes already reside in DiskTree roots. Consume
        // replay state before rebuilding hot indexes through ordinary row reads.
        for (table_id, pages) in mem::take(&mut self.dispatcher.page_history) {
            let table = self
                .resources
                .catalog
                .get_table(table_id)
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                        "rebuild hot indexes requires live runtime: table_id={table_id}"
                    ))
                })
                .change_context(RuntimeError::Recovery)?;
            for replay in pages.into_values() {
                let page_id = replay.page_id();
                drop(replay);
                let (entries, saturated) = table
                    .populate_index_via_row_page(&self.resources.pool_guards, page_id)
                    .await?;
                self.report.saturated |= saturated;
                recovery_add_count(
                    &mut self.report.work.index_rebuild_pages,
                    1,
                    &mut self.report.saturated,
                );
                recovery_add_count(
                    &mut self.report.work.index_entries_inserted,
                    entries,
                    &mut self.report.saturated,
                );
            }
        }
        Ok(())
    }

    async fn validate_loaded_table_metadata(&mut self) -> RuntimeOrFatalResult<()> {
        let table_ids = self
            .timeline
            .table_bounds
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for table_id in table_ids {
            let table = self
                .resources
                .catalog
                .get_table(table_id)
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "validate recovered user table metadata: table_id={table_id}"
                    ))
                })
                .change_context(RuntimeError::Recovery)?;
            let active_root = table.file().active_root_unchecked();
            let (_, catalog_metadata) = self
                .resources
                .catalog
                .user_table_metadata_from_catalog(&self.resources.pool_guards, table_id)
                .await?;
            if catalog_metadata != *active_root.metadata {
                let pending = self.pending_index_ddl_reconciliations.contains(&table_id);
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach(format!(
                        "recovered user table metadata mismatch after redo replay: table_id={table_id}, root_ts={}, catalog_replay_start_ts={}, pending_index_ddl_reconciliation={pending}, catalog_index_slot_count={}, root_index_slot_count={}",
                        active_root.root_ts,
                        self.timeline.catalog_replay_start_ts,
                        catalog_metadata.idx.index_slot_count_u32(),
                        active_root.metadata.idx.index_slot_count_u32()
                    ))
                    .change_context(RuntimeError::Recovery).into());
            }
            self.pending_index_ddl_reconciliations.remove(&table_id);
            table
                .finish_index_lifecycle_recovery()
                .change_context(RuntimeError::Recovery)
                .attach_with(|| {
                    format!("finish recovered index lifecycle classification: table_id={table_id}")
                })?;
        }
        if let Some(table_id) = self
            .pending_index_ddl_reconciliations
            .iter()
            .next()
            .copied()
        {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "pending index-DDL reconciliation left after recovered metadata validation: table_id={table_id}"
                ))
                .change_context(RuntimeError::Recovery).into());
        }
        Ok(())
    }

    fn cleanup_post_replay_absent_user_table_files(&self) -> IoResult<()> {
        let recovered_user_table_ids = self
            .timeline
            .table_bounds
            .keys()
            .copied()
            .collect::<FastHashSet<_>>();
        let deferred_drop_table_ids = self
            .resources
            .catalog
            .retained_dropped_table_ids_now()
            .into_iter()
            .collect::<FastHashSet<_>>();
        self.resources
            .table_fs
            .cleanup_recovery_absent_user_table_files(
                &recovered_user_table_ids,
                &deferred_drop_table_ids,
            )
    }

    async fn replay_ddl(
        &mut self,
        ddl: Box<DDLRedo>,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        match *ddl {
            DDLRedo::CreateTable(table_id) => {
                self.replay_create_table_ddl(table_id, dml, cts).await?
            }
            DDLRedo::DropTable(table_id) => self.replay_drop_table_ddl(table_id, dml, cts).await?,
            DDLRedo::CreateIndex {
                table_id,
                index_id,
                index_slot,
            } => {
                self.replay_create_index_ddl(
                    table_id,
                    IndexRef::new(index_id, index_slot),
                    dml,
                    cts,
                )
                .await?
            }
            DDLRedo::DropIndex {
                table_id,
                index_id,
                index_slot,
            } => {
                self.replay_drop_index_ddl(table_id, IndexRef::new(index_id, index_slot), dml, cts)
                    .await?
            }
            DDLRedo::CreateRowPage {
                table_id,
                page_id,
                start_row_id,
                end_row_id,
            } => {
                self.replay_create_row_page_ddl(
                    table_id,
                    page_id,
                    start_row_id,
                    end_row_id,
                    dml,
                    cts,
                )
                .await?
            }
            DDLRedo::DataCheckpoint { table_id, .. } => {
                self.replay_data_checkpoint_ddl(table_id, dml, cts)?
            }
            DDLRedo::TableReplaySilentWatermark { table_id } => {
                self.replay_table_replay_silent_watermark_ddl(table_id, dml, cts)
                    .await?
            }
        }
        Ok(())
    }

    async fn replay_create_table_ddl(
        &mut self,
        table_id: TableID,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        if !self.should_replay_catalog(cts) {
            return Ok(());
        }
        self.dispatcher.drain(table_id).await?;
        self.replay_catalog_modifications(dml).await?;
        // The table file stores only the latest active root, so create-table
        // redo may load metadata from later durable index DDL. Recovery
        // tracks that temporary gap until index-DDL redo reconciles catalog rows.
        let metadata_matched = self
            .resources
            .catalog
            .reload_create_table(
                self.resources.pools.mem.clone(),
                self.resources.pools.index.clone(),
                &self.resources.table_fs,
                self.resources.pools.disk.clone(),
                &self.resources.pool_guards,
                table_id,
            )
            .await?;
        let state = self
            .track_loaded_table(table_id)
            .change_context(RuntimeError::Recovery)?;
        self.timeline
            .seed_recovered_cts(state.max_recovered_cts_seed());
        let pending_index_ddl_reconciliation = !metadata_matched;
        // Validate the root/create CTS relation before marking the table
        // as pending, so impossible metadata divergence fails at the
        // source instead of surfacing only in final equality validation.
        validate_create_table_reloaded_root_ts(
            table_id,
            cts,
            state,
            pending_index_ddl_reconciliation,
        )
        .change_context(RuntimeError::Recovery)?;
        if pending_index_ddl_reconciliation {
            self.pending_index_ddl_reconciliations.insert(table_id);
        }
        Ok(())
    }

    async fn replay_drop_table_ddl(
        &mut self,
        table_id: TableID,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        if !self.should_replay_catalog(cts) {
            return Ok(());
        }
        self.dispatcher.drain(table_id).await?;
        self.replay_catalog_modifications(dml).await?;
        // The catalog DROP rows have been replayed, but the table runtime was
        // loaded before DDL replay so row/index redo could reach it. Remove the
        // live catalog entry now so later recovery phases stop treating this
        // table as an existing user table.
        let removed = self
            .resources
            .catalog
            .remove_live_user_table(table_id)
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!("replay drop table: table_id={table_id}"))
            })
            .change_context(RuntimeError::Recovery)?;
        let replay_floor = self
            .resources
            .catalog
            .effective_user_table_redo_replay_floor(table_id, removed.redo_replay_floor_snapshot());
        let table = Arc::try_unwrap(removed)
            .map_err(|table| {
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "replay drop table found stale runtime handle: table_id={table_id}, strong_count={}",
                Arc::strong_count(&table)
            ))
            })
            .change_context(RuntimeError::Recovery)?;
        // Recovery runs before normal runtime admission, so a committed DROP
        // should have the only remaining table runtime handle here. Destroy the
        // row/index runtime state immediately after logical removal.
        table.close_checkpoint_workflow_offline();
        table
            .destroy_dropped_runtime(&self.resources.pool_guards)
            .await?;
        if self.timeline.catalog_replay_start_ts <= cts {
            // The catalog checkpoint has not yet made this table absence
            // durable. Keep only the dropped replay floor so redo retention
            // protects the log range needed to recover the DROP again.
            self.resources
                .catalog
                .insert_dropped_table_floor(table_id, cts, replay_floor);
        }
        self.timeline.table_bounds.remove(&table_id);
        self.dispatcher.page_history.remove(&table_id);
        self.pending_index_ddl_reconciliations.remove(&table_id);
        Ok(())
    }

    async fn replay_create_index_ddl(
        &mut self,
        table_id: TableID,
        index: IndexRef,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        if !self.should_replay_catalog(cts) {
            return Ok(());
        }
        if self
            .classify_user_table_redo(table_id, cts, "replay create index")
            .change_context(RuntimeError::Recovery)?
            == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
        {
            return Ok(());
        }
        self.dispatcher.drain(table_id).await?;
        let proof = self
            .classify_index_ddl_root(IndexDdlKind::Create, table_id, index, cts)
            .change_context(RuntimeError::Recovery)?;
        match proof {
            IndexDdlRootProof::DurableFinalCreate | IndexDdlRootProof::DurableAllocationOnly => {
                self.replay_catalog_modifications(dml).await?;
            }
            IndexDdlRootProof::Provisional => {
                let table = self
                    .resources
                    .catalog
                    .get_table(table_id)
                    .ok_or_else(|| {
                        Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                            "replay provisional CREATE INDEX has no admitted table: table_id={table_id}, index={index}"
                        ))
                    })
                    .change_context(RuntimeError::Recovery)?;
                table
                    .reserve_provisional_index_create(index, cts)
                    .change_context(RuntimeError::Recovery)?;
            }
            IndexDdlRootProof::DurableFinalDrop => {
                unreachable!("create-index root proof cannot classify as durable final drop")
            }
        }
        Ok(())
    }

    async fn replay_drop_index_ddl(
        &mut self,
        table_id: TableID,
        index: IndexRef,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        if !self.should_replay_catalog(cts) {
            return Ok(());
        }
        if self
            .classify_user_table_redo(table_id, cts, "replay drop index")
            .change_context(RuntimeError::Recovery)?
            == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
        {
            return Ok(());
        }
        self.dispatcher.drain(table_id).await?;
        let proof = self
            .classify_index_ddl_root(IndexDdlKind::Drop, table_id, index, cts)
            .change_context(RuntimeError::Recovery)?;
        match proof {
            IndexDdlRootProof::DurableFinalDrop => {
                self.replay_catalog_modifications(dml).await?;
                let table = self
                    .resources
                    .catalog
                    .get_table(table_id)
                    .ok_or_else(|| {
                        Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                            "replay root-proven DROP INDEX has no admitted table: table_id={table_id}, index={index}"
                        ))
                    })
                    .change_context(RuntimeError::Recovery)?;
                table
                    .record_replayed_index_drop(index, cts)
                    .change_context(RuntimeError::Recovery)?;
            }
            IndexDdlRootProof::Provisional => {}
            IndexDdlRootProof::DurableFinalCreate | IndexDdlRootProof::DurableAllocationOnly => {
                unreachable!("drop-index root proof cannot classify as create proof")
            }
        }
        Ok(())
    }

    async fn replay_create_row_page_ddl(
        &mut self,
        table_id: TableID,
        page_id: PageID,
        start_row_id: RowID,
        end_row_id: RowID,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        debug_assert!(dml.is_empty());
        if self
            .classify_user_table_redo(table_id, cts, "replay create row page")
            .change_context(RuntimeError::Recovery)?
            == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
        {
            return Ok(());
        }
        if cts
            < self
                .table_heap_redo_start_ts(table_id)
                .change_context(RuntimeError::Recovery)?
        {
            return Ok(());
        }
        let table = self
            .resources
            .catalog
            .get_table(table_id)
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!("replay create row page: table_id={table_id}"))
            })
            .change_context(RuntimeError::Recovery)?;
        let Some(count) = end_row_id
            .checked_sub(start_row_id)
            .filter(|count| (1..=u64::from(u16::MAX)).contains(count))
        else {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "replay create row page has invalid row range: table_id={table_id}, page_id={page_id}, start_row_id={start_row_id}, end_row_id={end_row_id}"
                ))
                .change_context(RuntimeError::Recovery).into());
        };
        // Explicit allocation rejects duplicate pages before initializing replay state.
        let page_guard = table
            .row_store
            .allocate_row_page_at(&self.resources.pool_guards, count as usize, page_id)
            .await?;
        // Ordered page creation must reproduce the logged reservation. Validate
        // this once before publishing a descriptor trusted by later page access.
        let page = page_guard.page();
        let actual_start = page.header.start_row_id;
        let actual_end = actual_start + u64::from(page.header.max_row_count);
        if (actual_start, actual_end) != (start_row_id, end_row_id) {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "replay create row page range mismatch: table_id={table_id}, page_id={page_id}, expected_start={start_row_id}, expected_end={end_row_id}, actual_start={actual_start}, actual_end={actual_end}"
                ))
                .change_context(RuntimeError::Recovery).into());
        }
        recovery_add_count(
            &mut self.report.work.hot_pages_reconstructed,
            1,
            &mut self.report.saturated,
        );
        page_guard.unwrap_vmap().set_create_cts(cts);
        self.dispatcher
            .page_history
            .entry(table_id)
            .or_default()
            .insert(
                page_id,
                RowReplayState::new(RowPageDescriptor {
                    page_id,
                    start_row_id,
                    end_row_id,
                }),
            );

        Ok(())
    }

    fn replay_data_checkpoint_ddl(
        &mut self,
        table_id: TableID,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> RuntimeResult<()> {
        debug_assert!(dml.is_empty());
        if self
            .classify_user_table_redo(table_id, cts, "replay data checkpoint")
            .change_context(RuntimeError::Recovery)?
            == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
        {
            return Ok(());
        }
        if cts
            < self
                .table_heap_redo_start_ts(table_id)
                .change_context(RuntimeError::Recovery)?
        {
            return Ok(());
        }
        let _ = self
            .resources
            .catalog
            .get_table(table_id)
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!("replay data checkpoint: table_id={table_id}"))
            })
            .change_context(RuntimeError::Recovery)?;
        Ok(())
    }

    async fn replay_table_replay_silent_watermark_ddl(
        &mut self,
        table_id: TableID,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        if !self.should_replay_catalog(cts) {
            return Ok(());
        }
        if self
            .classify_user_table_redo(table_id, cts, "replay silent table watermark")
            .change_context(RuntimeError::Recovery)?
            == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
        {
            return Ok(());
        }
        self.replay_catalog_modifications(dml).await
    }

    fn classify_index_ddl_root(
        &self,
        kind: IndexDdlKind,
        table_id: TableID,
        index: IndexRef,
        cts: TrxID,
    ) -> DataIntegrityResult<IndexDdlRootProof> {
        let table = self.resources.catalog.get_table(table_id);
        let active_root = table
            .as_ref()
            .map(|table| table.file().active_root_unchecked());
        let ddl = ReplayVisibleIndexDdl::from_replay_visible(
            index,
            cts,
            self.timeline.catalog_replay_start_ts,
        );
        classify_index_ddl_root(kind, table_id, ddl, active_root)
    }

    /// Replay DML log.
    ///
    /// Catalog rows are replayed logically into catalog runtimes. User-table
    /// rows replay only heap and cold-delete state; hot secondary indexes are
    /// rebuilt after log replay from recovered RowStore pages.
    async fn replay_decoded_dml(
        &mut self,
        dml: BTreeMap<TableID, DecodedTable>,
        cts: TrxID,
        group: &DecodedGroup,
    ) -> RuntimeOrFatalResult<()> {
        for (table_id, table_dml) in dml {
            if let DecodedTable::Catalog(table_dml) = table_dml {
                if !self.should_replay_catalog(cts) {
                    continue;
                }
                let table = self
                    .resources
                    .catalog
                    .get_catalog_table(table_id)
                    .ok_or_else(|| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach(format!("replay catalog DML: table_id={table_id}"))
                    })
                    .change_context(RuntimeError::Recovery)?;
                self.replay_catalog_table_modifications(&table, &table_dml.rows)
                    .await?;
                continue;
            }
            if self
                .classify_user_table_redo(table_id, cts, "replay user table DML")
                .change_context(RuntimeError::Recovery)?
                == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
            {
                continue;
            }
            if cts
                < self
                    .table_replay_start_ts(table_id)
                    .change_context(RuntimeError::Recovery)?
            {
                continue;
            }
            let table = self
                .resources
                .catalog
                .get_table(table_id)
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!("replay user table DML: table_id={table_id}"))
                })
                .change_context(RuntimeError::Recovery)?;
            let DecodedTable::User(rows) = table_dml else {
                unreachable!("catalog DML handled above");
            };
            self.replay_decoded_rows(table_id, &table, rows, cts, group)
                .await?;
        }
        Ok(())
    }

    /// Replay catalog DML log.
    /// Page id and row id in log are ignored because we do not keep physical structure for metadata.
    async fn replay_catalog_modifications(
        &mut self,
        dml: BTreeMap<TableID, TableDML>,
    ) -> RuntimeOrFatalResult<()> {
        for (table_id, table_dml) in dml {
            let table = self
                .resources
                .catalog
                .get_catalog_table(table_id)
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                        "replay catalog table modifications: table_id={table_id}"
                    ))
                })
                .change_context(RuntimeError::Recovery)?;
            self.replay_catalog_table_modifications(&table, &table_dml.rows)
                .await?;
        }
        Ok(())
    }

    async fn replay_catalog_table_modifications(
        &mut self,
        table: &CatalogTable,
        rows: &BTreeMap<RowID, RowRedo>,
    ) -> RuntimeOrFatalResult<()> {
        for row in rows.values() {
            match &row.kind {
                RowRedoKind::Insert(_, vals) => {
                    table
                        .insert_no_trx(
                            &self.resources.pool_guards,
                            vals,
                            self.recovery_disable_dml_validation,
                        )
                        .await?;
                }
                RowRedoKind::DeleteByPrimaryKey(key) => {
                    table
                        .delete_primary_key_no_trx(
                            &self.resources.pool_guards,
                            key.index_slot,
                            &key.vals,
                            self.recovery_disable_dml_validation,
                        )
                        .await?;
                }
                RowRedoKind::UpdateByPrimaryKey(key, cols) => {
                    table
                        .update_primary_key_no_trx(
                            &self.resources.pool_guards,
                            key.index_slot,
                            &key.vals,
                            cols,
                            self.recovery_disable_dml_validation,
                        )
                        .await?;
                }
                RowRedoKind::Delete(_) | RowRedoKind::Update(..) => {
                    // Catalog row-id redo is invalid because catalog row IDs
                    // are rebuilt when checkpointed rows are loaded.
                    unreachable!()
                }
            }
            recovery_add_count(
                &mut self.report.work.catalog_row_ops_applied,
                1,
                &mut self.report.saturated,
            );
        }
        Ok(())
    }

    async fn replay_decoded_rows(
        &mut self,
        table_id: TableID,
        table: &Arc<Table>,
        rows: BTreeMap<RowID, DecodedRow>,
        cts: TrxID,
        group: &DecodedGroup,
    ) -> RuntimeOrFatalResult<()> {
        let heap_redo_start_ts = self
            .table_heap_redo_start_ts(table_id)
            .change_context(RuntimeError::Recovery)?;
        let deletion_cutoff_ts = self
            .table_deletion_cutoff_ts(table_id)
            .change_context(RuntimeError::Recovery)?;
        let pivot_row_id = table.file().active_root_unchecked().pivot_row_id;
        for row in rows.into_values() {
            let page_id = match &row.kind {
                DecodedRowKind::Insert(page_id, _) | DecodedRowKind::Update(page_id, _) => {
                    // Checkpointed rows and floor-covered redo need no hot history.
                    if !should_replay_heap_row(row.row_id, pivot_row_id, cts, heap_redo_start_ts) {
                        continue;
                    }
                    *page_id
                }
                DecodedRowKind::Delete(page_id) => {
                    if row.row_id < pivot_row_id {
                        if cts < deletion_cutoff_ts {
                            continue;
                        }
                        table.recover_cold_row_delete(row.row_id, cts)
                            .change_context(RuntimeError::Recovery)
                            .attach_with(|| format!("operation=recover_cold_row_delete, table_id={table_id}, row_id={}, cts={cts}", row.row_id))?;
                        recovery_add_count(
                            &mut self.report.work.cold_deletes,
                            1,
                            &mut self.report.saturated,
                        );
                        continue;
                    }
                    if cts < heap_redo_start_ts {
                        continue;
                    }
                    page_id.ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!("hot row delete redo requires page identity: operation=delete, table_id={table_id}, row_id={}, cts={cts}", row.row_id)))
                        .change_context(RuntimeError::Recovery)?
                }
                DecodedRowKind::Keyed(row) => {
                    return Err(invalid_user_table_keyed_redo(table_id, row, cts)
                        .change_context(RuntimeError::Recovery)
                        .into());
                }
            };
            self.dispatcher
                .admit(table, page_id, group.operation(&row, cts))
                .await?;
        }
        Ok(())
    }
}

#[inline]
fn validate_create_table_reloaded_root_ts(
    table_id: TableID,
    create_table_cts: TrxID,
    state: TableReplayBounds,
    pending_index_ddl_reconciliation: bool,
) -> DataIntegrityResult<()> {
    // Create-table redo reopens the latest table root. The initial create root
    // is published before the catalog commit and therefore carries the create
    // transaction STS, which can predate the create-table redo CTS. A metadata
    // mismatch accepted by reload still requires a later root publication so
    // recovery can prove pending index-DDL metadata has a durable table root.
    if pending_index_ddl_reconciliation && state.root_ts <= create_table_cts {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "create-table pending index-DDL reconciliation requires later table root: table_id={table_id}, root_ts={}, create_table_cts={create_table_cts}",
                state.root_ts
            )));
    }
    Ok(())
}

#[inline]
fn take_unsealed_terminal(
    mut terminals: Vec<UnsealedSegmentTerminal>,
    file_seq: u32,
) -> DataIntegrityResult<UnsealedSegmentTerminal> {
    if terminals.len() != 1 {
        return Err(unexpected_unsealed_terminal_error());
    }
    let terminal = terminals.pop().expect("terminal length was checked");
    if terminal.file_seq != file_seq {
        return Err(unexpected_unsealed_terminal_error());
    }
    Ok(terminal)
}

#[inline]
fn unexpected_unsealed_terminal_error() -> Report<DataIntegrityError> {
    Report::new(DataIntegrityError::LogFileCorrupted)
        .attach("redo unsealed terminal metadata did not match recovery plan")
}

#[inline]
fn invalid_user_table_keyed_redo(
    table_id: TableID,
    row: &RowRedo,
    cts: TrxID,
) -> Report<DataIntegrityError> {
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(format!(
            "key-based catalog redo is invalid for user table replay: table_id={table_id}, row_id={}, cts={cts}, kind={:?}",
            row.row_id, row.kind
        ))
}

#[inline]
fn should_replay_heap_row(
    row_id: RowID,
    pivot_row_id: RowID,
    cts: TrxID,
    heap_redo_start_ts: TrxID,
) -> bool {
    row_id >= pivot_row_id && cts >= heap_redo_start_ts
}

#[cfg(test)]
mod tests {
    use super::decode::{DecodedTable, DecodedTrxKind, decode_log};
    use super::dispatch::recycled_snapshot;
    use super::{
        RecoveryCoordinator, invalid_user_table_keyed_redo, should_replay_heap_row,
        validate_create_table_reloaded_root_ts,
    };
    use crate::catalog::TableIndexMetadata;
    use crate::catalog::storage::publish_first_redo_log_seq_for_test;
    use crate::catalog::storage::tests::begin_catalog_test_trx;
    use crate::catalog::{
        ActiveIndexSpec, CATALOG_TABLE_ID_START, CatalogIndexNo, CatalogSelectKey, ColumnID,
        ColumnOrdinal, IndexID, IndexObject, IndexOrder, IndexRef, IndexSlot, SecondaryIndexRoot,
        SecondaryIndexSlot, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags,
        StorageIndexKey, StorageIndexSpec, StorageTableSpec, TableIndexKeySpec, TableMetadata,
        TableObject, USER_TABLE_ID_START,
    };
    use crate::component::EnginePools;
    use crate::conf::{
        EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, RecoveryConfig, TrxSysConfig,
    };
    use crate::engine::Engine;
    use crate::error::RuntimeOrFatalError;
    use crate::error::{
        CompletionErrorBridge, DataIntegrityError, ErrorKind, InternalError, OperationError,
        RuntimeError, RuntimeOrFatalResult,
    };
    use crate::file::block_integrity::BLOCK_INTEGRITY_HEADER_SIZE;
    use crate::file::cow_file::tests::{corrupt_page_checksum, rewrite_page_with_checksum};
    use crate::index::build::{HotBuildPolicy, HotBuildSource};
    use crate::table::RowPageDescriptor;

    use crate::file::table_file::MutableTableFile;
    use crate::id::{BlockID, PageID, RowID, TableID, TrxID};
    use crate::index::{COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE, ColumnBlockIndex, RowLocation};
    use crate::log::LogSync;
    use crate::log::block_group::TrxLog;
    use crate::log::format::{
        REDO_BLOCK_GROUP_END, REDO_BLOCK_GROUP_START, REDO_DEFAULT_DATA_START_OFFSET,
        REDO_SUPER_BLOCK_SLOT_SIZE, RedoBlockHeader, RedoGroupStartExtension, RedoSuperBlock,
        parse_redo_super_block, serialize_redo_super_block, slot_offset,
    };
    use crate::log::redo::TableDML;
    use crate::log::redo::{DDLRedo, RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind};
    use crate::recovery::RowReplayState;
    use crate::recovery::{RecoveryResources, TableReplayBounds};
    use crate::row::RowRead;
    use crate::row::ops::{
        RowMutation, ScanRowDecision, SelectKey, SelectMvcc, UniqueMutationOutcome, UpdateCol,
    };
    use crate::serde::Ser;
    use crate::session::tests::{SessionTestExt, assert_checkpoint_published};
    use crate::table::tests::{
        assert_freeze_created, assert_table_data_integrity, trx_delete_row_by_id,
        trx_select_row_mvcc_by_id, trx_update_row_by_id,
    };
    use crate::table::{DeleteMarker, Table, TableRedoReplayFloor};
    use crate::trx::MIN_SNAPSHOT_TS;
    use crate::trx::ver_map::RowPageState;
    use crate::value::Val;
    use crate::value::ValKind;
    use crate::{CallbackResult, RecoveryReport, RecoveryWorkCounts};
    use std::time::Duration;

    use std::collections::BTreeMap;
    use std::fs::{self, File};
    use std::io::{Seek, SeekFrom, Write};
    use std::iter::repeat_n;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tempfile::TempDir;

    const LIGHTWEIGHT_RECOVERY_BUFFER_BYTES: usize = 16 * 1024 * 1024;
    const LIGHTWEIGHT_RECOVERY_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
    const LIGHTWEIGHT_RECOVERY_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;
    const CORRUPTION_RECOVERY_LOG_BLOCK_SIZE: usize = 4096;
    const CORRUPTION_RECOVERY_LOG_FILE_MAX_SIZE: usize = 128 * 1024;

    enum CatalogCheckpointOrder {
        BeforeTable,
        AfterTable,
    }

    /// Exercise finalized-registry capture with replay-owned sidecars in component tests.
    pub(crate) async fn capture_hot_build_test_source(
        engine: &Engine,
        table: Arc<Table>,
        spec: &TableIndexMetadata,
        states: Vec<RowReplayState>,
        policy: HotBuildPolicy,
    ) -> RuntimeOrFatalResult<HotBuildSource> {
        let mut recovery = row_recovery_for_table(engine, table.table_id());
        recovery.dispatcher.page_history.insert(
            table.table_id(),
            states
                .into_iter()
                .map(|state| (state.page_id(), state))
                .collect(),
        );
        recovery.resources.hot_build_policy = policy;
        #[cfg(feature = "profiling")]
        {
            recovery.resources.hot_build_profiler =
                engine.inner().core.trx_sys.hot_build_profiler.clone();
        }
        let source = recovery.capture_hot_index_build(table, spec).await?;
        assert!(recovery.dispatcher.page_history.is_empty());
        Ok(source)
    }

    // Keep the runtime carrier assertion separate from the public table error contract.
    fn assert_table_runtime_data_integrity(
        err: RuntimeOrFatalError,
        block_kind: &str,
        block_id: BlockID,
        expected: DataIntegrityError,
    ) {
        let RuntimeOrFatalError::Runtime(err) = err else {
            panic!("expected Runtime error, got {err:?}");
        };
        let report = format!("{err:?}");
        assert_eq!(
            err.current_context(),
            &RuntimeError::IndexAccess,
            "{report}"
        );
        assert_eq!(
            err.downcast_ref::<DataIntegrityError>().copied(),
            Some(expected),
            "{report}"
        );
        assert!(
            err.downcast_ref::<CompletionErrorBridge>().is_none(),
            "{report}"
        );
        assert!(report.contains("table_file"), "{report}");
        assert!(report.contains(block_kind), "{report}");
        assert!(report.contains(&format!("block_id={block_id}")), "{report}");
    }

    fn assert_report_accounting(report: &RecoveryReport) {
        assert!(!report.saturated, "{report:?}");
        assert_eq!(
            report.bootstrap_elapsed,
            report.engine_setup_elapsed
                + report.catalog_bootstrap_elapsed
                + report.transaction_bootstrap_elapsed
                + report.runtime_startup_elapsed
        );
        let phases = &report.phases;
        assert_eq!(
            report.transaction_bootstrap_elapsed,
            phases.preparation_elapsed
                + phases.user_table_bootstrap_elapsed
                + phases.redo_planning_elapsed
                + phases.redo_replay_elapsed
                + phases.validation_elapsed
                + phases.absent_file_cleanup_elapsed
                + phases.hot_index_rebuild_elapsed
                + phases.redo_repair_planning_elapsed
                + phases.redo_finalize_elapsed
                + phases.other_elapsed
        );
        let redo = &report.redo;
        assert_eq!(
            phases.redo_replay_elapsed,
            redo.stream_refill_elapsed + redo.apply_and_dispatch_elapsed
        );
        assert_eq!(
            redo.stream_refill_elapsed,
            redo.receive_wait_elapsed
                + redo.group_decode_elapsed
                + redo.reader_shutdown_elapsed
                + redo.stream_other_elapsed
        );
        let work = &report.work;
        assert_eq!(
            work.user_row_ops_seen,
            work.user_row_ops_applied + work.user_row_ops_skipped
        );
        assert_eq!(
            work.user_row_ops_applied,
            work.hot_inserts + work.hot_updates + work.hot_deletes + work.cold_deletes
        );
        assert_eq!(
            work.catalog_row_ops_seen,
            work.catalog_row_ops_applied + work.catalog_row_ops_skipped
        );
        assert!(redo.consumed_bytes >= redo.validated_payload_bytes);
    }

    fn recovery_engine_config(main_dir: impl Into<PathBuf>, log_file_stem: &str) -> EngineConfig {
        EngineConfig::default()
            .storage_root(main_dir)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .max_mem_size(64usize * 1024 * 1024)
                    .max_file_size(128usize * 1024 * 1024),
            )
            .trx(TrxSysConfig::default().log_file_stem(log_file_stem))
    }

    fn lightweight_recovery_engine_config(
        main_dir: impl Into<PathBuf>,
        log_file_stem: &str,
    ) -> EngineConfig {
        EngineConfig::default()
            .storage_root(main_dir)
            .meta_buffer(LIGHTWEIGHT_RECOVERY_BUFFER_BYTES)
            .index_buffer(
                EvictableBufferPoolConfig::default()
                    .swap_file("index.swp")
                    .max_mem_size(LIGHTWEIGHT_RECOVERY_BUFFER_BYTES)
                    .max_file_size(LIGHTWEIGHT_RECOVERY_MAX_FILE_BYTES),
            )
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .max_mem_size(LIGHTWEIGHT_RECOVERY_BUFFER_BYTES)
                    .max_file_size(LIGHTWEIGHT_RECOVERY_MAX_FILE_BYTES),
            )
            .recovery(RecoveryConfig::default().io_depth(1))
            .trx(
                TrxSysConfig::default()
                    .log_write_io_depth(1)
                    .catalog_checkpoint_scan_io_depth(1)
                    .log_file_stem(log_file_stem)
                    .purge_threads(1),
            )
            .file(
                FileSystemConfig::default()
                    .io_depth(1)
                    .readonly_buffer_size(LIGHTWEIGHT_RECOVERY_READONLY_BUFFER_BYTES),
            )
    }

    fn corruption_recovery_engine_config(
        main_dir: impl Into<PathBuf>,
        log_file_stem: &str,
    ) -> EngineConfig {
        lightweight_recovery_engine_config(main_dir, log_file_stem).trx(
            TrxSysConfig::default()
                .log_write_io_depth(1)
                .catalog_checkpoint_scan_io_depth(1)
                .log_block_size(CORRUPTION_RECOVERY_LOG_BLOCK_SIZE)
                .log_file_stem(log_file_stem)
                .log_file_max_size(CORRUPTION_RECOVERY_LOG_FILE_MAX_SIZE)
                .log_sync(LogSync::None)
                .purge_threads(1),
        )
    }

    fn retention_marker_recovery_engine_config(
        main_dir: impl Into<PathBuf>,
        log_file_stem: &str,
    ) -> EngineConfig {
        lightweight_recovery_engine_config(main_dir, log_file_stem).trx(
            TrxSysConfig::default()
                .log_write_io_depth(1)
                .catalog_checkpoint_scan_io_depth(1)
                .log_block_size(CORRUPTION_RECOVERY_LOG_BLOCK_SIZE)
                .log_file_stem(log_file_stem)
                .log_file_max_size(
                    REDO_DEFAULT_DATA_START_OFFSET + CORRUPTION_RECOVERY_LOG_BLOCK_SIZE,
                )
                .log_sync(LogSync::None)
                .purge_threads(1),
        )
    }

    async fn prepare_engine_with_retained_redo_suffix(
        main_dir: &Path,
        log_file_stem: &str,
    ) -> (Engine, TableID) {
        let engine = Engine::bootstrap(retention_marker_recovery_engine_config(
            main_dir,
            log_file_stem,
        ))
        .await
        .unwrap();
        let table_id = create_index_ddl_base_table(&engine, vec![base_unique_index_spec()]).await;
        let mut session = engine.new_session().unwrap();
        for value in 0..32 {
            let mut trx = session.begin_trx().unwrap();
            trx.table_insert_mvcc(table_id, vec![Val::from(value), Val::from(value)])
                .await
                .unwrap();
            trx.commit().await.unwrap();
            if redo_file_path(main_dir, log_file_stem, 1).exists() {
                break;
            }
        }
        assert!(
            redo_file_path(main_dir, log_file_stem, 1).exists(),
            "test setup should create retained redo suffix"
        );

        let table = engine.inner().core.catalog().get_table(table_id).unwrap();
        assert_checkpoint_published(&mut session, table.table_id()).await;
        drop(table);
        let mut durability_trx = session.begin_trx().unwrap();
        durability_trx
            .table_insert_mvcc(table_id, vec![Val::from(10_001), Val::from(10_001)])
            .await
            .unwrap();
        durability_trx.commit().await.unwrap();
        drop(session);
        engine
            .new_session()
            .unwrap()
            .checkpoint_catalog()
            .await
            .unwrap();
        publish_first_redo_log_seq_for_test(&engine.inner().core.catalog().storage, 1)
            .await
            .unwrap();
        (engine, table_id)
    }

    async fn prepare_checkpointed_recovery_floor(main_dir: &Path, log_file_stem: &str) -> TrxID {
        let engine = Engine::bootstrap(corruption_recovery_engine_config(main_dir, log_file_stem))
            .await
            .unwrap();
        let table_id = create_index_ddl_base_table(&engine, vec![base_unique_index_spec()]).await;
        let mut session = engine.new_session().unwrap();
        session.drop_table(table_id).await.unwrap();
        drop(session);
        engine
            .new_session()
            .unwrap()
            .checkpoint_catalog()
            .await
            .unwrap();
        let replay_floor = engine
            .inner()
            .core
            .catalog()
            .storage
            .checkpoint_snapshot()
            .catalog_replay_start_ts;
        assert!(replay_floor > MIN_SNAPSHOT_TS);
        drop(engine);
        remove_redo_family(main_dir, log_file_stem);
        replay_floor
    }

    fn redo_file_path(main_dir: &Path, log_file_stem: &str, file_seq: u32) -> PathBuf {
        main_dir.join(format!("{log_file_stem}.{file_seq:08x}"))
    }

    fn remove_redo_file(main_dir: &Path, log_file_stem: &str, file_seq: u32) {
        fs::remove_file(redo_file_path(main_dir, log_file_stem, file_seq)).unwrap();
    }

    fn remove_redo_family(main_dir: &Path, log_file_stem: &str) {
        let prefix = format!("{log_file_stem}.");
        for entry in fs::read_dir(main_dir).unwrap() {
            let path = entry.unwrap().path();
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if file_name.starts_with(&prefix) {
                fs::remove_file(path).unwrap();
            }
        }
    }

    fn write_bad_checksum_redo_file(
        main_dir: &Path,
        log_file_stem: &str,
        cts: TrxID,
        sealed: bool,
    ) {
        let path = main_dir.join(format!("{log_file_stem}.00000000"));
        let mut file = File::create(path).unwrap();
        file.set_len(CORRUPTION_RECOVERY_LOG_FILE_MAX_SIZE as u64)
            .unwrap();

        let open = RedoSuperBlock::initial(
            0,
            CORRUPTION_RECOVERY_LOG_BLOCK_SIZE,
            CORRUPTION_RECOVERY_LOG_FILE_MAX_SIZE,
        );
        write_redo_super_block_slot(&mut file, &open);

        if sealed {
            let durable_end_offset =
                REDO_DEFAULT_DATA_START_OFFSET + CORRUPTION_RECOVERY_LOG_BLOCK_SIZE;
            let sealed =
                RedoSuperBlock::sealed_from_open(&open, 1, durable_end_offset, Some((cts, cts)))
                    .unwrap();
            write_redo_super_block_slot(&mut file, &sealed);
        }

        let mut group = vec![0u8; CORRUPTION_RECOVERY_LOG_BLOCK_SIZE];
        let header = RedoBlockHeader {
            checksum: 1,
            flags: REDO_BLOCK_GROUP_START | REDO_BLOCK_GROUP_END,
            payload_len: 1,
            group_block_idx: 0,
        };
        let body_offset = header.ser(&mut group[..], 0);
        assert_eq!(body_offset, RedoBlockHeader::SIZE);
        let body_offset = RedoGroupStartExtension::new(1, 1, cts, cts)
            .unwrap()
            .ser(&mut group[..], body_offset);
        group[body_offset] = 0x7f;
        file.seek(SeekFrom::Start(REDO_DEFAULT_DATA_START_OFFSET as u64))
            .unwrap();
        file.write_all(&group).unwrap();
        file.flush().unwrap();
    }

    fn write_redo_super_block_slot(file: &mut File, super_block: &RedoSuperBlock) {
        let mut slot = vec![0u8; REDO_SUPER_BLOCK_SLOT_SIZE];
        serialize_redo_super_block(&mut slot, super_block).unwrap();
        file.seek(SeekFrom::Start(slot_offset(super_block.slot_no) as u64))
            .unwrap();
        file.write_all(&slot).unwrap();
    }

    async fn expect_log_recovery_corruption(main_dir: &Path, log_file_stem: &str) {
        let err =
            match Engine::bootstrap(corruption_recovery_engine_config(main_dir, log_file_stem))
                .await
            {
                Ok(engine) => {
                    drop(engine);
                    panic!("engine startup should fail on redo corruption");
                }
                Err(err) => err,
            };
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::LogFileCorrupted),
            "{err:?}"
        );
    }

    fn log_recovery_for_engine<'a>(
        engine: &'a Engine,
        catalog_replay_start_ts: TrxID,
    ) -> RecoveryCoordinator<'a> {
        let resources = RecoveryResources::new(
            EnginePools::new(
                engine.inner().core.pools.meta.clone(),
                engine.inner().core.pools.index.clone(),
                engine.inner().core.pools.mem.clone(),
                engine.inner().core.pools.disk.clone(),
            ),
            engine.inner().table_fs.clone(),
            engine.inner().thread_pool.clone(),
            engine.inner().core.catalog(),
        );
        let config = &engine.inner().trx_sys.config;
        let file_prefix = config.file_prefix().unwrap();
        let mut recovery_config = RecoveryConfig::default().io_depth(1);
        recovery_config
            .validate(engine.inner().thread_pool.worker_threads())
            .unwrap();
        let mut recovery = resources
            .prepare(config, &recovery_config, file_prefix)
            .unwrap();
        recovery.timeline.catalog_replay_start_ts = catalog_replay_start_ts;
        recovery.timeline.replay_floor = MIN_SNAPSHOT_TS;
        recovery
    }

    async fn replay_test_log(
        recovery: &mut RecoveryCoordinator<'_>,
        log: TrxLog,
    ) -> RuntimeOrFatalResult<()> {
        let mut group = decode_log(&log);
        let trx = group.transactions.pop().unwrap();
        recovery.replay_transaction(trx, &group).await
    }

    async fn replay_test_dml(
        recovery: &mut RecoveryCoordinator<'_>,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        let mut group = decode_log(&TrxLog::new(redo_header(cts), RedoLogs { ddl: None, dml }));
        let DecodedTrxKind::Dml(tables) = group.transactions.pop().unwrap().kind else {
            panic!()
        };
        recovery.replay_decoded_dml(tables, cts, &group).await
    }

    async fn replay_test_table_dml(
        recovery: &mut RecoveryCoordinator<'_>,
        table_id: TableID,
        table: &Arc<Table>,
        rows: BTreeMap<RowID, RowRedo>,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        let dml = BTreeMap::from([(table_id, TableDML { rows })]);
        let mut group = decode_log(&TrxLog::new(redo_header(cts), RedoLogs { ddl: None, dml }));
        let DecodedTrxKind::Dml(mut tables) = group.transactions.pop().unwrap().kind else {
            panic!()
        };
        let DecodedTable::User(rows) = tables.remove(&table_id).unwrap() else {
            panic!()
        };
        recovery
            .replay_decoded_rows(table_id, table, rows, cts, &group)
            .await
    }

    fn redo_header(cts: TrxID) -> RedoHeader {
        RedoHeader {
            cts,
            trx_kind: RedoTrxKind::User,
        }
    }

    fn unknown_table_dml_log(table_id: TableID, cts: TrxID) -> TrxLog {
        let mut redo = RedoLogs::default();
        redo.insert_dml(
            table_id,
            RowRedo {
                row_id: RowID::new(0),
                kind: RowRedoKind::Insert(PageID::new(1), vec![Val::from(1u32)]),
            },
        );
        TrxLog::new(redo_header(cts), redo)
    }

    fn unknown_table_create_row_page_log(table_id: TableID, cts: TrxID) -> TrxLog {
        TrxLog::new(
            redo_header(cts),
            RedoLogs {
                ddl: Some(Box::new(DDLRedo::CreateRowPage {
                    table_id,
                    page_id: PageID::new(2),
                    start_row_id: RowID::new(0),
                    end_row_id: RowID::new(1),
                })),
                dml: BTreeMap::default(),
            },
        )
    }

    fn unknown_table_data_checkpoint_log(table_id: TableID, cts: TrxID) -> TrxLog {
        TrxLog::new(
            redo_header(cts),
            RedoLogs {
                ddl: Some(Box::new(DDLRedo::DataCheckpoint {
                    table_id,
                    pivor_row_id: RowID::new(0),
                    checkpoint_ts: cts,
                })),
                dml: BTreeMap::default(),
            },
        )
    }

    fn index_ddl_columns() -> Vec<StorageColumnSpec> {
        vec![
            StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
        ]
    }

    fn base_unique_index_spec() -> StorageIndexSpec {
        StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK)
    }

    fn added_index_spec() -> StorageIndexSpec {
        StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty())
    }

    fn created_index_metadata() -> Arc<TableMetadata> {
        let mut metadata = TableMetadata::try_new_with_index_slot_count(
            index_ddl_columns(),
            vec![
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    base_unique_index_spec(),
                ),
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    added_index_spec(),
                ),
            ],
            IndexSlot::new(2),
        )
        .unwrap();
        metadata.storage_epoch = 1;
        Arc::new(metadata)
    }

    fn dropped_index_metadata() -> Arc<TableMetadata> {
        let metadata = TableMetadata::try_new_with_index_slot_count(
            index_ddl_columns(),
            vec![
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                    base_unique_index_spec(),
                ),
                ActiveIndexSpec::new(
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    added_index_spec(),
                ),
            ],
            IndexSlot::new(2),
        )
        .unwrap()
        .without_index(IndexRef::new(IndexID::new(1), IndexSlot::new(1)))
        .unwrap();
        Arc::new(metadata)
    }

    async fn create_index_ddl_base_table(
        engine: &Engine,
        indexes: Vec<StorageIndexSpec>,
    ) -> TableID {
        let mut session = engine.new_session().unwrap();
        let table_id = session
            .create_table(StorageTableSpec::new(index_ddl_columns()), indexes)
            .await
            .unwrap()
            .table_id();
        drop(session);
        table_id
    }

    async fn commit_create_index_catalog_ddl(engine: &Engine, table_id: TableID) -> TrxID {
        let session = engine.new_session().unwrap();
        let mut trx = begin_catalog_test_trx(&session);
        assert!(
            engine
                .inner()
                .core
                .catalog()
                .storage
                .tables()
                .replace(
                    trx.trx(),
                    &TableObject {
                        table_id,
                        storage_epoch: 1,
                        next_column_id: 2,
                        next_index_id: 2,
                        index_slot_count: 2,
                    },
                )
                .await
                .unwrap()
        );
        engine
            .inner()
            .core
            .catalog()
            .storage
            .indexes()
            .insert(
                trx.trx(),
                &IndexObject {
                    table_id,
                    index: IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                    index_flags: StorageIndexFlags::empty(),
                    keys: vec![TableIndexKeySpec {
                        column_id: ColumnID::new(1),
                        column_ordinal: ColumnOrdinal::new(1),
                        order: IndexOrder::Asc,
                    }]
                    .into_boxed_slice(),
                },
            )
            .await
            .unwrap();
        let cts = trx
            .commit(DDLRedo::CreateIndex {
                table_id,
                index_id: IndexID::new(1),
                index_slot: IndexSlot::new(1),
            })
            .await;
        drop(session);
        cts
    }

    async fn commit_drop_index_catalog_ddl(engine: &Engine, table_id: TableID) -> TrxID {
        let session = engine.new_session().unwrap();
        let mut trx = begin_catalog_test_trx(&session);
        assert!(
            engine
                .inner()
                .core
                .catalog()
                .storage
                .tables()
                .replace(
                    trx.trx(),
                    &TableObject {
                        table_id,
                        storage_epoch: 1,
                        next_column_id: 2,
                        next_index_id: 2,
                        index_slot_count: 2,
                    },
                )
                .await
                .unwrap()
        );
        assert!(
            engine
                .inner()
                .core
                .catalog()
                .storage
                .indexes()
                .delete_by_id(trx.trx(), table_id, IndexID::new(1))
                .await
                .unwrap()
        );
        let cts = trx
            .commit(DDLRedo::DropIndex {
                table_id,
                index_id: IndexID::new(1),
                index_slot: IndexSlot::new(1),
            })
            .await;
        drop(session);
        cts
    }

    async fn publish_index_metadata_root(
        engine: &Engine,
        table_id: TableID,
        metadata: Arc<TableMetadata>,
        cts: TrxID,
    ) {
        let table = engine.inner().core.catalog().get_table(table_id).unwrap();
        let table_file = Arc::clone(table.file());
        let mut slots = table_file
            .active_root_unchecked()
            .secondary_index_slots
            .clone();
        slots.resize(metadata.idx.index_slot_count(), SecondaryIndexSlot::Vacant);
        for (index_slot, state) in slots.iter_mut().enumerate() {
            let index_slot = IndexSlot::try_from(index_slot).unwrap();
            match metadata.idx.index_spec(index_slot) {
                Some(index) => {
                    if state.index_id() != Some(index.index.id()) {
                        *state = SecondaryIndexSlot::Active {
                            index_id: index.index.id(),
                            root: SecondaryIndexRoot::Empty,
                        };
                    }
                }
                None => {
                    if let SecondaryIndexSlot::Active { index_id, .. } = *state {
                        *state = SecondaryIndexSlot::Retired(index_id);
                    }
                }
            }
        }
        let mut mutable = MutableTableFile::fork(
            &table_file,
            engine.inner().table_fs.background_writes(),
            table.disk_pool().clone(),
            engine.inner().core.pools.pool_guards().disk_guard().clone(),
        );
        mutable.replace_metadata_and_secondary_index_slots(metadata, slots);
        engine
            .inner()
            .trx_sys
            .publish_table_file_root(mutable, cts, false)
            .await
            .unwrap();
        drop(table);
    }

    async fn prepare_checkpointed_unique_row(engine: &Engine) -> (TableID, RowID) {
        let mut session = engine.new_session().unwrap();
        let table_id = session
            .create_table(
                StorageTableSpec::new(vec![
                    StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                ]),
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::UK,
                )],
            )
            .await
            .unwrap()
            .table_id();
        session.checkpoint_catalog().await.unwrap();
        let catalog_replay_start_ts = engine
            .inner()
            .core
            .catalog()
            .storage
            .checkpoint_snapshot()
            .catalog_replay_start_ts;
        let mut trx = session.begin_trx().unwrap();
        let row_id = trx
            .table_insert_mvcc(table_id, vec![Val::from(7u32), Val::from("cold-row")])
            .await
            .unwrap();
        trx.commit().await.unwrap();
        assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
        let mut checkpoint_session = engine.new_session().unwrap();
        assert_checkpoint_published(&mut checkpoint_session, table_id).await;
        let table = engine.inner().core.catalog().get_table(table_id).unwrap();
        let root = table.file().active_root_unchecked();
        assert!(root.heap_redo_start_ts > catalog_replay_start_ts);
        assert!(root.pivot_row_id > row_id);
        (table_id, row_id)
    }

    async fn assert_recovered_unique_tiers(
        engine: &Engine,
        table_id: TableID,
        key: &SelectKey,
        cold_row_id: RowID,
        hot_row_id: Option<RowID>,
    ) {
        let table = engine.inner().core.catalog().get_table(table_id).unwrap();
        let session = engine.new_session().unwrap();
        let pool_guards = session.pool_guards();
        let layout = table.layout_snapshot();
        let index = layout.secondary_index(key.index_slot).unwrap();
        let root = table
            .file()
            .active_root_unchecked()
            .secondary_index_root(key.index_slot);
        let disk = index
            .disk_runtime()
            .open_unique_at(root, pool_guards.disk_guard())
            .unwrap();
        assert_eq!(disk.lookup(&key.vals).await.unwrap(), Some(cold_row_id));
        assert_eq!(
            index
                .unique_mem()
                .unwrap()
                .bind(pool_guards.index_guard())
                .lookup(&key.vals, MIN_SNAPSHOT_TS)
                .await
                .unwrap(),
            hot_row_id.map(|row_id| (row_id, false)),
            "memory index must contain only replayed hot rows"
        );
        assert_eq!(
            index
                .bind_unique_unchecked(&pool_guards, root)
                .unwrap()
                .lookup(&key.vals, MIN_SNAPSHOT_TS)
                .await
                .unwrap(),
            Some((hot_row_id.unwrap_or(cold_row_id), false)),
            "combined lookup must prefer the hot replacement"
        );
    }

    async fn assert_float_unique_rows(
        engine: &Engine,
        table_id: TableID,
        rows: &[(Val, SelectKey, RowID)],
        stage: &str,
    ) {
        let mut session = engine.new_session().unwrap();
        for (stored, key, _) in rows {
            let probe = &key.vals[0];
            let mut trx = session.begin_trx().unwrap();
            let row = trx_select_row_mvcc_by_id(&mut trx, table_id, key, &[0, 1])
                .await
                .unwrap()
                .unwrap_found();
            assert_eq!(
                row,
                vec![stored.clone(), Val::from(7u32)],
                "{stage}, key={key:?}"
            );
            // Val equality canonicalizes zeros and NaNs, so inspect the stored bits too.
            assert_eq!(
                row[0].as_f32().map(f32::to_bits),
                stored.as_f32().map(f32::to_bits),
                "{stage}: f32 row bits"
            );
            assert_eq!(
                row[0].as_f64().map(f64::to_bits),
                stored.as_f64().map(f64::to_bits),
                "{stage}: f64 row bits"
            );
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .table_insert_mvcc(table_id, vec![probe.clone(), Val::from(7u32)])
                .await
                .unwrap_err();
            assert_eq!(
                err.operation_error(),
                Some(OperationError::DuplicateKey),
                "{stage}, key={key:?}"
            );
            trx.rollback().await.unwrap();
        }
    }

    async fn prepare_index_recovery(
        log_file_stem: &str,
        indexes: Vec<StorageIndexSpec>,
        checkpoint_order: CatalogCheckpointOrder,
    ) -> (TempDir, EngineConfig, Engine, TableID) {
        let dir = TempDir::new().unwrap();
        let config = lightweight_recovery_engine_config(dir.path(), log_file_stem);
        let engine = Engine::bootstrap(config.clone()).await.unwrap();
        if matches!(checkpoint_order, CatalogCheckpointOrder::BeforeTable) {
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
        }
        let table_id = create_index_ddl_base_table(&engine, indexes).await;
        if matches!(checkpoint_order, CatalogCheckpointOrder::AfterTable) {
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
        }
        (dir, config, engine, table_id)
    }

    async fn restart_and_assert_index_state(
        engine: Engine,
        config: EngineConfig,
        table_id: TableID,
        index_slot_count: u32,
        index_one_active: bool,
    ) {
        drop(engine);
        let recovered = Engine::bootstrap(config).await.unwrap();
        assert_recovered_index_state(&recovered, table_id, index_slot_count, index_one_active)
            .await;
    }

    async fn assert_recovered_index_state(
        engine: &Engine,
        table_id: TableID,
        index_slot_count: u32,
        index_one_active: bool,
    ) {
        let table = engine.inner().core.catalog().get_table(table_id).unwrap();
        let metadata = table.metadata();
        assert_eq!(metadata.idx.index_slot_count_u32(), index_slot_count);
        assert_eq!(metadata.idx.next_index_id(), u64::from(index_slot_count));
        let mut expected_indexes = vec![IndexRef::new(IndexID::new(0), IndexSlot::new(0))];
        if index_one_active {
            expected_indexes.push(IndexRef::new(IndexID::new(1), IndexSlot::new(1)));
        }
        assert_eq!(
            metadata
                .idx
                .active_indexes()
                .map(|(_, index)| index.index)
                .collect::<Vec<_>>(),
            expected_indexes,
            "runtime index identities"
        );

        let session = engine.new_session().unwrap();
        let table_obj = engine
            .inner()
            .core
            .catalog()
            .storage
            .tables()
            .find_uncommitted_by_id(&session.pool_guards(), table_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(table_obj.index_slot_count, index_slot_count);
        assert_eq!(table_obj.next_index_id, u64::from(index_slot_count));
        let indexes = engine
            .inner()
            .core
            .catalog()
            .storage
            .indexes()
            .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
            .await
            .unwrap();
        let mut catalog_indexes: Vec<_> = indexes.iter().map(|index| index.index).collect();
        catalog_indexes.sort_unstable_by_key(|index| index.id().get());
        assert_eq!(
            catalog_indexes, expected_indexes,
            "catalog index identities"
        );
        drop(session);
        drop(table);
    }

    fn corrupt_blob_header_kind(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        start_offset: u16,
    ) {
        let byte_offset = BLOCK_INTEGRITY_HEADER_SIZE
            + COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE
            + start_offset as usize;
        rewrite_page_with_checksum(path, page_id, |page| {
            page[byte_offset] = 0xFF;
        });
    }

    fn row_recovery_for_table(engine: &Engine, table_id: TableID) -> RecoveryCoordinator<'_> {
        let mut recovery = log_recovery_for_engine(engine, MIN_SNAPSHOT_TS);
        recovery.timeline.table_bounds.insert(
            table_id,
            TableReplayBounds {
                root_ts: MIN_SNAPSHOT_TS,
                heap_redo_start_ts: TrxID::new(10),
                deletion_cutoff_ts: TrxID::new(10),
            },
        );
        recovery
    }

    async fn replay_test_row(
        recovery: &mut RecoveryCoordinator<'_>,
        table_id: TableID,
        row_id: RowID,
        kind: RowRedoKind,
        cts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        let mut redo = RedoLogs::default();
        redo.insert_dml(table_id, RowRedo { row_id, kind });
        replay_test_dml(recovery, redo.dml, cts).await?;
        recovery.dispatcher.drain_all().await?;
        recovery.dispatcher.merge_counts(&mut recovery.report);
        Ok(())
    }

    fn clone_hot_redo_entry((id, row): (&RowID, &RowRedo)) -> (RowID, RowRedo) {
        let kind = match &row.kind {
            RowRedoKind::Insert(id, vals) => RowRedoKind::Insert(*id, vals.clone()),
            RowRedoKind::Update(id, cols) => RowRedoKind::Update(*id, cols.clone()),
            RowRedoKind::Delete(id) => RowRedoKind::Delete(*id),
            _ => unreachable!("hot replay fixture requires physical row redo"),
        };
        (*id, RowRedo { row_id: *id, kind })
    }

    fn assert_replay_integrity(
        err: RuntimeOrFatalError,
        expected: DataIntegrityError,
        reason: &str,
    ) {
        let RuntimeOrFatalError::Runtime(err) = err else {
            panic!("expected Runtime error, got {err:?}");
        };
        assert_eq!(err.downcast_ref::<DataIntegrityError>(), Some(&expected));
        let report = format!("{err:?}");
        assert!(report.contains(reason), "{report}");
    }

    fn assert_duplicate_recovery_page_allocation(
        err: RuntimeOrFatalError,
        table_id: TableID,
        page_id: PageID,
    ) {
        let RuntimeOrFatalError::Runtime(err) = err else {
            panic!("expected Runtime error, got {err:?}");
        };
        assert_eq!(
            err.downcast_ref::<InternalError>(),
            Some(&InternalError::BufferPageAlreadyAllocated)
        );
        let report = format!("{err:?}");
        assert!(report.contains("operation=allocate_page_at"), "{report}");
        assert!(report.contains(&format!("table_id={table_id}")), "{report}");
        assert!(report.contains(&format!("page_id={page_id}")), "{report}");
    }

    /// Purpose: Filter heap redo at the published row pivot and replay timestamp boundary.
    /// Expected: Rows below the pivot or older than the replay floor are skipped, while equality is eligible.
    #[test]
    fn test_heap_replay_requires_row_at_or_above_published_pivot() {
        let pivot_row_id = RowID::new(100);
        let replay_start_ts = TrxID::new(10);
        assert!(!should_replay_heap_row(
            RowID::new(99),
            pivot_row_id,
            TrxID::new(11),
            replay_start_ts,
        ));
        assert!(!should_replay_heap_row(
            RowID::new(100),
            pivot_row_id,
            TrxID::new(9),
            replay_start_ts,
        ));
        assert!(should_replay_heap_row(
            RowID::new(100),
            pivot_row_id,
            replay_start_ts,
            replay_start_ts,
        ));
    }

    /// Purpose: Classify catalog-style keyed redo incorrectly addressed to a user table.
    /// Expected: The invalid-payload report identifies the operation and row context without inventing a page identity.
    #[test]
    fn test_invalid_user_table_keyed_redo_reports_invalid_payload() {
        let table_id = TableID::new(42);
        let cts = TrxID::new(11);
        let cases = [
            (
                "DeleteByPrimaryKey",
                RowRedoKind::DeleteByPrimaryKey(CatalogSelectKey::new(
                    CatalogIndexNo::new(0),
                    vec![Val::from(42u64)],
                )),
            ),
            (
                "UpdateByPrimaryKey",
                RowRedoKind::UpdateByPrimaryKey(
                    CatalogSelectKey::new(CatalogIndexNo::new(0), vec![Val::from(42u64)]),
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from(7u64),
                    }],
                ),
            ),
        ];

        for (expected_kind, kind) in cases {
            let row = RowRedo {
                row_id: RowID::new(9),
                kind,
            };
            let err = invalid_user_table_keyed_redo(table_id, &row, cts);
            let report = format!("{err:?}");
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload),
                "{report}"
            );
            assert!(report.contains("key-based catalog redo"), "{report}");
            assert!(report.contains("table_id=42"), "{report}");
            assert!(!report.contains("page_id="), "{report}");
            assert!(report.contains("row_id=9"), "{report}");
            assert!(report.contains("cts=11"), "{report}");
            assert!(report.contains(expected_kind), "{report}");
        }
    }

    /// Purpose: Determine the table replay floor independently of root publication time.
    /// Expected: The earlier heap or deletion boundary controls replay even when the root timestamp precedes both.
    #[test]
    fn test_recovery_table_state_replay_start_uses_heap_and_deletion_floor() {
        let heap_first = TableReplayBounds {
            root_ts: TrxID::new(5),
            heap_redo_start_ts: TrxID::new(7),
            deletion_cutoff_ts: TrxID::new(11),
        };
        assert_eq!(heap_first.replay_start_ts(), TrxID::new(7));

        let deletion_first = TableReplayBounds {
            root_ts: TrxID::new(17),
            heap_redo_start_ts: TrxID::new(19),
            deletion_cutoff_ts: TrxID::new(13),
        };
        assert_eq!(deletion_first.replay_start_ts(), TrxID::new(13));
    }

    /// Purpose: Validate reloaded roots for ordinary and pending table creation.
    /// Expected: Initial roots are accepted, while pending creation requires a root published after its commit.
    #[test]
    fn test_create_table_root_ts_validation_accepts_initial_sts_root() {
        let root_before_create = TableReplayBounds {
            root_ts: TrxID::new(9),
            heap_redo_start_ts: TrxID::new(9),
            deletion_cutoff_ts: TrxID::new(9),
        };
        validate_create_table_reloaded_root_ts(
            USER_TABLE_ID_START,
            TrxID::new(10),
            root_before_create,
            false,
        )
        .unwrap();

        let pending_without_later_root = TableReplayBounds {
            root_ts: TrxID::new(10),
            heap_redo_start_ts: TrxID::new(10),
            deletion_cutoff_ts: TrxID::new(10),
        };
        let err = validate_create_table_reloaded_root_ts(
            USER_TABLE_ID_START,
            TrxID::new(10),
            pending_without_later_root,
            true,
        )
        .unwrap_err();
        assert_eq!(
            err.downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidRootInvariant)
        );
        let report = format!("{err:?}");
        assert!(report.contains("requires later table root"), "{report}");
        assert!(report.contains("root_ts=10"), "{report}");

        let pending_with_later_root = TableReplayBounds {
            root_ts: TrxID::new(11),
            heap_redo_start_ts: TrxID::new(10),
            deletion_cutoff_ts: TrxID::new(10),
        };
        validate_create_table_reloaded_root_ts(
            USER_TABLE_ID_START,
            TrxID::new(10),
            pending_with_later_root,
            true,
        )
        .unwrap();
    }

    /// Purpose: Apply heap replay filtering before validating page identity and replay registration.
    /// Expected: Obsolete redo is skipped, while eligible redo without required page state reports contextual errors.
    #[test]
    fn test_hot_replay_filters_before_requiring_registered_page() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                temp_dir.path(),
                "replay-sidecar-lookup",
            ))
            .await
            .unwrap();
            let table_id = create_index_ddl_base_table(&engine, vec![]).await;
            let mut recovery = row_recovery_for_table(&engine, table_id);
            for (kind, expected, reason) in [
                (
                    RowRedoKind::Insert(PageID::new(30), vec![Val::from(1i32), Val::from(2i32)]),
                    DataIntegrityError::InvalidRootInvariant,
                    "missing row replay state",
                ),
                (
                    RowRedoKind::Update(
                        PageID::new(30),
                        vec![UpdateCol {
                            idx: 0,
                            val: Val::from(1i32),
                        }],
                    ),
                    DataIntegrityError::InvalidRootInvariant,
                    "missing row replay state",
                ),
                (
                    RowRedoKind::Delete(Some(PageID::new(30))),
                    DataIntegrityError::InvalidRootInvariant,
                    "missing row replay state",
                ),
                (
                    RowRedoKind::Delete(None),
                    DataIntegrityError::InvalidPayload,
                    "requires page identity",
                ),
            ] {
                let table = engine.inner().core.catalog().get_table(table_id).unwrap();
                let rows = BTreeMap::from([(
                    RowID::new(0),
                    RowRedo {
                        row_id: RowID::new(0),
                        kind,
                    },
                )]);
                let skipped_rows = rows.iter().map(clone_hot_redo_entry).collect();
                replay_test_table_dml(&mut recovery, table_id, &table, skipped_rows, TrxID::new(9))
                    .await
                    .unwrap();
                let err =
                    replay_test_table_dml(&mut recovery, table_id, &table, rows, TrxID::new(10))
                        .await
                        .unwrap_err();
                let report = format!("{err:?}");
                for context in [
                    format!("table_id={table_id}"),
                    "row_id=0".to_owned(),
                    "cts=10".to_owned(),
                ] {
                    assert!(report.contains(&context), "{report}");
                }
                assert_replay_integrity(err, expected, reason);
                assert!(recovery.dispatcher.page_history.is_empty());
            }
        });
    }

    /// Purpose: Replay checkpointed cold rows without depending on hot-page history.
    /// Expected: Covered inserts are skipped, matching cold deletes are idempotent, and conflicting delete timestamps fail.
    #[test]
    fn test_cold_replay_ignores_hot_page_identity() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                temp_dir.path(),
                "replay-cold-without-sidecar",
            ))
            .await
            .unwrap();
            let table_id =
                create_index_ddl_base_table(&engine, vec![base_unique_index_spec()]).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.table_insert_mvcc(table_id, vec![Val::from(1i32), Val::from(2i32)])
                .await
                .unwrap();
            trx.commit().await.unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let root = table.file().active_root_unchecked();
            let row_id = RowID::new(0);
            assert!(row_id < root.pivot_row_id);
            let cts = root.deletion_cutoff_ts;
            let mut recovery = log_recovery_for_engine(&engine, MIN_SNAPSHOT_TS);
            recovery.track_loaded_table(table_id).unwrap();
            // Heap inserts below the pivot are checkpoint-covered despite newer CTS.
            let rows = BTreeMap::from([(
                row_id,
                RowRedo {
                    row_id,
                    kind: RowRedoKind::Insert(
                        PageID::new(30),
                        vec![Val::from(1i32), Val::from(2i32)],
                    ),
                },
            )]);
            replay_test_table_dml(&mut recovery, table_id, &table, rows, cts)
                .await
                .unwrap();
            for page_id in [None, Some(PageID::new(30))] {
                replay_test_row(
                    &mut recovery,
                    table_id,
                    row_id,
                    RowRedoKind::Delete(page_id),
                    cts,
                )
                .await
                .unwrap();
                assert!(recovery.dispatcher.page_history.is_empty());
                assert!(matches!(
                    table.deletion_buffer().get(row_id),
                    Some(DeleteMarker::Committed(actual)) if actual == cts
                ));
            }
            let err = replay_test_row(
                &mut recovery,
                table_id,
                row_id,
                RowRedoKind::Delete(None),
                cts + 1,
            )
            .await
            .unwrap_err();
            let report = format!("{err:?}");
            assert!(
                report.contains("operation=recover_cold_row_delete"),
                "{report}"
            );
            assert_replay_integrity(
                err,
                DataIntegrityError::InvalidRootInvariant,
                "conflicting committed cold-row deletion",
            );
        });
    }

    /// Purpose: Reject malformed redo row ranges before they become trusted replay descriptors.
    /// Expected: Empty, reversed, oversized, and displaced ranges return integrity errors without publishing a descriptor or counting a reconstructed page.
    #[test]
    fn test_replay_create_row_page_validates_captured_range() {
        smol::block_on(async {
            for (case, start, end, reason) in [
                ("empty", 70, 70, "invalid row range"),
                ("reversed", 71, 70, "invalid row range"),
                (
                    "oversized",
                    70,
                    70 + u64::from(u16::MAX) + 1,
                    "invalid row range",
                ),
                ("gap", 71, 141, "range mismatch"),
                ("overlap", 69, 139, "range mismatch"),
            ] {
                let temp_dir = TempDir::new().unwrap();
                let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                    temp_dir.path(),
                    "replay-page-range",
                ))
                .await
                .unwrap();
                let table_id = create_index_ddl_base_table(&engine, vec![]).await;
                let mut recovery = row_recovery_for_table(&engine, table_id);
                let original_page = PageID::new(30);
                recovery
                    .replay_create_row_page_ddl(
                        table_id,
                        original_page,
                        RowID::new(0),
                        RowID::new(70),
                        BTreeMap::new(),
                        TrxID::new(10),
                    )
                    .await
                    .unwrap();
                let invalid_page = PageID::new(31);
                let error = recovery
                    .replay_create_row_page_ddl(
                        table_id,
                        invalid_page,
                        RowID::new(start),
                        RowID::new(end),
                        BTreeMap::new(),
                        TrxID::new(11),
                    )
                    .await
                    .expect_err(case);
                assert!(
                    matches!(&error, RuntimeOrFatalError::Runtime(report) if report.current_context() == &RuntimeError::Recovery),
                    "{case}: {error:?}"
                );
                let report = format!("{error:?}");
                assert!(
                    report.contains(&format!("table_id={table_id}")),
                    "{case}: {report}"
                );
                assert!(
                    report.contains(&format!("page_id={invalid_page}")),
                    "{case}: {report}"
                );
                assert_replay_integrity(error, DataIntegrityError::InvalidPayload, reason);
                let history = &recovery.dispatcher.page_history[&table_id];
                assert_eq!(history.len(), 1, "{case}");
                assert!(history.contains_key(&original_page), "{case}");
                assert!(!history.contains_key(&invalid_page), "{case}");
                assert_eq!(recovery.report.work.hot_pages_reconstructed, 1, "{case}");
            }
        });
    }

    /// Purpose: Rebuild indexes from sparse replay histories with differing page allocation orders.
    /// Expected: Only live rows enter indexes, replay histories are consumed, and active version maps retain their identity.
    #[test]
    fn test_replay_rebuild_consumes_sidecars_and_retains_version_maps() {
        smol::block_on(async {
            for ids in [[30, 31, 32], [32, 31, 30]] {
                let temp_dir = TempDir::new().unwrap();
                let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                    temp_dir.path(),
                    "replay-sidecar-rebuild",
                ))
                .await
                .unwrap();
                let table_id = create_index_ddl_base_table(
                    &engine,
                    vec![base_unique_index_spec(), added_index_spec()],
                )
                .await;
                let table = engine.inner().core.catalog().get_table(table_id).unwrap();
                let mut recovery = row_recovery_for_table(&engine, table_id);
                let mut maps = Vec::new();
                for (n, id) in ids.into_iter().enumerate() {
                    let page_id = PageID::new(id);
                    let first = RowID::new(n as u64 * 70);
                    let create_cts = TrxID::new(10 + n as u64);
                    recovery
                        .replay_create_row_page_ddl(
                            table_id,
                            page_id,
                            first,
                            first + 70,
                            BTreeMap::new(),
                            create_cts,
                        )
                        .await
                        .unwrap();
                    let page = table
                        .row_store
                        .must_get_row_page_shared(&recovery.resources.pool_guards, page_id)
                        .await
                        .unwrap();
                    assert_eq!(page.unwrap_vmap().create_cts(), create_cts);
                    maps.push((page_id, page.unwrap_vmap() as *const _ as usize, create_cts));
                }
                for (row_id, page_id, key, cts) in [
                    (69, ids[0], 69, 13),
                    (0, ids[0], 0, 14),
                    (72, ids[1], 72, 15),
                ] {
                    replay_test_row(
                        &mut recovery,
                        table_id,
                        RowID::new(row_id),
                        RowRedoKind::Insert(
                            PageID::new(page_id),
                            vec![Val::from(key), Val::from(7i32)],
                        ),
                        TrxID::new(cts),
                    )
                    .await
                    .unwrap();
                }
                replay_test_row(
                    &mut recovery,
                    table_id,
                    RowID::new(69),
                    RowRedoKind::Delete(Some(PageID::new(ids[0]))),
                    TrxID::new(16),
                )
                .await
                .unwrap();
                let err = recovery
                    .replay_create_row_page_ddl(
                        table_id,
                        PageID::new(ids[0]),
                        RowID::new(0),
                        RowID::new(70),
                        BTreeMap::new(),
                        TrxID::new(17),
                    )
                    .await
                    .unwrap_err();
                assert_duplicate_recovery_page_allocation(err, table_id, PageID::new(ids[0]));
                let original = &recovery.dispatcher.page_history[&table_id][&PageID::new(ids[0])];
                assert!(original.is_inserted(69));
                assert!(original.is_inserted(0));
                assert_eq!(recovery.dispatcher.page_history[&table_id].len(), 3);
                recovery.rebuild_hot_indexes().await.unwrap();
                assert!(recovery.dispatcher.page_history.is_empty());
                assert_eq!(recovery.report.work.hot_pages_reconstructed, 3);
                assert_eq!(recovery.report.work.index_rebuild_pages, 3);
                assert_eq!(recovery.report.work.index_entries_inserted, 4);
                assert_eq!(recovery.report.work.hot_inserts, 3);
                assert_eq!(recovery.report.work.hot_deletes, 1);
                for (page_id, address, create_cts) in maps {
                    let page = table
                        .row_store
                        .must_get_row_page_shared(&recovery.resources.pool_guards, page_id)
                        .await
                        .unwrap();
                    assert_eq!(page.unwrap_vmap() as *const _ as usize, address);
                    assert_eq!(page.unwrap_vmap().create_cts(), create_cts);
                    assert_eq!(*page.unwrap_vmap().read_state(), RowPageState::Active);
                    for slot in 0..70 {
                        assert!(page.unwrap_vmap().read_latch(slot).is_none());
                    }
                }
                let layout = table.layout_snapshot();
                let unique = layout
                    .expect_secondary_index(IndexRef::new(IndexID::new(0), IndexSlot::new(0)))
                    .unique_mem()
                    .unwrap()
                    .bind(recovery.resources.pool_guards.index_guard());
                let non_unique = layout
                    .expect_secondary_index(IndexRef::new(IndexID::new(1), IndexSlot::new(1)))
                    .non_unique_mem()
                    .unwrap()
                    .bind(recovery.resources.pool_guards.index_guard());
                for key in [0i32, 69, 72] {
                    let row_id = RowID::new(key as u64);
                    assert_eq!(
                        unique
                            .lookup(&[Val::from(key)], MIN_SNAPSHOT_TS)
                            .await
                            .unwrap(),
                        (key != 69).then_some((row_id, false))
                    );
                    assert_eq!(
                        non_unique
                            .lookup_unique(&[Val::from(7i32)], row_id, MIN_SNAPSHOT_TS)
                            .await
                            .unwrap(),
                        (key != 69).then_some(true)
                    );
                }
            }
        });
    }

    /// Purpose: Reuse a dropped table's page identity during recovery.
    /// Expected: Dropping removes old insertion history and the replacement table can replay into a fresh page.
    #[test]
    fn test_replay_drop_removes_sidecars_before_page_reuse() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                temp_dir.path(),
                "replay-sidecar-reuse",
            ))
            .await
            .unwrap();
            let first_table = create_index_ddl_base_table(&engine, vec![]).await;
            let second_table = create_index_ddl_base_table(&engine, vec![]).await;
            let mut recovery = row_recovery_for_table(&engine, first_table);
            let bounds = recovery.timeline.table_bounds[&first_table];
            recovery.timeline.table_bounds.insert(second_table, bounds);
            let page_id = PageID::new(30);
            recovery
                .replay_create_row_page_ddl(
                    first_table,
                    page_id,
                    RowID::new(0),
                    RowID::new(70),
                    BTreeMap::new(),
                    TrxID::new(10),
                )
                .await
                .unwrap();
            let mut redo = RedoLogs::default();
            redo.insert_dml(
                first_table,
                RowRedo {
                    row_id: RowID::new(0),
                    kind: RowRedoKind::Insert(page_id, vec![Val::from(1i32), Val::from(2i32)]),
                },
            );
            replay_test_dml(&mut recovery, redo.dml, TrxID::new(11))
                .await
                .unwrap();
            recovery
                .replay_drop_table_ddl(first_table, BTreeMap::new(), TrxID::new(12))
                .await
                .unwrap();
            assert!(!recovery.dispatcher.page_history.contains_key(&first_table));
            assert_eq!(recycled_snapshot(&recovery.dispatcher).0, 1);
            recovery
                .replay_create_row_page_ddl(
                    second_table,
                    page_id,
                    RowID::new(0),
                    RowID::new(70),
                    BTreeMap::new(),
                    TrxID::new(13),
                )
                .await
                .unwrap();
            let replay = &recovery.dispatcher.page_history[&second_table][&page_id];
            assert!(!replay.is_inserted(0));
            replay_test_row(
                &mut recovery,
                second_table,
                RowID::new(0),
                RowRedoKind::Insert(page_id, vec![Val::from(3i32), Val::from(4i32)]),
                TrxID::new(14),
            )
            .await
            .unwrap();
            recovery.rebuild_hot_indexes().await.unwrap();
            assert!(recovery.dispatcher.page_history.is_empty());
            assert_eq!(recovery.report.work.hot_pages_reconstructed, 2);
            assert_eq!(recovery.report.work.index_rebuild_pages, 1);
            assert_eq!(recovery.report.work.index_entries_inserted, 0);
        });
    }

    /// Purpose: Rebuild indexes after a transaction replaces a row across pages and updates another table.
    /// Expected: Unique and non-unique lookups reflect final row images without retaining obsolete keys.
    #[test]
    fn test_cross_page_replacement_rebuilds_final_unique_and_non_unique_indexes() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                temp.path(),
                "parallel-index-rebuild",
            ))
            .await
            .unwrap();
            let first = create_index_ddl_base_table(
                &engine,
                vec![base_unique_index_spec(), added_index_spec()],
            )
            .await;
            let other = create_index_ddl_base_table(
                &engine,
                vec![base_unique_index_spec(), added_index_spec()],
            )
            .await;
            let mut recovery = row_recovery_for_table(&engine, first);
            recovery
                .timeline
                .table_bounds
                .insert(other, recovery.timeline.table_bounds[&first]);
            for (table, page, start) in [(first, 30, 0), (first, 31, 70), (other, 32, 0)] {
                recovery
                    .replay_create_row_page_ddl(
                        table,
                        PageID::new(page),
                        RowID::new(start),
                        RowID::new(start + 70),
                        BTreeMap::new(),
                        TrxID::new(10),
                    )
                    .await
                    .unwrap();
            }
            let mut redo = RedoLogs::default();
            for (table, page) in [(first, 30), (other, 32)] {
                redo.insert_dml(
                    table,
                    RowRedo {
                        row_id: RowID::new(0),
                        kind: RowRedoKind::Insert(
                            PageID::new(page),
                            vec![Val::from(7i32), Val::from(8i32)],
                        ),
                    },
                );
            }
            replay_test_log(
                &mut recovery,
                TrxLog::new(redo_header(TrxID::new(11)), redo),
            )
            .await
            .unwrap();
            recovery.dispatcher.progress().unwrap();
            // One committed transaction replaces a row across pages while also
            // updating a second table. Indexes must only see the final images.
            let mut redo = RedoLogs::default();
            redo.insert_dml(
                first,
                RowRedo {
                    row_id: RowID::new(0),
                    kind: RowRedoKind::Delete(Some(PageID::new(30))),
                },
            );
            redo.insert_dml(
                first,
                RowRedo {
                    row_id: RowID::new(75),
                    kind: RowRedoKind::Insert(
                        PageID::new(31),
                        vec![Val::from(7i32), Val::from(9i32)],
                    ),
                },
            );
            redo.insert_dml(
                other,
                RowRedo {
                    row_id: RowID::new(0),
                    kind: RowRedoKind::Update(
                        PageID::new(32),
                        vec![UpdateCol {
                            idx: 1,
                            val: Val::from(9i32),
                        }],
                    ),
                },
            );
            replay_test_log(
                &mut recovery,
                TrxLog::new(redo_header(TrxID::new(12)), redo),
            )
            .await
            .unwrap();
            recovery.dispatcher.drain_all().await.unwrap();
            recovery.dispatcher.merge_counts(&mut recovery.report);
            recovery.rebuild_hot_indexes().await.unwrap();
            assert_eq!(recovery.report.work.hot_inserts, 3);
            assert_eq!(recovery.report.work.hot_updates, 1);
            assert_eq!(recovery.report.work.hot_deletes, 1);
            assert_eq!(recovery.report.work.index_entries_inserted, 4);
            for (id, row) in [(first, RowID::new(75)), (other, RowID::new(0))] {
                let table = engine.inner().core.catalog().get_table(id).unwrap();
                let layout = table.layout_snapshot();
                let unique = layout
                    .expect_secondary_index(IndexRef::new(IndexID::new(0), IndexSlot::new(0)))
                    .unique_mem()
                    .unwrap()
                    .bind(recovery.resources.pool_guards.index_guard());
                assert_eq!(
                    unique
                        .lookup(&[Val::from(7i32)], MIN_SNAPSHOT_TS)
                        .await
                        .unwrap(),
                    Some((row, false))
                );
                let non_unique = layout
                    .expect_secondary_index(IndexRef::new(IndexID::new(1), IndexSlot::new(1)))
                    .non_unique_mem()
                    .unwrap()
                    .bind(recovery.resources.pool_guards.index_guard());
                assert_eq!(
                    non_unique
                        .lookup_unique(&[Val::from(9i32)], row, MIN_SNAPSHOT_TS)
                        .await
                        .unwrap(),
                    Some(true)
                );
                assert_eq!(
                    non_unique
                        .lookup_unique(&[Val::from(8i32)], row, MIN_SNAPSHOT_TS)
                        .await
                        .unwrap(),
                    None
                );
            }
        });
    }

    /// Purpose: Scope recovery DDL barriers while replay jobs on multiple tables are blocked.
    /// Expected: New pages can be created independently, and dropping waits only for the affected table before page reuse.
    #[test]
    fn test_creation_and_drop_fence_only_the_affected_table() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                temp.path(),
                "parallel-ddl",
            ))
            .await
            .unwrap();
            let first = create_index_ddl_base_table(&engine, vec![]).await;
            let other = create_index_ddl_base_table(&engine, vec![]).await;
            let mut recovery = row_recovery_for_table(&engine, first);
            recovery
                .timeline
                .table_bounds
                .insert(other, recovery.timeline.table_bounds[&first]);
            for (table, page) in [(first, 30), (other, 32)] {
                recovery
                    .replay_create_row_page_ddl(
                        table,
                        PageID::new(page),
                        RowID::new(0),
                        RowID::new(70),
                        BTreeMap::new(),
                        TrxID::new(10),
                    )
                    .await
                    .unwrap();
            }
            let table = engine.inner().core.catalog().get_table(first).unwrap();
            let other_table = engine.inner().core.catalog().get_table(other).unwrap();
            let held = table
                .row_store
                .must_get_row_page_exclusive(&recovery.resources.pool_guards, PageID::new(30))
                .await
                .unwrap();
            let other_held = other_table
                .row_store
                .must_get_row_page_exclusive(&recovery.resources.pool_guards, PageID::new(32))
                .await
                .unwrap();
            drop(table); // DROP must own the only runtime handle after jobs drain.
            let mut redo = RedoLogs::default();
            for (table, page) in [(first, 30), (other, 32)] {
                redo.insert_dml(
                    table,
                    RowRedo {
                        row_id: RowID::new(0),
                        kind: RowRedoKind::Insert(
                            PageID::new(page),
                            vec![Val::from(1i32), Val::from(2i32)],
                        ),
                    },
                );
            }
            replay_test_log(
                &mut recovery,
                TrxLog::new(redo_header(TrxID::new(11)), redo),
            )
            .await
            .unwrap();
            recovery.dispatcher.progress().unwrap();
            let mut redo = RedoLogs::default();
            redo.insert_dml(
                first,
                RowRedo {
                    row_id: RowID::new(1),
                    kind: RowRedoKind::Insert(
                        PageID::new(30),
                        vec![Val::from(3i32), Val::from(4i32)],
                    ),
                },
            );
            replay_test_log(
                &mut recovery,
                TrxLog::new(redo_header(TrxID::new(12)), redo),
            )
            .await
            .unwrap();
            // Creation of a distinct page must proceed while an older page is blocked.
            recovery
                .replay_create_row_page_ddl(
                    first,
                    PageID::new(31),
                    RowID::new(70),
                    RowID::new(140),
                    BTreeMap::new(),
                    TrxID::new(13),
                )
                .await
                .unwrap();
            let error = recovery
                .replay_create_row_page_ddl(
                    first,
                    PageID::new(30),
                    RowID::new(0),
                    RowID::new(70),
                    BTreeMap::new(),
                    TrxID::new(14),
                )
                .await
                .unwrap_err();
            assert_duplicate_recovery_page_allocation(error, first, PageID::new(30));
            let mut ddl =
                Box::pin(recovery.replay_drop_table_ddl(first, BTreeMap::new(), TrxID::new(15)));
            assert!(futures::FutureExt::now_or_never(ddl.as_mut()).is_none());
            drop(held);
            ddl.await.unwrap(); // Both submitted and partial pending T batches finished.
            assert!(!recovery.dispatcher.page_history.contains_key(&first));
            assert!(futures::FutureExt::now_or_never(recovery.dispatcher.drain(other)).is_none());
            assert!(!recovery.dispatcher.page_history[&other].contains_key(&PageID::new(32)));
            // T's PageID can be reused before U's unrelated earlier batch finishes.
            recovery
                .replay_create_row_page_ddl(
                    other,
                    PageID::new(30),
                    RowID::new(70),
                    RowID::new(140),
                    BTreeMap::new(),
                    TrxID::new(16),
                )
                .await
                .unwrap();
            assert!(!recovery.dispatcher.page_history[&other][&PageID::new(30)].is_inserted(0));
            drop(other_held);
            recovery.dispatcher.drain_all().await.unwrap();
            recovery.dispatcher.merge_counts(&mut recovery.report);
            assert_eq!(recovery.report.work.hot_inserts, 3);
            assert_eq!(recovery.report.work.user_row_ops_seen, 3);
            recovery.rebuild_hot_indexes().await.unwrap();
            assert_eq!(recovery.report.work.index_rebuild_pages, 2);
        });
    }

    /// Purpose: Clean up replay histories when index reconstruction encounters invalid state.
    /// Expected: Duplicate keys and orphaned table histories fail with contextual errors and leave no retained histories.
    #[test]
    fn test_failed_index_rebuild_consumes_remaining_sidecars() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                temp_dir.path(),
                "replay-sidecar-failure",
            ))
            .await
            .unwrap();
            let table_id =
                create_index_ddl_base_table(&engine, vec![base_unique_index_spec()]).await;
            let mut recovery = row_recovery_for_table(&engine, table_id);
            for n in 0..3 {
                let page_id = PageID::new(30 + n);
                recovery
                    .replay_create_row_page_ddl(
                        table_id,
                        page_id,
                        RowID::new(n * 70),
                        RowID::new((n + 1) * 70),
                        BTreeMap::new(),
                        TrxID::new(10 + n * 2),
                    )
                    .await
                    .unwrap();
                if n < 2 {
                    replay_test_row(
                        &mut recovery,
                        table_id,
                        RowID::new(n * 70),
                        RowRedoKind::Insert(page_id, vec![Val::from(1i32), Val::from(2i32)]),
                        TrxID::new(11 + n * 2),
                    )
                    .await
                    .unwrap();
                }
            }
            let err = recovery.rebuild_hot_indexes().await.unwrap_err();
            assert_replay_integrity(
                err,
                DataIntegrityError::UnexpectedRecoveryDuplicateKey,
                "duplicate",
            );
            assert!(recovery.dispatcher.page_history.is_empty());
            // An orphaned group must fail even if it contains no populated rows.
            let missing_table_id = USER_TABLE_ID_START + 999;
            recovery
                .dispatcher
                .page_history
                .entry(missing_table_id)
                .or_default()
                .insert(
                    PageID::new(33),
                    RowReplayState::new(RowPageDescriptor {
                        page_id: PageID::new(33),
                        start_row_id: RowID::new(0),
                        end_row_id: RowID::new(70),
                    }),
                );
            let err = recovery.rebuild_hot_indexes().await.unwrap_err();
            assert_replay_integrity(
                err,
                DataIntegrityError::InvalidRootInvariant,
                "requires live runtime",
            );
            assert!(recovery.dispatcher.page_history.is_empty());
        });
    }

    /// Purpose: Skip redo for unknown tables when catalog or global replay floors already cover it.
    /// Expected: Obsolete row and DDL records succeed without table lookup and skipped operations remain accounted for.
    #[test]
    fn test_log_recovery_skips_checkpoint_covered_unknown_user_table_redo() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                temp_dir.path().to_path_buf(),
                "recover-unknown-table-skip",
            ))
            .await
            .unwrap();
            let unknown_table_id = USER_TABLE_ID_START + 142;
            let mut recovery = log_recovery_for_engine(&engine, TrxID::new(10));

            replay_test_log(
                &mut recovery,
                unknown_table_dml_log(unknown_table_id, TrxID::new(9)),
            )
            .await
            .unwrap();

            replay_test_log(
                &mut recovery,
                unknown_table_create_row_page_log(unknown_table_id, TrxID::new(9)),
            )
            .await
            .unwrap();

            replay_test_log(
                &mut recovery,
                unknown_table_data_checkpoint_log(unknown_table_id, TrxID::new(9)),
            )
            .await
            .unwrap();

            assert_eq!(recovery.report.work.user_row_ops_seen, 1);
            // The coarse floor still counts decoded row maps, including DML carried by DDL.
            recovery.timeline.replay_floor = TrxID::new(10);
            let mut redo = RedoLogs::default();
            for (table_id, count) in [(CATALOG_TABLE_ID_START, 2), (unknown_table_id, 3)] {
                for row in 0..count {
                    redo.insert_dml(
                        table_id,
                        RowRedo {
                            row_id: RowID::new(row),
                            kind: RowRedoKind::Delete(None),
                        },
                    );
                }
            }
            redo.ddl = Some(Box::new(DDLRedo::DropTable(unknown_table_id)));
            replay_test_log(&mut recovery, TrxLog::new(redo_header(TrxID::new(9)), redo))
                .await
                .unwrap();
            recovery.report.finish_transaction(Duration::ZERO);
            assert!(!recovery.report.saturated);
            assert_eq!(recovery.report.work.catalog_row_ops_seen, 2);
            assert_eq!(recovery.report.work.catalog_row_ops_skipped, 2);
            assert_eq!(recovery.report.work.user_row_ops_seen, 4);
            assert_eq!(recovery.report.work.user_row_ops_skipped, 4);
            drop(recovery);
            drop(engine);
        });
    }

    /// Purpose: Validate user-table existence at the inclusive catalog replay boundary.
    /// Expected: Eligible DML and page-creation redo for unknown tables fail with recovery-ordering context.
    #[test]
    fn test_log_recovery_fails_unknown_user_table_redo_at_catalog_boundary() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                temp_dir.path().to_path_buf(),
                "recover-unknown-table-invalid",
            ))
            .await
            .unwrap();
            let unknown_table_id = USER_TABLE_ID_START + 143;

            let mut dml_recovery = log_recovery_for_engine(&engine, TrxID::new(10));
            let err = replay_test_log(
                &mut dml_recovery,
                unknown_table_dml_log(unknown_table_id, TrxID::new(10)),
            )
            .await
            .unwrap_err();
            let RuntimeOrFatalError::Runtime(err) = err else {
                panic!("expected Runtime error, got {err:?}");
            };
            assert_eq!(*err.current_context(), RuntimeError::Recovery);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(report.contains("invalid recovery ordering"), "{report}");
            assert!(report.contains("replay user table DML"), "{report}");

            let mut ddl_recovery = log_recovery_for_engine(&engine, TrxID::new(10));
            let err = replay_test_log(
                &mut ddl_recovery,
                unknown_table_create_row_page_log(unknown_table_id, TrxID::new(10)),
            )
            .await
            .unwrap_err();
            let RuntimeOrFatalError::Runtime(err) = err else {
                panic!("expected Runtime error, got {err:?}");
            };
            assert_eq!(*err.current_context(), RuntimeError::Recovery);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(report.contains("invalid recovery ordering"), "{report}");
            assert!(report.contains("replay create row page"), "{report}");

            drop(ddl_recovery);
            drop(dml_recovery);
            drop(engine);
        });
    }

    /// Purpose: Recover a provisional index creation followed by a later durable index creation.
    /// Expected: The provisional slot remains vacant and the later index retains its allocation across restarts and checkpointing.
    #[test]
    fn test_recovery_quarantines_provisional_create_before_later_durable_create() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                main_dir.clone(),
                "recover-provisional-create-index",
            ))
            .await
            .unwrap();
            let table_id =
                create_index_ddl_base_table(&engine, vec![base_unique_index_spec()]).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let _cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            drop(engine);

            let recovered = Engine::bootstrap(lightweight_recovery_engine_config(
                main_dir.clone(),
                "recover-provisional-create-index",
            ))
            .await
            .unwrap();
            assert_recovered_index_state(&recovered, table_id, 1, false).await;

            let mut session = recovered.new_session().unwrap();
            let later_id = session
                .create_index(table_id, added_index_spec())
                .await
                .unwrap();
            assert_eq!(later_id, IndexID::new(2));
            let table = recovered
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .unwrap();
            assert_eq!(table.metadata().idx.index_slot_count_u32(), 3);
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_slots[1],
                SecondaryIndexSlot::Vacant
            );
            assert!(matches!(
                table.file().active_root_unchecked().secondary_index_slots[2],
                SecondaryIndexSlot::Active {
                    index_id,
                    root: SecondaryIndexRoot::Empty,
                } if index_id == later_id
            ));
            drop(table);
            drop(session);
            drop(recovered);

            let recovered = Engine::bootstrap(lightweight_recovery_engine_config(
                main_dir.clone(),
                "recover-provisional-create-index",
            ))
            .await
            .unwrap();
            let table = recovered
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .unwrap();
            assert_eq!(
                table
                    .metadata()
                    .idx
                    .resolve_index_id(later_id)
                    .unwrap()
                    .slot(),
                IndexSlot::new(2)
            );
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_slots[1],
                SecondaryIndexSlot::Vacant
            );
            drop(table);
            recovered
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            drop(recovered);

            let recovered = Engine::bootstrap(lightweight_recovery_engine_config(
                main_dir,
                "recover-provisional-create-index",
            ))
            .await
            .unwrap();
            let table = recovered
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .unwrap();
            assert_eq!(
                table
                    .metadata()
                    .idx
                    .resolve_index_id(later_id)
                    .unwrap()
                    .slot(),
                IndexSlot::new(2)
            );
            assert_eq!(
                table.file().active_root_unchecked().secondary_index_slots[1],
                SecondaryIndexSlot::Vacant
            );
            drop(table);
            drop(recovered);
        });
    }

    /// Purpose: Recover index creation backed by a published table root.
    /// Expected: Catalog and runtime metadata expose the created index and retain its allocation history.
    #[test]
    fn test_recovery_replays_root_proven_create_index_redo() {
        smol::block_on(async {
            let (_dir, config, engine, table_id) = prepare_index_recovery(
                "recover-create-index",
                vec![base_unique_index_spec()],
                CatalogCheckpointOrder::AfterTable,
            )
            .await;
            let create_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), create_cts)
                .await;
            restart_and_assert_index_state(engine, config, table_id, 2, true).await;
        });
    }

    /// Purpose: Recover index removal backed by a published table root.
    /// Expected: The dropped index is absent while its allocated slot remains part of the metadata history.
    #[test]
    fn test_recovery_replays_root_proven_drop_index_redo() {
        smol::block_on(async {
            let (_dir, config, engine, table_id) = prepare_index_recovery(
                "recover-drop-index",
                vec![base_unique_index_spec(), added_index_spec()],
                CatalogCheckpointOrder::AfterTable,
            )
            .await;
            let drop_cts = commit_drop_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, dropped_index_metadata(), drop_cts)
                .await;
            restart_and_assert_index_state(engine, config, table_id, 2, false).await;
        });
    }

    /// Purpose: Recover durable creation and subsequent removal of the same index.
    /// Expected: The final index is absent and its allocated identity and slot remain consumed.
    #[test]
    fn test_recovery_replays_create_then_drop_index_allocation_history() {
        smol::block_on(async {
            let (_dir, config, engine, table_id) = prepare_index_recovery(
                "recover-create-drop-index",
                vec![base_unique_index_spec()],
                CatalogCheckpointOrder::AfterTable,
            )
            .await;
            let create_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), create_cts)
                .await;
            let drop_cts = commit_drop_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, dropped_index_metadata(), drop_cts)
                .await;
            restart_and_assert_index_state(engine, config, table_id, 2, false).await;
        });
    }

    /// Purpose: Recover a table created after the catalog checkpoint with a later index-creation root.
    /// Expected: Table bootstrap accepts the later root and recovers the created index.
    #[test]
    fn test_recovery_replays_new_table_with_later_create_index_root() {
        smol::block_on(async {
            let (_dir, config, engine, table_id) = prepare_index_recovery(
                "recover-new-table-create-index",
                vec![base_unique_index_spec()],
                CatalogCheckpointOrder::BeforeTable,
            )
            .await;
            let create_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), create_cts)
                .await;
            restart_and_assert_index_state(engine, config, table_id, 2, true).await;
        });
    }

    /// Purpose: Recover a new table whose later roots include index creation and removal.
    /// Expected: The table retains index allocation history while recovering only its surviving index.
    #[test]
    fn test_recovery_replays_new_table_with_later_create_drop_index_roots() {
        smol::block_on(async {
            let (_dir, config, engine, table_id) = prepare_index_recovery(
                "recover-new-table-create-drop-index",
                vec![base_unique_index_spec()],
                CatalogCheckpointOrder::BeforeTable,
            )
            .await;
            let create_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), create_cts)
                .await;
            let drop_cts = commit_drop_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, dropped_index_metadata(), drop_cts)
                .await;
            restart_and_assert_index_state(engine, config, table_id, 2, false).await;
        });
    }

    /// Purpose: Checkpoint catalog redo for an index creation lacking durable root proof.
    /// Expected: The checkpoint advances past the provisional DDL without making the index visible after restart.
    #[test]
    fn test_catalog_checkpoint_skips_unproved_index_ddl_catalog_dml() {
        smol::block_on(async {
            let (_dir, config, engine, table_id) = prepare_index_recovery(
                "checkpoint-skip-provisional-index",
                vec![base_unique_index_spec()],
                CatalogCheckpointOrder::AfterTable,
            )
            .await;
            let create_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let snapshot = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert!(snapshot.catalog_replay_start_ts > create_cts);
            restart_and_assert_index_state(engine, config, table_id, 1, false).await;
        });
    }

    /// Purpose: Checkpoint catalog redo for an index creation backed by a published root.
    /// Expected: The checkpoint advances past the DDL and restart retains the durable index.
    #[test]
    fn test_catalog_checkpoint_includes_root_proven_index_ddl_catalog_dml() {
        smol::block_on(async {
            let (_dir, config, engine, table_id) = prepare_index_recovery(
                "checkpoint-include-durable-index",
                vec![base_unique_index_spec()],
                CatalogCheckpointOrder::AfterTable,
            )
            .await;
            let create_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), create_cts)
                .await;
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let snapshot = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert!(snapshot.catalog_replay_start_ts > create_cts);
            restart_and_assert_index_state(engine, config, table_id, 2, true).await;
        });
    }

    /// Purpose: Skip a corrupt sealed redo segment whose timestamp range is obsolete.
    /// Expected: Bootstrap avoids decoding the segment and subsequent runtime timestamps exceed its recorded maximum.
    #[test]
    fn test_log_recover_skips_corrupt_obsolete_sealed_segment_and_seeds_cts() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path();
            let log_file_stem = "recover-sealed-skip-corrupt";
            let replay_floor = prepare_checkpointed_recovery_floor(main_dir, log_file_stem).await;
            let skipped_max_cts = TrxID::new(replay_floor.as_u64() - 1);
            write_bad_checksum_redo_file(main_dir, log_file_stem, skipped_max_cts, true);

            let recovered =
                Engine::bootstrap(corruption_recovery_engine_config(main_dir, log_file_stem))
                    .await
                    .unwrap();
            let report = recovered.recovery_report();
            assert_report_accounting(report);
            assert_eq!(report.work.redo_segments_discovered, 1);
            assert_eq!(report.work.redo_segments_selected, 0);
            assert_eq!(report.redo.transactions_decoded, 0);
            assert_eq!(report.work.user_row_ops_seen, 0);
            let mut session = recovered.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            assert!(
                trx.sts() > skipped_max_cts,
                "next runtime timestamp {} must exceed skipped sealed max CTS {skipped_max_cts}",
                trx.sts()
            );
            trx.rollback().await.unwrap();
            drop(session);
            drop(recovered);
        });
    }

    /// Purpose: Validate a sealed segment whose timestamp range reaches the replay floor.
    /// Expected: Bootstrap scans the boundary segment and rejects its checksum corruption.
    #[test]
    fn test_log_recover_scans_boundary_sealed_segment_and_fails_corruption() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path();
            let log_file_stem = "recover-sealed-boundary-corrupt";
            let replay_floor = prepare_checkpointed_recovery_floor(main_dir, log_file_stem).await;
            write_bad_checksum_redo_file(main_dir, log_file_stem, replay_floor, true);

            expect_log_recovery_corruption(main_dir, log_file_stem).await;
        });
    }

    /// Purpose: Recover from a group-start checksum failure in an unsealed redo tail.
    /// Expected: Bootstrap succeeds and retains an open redo file without publishing a sealed alternate superblock.
    #[test]
    fn test_log_recover_discards_unsealed_group_start_checksum_tail() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path();
            let log_file_stem = "recover-unsealed-checksum-tail";
            let replay_floor = prepare_checkpointed_recovery_floor(main_dir, log_file_stem).await;
            write_bad_checksum_redo_file(main_dir, log_file_stem, replay_floor, false);

            let recovered =
                Engine::bootstrap(corruption_recovery_engine_config(main_dir, log_file_stem))
                    .await
                    .unwrap();
            let bytes = fs::read(main_dir.join(format!("{log_file_stem}.00000000"))).unwrap();
            let open = parse_redo_super_block(&bytes[..REDO_SUPER_BLOCK_SLOT_SIZE], 0, 0).unwrap();
            assert!(!open.is_sealed());
            let slot1 = &bytes[REDO_SUPER_BLOCK_SLOT_SIZE..REDO_DEFAULT_DATA_START_OFFSET];
            let err = parse_redo_super_block(slot1, 0, 1).unwrap_err();
            assert_eq!(*err.current_context(), DataIntegrityError::InvalidMagic);
            drop(recovered);
        });
    }

    /// Purpose: Restart after reclaiming redo files older than the persisted retention marker.
    /// Expected: Bootstrap accepts the retained suffix despite the missing obsolete prefix.
    #[test]
    fn test_log_recover_accepts_missing_prefix_below_first_retained_redo() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path();
            let log_file_stem = "recover-retained-prefix";
            let (engine, _) =
                prepare_engine_with_retained_redo_suffix(main_dir, log_file_stem).await;
            drop(engine);
            remove_redo_file(main_dir, log_file_stem, 0);

            let recovered = Engine::bootstrap(retention_marker_recovery_engine_config(
                main_dir,
                log_file_stem,
            ))
            .await
            .unwrap();
            drop(recovered);
        });
    }

    /// Purpose: Restart with the first required retained redo segment missing.
    /// Expected: Bootstrap reports a redo sequence gap with the owning log-access error.
    #[test]
    fn test_log_recover_rejects_missing_first_retained_redo_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path();
            let log_file_stem = "recover-missing-retained";
            let (engine, _) =
                prepare_engine_with_retained_redo_suffix(main_dir, log_file_stem).await;
            drop(engine);
            remove_redo_file(main_dir, log_file_stem, 0);
            remove_redo_file(main_dir, log_file_stem, 1);

            let err = match Engine::bootstrap(retention_marker_recovery_engine_config(
                main_dir,
                log_file_stem,
            ))
            .await
            {
                Ok(engine) => {
                    drop(engine);
                    panic!("engine startup should reject missing first retained redo file");
                }
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Runtime);
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::RedoLogAccess)
            );
            assert_eq!(
                err.report().downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::RedoLogSequenceGap)
            );
        });
    }

    /// Purpose: Scan catalog checkpoint redo after reclaiming an obsolete prefix.
    /// Expected: The scan accepts the retained suffix and reaches the durable upper boundary.
    #[test]
    fn test_catalog_checkpoint_scan_accepts_missing_prefix_below_first_retained_redo() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path();
            let log_file_stem = "scan-retained-prefix";
            let (engine, table_id) =
                prepare_engine_with_retained_redo_suffix(main_dir, log_file_stem).await;
            remove_redo_file(main_dir, log_file_stem, 0);

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.table_insert_mvcc(table_id, vec![Val::from(10_000), Val::from(10_000)])
                .await
                .unwrap();
            trx.commit().await.unwrap();
            drop(session);

            let batch = engine
                .inner()
                .core
                .catalog()
                .scan_checkpoint_batch(
                    engine.inner().trx_sys.persisted_watermark_cts(),
                    engine
                        .inner()
                        .trx_sys
                        .catalog_checkpoint_scan_config()
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                batch.stop_reason,
                crate::catalog::CatalogCheckpointScanStopReason::ReachedDurableUpper
            );
            drop(engine);
        });
    }

    /// Purpose: Report recovery work when bootstrapping an empty storage root.
    /// Expected: Recovery records no replay work and its report remains unchanged after shutdown.
    #[test]
    fn test_log_recover_empty() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(recovery_engine_config(main_dir, "recover1"))
                .await
                .unwrap();

            let report = *engine.recovery_report();
            assert_report_accounting(&report);
            assert_eq!(report.work, RecoveryWorkCounts::default());
            assert_eq!(report.redo.transactions_decoded, 0);
            engine.shutdown();
            assert_eq!(engine.recovery_report(), &report);
            drop(engine);
        })
    }

    /// Purpose: Recover a table definition with unique and ordered composite indexes.
    /// Expected: Restart restores the table and its complete column and index metadata.
    #[test]
    fn test_log_recover_ddl() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(recovery_engine_config(main_dir.clone(), "recover2"))
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_spec = StorageTableSpec::new(vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
            ]);
            let index_specs = vec![
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                StorageIndexSpec::new(
                    vec![
                        StorageIndexKey {
                            column_ordinal: ColumnOrdinal::new(1),
                            order: IndexOrder::Desc,
                        },
                        StorageIndexKey::new(2),
                    ],
                    StorageIndexFlags::empty(),
                ),
            ];
            let expected_metadata =
                TableMetadata::try_new(table_spec.columns.clone(), index_specs.clone())
                    .expect("valid table metadata");

            let table_id = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap()
                .table_id();

            drop(session);
            drop(engine);

            // second recovery.
            let engine = Engine::bootstrap(recovery_engine_config(main_dir, "recover2"))
                .await
                .unwrap();

            assert!(engine.inner().core.catalog().get_table(table_id).is_some());
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            assert_eq!(table.metadata().as_ref(), &expected_metadata);

            drop(table);
            drop(engine);
        })
    }

    /// Purpose: Recover committed inserts, updates, and deletes across multiple transactions.
    /// Expected: Restart preserves surviving row values and deleted-key absence with consistent replay accounting.
    #[test]
    fn test_log_recover_dml() {
        smol::block_on(async {
            const DML_SIZE: usize = 1000;
            const INS_STEP: usize = 10;
            const UPD_STEP: usize = 11;
            const DEL_STEP: usize = 13;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(recovery_engine_config(main_dir.clone(), "recover3"))
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_spec = StorageTableSpec::new(vec![
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ]);

            let table_id = session
                .create_table(
                    table_spec,
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();

            let s: String = repeat_n('0', 100).collect();
            // insert
            for i in (0..DML_SIZE).step_by(INS_STEP) {
                let mut trx = session.begin_trx().unwrap();
                for j in i..i + INS_STEP {
                    let res = trx
                        .table_insert_mvcc(table_id, vec![Val::from(j as u32), Val::from(&s[..])])
                        .await;
                    assert!(res.is_ok());
                }
                trx.commit().await.unwrap();
            }
            // update
            let s2: String = repeat_n('2', 100).collect();
            for i in (0..DML_SIZE).step_by(UPD_STEP) {
                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(IndexSlot::new(0), vec![Val::from(i as u32)]);
                let uc = UpdateCol {
                    idx: 1,
                    val: Val::from(&s2[..]),
                };
                let res = trx_update_row_by_id(&mut trx, table_id, &key, vec![uc]).await;
                assert!(matches!(res, Ok(UniqueMutationOutcome::Updated(_))));
                trx.commit().await.unwrap();
            }
            // delete
            for i in (0..DML_SIZE).step_by(DEL_STEP) {
                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(IndexSlot::new(0), vec![Val::from(i as u32)]);
                let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
                assert!(matches!(res, Ok(UniqueMutationOutcome::Deleted)));
                trx.commit().await.unwrap();
            }

            drop(session);
            drop(engine);

            // second recovery.
            let engine = Engine::bootstrap(recovery_engine_config(main_dir, "recover3"))
                .await
                .unwrap();

            let report = engine.recovery_report();
            assert_report_accounting(report);
            assert_eq!(report.work.hot_inserts, DML_SIZE as u64);
            assert_eq!(report.work.hot_updates, DML_SIZE.div_ceil(UPD_STEP) as u64);
            assert_eq!(report.work.hot_deletes, DML_SIZE.div_ceil(DEL_STEP) as u64);
            assert_eq!(report.work.user_row_ops_skipped, 0);
            assert_eq!(
                report.work.index_entries_inserted,
                (DML_SIZE - DML_SIZE.div_ceil(DEL_STEP)) as u64
            );
            assert!(report.work.catalog_row_ops_applied > 0);
            assert_eq!(
                report.work.catalog_row_ops_seen,
                report.work.catalog_row_ops_applied
            );
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut session = engine.new_session().unwrap();
            let mut rows = 0usize;
            {
                let layout = table.layout_snapshot();
                let pivot_row_id = table.file().active_root_unchecked().pivot_row_id;
                table
                    .accessor_with_layout(&layout)
                    .mem_scan_uncommitted_from(
                        &session.pool_guards(),
                        pivot_row_id,
                        |_metadata, row| {
                            assert!(row.row_id().as_usize() <= DML_SIZE);
                            rows += if row.is_deleted() { 0 } else { 1 };
                            true
                        },
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(rows, DML_SIZE - (DML_SIZE / DEL_STEP + 1));

            let mut trx = session.begin_trx().unwrap();
            for id in 0..DML_SIZE {
                let key = SelectKey::new(IndexSlot::new(0), vec![Val::from(id as u32)]);
                let row = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1])
                    .await
                    .unwrap();
                if id % DEL_STEP == 0 {
                    assert_eq!(row, SelectMvcc::NotFound, "deleted key={id}");
                } else {
                    let expected = if id % UPD_STEP == 0 { &s2 } else { &s };
                    assert_eq!(
                        row,
                        SelectMvcc::Found(vec![Val::from(id as u32), Val::from(expected.as_str())]),
                        "surviving key={id}"
                    );
                }
            }
            trx.commit().await.unwrap();

            drop(session);
            drop(table);
            drop(engine);
        })
    }

    /// Purpose: Bootstrap table metadata from a published catalog checkpoint.
    /// Expected: The table is restored without applying covered catalog row redo and startup reports stay immutable.
    #[test]
    fn test_log_recover_bootstraps_catalog_from_checkpoint() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(recovery_engine_config(main_dir.clone(), "recover4"))
                .await
                .unwrap();

            let initial_report = *engine.recovery_report();
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::U32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            assert_eq!(engine.recovery_report(), &initial_report);
            let snap = engine.inner().core.catalog().storage.checkpoint_snapshot();
            assert!(snap.catalog_replay_start_ts > MIN_SNAPSHOT_TS);

            drop(session);
            drop(engine);

            let engine = Engine::bootstrap(recovery_engine_config(main_dir, "recover4"))
                .await
                .unwrap();

            assert_report_accounting(engine.recovery_report());
            assert_eq!(engine.recovery_report().work.checkpoint_user_tables, 1);
            assert_eq!(engine.recovery_report().work.catalog_row_ops_applied, 0);
            assert!(engine.inner().core.catalog().get_table(table_id).is_some());
            drop(engine);
        })
    }

    /// Purpose: Distinguish replayed silent watermarks from checkpointed durable replay floors.
    /// Expected: Only checkpointed watermarks advance recovered table floors; uncheckpointed rows are merely replayed.
    #[test]
    fn test_recovery_uses_silent_watermark_only_after_catalog_checkpoint() {
        async fn prepare_silent_watermark(
            main_dir: PathBuf,
            log_file_stem: &'static str,
            checkpoint_catalog: bool,
        ) -> (TableID, TableRedoReplayFloor, TableRedoReplayFloor) {
            let engine =
                Engine::bootstrap(lightweight_recovery_engine_config(main_dir, log_file_stem))
                    .await
                    .unwrap();
            let table_id =
                create_index_ddl_base_table(&engine, vec![base_unique_index_spec()]).await;
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let root_floor = table.redo_replay_floor_snapshot();
            let mut session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut session, table.table_id()).await;
            let watermark = engine
                .inner()
                .core
                .catalog()
                .storage
                .table_replay_silent_watermarks()
                .find_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .expect("silent checkpoint should write catalog row");
            let watermark_floor = TableRedoReplayFloor {
                heap_redo_start_ts: watermark.heap_redo_start_ts,
                deletion_cutoff_ts: watermark.deletion_cutoff_ts,
            };
            assert!(watermark_floor.heap_redo_start_ts > root_floor.heap_redo_start_ts);
            assert!(watermark_floor.deletion_cutoff_ts > root_floor.deletion_cutoff_ts);
            if checkpoint_catalog {
                let mut durability_trx = session.begin_trx().unwrap();
                durability_trx
                    .table_insert_mvcc(table_id, vec![Val::from(1i32), Val::from(1i32)])
                    .await
                    .unwrap();
                durability_trx.commit().await.unwrap();
                session.checkpoint_catalog().await.unwrap();
                assert_eq!(
                    engine
                        .inner()
                        .core
                        .catalog()
                        .storage
                        .checkpointed_silent_watermarks()
                        .get(&table_id)
                        .copied(),
                    Some(watermark_floor)
                );
            }
            drop(table);
            drop(session);
            drop(engine);
            (table_id, root_floor, watermark_floor)
        }

        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let unchecked_dir = temp_dir.path().join("unchecked");
            fs::create_dir_all(&unchecked_dir).unwrap();
            let checked_dir = temp_dir.path().join("checked");
            fs::create_dir_all(&checked_dir).unwrap();

            let (table_id, root_floor, watermark_floor) =
                prepare_silent_watermark(unchecked_dir.clone(), "recover-silent-unchecked", false)
                    .await;
            let recovered = Engine::bootstrap(lightweight_recovery_engine_config(
                unchecked_dir,
                "recover-silent-unchecked",
            ))
            .await
            .unwrap();
            let snapshot = recovered
                .inner()
                .core
                .catalog()
                .storage
                .checkpoint_snapshot();
            let (live_before_catalog_checkpoint, _) = recovered
                .inner()
                .core
                .catalog()
                .snapshot_user_table_redo_floors(snapshot.catalog_replay_start_ts);
            assert_eq!(live_before_catalog_checkpoint.len(), 1);
            assert_eq!(live_before_catalog_checkpoint[0].floor, root_floor);
            assert!(
                recovered
                    .inner()
                    .core
                    .catalog()
                    .storage
                    .checkpointed_silent_watermarks()
                    .get(&table_id)
                    .is_none()
            );
            let session = recovered.new_session().unwrap();
            let replayed_watermark = recovered
                .inner()
                .core
                .catalog()
                .storage
                .table_replay_silent_watermarks()
                .find_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .expect("recovery should replay uncheckpointed watermark row");
            assert_eq!(
                replayed_watermark.heap_redo_start_ts,
                watermark_floor.heap_redo_start_ts
            );
            assert_eq!(
                replayed_watermark.deletion_cutoff_ts,
                watermark_floor.deletion_cutoff_ts
            );
            drop(session);
            drop(recovered);

            let (_table_id, _, expected_floor) =
                prepare_silent_watermark(checked_dir.clone(), "recover-silent-checked", true).await;
            let recovered = Engine::bootstrap(lightweight_recovery_engine_config(
                checked_dir,
                "recover-silent-checked",
            ))
            .await
            .unwrap();
            let snapshot = recovered
                .inner()
                .core
                .catalog()
                .storage
                .checkpoint_snapshot();
            let (live_after_catalog_checkpoint, _) = recovered
                .inner()
                .core
                .catalog()
                .snapshot_user_table_redo_floors(snapshot.catalog_replay_start_ts);
            assert_eq!(live_after_catalog_checkpoint.len(), 1);
            assert_eq!(live_after_catalog_checkpoint[0].floor, expected_floor);
            drop(recovered);
        })
    }

    /// Purpose: Read a checkpointed row through its persisted secondary index after restart.
    /// Expected: Disk and combined lookups return the cold row while hot row pages and the memory index remain empty.
    #[test]
    fn test_log_recover_reads_checkpointed_secondary_from_disk_tree_without_mem_backfill() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let config = recovery_engine_config(temp_dir.path(), "recover5");
            let engine = Engine::bootstrap(config.clone()).await.unwrap();
            let (table_id, cold_row_id) = prepare_checkpointed_unique_row(&engine).await;
            drop(engine);

            let engine = Engine::bootstrap(config).await.unwrap();
            let mut session = engine.new_session().unwrap();
            assert_eq!(session.total_row_pages(table_id).await.unwrap(), 0);
            let key = SelectKey::new(IndexSlot::new(0), vec![Val::from(7u32)]);
            assert_recovered_unique_tiers(&engine, table_id, &key, cold_row_id, None).await;
            let mut trx = session.begin_trx().unwrap();
            let row = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
            assert_eq!(
                row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("cold-row")]
            );
            trx.commit().await.unwrap();
        });
    }

    /// Purpose: Protect float unique-key equality across hot rows, checkpointing, and restart.
    /// Expected: Equivalent zeros and NaNs find the same row and reject duplicates in scalar and composite indexes while stored bits survive.
    #[test]
    fn test_float_unique_keys_survive_checkpoint_and_reopen() {
        smol::block_on(async {
            let cases = [
                (
                    ValKind::F32,
                    [
                        (Val::from(-0.0f32), Val::from(0.0f32)),
                        (
                            Val::from(f32::from_bits(0xffc0_1234)),
                            Val::from(f32::from_bits(0x7f80_0001)),
                        ),
                    ],
                ),
                (
                    ValKind::F64,
                    [
                        (Val::from(-0.0f64), Val::from(0.0f64)),
                        (
                            Val::from(f64::from_bits(0xfff8_0000_0000_1234)),
                            Val::from(f64::from_bits(0x7ff0_0000_0000_0001)),
                        ),
                    ],
                ),
            ];
            for (kind, pairs) in cases {
                for composite in [false, true] {
                    let dir = TempDir::new().unwrap();
                    let config = lightweight_recovery_engine_config(dir.path(), "float_keys");
                    let engine = Engine::bootstrap(config.clone()).await.unwrap();
                    let mut session = engine.new_session().unwrap();
                    let mut keys = vec![StorageIndexKey::new(0)];
                    if composite {
                        keys.push(StorageIndexKey::new(1));
                    }
                    let table_id = session
                        .create_table(
                            StorageTableSpec::new(vec![
                                StorageColumnSpec::new(kind, StorageColumnFlags::empty()),
                                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                            ]),
                            vec![StorageIndexSpec::new(keys, StorageIndexFlags::UK)],
                        )
                        .await
                        .unwrap()
                        .table_id();
                    session.checkpoint_catalog().await.unwrap();
                    let mut trx = session.begin_trx().unwrap();
                    let mut rows = vec![];
                    for (stored, probe) in &pairs {
                        let row_id = trx
                            .table_insert_mvcc(table_id, vec![stored.clone(), Val::from(7u32)])
                            .await
                            .unwrap();
                        let mut vals = vec![probe.clone()];
                        if composite {
                            vals.push(Val::from(7u32));
                        }
                        let key = SelectKey::new(IndexSlot::new(0), vals);
                        rows.push((stored.clone(), key, row_id));
                    }
                    trx.commit().await.unwrap();
                    assert_float_unique_rows(&engine, table_id, &rows, "hot").await;
                    assert_freeze_created(
                        session.freeze_table(table_id, usize::MAX).await.unwrap(),
                    );
                    assert_checkpoint_published(&mut session, table_id).await;
                    assert_float_unique_rows(&engine, table_id, &rows, "checkpointed").await;
                    drop(session);
                    drop(engine);

                    let engine = Engine::bootstrap(config).await.unwrap();
                    // Failed inserts can allocate hot pages; check the persisted index
                    // and absence of matching hot entries directly.
                    for (_, key, row_id) in &rows {
                        assert_recovered_unique_tiers(&engine, table_id, key, *row_id, None).await;
                    }
                    assert_float_unique_rows(&engine, table_id, &rows, "reopened").await;
                }
            }
        });
    }

    /// Purpose: Recover a hot replacement sharing a deleted cold row's unique key.
    /// Expected: Combined lookup selects the hot replacement even though the persisted index still contains the cold identity.
    #[test]
    fn test_log_recover_rebuilds_hot_unique_memindex_over_checkpointed_cold_duplicate() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let config = recovery_engine_config(temp_dir.path(), "recover11");
            let engine = Engine::bootstrap(config.clone()).await.unwrap();
            let (table_id, cold_row_id) = prepare_checkpointed_unique_row(&engine).await;
            let key = SelectKey::new(IndexSlot::new(0), vec![Val::from(7u32)]);
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let delete = trx_delete_row_by_id(&mut trx, table_id, &key).await;
            assert!(matches!(delete, Ok(UniqueMutationOutcome::Deleted)));
            trx.commit().await.unwrap();
            let mut trx = session.begin_trx().unwrap();
            let hot_row_id = trx
                .table_insert_mvcc(table_id, vec![Val::from(7u32), Val::from("hot-row")])
                .await
                .unwrap();
            assert_ne!(cold_row_id, hot_row_id);
            trx.commit().await.unwrap();
            drop(session);
            drop(engine);

            let engine = Engine::bootstrap(config).await.unwrap();
            let mut session = engine.new_session().unwrap();
            assert!(session.total_row_pages(table_id).await.unwrap() > 0);
            assert_recovered_unique_tiers(&engine, table_id, &key, cold_row_id, Some(hot_row_id))
                .await;
            let mut trx = session.begin_trx().unwrap();
            let row = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
            assert_eq!(
                row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("hot-row")]
            );
            trx.commit().await.unwrap();
        });
    }

    /// Purpose: Recover a cold deletion among rows sharing a non-unique index key.
    /// Expected: Logical lookups hide only the deleted row while retaining the other equal-key rows.
    #[test]
    fn test_log_recover_non_unique_disk_tree_scan_suppresses_exact_cold_delete() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                main_dir.clone(),
                "recover12",
            ))
            .await
            .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ]),
                    vec![
                        StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty(),
                        ),
                    ],
                )
                .await
                .unwrap()
                .table_id();

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut same_row_ids = Vec::new();
            let mut trx = session.begin_trx().unwrap();
            for id in [1u32, 2, 3] {
                let insert = trx
                    .table_insert_mvcc(
                        table.table_id(),
                        vec![Val::from(id), Val::from("same-name")],
                    )
                    .await;
                let Ok(row_id) = insert else {
                    panic!("same-name insert should succeed");
                };
                same_row_ids.push(row_id);
            }
            trx.commit().await.unwrap();

            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table.table_id()).await;
            assert!(
                same_row_ids
                    .iter()
                    .all(|row_id| *row_id < table.file().active_root_unchecked().pivot_row_id)
            );

            let delete_key = SelectKey::new(IndexSlot::new(0), vec![Val::from(2u32)]);
            let mut trx = session.begin_trx().unwrap();
            let delete = trx_delete_row_by_id(&mut trx, table.table_id(), &delete_key).await;
            assert!(matches!(delete, Ok(UniqueMutationOutcome::Deleted)));
            trx.commit().await.unwrap();

            drop(table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine =
                Engine::bootstrap(lightweight_recovery_engine_config(main_dir, "recover12"))
                    .await
                    .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut session = engine.new_session().unwrap();
            assert_eq!(session.total_row_pages(table.table_id()).await.unwrap(), 0);

            let name_key = SelectKey::new(IndexSlot::new(1), vec![Val::from("same-name")]);
            let layout = table.layout_snapshot();
            let index_slot = name_key.index_slot;
            let non_unique = layout.secondary_index(index_slot).unwrap();
            let range = non_unique
                .key_encoder()
                .encode_non_unique_equal_range(&name_key.vals);
            let root = table
                .file()
                .active_root_unchecked()
                .secondary_index_root(index_slot);
            let disk_rows = {
                let pool_guards = session.pool_guards();
                let disk = non_unique
                    .disk_runtime()
                    .open_non_unique_at(root, pool_guards.disk_guard())
                    .unwrap();
                let mut stream = disk.scan_candidate_stream(&range);
                let mut rows = Vec::new();
                while let Some(batch) = stream.next_batch().await.unwrap() {
                    rows.extend(batch.into_iter().map(|candidate| candidate.row_id));
                }
                rows
            };
            assert_eq!(disk_rows, same_row_ids);
            drop(layout);
            match table.deletion_buffer().get(same_row_ids[1]).unwrap() {
                DeleteMarker::Committed(_) => (),
                DeleteMarker::Ref(_) => panic!("recovered cold delete should be committed"),
            }

            let mut trx = session.begin_trx().unwrap();
            let rows = trx
                .table_index_lookup_mvcc(
                    crate::TableIndex(table.table_id(), IndexID::new(1)),
                    &name_key.vals,
                    &[0, 1],
                )
                .await
                .unwrap()
                .unwrap_rows();
            assert_eq!(
                rows,
                vec![
                    vec![Val::from(1u32), Val::from("same-name")],
                    vec![Val::from(3u32), Val::from("same-name")],
                ]
            );
            let deleted =
                trx_select_row_mvcc_by_id(&mut trx, table.table_id(), &delete_key, &[0, 1]).await;
            assert!(matches!(deleted, Ok(SelectMvcc::NotFound)));
            trx.commit().await.unwrap();

            drop(table);
            drop(session);
            drop(engine);
        })
    }

    /// Purpose: Recover full-table mutations that delete, update, and retain both cold and hot rows.
    /// Expected: Scans and index lookups expose the committed final values and exclude deleted rows.
    #[test]
    fn test_log_recover_full_table_mutation_mixed_cold_hot_actions() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let log_file_stem = "recover-full-table-mutation";
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                main_dir.clone(),
                log_file_stem,
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ]),
                    vec![
                        StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty(),
                        ),
                    ],
                )
                .await
                .unwrap()
                .table_id();
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let mut trx = session.begin_trx().unwrap();
            for id in 0u32..3 {
                trx.table_insert_mvcc(table_id, vec![Val::from(id), Val::from("cold")])
                    .await
                    .unwrap();
            }
            trx.commit().await.unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let mut trx = session.begin_trx().unwrap();
            for id in [10u32, 11] {
                trx.table_insert_mvcc(table_id, vec![Val::from(id), Val::from("hot")])
                    .await
                    .unwrap();
            }
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let outcome = trx
                .table_mutate_mvcc(table_id, |row| -> CallbackResult<_> {
                    Ok(match row.val(0)?.as_u32().unwrap() {
                        0 | 10 => RowMutation::Delete,
                        1 => RowMutation::Update(vec![UpdateCol {
                            idx: 1,
                            val: Val::from("cold-updated"),
                        }]),
                        11 => RowMutation::Update(vec![UpdateCol {
                            idx: 1,
                            val: Val::from("hot-updated"),
                        }]),
                        2 => RowMutation::Skip,
                        _ => unreachable!(),
                    })
                })
                .await
                .unwrap();
            assert_eq!(outcome.delete_count, 2);
            assert_eq!(outcome.update_count, 2);
            trx.commit().await.unwrap();

            drop(session);
            drop(engine);

            let engine =
                Engine::bootstrap(lightweight_recovery_engine_config(main_dir, log_file_stem))
                    .await
                    .unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let mut stream = trx
                .table_scan_mvcc_stream(table_id, &[0, 1], |_| -> CallbackResult<_> {
                    Ok(ScanRowDecision::Include)
                })
                .await
                .unwrap();
            let mut rows = Vec::new();
            while let Some(row) = stream.next().await.unwrap() {
                rows.push((
                    row[0].as_u32().unwrap(),
                    row[1].as_str().unwrap().to_owned(),
                ));
            }
            drop(stream);
            rows.sort_unstable();
            assert_eq!(
                rows,
                vec![
                    (1, "cold-updated".to_owned()),
                    (2, "cold".to_owned()),
                    (11, "hot-updated".to_owned()),
                ]
            );
            let cold_rows = trx
                .table_index_lookup_mvcc(
                    crate::TableIndex(table_id, crate::IndexID::new(1)),
                    &[Val::from("cold")],
                    &[0],
                )
                .await
                .unwrap()
                .unwrap_rows();
            assert_eq!(cold_rows, vec![vec![Val::from(2u32)]]);
            for deleted_id in [0u32, 10] {
                let deleted = trx
                    .table_lookup_unique_mvcc(
                        crate::TableIndex(table_id, crate::IndexID::new(0)),
                        &[Val::from(deleted_id)],
                        &[0],
                    )
                    .await
                    .unwrap();
                assert_eq!(deleted, SelectMvcc::NotFound);
            }
            trx.commit().await.unwrap();

            drop(session);
            drop(engine);
        })
    }

    /// Purpose: Combine a checkpointed cold row with later committed heap redo during restart.
    /// Expected: Both persisted and replayed rows are readable with their original values.
    #[test]
    fn test_log_recover_replays_post_checkpoint_heap_redo_after_bootstrap() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(recovery_engine_config(main_dir.clone(), "recover6"))
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let catalog_replay_start_ts = engine
                .inner()
                .core
                .catalog()
                .storage
                .checkpoint_snapshot()
                .catalog_replay_start_ts;
            assert!(catalog_replay_start_ts > MIN_SNAPSHOT_TS);

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();

            let mut trx = session.begin_trx().unwrap();
            let insert = trx
                .table_insert_mvcc(
                    table.table_id(),
                    vec![Val::from(7u32), Val::from("cold-row")],
                )
                .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table.table_id()).await;
            let root_after_checkpoint = table.file().active_root_unchecked();
            assert!(root_after_checkpoint.heap_redo_start_ts > catalog_replay_start_ts);

            let mut trx = session.begin_trx().unwrap();
            let insert = trx
                .table_insert_mvcc(
                    table.table_id(),
                    vec![Val::from(8u32), Val::from("hot-row")],
                )
                .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            drop(table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine = Engine::bootstrap(recovery_engine_config(main_dir, "recover6"))
                .await
                .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut session = engine.new_session().unwrap();
            assert!(session.total_row_pages(table.table_id()).await.unwrap() > 0);

            let mut trx = session.begin_trx().unwrap();

            let cold_key = SelectKey::new(IndexSlot::new(0), vec![Val::from(7u32)]);
            let cold_row =
                trx_select_row_mvcc_by_id(&mut trx, table.table_id(), &cold_key, &[0, 1]).await;
            assert_eq!(
                cold_row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("cold-row")]
            );

            let hot_key = SelectKey::new(IndexSlot::new(0), vec![Val::from(8u32)]);
            let hot_row =
                trx_select_row_mvcc_by_id(&mut trx, table.table_id(), &hot_key, &[0, 1]).await;
            assert_eq!(
                hot_row.unwrap().unwrap_found(),
                vec![Val::from(8u32), Val::from("hot-row")]
            );

            trx.commit().await.unwrap();

            drop(table);
            drop(session);
            drop(engine);
        })
    }

    /// Purpose: Filter checkpoint-covered inserts even when their commit reaches the heap replay floor.
    /// Expected: Cold rows retain persisted placement, hot rows are replayed, and checkpointed deletion remains effective.
    #[test]
    fn test_log_recover_skips_checkpointed_tail_insert_newer_than_heap_redo_start() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let log_file_stem = "recover-checkpointed-tail";
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                main_dir.clone(),
                log_file_stem,
            ))
            .await
            .unwrap();

            let mut setup_session = engine.new_session().unwrap();
            let table_id = setup_session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let payload = "x".repeat(1024);
            let mut insert_trx = setup_session.begin_trx().unwrap();
            let mut row_ids = Vec::with_capacity(200);
            for id in 0..200u32 {
                row_ids.push(
                    insert_trx
                        .table_insert_mvcc(
                            table.table_id(),
                            vec![Val::from(id), Val::from(payload.as_str())],
                        )
                        .await
                        .unwrap(),
                );
            }
            let insert_cts = insert_trx.commit().await.unwrap();

            let mut checkpoint_session = engine.new_session().unwrap();
            let frozen_batch =
                assert_freeze_created(checkpoint_session.freeze_table(table_id, 1).await.unwrap());
            assert_eq!(frozen_batch.page_count(), 1);

            let deleted_row_id = row_ids[0];
            let cold_row_id = row_ids[1];
            let hot_row_id = *row_ids.last().unwrap();
            let mut delete_trx = setup_session.begin_trx().unwrap();
            let delete = trx_delete_row_by_id(
                &mut delete_trx,
                table.table_id(),
                &SelectKey::new(IndexSlot::new(0), vec![Val::from(0u32)]),
            )
            .await;
            assert!(matches!(delete, Ok(UniqueMutationOutcome::Deleted)));
            let delete_cts = delete_trx.commit().await.unwrap();

            checkpoint_session
                .wait_for_gc_horizon_after(delete_cts)
                .await
                .unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table_id).await;
            let active_root = table.file().active_root_unchecked();
            let pivot_row_id = active_root.pivot_row_id;
            let heap_redo_start_ts = active_root.heap_redo_start_ts;
            assert!(deleted_row_id < pivot_row_id);
            assert!(cold_row_id < pivot_row_id);
            assert!(hot_row_id >= pivot_row_id);
            assert!(insert_cts >= heap_redo_start_ts);

            drop(table);
            drop(checkpoint_session);
            drop(setup_session);
            drop(engine);

            let engine =
                Engine::bootstrap(lightweight_recovery_engine_config(main_dir, log_file_stem))
                    .await
                    .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            assert_eq!(
                table.file().active_root_unchecked().pivot_row_id,
                pivot_row_id
            );
            let mut session = engine.new_session().unwrap();
            let guards = session.pool_guards();
            assert!(matches!(
                table.find_row(&guards, cold_row_id).await.unwrap(),
                RowLocation::LwcBlock(..)
            ));
            assert!(matches!(
                table.find_row(&guards, hot_row_id).await.unwrap(),
                RowLocation::RowPage(..)
            ));
            drop(guards);

            let mut trx = session.begin_trx().unwrap();
            let deleted_row = trx_select_row_mvcc_by_id(
                &mut trx,
                table.table_id(),
                &SelectKey::new(IndexSlot::new(0), vec![Val::from(0u32)]),
                &[0, 1],
            )
            .await;
            assert!(matches!(deleted_row, Ok(SelectMvcc::NotFound)));
            let cold_row = trx_select_row_mvcc_by_id(
                &mut trx,
                table.table_id(),
                &SelectKey::new(IndexSlot::new(0), vec![Val::from(1u32)]),
                &[0, 1],
            )
            .await;
            assert_eq!(
                cold_row.unwrap().unwrap_found(),
                vec![Val::from(1u32), Val::from(payload.as_str())]
            );
            let hot_row = trx_select_row_mvcc_by_id(
                &mut trx,
                table.table_id(),
                &SelectKey::new(IndexSlot::new(0), vec![Val::from(199u32)]),
                &[0, 1],
            )
            .await;
            assert_eq!(
                hot_row.unwrap().unwrap_found(),
                vec![Val::from(199u32), Val::from(payload.as_str())]
            );
            trx.commit().await.unwrap();

            drop(table);
            drop(session);
            drop(engine);
        })
    }

    /// Purpose: Distinguish checkpoint-covered cold deletions from newer deletion redo.
    /// Expected: Only newer deletes rebuild committed markers, while both deleted rows remain invisible after restart.
    #[test]
    fn test_log_recover_skips_checkpointed_and_replays_newer_cold_deletes() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(recovery_engine_config(main_dir.clone(), "recover10"))
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::U32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut trx = session.begin_trx().unwrap();
            for i in 0..10u32 {
                let insert = trx
                    .table_insert_mvcc(table.table_id(), vec![Val::from(i)])
                    .await;
                assert!(insert.is_ok());
            }
            trx.commit().await.unwrap();

            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table.table_id()).await;

            let mut trx = session.begin_trx().unwrap();
            let key0 = SelectKey::new(IndexSlot::new(0), vec![Val::from(0u32)]);
            let delete = trx_delete_row_by_id(&mut trx, table.table_id(), &key0).await;
            assert!(matches!(delete, Ok(UniqueMutationOutcome::Deleted)));
            trx.commit().await.unwrap();

            let marker0_ts = match table.deletion_buffer().get(RowID::new(0)).unwrap() {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            session.wait_for_gc_horizon_after(marker0_ts).await.unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table.table_id()).await;
            let checkpointed_cutoff = table.file().active_root_unchecked().deletion_cutoff_ts;
            assert!(checkpointed_cutoff > marker0_ts);

            let mut trx = session.begin_trx().unwrap();
            let key1 = SelectKey::new(IndexSlot::new(0), vec![Val::from(1u32)]);
            let delete = trx_delete_row_by_id(&mut trx, table.table_id(), &key1).await;
            assert!(matches!(delete, Ok(UniqueMutationOutcome::Deleted)));
            trx.commit().await.unwrap();
            let marker1_ts = match table.deletion_buffer().get(RowID::new(1)).unwrap() {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            assert!(marker1_ts >= checkpointed_cutoff);

            let mut trx = session.begin_trx().unwrap();
            let insert = trx
                .table_insert_mvcc(table.table_id(), vec![Val::from(100u32)])
                .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            drop(table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine = Engine::bootstrap(recovery_engine_config(main_dir, "recover10"))
                .await
                .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let report = engine.recovery_report();
            assert_report_accounting(report);
            assert_eq!(report.work.cold_deletes, 1);
            assert_eq!(report.work.hot_inserts, 1);
            assert!(report.work.user_row_ops_skipped >= 11);
            assert_eq!(
                table.file().active_root_unchecked().deletion_cutoff_ts,
                checkpointed_cutoff
            );
            assert!(table.deletion_buffer().get(RowID::new(0)).is_none());
            match table.deletion_buffer().get(RowID::new(1)).unwrap() {
                DeleteMarker::Committed(ts) => assert_eq!(ts, marker1_ts),
                DeleteMarker::Ref(_) => panic!("recovered cold delete should be committed"),
            }

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            let row0 = trx_select_row_mvcc_by_id(
                &mut trx,
                table.table_id(),
                &SelectKey::new(IndexSlot::new(0), vec![Val::from(0u32)]),
                &[0],
            )
            .await;
            assert!(matches!(row0, Ok(SelectMvcc::NotFound)));

            let row1 = trx_select_row_mvcc_by_id(
                &mut trx,
                table.table_id(),
                &SelectKey::new(IndexSlot::new(0), vec![Val::from(1u32)]),
                &[0],
            )
            .await;
            assert!(matches!(row1, Ok(SelectMvcc::NotFound)));

            let row100 = trx_select_row_mvcc_by_id(
                &mut trx,
                table.table_id(),
                &SelectKey::new(IndexSlot::new(0), vec![Val::from(100u32)]),
                &[0],
            )
            .await;
            assert_eq!(row100.unwrap().unwrap_found(), vec![Val::from(100u32)]);

            trx.commit().await.unwrap();
            drop(table);
            drop(session);
            drop(engine);
        })
    }

    /// Purpose: Recover tables with different persistence states under the same catalog checkpoint.
    /// Expected: Checkpointed rows remain cold and replay-only rows regain hot pages with correct values.
    #[test]
    fn test_log_recover_handles_mixed_user_table_checkpoint_states() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(recovery_engine_config(main_dir.clone(), "recover7"))
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let checkpointed_table_id = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();
            let replay_only_table_id = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let baseline_catalog_replay_start_ts = engine
                .inner()
                .core
                .catalog()
                .storage
                .checkpoint_snapshot()
                .catalog_replay_start_ts;
            assert!(baseline_catalog_replay_start_ts > MIN_SNAPSHOT_TS);

            let checkpointed_table = engine
                .inner()
                .core
                .catalog()
                .get_table(checkpointed_table_id)
                .unwrap();
            let replay_only_table = engine
                .inner()
                .core
                .catalog()
                .get_table(replay_only_table_id)
                .unwrap();

            let mut trx = session.begin_trx().unwrap();
            let insert = trx
                .table_insert_mvcc(
                    checkpointed_table.table_id(),
                    vec![Val::from(7u32), Val::from("persisted-row")],
                )
                .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            assert_freeze_created(
                session
                    .freeze_table(checkpointed_table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, checkpointed_table.table_id())
                .await;

            let mut trx = session.begin_trx().unwrap();
            let insert = trx
                .table_insert_mvcc(
                    replay_only_table.table_id(),
                    vec![Val::from(8u32), Val::from("replayed-row")],
                )
                .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            assert!(
                checkpointed_table
                    .file()
                    .active_root_unchecked()
                    .pivot_row_id
                    > RowID::new(0)
            );
            assert_eq!(
                replay_only_table
                    .file()
                    .active_root_unchecked()
                    .pivot_row_id,
                RowID::new(0)
            );
            assert!(
                checkpointed_table
                    .file()
                    .active_root_unchecked()
                    .heap_redo_start_ts
                    > baseline_catalog_replay_start_ts
            );

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            let final_catalog_replay_start_ts = engine
                .inner()
                .core
                .catalog()
                .storage
                .checkpoint_snapshot()
                .catalog_replay_start_ts;
            assert!(final_catalog_replay_start_ts > baseline_catalog_replay_start_ts);

            drop(replay_only_table);
            drop(checkpointed_table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine = Engine::bootstrap(recovery_engine_config(main_dir, "recover7"))
                .await
                .unwrap();

            let checkpointed_table = engine
                .inner()
                .core
                .catalog()
                .get_table(checkpointed_table_id)
                .unwrap();
            let replay_only_table = engine
                .inner()
                .core
                .catalog()
                .get_table(replay_only_table_id)
                .unwrap();

            let mut session = engine.new_session().unwrap();
            assert_eq!(
                session
                    .total_row_pages(checkpointed_table.table_id())
                    .await
                    .unwrap(),
                0
            );
            assert!(
                session
                    .total_row_pages(replay_only_table.table_id())
                    .await
                    .unwrap()
                    > 0
            );

            let mut trx = session.begin_trx().unwrap();

            let checkpointed_key = SelectKey::new(IndexSlot::new(0), vec![Val::from(7u32)]);
            let checkpointed_row = trx_select_row_mvcc_by_id(
                &mut trx,
                checkpointed_table.table_id(),
                &checkpointed_key,
                &[0, 1],
            )
            .await;
            assert_eq!(
                checkpointed_row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("persisted-row")]
            );

            let replay_only_key = SelectKey::new(IndexSlot::new(0), vec![Val::from(8u32)]);
            let replay_only_row = trx_select_row_mvcc_by_id(
                &mut trx,
                replay_only_table.table_id(),
                &replay_only_key,
                &[0, 1],
            )
            .await;
            assert_eq!(
                replay_only_row.unwrap().unwrap_found(),
                vec![Val::from(8u32), Val::from("replayed-row")]
            );

            trx.commit().await.unwrap();

            drop(replay_only_table);
            drop(checkpointed_table);
            drop(session);
            drop(engine);
        })
    }

    /// Purpose: Defer validation of a corrupted persisted column block during recovery.
    /// Expected: Bootstrap succeeds and the first row read reports the block's checksum failure with context.
    #[test]
    fn test_log_recover_defers_corrupted_persisted_lwc_block_until_read() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(recovery_engine_config(main_dir.clone(), "recover8"))
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut trx = session.begin_trx().unwrap();
            let insert = trx
                .table_insert_mvcc(
                    table.table_id(),
                    vec![Val::from(7u32), Val::from("persisted-row")],
                )
                .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table.table_id()).await;

            let active_root = table.file().active_root_unchecked();
            let block_id = {
                let disk_pool_guard = table.disk_pool().create_base_guard();
                let index = ColumnBlockIndex::new(
                    active_root.column_block_index_root,
                    active_root.pivot_row_id,
                    table.file().file_kind(),
                    table.file().sparse_file(),
                    table.disk_pool(),
                    &disk_pool_guard,
                );
                let entry = index
                    .collect_leaf_entries()
                    .await
                    .unwrap()
                    .into_iter()
                    .next()
                    .expect("checkpointed table should publish a persisted LWC block");
                entry.block_id()
            };

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            drop(checkpoint_session);
            drop(table);
            drop(session);
            drop(engine);

            corrupt_page_checksum(table_file_path, block_id);

            let engine = Engine::bootstrap(recovery_engine_config(main_dir, "recover8"))
                .await
                .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(IndexSlot::new(0), vec![Val::from(7u32)]);
            let res = trx_select_row_mvcc_by_id(&mut trx, table.table_id(), &key, &[0, 1]).await;
            let err = match res {
                Err(err) => err,
                other => panic!("expected persisted LWC corruption on read, got {other:?}"),
            };
            assert_table_data_integrity(
                err,
                "lwc_block",
                block_id,
                DataIntegrityError::ChecksumMismatch,
            );
            trx.rollback().await.unwrap();

            drop(table);
            drop(session);
            drop(engine);
        })
    }

    /// Purpose: Defer validation of malformed persisted deletion metadata during recovery.
    /// Expected: Bootstrap succeeds and loading deletion deltas reports invalid framing with blob context.
    #[test]
    fn test_log_recover_defers_invalid_delete_blob_framing_until_delta_load() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(recovery_engine_config(main_dir.clone(), "recover9"))
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::U32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();

            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let mut trx = session.begin_trx().unwrap();
            for i in 0..80u32 {
                let insert = trx
                    .table_insert_mvcc(table.table_id(), vec![Val::from(i)])
                    .await;
                assert!(insert.is_ok());
            }
            trx.commit().await.unwrap();

            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table.table_id()).await;

            let mut trx = session.begin_trx().unwrap();
            for i in 0..64u32 {
                let key = SelectKey::new(IndexSlot::new(0), vec![Val::from(i)]);
                let delete = trx_delete_row_by_id(&mut trx, table.table_id(), &key).await;
                assert!(matches!(delete, Ok(UniqueMutationOutcome::Deleted)));
            }
            trx.commit().await.unwrap();

            let marker = table.deletion_buffer().get(RowID::new(0)).unwrap();
            let marker_ts = match marker {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            session.wait_for_gc_horizon_after(marker_ts).await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let insert = trx
                .table_insert_mvcc(table.table_id(), vec![Val::from(1000u32)])
                .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut checkpoint_session, table.table_id()).await;

            let active_root = table.file().active_root_unchecked();
            let blob_ref = {
                let disk_pool_guard = table.disk_pool().create_base_guard();
                let index = ColumnBlockIndex::new(
                    active_root.column_block_index_root,
                    active_root.pivot_row_id,
                    table.file().file_kind(),
                    table.file().sparse_file(),
                    table.disk_pool(),
                    &disk_pool_guard,
                );
                let entry = index
                    .locate_block(RowID::new(0))
                    .await
                    .unwrap()
                    .expect("checkpointed table should keep the deleted row's block entry");
                entry
                    .deletion_blob_ref()
                    .expect("delete checkpoint should offload large delete sets")
            };

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            drop(checkpoint_session);
            drop(table);
            drop(session);
            drop(engine);

            corrupt_blob_header_kind(
                table_file_path,
                blob_ref.start_block_id,
                blob_ref.start_offset,
            );

            let engine = Engine::bootstrap(recovery_engine_config(main_dir, "recover9"))
                .await
                .unwrap();

            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let session = engine.new_session().unwrap();
            let active_root = table.file().active_root_unchecked();
            {
                let pool_guards = session.pool_guards();
                let index = ColumnBlockIndex::new(
                    active_root.column_block_index_root,
                    active_root.pivot_row_id,
                    table.file().file_kind(),
                    table.file().sparse_file(),
                    table.disk_pool(),
                    pool_guards.disk_guard(),
                );
                let entry = index
                    .locate_block(RowID::new(0))
                    .await
                    .unwrap()
                    .expect("deleted row should still have a checkpoint entry");
                let err = match index.load_delete_deltas_and_row_ids(&entry).await {
                    Ok(_) => panic!("expected invalid delete blob on delta load"),
                    Err(err) => err,
                };
                assert_table_runtime_data_integrity(
                    err,
                    "column_deletion_blob",
                    blob_ref.start_block_id,
                    DataIntegrityError::InvalidPayload,
                );
            }

            drop(table);
            drop(session);
            drop(engine);
        })
    }

    /// Purpose: Handle decoded keyed user-table redo at the replay floor under either validation setting.
    /// Expected: Older operations are skipped, eligible keyed operations are rejected, and decoding remains accounted for.
    #[test]
    fn keyed_user_redo_is_fully_decoded_before_floor_filtering_and_rejection() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_recovery_engine_config(
                temp.path(),
                "keyed-packed",
            ))
            .await
            .unwrap();
            let table_id = create_index_ddl_base_table(&engine, vec![]).await;
            for disable in [false, true] {
                let mut recovery = row_recovery_for_table(&engine, table_id);
                recovery.recovery_disable_dml_validation = disable;
                for update in [false, true] {
                    for cts in [9, 10] {
                        let key =
                            CatalogSelectKey::new(CatalogIndexNo::new(0), vec![Val::from(42u64)]);
                        let kind = if update {
                            RowRedoKind::UpdateByPrimaryKey(
                                key,
                                vec![UpdateCol {
                                    idx: 1,
                                    val: Val::from("fully decoded bytes"),
                                }],
                            )
                        } else {
                            RowRedoKind::DeleteByPrimaryKey(key)
                        };
                        let mut redo = RedoLogs::default();
                        redo.insert_dml(
                            table_id,
                            RowRedo {
                                row_id: RowID::new(1),
                                kind,
                            },
                        );
                        let result = replay_test_log(
                            &mut recovery,
                            TrxLog::new(redo_header(TrxID::new(cts)), redo),
                        )
                        .await;
                        if cts == 9 {
                            result.unwrap();
                        } else {
                            assert_replay_integrity(
                                result.unwrap_err(),
                                DataIntegrityError::InvalidPayload,
                                "key-based catalog redo",
                            );
                        }
                    }
                }
                assert_eq!(recovery.report.work.user_row_ops_seen, 4);
                assert_eq!(recovery.timeline.max_recovered_cts, TrxID::new(10));
            }
        });
    }
}
