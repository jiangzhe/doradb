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
mod resources;
mod row_state;
pub(crate) mod stream;
mod timeline;

use crate::buffer::BufferPool;
use crate::buffer::guard::PageGuard;
use crate::catalog::{
    CatalogTable, IndexDdlKind, IndexDdlRootProof, TableColumnLayout, classify_index_ddl_root,
    is_catalog_obj_id, is_user_obj_id,
};
use crate::error::{DataIntegrityError, Error, OperationError, Result};
use crate::id::{PageID, RowID, TableID, TrxID};
use crate::latch::LatchFallbackMode;
use crate::log::RedoLogInitializer;
use crate::log::block_group::TrxLog;
use crate::log::redo::{DDLRedo, RedoLogs, RowRedo, RowRedoKind, TableDML};
use crate::map::{FastHashMap, FastHashSet};
use crate::row::RowPage;
use crate::table::Table;
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::purge::DroppedTableFileDeleteItem;
use stream::RedoReplayPlanner;

use error_stack::Report;
pub(crate) use resources::{RecoveryBuffers, RecoveryResources};
pub(crate) use row_state::RowRecoveryMap;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
pub(crate) use timeline::{RecoveryTimeline, TableReplayBounds};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UserTableRedoAction {
    Replay,
    SkipCheckpointCoveredUnknownTable,
}

/// Recovery coordinator for checkpoint bootstrap, redo replay, final repair, and redo startup.
pub(crate) struct RecoveryCoordinator<'a> {
    /// Catalog, table files, and buffer-pool resources used by recovery.
    resources: RecoveryResources<'a>,
    /// Planner for the ordered redo-log stream.
    redo_planner: RedoReplayPlanner,
    /// Direct-IO read-ahead depth used by startup redo recovery.
    redo_read_depth: usize,
    /// Value-only initializer for the writable redo log created after recovery.
    initializer: RedoLogInitializer,
    /// Replay cursors, per-table bounds, and recovered CTS watermark.
    timeline: RecoveryTimeline,
    /// Tables loaded from table-file metadata while catalog index DDL redo is pending.
    pending_index_ddl_reconciliations: FastHashSet<TableID>,
    /// Hot row pages touched by redo replay, grouped by table for post-replay
    /// index rebuild and undo-map refresh.
    recovered_tables: FastHashMap<TableID, BTreeSet<PageID>>,
    /// Dropped user-table files whose runtime state was destroyed during replay.
    dropped_table_file_deletes: Vec<DroppedTableFileDeleteItem>,
}

impl<'a> RecoveryCoordinator<'a> {
    /// Create a recovery coordinator from prepared resources and redo startup state.
    #[inline]
    pub(crate) fn new(
        resources: RecoveryResources<'a>,
        redo_planner: RedoReplayPlanner,
        redo_read_depth: usize,
        initializer: RedoLogInitializer,
    ) -> Self {
        RecoveryCoordinator {
            resources,
            redo_planner,
            redo_read_depth,
            initializer,
            timeline: RecoveryTimeline::new(MIN_SNAPSHOT_TS),
            pending_index_ddl_reconciliations: FastHashSet::default(),
            recovered_tables: FastHashMap::default(),
            dropped_table_file_deletes: Vec::new(),
        }
    }

    /// Replay redo, rebuild indexes, and return recovery outcomes plus redo startup.
    #[inline]
    pub(crate) async fn recover_all(
        mut self,
    ) -> Result<(TrxID, Vec<DroppedTableFileDeleteItem>, RedoLogInitializer)> {
        self.bootstrap_checkpointed_user_tables().await?;
        let (skipped_max_recovered_cts, mut redo_stream) = self
            .redo_planner
            .plan_stream(self.timeline.replay_floor, self.redo_read_depth)?;
        if let Some(skipped_max_cts) = skipped_max_recovered_cts {
            self.timeline.max_recovered_cts = self.timeline.max_recovered_cts.max(skipped_max_cts);
        }
        // 1. replay DDL and DML into catalog metadata, hot RowStore pages, and
        //    cold delete markers.
        while let Some(log) = redo_stream.try_next().await? {
            self.replay_log(log).await?;
        }
        // 2. Ensure catalog metadata caught up with table-file roots.
        self.validate_loaded_table_metadata().await?;
        // 3. Remove create-table provisional files whose catalog redo never
        //    became durable.
        self.cleanup_post_replay_absent_user_table_files()?;
        // 4. Rebuild hot secondary-index state from recovered RowStore pages
        //    and refresh pages to enable undo maps.
        self.recover_indexes_and_refresh_pages().await?;

        Ok((
            self.timeline.max_recovered_cts,
            self.dropped_table_file_deletes,
            self.initializer,
        ))
    }

    async fn bootstrap_checkpointed_user_tables(&mut self) -> Result<()> {
        let snapshot = self.resources.catalog.storage.checkpoint_snapshot()?;
        self.timeline
            .seed_catalog_checkpoint(snapshot.catalog_replay_start_ts);

        let checkpointed_tables = self
            .resources
            .catalog
            .storage
            .tables()
            .list_uncommitted(&self.resources.buffers.pool_guards)
            .await?;
        let checkpointed_user_table_ids = checkpointed_tables
            .iter()
            .filter(|table| is_user_obj_id(table.table_id))
            .map(|table| table.table_id)
            .collect::<FastHashSet<_>>();
        self.resources
            .table_fs
            .cleanup_checkpoint_absent_user_table_files(
                snapshot.meta.next_table_id,
                &checkpointed_user_table_ids,
            )?;

        for table in checkpointed_tables {
            if !is_user_obj_id(table.table_id) {
                continue;
            }
            // Checkpoint bootstrap can see table-file metadata that already includes
            // index-DDL roots whose catalog rows are replayed later. Such a
            // narrow mismatch is temporary and must reconcile by final validation.
            let metadata_matched = self
                .resources
                .catalog
                .reload_create_table(
                    self.resources.buffers.mem_pool.clone(),
                    self.resources.buffers.index_pool.clone(),
                    &self.resources.table_fs,
                    self.resources.buffers.disk_pool.clone(),
                    table.table_id,
                )
                .await?;
            let state = self.track_loaded_table(table.table_id).await?;
            let pending_index_ddl_reconciliation = !metadata_matched;
            if pending_index_ddl_reconciliation {
                // The catalog checkpoint cursor has already skipped older catalog
                // redo. A pending mismatch is recoverable only when the file root
                // is at or beyond that cursor, so later replay can still supply
                // the catalog index-DDL rows needed to match the root metadata.
                self.validate_checkpoint_pending_reconciliation_cts(table.table_id, state)?;
                self.pending_index_ddl_reconciliations
                    .insert(table.table_id);
            }
            self.timeline.seed_table_bounds(state);
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
    ) -> Result<UserTableRedoAction> {
        if self.timeline.table_bounds.contains_key(&table_id) {
            return Ok(UserTableRedoAction::Replay);
        }
        if cts < self.timeline.catalog_replay_start_ts {
            return Ok(UserTableRedoAction::SkipCheckpointCoveredUnknownTable);
        }
        Err(Report::new(OperationError::TableNotFound)
            .attach(format!(
                "invalid recovery ordering: {context}: unknown user table redo at or after catalog replay boundary: table_id={table_id}, cts={cts}, catalog_replay_start_ts={}",
                self.timeline.catalog_replay_start_ts
            ))
            .into())
    }

    async fn track_loaded_table(&mut self, table_id: TableID) -> Result<TableReplayBounds> {
        let table = self
            .resources
            .catalog
            .get_table(table_id)
            .await
            .ok_or_else(|| {
                Error::from(
                    Report::new(OperationError::TableNotFound)
                        .attach(format!("track loaded table runtime: table_id={table_id}")),
                )
            })?;
        // `recovery_bootstrap_unchecked`: recovery records replay floors from
        // the table root loaded during restart, before normal transactions run.
        let active_root = table.file().active_root_unchecked();
        let state = TableReplayBounds {
            root_ts: active_root.root_ts,
            heap_redo_start_ts: active_root.heap_redo_start_ts,
            deletion_cutoff_ts: active_root.deletion_cutoff_ts,
        };
        let old = self.timeline.table_bounds.insert(table_id, state);
        if old.is_some() {
            return Err(Report::new(OperationError::TableAlreadyExists)
                .attach(format!("track loaded table state: table_id={table_id}"))
                .into());
        }
        Ok(state)
    }

    #[inline]
    fn validate_checkpoint_pending_reconciliation_cts(
        &self,
        table_id: TableID,
        state: TableReplayBounds,
    ) -> Result<()> {
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
                ))
                .into());
        }
        Ok(())
    }

    #[inline]
    fn table_heap_redo_start_ts(&self, table_id: TableID) -> Result<TrxID> {
        self.timeline
            .table_bounds
            .get(&table_id)
            .map(|state| state.heap_redo_start_ts)
            .ok_or_else(|| {
                Error::from(Report::new(OperationError::TableNotFound).attach(format!(
                    "lookup heap redo start timestamp: table_id={table_id}"
                )))
            })
    }

    #[inline]
    fn table_deletion_cutoff_ts(&self, table_id: TableID) -> Result<TrxID> {
        self.timeline
            .table_bounds
            .get(&table_id)
            .map(|state| state.deletion_cutoff_ts)
            .ok_or_else(|| {
                Error::from(Report::new(OperationError::TableNotFound).attach(format!(
                    "lookup deletion cutoff timestamp: table_id={table_id}"
                )))
            })
    }

    #[inline]
    fn table_replay_start_ts(&self, table_id: TableID) -> Result<TrxID> {
        self.timeline
            .table_bounds
            .get(&table_id)
            .map(|state| state.replay_start_ts())
            .ok_or_else(|| {
                Error::from(Report::new(OperationError::TableNotFound).attach(format!(
                    "lookup replay start timestamp: table_id={table_id}"
                )))
            })
    }

    async fn replay_log(&mut self, log: TrxLog) -> Result<()> {
        // sequentially replay redo log.
        let (header, RedoLogs { ddl, dml }) = log.into_inner();
        self.timeline.max_recovered_cts = self.timeline.max_recovered_cts.max(header.cts);
        if header.cts < self.timeline.replay_floor {
            return Ok(());
        }

        if let Some(ddl) = ddl {
            // DDL is a pipeline breaker: all previously dispatched DML must
            // finish before metadata replay mutates catalog/table state.
            self.wait_for_dml_done().await?;
            self.replay_ddl(ddl, dml, header.cts).await?;
        } else {
            // replay DML-only transaction.
            // todo: dispatch DML execution to multiple threads.
            self.dispatch_dml(dml, header.cts).await?;
        }
        Ok(())
    }

    async fn recover_indexes_and_refresh_pages(&mut self) -> Result<()> {
        // Checkpointed cold secondary-index state is already available through
        // the table's DiskTree roots. Rebuild only hot row-page MemIndex state.
        for (table_id, pages) in &self.recovered_tables {
            if let Some(table) = self.resources.catalog.get_table(*table_id).await {
                let metadata = table.metadata();
                for page_id in pages {
                    table
                        .populate_index_via_row_page(&self.resources.buffers.pool_guards, *page_id)
                        .await?;
                    self.refresh_page(Arc::clone(&metadata.col), *page_id)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn validate_loaded_table_metadata(&mut self) -> Result<()> {
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
                .await
                .ok_or_else(|| {
                    Error::from(Report::new(OperationError::TableNotFound).attach(format!(
                        "validate recovered user table metadata: table_id={table_id}"
                    )))
                })?;
            let active_root = table.file().active_root_unchecked();
            let (_, catalog_metadata) = self
                .resources
                .catalog
                .user_table_metadata_from_catalog(&self.resources.buffers.pool_guards, table_id)
                .await?;
            if catalog_metadata != *active_root.metadata {
                let pending = self.pending_index_ddl_reconciliations.contains(&table_id);
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach(format!(
                        "recovered user table metadata mismatch after redo replay: table_id={table_id}, root_ts={}, catalog_replay_start_ts={}, pending_index_ddl_reconciliation={pending}, catalog_next_index_no={}, root_next_index_no={}",
                        active_root.root_ts,
                        self.timeline.catalog_replay_start_ts,
                        catalog_metadata.idx.next_index_no(),
                        active_root.metadata.idx.next_index_no()
                    ))
                    .into());
            }
            self.pending_index_ddl_reconciliations.remove(&table_id);
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
                .into());
        }
        Ok(())
    }

    fn cleanup_post_replay_absent_user_table_files(&self) -> Result<()> {
        let recovered_user_table_ids = self
            .timeline
            .table_bounds
            .keys()
            .copied()
            .collect::<FastHashSet<_>>();
        let deferred_drop_table_ids = self
            .dropped_table_file_deletes
            .iter()
            .map(|item| item.table_id)
            .collect::<FastHashSet<_>>();
        self.resources
            .table_fs
            .cleanup_recovery_absent_user_table_files(
                &recovered_user_table_ids,
                &deferred_drop_table_ids,
            )
    }

    async fn refresh_page(
        &self,
        col_layout: Arc<TableColumnLayout>,
        page_id: PageID,
    ) -> Result<()> {
        let mut page_guard = self
            .resources
            .buffers
            .mem_pool
            .get_page::<RowPage>(
                self.resources.buffers.pool_guards.mem_guard(),
                page_id,
                LatchFallbackMode::Exclusive,
            )
            .await?
            .lock_exclusive_async()
            .await
            .unwrap();

        let create_cts = page_guard
            .bf()
            .ctx
            .as_ref()
            .and_then(|ctx| ctx.recover())
            .map(|rec| rec.create_cts())
            .unwrap_or(TrxID::new(0));
        let max_row_count = page_guard.page().header.max_row_count as usize;
        page_guard.bf_mut().init_undo_map(col_layout, max_row_count);
        if let Some(row_ver) = page_guard.bf().ctx.as_ref().and_then(|ctx| ctx.row_ver()) {
            row_ver.set_create_cts(create_cts);
        }
        Ok(())
    }

    async fn replay_ddl(
        &mut self,
        ddl: Box<DDLRedo>,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> Result<()> {
        match &*ddl {
            DDLRedo::CreateTable(table_id) => {
                if !self.should_replay_catalog(cts) {
                    return Ok(());
                }
                self.replay_catalog_modifications(dml).await?;
                // The table file stores only the latest active root, so create-table
                // redo may load metadata from later durable index DDL. Recovery
                // tracks that temporary gap until index-DDL redo reconciles catalog rows.
                let metadata_matched = self
                    .resources
                    .catalog
                    .reload_create_table(
                        self.resources.buffers.mem_pool.clone(),
                        self.resources.buffers.index_pool.clone(),
                        &self.resources.table_fs,
                        self.resources.buffers.disk_pool.clone(),
                        *table_id,
                    )
                    .await?;
                let state = self.track_loaded_table(*table_id).await?;
                self.timeline
                    .seed_recovered_cts(state.max_recovered_cts_seed());
                let pending_index_ddl_reconciliation = !metadata_matched;
                // Validate the root/create CTS relation before marking the table
                // as pending, so impossible metadata divergence fails at the
                // source instead of surfacing only in final equality validation.
                validate_create_table_reloaded_root_ts(
                    *table_id,
                    cts,
                    state,
                    pending_index_ddl_reconciliation,
                )?;
                if pending_index_ddl_reconciliation {
                    self.pending_index_ddl_reconciliations.insert(*table_id);
                }
            }
            DDLRedo::DropTable(table_id) => {
                if !self.should_replay_catalog(cts) {
                    return Ok(());
                }
                self.replay_catalog_modifications(dml).await?;
                let removed = self
                    .resources
                    .catalog
                    .remove_user_table(*table_id)
                    .ok_or_else(|| {
                        Error::from(
                            Report::new(OperationError::TableNotFound)
                                .attach(format!("replay drop table: table_id={table_id}")),
                        )
                    })?;
                let table = Arc::try_unwrap(removed).map_err(|table| {
                    Error::from(Report::new(OperationError::TableNotFound).attach(format!(
                        "replay drop table found stale runtime handle: table_id={table_id}, strong_count={}",
                        Arc::strong_count(&table)
                    )))
                })?;
                table
                    .destroy_dropped_runtime(&self.resources.buffers.pool_guards)
                    .await?;
                self.dropped_table_file_deletes
                    .push(DroppedTableFileDeleteItem::new(*table_id, cts));
                self.timeline.table_bounds.remove(table_id);
                self.recovered_tables.remove(table_id);
                self.pending_index_ddl_reconciliations.remove(table_id);
            }
            DDLRedo::CreateIndex { table_id, index_no } => {
                if !self.should_replay_catalog(cts) {
                    return Ok(());
                }
                if self.classify_user_table_redo(*table_id, cts, "replay create index")?
                    == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
                {
                    return Ok(());
                }
                let proof =
                    self.classify_index_ddl_root(IndexDdlKind::Create, *table_id, *index_no, cts)?;
                match proof {
                    IndexDdlRootProof::DurableFinalCreate
                    | IndexDdlRootProof::DurableAllocationOnly => {
                        self.replay_catalog_modifications(dml).await?;
                    }
                    IndexDdlRootProof::Provisional => {}
                    IndexDdlRootProof::DurableFinalDrop => unreachable!(
                        "create-index root proof cannot classify as durable final drop"
                    ),
                }
            }
            DDLRedo::DropIndex { table_id, index_no } => {
                if !self.should_replay_catalog(cts) {
                    return Ok(());
                }
                if self.classify_user_table_redo(*table_id, cts, "replay drop index")?
                    == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
                {
                    return Ok(());
                }
                let proof =
                    self.classify_index_ddl_root(IndexDdlKind::Drop, *table_id, *index_no, cts)?;
                match proof {
                    IndexDdlRootProof::DurableFinalDrop => {
                        self.replay_catalog_modifications(dml).await?;
                    }
                    IndexDdlRootProof::Provisional => {}
                    IndexDdlRootProof::DurableFinalCreate
                    | IndexDdlRootProof::DurableAllocationOnly => {
                        unreachable!("drop-index root proof cannot classify as create proof")
                    }
                }
            }
            DDLRedo::CreateRowPage {
                table_id,
                page_id,
                start_row_id,
                end_row_id,
            } => {
                debug_assert!(dml.is_empty());
                if self.classify_user_table_redo(*table_id, cts, "replay create row page")?
                    == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
                {
                    return Ok(());
                }
                if cts < self.table_heap_redo_start_ts(*table_id)? {
                    return Ok(());
                }
                // Row page creation is guaranteed to be ordered in the redo log,
                // so its safe to recreate it and the row id range must be identical.
                let table = self
                    .resources
                    .catalog
                    .get_table(*table_id)
                    .await
                    .ok_or_else(|| {
                        Error::from(
                            Report::new(OperationError::TableNotFound)
                                .attach(format!("replay create row page: table_id={table_id}")),
                        )
                    })?;
                let count = *end_row_id - *start_row_id;
                let mut page_guard = table
                    .mem
                    .allocate_row_page_at(
                        &self.resources.buffers.pool_guards,
                        count as usize,
                        *page_id,
                    )
                    .await?;
                // Here we switch row page to recover mode.
                page_guard.bf_mut().init_recover_map(cts);

                // Record recovered pages so we can recover indexes and refresh undo map at end.
                // Note: we do not need to recover catalog tables because they are specially handled.
                if self.resources.catalog.is_user_table(*table_id) {
                    self.recovered_tables
                        .entry(*table_id)
                        .or_default()
                        .insert(*page_id);
                }

                debug_assert!({
                    let page = page_guard.page();
                    page.header.start_row_id == *start_row_id
                        && page.header.start_row_id + page.header.max_row_count as u64
                            == *end_row_id
                });
            }
            DDLRedo::DataCheckpoint { table_id, .. } => {
                debug_assert!(dml.is_empty());
                if self.classify_user_table_redo(*table_id, cts, "replay data checkpoint")?
                    == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
                {
                    return Ok(());
                }
                if cts < self.table_heap_redo_start_ts(*table_id)? {
                    return Ok(());
                }
                let _ = self
                    .resources
                    .catalog
                    .get_table(*table_id)
                    .await
                    .ok_or_else(|| {
                        Error::from(
                            Report::new(OperationError::TableNotFound)
                                .attach(format!("replay data checkpoint: table_id={table_id}")),
                        )
                    })?;
            }
        }
        Ok(())
    }

    fn classify_index_ddl_root(
        &self,
        kind: IndexDdlKind,
        table_id: TableID,
        index_no: u16,
        cts: TrxID,
    ) -> Result<IndexDdlRootProof> {
        let table = self.resources.catalog.get_table_now(table_id);
        let active_root = table
            .as_ref()
            .map(|table| table.file().active_root_unchecked());
        classify_index_ddl_root(kind, table_id, index_no, cts, active_root)
    }

    async fn dispatch_dml(&mut self, dml: BTreeMap<TableID, TableDML>, cts: TrxID) -> Result<()> {
        // Sequential today; kept as the DML dispatch boundary for future
        // parallel recovery.
        self.replay_dml(dml, cts).await
    }

    async fn wait_for_dml_done(&mut self) -> Result<()> {
        // Sequential replay has no outstanding DML. Future parallel dispatch
        // must synchronize here before replaying a DDL pipeline breaker.
        Ok(())
    }

    /// Replay DML log.
    ///
    /// Catalog rows are replayed logically into catalog runtimes. User-table
    /// rows replay only heap and cold-delete state; hot secondary indexes are
    /// rebuilt after log replay from recovered RowStore pages.
    async fn replay_dml(&mut self, dml: BTreeMap<TableID, TableDML>, cts: TrxID) -> Result<()> {
        for (table_id, table_dml) in dml {
            if is_catalog_obj_id(table_id) {
                if !self.should_replay_catalog(cts) {
                    continue;
                }
                let table = self
                    .resources
                    .catalog
                    .get_catalog_table(table_id)
                    .ok_or_else(|| {
                        Error::from(
                            Report::new(OperationError::TableNotFound)
                                .attach(format!("replay catalog DML: table_id={table_id}")),
                        )
                    })?;
                self.replay_catalog_table_modifications(&table, &table_dml.rows)
                    .await?;
                continue;
            }
            if self.classify_user_table_redo(table_id, cts, "replay user table DML")?
                == UserTableRedoAction::SkipCheckpointCoveredUnknownTable
            {
                continue;
            }
            if cts < self.table_replay_start_ts(table_id)? {
                continue;
            }
            let table = self
                .resources
                .catalog
                .get_table(table_id)
                .await
                .ok_or_else(|| {
                    Error::from(
                        Report::new(OperationError::TableNotFound)
                            .attach(format!("replay user table DML: table_id={table_id}")),
                    )
                })?;
            self.replay_table_dml(table_id, &table, &table_dml.rows, cts)
                .await?;
        }
        Ok(())
    }

    /// Replay catalog DML log.
    /// Page id and row id in log are ignored because we do not keep physical structure for metadata.
    async fn replay_catalog_modifications(
        &mut self,
        dml: BTreeMap<TableID, TableDML>,
    ) -> Result<()> {
        for (table_id, table_dml) in dml {
            let table = self
                .resources
                .catalog
                .get_catalog_table(table_id)
                .ok_or_else(|| {
                    Error::from(Report::new(OperationError::TableNotFound).attach(format!(
                        "replay catalog table modifications: table_id={table_id}"
                    )))
                })?;
            self.replay_catalog_table_modifications(&table, &table_dml.rows)
                .await?;
        }
        Ok(())
    }

    async fn replay_catalog_table_modifications(
        &mut self,
        table: &CatalogTable,
        rows: &BTreeMap<RowID, RowRedo>,
    ) -> Result<()> {
        for row in rows.values() {
            match &row.kind {
                RowRedoKind::Insert(vals) => {
                    table
                        .insert_no_trx(&self.resources.buffers.pool_guards, vals)
                        .await?;
                }
                RowRedoKind::DeleteByUniqueKey(key) => {
                    table
                        .delete_unique_no_trx(&self.resources.buffers.pool_guards, key)
                        .await?;
                }
                RowRedoKind::Delete | RowRedoKind::Update(_) => {
                    // updates of catalog are implemented as DeleteByUniqueKey and Insert.
                    unreachable!()
                }
            }
        }
        Ok(())
    }

    async fn replay_table_dml(
        &mut self,
        table_id: TableID,
        table: &Table,
        rows: &BTreeMap<RowID, RowRedo>,
        cts: TrxID,
    ) -> Result<()> {
        let heap_redo_start_ts = self.table_heap_redo_start_ts(table_id)?;
        let deletion_cutoff_ts = self.table_deletion_cutoff_ts(table_id)?;
        let pivot_row_id = table.file().active_root_unchecked().pivot_row_id;
        for row in rows.values() {
            match &row.kind {
                RowRedoKind::Insert(vals) => {
                    if cts < heap_redo_start_ts {
                        continue;
                    }
                    table
                        .recover_row_insert(
                            &self.resources.buffers.pool_guards,
                            row.page_id,
                            row.row_id,
                            vals,
                            cts,
                        )
                        .await?;
                }
                RowRedoKind::Update(vals) => {
                    if cts < heap_redo_start_ts {
                        continue;
                    }
                    table
                        .recover_row_update(
                            &self.resources.buffers.pool_guards,
                            row.page_id,
                            row.row_id,
                            vals,
                            cts,
                        )
                        .await?;
                }
                RowRedoKind::Delete => {
                    if row.row_id < pivot_row_id {
                        if cts < deletion_cutoff_ts {
                            continue;
                        }
                    } else if cts < heap_redo_start_ts {
                        continue;
                    }
                    table
                        .recover_row_delete(
                            &self.resources.buffers.pool_guards,
                            row.page_id,
                            row.row_id,
                            cts,
                        )
                        .await?;
                }
                RowRedoKind::DeleteByUniqueKey(_) => {
                    // We do not allow DeleteByUniqueKey log on data tables.
                    unreachable!();
                }
            }
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
) -> Result<()> {
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
            ))
            .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{RecoveryCoordinator, validate_create_table_reloaded_root_ts};
    use crate::buffer::PoolRole;
    use crate::catalog::{
        ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexAttributes, IndexColumnObject,
        IndexKey, IndexObject, IndexOrder, IndexSpec, TableMetadata, TableObject, TableSpec,
        USER_OBJ_ID_START,
    };
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{CompletionErrorKind, DataIntegrityError, Error, OperationError, Result};
    use crate::file::block_integrity::{BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum};
    use crate::file::cow_file::{COW_FILE_PAGE_SIZE, SUPER_BLOCK_ID};
    use crate::file::table_file::MutableTableFile;
    use crate::id::{BlockID, PageID, RowID, TableID, TrxID};
    use crate::index::{COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE, ColumnBlockIndex, UniqueIndex};
    use crate::log::LogSync;
    use crate::log::block_group::TrxLog;
    use crate::log::format::{
        REDO_BLOCK_GROUP_END, REDO_BLOCK_GROUP_START, REDO_DEFAULT_DATA_START_OFFSET,
        REDO_SUPER_BLOCK_SLOT_SIZE, RedoBlockHeader, RedoGroupStartExtension, RedoSuperBlock,
        serialize_redo_super_block, slot_offset,
    };
    use crate::log::redo::{DDLRedo, RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind};
    use crate::recovery::{RecoveryBuffers, RecoveryResources, RowRecoveryMap, TableReplayBounds};
    use crate::row::RowRead;
    use crate::row::ops::{DeleteMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
    use crate::serde::Ser;
    use crate::session::{Session, tests::SessionTestExt};
    use crate::table::{CheckpointOutcome, DeleteMarker, Table};
    use crate::trx::{MIN_SNAPSHOT_TS, Transaction};
    use crate::value::Val;
    use crate::value::ValKind;
    use smol::Timer;
    use std::collections::BTreeMap;
    use std::fs::{self, File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::iter::repeat_n;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    const LIGHTWEIGHT_RECOVERY_BUFFER_BYTES: usize = 16 * 1024 * 1024;
    const LIGHTWEIGHT_RECOVERY_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
    const LIGHTWEIGHT_RECOVERY_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;
    const CORRUPTION_RECOVERY_LOG_BLOCK_SIZE: usize = 4096;
    const CORRUPTION_RECOVERY_LOG_FILE_MAX_SIZE: usize = 128 * 1024;

    fn assert_table_data_integrity(
        err: Error,
        block_kind: &str,
        block_id: BlockID,
        expected: DataIntegrityError,
    ) {
        let report = format!("{err:?}");
        if err.completion_error() == Some(CompletionErrorKind::DataIntegrity(expected)) {
            assert!(report.contains("propagate from other threads"), "{report}");
            assert!(report.contains("wait for"), "{report}");
            return;
        }
        assert_eq!(err.data_integrity_error(), Some(expected), "{report}");
        assert!(report.contains("table-file"), "{report}");
        assert!(report.contains(block_kind), "{report}");
        assert!(report.contains(&format!("block_id={block_id}")), "{report}");
    }

    fn lightweight_recovery_engine_config(
        main_dir: impl Into<PathBuf>,
        log_file_stem: &str,
    ) -> EngineConfig {
        EngineConfig::default()
            .storage_root(main_dir)
            .meta_buffer(LIGHTWEIGHT_RECOVERY_BUFFER_BYTES)
            .index_buffer(LIGHTWEIGHT_RECOVERY_BUFFER_BYTES)
            .index_max_file_size(LIGHTWEIGHT_RECOVERY_MAX_FILE_BYTES)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(LIGHTWEIGHT_RECOVERY_BUFFER_BYTES)
                    .max_file_size(LIGHTWEIGHT_RECOVERY_MAX_FILE_BYTES),
            )
            .trx(
                TrxSysConfig::default()
                    .log_write_io_depth(1)
                    .recovery_io_depth(1)
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
                .recovery_io_depth(1)
                .catalog_checkpoint_scan_io_depth(1)
                .log_block_size(CORRUPTION_RECOVERY_LOG_BLOCK_SIZE)
                .log_file_stem(log_file_stem)
                .log_file_max_size(CORRUPTION_RECOVERY_LOG_FILE_MAX_SIZE)
                .log_sync(LogSync::None)
                .purge_threads(1),
        )
    }

    async fn prepare_checkpointed_recovery_floor(main_dir: &Path, log_file_stem: &str) -> TrxID {
        let engine = corruption_recovery_engine_config(main_dir, log_file_stem)
            .build()
            .await
            .unwrap();
        let table_id = create_index_ddl_base_table(&engine, vec![primary_index_spec()]).await;
        let mut session = engine.new_session().unwrap();
        session.drop_table(table_id).await.unwrap();
        drop(session);
        engine
            .catalog()
            .checkpoint_now(&engine.inner().trx_sys)
            .await
            .unwrap();
        let replay_floor = engine
            .catalog()
            .storage
            .checkpoint_snapshot()
            .unwrap()
            .catalog_replay_start_ts;
        assert!(replay_floor > MIN_SNAPSHOT_TS);
        drop(engine);
        remove_redo_family(main_dir, log_file_stem);
        replay_floor
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
        let err = match corruption_recovery_engine_config(main_dir, log_file_stem)
            .build()
            .await
        {
            Ok(engine) => {
                drop(engine);
                panic!("engine startup should fail on redo corruption");
            }
            Err(err) => err,
        };
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::LogFileCorrupted),
            "{err:?}"
        );
    }

    fn log_recovery_for_engine<'a>(
        engine: &'a Engine,
        catalog_replay_start_ts: TrxID,
    ) -> RecoveryCoordinator<'a> {
        let buffers = RecoveryBuffers::new(
            engine.inner().meta_pool.clone_inner(),
            engine.inner().index_pool.clone_inner(),
            engine.inner().mem_pool.clone_inner(),
            engine.inner().disk_pool.clone_inner(),
        );
        let resources =
            RecoveryResources::new(buffers, engine.inner().table_fs.clone(), engine.catalog());
        let mut recovery = engine
            .inner()
            .trx_sys
            .config
            .prepare_recovery(resources)
            .unwrap();
        recovery.timeline.catalog_replay_start_ts = catalog_replay_start_ts;
        recovery.timeline.replay_floor = MIN_SNAPSHOT_TS;
        recovery
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
                page_id: PageID::new(1),
                row_id: RowID::new(0),
                kind: RowRedoKind::Insert(vec![Val::from(1u32)]),
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
                    sts: cts,
                })),
                dml: BTreeMap::default(),
            },
        )
    }

    async fn checkpoint_published(table: &Table, session: &mut Session) -> TrxID {
        match session.checkpoint_table(table.table_id()).await.unwrap() {
            CheckpointOutcome::Published { checkpoint_ts } => checkpoint_ts,
            CheckpointOutcome::Delayed { reason } => {
                panic!("checkpoint should publish, delayed by {reason:?}")
            }
            CheckpointOutcome::Cancelled { reason } => {
                panic!("checkpoint should publish, cancelled by {reason:?}")
            }
        }
    }

    async fn trx_insert_row(trx: &mut Transaction, table: &Table, cols: Vec<Val>) -> Result<RowID> {
        trx_insert_row_by_id(trx, table.table_id(), cols).await
    }

    async fn trx_insert_row_by_id(
        trx: &mut Transaction,
        table_id: TableID,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        trx.exec(async |stmt| stmt.table_insert_mvcc(table_id, cols).await)
            .await
    }

    async fn trx_delete_row(
        trx: &mut Transaction,
        table: &Table,
        key: &SelectKey,
    ) -> Result<DeleteMvcc> {
        trx_delete_row_by_id(trx, table.table_id(), key).await
    }

    async fn trx_delete_row_by_id(
        trx: &mut Transaction,
        table_id: TableID,
        key: &SelectKey,
    ) -> Result<DeleteMvcc> {
        trx.exec(async |stmt| stmt.table_delete_unique_mvcc(table_id, key, false).await)
            .await
    }

    async fn trx_update_row_by_id(
        trx: &mut Transaction,
        table_id: TableID,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        trx.exec(async |stmt| stmt.table_update_unique_mvcc(table_id, key, update).await)
            .await
    }

    async fn trx_select_row_mvcc(
        trx: &mut Transaction,
        table: &Table,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        trx.exec(async |stmt| {
            stmt.table_lookup_unique_mvcc(table.table_id(), key, user_read_set)
                .await
        })
        .await
    }

    fn index_ddl_columns() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
            ColumnSpec::new("value", ValKind::I32, ColumnAttributes::empty()),
        ]
    }

    fn primary_index_spec() -> IndexSpec {
        IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)
    }

    fn added_index_spec() -> IndexSpec {
        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty())
    }

    fn created_index_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new_with_next_index_no(
                index_ddl_columns(),
                vec![
                    ActiveIndexSpec::new(0, primary_index_spec()),
                    ActiveIndexSpec::new(1, added_index_spec()),
                ],
                2,
            )
            .unwrap(),
        )
    }

    fn dropped_index_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new_with_next_index_no(
                index_ddl_columns(),
                vec![ActiveIndexSpec::new(0, primary_index_spec())],
                2,
            )
            .unwrap(),
        )
    }

    async fn create_index_ddl_base_table(engine: &Engine, indexes: Vec<IndexSpec>) -> TableID {
        let mut session = engine.new_session().unwrap();
        let table_id = session
            .create_table(TableSpec::new(index_ddl_columns()), indexes)
            .await
            .unwrap();
        drop(session);
        table_id
    }

    async fn commit_create_index_catalog_ddl(engine: &Engine, table_id: TableID) -> TrxID {
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        trx.exec(async |stmt| {
            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .delete_by_id(stmt, table_id)
                    .await
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .tables()
                    .insert(
                        stmt,
                        &TableObject {
                            table_id,
                            next_index_no: 2,
                        },
                    )
                    .await
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .indexes()
                    .insert(
                        stmt,
                        &IndexObject {
                            table_id,
                            index_no: 1,
                            index_attributes: IndexAttributes::empty(),
                        },
                    )
                    .await
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .index_columns()
                    .insert(
                        stmt,
                        &IndexColumnObject {
                            table_id,
                            index_no: 1,
                            index_column_no: 0,
                            column_no: 1,
                            index_order: IndexOrder::Asc,
                        },
                    )
                    .await
            );
            let old = stmt.effects_mut().set_ddl_redo(DDLRedo::CreateIndex {
                table_id,
                index_no: 1,
            });
            debug_assert!(old.is_none());
            Ok(())
        })
        .await
        .unwrap();
        let cts = trx.commit().await.unwrap();
        drop(session);
        cts
    }

    async fn commit_drop_index_catalog_ddl(engine: &Engine, table_id: TableID) -> TrxID {
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        trx.exec(async |stmt| {
            assert_eq!(
                engine
                    .catalog()
                    .storage
                    .index_columns()
                    .delete_by_index(stmt, table_id, 1)
                    .await?,
                1
            );
            assert!(
                engine
                    .catalog()
                    .storage
                    .indexes()
                    .delete_by_id(stmt, table_id, 1)
                    .await
            );
            let old = stmt.effects_mut().set_ddl_redo(DDLRedo::DropIndex {
                table_id,
                index_no: 1,
            });
            debug_assert!(old.is_none());
            Ok(())
        })
        .await
        .unwrap();
        let cts = trx.commit().await.unwrap();
        drop(session);
        cts
    }

    async fn publish_index_metadata_root(
        engine: &Engine,
        table_id: TableID,
        metadata: Arc<TableMetadata>,
        cts: TrxID,
    ) {
        let table = engine.catalog().get_table(table_id).await.unwrap();
        let table_file = Arc::clone(table.file());
        let mut roots = table_file
            .active_root_unchecked()
            .secondary_index_roots
            .clone();
        roots.resize(metadata.idx.index_slot_count(), SUPER_BLOCK_ID);
        for (index_no, root) in roots.iter_mut().enumerate() {
            if metadata.idx.index_spec(index_no).is_none() {
                *root = SUPER_BLOCK_ID;
            }
        }
        let mut mutable = MutableTableFile::fork(
            &table_file,
            engine.inner().table_fs.background_writes(),
            table.disk_pool().clone(),
        );
        mutable
            .replace_metadata_and_secondary_index_roots(metadata, roots)
            .unwrap();
        engine
            .inner()
            .trx_sys
            .publish_table_file_root(mutable, cts, false)
            .await
            .unwrap();
        drop(table);
    }

    async fn assert_recovered_index_state(
        engine: &Engine,
        table_id: TableID,
        next_index_no: u16,
        index_one_active: bool,
    ) {
        let table = engine.catalog().get_table(table_id).await.unwrap();
        let metadata = table.metadata();
        assert_eq!(metadata.idx.next_index_no(), next_index_no);
        assert_eq!(metadata.idx.index_spec(1).is_some(), index_one_active);

        let session = engine.new_session().unwrap();
        let table_obj = engine
            .catalog()
            .storage
            .tables()
            .find_uncommitted_by_id(&session.pool_guards(), table_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(table_obj.next_index_no, next_index_no);
        let indexes = engine
            .catalog()
            .storage
            .indexes()
            .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
            .await
            .unwrap();
        assert_eq!(
            indexes.iter().any(|index| index.index_no == 1),
            index_one_active
        );
        drop(session);
        drop(table);
    }

    #[test]
    fn test_recover_map_tracks_create_cts_and_entries() {
        let mut map = RowRecoveryMap::new(TrxID::new(7));
        assert_eq!(map.create_cts(), TrxID::new(7));
        assert!(map.is_vacant(0));

        map.insert_at(2, TrxID::new(11));
        assert!(map.is_vacant(0));
        assert!(map.is_vacant(1));
        assert!(!map.is_vacant(2));
        assert_eq!(map.at(2), Some(TrxID::new(11)));
        assert_eq!(map.at(3), None);

        map.update_at(2, TrxID::new(13));
        assert_eq!(map.at(2), Some(TrxID::new(13)));
    }

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

    #[test]
    fn test_create_table_root_ts_validation_accepts_initial_sts_root() {
        let root_before_create = TableReplayBounds {
            root_ts: TrxID::new(9),
            heap_redo_start_ts: TrxID::new(9),
            deletion_cutoff_ts: TrxID::new(9),
        };
        validate_create_table_reloaded_root_ts(
            USER_OBJ_ID_START,
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
            USER_OBJ_ID_START,
            TrxID::new(10),
            pending_without_later_root,
            true,
        )
        .unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
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
            USER_OBJ_ID_START,
            TrxID::new(10),
            pending_with_later_root,
            true,
        )
        .unwrap();
    }

    #[test]
    fn test_log_recovery_skips_checkpoint_covered_unknown_user_table_redo() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_recovery_engine_config(
                temp_dir.path().to_path_buf(),
                "recover-unknown-table-skip",
            )
            .build()
            .await
            .unwrap();
            let unknown_table_id = USER_OBJ_ID_START + 142;
            let mut recovery = log_recovery_for_engine(&engine, TrxID::new(10));

            recovery
                .replay_log(unknown_table_dml_log(unknown_table_id, TrxID::new(9)))
                .await
                .unwrap();

            recovery
                .replay_log(unknown_table_create_row_page_log(
                    unknown_table_id,
                    TrxID::new(9),
                ))
                .await
                .unwrap();

            recovery
                .replay_log(unknown_table_data_checkpoint_log(
                    unknown_table_id,
                    TrxID::new(9),
                ))
                .await
                .unwrap();

            drop(recovery);
            drop(engine);
        });
    }

    #[test]
    fn test_log_recovery_fails_unknown_user_table_redo_at_catalog_boundary() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_recovery_engine_config(
                temp_dir.path().to_path_buf(),
                "recover-unknown-table-invalid",
            )
            .build()
            .await
            .unwrap();
            let unknown_table_id = USER_OBJ_ID_START + 143;

            let mut dml_recovery = log_recovery_for_engine(&engine, TrxID::new(10));
            let err = dml_recovery
                .replay_log(unknown_table_dml_log(unknown_table_id, TrxID::new(10)))
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            let report = format!("{err:?}");
            assert!(report.contains("invalid recovery ordering"), "{report}");
            assert!(report.contains("replay user table DML"), "{report}");

            let mut ddl_recovery = log_recovery_for_engine(&engine, TrxID::new(10));
            let err = ddl_recovery
                .replay_log(unknown_table_create_row_page_log(
                    unknown_table_id,
                    TrxID::new(10),
                ))
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            let report = format!("{err:?}");
            assert!(report.contains("invalid recovery ordering"), "{report}");
            assert!(report.contains("replay create row page"), "{report}");

            drop(ddl_recovery);
            drop(dml_recovery);
            drop(engine);
        });
    }

    #[test]
    fn test_recovery_skips_provisional_create_index_redo() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_recovery_engine_config(
                main_dir.clone(),
                "recover-provisional-create-index",
            )
            .build()
            .await
            .unwrap();
            let table_id = create_index_ddl_base_table(&engine, vec![primary_index_spec()]).await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let _cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            drop(engine);

            let recovered =
                lightweight_recovery_engine_config(main_dir, "recover-provisional-create-index")
                    .build()
                    .await
                    .unwrap();
            assert_recovered_index_state(&recovered, table_id, 1, false).await;
            drop(recovered);
        });
    }

    #[test]
    fn test_recovery_replays_root_proven_create_index_redo() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                lightweight_recovery_engine_config(main_dir.clone(), "recover-create-index")
                    .build()
                    .await
                    .unwrap();
            let table_id = create_index_ddl_base_table(&engine, vec![primary_index_spec()]).await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), cts).await;
            drop(engine);

            let recovered = lightweight_recovery_engine_config(main_dir, "recover-create-index")
                .build()
                .await
                .unwrap();
            assert_recovered_index_state(&recovered, table_id, 2, true).await;
            drop(recovered);
        });
    }

    #[test]
    fn test_recovery_replays_root_proven_drop_index_redo() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_recovery_engine_config(main_dir.clone(), "recover-drop-index")
                .build()
                .await
                .unwrap();
            let table_id = create_index_ddl_base_table(
                &engine,
                vec![primary_index_spec(), added_index_spec()],
            )
            .await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let cts = commit_drop_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, dropped_index_metadata(), cts).await;
            drop(engine);

            let recovered = lightweight_recovery_engine_config(main_dir, "recover-drop-index")
                .build()
                .await
                .unwrap();
            assert_recovered_index_state(&recovered, table_id, 2, false).await;
            drop(recovered);
        });
    }

    #[test]
    fn test_recovery_replays_create_then_drop_index_allocation_history() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                lightweight_recovery_engine_config(main_dir.clone(), "recover-create-drop-index")
                    .build()
                    .await
                    .unwrap();
            let table_id = create_index_ddl_base_table(&engine, vec![primary_index_spec()]).await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let create_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), create_cts)
                .await;
            let drop_cts = commit_drop_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, dropped_index_metadata(), drop_cts)
                .await;
            drop(engine);

            let recovered =
                lightweight_recovery_engine_config(main_dir, "recover-create-drop-index")
                    .build()
                    .await
                    .unwrap();
            assert_recovered_index_state(&recovered, table_id, 2, false).await;
            drop(recovered);
        });
    }

    #[test]
    fn test_recovery_replays_new_table_with_later_create_index_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_recovery_engine_config(
                main_dir.clone(),
                "recover-new-table-create-index",
            )
            .build()
            .await
            .unwrap();
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let table_id = create_index_ddl_base_table(&engine, vec![primary_index_spec()]).await;
            let index_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), index_cts)
                .await;
            drop(engine);

            let recovered =
                lightweight_recovery_engine_config(main_dir, "recover-new-table-create-index")
                    .build()
                    .await
                    .unwrap();
            assert_recovered_index_state(&recovered, table_id, 2, true).await;
            drop(recovered);
        });
    }

    #[test]
    fn test_recovery_replays_new_table_with_later_create_drop_index_roots() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_recovery_engine_config(
                main_dir.clone(),
                "recover-new-table-create-drop-index",
            )
            .build()
            .await
            .unwrap();
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let table_id = create_index_ddl_base_table(&engine, vec![primary_index_spec()]).await;
            let create_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), create_cts)
                .await;
            let drop_cts = commit_drop_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, dropped_index_metadata(), drop_cts)
                .await;
            drop(engine);

            let recovered =
                lightweight_recovery_engine_config(main_dir, "recover-new-table-create-drop-index")
                    .build()
                    .await
                    .unwrap();
            assert_recovered_index_state(&recovered, table_id, 2, false).await;
            drop(recovered);
        });
    }

    #[test]
    fn test_catalog_checkpoint_skips_unproved_index_ddl_catalog_dml() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_recovery_engine_config(
                main_dir.clone(),
                "checkpoint-skip-provisional-index",
            )
            .build()
            .await
            .unwrap();
            let table_id = create_index_ddl_base_table(&engine, vec![primary_index_spec()]).await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let ddl_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            let snapshot = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snapshot.catalog_replay_start_ts > ddl_cts);
            drop(engine);

            let recovered =
                lightweight_recovery_engine_config(main_dir, "checkpoint-skip-provisional-index")
                    .build()
                    .await
                    .unwrap();
            assert_recovered_index_state(&recovered, table_id, 1, false).await;
            drop(recovered);
        });
    }

    #[test]
    fn test_catalog_checkpoint_includes_root_proven_index_ddl_catalog_dml() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_recovery_engine_config(
                main_dir.clone(),
                "checkpoint-include-durable-index",
            )
            .build()
            .await
            .unwrap();
            let table_id = create_index_ddl_base_table(&engine, vec![primary_index_spec()]).await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let ddl_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), ddl_cts).await;
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            let snapshot = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snapshot.catalog_replay_start_ts > ddl_cts);
            drop(engine);

            let recovered =
                lightweight_recovery_engine_config(main_dir, "checkpoint-include-durable-index")
                    .build()
                    .await
                    .unwrap();
            assert_recovered_index_state(&recovered, table_id, 2, true).await;
            drop(recovered);
        });
    }

    fn corrupt_page_checksum(path: impl AsRef<Path>, page_id: impl Into<u64>) {
        let page_id = page_id.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = page_id * COW_FILE_PAGE_SIZE as u64 + (COW_FILE_PAGE_SIZE as u64 - 1);
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.flush().unwrap();
    }

    fn rewrite_page_with_checksum(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        rewrite: impl FnOnce(&mut [u8]),
    ) {
        let page_id = page_id.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = page_id * COW_FILE_PAGE_SIZE as u64;
        let mut page = vec![0u8; COW_FILE_PAGE_SIZE];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut page).unwrap();
        rewrite(&mut page);
        write_block_checksum(&mut page);
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&page).unwrap();
        file.flush().unwrap();
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

    #[test]
    fn test_log_recover_skips_corrupt_obsolete_sealed_segment_and_seeds_cts() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path();
            let log_file_stem = "recover-sealed-skip-corrupt";
            let replay_floor = prepare_checkpointed_recovery_floor(main_dir, log_file_stem).await;
            let skipped_max_cts = TrxID::new(replay_floor.as_u64() - 1);
            write_bad_checksum_redo_file(main_dir, log_file_stem, skipped_max_cts, true);

            let recovered = corruption_recovery_engine_config(main_dir, log_file_stem)
                .build()
                .await
                .unwrap();
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

    #[test]
    fn test_log_recover_fails_replay_relevant_group_checksum_mismatch() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path();
            let log_file_stem = "recover-relevant-checksum-corrupt";
            let replay_floor = prepare_checkpointed_recovery_floor(main_dir, log_file_stem).await;
            write_bad_checksum_redo_file(main_dir, log_file_stem, replay_floor, false);

            expect_log_recovery_corruption(main_dir, log_file_stem).await;
        });
    }

    #[test]
    fn test_log_recover_empty() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover1"))
                .build()
                .await
                .unwrap();

            drop(engine);
        })
    }

    #[test]
    fn test_log_recover_ddl() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover2"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_spec = TableSpec::new(vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::empty()),
                ColumnSpec::new("c2", ValKind::U32, ColumnAttributes::empty()),
            ]);
            let index_specs = vec![
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new(
                    vec![
                        IndexKey {
                            col_no: 1,
                            order: IndexOrder::Desc,
                        },
                        IndexKey::new(2),
                    ],
                    IndexAttributes::empty(),
                ),
            ];
            let expected_metadata =
                TableMetadata::try_new(table_spec.columns.clone(), index_specs.clone())
                    .expect("valid table metadata");

            let table_id = session.create_table(table_spec, index_specs).await.unwrap();

            drop(session);
            drop(engine);

            // second recovery.
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover2"))
                .build()
                .await
                .unwrap();

            assert!(engine.catalog().get_table(table_id).await.is_some());
            let table = engine.catalog().get_table(table_id).await.unwrap();
            assert_eq!(table.metadata().as_ref(), &expected_metadata);

            drop(table);
            drop(engine);
        })
    }

    #[test]
    fn test_log_recover_dml() {
        smol::block_on(async {
            const DML_SIZE: usize = 1000;
            const INS_STEP: usize = 10;
            const UPD_STEP: usize = 11;
            const DEL_STEP: usize = 13;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover3"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_spec = TableSpec::new(vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::VarByte, ColumnAttributes::empty()),
            ]);

            let table_id = session
                .create_table(
                    table_spec,
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();

            let s: String = repeat_n('0', 100).collect();
            // insert
            for i in (0..DML_SIZE).step_by(INS_STEP) {
                let mut trx = session.begin_trx().unwrap();
                for j in i..i + INS_STEP {
                    let res = trx_insert_row_by_id(
                        &mut trx,
                        table_id,
                        vec![Val::from(j as u32), Val::from(&s[..])],
                    )
                    .await;
                    assert!(res.is_ok());
                }
                trx.commit().await.unwrap();
            }
            // update
            let s2: String = repeat_n('2', 100).collect();
            for i in (0..DML_SIZE).step_by(UPD_STEP) {
                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(i as u32)]);
                let uc = UpdateCol {
                    idx: 1,
                    val: Val::from(&s2[..]),
                };
                let res = trx_update_row_by_id(&mut trx, table_id, &key, vec![uc]).await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                trx.commit().await.unwrap();
            }
            // delete
            for i in (0..DML_SIZE).step_by(DEL_STEP) {
                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(i as u32)]);
                let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
                trx.commit().await.unwrap();
            }

            drop(session);
            drop(engine);

            // second recovery.
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover3"))
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let session = engine.new_session().unwrap();
            let mut rows = 0usize;
            {
                let layout = table.layout_snapshot();
                table
                    .accessor_with_layout(&layout)
                    .mem_scan_uncommitted(&session.pool_guards(), |_metadata, row| {
                        assert!(row.row_id().as_usize() <= DML_SIZE);
                        rows += if row.is_deleted() { 0 } else { 1 };
                        true
                    })
                    .await
                    .unwrap();
            }
            assert_eq!(rows, DML_SIZE - (DML_SIZE / DEL_STEP + 1));

            drop(session);
            drop(table);
            drop(engine);
        })
    }

    #[test]
    fn test_log_recover_bootstraps_catalog_from_checkpoint() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover4"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec::new(vec![ColumnSpec::new(
                        "id",
                        ValKind::U32,
                        ColumnAttributes::empty(),
                    )]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            let snap = engine.catalog().storage.checkpoint_snapshot().unwrap();
            assert!(snap.catalog_replay_start_ts > MIN_SNAPSHOT_TS);

            drop(session);
            drop(engine);

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover4"))
                .build()
                .await
                .unwrap();

            assert!(engine.catalog().get_table(table_id).await.is_some());
            drop(engine);
        })
    }

    #[test]
    fn test_log_recover_reads_checkpointed_secondary_from_disk_tree_without_mem_backfill() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover5"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::U32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            let catalog_replay_start_ts = engine
                .catalog()
                .storage
                .checkpoint_snapshot()
                .unwrap()
                .catalog_replay_start_ts;

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &table,
                vec![Val::from(7u32), Val::from("cold-row")],
            )
            .await;
            let cold_row_id = match insert {
                Ok(row_id) => row_id,
                other => panic!("expected cold insert success, got {other:?}"),
            };
            trx.commit().await.unwrap();

            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;
            let root_after_checkpoint = table.file().active_root_unchecked();
            assert!(root_after_checkpoint.heap_redo_start_ts > catalog_replay_start_ts);

            drop(table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover5"))
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            assert_eq!(session.total_row_pages(table.table_id()).await.unwrap(), 0);

            let key = SelectKey::new(0, vec![Val::from(7u32)]);
            let layout = table.layout_snapshot();
            let index = layout.secondary_index(key.index_no).unwrap();
            let root = table.file().active_root_unchecked().secondary_index_roots[key.index_no];
            {
                let pool_guards = session.pool_guards();
                let disk = index
                    .disk_runtime()
                    .open_unique_at(root, pool_guards.disk_guard())
                    .unwrap();
                assert_eq!(disk.lookup(&key.vals).await.unwrap(), Some(cold_row_id));
            }
            assert_eq!(
                index
                    .bind_unique(root)
                    .unwrap()
                    .lookup(
                        session.pool_guards().index_guard(),
                        &key.vals,
                        MIN_SNAPSHOT_TS
                    )
                    .await
                    .unwrap(),
                Some((cold_row_id, false))
            );
            let mut trx = session.begin_trx().unwrap();
            let row = trx_select_row_mvcc(&mut trx, &table, &key, &[0, 1]).await;
            assert_eq!(
                row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("cold-row")]
            );
            trx.commit().await.unwrap();

            drop(layout);
            drop(table);
            drop(session);
            drop(engine);
        })
    }

    #[test]
    fn test_log_recover_rebuilds_hot_unique_memindex_over_checkpointed_cold_duplicate() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover11"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::U32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &table,
                vec![Val::from(7u32), Val::from("cold-row")],
            )
            .await;
            let Ok(cold_row_id) = insert else {
                panic!("cold insert should succeed");
            };
            trx.commit().await.unwrap();

            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;
            assert!(table.file().active_root_unchecked().pivot_row_id > cold_row_id);

            let key = SelectKey::new(0, vec![Val::from(7u32)]);
            let mut trx = session.begin_trx().unwrap();
            let delete = trx_delete_row(&mut trx, &table, &key).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &table,
                vec![Val::from(7u32), Val::from("hot-row")],
            )
            .await;
            let Ok(hot_row_id) = insert else {
                panic!("hot insert should reclaim deleted cold key");
            };
            assert_ne!(cold_row_id, hot_row_id);
            trx.commit().await.unwrap();

            drop(table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover11"))
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            assert!(session.total_row_pages(table.table_id()).await.unwrap() > 0);

            let layout = table.layout_snapshot();
            let index = layout.secondary_index(key.index_no).unwrap();
            let root = table.file().active_root_unchecked().secondary_index_roots[key.index_no];
            {
                let pool_guards = session.pool_guards();
                let disk = index
                    .disk_runtime()
                    .open_unique_at(root, pool_guards.disk_guard())
                    .unwrap();
                assert_eq!(disk.lookup(&key.vals).await.unwrap(), Some(cold_row_id));
            }
            assert_eq!(
                index
                    .bind_unique(root)
                    .unwrap()
                    .lookup(
                        session.pool_guards().index_guard(),
                        &key.vals,
                        MIN_SNAPSHOT_TS
                    )
                    .await
                    .unwrap(),
                Some((hot_row_id, false))
            );

            let mut trx = session.begin_trx().unwrap();
            let row = trx_select_row_mvcc(&mut trx, &table, &key, &[0, 1]).await;
            assert_eq!(
                row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("hot-row")]
            );
            trx.commit().await.unwrap();

            drop(layout);
            drop(table);
            drop(session);
            drop(engine);
        })
    }

    #[test]
    fn test_log_recover_non_unique_disk_tree_scan_suppresses_exact_cold_delete() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_recovery_engine_config(main_dir.clone(), "recover12")
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::U32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    vec![
                        IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                    ],
                )
                .await
                .unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut same_row_ids = Vec::new();
            let mut trx = session.begin_trx().unwrap();
            for id in [1u32, 2, 3] {
                let insert = trx_insert_row(
                    &mut trx,
                    &table,
                    vec![Val::from(id), Val::from("same-name")],
                )
                .await;
                let Ok(row_id) = insert else {
                    panic!("same-name insert should succeed");
                };
                same_row_ids.push(row_id);
            }
            trx.commit().await.unwrap();

            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;
            assert!(
                same_row_ids
                    .iter()
                    .all(|row_id| *row_id < table.file().active_root_unchecked().pivot_row_id)
            );

            let delete_key = SelectKey::new(0, vec![Val::from(2u32)]);
            let mut trx = session.begin_trx().unwrap();
            let delete = trx_delete_row(&mut trx, &table, &delete_key).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx.commit().await.unwrap();

            drop(table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine = lightweight_recovery_engine_config(main_dir, "recover12")
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            assert_eq!(session.total_row_pages(table.table_id()).await.unwrap(), 0);

            let name_key = SelectKey::new(1, vec![Val::from("same-name")]);
            let layout = table.layout_snapshot();
            let non_unique = layout.secondary_index(name_key.index_no).unwrap();
            let root =
                table.file().active_root_unchecked().secondary_index_roots[name_key.index_no];
            let disk_rows = {
                let pool_guards = session.pool_guards();
                let disk = non_unique
                    .disk_runtime()
                    .open_non_unique_at(root, pool_guards.disk_guard())
                    .unwrap();
                disk.prefix_scan_entries(&name_key.vals)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(_, row_id)| row_id)
                    .collect::<Vec<_>>()
            };
            assert_eq!(disk_rows, same_row_ids);
            drop(layout);
            match table.deletion_buffer().get(same_row_ids[1]).unwrap() {
                DeleteMarker::Committed(_) => (),
                DeleteMarker::Ref(_) => panic!("recovered cold delete should be committed"),
            }

            let mut trx = session.begin_trx().unwrap();
            let rows = trx
                .exec(async |stmt| {
                    Ok(stmt
                        .table_index_scan_mvcc(table.table_id(), &name_key, &[0, 1])
                        .await?
                        .unwrap_rows())
                })
                .await
                .unwrap();
            assert_eq!(
                rows,
                vec![
                    vec![Val::from(1u32), Val::from("same-name")],
                    vec![Val::from(3u32), Val::from("same-name")],
                ]
            );
            let deleted = trx_select_row_mvcc(&mut trx, &table, &delete_key, &[0, 1]).await;
            assert!(matches!(deleted, Ok(SelectMvcc::NotFound)));
            trx.commit().await.unwrap();

            drop(table);
            drop(session);
            drop(engine);
        })
    }

    #[test]
    fn test_log_recover_replays_post_checkpoint_heap_redo_after_bootstrap() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover6"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::U32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            let catalog_replay_start_ts = engine
                .catalog()
                .storage
                .checkpoint_snapshot()
                .unwrap()
                .catalog_replay_start_ts;
            assert!(catalog_replay_start_ts > MIN_SNAPSHOT_TS);

            let table = engine.catalog().get_table(table_id).await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &table,
                vec![Val::from(7u32), Val::from("cold-row")],
            )
            .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;
            let root_after_checkpoint = table.file().active_root_unchecked();
            assert!(root_after_checkpoint.heap_redo_start_ts > catalog_replay_start_ts);

            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &table,
                vec![Val::from(8u32), Val::from("hot-row")],
            )
            .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            drop(table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover6"))
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            assert!(session.total_row_pages(table.table_id()).await.unwrap() > 0);

            let mut trx = session.begin_trx().unwrap();

            let cold_key = SelectKey::new(0, vec![Val::from(7u32)]);
            let cold_row = trx_select_row_mvcc(&mut trx, &table, &cold_key, &[0, 1]).await;
            assert_eq!(
                cold_row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("cold-row")]
            );

            let hot_key = SelectKey::new(0, vec![Val::from(8u32)]);
            let hot_row = trx_select_row_mvcc(&mut trx, &table, &hot_key, &[0, 1]).await;
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

    #[test]
    fn test_log_recover_skips_checkpointed_and_replays_newer_cold_deletes() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover10"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec::new(vec![ColumnSpec::new(
                        "id",
                        ValKind::U32,
                        ColumnAttributes::empty(),
                    )]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.begin_trx().unwrap();
            for i in 0..10u32 {
                let insert = trx_insert_row(&mut trx, &table, vec![Val::from(i)]).await;
                assert!(insert.is_ok());
            }
            trx.commit().await.unwrap();

            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;

            let mut trx = session.begin_trx().unwrap();
            let key0 = SelectKey::new(0, vec![Val::from(0u32)]);
            let delete = trx_delete_row(&mut trx, &table, &key0).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx.commit().await.unwrap();

            let marker0_ts = match table.deletion_buffer().get(RowID::new(0)).unwrap() {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            let trx_sys = session.engine().trx_sys.clone();
            let mut ready = false;
            for _ in 0..50 {
                if trx_sys.calc_min_active_sts_for_gc() > marker0_ts {
                    ready = true;
                    break;
                }
                Timer::after(Duration::from_millis(20)).await;
            }
            assert!(
                ready,
                "deletion marker ts {} not yet below checkpoint cutoff",
                marker0_ts
            );
            checkpoint_published(&table, &mut checkpoint_session).await;
            let checkpointed_cutoff = table.file().active_root_unchecked().deletion_cutoff_ts;
            assert!(checkpointed_cutoff > marker0_ts);

            let mut trx = session.begin_trx().unwrap();
            let key1 = SelectKey::new(0, vec![Val::from(1u32)]);
            let delete = trx_delete_row(&mut trx, &table, &key1).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx.commit().await.unwrap();
            let marker1_ts = match table.deletion_buffer().get(RowID::new(1)).unwrap() {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            assert!(marker1_ts >= checkpointed_cutoff);

            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(&mut trx, &table, vec![Val::from(100u32)]).await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            drop(trx_sys);
            drop(table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover10"))
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
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

            let row0 = trx_select_row_mvcc(
                &mut trx,
                &table,
                &SelectKey::new(0, vec![Val::from(0u32)]),
                &[0],
            )
            .await;
            assert!(matches!(row0, Ok(SelectMvcc::NotFound)));

            let row1 = trx_select_row_mvcc(
                &mut trx,
                &table,
                &SelectKey::new(0, vec![Val::from(1u32)]),
                &[0],
            )
            .await;
            assert!(matches!(row1, Ok(SelectMvcc::NotFound)));

            let row100 = trx_select_row_mvcc(
                &mut trx,
                &table,
                &SelectKey::new(0, vec![Val::from(100u32)]),
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

    #[test]
    fn test_log_recover_handles_mixed_user_table_checkpoint_states() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover7"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let checkpointed_table_id = session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::U32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();
            let replay_only_table_id = session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::U32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            let baseline_catalog_replay_start_ts = engine
                .catalog()
                .storage
                .checkpoint_snapshot()
                .unwrap()
                .catalog_replay_start_ts;
            assert!(baseline_catalog_replay_start_ts > MIN_SNAPSHOT_TS);

            let checkpointed_table = engine
                .catalog()
                .get_table(checkpointed_table_id)
                .await
                .unwrap();
            let replay_only_table = engine
                .catalog()
                .get_table(replay_only_table_id)
                .await
                .unwrap();

            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &checkpointed_table,
                vec![Val::from(7u32), Val::from("persisted-row")],
            )
            .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            session
                .freeze_table(checkpointed_table.table_id(), usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(&checkpointed_table, &mut checkpoint_session).await;

            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &replay_only_table,
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
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            let final_catalog_replay_start_ts = engine
                .catalog()
                .storage
                .checkpoint_snapshot()
                .unwrap()
                .catalog_replay_start_ts;
            assert!(final_catalog_replay_start_ts > baseline_catalog_replay_start_ts);

            drop(replay_only_table);
            drop(checkpointed_table);
            drop(checkpoint_session);
            drop(session);
            drop(engine);

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover7"))
                .build()
                .await
                .unwrap();

            let checkpointed_table = engine
                .catalog()
                .get_table(checkpointed_table_id)
                .await
                .unwrap();
            let replay_only_table = engine
                .catalog()
                .get_table(replay_only_table_id)
                .await
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

            let checkpointed_key = SelectKey::new(0, vec![Val::from(7u32)]);
            let checkpointed_row =
                trx_select_row_mvcc(&mut trx, &checkpointed_table, &checkpointed_key, &[0, 1])
                    .await;
            assert_eq!(
                checkpointed_row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("persisted-row")]
            );

            let replay_only_key = SelectKey::new(0, vec![Val::from(8u32)]);
            let replay_only_row =
                trx_select_row_mvcc(&mut trx, &replay_only_table, &replay_only_key, &[0, 1]).await;
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

    #[test]
    fn test_log_recover_defers_corrupted_persisted_lwc_block_until_read() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover8"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::U32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &table,
                vec![Val::from(7u32), Val::from("persisted-row")],
            )
            .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;

            let active_root = table.file().active_root_unchecked();
            let block_id = {
                let disk_pool_guard = table.disk_pool().pool_guard();
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

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover8"))
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(7u32)]);
            let res = trx_select_row_mvcc(&mut trx, &table, &key, &[0, 1]).await;
            let err = match res {
                Err(err) => err,
                other => panic!("expected persisted LWC corruption on read, got {other:?}"),
            };
            assert_table_data_integrity(
                err,
                "lwc-block",
                block_id,
                DataIntegrityError::ChecksumMismatch,
            );
            trx.rollback().await.unwrap();

            drop(table);
            drop(session);
            drop(engine);
        })
    }

    #[test]
    fn test_log_recover_defers_invalid_delete_blob_framing_until_delta_load() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover9"))
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec::new(vec![ColumnSpec::new(
                        "id",
                        ValKind::U32,
                        ColumnAttributes::empty(),
                    )]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
                .unwrap();

            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.begin_trx().unwrap();
            for i in 0..80u32 {
                let insert = trx_insert_row(&mut trx, &table, vec![Val::from(i)]).await;
                assert!(insert.is_ok());
            }
            trx.commit().await.unwrap();

            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;

            let mut trx = session.begin_trx().unwrap();
            for i in 0..64u32 {
                let key = SelectKey::new(0, vec![Val::from(i)]);
                let delete = trx_delete_row(&mut trx, &table, &key).await;
                assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            }
            trx.commit().await.unwrap();

            let marker = table.deletion_buffer().get(RowID::new(0)).unwrap();
            let marker_ts = match marker {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            let trx_sys = session.engine().trx_sys.clone();
            let mut ready = false;
            for _ in 0..50 {
                if trx_sys.calc_min_active_sts_for_gc() > marker_ts {
                    ready = true;
                    break;
                }
                Timer::after(Duration::from_millis(20)).await;
            }
            assert!(
                ready,
                "deletion marker ts {} not yet below checkpoint cutoff",
                marker_ts
            );

            let mut trx = session.begin_trx().unwrap();
            let insert = trx_insert_row(&mut trx, &table, vec![Val::from(1000u32)]).await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            session
                .freeze_table(table.table_id(), usize::MAX)
                .await
                .unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;

            let active_root = table.file().active_root_unchecked();
            let blob_ref = {
                let disk_pool_guard = table.disk_pool().pool_guard();
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
            drop(trx_sys);
            drop(checkpoint_session);
            drop(table);
            drop(session);
            drop(engine);

            corrupt_blob_header_kind(
                table_file_path,
                blob_ref.start_block_id,
                blob_ref.start_offset,
            );

            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default().log_file_stem("recover9"))
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
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
                let err = match index.load_delete_deltas(&entry).await {
                    Ok(_) => panic!("expected invalid delete blob on delta load"),
                    Err(err) => err,
                };
                assert_table_data_integrity(
                    err,
                    "column-deletion-blob",
                    blob_ref.start_block_id,
                    DataIntegrityError::InvalidPayload,
                );
            }

            drop(table);
            drop(session);
            drop(engine);
        })
    }
}
