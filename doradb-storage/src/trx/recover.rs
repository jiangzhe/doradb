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
use crate::buffer::PageID;
use crate::buffer::guard::PageGuard;
use crate::buffer::{
    BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuards, PoolRole, ReadonlyBufferPool,
};
use crate::catalog::{
    Catalog, CatalogTable, IndexDdlKind, IndexDdlRootProof, TableColumnLayout, TableID,
    classify_index_ddl_root, is_catalog_obj_id, is_user_obj_id,
};
use crate::error::{DataIntegrityError, Error, OperationError, Result};
use crate::file::fs::FileSystem;
use crate::latch::LatchFallbackMode;
use crate::quiescent::QuiescentGuard;
use crate::row::{RowID, RowPage};
use crate::table::Table;
use crate::trx::log::{LogPartition, LogPartitionInitializer};
use crate::trx::log_replay::{LogMerger, LogPartitionStream, TrxLog};
use crate::trx::purge::{DroppedTableFileDeleteItem, GC};
use crate::trx::redo::{DDLRedo, RedoLogs, RowRedo, RowRedoKind, TableDML};
use crate::trx::{MAX_SNAPSHOT_TS, MIN_SNAPSHOT_TS, TrxID};
use crossbeam_utils::CachePadded;
use error_stack::Report;
use flume::Receiver;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Per-row recovery map used while rebuilding one row page from redo.
pub struct RecoverMap {
    create_cts: TrxID,
    entries: Vec<Option<TrxID>>,
}

impl RecoverMap {
    /// Returns an empty recover map.
    #[inline]
    pub fn empty() -> Self {
        RecoverMap::new(0)
    }

    /// Returns a recover map with given create CTS.
    #[inline]
    pub fn new(create_cts: TrxID) -> Self {
        RecoverMap {
            create_cts,
            entries: vec![],
        }
    }

    /// Returns CTS when this page is created.
    #[inline]
    pub fn create_cts(&self) -> TrxID {
        self.create_cts
    }

    /// Returns whether entry of given row position is vacant.
    #[inline]
    pub fn is_vacant(&self, row_idx: usize) -> bool {
        row_idx >= self.entries.len() || self.entries[row_idx].is_none()
    }

    /// Insert CTS at given row position.
    #[inline]
    pub fn insert_at(&mut self, row_idx: usize, cts: TrxID) {
        while self.entries.len() <= row_idx {
            self.entries.push(None);
        }
        self.entries[row_idx] = Some(cts);
    }

    /// Update CTS at given row position.
    #[inline]
    pub fn update_at(&mut self, row_idx: usize, cts: TrxID) {
        debug_assert!(row_idx < self.entries.len());
        debug_assert!(self.at(row_idx).unwrap() <= cts);
        self.entries[row_idx].replace(cts);
    }

    /// Returns CTS at given row position.
    #[inline]
    pub fn at(&self, row_idx: usize) -> Option<TrxID> {
        self.entries.get(row_idx).and_then(|v| *v)
    }
}

pub(crate) async fn log_recover(
    meta_pool: &FixedBufferPool,
    deps: RecoveryDeps,
    catalog: &Catalog,
    mut log_partition_initializers: Vec<LogPartitionInitializer>,
) -> Result<(
    Vec<CachePadded<LogPartition>>,
    Vec<Receiver<GC>>,
    TrxID,
    Vec<DroppedTableFileDeleteItem>,
)> {
    let RecoveryDeps {
        index_pool,
        mem_pool,
        table_fs,
        disk_pool,
    } = deps;
    // In recovery, we disable GC and redo logging.
    // All data are purely processed in memory and if
    // any failure occurs, we abort the whole process.
    let log_partitions = log_partition_initializers.len();
    let mut log_merger = LogMerger::default();
    for initializer in log_partition_initializers {
        let stream = initializer.stream();
        log_merger.add_stream(stream)?;
    }
    let log_recovery = LogRecovery::new(
        meta_pool, index_pool, mem_pool, table_fs, disk_pool, catalog, log_merger,
    );
    let (log_streams, max_recovered_cts, dropped_table_file_deletes) =
        log_recovery.recover_all().await?;
    let next_trx_ts = max_recovered_cts
        .checked_add(1)
        .filter(|ts| *ts < MAX_SNAPSHOT_TS)
        .ok_or_else(|| {
            Error::from(
                Report::new(DataIntegrityError::LogFileCorrupted).attach(format!(
                    "recovered commit timestamp out of range: max_recovered_cts={max_recovered_cts}"
                )),
            )
        })?;
    log_partition_initializers = log_streams
        .into_iter()
        .map(|s| s.into_initializer())
        .collect();
    log_partition_initializers.sort_by_key(|i| i.log_no);
    debug_assert_eq!(log_partition_initializers.len(), log_partitions);

    let mut partitions = vec![];
    let mut gc_rxs = vec![];
    for initializer in log_partition_initializers {
        let (partition, gc_rx) = initializer.finish()?;
        partitions.push(CachePadded::new(partition));
        gc_rxs.push(gc_rx);
    }
    Ok((partitions, gc_rxs, next_trx_ts, dropped_table_file_deletes))
}

pub(crate) struct RecoveryDeps {
    pub(crate) index_pool: QuiescentGuard<EvictableBufferPool>,
    pub(crate) mem_pool: QuiescentGuard<EvictableBufferPool>,
    pub(crate) table_fs: QuiescentGuard<FileSystem>,
    pub(crate) disk_pool: QuiescentGuard<ReadonlyBufferPool>,
}

/// Redo-log recovery coordinator for catalog metadata and user tables.
pub struct LogRecovery<'a> {
    /// Mutable secondary-index buffer pool used while rebuilding hot MemIndex state.
    index_pool: QuiescentGuard<EvictableBufferPool>,
    /// Mutable row-page buffer pool used for recovered heap pages.
    mem_pool: QuiescentGuard<EvictableBufferPool>,
    /// Table file system used to reload checkpointed user-table files.
    table_fs: QuiescentGuard<FileSystem>,
    /// Readonly disk buffer pool used by checkpointed column and DiskTree state.
    disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    /// Catalog runtime being rebuilt from checkpointed metadata and redo logs.
    catalog: &'a Catalog,
    /// Ordered view over all redo-log partitions.
    log_merger: LogMerger,
    /// Catalog checkpoint boundary. Catalog redo before this timestamp is
    /// already reflected in checkpointed catalog state.
    catalog_replay_start_ts: TrxID,
    /// Earliest redo timestamp that may still affect any loaded runtime state.
    ///
    /// This starts at the catalog replay boundary and is lowered by loaded
    /// user-table heap/delete replay starts so old irrelevant log records can be
    /// skipped before per-table filtering.
    replay_floor: TrxID,
    /// Highest timestamp observed in checkpoint metadata, table roots, or redo
    /// log headers during recovery.
    ///
    /// This is a timestamp-generator watermark, not a replay filter. It is
    /// updated even for redo records skipped by replay boundaries so runtime
    /// transaction timestamps restart at `max_recovered_cts + 1` and never reuse
    /// a historical CTS.
    max_recovered_cts: TrxID,
    /// Per loaded user table, the persisted replay boundaries from its active root.
    table_states: HashMap<TableID, RecoveryTableState>,
    /// Tables loaded from table-file metadata while catalog index DDL redo is pending.
    pending_index_ddl_reconciliations: HashSet<TableID>,
    /// Hot row pages touched by redo replay, grouped by table for post-replay
    /// index rebuild and undo-map refresh.
    recovered_tables: HashMap<TableID, BTreeSet<PageID>>,
    /// Count of unknown user-table redo entries skipped because checkpointed
    /// catalog absence proves they are older than the catalog replay boundary.
    skipped_checkpoint_covered_unknown_table_redo: usize,
    /// Dropped user-table files whose runtime state was destroyed during replay.
    dropped_table_file_deletes: Vec<DroppedTableFileDeleteItem>,
    /// Stable pool guards shared by recovery operations.
    pool_guards: PoolGuards,
}

#[derive(Clone, Copy, Debug)]
struct RecoveryTableState {
    /// Active table-root publication timestamp observed during recovery bootstrap.
    root_trx_id: TrxID,
    /// Lower bound for replaying heap row-page redo for this table.
    heap_redo_start_ts: TrxID,
    /// Lower bound for replaying persisted cold-delete metadata for this table.
    deletion_cutoff_ts: TrxID,
}

impl RecoveryTableState {
    #[inline]
    fn replay_start_ts(self) -> TrxID {
        self.heap_redo_start_ts.min(self.deletion_cutoff_ts)
    }
}

#[inline]
fn validate_create_table_reloaded_root_cts(
    table_id: TableID,
    create_table_cts: TrxID,
    state: RecoveryTableState,
    pending_index_ddl_reconciliation: bool,
) -> Result<()> {
    // Create-table redo reopens the latest table root, not a historical root
    // pinned to the create-table CTS. The root is allowed to be newer, but it
    // must at least include the create-table publication itself.
    if state.root_trx_id < create_table_cts {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "recovered create-table root predates create-table redo: table_id={table_id}, root_trx_id={}, create_table_cts={create_table_cts}",
                state.root_trx_id
            ))
            .into());
    }
    // A metadata mismatch accepted by reload means the file root is ahead of
    // the catalog rows via later index DDL. If the root CTS is exactly the
    // create-table CTS, there is no later root publication to justify that
    // temporary divergence.
    if pending_index_ddl_reconciliation && state.root_trx_id <= create_table_cts {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "create-table pending index-DDL reconciliation requires later table root: table_id={table_id}, root_trx_id={}, create_table_cts={create_table_cts}",
                state.root_trx_id
            ))
            .into());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UserTableRedoAction {
    Replay,
    SkipCheckpointCoveredUnknownTable,
}

impl<'a> LogRecovery<'a> {
    #[inline]
    fn new(
        meta_pool: &FixedBufferPool,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        table_fs: QuiescentGuard<FileSystem>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
        catalog: &'a Catalog,
        log_merger: LogMerger,
    ) -> Self {
        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, meta_pool.pool_guard())
            .push(PoolRole::Index, index_pool.pool_guard())
            .push(PoolRole::Mem, mem_pool.pool_guard())
            .push(PoolRole::Disk, disk_pool.pool_guard())
            .build();
        LogRecovery {
            index_pool,
            mem_pool,
            table_fs,
            disk_pool,
            catalog,
            log_merger,
            catalog_replay_start_ts: MIN_SNAPSHOT_TS,
            replay_floor: MIN_SNAPSHOT_TS,
            max_recovered_cts: MIN_SNAPSHOT_TS,
            table_states: HashMap::new(),
            pending_index_ddl_reconciliations: HashSet::new(),
            recovered_tables: HashMap::new(),
            skipped_checkpoint_covered_unknown_table_redo: 0,
            dropped_table_file_deletes: Vec::new(),
            pool_guards,
        }
    }

    /// Replay all redo streams, rebuild indexes, and return reopened partition streams.
    #[inline]
    pub(crate) async fn recover_all(
        mut self,
    ) -> Result<(
        Vec<LogPartitionStream>,
        TrxID,
        Vec<DroppedTableFileDeleteItem>,
    )> {
        self.bootstrap_checkpointed_user_tables().await?;
        // 1. replay DDL and DML into catalog metadata, hot RowStore pages, and
        //    cold delete markers.
        while let Some(log) = self.log_merger.try_next()? {
            self.replay_log(log).await?;
        }
        // 2. Ensure catalog metadata caught up with table-file roots.
        self.validate_loaded_table_metadata().await?;
        // 3. Rebuild hot secondary-index state from recovered RowStore pages
        //    and refresh pages to enable undo maps.
        self.recover_indexes_and_refresh_pages().await?;

        Ok((
            self.log_merger.finished_streams(),
            self.max_recovered_cts,
            self.dropped_table_file_deletes,
        ))
    }

    async fn bootstrap_checkpointed_user_tables(&mut self) -> Result<()> {
        let snapshot = self.catalog.storage.checkpoint_snapshot()?;
        self.catalog_replay_start_ts = snapshot.catalog_replay_start_ts;
        self.replay_floor = snapshot.catalog_replay_start_ts;
        self.max_recovered_cts = self.max_recovered_cts.max(snapshot.catalog_replay_start_ts);

        let checkpointed_tables = self
            .catalog
            .storage
            .tables()
            .list_uncommitted(&self.pool_guards)
            .await?;
        let checkpointed_user_table_ids = checkpointed_tables
            .iter()
            .filter(|table| is_user_obj_id(table.table_id))
            .map(|table| table.table_id)
            .collect::<HashSet<_>>();
        self.table_fs.cleanup_checkpoint_absent_user_table_files(
            snapshot.meta.next_user_obj_id,
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
                .catalog
                .reload_create_table(
                    self.mem_pool.clone(),
                    self.index_pool.clone(),
                    &self.table_fs,
                    self.disk_pool.clone(),
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
            self.replay_floor = self.replay_floor.min(state.replay_start_ts());
        }
        Ok(())
    }

    #[inline]
    fn should_replay_catalog(&self, cts: TrxID) -> bool {
        cts >= self.catalog_replay_start_ts
    }

    #[inline]
    fn classify_user_table_redo(
        &self,
        table_id: TableID,
        cts: TrxID,
        context: &'static str,
    ) -> Result<UserTableRedoAction> {
        if self.table_states.contains_key(&table_id) {
            return Ok(UserTableRedoAction::Replay);
        }
        if cts < self.catalog_replay_start_ts {
            return Ok(UserTableRedoAction::SkipCheckpointCoveredUnknownTable);
        }
        Err(Report::new(OperationError::TableNotFound)
            .attach(format!(
                "invalid recovery ordering: {context}: unknown user table redo at or after catalog replay boundary: table_id={table_id}, cts={cts}, catalog_replay_start_ts={}",
                self.catalog_replay_start_ts
            ))
            .into())
    }

    #[inline]
    fn record_skipped_checkpoint_covered_unknown_table_redo(&mut self, count: usize) {
        self.skipped_checkpoint_covered_unknown_table_redo = self
            .skipped_checkpoint_covered_unknown_table_redo
            .saturating_add(count);
    }

    #[cfg(test)]
    #[inline]
    fn skipped_checkpoint_covered_unknown_table_redo(&self) -> usize {
        self.skipped_checkpoint_covered_unknown_table_redo
    }

    async fn track_loaded_table(&mut self, table_id: TableID) -> Result<RecoveryTableState> {
        let table = self.catalog.get_table(table_id).await.ok_or_else(|| {
            Error::from(
                Report::new(OperationError::TableNotFound)
                    .attach(format!("track loaded table runtime: table_id={table_id}")),
            )
        })?;
        // `recovery_bootstrap_unchecked`: recovery records replay floors from
        // the table root loaded during restart, before normal transactions run.
        let active_root = table.file().active_root_unchecked();
        let state = RecoveryTableState {
            root_trx_id: active_root.trx_id,
            heap_redo_start_ts: active_root.heap_redo_start_ts,
            deletion_cutoff_ts: active_root.deletion_cutoff_ts,
        };
        self.max_recovered_cts = self
            .max_recovered_cts
            .max(state.root_trx_id)
            .max(state.heap_redo_start_ts)
            .max(state.deletion_cutoff_ts);
        let old = self.table_states.insert(table_id, state);
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
        state: RecoveryTableState,
    ) -> Result<()> {
        // No upper bound is checked here: a table root may legitimately be newer
        // than the catalog checkpoint. The invalid state is the opposite case,
        // where a pre-cursor root would require catalog redo that checkpoint
        // bootstrap has already declared unnecessary.
        if state.root_trx_id < self.catalog_replay_start_ts {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "checkpointed table pending index-DDL reconciliation has root before catalog replay boundary: table_id={table_id}, root_trx_id={}, catalog_replay_start_ts={}",
                    state.root_trx_id,
                    self.catalog_replay_start_ts
                ))
                .into());
        }
        Ok(())
    }

    #[inline]
    fn table_heap_redo_start_ts(&self, table_id: TableID) -> Result<TrxID> {
        self.table_states
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
        self.table_states
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
        self.table_states
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
        self.max_recovered_cts = self.max_recovered_cts.max(header.cts);
        if header.cts < self.replay_floor {
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
            if let Some(table) = self.catalog.get_table(*table_id).await {
                let metadata = table.metadata();
                for page_id in pages {
                    table
                        .populate_index_via_row_page(&self.pool_guards, *page_id)
                        .await?;
                    self.refresh_page(Arc::clone(&metadata.col), *page_id)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn validate_loaded_table_metadata(&mut self) -> Result<()> {
        let table_ids = self.table_states.keys().copied().collect::<Vec<_>>();
        for table_id in table_ids {
            let table = self.catalog.get_table(table_id).await.ok_or_else(|| {
                Error::from(Report::new(OperationError::TableNotFound).attach(format!(
                    "validate recovered user table metadata: table_id={table_id}"
                )))
            })?;
            let active_root = table.file().active_root_unchecked();
            let (_, catalog_metadata) = self
                .catalog
                .user_table_metadata_from_catalog(&self.pool_guards, table_id)
                .await?;
            if catalog_metadata != *active_root.metadata {
                let pending = self.pending_index_ddl_reconciliations.contains(&table_id);
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach(format!(
                        "recovered user table metadata mismatch after redo replay: table_id={table_id}, root_trx_id={}, catalog_replay_start_ts={}, pending_index_ddl_reconciliation={pending}, catalog_next_index_no={}, root_next_index_no={}",
                        active_root.trx_id,
                        self.catalog_replay_start_ts,
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

    async fn refresh_page(
        &self,
        col_layout: Arc<TableColumnLayout>,
        page_id: PageID,
    ) -> Result<()> {
        let mut page_guard = self
            .mem_pool
            .get_page::<RowPage>(
                self.pool_guards.mem_guard(),
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
            .unwrap_or(0);
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
                    .catalog
                    .reload_create_table(
                        self.mem_pool.clone(),
                        self.index_pool.clone(),
                        &self.table_fs,
                        self.disk_pool.clone(),
                        *table_id,
                    )
                    .await?;
                let state = self.track_loaded_table(*table_id).await?;
                let pending_index_ddl_reconciliation = !metadata_matched;
                // Validate the root/create CTS relation before marking the table
                // as pending, so impossible metadata divergence fails at the
                // source instead of surfacing only in final equality validation.
                validate_create_table_reloaded_root_cts(
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
                let removed = self.catalog.remove_user_table(*table_id).ok_or_else(|| {
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
                table.destroy_dropped_runtime(&self.pool_guards).await?;
                self.dropped_table_file_deletes
                    .push(DroppedTableFileDeleteItem::new(*table_id, cts));
                self.table_states.remove(table_id);
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
                    self.record_skipped_checkpoint_covered_unknown_table_redo(1);
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
                    self.record_skipped_checkpoint_covered_unknown_table_redo(1);
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
                    self.record_skipped_checkpoint_covered_unknown_table_redo(1);
                    return Ok(());
                }
                if cts < self.table_heap_redo_start_ts(*table_id)? {
                    return Ok(());
                }
                // Row page creation is guaranteed to be ordered in the redo log,
                // so its safe to recreate it and the row id range must be identical.
                let table = self.catalog.get_table(*table_id).await.ok_or_else(|| {
                    Error::from(
                        Report::new(OperationError::TableNotFound)
                            .attach(format!("replay create row page: table_id={table_id}")),
                    )
                })?;
                let count = end_row_id - start_row_id;
                let mut page_guard = table
                    .mem
                    .allocate_row_page_at(&self.pool_guards, count as usize, *page_id)
                    .await?;
                // Here we switch row page to recover mode.
                page_guard.bf_mut().init_recover_map(cts);

                // Record recovered pages so we can recover indexes and refresh undo map at end.
                // Note: we do not need to recover catalog tables because they are specially handled.
                if self.catalog.is_user_table(*table_id) {
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
                    self.record_skipped_checkpoint_covered_unknown_table_redo(1);
                    return Ok(());
                }
                if cts < self.table_heap_redo_start_ts(*table_id)? {
                    return Ok(());
                }
                let _ = self.catalog.get_table(*table_id).await.ok_or_else(|| {
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
        let table = self.catalog.get_table_now(table_id);
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
                let table = self.catalog.get_catalog_table(table_id).ok_or_else(|| {
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
                self.record_skipped_checkpoint_covered_unknown_table_redo(table_dml.rows.len());
                continue;
            }
            if cts < self.table_replay_start_ts(table_id)? {
                continue;
            }
            let table = self.catalog.get_table(table_id).await.ok_or_else(|| {
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
            let table = self.catalog.get_catalog_table(table_id).ok_or_else(|| {
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
                    table.insert_no_trx(&self.pool_guards, vals).await?;
                }
                RowRedoKind::DeleteByUniqueKey(key) => {
                    table.delete_unique_no_trx(&self.pool_guards, key).await?;
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
                        .recover_row_insert(&self.pool_guards, row.page_id, row.row_id, vals, cts)
                        .await?;
                }
                RowRedoKind::Update(vals) => {
                    if cts < heap_redo_start_ts {
                        continue;
                    }
                    table
                        .recover_row_update(&self.pool_guards, row.page_id, row.row_id, vals, cts)
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
                        .recover_row_delete(&self.pool_guards, row.page_id, row.row_id, cts)
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

#[cfg(test)]
mod tests {
    use super::{
        LogRecovery, RecoverMap, RecoveryTableState, validate_create_table_reloaded_root_cts,
    };
    use crate::buffer::{PageID, PoolRole};
    use crate::catalog::{
        ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexAttributes, IndexColumnObject,
        IndexKey, IndexObject, IndexOrder, IndexSpec, TableID, TableMetadata, TableObject,
        TableSpec, USER_OBJ_ID_START,
    };
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
    use crate::error::{CompletionErrorKind, DataIntegrityError, Error, OperationError};
    use crate::file::block_integrity::{BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum};
    use crate::file::cow_file::{COW_FILE_PAGE_SIZE, SUPER_BLOCK_ID};
    use crate::file::table_file::MutableTableFile;
    use crate::index::{
        COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE, ColumnBlockIndex, UniqueIndex,
        load_entry_deletion_deltas,
    };
    use crate::row::ops::{DeleteMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
    use crate::row::{RowID, RowRead};
    use crate::table::{CheckpointOutcome, DeleteMarker, Table, TablePersistence};
    use crate::trx::log_replay::{LogMerger, TrxLog};
    use crate::trx::redo::{DDLRedo, RedoHeader, RedoLogs, RedoTrxKind, RowRedo, RowRedoKind};
    use crate::trx::stmt::Statement;
    use crate::trx::{ActiveTrx, MIN_SNAPSHOT_TS, TrxID};
    use crate::value::Val;
    use crate::value::ValKind;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::Arc;
    use tempfile::TempDir;

    const LIGHTWEIGHT_RECOVERY_BUFFER_BYTES: usize = 16 * 1024 * 1024;
    const LIGHTWEIGHT_RECOVERY_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
    const LIGHTWEIGHT_RECOVERY_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;

    fn assert_table_data_integrity(
        err: Error,
        block_kind: &str,
        block_id: crate::file::BlockID,
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
        main_dir: impl Into<std::path::PathBuf>,
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
                    .io_depth_per_log(1)
                    .log_file_stem(log_file_stem)
                    .purge_threads(1),
            )
            .file(
                FileSystemConfig::default()
                    .io_depth(1)
                    .readonly_buffer_size(LIGHTWEIGHT_RECOVERY_READONLY_BUFFER_BYTES),
            )
    }

    fn log_recovery_for_engine<'a>(
        engine: &'a crate::engine::Engine,
        catalog_replay_start_ts: TrxID,
    ) -> LogRecovery<'a> {
        let mut recovery = LogRecovery::new(
            &engine.meta_pool,
            engine.index_pool.clone_inner(),
            engine.mem_pool.clone_inner(),
            engine.table_fs.clone(),
            engine.disk_pool.clone_inner(),
            engine.catalog(),
            LogMerger::default(),
        );
        recovery.catalog_replay_start_ts = catalog_replay_start_ts;
        recovery.replay_floor = MIN_SNAPSHOT_TS;
        recovery
    }

    fn redo_header(cts: TrxID) -> RedoHeader {
        RedoHeader {
            cts,
            trx_kind: RedoTrxKind::User,
        }
    }

    fn unknown_table_dml_log(table_id: u64, cts: TrxID) -> TrxLog {
        let mut redo = RedoLogs::default();
        redo.insert_dml(
            table_id,
            RowRedo {
                page_id: PageID::new(1),
                row_id: 0,
                kind: RowRedoKind::Insert(vec![Val::from(1u32)]),
            },
        );
        TrxLog::new(redo_header(cts), redo)
    }

    fn unknown_table_create_row_page_log(table_id: u64, cts: TrxID) -> TrxLog {
        TrxLog::new(
            redo_header(cts),
            RedoLogs {
                ddl: Some(Box::new(DDLRedo::CreateRowPage {
                    table_id,
                    page_id: PageID::new(2),
                    start_row_id: 0,
                    end_row_id: 1,
                })),
                dml: Default::default(),
            },
        )
    }

    fn unknown_table_data_checkpoint_log(table_id: u64, cts: TrxID) -> TrxLog {
        TrxLog::new(
            redo_header(cts),
            RedoLogs {
                ddl: Some(Box::new(DDLRedo::DataCheckpoint {
                    table_id,
                    pivor_row_id: 0,
                    sts: cts,
                })),
                dml: Default::default(),
            },
        )
    }

    async fn checkpoint_published(table: &Table, session: &mut crate::session::Session) -> TrxID {
        match table.checkpoint(session).await.unwrap() {
            CheckpointOutcome::Published { checkpoint_ts } => checkpoint_ts,
            CheckpointOutcome::Delayed { reason } => {
                panic!("checkpoint should publish, delayed by {reason:?}")
            }
            CheckpointOutcome::Cancelled { reason } => {
                panic!("checkpoint should publish, cancelled by {reason:?}")
            }
        }
    }

    async fn stmt_insert_row(
        stmt: &mut Statement<'_>,
        table: &Table,
        cols: Vec<Val>,
    ) -> crate::error::Result<RowID> {
        stmt.table_insert_mvcc(table, cols).await
    }

    async fn stmt_delete_row(
        stmt: &mut Statement<'_>,
        table: &Table,
        key: &SelectKey,
    ) -> crate::error::Result<DeleteMvcc> {
        stmt.table_delete_unique_mvcc(table, key, false).await
    }

    async fn stmt_update_row(
        stmt: &mut Statement<'_>,
        table: &Table,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> crate::error::Result<UpdateMvcc> {
        stmt.table_update_unique_mvcc(table, key, update).await
    }

    async fn stmt_select_row_mvcc(
        stmt: &mut Statement<'_>,
        table: &Table,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> crate::error::Result<SelectMvcc> {
        stmt.table_lookup_unique_mvcc(table, key, user_read_set)
            .await
    }

    async fn trx_insert_row(
        trx: &mut ActiveTrx,
        table: &Table,
        cols: Vec<Val>,
    ) -> crate::error::Result<RowID> {
        trx.exec(async |stmt| stmt_insert_row(stmt, table, cols).await)
            .await
    }

    async fn trx_delete_row(
        trx: &mut ActiveTrx,
        table: &Table,
        key: &SelectKey,
    ) -> crate::error::Result<DeleteMvcc> {
        trx.exec(async |stmt| stmt_delete_row(stmt, table, key).await)
            .await
    }

    async fn trx_update_row(
        trx: &mut ActiveTrx,
        table: &Table,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> crate::error::Result<UpdateMvcc> {
        trx.exec(async |stmt| stmt_update_row(stmt, table, key, update).await)
            .await
    }

    async fn trx_select_row_mvcc(
        trx: &mut ActiveTrx,
        table: &Table,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> crate::error::Result<SelectMvcc> {
        trx.exec(async |stmt| stmt_select_row_mvcc(stmt, table, key, user_read_set).await)
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

    async fn create_index_ddl_base_table(
        engine: &crate::engine::Engine,
        indexes: Vec<IndexSpec>,
    ) -> TableID {
        let mut session = engine.try_new_session().unwrap();
        let table_id = session
            .create_table(TableSpec::new(index_ddl_columns()), indexes)
            .await
            .unwrap();
        drop(session);
        table_id
    }

    async fn commit_create_index_catalog_ddl(
        engine: &crate::engine::Engine,
        table_id: TableID,
    ) -> TrxID {
        let mut session = engine.try_new_session().unwrap();
        let mut trx = session.try_begin_trx().unwrap().unwrap();
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

    async fn commit_drop_index_catalog_ddl(
        engine: &crate::engine::Engine,
        table_id: TableID,
    ) -> TrxID {
        let mut session = engine.try_new_session().unwrap();
        let mut trx = session.try_begin_trx().unwrap().unwrap();
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
        engine: &crate::engine::Engine,
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
            engine.table_fs.background_writes(),
            table.disk_pool().clone(),
        );
        mutable
            .replace_metadata_and_secondary_index_roots(metadata, roots)
            .unwrap();
        let (_table_file, old_root) = mutable.commit(cts, false).await.unwrap();
        drop(old_root);
        drop(table);
    }

    async fn assert_recovered_index_state(
        engine: &crate::engine::Engine,
        table_id: TableID,
        next_index_no: u16,
        index_one_active: bool,
    ) {
        let table = engine.catalog().get_table(table_id).await.unwrap();
        let metadata = table.metadata();
        assert_eq!(metadata.idx.next_index_no(), next_index_no);
        assert_eq!(metadata.idx.index_spec(1).is_some(), index_one_active);

        let session = engine.try_new_session().unwrap();
        let table_obj = engine
            .catalog()
            .storage
            .tables()
            .find_uncommitted_by_id(session.pool_guards(), table_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(table_obj.next_index_no, next_index_no);
        let indexes = engine
            .catalog()
            .storage
            .indexes()
            .list_uncommitted_by_table_id(session.pool_guards(), table_id)
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
        let mut map = RecoverMap::new(7);
        assert_eq!(map.create_cts(), 7);
        assert!(map.is_vacant(0));

        map.insert_at(2, 11);
        assert!(map.is_vacant(0));
        assert!(map.is_vacant(1));
        assert!(!map.is_vacant(2));
        assert_eq!(map.at(2), Some(11));
        assert_eq!(map.at(3), None);

        map.update_at(2, 13);
        assert_eq!(map.at(2), Some(13));
    }

    #[test]
    fn test_recovery_table_state_replay_start_uses_heap_and_deletion_floor() {
        let heap_first = RecoveryTableState {
            root_trx_id: 5,
            heap_redo_start_ts: 7,
            deletion_cutoff_ts: 11,
        };
        assert_eq!(heap_first.replay_start_ts(), 7);

        let deletion_first = RecoveryTableState {
            root_trx_id: 17,
            heap_redo_start_ts: 19,
            deletion_cutoff_ts: 13,
        };
        assert_eq!(deletion_first.replay_start_ts(), 13);
    }

    #[test]
    fn test_create_table_root_cts_validation_rejects_impossible_states() {
        let root_before_create = RecoveryTableState {
            root_trx_id: 9,
            heap_redo_start_ts: 9,
            deletion_cutoff_ts: 9,
        };
        let err = validate_create_table_reloaded_root_cts(
            USER_OBJ_ID_START,
            10,
            root_before_create,
            false,
        )
        .unwrap_err();
        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidRootInvariant)
        );
        let report = format!("{err:?}");
        assert!(report.contains("predates create-table redo"), "{report}");

        let pending_without_later_root = RecoveryTableState {
            root_trx_id: 10,
            heap_redo_start_ts: 10,
            deletion_cutoff_ts: 10,
        };
        let err = validate_create_table_reloaded_root_cts(
            USER_OBJ_ID_START,
            10,
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

        let pending_with_later_root = RecoveryTableState {
            root_trx_id: 11,
            heap_redo_start_ts: 10,
            deletion_cutoff_ts: 10,
        };
        validate_create_table_reloaded_root_cts(
            USER_OBJ_ID_START,
            10,
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
            let mut recovery = log_recovery_for_engine(&engine, 10);

            recovery
                .replay_log(unknown_table_dml_log(unknown_table_id, 9))
                .await
                .unwrap();
            assert_eq!(recovery.skipped_checkpoint_covered_unknown_table_redo(), 1);

            recovery
                .replay_log(unknown_table_create_row_page_log(unknown_table_id, 9))
                .await
                .unwrap();
            assert_eq!(recovery.skipped_checkpoint_covered_unknown_table_redo(), 2);

            recovery
                .replay_log(unknown_table_data_checkpoint_log(unknown_table_id, 9))
                .await
                .unwrap();
            assert_eq!(recovery.skipped_checkpoint_covered_unknown_table_redo(), 3);

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

            let mut dml_recovery = log_recovery_for_engine(&engine, 10);
            let err = dml_recovery
                .replay_log(unknown_table_dml_log(unknown_table_id, 10))
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            let report = format!("{err:?}");
            assert!(report.contains("invalid recovery ordering"), "{report}");
            assert!(report.contains("replay user table DML"), "{report}");
            assert_eq!(
                dml_recovery.skipped_checkpoint_covered_unknown_table_redo(),
                0
            );

            let mut ddl_recovery = log_recovery_for_engine(&engine, 10);
            let err = ddl_recovery
                .replay_log(unknown_table_create_row_page_log(unknown_table_id, 10))
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            let report = format!("{err:?}");
            assert!(report.contains("invalid recovery ordering"), "{report}");
            assert!(report.contains("replay create row page"), "{report}");
            assert_eq!(
                ddl_recovery.skipped_checkpoint_covered_unknown_table_redo(),
                0
            );

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
                .checkpoint_now(&engine.trx_sys)
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
                .checkpoint_now(&engine.trx_sys)
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
                .checkpoint_now(&engine.trx_sys)
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
                .checkpoint_now(&engine.trx_sys)
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
                .checkpoint_now(&engine.trx_sys)
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
                .checkpoint_now(&engine.trx_sys)
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
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let ddl_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
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
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let ddl_cts = commit_create_index_catalog_ddl(&engine, table_id).await;
            publish_index_metadata_root(&engine, table_id, created_index_metadata(), ddl_cts).await;
            engine
                .catalog()
                .checkpoint_now(&engine.trx_sys)
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

    fn corrupt_page_checksum(path: impl AsRef<std::path::Path>, page_id: impl Into<u64>) {
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
        path: impl AsRef<std::path::Path>,
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
        path: impl AsRef<std::path::Path>,
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

            let mut session = engine.try_new_session().unwrap();
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
                TableMetadata::new(table_spec.columns.clone(), index_specs.clone());

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

            let mut session = engine.try_new_session().unwrap();
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

            let table = session
                .engine()
                .catalog()
                .get_table(table_id)
                .await
                .unwrap();

            let s: String = std::iter::repeat_n('0', 100).collect();
            // insert
            for i in (0..DML_SIZE).step_by(INS_STEP) {
                let mut trx = session.try_begin_trx().unwrap().unwrap();
                for j in i..i + INS_STEP {
                    let res = trx_insert_row(
                        &mut trx,
                        &table,
                        vec![Val::from(j as u32), Val::from(&s[..])],
                    )
                    .await;
                    assert!(res.is_ok());
                }
                trx.commit().await.unwrap();
            }
            // update
            let s2: String = std::iter::repeat_n('2', 100).collect();
            for i in (0..DML_SIZE).step_by(UPD_STEP) {
                let mut trx = session.try_begin_trx().unwrap().unwrap();
                let key = SelectKey::new(0, vec![Val::from(i as u32)]);
                let uc = UpdateCol {
                    idx: 1,
                    val: Val::from(&s2[..]),
                };
                let res = trx_update_row(&mut trx, &table, &key, vec![uc]).await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                trx.commit().await.unwrap();
            }
            // delete
            for i in (0..DML_SIZE).step_by(DEL_STEP) {
                let mut trx = session.try_begin_trx().unwrap().unwrap();
                let key = SelectKey::new(0, vec![Val::from(i as u32)]);
                let res = trx_delete_row(&mut trx, &table, &key).await;
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
                trx.commit().await.unwrap();
            }

            drop(table);
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
            let session = engine.try_new_session().unwrap();
            let mut rows = 0usize;
            {
                let layout = table.layout_snapshot();
                table
                    .accessor_with_layout(&layout)
                    .mem_scan_uncommitted(session.pool_guards(), |_metadata, row| {
                        assert!(row.row_id() as usize <= DML_SIZE);
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

            let mut session = engine.try_new_session().unwrap();
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
                .checkpoint_now(&engine.trx_sys)
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

            let mut session = engine.try_new_session().unwrap();
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
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();
            let catalog_replay_start_ts = engine
                .catalog()
                .storage
                .checkpoint_snapshot()
                .unwrap()
                .catalog_replay_start_ts;

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
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

            table.freeze(&session, usize::MAX).await.unwrap();
            let mut checkpoint_session = engine.try_new_session().unwrap();
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
            let mut session = engine.try_new_session().unwrap();
            assert_eq!(table.total_row_pages(session.pool_guards()).await, 0);

            let key = SelectKey::new(0, vec![Val::from(7u32)]);
            let layout = table.layout_snapshot();
            let index = layout.secondary_index(key.index_no).unwrap();
            let root = table.file().active_root_unchecked().secondary_index_roots[key.index_no];
            {
                let disk = index
                    .disk_runtime()
                    .open_unique_at(root, session.pool_guards().disk_guard())
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
            let mut trx = session.try_begin_trx().unwrap().unwrap();
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

            let mut session = engine.try_new_session().unwrap();
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
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
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

            table.freeze(&session, usize::MAX).await.unwrap();
            let mut checkpoint_session = engine.try_new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;
            assert!(table.file().active_root_unchecked().pivot_row_id > cold_row_id);

            let key = SelectKey::new(0, vec![Val::from(7u32)]);
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let delete = trx_delete_row(&mut trx, &table, &key).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx.commit().await.unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
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
            let mut session = engine.try_new_session().unwrap();
            assert!(table.total_row_pages(session.pool_guards()).await > 0);

            let layout = table.layout_snapshot();
            let index = layout.secondary_index(key.index_no).unwrap();
            let root = table.file().active_root_unchecked().secondary_index_roots[key.index_no];
            {
                let disk = index
                    .disk_runtime()
                    .open_unique_at(root, session.pool_guards().disk_guard())
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

            let mut trx = session.try_begin_trx().unwrap().unwrap();
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

            let mut session = engine.try_new_session().unwrap();
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
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut same_row_ids = Vec::new();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
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

            table.freeze(&session, usize::MAX).await.unwrap();
            let mut checkpoint_session = engine.try_new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;
            assert!(
                same_row_ids
                    .iter()
                    .all(|row_id| *row_id < table.file().active_root_unchecked().pivot_row_id)
            );

            let delete_key = SelectKey::new(0, vec![Val::from(2u32)]);
            let mut trx = session.try_begin_trx().unwrap().unwrap();
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
            let mut session = engine.try_new_session().unwrap();
            assert_eq!(table.total_row_pages(session.pool_guards()).await, 0);

            let name_key = SelectKey::new(1, vec![Val::from("same-name")]);
            let layout = table.layout_snapshot();
            let non_unique = layout.secondary_index(name_key.index_no).unwrap();
            let root =
                table.file().active_root_unchecked().secondary_index_roots[name_key.index_no];
            let disk_rows = {
                let disk = non_unique
                    .disk_runtime()
                    .open_non_unique_at(root, session.pool_guards().disk_guard())
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

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let rows = trx
                .exec(async |stmt| {
                    Ok(stmt
                        .table_index_scan_mvcc(&table, &name_key, &[0, 1])
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

            let mut session = engine.try_new_session().unwrap();
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
                .checkpoint_now(&engine.trx_sys)
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

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &table,
                vec![Val::from(7u32), Val::from("cold-row")],
            )
            .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await.unwrap();
            let mut checkpoint_session = engine.try_new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;
            let root_after_checkpoint = table.file().active_root_unchecked();
            assert!(root_after_checkpoint.heap_redo_start_ts > catalog_replay_start_ts);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
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
            let mut session = engine.try_new_session().unwrap();
            assert!(table.total_row_pages(session.pool_guards()).await > 0);

            let mut trx = session.try_begin_trx().unwrap().unwrap();

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

            let mut session = engine.try_new_session().unwrap();
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
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..10u32 {
                let insert = trx_insert_row(&mut trx, &table, vec![Val::from(i)]).await;
                assert!(insert.is_ok());
            }
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await.unwrap();
            let mut checkpoint_session = engine.try_new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let key0 = SelectKey::new(0, vec![Val::from(0u32)]);
            let delete = trx_delete_row(&mut trx, &table, &key0).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx.commit().await.unwrap();

            let marker0_ts = match table.deletion_buffer().get(0).unwrap() {
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
                smol::Timer::after(std::time::Duration::from_millis(20)).await;
            }
            assert!(
                ready,
                "deletion marker ts {} not yet below checkpoint cutoff",
                marker0_ts
            );
            checkpoint_published(&table, &mut checkpoint_session).await;
            let checkpointed_cutoff = table.file().active_root_unchecked().deletion_cutoff_ts;
            assert!(checkpointed_cutoff > marker0_ts);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let key1 = SelectKey::new(0, vec![Val::from(1u32)]);
            let delete = trx_delete_row(&mut trx, &table, &key1).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx.commit().await.unwrap();
            let marker1_ts = match table.deletion_buffer().get(1).unwrap() {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            assert!(marker1_ts >= checkpointed_cutoff);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
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
            assert!(table.deletion_buffer().get(0).is_none());
            match table.deletion_buffer().get(1).unwrap() {
                DeleteMarker::Committed(ts) => assert_eq!(ts, marker1_ts),
                DeleteMarker::Ref(_) => panic!("recovered cold delete should be committed"),
            }

            let mut session = engine.try_new_session().unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();

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

            let mut session = engine.try_new_session().unwrap();
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
                .checkpoint_now(&engine.trx_sys)
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

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &checkpointed_table,
                vec![Val::from(7u32), Val::from("persisted-row")],
            )
            .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            checkpointed_table
                .freeze(&session, usize::MAX)
                .await
                .unwrap();
            let mut checkpoint_session = engine.try_new_session().unwrap();
            checkpoint_published(&checkpointed_table, &mut checkpoint_session).await;

            let mut trx = session.try_begin_trx().unwrap().unwrap();
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
                    > 0
            );
            assert_eq!(
                replay_only_table
                    .file()
                    .active_root_unchecked()
                    .pivot_row_id,
                0
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
                .checkpoint_now(&engine.trx_sys)
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

            let mut session = engine.try_new_session().unwrap();
            assert_eq!(
                checkpointed_table
                    .total_row_pages(session.pool_guards())
                    .await,
                0
            );
            assert!(
                replay_only_table
                    .total_row_pages(session.pool_guards())
                    .await
                    > 0
            );

            let mut trx = session.try_begin_trx().unwrap().unwrap();

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

            let mut session = engine.try_new_session().unwrap();
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
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let insert = trx_insert_row(
                &mut trx,
                &table,
                vec![Val::from(7u32), Val::from("persisted-row")],
            )
            .await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await.unwrap();
            let mut checkpoint_session = engine.try_new_session().unwrap();
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

            let table_file_path = engine.table_fs.user_table_file_path(table_id);
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
            let mut session = engine.try_new_session().unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
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

            let mut session = engine.try_new_session().unwrap();
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
                .checkpoint_now(&engine.trx_sys)
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..80u32 {
                let insert = trx_insert_row(&mut trx, &table, vec![Val::from(i)]).await;
                assert!(insert.is_ok());
            }
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await.unwrap();
            let mut checkpoint_session = engine.try_new_session().unwrap();
            checkpoint_published(&table, &mut checkpoint_session).await;

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..64u32 {
                let key = SelectKey::new(0, vec![Val::from(i)]);
                let delete = trx_delete_row(&mut trx, &table, &key).await;
                assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            }
            trx.commit().await.unwrap();

            let marker = table.deletion_buffer().get(0).unwrap();
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
                smol::Timer::after(std::time::Duration::from_millis(20)).await;
            }
            assert!(
                ready,
                "deletion marker ts {} not yet below checkpoint cutoff",
                marker_ts
            );

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let insert = trx_insert_row(&mut trx, &table, vec![Val::from(1000u32)]).await;
            assert!(insert.is_ok());
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await.unwrap();
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
                    .locate_block(0)
                    .await
                    .unwrap()
                    .expect("checkpointed table should keep the deleted row's block entry");
                entry
                    .deletion_blob_ref()
                    .expect("delete checkpoint should offload large delete sets")
            };

            let table_file_path = engine.table_fs.user_table_file_path(table_id);
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
            let session = engine.try_new_session().unwrap();
            let active_root = table.file().active_root_unchecked();
            let index = ColumnBlockIndex::new(
                active_root.column_block_index_root,
                active_root.pivot_row_id,
                table.file().file_kind(),
                table.file().sparse_file(),
                table.disk_pool(),
                session.pool_guards().disk_guard(),
            );
            let entry = index
                .locate_block(0)
                .await
                .unwrap()
                .expect("deleted row should still have a checkpoint entry");
            let err = match load_entry_deletion_deltas(&index, &entry).await {
                Ok(_) => panic!("expected invalid delete blob on delta load"),
                Err(err) => err,
            };
            assert_table_data_integrity(
                err,
                "column-deletion-blob",
                blob_ref.start_block_id,
                DataIntegrityError::InvalidPayload,
            );

            drop(table);
            drop(session);
            drop(engine);
        })
    }
}
