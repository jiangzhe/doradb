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
    Catalog, CatalogTable, TableID, TableMetadata, is_catalog_obj_id, is_user_obj_id,
};
use crate::error::{Error, Result};
use crate::file::fs::FileSystem;
use crate::latch::LatchFallbackMode;
use crate::quiescent::QuiescentGuard;
use crate::row::{RowID, RowPage};
use crate::table::{Table, TableRecover};
use crate::trx::log::{LogPartition, LogPartitionInitializer};
use crate::trx::log_replay::{LogMerger, LogPartitionStream, TrxLog};
use crate::trx::purge::GC;
use crate::trx::redo::{DDLRedo, RedoLogs, RowRedo, RowRedoKind, TableDML};
use crate::trx::{MAX_SNAPSHOT_TS, MIN_SNAPSHOT_TS, TrxID};
use crossbeam_utils::CachePadded;
use flume::Receiver;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    skip: bool,
) -> Result<(Vec<CachePadded<LogPartition>>, Vec<Receiver<GC>>, TrxID)> {
    let RecoveryDeps {
        index_pool,
        mem_pool,
        table_fs,
        disk_pool,
    } = deps;
    // In recovery, we disable GC and redo logging.
    // All data are purely processed in memory and if
    // any failure occurs, we abort the whole process.
    let mut next_trx_ts = MIN_SNAPSHOT_TS;
    if !skip {
        let log_partitions = log_partition_initializers.len();
        let mut log_merger = LogMerger::default();
        for initializer in log_partition_initializers {
            let stream = initializer.stream();
            log_merger.add_stream(stream)?;
        }
        let log_recovery = LogRecovery::new(
            meta_pool, index_pool, mem_pool, table_fs, disk_pool, catalog, log_merger,
        );
        let (log_streams, max_recovered_cts) = log_recovery.recover_all().await?;
        next_trx_ts = max_recovered_cts
            .checked_add(1)
            .filter(|ts| *ts < MAX_SNAPSHOT_TS)
            .ok_or(Error::InvalidState)?;
        log_partition_initializers = log_streams
            .into_iter()
            .map(|s| s.into_initializer())
            .collect();
        log_partition_initializers.sort_by_key(|i| i.log_no);
        debug_assert_eq!(log_partition_initializers.len(), log_partitions);
    }
    let mut partitions = vec![];
    let mut gc_rxs = vec![];
    for initializer in log_partition_initializers {
        let (partition, gc_rx) = initializer.finish()?;
        partitions.push(CachePadded::new(partition));
        gc_rxs.push(gc_rx);
    }
    Ok((partitions, gc_rxs, next_trx_ts))
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
    /// Hot row pages touched by redo replay, grouped by table for post-replay
    /// index rebuild and undo-map refresh.
    recovered_tables: HashMap<TableID, BTreeSet<PageID>>,
    /// Stable pool guards shared by recovery operations.
    pool_guards: PoolGuards,
}

#[derive(Clone, Copy, Debug)]
struct RecoveryTableState {
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
            recovered_tables: HashMap::new(),
            pool_guards,
        }
    }

    /// Replay all redo streams, rebuild indexes, and return reopened partition streams.
    #[inline]
    pub async fn recover_all(mut self) -> Result<(Vec<LogPartitionStream>, TrxID)> {
        self.bootstrap_checkpointed_user_tables().await?;
        // 1. replay all DDLs and DMLs.
        while let Some(log) = self.log_merger.try_next()? {
            self.replay_log(log).await?;
        }
        // 2. Rebuild all indexes and refresh pages to enable undo map.
        self.recover_indexes_and_refresh_pages().await?;

        Ok((self.log_merger.finished_streams(), self.max_recovered_cts))
    }

    async fn bootstrap_checkpointed_user_tables(&mut self) -> Result<()> {
        let snapshot = self.catalog.storage.checkpoint_snapshot()?;
        self.catalog_replay_start_ts = snapshot.catalog_replay_start_ts;
        self.replay_floor = snapshot.catalog_replay_start_ts;
        self.max_recovered_cts = self.max_recovered_cts.max(snapshot.catalog_replay_start_ts);

        for table in self
            .catalog
            .storage
            .tables()
            .list_uncommitted(&self.pool_guards)
            .await
        {
            if !is_user_obj_id(table.table_id) {
                continue;
            }
            self.catalog
                .reload_create_table(
                    self.mem_pool.clone(),
                    self.index_pool.clone(),
                    &self.table_fs,
                    self.disk_pool.clone(),
                    &self.pool_guards,
                    table.table_id,
                )
                .await?;
            self.track_loaded_table(table.table_id).await?;
            let state = self
                .table_states
                .get(&table.table_id)
                .ok_or(Error::TableNotFound)?;
            self.replay_floor = self.replay_floor.min(state.replay_start_ts());
        }
        Ok(())
    }

    #[inline]
    fn should_replay_catalog(&self, cts: TrxID) -> bool {
        cts >= self.catalog_replay_start_ts
    }

    async fn track_loaded_table(&mut self, table_id: TableID) -> Result<()> {
        let table = self
            .catalog
            .get_table(table_id)
            .await
            .ok_or(Error::TableNotFound)?;
        let state = RecoveryTableState {
            heap_redo_start_ts: table.file().active_root().heap_redo_start_ts,
            deletion_cutoff_ts: table.file().active_root().deletion_cutoff_ts,
        };
        self.max_recovered_cts = self
            .max_recovered_cts
            .max(state.heap_redo_start_ts)
            .max(state.deletion_cutoff_ts);
        let old = self.table_states.insert(table_id, state);
        if old.is_some() {
            return Err(Error::TableAlreadyExists);
        }
        Ok(())
    }

    #[inline]
    fn table_heap_redo_start_ts(&self, table_id: TableID) -> Result<TrxID> {
        self.table_states
            .get(&table_id)
            .map(|state| state.heap_redo_start_ts)
            .ok_or(Error::TableNotFound)
    }

    #[inline]
    fn table_deletion_cutoff_ts(&self, table_id: TableID) -> Result<TrxID> {
        self.table_states
            .get(&table_id)
            .map(|state| state.deletion_cutoff_ts)
            .ok_or(Error::TableNotFound)
    }

    #[inline]
    fn table_replay_start_ts(&self, table_id: TableID) -> Result<TrxID> {
        self.table_states
            .get(&table_id)
            .map(|state| state.replay_start_ts())
            .ok_or(Error::TableNotFound)
    }

    async fn replay_log(&mut self, log: TrxLog) -> Result<()> {
        // sequentially replay redo log.
        let (header, RedoLogs { ddl, dml }) = log.into_inner();
        self.max_recovered_cts = self.max_recovered_cts.max(header.cts);
        if header.cts < self.replay_floor {
            return Ok(());
        }

        if let Some(ddl) = ddl {
            // Execute DDL after all previous DML is done.
            // We treat every DDL as pipeline breaker.
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
                let metadata = Arc::new(table.metadata().clone());
                for page_id in pages {
                    table
                        .populate_index_via_row_page(&self.pool_guards, *page_id)
                        .await?;
                    self.refresh_page(Arc::clone(&metadata), *page_id).await?;
                }
            }
        }
        Ok(())
    }

    async fn refresh_page(&self, metadata: Arc<TableMetadata>, page_id: PageID) -> Result<()> {
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
        page_guard.bf_mut().init_undo_map(metadata, max_row_count);
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
                self.catalog
                    .reload_create_table(
                        self.mem_pool.clone(),
                        self.index_pool.clone(),
                        &self.table_fs,
                        self.disk_pool.clone(),
                        &self.pool_guards,
                        *table_id,
                    )
                    .await?;
                self.track_loaded_table(*table_id).await?;
            }
            DDLRedo::DropTable(table_id) => {
                if !self.should_replay_catalog(cts) {
                    return Ok(());
                }
                self.replay_catalog_modifications(dml).await?;
                if self.catalog.remove_user_table(*table_id).is_none() {
                    return Err(Error::TableNotFound);
                }
                self.table_states.remove(table_id);
                self.recovered_tables.remove(table_id);
            }
            DDLRedo::CreateRowPage {
                table_id,
                page_id,
                start_row_id,
                end_row_id,
            } => {
                debug_assert!(dml.is_empty());
                if cts < self.table_heap_redo_start_ts(*table_id)? {
                    return Ok(());
                }
                // Row page creation is guaranteed to be ordered in the redo log,
                // so its safe to recreate it and the row id range must be identical.
                let table = self
                    .catalog
                    .get_table(*table_id)
                    .await
                    .ok_or(Error::TableNotFound)?;
                let count = end_row_id - start_row_id;
                let mut page_guard = table
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
                if cts < self.table_heap_redo_start_ts(*table_id)? {
                    return Ok(());
                }
                let _ = self
                    .catalog
                    .get_table(*table_id)
                    .await
                    .ok_or(Error::TableNotFound)?;
            }
            _ => todo!(),
        }
        Ok(())
    }

    async fn dispatch_dml(&mut self, dml: BTreeMap<TableID, TableDML>, cts: TrxID) -> Result<()> {
        self.replay_dml(dml, cts, true).await
    }

    async fn wait_for_dml_done(&mut self) -> Result<()> {
        // todo: add synchronization if dispatch recover tasks to multiple threads.
        Ok(())
    }

    /// Replay DML log.
    /// Version chain is not maintained because this is recovery process.
    async fn replay_dml(
        &mut self,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
        disable_index: bool,
    ) -> Result<()> {
        for (table_id, table_dml) in dml {
            if is_catalog_obj_id(table_id) {
                if !self.should_replay_catalog(cts) {
                    continue;
                }
                let table = self
                    .catalog
                    .get_catalog_table(table_id)
                    .ok_or(Error::TableNotFound)?;
                self.replay_catalog_table_modifications(&table, &table_dml.rows)
                    .await?;
                continue;
            }
            if cts < self.table_replay_start_ts(table_id)? {
                continue;
            }
            let table = self
                .catalog
                .get_table(table_id)
                .await
                .ok_or(Error::TableNotFound)?;
            self.replay_table_dml(table_id, &table, &table_dml.rows, cts, disable_index)
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
                .catalog
                .get_catalog_table(table_id)
                .ok_or(Error::TableNotFound)?;
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
        disable_index: bool,
    ) -> Result<()> {
        let heap_redo_start_ts = self.table_heap_redo_start_ts(table_id)?;
        let deletion_cutoff_ts = self.table_deletion_cutoff_ts(table_id)?;
        let pivot_row_id = table.file().active_root().pivot_row_id;
        for row in rows.values() {
            match &row.kind {
                RowRedoKind::Insert(vals) => {
                    if cts < heap_redo_start_ts {
                        continue;
                    }
                    table
                        .recover_row_insert(
                            &self.pool_guards,
                            row.page_id,
                            row.row_id,
                            vals,
                            cts,
                            disable_index,
                        )
                        .await?;
                }
                RowRedoKind::Update(vals) => {
                    if cts < heap_redo_start_ts {
                        continue;
                    }
                    table
                        .recover_row_update(
                            &self.pool_guards,
                            row.page_id,
                            row.row_id,
                            vals,
                            cts,
                            disable_index,
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
                            &self.pool_guards,
                            row.page_id,
                            row.row_id,
                            cts,
                            disable_index,
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

#[cfg(test)]
mod tests {
    use super::{RecoverMap, RecoveryTableState};
    use crate::buffer::PoolRole;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexOrder, IndexSpec,
        TableMetadata, TableSpec,
    };
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind};
    use crate::file::block_integrity::{BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum};
    use crate::file::cow_file::COW_FILE_PAGE_SIZE;
    use crate::index::{
        COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE, ColumnBlockIndex, UniqueIndex,
        load_entry_deletion_deltas,
    };
    use crate::row::RowRead;
    use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
    use crate::table::{DeleteMarker, TableAccess, TablePersistence};
    use crate::trx::MIN_SNAPSHOT_TS;
    use crate::value::Val;
    use crate::value::ValKind;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::TempDir;

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
            heap_redo_start_ts: 7,
            deletion_cutoff_ts: 11,
        };
        assert_eq!(heap_first.replay_start_ts(), 7);

        let deletion_first = RecoveryTableState {
            heap_redo_start_ts: 19,
            deletion_cutoff_ts: 13,
        };
        assert_eq!(deletion_first.replay_start_ts(), 13);
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover1")
                        .skip_recovery(false),
                )
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover2")
                        .skip_recovery(false),
                )
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
                IndexSpec::new("idx_t1_pk", vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new(
                    "idx_t1_c1_c2",
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover2")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            assert!(engine.catalog().get_table(table_id).await.is_some());
            let table = engine.catalog().get_table(table_id).await.unwrap();
            assert_eq!(table.metadata(), &expected_metadata);

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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover3")
                        .skip_recovery(false),
                )
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
                    vec![IndexSpec::new(
                        "idx_t1_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
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
                    let mut stmt = trx.start_stmt();
                    let res = stmt
                        .insert_row(&table, vec![Val::from(j as u32), Val::from(&s[..])])
                        .await;
                    assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
                    trx = stmt.succeed();
                }
                trx.commit().await.unwrap();
            }
            // update
            let s2: String = std::iter::repeat_n('2', 100).collect();
            for i in (0..DML_SIZE).step_by(UPD_STEP) {
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(i as u32)]);
                let uc = UpdateCol {
                    idx: 1,
                    val: Val::from(&s2[..]),
                };
                let res = stmt.update_row(&table, &key, vec![uc]).await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                stmt.succeed().commit().await.unwrap();
            }
            // delete
            for i in (0..DML_SIZE).step_by(DEL_STEP) {
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(i as u32)]);
                let res = stmt.delete_row(&table, &key).await;
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
                stmt.succeed().commit().await.unwrap();
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover3")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let session = engine.try_new_session().unwrap();
            let mut rows = 0usize;
            table
                .accessor()
                .table_scan_uncommitted(session.pool_guards(), |_metadata, row| {
                    assert!(row.row_id() as usize <= DML_SIZE);
                    rows += if row.is_deleted() { 0 } else { 1 };
                    true
                })
                .await;
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover4")
                        .skip_recovery(false),
                )
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
                    vec![IndexSpec::new(
                        "idx_t4_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover4")
                        .skip_recovery(false),
                )
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover5")
                        .skip_recovery(false),
                )
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
                    vec![IndexSpec::new(
                        "idx_t5_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
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
            let mut stmt = trx.start_stmt();
            let insert = stmt
                .insert_row(&table, vec![Val::from(7u32), Val::from("cold-row")])
                .await;
            let cold_row_id = match insert {
                Ok(InsertMvcc::Inserted(row_id)) => row_id,
                other => panic!("expected cold insert success, got {other:?}"),
            };
            trx = stmt.succeed();
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await;
            let mut checkpoint_session = engine.try_new_session().unwrap();
            table.checkpoint(&mut checkpoint_session).await.unwrap();
            let root_after_checkpoint = table.file().active_root();
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover5")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.try_new_session().unwrap();
            assert_eq!(table.total_row_pages(session.pool_guards()).await, 0);

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(7u32)]);
            let index = table.sec_idx()[key.index_no].unique().unwrap();
            let disk = index
                .disk()
                .open_unique(session.pool_guards().disk_guard())
                .unwrap();
            assert_eq!(disk.lookup(&key.vals).await.unwrap(), Some(cold_row_id));
            assert_eq!(
                index
                    .lookup(
                        session.pool_guards().index_guard(),
                        &key.vals,
                        MIN_SNAPSHOT_TS
                    )
                    .await
                    .unwrap(),
                Some((cold_row_id, false))
            );
            let row = stmt.select_row_mvcc(&table, &key, &[0, 1]).await;
            assert_eq!(
                row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("cold-row")]
            );
            stmt.succeed().commit().await.unwrap();

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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover11")
                        .skip_recovery(false),
                )
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
                    vec![IndexSpec::new(
                        "idx_t11_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
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
            let mut stmt = trx.start_stmt();
            let insert = stmt
                .insert_row(&table, vec![Val::from(7u32), Val::from("cold-row")])
                .await;
            let Ok(InsertMvcc::Inserted(cold_row_id)) = insert else {
                panic!("cold insert should succeed");
            };
            trx = stmt.succeed();
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await;
            let mut checkpoint_session = engine.try_new_session().unwrap();
            table.checkpoint(&mut checkpoint_session).await.unwrap();
            assert!(table.file().active_root().pivot_row_id > cold_row_id);

            let key = SelectKey::new(0, vec![Val::from(7u32)]);
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let delete = stmt.delete_row(&table, &key).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx = stmt.succeed();
            trx.commit().await.unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let insert = stmt
                .insert_row(&table, vec![Val::from(7u32), Val::from("hot-row")])
                .await;
            let Ok(InsertMvcc::Inserted(hot_row_id)) = insert else {
                panic!("hot insert should reclaim deleted cold key");
            };
            assert_ne!(cold_row_id, hot_row_id);
            trx = stmt.succeed();
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover11")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.try_new_session().unwrap();
            assert!(table.total_row_pages(session.pool_guards()).await > 0);

            let index = table.sec_idx()[key.index_no].unique().unwrap();
            let disk = index
                .disk()
                .open_unique(session.pool_guards().disk_guard())
                .unwrap();
            assert_eq!(disk.lookup(&key.vals).await.unwrap(), Some(cold_row_id));
            assert_eq!(
                index
                    .lookup(
                        session.pool_guards().index_guard(),
                        &key.vals,
                        MIN_SNAPSHOT_TS
                    )
                    .await
                    .unwrap(),
                Some((hot_row_id, false))
            );

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let row = stmt.select_row_mvcc(&table, &key, &[0, 1]).await;
            assert_eq!(
                row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("hot-row")]
            );
            stmt.succeed().commit().await.unwrap();

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
            let engine = EngineConfig::default()
                .storage_root(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover12")
                        .skip_recovery(false),
                )
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
                        IndexSpec::new("idx_t12_pk", vec![IndexKey::new(0)], IndexAttributes::PK),
                        IndexSpec::new(
                            "idx_t12_name",
                            vec![IndexKey::new(1)],
                            IndexAttributes::empty(),
                        ),
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
                let mut stmt = trx.start_stmt();
                let insert = stmt
                    .insert_row(&table, vec![Val::from(id), Val::from("same-name")])
                    .await;
                let Ok(InsertMvcc::Inserted(row_id)) = insert else {
                    panic!("same-name insert should succeed");
                };
                same_row_ids.push(row_id);
                trx = stmt.succeed();
            }
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await;
            let mut checkpoint_session = engine.try_new_session().unwrap();
            table.checkpoint(&mut checkpoint_session).await.unwrap();
            assert!(
                same_row_ids
                    .iter()
                    .all(|row_id| *row_id < table.file().active_root().pivot_row_id)
            );

            let delete_key = SelectKey::new(0, vec![Val::from(2u32)]);
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let delete = stmt.delete_row(&table, &delete_key).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx = stmt.succeed();
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover12")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.try_new_session().unwrap();
            assert_eq!(table.total_row_pages(session.pool_guards()).await, 0);

            let name_key = SelectKey::new(1, vec![Val::from("same-name")]);
            let non_unique = table.sec_idx()[name_key.index_no].non_unique().unwrap();
            let disk = non_unique
                .disk()
                .open_non_unique(session.pool_guards().disk_guard())
                .unwrap();
            let disk_rows = disk
                .prefix_scan_entries(&name_key.vals)
                .await
                .unwrap()
                .into_iter()
                .map(|(_, row_id)| row_id)
                .collect::<Vec<_>>();
            assert_eq!(disk_rows, same_row_ids);
            match table.deletion_buffer().get(same_row_ids[1]).unwrap() {
                DeleteMarker::Committed(_) => (),
                DeleteMarker::Ref(_) => panic!("recovered cold delete should be committed"),
            }

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let rows = table
                .accessor()
                .index_scan_mvcc(&stmt, &name_key, &[0, 1])
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
            let deleted = stmt.select_row_mvcc(&table, &delete_key, &[0, 1]).await;
            assert!(matches!(deleted, Ok(SelectMvcc::NotFound)));
            stmt.succeed().commit().await.unwrap();

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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover6")
                        .skip_recovery(false),
                )
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
                    vec![IndexSpec::new(
                        "idx_t6_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
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
            let mut stmt = trx.start_stmt();
            let insert = stmt
                .insert_row(&table, vec![Val::from(7u32), Val::from("cold-row")])
                .await;
            assert!(matches!(insert, Ok(InsertMvcc::Inserted(_))));
            trx = stmt.succeed();
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await;
            let mut checkpoint_session = engine.try_new_session().unwrap();
            table.checkpoint(&mut checkpoint_session).await.unwrap();
            let root_after_checkpoint = table.file().active_root();
            assert!(root_after_checkpoint.heap_redo_start_ts > catalog_replay_start_ts);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let insert = stmt
                .insert_row(&table, vec![Val::from(8u32), Val::from("hot-row")])
                .await;
            assert!(matches!(insert, Ok(InsertMvcc::Inserted(_))));
            trx = stmt.succeed();
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover6")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.try_new_session().unwrap();
            assert!(table.total_row_pages(session.pool_guards()).await > 0);

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();

            let cold_key = SelectKey::new(0, vec![Val::from(7u32)]);
            let cold_row = stmt.select_row_mvcc(&table, &cold_key, &[0, 1]).await;
            assert_eq!(
                cold_row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("cold-row")]
            );

            let hot_key = SelectKey::new(0, vec![Val::from(8u32)]);
            let hot_row = stmt.select_row_mvcc(&table, &hot_key, &[0, 1]).await;
            assert_eq!(
                hot_row.unwrap().unwrap_found(),
                vec![Val::from(8u32), Val::from("hot-row")]
            );

            stmt.succeed().commit().await.unwrap();

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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover10")
                        .skip_recovery(false),
                )
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
                    vec![IndexSpec::new(
                        "idx_t10_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
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
                let mut stmt = trx.start_stmt();
                let insert = stmt.insert_row(&table, vec![Val::from(i)]).await;
                assert!(matches!(insert, Ok(InsertMvcc::Inserted(_))));
                trx = stmt.succeed();
            }
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await;
            let mut checkpoint_session = engine.try_new_session().unwrap();
            table.checkpoint(&mut checkpoint_session).await.unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let key0 = SelectKey::new(0, vec![Val::from(0u32)]);
            let mut stmt = trx.start_stmt();
            let delete = stmt.delete_row(&table, &key0).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx = stmt.succeed();
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
            table.checkpoint(&mut checkpoint_session).await.unwrap();
            let checkpointed_cutoff = table.file().active_root().deletion_cutoff_ts;
            assert!(checkpointed_cutoff > marker0_ts);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let key1 = SelectKey::new(0, vec![Val::from(1u32)]);
            let mut stmt = trx.start_stmt();
            let delete = stmt.delete_row(&table, &key1).await;
            assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
            trx = stmt.succeed();
            trx.commit().await.unwrap();
            let marker1_ts = match table.deletion_buffer().get(1).unwrap() {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            assert!(marker1_ts >= checkpointed_cutoff);

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let insert = stmt.insert_row(&table, vec![Val::from(100u32)]).await;
            assert!(matches!(insert, Ok(InsertMvcc::Inserted(_))));
            trx = stmt.succeed();
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover10")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            assert_eq!(
                table.file().active_root().deletion_cutoff_ts,
                checkpointed_cutoff
            );
            assert!(table.deletion_buffer().get(0).is_none());
            match table.deletion_buffer().get(1).unwrap() {
                DeleteMarker::Committed(ts) => assert_eq!(ts, marker1_ts),
                DeleteMarker::Ref(_) => panic!("recovered cold delete should be committed"),
            }

            let mut session = engine.try_new_session().unwrap();
            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();

            let row0 = stmt
                .select_row_mvcc(&table, &SelectKey::new(0, vec![Val::from(0u32)]), &[0])
                .await;
            assert!(matches!(row0, Ok(SelectMvcc::NotFound)));

            let row1 = stmt
                .select_row_mvcc(&table, &SelectKey::new(0, vec![Val::from(1u32)]), &[0])
                .await;
            assert!(matches!(row1, Ok(SelectMvcc::NotFound)));

            let row100 = stmt
                .select_row_mvcc(&table, &SelectKey::new(0, vec![Val::from(100u32)]), &[0])
                .await;
            assert_eq!(row100.unwrap().unwrap_found(), vec![Val::from(100u32)]);

            stmt.succeed().commit().await.unwrap();
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover7")
                        .skip_recovery(false),
                )
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
                    vec![IndexSpec::new(
                        "idx_t7a_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
                )
                .await
                .unwrap();
            let replay_only_table_id = session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::U32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    vec![IndexSpec::new(
                        "idx_t7b_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
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
            let mut stmt = trx.start_stmt();
            let insert = stmt
                .insert_row(
                    &checkpointed_table,
                    vec![Val::from(7u32), Val::from("persisted-row")],
                )
                .await;
            assert!(matches!(insert, Ok(InsertMvcc::Inserted(_))));
            trx = stmt.succeed();
            trx.commit().await.unwrap();

            checkpointed_table.freeze(&session, usize::MAX).await;
            let mut checkpoint_session = engine.try_new_session().unwrap();
            checkpointed_table
                .checkpoint(&mut checkpoint_session)
                .await
                .unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let insert = stmt
                .insert_row(
                    &replay_only_table,
                    vec![Val::from(8u32), Val::from("replayed-row")],
                )
                .await;
            assert!(matches!(insert, Ok(InsertMvcc::Inserted(_))));
            trx = stmt.succeed();
            trx.commit().await.unwrap();

            assert!(checkpointed_table.file().active_root().pivot_row_id > 0);
            assert_eq!(replay_only_table.file().active_root().pivot_row_id, 0);
            assert!(
                checkpointed_table.file().active_root().heap_redo_start_ts
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover7")
                        .skip_recovery(false),
                )
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

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();

            let checkpointed_key = SelectKey::new(0, vec![Val::from(7u32)]);
            let checkpointed_row = stmt
                .select_row_mvcc(&checkpointed_table, &checkpointed_key, &[0, 1])
                .await;
            assert_eq!(
                checkpointed_row.unwrap().unwrap_found(),
                vec![Val::from(7u32), Val::from("persisted-row")]
            );

            let replay_only_key = SelectKey::new(0, vec![Val::from(8u32)]);
            let replay_only_row = stmt
                .select_row_mvcc(&replay_only_table, &replay_only_key, &[0, 1])
                .await;
            assert_eq!(
                replay_only_row.unwrap().unwrap_found(),
                vec![Val::from(8u32), Val::from("replayed-row")]
            );

            stmt.succeed().commit().await.unwrap();

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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover8")
                        .skip_recovery(false),
                )
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
                    vec![IndexSpec::new(
                        "idx_t8_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
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
            let mut stmt = trx.start_stmt();
            let insert = stmt
                .insert_row(&table, vec![Val::from(7u32), Val::from("persisted-row")])
                .await;
            assert!(matches!(insert, Ok(InsertMvcc::Inserted(_))));
            trx = stmt.succeed();
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await;
            let mut checkpoint_session = engine.try_new_session().unwrap();
            table.checkpoint(&mut checkpoint_session).await.unwrap();

            let active_root = table.file().active_root();
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

            let table_file_path = engine.table_fs.table_file_path(table_id);
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover8")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut session = engine.try_new_session().unwrap();
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(7u32)]);
            let res = stmt.select_row_mvcc(&table, &key, &[0, 1]).await;
            match res {
                Err(Error::BlockCorrupted {
                    file_kind: FileKind::TableFile,
                    block_kind: BlockKind::LwcBlock,
                    block_id: page_id,
                    cause: BlockCorruptionCause::ChecksumMismatch,
                }) => assert_eq!(page_id, block_id),
                other => panic!("expected persisted LWC corruption on read, got {other:?}"),
            }
            trx = stmt.fail().await.unwrap();
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover9")
                        .skip_recovery(false),
                )
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
                    vec![IndexSpec::new(
                        "idx_t9_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
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
                let mut stmt = trx.start_stmt();
                let insert = stmt.insert_row(&table, vec![Val::from(i)]).await;
                assert!(matches!(insert, Ok(InsertMvcc::Inserted(_))));
                trx = stmt.succeed();
            }
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await;
            let mut checkpoint_session = engine.try_new_session().unwrap();
            table.checkpoint(&mut checkpoint_session).await.unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..64u32 {
                let key = SelectKey::new(0, vec![Val::from(i)]);
                let mut stmt = trx.start_stmt();
                let delete = stmt.delete_row(&table, &key).await;
                assert!(matches!(delete, Ok(DeleteMvcc::Deleted)));
                trx = stmt.succeed();
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
            let mut stmt = trx.start_stmt();
            let insert = stmt.insert_row(&table, vec![Val::from(1000u32)]).await;
            assert!(matches!(insert, Ok(InsertMvcc::Inserted(_))));
            trx = stmt.succeed();
            trx.commit().await.unwrap();

            table.freeze(&session, usize::MAX).await;
            table.checkpoint(&mut checkpoint_session).await.unwrap();

            let active_root = table.file().active_root();
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

            let table_file_path = engine.table_fs.table_file_path(table_id);
            drop(trx_sys);
            drop(checkpoint_session);
            drop(table);
            drop(session);
            drop(engine);

            corrupt_blob_header_kind(
                table_file_path,
                blob_ref.start_page_id,
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
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("recover9")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let session = engine.try_new_session().unwrap();
            let active_root = table.file().active_root();
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
            match err {
                Error::BlockCorrupted {
                    file_kind: FileKind::TableFile,
                    block_kind: BlockKind::ColumnDeletionBlob,
                    block_id: page_id,
                    cause: BlockCorruptionCause::InvalidPayload,
                } => assert_eq!(page_id, blob_ref.start_page_id),
                other => panic!("expected invalid delete blob on delta load, got {other:?}"),
            }

            drop(table);
            drop(session);
            drop(engine);
        })
    }
}
