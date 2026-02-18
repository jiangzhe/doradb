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
use crate::buffer::guard::PageGuard;
use crate::buffer::page::PageID;
use crate::buffer::{BufferPool, FixedBufferPool};
use crate::catalog::{Catalog, TableID, TableMetadata};
use crate::error::{Error, Result};
use crate::file::table_fs::TableFileSystem;
use crate::latch::LatchFallbackMode;
use crate::row::{RowID, RowPage};
use crate::table::{Table, TableAccess, TableRecover};
use crate::trx::TrxID;
use crate::trx::log::{LogPartition, LogPartitionInitializer};
use crate::trx::log_replay::{LogMerger, LogPartitionStream, TrxLog};
use crate::trx::purge::GC;
use crate::trx::redo::{DDLRedo, RedoLogs, RowRedo, RowRedoKind, TableDML};
use crossbeam_utils::CachePadded;
use flume::Receiver;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

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

#[cfg(test)]
mod basic_tests {
    use super::RecoverMap;

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
}

pub(super) async fn log_recover<P: BufferPool>(
    index_pool: &'static FixedBufferPool,
    data_pool: &'static P,
    table_fs: &'static TableFileSystem,
    catalog: &mut Catalog,
    mut log_partition_initializers: Vec<LogPartitionInitializer>,
    skip: bool,
) -> Result<(Vec<CachePadded<LogPartition>>, Vec<Receiver<GC>>)> {
    // In recovery, we disable GC and redo logging.
    // All data are purely processed in memory and if
    // any failure occurs, we abort the whole process.
    if !skip {
        let log_partitions = log_partition_initializers.len();
        let mut log_merger = LogMerger::default();
        for initializer in log_partition_initializers {
            let stream = initializer.stream();
            log_merger.add_stream(stream)?;
        }
        let log_recovery = LogRecovery::new(index_pool, data_pool, table_fs, catalog, log_merger);
        let log_streams = log_recovery.recover_all().await?;
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
    Ok((partitions, gc_rxs))
}

pub struct LogRecovery<'a, P: BufferPool> {
    index_pool: &'static FixedBufferPool,
    data_pool: &'static P,
    table_fs: &'static TableFileSystem,
    catalog: &'a mut Catalog,
    log_merger: LogMerger,
    recovered_tables: HashMap<TableID, BTreeSet<PageID>>,
}

impl<'a, P: BufferPool> LogRecovery<'a, P> {
    #[inline]
    pub fn new(
        index_pool: &'static FixedBufferPool,
        data_pool: &'static P,
        table_fs: &'static TableFileSystem,
        catalog: &'a mut Catalog,
        log_merger: LogMerger,
    ) -> Self {
        LogRecovery {
            index_pool,
            data_pool,
            table_fs,
            catalog,
            log_merger,
            recovered_tables: HashMap::new(),
        }
    }

    #[inline]
    pub async fn recover_all(mut self) -> Result<Vec<LogPartitionStream>> {
        // 1. replay all DDLs and DMLs.
        while let Some(log) = self.log_merger.try_next()? {
            self.replay_log(log).await?;
        }
        // 2. Rebuild all indexes and refresh pages to enable undo map.
        self.recover_indexes_and_refresh_pages().await;

        Ok(self.log_merger.finished_streams())
    }

    async fn replay_log(&mut self, log: TrxLog) -> Result<()> {
        // sequentially replay redo log.
        let (header, RedoLogs { ddl, dml }) = log.into_inner();

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

    async fn recover_indexes_and_refresh_pages(&mut self) {
        // recover index with all data.
        // todo: integrate with checkpoint in future.
        // Insert part is simple.
        // Update and Delete require a robust consideration.
        for (table_id, pages) in &self.recovered_tables {
            if let Some(table) = self.catalog.get_table(*table_id).await {
                let metadata = &table.file.active_root().metadata;
                for page_id in pages {
                    table.populate_index_via_row_page(*page_id).await;
                    self.refresh_page(Arc::clone(metadata), *page_id).await;
                }
            }
        }
    }

    async fn refresh_page(&self, metadata: Arc<TableMetadata>, page_id: PageID) {
        let mut page_guard = self
            .data_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
            .await
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
    }

    async fn replay_ddl(
        &mut self,
        ddl: Box<DDLRedo>,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> Result<()> {
        match &*ddl {
            DDLRedo::CreateSchema(schema_id) => {
                self.replay_catalog_modifications(dml).await?;
                self.catalog.reload_schema(*schema_id, true).await?;
            }
            DDLRedo::DropSchema(schema_id) => {
                self.replay_catalog_modifications(dml).await?;
                self.catalog.reload_schema(*schema_id, false).await?;
            }
            DDLRedo::CreateTable(table_id) => {
                self.replay_catalog_modifications(dml).await?;
                self.catalog
                    .reload_create_table(self.index_pool, self.table_fs, *table_id)
                    .await?;
            }
            DDLRedo::CreateRowPage {
                table_id,
                page_id,
                start_row_id,
                end_row_id,
            } => {
                debug_assert!(dml.is_empty());
                // Row page creation is guaranteed to be ordered in the redo log,
                // so its safe to recreate it and the row id range must be identical.
                let table = self
                    .catalog
                    .get_table(*table_id)
                    .await
                    .ok_or(Error::TableNotFound)?;
                let count = end_row_id - start_row_id;
                let mut page_guard = table
                    .blk_idx
                    .allocate_row_page_at(self.data_pool, count as usize, *page_id)
                    .await;
                // Here we switch row page to recover mode.
                page_guard.bf_mut().init_recover_map(cts);

                // Record recovered pages so we can recover indexes and refresh undo map at end.
                // Note: we do not need to recover catalog tables because they are specially handled.
                if *table_id as usize >= self.catalog.storage.len() {
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
            DDLRedo::DataCheckpoint { .. } => {
                debug_assert!(dml.is_empty());
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
            let table = self
                .catalog
                .get_table(table_id)
                .await
                .ok_or(Error::TableNotFound)?;
            self.replay_table_dml(&table, &table_dml.rows, cts, disable_index)
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
                .get_table(table_id)
                .await
                .ok_or(Error::TableNotFound)?;
            self.replay_catalog_table_modifications(&table, &table_dml.rows)
                .await;
        }
        Ok(())
    }

    async fn replay_catalog_table_modifications(
        &mut self,
        table: &Table,
        rows: &BTreeMap<RowID, RowRedo>,
    ) {
        for row in rows.values() {
            match &row.kind {
                RowRedoKind::Insert(vals) => {
                    table.insert_no_trx(vals).await;
                }
                RowRedoKind::DeleteByUniqueKey(key) => {
                    table.delete_unique_no_trx(key).await;
                }
                RowRedoKind::Delete | RowRedoKind::Update(_) => {
                    // updates of catalog are implemented as DeleteByUniqueKey and Insert.
                    unreachable!()
                }
            }
        }
    }

    async fn replay_table_dml(
        &mut self,
        table: &Table,
        rows: &BTreeMap<RowID, RowRedo>,
        cts: TrxID,
        disable_index: bool,
    ) -> Result<()> {
        for row in rows.values() {
            match &row.kind {
                RowRedoKind::Insert(vals) => {
                    table
                        .recover_row_insert(row.page_id, row.row_id, vals, cts, disable_index)
                        .await;
                }
                RowRedoKind::Update(vals) => {
                    table
                        .recover_row_update(row.page_id, row.row_id, vals, cts, disable_index)
                        .await;
                }
                RowRedoKind::Delete => {
                    table
                        .recover_row_delete(row.page_id, row.row_id, cts, disable_index)
                        .await;
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
    use crate::buffer::EvictableBufferPoolConfig;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
    };
    use crate::engine::EngineConfig;
    use crate::row::RowRead;
    use crate::row::ops::{SelectKey, UpdateCol};
    use crate::table::TableAccess;
    use crate::trx::sys_conf::TrxSysConfig;
    use crate::value::Val;
    use crate::value::ValKind;
    use tempfile::TempDir;

    #[test]
    fn test_log_recover_empty() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("recover1")
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
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("recover2")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session();
            let schema_id = session.create_schema("db1", false).await.unwrap();

            let table_spec = TableSpec::new(
                "t1",
                vec![
                    ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                    ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::empty()),
                ],
            );

            let table_id = session
                .create_table(
                    schema_id,
                    table_spec,
                    vec![IndexSpec::new(
                        "idx_t1_pk",
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    )],
                )
                .await
                .unwrap();

            drop(session);
            drop(engine);

            // second recovery.
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("recover2")
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
    fn test_log_recover_dml() {
        smol::block_on(async {
            const DML_SIZE: usize = 1000;
            const INS_STEP: usize = 10;
            const UPD_STEP: usize = 11;
            const DEL_STEP: usize = 13;

            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir.clone())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("recover3")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let mut session = engine.new_session();
            let schema_id = session.create_schema("db1", false).await.unwrap();

            let table_spec = TableSpec::new(
                "t1",
                vec![
                    ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                    ColumnSpec::new("c1", ValKind::VarByte, ColumnAttributes::empty()),
                ],
            );

            let table_id = session
                .create_table(
                    schema_id,
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
                let mut trx = session.begin_trx().unwrap();
                for j in i..i + INS_STEP {
                    let mut stmt = trx.start_stmt();
                    let res = stmt
                        .insert_row(&table, vec![Val::from(j as u32), Val::from(&s[..])])
                        .await;
                    assert!(res.is_ok());
                    trx = stmt.succeed();
                }
                trx.commit().await.unwrap();
            }
            // update
            let s2: String = std::iter::repeat_n('2', 100).collect();
            for i in (0..DML_SIZE).step_by(UPD_STEP) {
                let trx = session.begin_trx().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(i as u32)]);
                let uc = UpdateCol {
                    idx: 1,
                    val: Val::from(&s2[..]),
                };
                let res = stmt.update_row(&table, &key, vec![uc]).await;
                assert!(res.is_ok());
                stmt.succeed().commit().await.unwrap();
            }
            // delete
            for i in (0..DML_SIZE).step_by(DEL_STEP) {
                let trx = session.begin_trx().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(i as u32)]);
                let res = stmt.delete_row(&table, &key).await;
                assert!(res.is_ok());
                stmt.succeed().commit().await.unwrap();
            }

            drop(table);
            drop(session);
            drop(engine);

            // second recovery.
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("recover3")
                        .skip_recovery(false),
                )
                .build()
                .await
                .unwrap();

            let table = engine.catalog().get_table(table_id).await.unwrap();
            let mut rows = 0usize;
            table
                .table_scan_uncommitted(0, |_metadata, row| {
                    assert!(row.row_id() as usize <= DML_SIZE);
                    rows += if row.is_deleted() { 0 } else { 1 };
                    true
                })
                .await;
            assert_eq!(rows, DML_SIZE - (DML_SIZE / DEL_STEP + 1));

            drop(engine);
        })
    }
}
