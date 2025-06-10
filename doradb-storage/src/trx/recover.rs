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
use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::latch::LatchFallbackMode;
use crate::row::{RowID, RowPage};
use crate::serde::LenPrefixPod;
use crate::table::{Table, TableID};
use crate::trx::log::{LogMerger, LogPartition, LogPartitionInitializer, LogPartitionStream};
use crate::trx::purge::GC;
use crate::trx::redo::{DDLRedo, RedoHeader, RedoLogs, RowRedo, RowRedoKind, TableDML};
use crate::trx::TrxID;
use crossbeam_utils::CachePadded;
use flume::Receiver;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub struct RecoverMap(Vec<Option<TrxID>>);

impl RecoverMap {
    /// Returns an empty recover map.
    #[inline]
    pub fn empty() -> Self {
        RecoverMap(vec![])
    }

    /// Returns whether entry of given row position is vacant.
    #[inline]
    pub fn is_vacant(&self, row_idx: usize) -> bool {
        row_idx >= self.0.len() || self.0[row_idx].is_none()
    }

    /// Insert CTS at given row position.
    #[inline]
    pub fn insert_at(&mut self, row_idx: usize, cts: TrxID) {
        while self.0.len() <= row_idx {
            self.0.push(None);
        }
        self.0[row_idx] = Some(cts);
    }

    /// Update CTS at given row position.
    #[inline]
    pub fn update_at(&mut self, row_idx: usize, cts: TrxID) {
        debug_assert!(row_idx < self.0.len());
        debug_assert!(self.at(row_idx).unwrap() <= cts);
        self.0[row_idx].replace(cts);
    }

    /// Returns CTS at given row position.
    #[inline]
    pub fn at(&self, row_idx: usize) -> Option<TrxID> {
        self.0.get(row_idx).and_then(|v| v.clone())
    }
}

pub(super) async fn log_recover<P: BufferPool>(
    buf_pool: &'static P,
    catalog: &mut Catalog<P>,
    mut log_partition_initializers: Vec<LogPartitionInitializer>,
    skip: bool,
) -> Result<(Vec<CachePadded<LogPartition<P>>>, Vec<Receiver<GC<P>>>)> {
    // In recovery, we disable GC and redo logging.
    // All data are purely processed in memory and if
    // any failure occurs, we abort the whole process.
    if !skip {
        let log_partitions = log_partition_initializers.len();
        let mut log_merger = LogMerger::new();
        for initializer in log_partition_initializers {
            let stream = initializer.stream();
            log_merger.add_stream(stream)?;
        }
        let log_recovery = LogRecovery::new(buf_pool, catalog, log_merger);
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
    buf_pool: &'static P,
    catalog: &'a mut Catalog<P>,
    log_merger: LogMerger,
    recovered_tables: HashMap<TableID, BTreeSet<PageID>>,
}

impl<'a, P: BufferPool> LogRecovery<'a, P> {
    #[inline]
    pub fn new(buf_pool: &'static P, catalog: &'a mut Catalog<P>, log_merger: LogMerger) -> Self {
        LogRecovery {
            buf_pool,
            catalog,
            log_merger,
            recovered_tables: HashMap::new(),
        }
    }

    #[inline]
    pub async fn recover_all(mut self) -> Result<Vec<LogPartitionStream>> {
        // 1. replay all DDLs and DMLs.
        while let Some(log) = self.log_merger.next()? {
            self.replay_log(log).await?;
        }
        // 2. Rebuild all indexes and refresh pages to enable undo map.
        self.recover_indexes_and_refresh_pages().await;

        Ok(self.log_merger.finished_streams())
    }

    async fn replay_log(&mut self, log: LenPrefixPod<RedoHeader, RedoLogs>) -> Result<()> {
        // sequentially replay redo log.
        let RedoLogs { ddl, dml } = log.payload;

        if let Some(ddl) = ddl {
            // Execute DDL after all previous DML is done.
            // We treat every DDL as pipeline breaker.
            self.wait_for_dml_done().await?;
            self.replay_ddl(ddl, dml, log.header.cts).await?;
        } else {
            // replay DML-only transaction.
            // todo: dispatch DML execution to multiple threads.
            self.dispatch_dml(dml, log.header.cts).await?;
        }
        Ok(())
    }

    async fn recover_indexes_and_refresh_pages(&mut self) {
        // recover index with all data.
        // todo: integrate with checkpoint in future.
        for (table_id, pages) in &self.recovered_tables {
            if let Some(table) = self.catalog.get_table(*table_id) {
                for page_id in pages {
                    table
                        .populate_index_via_row_page(self.buf_pool, *page_id)
                        .await;
                    self.refresh_page(*page_id).await;
                }
            }
        }
    }

    async fn refresh_page(&self, page_id: PageID) {
        let mut page_guard = self
            .buf_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
            .await
            .exclusive_async()
            .await;

        let max_row_count = page_guard.page().header.max_row_count as usize;
        page_guard.bf_mut().init_undo_map(max_row_count);
    }

    async fn replay_ddl(
        &mut self,
        ddl: Box<DDLRedo>,
        dml: BTreeMap<TableID, TableDML>,
        cts: TrxID,
    ) -> Result<()> {
        match &*ddl {
            DDLRedo::CreateSchema(schema_id) => {
                self.replay_dml(dml, cts, false).await?;
                self.catalog
                    .reload_schema(self.buf_pool, *schema_id, true)
                    .await?;
            }
            DDLRedo::DropSchema(schema_id) => {
                self.replay_dml(dml, cts, false).await?;
                self.catalog
                    .reload_schema(self.buf_pool, *schema_id, false)
                    .await?;
            }
            DDLRedo::CreateTable(table_id) => {
                self.replay_dml(dml, cts, false).await?;
                self.catalog
                    .reload_create_table(self.buf_pool, *table_id)
                    .await?;
            }
            DDLRedo::CreateRowPage {
                table_id,
                page_id,
                start_row_id,
                end_row_id,
            } => {
                // Row page creation is guaranteed to be ordered in the redo log,
                // so its safe to recreate it and the row id range must be identical.
                let table = self
                    .catalog
                    .get_table(*table_id)
                    .ok_or(Error::TableNotFound)?;
                let count = end_row_id - start_row_id;
                let mut page_guard = table
                    .blk_idx
                    .allocate_row_page_at(self.buf_pool, count as usize, &table.metadata, *page_id)
                    .await;
                // Here we switch row page to recover mode.
                page_guard.bf_mut().init_recover_map();

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
                .ok_or_else(|| Error::TableNotFound)?;
            self.replay_table_dml(&table, &table_dml.rows, cts, disable_index)
                .await?;
        }
        Ok(())
    }

    async fn replay_table_dml(
        &mut self,
        table: &Table<P>,
        rows: &BTreeMap<RowID, RowRedo>,
        cts: TrxID,
        disable_index: bool,
    ) -> Result<()> {
        for row in rows.values() {
            match &row.kind {
                RowRedoKind::Insert(vals) => {
                    table
                        .recover_row_insert(
                            self.buf_pool,
                            row.page_id,
                            row.row_id,
                            vals,
                            cts,
                            disable_index,
                        )
                        .await;
                }
                RowRedoKind::Update(vals) => {
                    table
                        .recover_row_update(
                            self.buf_pool,
                            row.page_id,
                            row.row_id,
                            vals,
                            cts,
                            disable_index,
                        )
                        .await;
                }
                RowRedoKind::Delete => {
                    table
                        .recover_row_delete(
                            self.buf_pool,
                            row.page_id,
                            row.row_id,
                            cts,
                            disable_index,
                        )
                        .await;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::EvictableBufferPoolConfig;
    use crate::engine::EngineConfig;
    use crate::trx::sys_conf::TrxSysConfig;
    use crate::trx::tests::remove_files;
    use doradb_catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
    };
    use doradb_datatype::PreciseType;

    #[test]
    fn test_log_recover_empty() {
        smol::block_on(async {
            let engine = EngineConfig::default()
                .buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(16usize * 1024 * 1024)
                        .max_file_size(32usize * 1024 * 1024)
                        .file_path("databuffer_recover.bin"),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("recover1")
                        .skip_recovery(false),
                )
                .build()
                .unwrap()
                .init()
                .await
                .unwrap();

            drop(engine);

            let _ = std::fs::remove_file("databuffer_recover.bin");
            remove_files("recover1*");
        })
    }

    #[test]
    fn test_log_recover_ddl() {
        smol::block_on(async {
            remove_files("recover2*");
            let engine = EngineConfig::default()
                .buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(16usize * 1024 * 1024)
                        .max_file_size(32usize * 1024 * 1024)
                        .file_path("databuffer_recover.bin"),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("recover2")
                        .skip_recovery(false),
                )
                .build()
                .unwrap()
                .init()
                .await
                .unwrap();

            let mut session = engine.new_session();
            let mut trx = session.begin_trx();
            let mut stmt = trx.start_stmt();
            let schema_id = stmt.create_schema("db1").await.unwrap();
            trx = stmt.succeed();
            session = trx.commit().await.unwrap();

            let mut trx = session.begin_trx();
            let mut stmt = trx.start_stmt();
            let table_spec = TableSpec::new(
                "t1",
                vec![
                    ColumnSpec::new("c0", PreciseType::u32(), ColumnAttributes::empty()),
                    ColumnSpec::new("c1", PreciseType::u64(), ColumnAttributes::empty()),
                ],
            );

            let table_id = stmt
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
            trx = stmt.succeed();
            session = trx.commit().await.unwrap();

            drop(session);
            drop(engine);

            // second recovery.
            let engine = EngineConfig::default()
                .buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(16usize * 1024 * 1024)
                        .max_file_size(32usize * 1024 * 1024)
                        .file_path("databuffer_recover.bin"),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("recover2")
                        .skip_recovery(false),
                )
                .build()
                .unwrap()
                .init()
                .await
                .unwrap();

            assert!(engine.catalog().get_table(table_id).is_some());

            drop(engine);
            let _ = std::fs::remove_file("databuffer_recover.bin");
            remove_files("recover2*");
        })
    }
}
