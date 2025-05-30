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
use crate::buffer::BufferPool;
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::row::RowID;
use crate::serde::LenPrefixPod;
use crate::table::{Table, TableID};
use crate::trx::log::{LogMerger, LogPartition, LogPartitionInitializer};
use crate::trx::purge::GC;
use crate::trx::redo::{DDLRedo, RowRedo, TableDML};
use crate::trx::redo::{RedoHeader, RedoLogs};
use crossbeam_utils::CachePadded;
use flume::Receiver;
use std::collections::BTreeMap;

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
        while let Some(log) = log_merger.next()? {
            // replay_log(buf_pool, catalog, log).await?;
            // println!("log={:?}", log);
        }
        // after all logs replayed, we setup new log files
        log_partition_initializers = log_merger
            .finished_streams()
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

pub async fn replay_log<P: BufferPool>(
    buf_pool: &'static P,
    catalog: &mut Catalog<P>,
    log: LenPrefixPod<RedoHeader, RedoLogs>,
) -> Result<()> {
    // sequentially replay redo log.
    let RedoLogs { ddl, dml } = log.payload;

    if let Some(ddl) = ddl {
        match &*ddl {
            DDLRedo::CreateSchema(schema_id) => {
                replay_dml(buf_pool, catalog, &dml).await?;
                catalog.reload_schema(buf_pool, *schema_id, true).await?;
            }
            DDLRedo::DropSchema(schema_id) => {
                replay_dml(buf_pool, catalog, &dml).await?;
                catalog.reload_schema(buf_pool, *schema_id, false).await?;
            }
            DDLRedo::CreateTable(table_id) => {
                replay_dml(buf_pool, catalog, &dml).await?;
                catalog.reload_table(buf_pool, *table_id, true).await?;
            }
            _ => todo!(),
        }
    } else {
        // replay DML-only transaction.
        replay_dml(buf_pool, catalog, &dml).await?;
    }
    Ok(())
}

/// Replay DML log.
/// Version chain is not maintained because this is recovery process.
async fn replay_dml<P: BufferPool>(
    buf_pool: &'static P,
    catalog: &mut Catalog<P>,
    dml: &BTreeMap<TableID, TableDML>,
) -> Result<()> {
    for (table_id, table_dml) in dml {
        let table = catalog
            .get_table(*table_id)
            .ok_or_else(|| Error::TableNotFound)?;
        replay_table_dml(buf_pool, catalog, &table, &table_dml.rows).await?;
    }
    Ok(())
}

pub async fn replay_table_dml<P: BufferPool>(
    buf_pool: &'static P,
    catalog: &mut Catalog<P>,
    table_id: &Table<P>,
    rows: &BTreeMap<RowID, RowRedo>,
) -> Result<()> {
    // todo
    Ok(())
}

#[cfg(test)]
mod tests {}
