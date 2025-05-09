//! Multi-threaded transaction processing.
//! This example runs empty transactions via multiple threads.
//! Its goal is to testing system bottleneck on starting and committing transactions.
use byte_unit::{Byte, ParseError};
use clap::Parser;
use crossbeam_utils::sync::WaitGroup;
use doradb_catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, SchemaID, TableSpec,
};
use doradb_datatype::{Collation, PreciseType};
use doradb_storage::buffer::BufferPool;
use doradb_storage::engine::Engine;
use doradb_storage::lifetime::StaticLifetime;
use doradb_storage::table::TableID;
use doradb_storage::trx::log::LogSync;
use doradb_storage::trx::sys::TrxSysConfig;
use doradb_storage::value::Val;
use easy_parallel::Parallel;
use semistr::SemiStr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static ALLOCATOR: Jemalloc = Jemalloc;

fn main() {
    smol::block_on(async {
        let args = Args::parse();

        let engine = Engine::new_fixed(
            args.buffer_pool_size,
            TrxSysConfig::default()
                .log_file_prefix(args.log_file_prefix.to_string())
                .log_partitions(args.log_partitions)
                .io_depth_per_log(args.io_depth_per_log)
                .log_file_max_size(args.log_file_max_size)
                .log_sync(args.log_sync)
                .max_io_size(args.max_io_size)
                .purge_threads(args.purge_threads),
        )
        .await
        .unwrap();
        println!("buffer pool size is {}", engine.buf_pool.size());

        let table_id = sbtest(engine).await;
        // start benchmark
        {
            let start = Instant::now();
            let wg = WaitGroup::new();
            let stop = Arc::new(AtomicBool::new(false));
            let ex = smol::Executor::new();
            let (notify, shutdown) = flume::unbounded::<()>();
            // start transaction sessions.
            for sess_id in 0..args.sessions {
                let wg = wg.clone();
                let stop = Arc::clone(&stop);
                ex.spawn(worker(
                    engine,
                    table_id,
                    sess_id as i32,
                    args.sessions as i32,
                    stop,
                    wg,
                ))
                .detach();
            }
            // start system threads.
            let _ = Parallel::new()
                .each(0..args.threads, |_| {
                    smol::block_on(ex.run(shutdown.recv_async()))
                })
                .finish({
                    let stop = Arc::clone(&stop);
                    move || {
                        std::thread::sleep(args.duration);
                        stop.store(true, Ordering::SeqCst);
                        wg.wait();
                        drop(notify)
                    }
                });
            let dur = start.elapsed();
            let stats = engine.trx_sys.trx_sys_stats();
            let total_trx_count = stats.trx_count;
            let commit_count = stats.commit_count;
            let log_bytes = stats.log_bytes;
            let sync_count = stats.sync_count;
            let sync_nanos = stats.sync_nanos;
            let sync_latency = if sync_count == 0 {
                0f64
            } else {
                sync_nanos as f64 / 1000f64 / sync_count as f64
            };
            let io_submit_count = stats.io_submit_count;
            let io_submit_nanos = stats.io_submit_nanos;
            let io_submit_latency = if io_submit_count == 0 {
                0f64
            } else {
                io_submit_nanos as f64 / 1000f64 / io_submit_count as f64
            };
            let io_wait_count = stats.io_wait_count;
            let io_wait_nanos = stats.io_wait_nanos;
            let io_wait_latency = if io_wait_count == 0 {
                0f64
            } else {
                io_wait_nanos as f64 / 1000f64 / io_wait_count as f64
            };
            let trx_per_group = if commit_count == 0 {
                0f64
            } else {
                total_trx_count as f64 / commit_count as f64
            };
            let tps = total_trx_count as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
            println!(
                "threads={},dur={},total_trx={},groups={},sync={},sync_dur={:.2}us,\
                io_submit={},io_submit_dur={:.2}us,io_wait={},io_wait_dur={:.2}us,\
                trx/grp={:.2},trx/s={:.0},log/s={:.2}MB,purge_trx={},purge_row={},purge_index={}",
                args.threads,
                dur.as_micros(),
                total_trx_count,
                commit_count,
                sync_count,
                sync_latency,
                io_submit_count,
                io_submit_latency,
                io_wait_count,
                io_wait_latency,
                trx_per_group,
                tps,
                log_bytes as f64 / dur.as_micros() as f64,
                stats.purge_trx_count,
                stats.purge_row_count,
                stats.purge_index_count,
            );
        }
        unsafe {
            StaticLifetime::drop_static(engine);
        }
    })
}

#[inline]
async fn worker<P: BufferPool>(
    engine: &'static Engine<P>,
    table_id: TableID,
    id_start: i32,
    id_step: i32,
    stop: Arc<AtomicBool>,
    wg: WaitGroup,
) {
    let table = engine.catalog().get_table(table_id).unwrap();
    let mut session = engine.new_session();
    let stop = &*stop;
    let mut id = id_start;
    let mut c = [0u8; 120];
    let mut pad = [0u8; 60];
    while !stop.load(Ordering::Relaxed) {
        let k = fastrand::i32(0..1024 * 1024);
        c.iter_mut().for_each(|b| {
            *b = fastrand::alphabetic() as u8;
        });
        pad.iter_mut().for_each(|b| {
            *b = fastrand::alphabetic() as u8;
        });
        let mut trx = session.begin_trx();
        let mut stmt = trx.start_stmt();
        let res = stmt
            .insert_row(
                &table,
                vec![
                    Val::from(id),
                    Val::from(k),
                    Val::from(&c[..]),
                    Val::from(&pad[..]),
                ],
            )
            .await;
        assert!(res.is_ok());
        trx = stmt.succeed();
        match trx.commit().await {
            Ok(s) => session = s,
            Err(_) => return,
        }
        id += id_step;
    }
    drop(wg);
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// thread number to run transactions
    #[arg(long, default_value = "1")]
    threads: usize,

    #[arg(long, default_value = "1")]
    sessions: usize,

    /// Number of transactions at least one thread should complete
    #[arg(long, default_value = "10s", value_parser = humantime::parse_duration)]
    duration: Duration,

    /// path of redo log file
    #[arg(long, default_value = "redo.log")]
    log_file_prefix: String,

    #[arg(long, default_value = "1")]
    log_partitions: usize,

    #[arg(long, default_value = "fsync", value_parser = LogSync::from_str)]
    log_sync: LogSync,

    /// size of log file
    #[arg(long, default_value = "1GiB", value_parser = parse_byte_size)]
    log_file_max_size: usize,

    #[arg(long, default_value = "8192", value_parser = parse_byte_size)]
    max_io_size: usize,

    #[arg(long, default_value = "32")]
    io_depth_per_log: usize,

    #[arg(long, default_value = "2GiB", value_parser = parse_byte_size)]
    buffer_pool_size: usize,

    /// whether to enable GC
    #[arg(long, action = clap::ArgAction::Set, default_value = "true", value_parser = clap::builder::BoolishValueParser::new())]
    gc_enabled: bool,

    #[arg(long, default_value = "1")]
    purge_threads: usize,
}

#[inline]
fn parse_byte_size(input: &str) -> Result<usize, ParseError> {
    Byte::parse_str(input, true).map(|b| b.as_u64() as usize)
}

#[inline]
pub(crate) async fn db1<P: BufferPool>(engine: &'static Engine<P>) -> SchemaID {
    let session = engine.new_session();
    let trx = session.begin_trx();
    let mut stmt = trx.start_stmt();

    let schema_id = stmt.create_schema("db1").await.unwrap();

    let trx = stmt.succeed();
    let session = trx.commit().await.unwrap();
    drop(session);
    schema_id
}

/// Sbtest is target table of sysbench.
#[inline]
pub async fn sbtest<P: BufferPool>(engine: &'static Engine<P>) -> TableID {
    let schema_id = db1(engine).await;

    let session = engine.new_session();
    let trx = session.begin_trx();
    let mut stmt = trx.start_stmt();

    let table_id = stmt
        .create_table(
            schema_id,
            TableSpec {
                table_name: SemiStr::new("sbtest"),
                columns: vec![
                    ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    ColumnSpec {
                        column_name: SemiStr::new("k"),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    ColumnSpec {
                        column_name: SemiStr::new("c"),
                        column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                        column_attributes: ColumnAttributes::empty(),
                    },
                    ColumnSpec {
                        column_name: SemiStr::new("pad"),
                        column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
            },
            vec![
                IndexSpec::new("idx_sbtest_id", vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new(
                    "idx_sbtest_k",
                    vec![IndexKey::new(1)],
                    IndexAttributes::empty(),
                ),
            ],
        )
        .await
        .unwrap();

    let trx = stmt.succeed();
    let session = trx.commit().await.unwrap();
    drop(session);
    table_id
}
