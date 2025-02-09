//! Multi-threaded transaction processing.
//! This example runs empty transactions via multiple threads.
//! Its goal is to testing system bottleneck on starting and committing transactions.
use byte_unit::{Byte, ParseError};
use clap::Parser;
use crossbeam_utils::sync::WaitGroup;
use doradb_storage::buffer::FixedBufferPool;
use doradb_storage::catalog::{Catalog, IndexKey, IndexSchema, TableSchema};
use doradb_storage::lifetime::StaticLifetime;
use doradb_storage::session::Session;
use doradb_storage::table::TableID;
use doradb_storage::trx::log::LogSync;
use doradb_storage::trx::sys::{TransactionSystem, TrxSysConfig};
use doradb_storage::value::{Val, ValKind};
use easy_parallel::Parallel;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

fn main() {
    let args = Args::parse();

    let buf_pool = FixedBufferPool::with_capacity_static(args.buffer_pool_size).unwrap();
    println!("buffer pool size is {}", buf_pool.size());
    let catalog = Catalog::empty_static();
    let trx_sys = TrxSysConfig::default()
        .log_file_prefix(args.log_file_prefix.to_string())
        .log_partitions(args.log_partitions)
        .io_depth_per_log(args.io_depth_per_log)
        .log_file_max_size(args.log_file_max_size)
        .log_sync(args.log_sync)
        .log_drop(args.log_drop)
        .max_io_size(args.max_io_size)
        .gc(args.gc_enabled)
        .purge_threads(args.purge_threads)
        .build_static(buf_pool, catalog);
    // create empty table
    let table_id = catalog.create_table(
        buf_pool,
        TableSchema::new(
            vec![
                ValKind::I32.nullable(false),
                ValKind::I32.nullable(false),
                ValKind::VarByte.nullable(false),
                ValKind::VarByte.nullable(false),
            ],
            vec![IndexSchema::new(vec![IndexKey::new(0)], true)],
        ),
    );
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
                buf_pool,
                trx_sys,
                catalog,
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
        let stats = trx_sys.trx_sys_stats();
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
        StaticLifetime::drop_static(trx_sys);
        StaticLifetime::drop_static(catalog);
        StaticLifetime::drop_static(buf_pool);
    }
}

#[inline]
async fn worker(
    buf_pool: &FixedBufferPool,
    trx_sys: &TransactionSystem,
    catalog: &'static Catalog<FixedBufferPool>,
    table_id: TableID,
    id_start: i32,
    id_step: i32,
    stop: Arc<AtomicBool>,
    wg: WaitGroup,
) {
    let table = catalog.get_table(table_id).unwrap();
    let mut session = Session::new();
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
        let mut trx = session.begin_trx(trx_sys);
        let mut stmt = trx.start_stmt();
        let res = table
            .insert_row(
                buf_pool,
                &mut stmt,
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
        match trx_sys.commit(trx, buf_pool, &catalog).await {
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

    #[arg(long, default_value = "false")]
    log_drop: bool,

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
