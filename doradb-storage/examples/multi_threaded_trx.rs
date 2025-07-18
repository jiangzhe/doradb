//! Multi-threaded transaction processing.
//! This example runs empty transactions via multiple threads.
//! Its goal is to testing system bottleneck on starting and committing transactions.
use byte_unit::{Byte, ParseError};
use clap::Parser;
use crossbeam_utils::sync::WaitGroup;
use doradb_storage::buffer::EvictableBufferPoolConfig;
use doradb_storage::engine::{Engine, EngineConfig};
use doradb_storage::trx::log::LogSync;
use doradb_storage::trx::sys_conf::TrxSysConfig;
use easy_parallel::Parallel;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

fn main() {
    smol::block_on(async {
        let args = Args::parse();

        let engine = EngineConfig::default()
            .meta_buffer(64usize * 1024 * 1024)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .max_mem_size(2usize * 1024 * 1024 * 1024)
                    .max_file_size(3usize * 1024 * 1024 * 1024)
                    .file_path("databuffer_bench2.bin"),
            )
            .trx(
                TrxSysConfig::default()
                    .log_file_prefix(args.log_file_prefix.to_string())
                    .log_partitions(args.log_partitions)
                    .io_depth_per_log(args.io_depth_per_log)
                    .log_file_max_size(args.log_file_max_size)
                    .log_sync(args.log_sync)
                    .max_io_size(args.max_io_size)
                    .skip_recovery(true),
            )
            .build()
            .unwrap()
            .init()
            .await
            .unwrap();
        {
            let start = Instant::now();
            let wg = WaitGroup::new();
            let stop = Arc::new(AtomicBool::new(false));
            let ex = smol::Executor::new();
            let (notify, shutdown) = flume::unbounded::<()>();
            // start transaction sessions.
            for _ in 0..args.sessions {
                let wg = wg.clone();
                let stop = Arc::clone(&stop);
                let engine = engine.weak();
                ex.spawn(worker(engine, stop, wg)).detach();
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
            let io_submit_count = stats.io_submit_count;
            let io_submit_nanos = stats.io_submit_nanos;
            let io_wait_count = stats.io_wait_count;
            let io_wait_nanos = stats.io_wait_nanos;
            println!(
            "threads={},dur={},total_trx={},groups={},sync={},sync_dur={:.2}us,io_submit={},io_submit_dur={:.2}us,io_wait={},io_wait_dur={:.2}us,trx/grp={:.2},trx/s={:.0},log/s={:.2}MB",
            args.threads,
            dur.as_micros(),
            total_trx_count,
            commit_count,
            sync_count,
            if sync_count == 0 { 0f64 } else { sync_nanos as f64 / 1000f64 / sync_count as f64 },
            io_submit_count,
            if io_submit_count == 0 { 0f64 } else { io_submit_nanos as f64 / 1000f64 / io_submit_count as f64 },
            io_wait_count,
            if io_wait_count == 0 { 0f64 } else { io_wait_nanos as f64 / 1000f64 / io_wait_count as f64 },
            if commit_count == 0 { 0f64 } else { total_trx_count as f64 / commit_count as f64 },
            total_trx_count as f64 * 1_000_000_000f64 / dur.as_nanos() as f64,
            log_bytes as f64 / dur.as_micros() as f64,
        );
        }
        drop(engine);

        let _ = std::fs::remove_file("databuffer_bench3.bin");
    })
}

#[inline]
async fn worker(engine: Engine, stop: Arc<AtomicBool>, wg: WaitGroup) {
    let mut session = engine.new_session();
    let stop = &*stop;
    while !stop.load(Ordering::Relaxed) {
        let mut trx = session.begin_trx();
        trx.add_pseudo_redo_log_entry();
        match trx.commit().await {
            Ok(s) => session = s,
            Err(_) => return,
        }
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

    #[arg(long, default_value = "8KiB", value_parser = parse_byte_size)]
    max_io_size: usize,

    #[arg(long, default_value = "32")]
    io_depth_per_log: usize,

    /// whether to enable GC
    #[arg(long)]
    gc_enabled: bool,
}

#[inline]
fn parse_byte_size(input: &str) -> Result<usize, ParseError> {
    Byte::parse_str(input, true).map(|b| b.as_u64() as usize)
}
