//! Multi-threaded transaction processing.
//! This example runs empty transactions via multiple threads.
//! Its goal is to testing system bottleneck on starting and committing transactions.
use clap::Parser;
use crossbeam_utils::sync::WaitGroup;
use doradb_storage::prelude::*;
use doradb_storage::trx::redo::{RedoBin, RedoLogger};
use easy_parallel::Parallel;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

fn main() {
    let args = Args::parse();

    let trx_sys = TransactionSystem::new_static();
    if args.gc_enabled {
        trx_sys.start_gc_thread();
    }
    if let Some(log_file) = &args.log_file {
        let logger = NormalRedoLogger::new(log_file, args.buf_size);
        trx_sys.set_redo_logger(Box::new(logger));
    }
    {
        let start = Instant::now();
        let wg = WaitGroup::new();
        let total_trx_count = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let ex = smol::Executor::new();
        let (notify, shutdown) = flume::unbounded::<()>();
        // start transaction sessions.
        for _ in 0..args.sessions {
            let wg = wg.clone();
            let stop = Arc::clone(&stop);
            let total_trx_count = Arc::clone(&total_trx_count);
            ex.spawn(worker(trx_sys, total_trx_count, stop, wg))
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
        let stats = trx_sys.group_commit_stats_v4();
        println!(
            "threads={:?},total_trx={:?},dur={},trx/s={:.3},groups={:?},trx/grp={:.3},log/s={:.3}MB",
            args.threads,
            total_trx_count.load(Ordering::Relaxed),
            dur.as_micros(),
            total_trx_count.load(Ordering::Relaxed) as f64 * 1_000_000_000f64 / dur.as_nanos() as f64,
            stats.commit_count,
            if stats.commit_count == 0 { 0f64 } else { stats.trx_count as f64 / stats.commit_count as f64 },
            stats.log_bytes as f64 / dur.as_micros() as f64,
        );
    }
    unsafe {
        TransactionSystem::drop_static(trx_sys);
    }
}

#[inline]
async fn worker(
    trx_sys: &TransactionSystem,
    total_count: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    wg: WaitGroup,
) {
    let mut count = 0;
    let stop = &*stop;
    while !stop.load(Ordering::Relaxed) {
        let mut trx = trx_sys.new_trx();
        trx.add_pseudo_redo_log_entry();
        trx_sys.commit_v4(trx).await;
        count += 1;
    }
    total_count.fetch_add(count, Ordering::AcqRel);
    drop(wg);
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// thread number to run transactions
    #[arg(short, long, default_value = "4")]
    threads: usize,

    #[arg(short, long, default_value = "4")]
    sessions: usize,

    /// Number of transactions at least one thread should complete
    #[arg(short, long, default_value = "10s", value_parser = humantime::parse_duration)]
    duration: Duration,

    /// path of redo log file
    #[arg(short, long, default_value = "redo.log")]
    log_file: Option<String>,

    /// size of redo log buffer
    #[arg(short, long, default_value = "4096")]
    buf_size: usize,

    /// whether to enable GC
    #[arg(short, long)]
    gc_enabled: bool,
}

struct CtsOnlyRedoLogger {
    writer: BufWriter<File>,
}

impl CtsOnlyRedoLogger {
    #[inline]
    fn new(log_file: &str, buf_size: usize) -> Self {
        let f = File::create(log_file).expect("fail to create log file");
        let writer = BufWriter::with_capacity(buf_size, f);
        CtsOnlyRedoLogger { writer }
    }
}

impl RedoLogger for CtsOnlyRedoLogger {
    #[inline]
    fn write(&mut self, cts: TrxID, _redo_bin: RedoBin) -> usize {
        let s = format!("{}\n", cts);
        self.writer.write_all(s.as_bytes()).unwrap();
        s.len()
    }

    #[inline]
    fn flush(&mut self) {
        self.writer.flush().unwrap();
    }
}

struct NormalRedoLogger {
    writer: BufWriter<File>,
}

impl NormalRedoLogger {
    #[inline]
    fn new(log_file: &str, buf_size: usize) -> Self {
        let f = File::create(log_file).expect("fail to create log file");
        let writer = BufWriter::with_capacity(buf_size, f);
        NormalRedoLogger { writer }
    }
}

impl RedoLogger for NormalRedoLogger {
    #[inline]
    fn write(&mut self, _cts: TrxID, redo_bin: RedoBin) -> usize {
        self.writer.write_all(&redo_bin).unwrap();
        redo_bin.len()
    }

    #[inline]
    fn flush(&mut self) {
        self.writer.flush().unwrap();
    }
}