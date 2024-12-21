//! Multi-threaded transaction processing.
//! This example runs empty transactions via multiple threads.
//! Its goal is to testing system bottleneck on starting and committing transactions.
use clap::Parser;
use crossbeam_utils::sync::WaitGroup;
use doradb_storage::prelude::*;
use doradb_storage::trx::redo::RedoBin;
use doradb_storage::trx::sys_v6::{RedoLogger, RedoSyncer, TransactionSystem};
use easy_parallel::Parallel;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

fn main() {
    let args = Args::parse();

    let trx_sys = TransactionSystem::new_static(args.log_len_threshold);
    if args.gc_enabled {
        trx_sys.start_gc_thread();
    }
    if let Some(log_file) = &args.log_file {
        let f = File::create(log_file).expect("fail to create log file");
        let fd = f.as_raw_fd();
        let logger = NormalRedoLogger::new(f, args.log_len_threshold * 2);
        let syncer = NormalRedoSyncer(fd);
        trx_sys.set_logger_and_syncer(Box::new(logger), Box::new(syncer));
    }
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
            ex.spawn(worker(trx_sys, stop, wg)).detach();
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
        let stats = trx_sys.group_commit_stats_v6();
        let total_trx_count = stats.trx_count;
        println!(
            "threads={:?},dur={},total_trx={:?},groups={:?},trx/grp={:.3},trx/s={:.0},log/s={:.2}MB",
            args.threads,
            dur.as_micros(),
            total_trx_count,
            stats.commit_count,
            if stats.commit_count == 0 { 0f64 } else { total_trx_count as f64 / stats.commit_count as f64 },
            total_trx_count as f64 * 1_000_000_000f64 / dur.as_nanos() as f64,
            stats.log_bytes as f64 / dur.as_micros() as f64,
        );
    }
    unsafe {
        TransactionSystem::drop_static(trx_sys);
    }
}

#[inline]
async fn worker(trx_sys: &TransactionSystem, stop: Arc<AtomicBool>, wg: WaitGroup) {
    let stop = &*stop;
    while !stop.load(Ordering::Relaxed) {
        let mut trx = trx_sys.new_trx();
        trx.add_pseudo_redo_log_entry();
        trx_sys.commit_v6(trx).await;
    }
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
    log_len_threshold: usize,

    /// whether to enable GC
    #[arg(short, long)]
    gc_enabled: bool,
}

struct NormalRedoLogger {
    writer: BufWriter<File>,
}

impl NormalRedoLogger {
    #[inline]
    fn new(f: File, buf_size: usize) -> Self {
        let writer = BufWriter::with_capacity(buf_size, f);
        NormalRedoLogger { writer }
    }
}

impl RedoLogger for NormalRedoLogger {
    #[inline]
    fn write(&mut self, _cts: TrxID, redo_bin: &RedoBin) -> usize {
        self.writer.write_all(&redo_bin).unwrap();
        redo_bin.len()
    }

    #[inline]
    fn flush(&mut self) {
        self.writer.flush().unwrap();
    }
}

struct NormalRedoSyncer(RawFd);

impl RedoSyncer for NormalRedoSyncer {
    #[inline]
    fn sync(&mut self) {
        unsafe {
            libc::fsync(self.0);
        }
    }
}
