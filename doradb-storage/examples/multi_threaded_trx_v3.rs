//! Multi-threaded transaction processing.
//! This example runs empty transactions via multiple threads.
//! Its goal is to testing system bottleneck on starting and committing transactions.
use clap::Parser;
use doradb_storage::prelude::*;
use doradb_storage::trx::redo::RedoBin;
use doradb_storage::trx::sys_v3::{RedoLogger, TransactionSystem};
use parking_lot::Mutex;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

fn main() {
    let args = Args::parse();

    let mut loggers = vec![];
    for i in 0..args.partitions {
        let log_file = format!("{}.{}", args.log_file_base, i);
        let logger = NormalRedoLogger::new(&log_file, args.buf_size);
        loggers.push(Mutex::new(Box::new(logger) as Box<dyn RedoLogger>));
    }

    let trx_sys = TransactionSystem::new_static(args.partitions, loggers);
    if args.gc_enabled {
        trx_sys.start_gc_thread_v3();
    }
    {
        let start = Instant::now();
        let total_trx_count = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = vec![];
        for _ in 0..args.threads {
            let stop = Arc::clone(&stop);
            let total_trx_count = Arc::clone(&total_trx_count);
            let handle = std::thread::spawn(|| {
                worker(trx_sys, total_trx_count, stop);
            });
            handles.push(handle);
        }
        std::thread::sleep(args.duration);
        stop.store(true, Ordering::SeqCst);
        for h in handles {
            h.join().unwrap();
        }
        let dur = start.elapsed();
        let stats = trx_sys.group_commit_stats_v3();
        let commit_count = stats.iter().map(|g| g.commit_count).sum::<usize>();
        let trx_count = stats.iter().map(|g| g.trx_count).sum::<usize>();
        let log_bytes = stats.iter().map(|g| g.log_bytes).sum::<usize>();
        let total_trx_count = total_trx_count.load(Ordering::Relaxed);
        println!(
            "threads={:?},dur={},total_trx={:?},groups={:?},trx/grp={:.3},trx/s={:.0},log/s={:.2}MB",
            args.threads,
            dur.as_micros(),
            total_trx_count,
            commit_count,
            if commit_count == 0 { 0f64 } else { trx_count as f64 / commit_count as f64 },
            total_trx_count as f64 * 1_000_000_000f64 / dur.as_nanos() as f64,
            log_bytes as f64 / dur.as_micros() as f64,
        );
    }
    unsafe {
        TransactionSystem::drop_static(trx_sys);
    }
}

#[inline]
fn worker(trx_sys: &TransactionSystem, total_count: Arc<AtomicUsize>, stop: Arc<AtomicBool>) {
    let mut count = 0;
    let stop = &*stop;
    while !stop.load(Ordering::Relaxed) {
        let mut trx = trx_sys.new_trx();
        trx.add_pseudo_redo_log_entry();
        trx_sys.commit_v3(trx);
        count += 1;
    }
    total_count.fetch_add(count, Ordering::AcqRel);
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// thread number to run transactions
    #[arg(short, long, default_value = "4")]
    threads: usize,

    #[arg(short, long, default_value = "1")]
    partitions: usize,

    /// Number of transactions at least one thread should complete
    #[arg(short, long, default_value = "10s", value_parser = humantime::parse_duration)]
    duration: Duration,

    /// path of redo log file
    #[arg(short, long, default_value = "redo.log")]
    log_file_base: String,

    /// size of redo log buffer
    #[arg(short, long, default_value = "4096")]
    buf_size: usize,

    /// whether to enable GC
    #[arg(short, long)]
    gc_enabled: bool,
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
        self.writer.get_mut().sync_all().unwrap();
    }
}
