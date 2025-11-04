use clap::Parser;
use doradb_storage::io::{AIOManager, AIOManagerConfig, DirectBuf};
use rand::RngCore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    let args = Args::parse();
    let stop = Arc::new(AtomicBool::new(false));
    let aio_mgr = AIOManagerConfig::default()
        .max_events(args.io_depth)
        .build_static()
        .unwrap();
    let mut handles = vec![];
    let start = Instant::now();
    for id in 0..args.log_partitions {
        let args = args.clone();
        let stop = Arc::clone(&stop);
        let handle = thread::spawn(move || worker(id, &aio_mgr, args, stop));
        handles.push(handle);
    }

    thread::sleep(args.duration);
    stop.store(true, Ordering::SeqCst);

    let mut log_bytes = 0;
    for h in handles {
        log_bytes += h.join().unwrap();
    }
    let dur = start.elapsed();
    println!(
        "partitions={}, dur={}ms, log={:.3}MB/s",
        args.log_partitions,
        dur.as_millis(),
        log_bytes as f64 / dur.as_micros() as f64
    );
}

fn worker(id: usize, aio_mgr: &'static AIOManager, args: Args, stop: Arc<AtomicBool>) -> usize {
    let file_name = format!("{}.{}", &args.log_file_prefix, id);

    let file = aio_mgr
        .create_sparse_file(&file_name, args.log_file_max_size)
        .unwrap();
    let syncer = file.syncer();

    let log_io_depth = args.io_depth / args.log_partitions;

    let mut thd_rng = rand::rng();

    let mut id = 0;
    let mut inflight = HashMap::new();
    let mut reqs = vec![];
    let mut events = aio_mgr.events();
    let mut log_bytes = 0;
    while !stop.load(Ordering::Relaxed) {
        let batch_size = (thd_rng.next_u32() as usize % log_io_depth) + 1;
        for _ in 0..batch_size {
            id += 1;
            let buf = DirectBuf::uninit(args.max_io_size);
            let (offset, _) = file.alloc(buf.capacity()).unwrap();
            let aio = file.pwrite_direct(id, offset, buf);
            reqs.push(aio.iocb().load(Ordering::Relaxed));
            inflight.insert(aio.key, aio);
        }
        aio_mgr.submit(&mut reqs);
        let finish_count = aio_mgr.wait_at_least(&mut events, 1, |key, res| {
            assert!(res.is_ok());
            log_bytes += res.unwrap();
            inflight.remove(&key);
        });
        if args.sync != 0 && finish_count >= args.sync {
            syncer.fdatasync();
        }
    }
    aio_mgr.forget_sparse_file(file);
    log_bytes
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// path of redo log file
    #[arg(long, default_value = "redo.log")]
    log_file_prefix: String,

    #[arg(long, default_value = "1")]
    log_partitions: usize,

    /// size of log file
    #[arg(long, default_value = "2147483648")]
    log_file_max_size: usize,

    #[arg(long, default_value = "8192")]
    max_io_size: usize,

    #[arg(long, default_value = "32")]
    io_depth: usize,

    #[arg(long, default_value = "10s", value_parser = humantime::parse_duration)]
    duration: Duration,

    #[arg(long, default_value = "0")]
    sync: usize,
}
