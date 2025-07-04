use byte_unit::{Byte, ParseError};
use clap::Parser;
use doradb_storage::buffer::{BufferPool, FixedBufferPool};
use doradb_storage::index::{BTree, BTreeCompactConfig};
use doradb_storage::lifetime::StaticLifetime;
use perfcnt::linux::{HardwareEventType as Hardware, PerfCounterBuilderLinux as Builder};
use perfcnt::{AbstractPerfCounter, PerfCounter};
use rand::distributions::{Distribution, Uniform};
use std::collections::BTreeMap;

use std::time::Duration;
use std::time::Instant;

fn main() {
    let args = Args::parse();
    smol::block_on(async {
        let args = args.clone();

        single_thread(&args).await;
    });
}

async fn single_thread(args: &Args) {
    single_thread_bench_btree(args).await;

    single_thread_bench_stdmap(args).await;

    single_thread_bench_bplustree(args).await;
}

async fn single_thread_bench_btree(args: &Args) {
    let pool = FixedBufferPool::with_capacity_static(args.mem_size).unwrap();
    {
        let tree = BTree::new(pool, 1).await;
        let mut perf_monitor = PerfMonitor::new();
        perf_monitor.start();

        match &args.mode[..] {
            "seq" => {
                for i in 0..args.total_rows {
                    tree.insert(&i.to_be_bytes(), i, 100).await;
                }
            }
            "rand" => {
                let between = Uniform::from(0..args.total_rows);
                let mut thd_rng = rand::thread_rng();
                for i in 0..args.total_rows {
                    let k = between.sample(&mut thd_rng);
                    tree.insert(&k.to_be_bytes(), i, 100).await;
                }
            }
            _ => panic!("unknown mode"),
        }
        let perf_stats = perf_monitor.stop();

        let qps = args.total_rows as f64 * 1_000_000_000f64 / perf_stats.dur.as_nanos() as f64;
        let op_nanos =
            perf_stats.dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
        println!(
            "btree {} insert: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
            args.mode,
            args.threads,
            perf_stats.dur.as_millis(),
            args.total_rows,
            qps,
            op_nanos
        );

        if args.compact {
            let purge_list = tree
                .compact_all::<u64>(BTreeCompactConfig::new(1.0, 1.0))
                .await;
            for g in purge_list {
                pool.deallocate_page(g);
            }
        }
        let stat = tree.collect_space_statistics().await;
        println!("compact={}, space stats: {:?}", args.compact, stat);

        perf_monitor.start();

        match &args.search_mode[..] {
            "seq" => {
                for i in 0..args.total_rows {
                    tree.lookup_optimistic::<u64>(&i.to_be_bytes()).await;
                }
            }
            "rand" => {
                let between = Uniform::from(0..args.total_rows);
                let mut thd_rng = rand::thread_rng();
                for i in 0..args.total_rows {
                    let k = between.sample(&mut thd_rng);
                    tree.insert(&k.to_be_bytes(), i, 100).await;
                }
            }
            _ => panic!("unknown search mode"),
        }

        let perf_stats = perf_monitor.stop();

        let qps = args.total_rows as f64 * 1_000_000_000f64 / perf_stats.dur.as_nanos() as f64;
        let op_nanos =
            perf_stats.dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
        println!(
            "btree {} lookup: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
            args.mode,
            args.threads,
            perf_stats.dur.as_millis(),
            args.total_rows,
            qps,
            op_nanos
        );
    }
    unsafe {
        StaticLifetime::drop_static(pool);
    }
}

async fn single_thread_bench_stdmap(args: &Args) {
    let mut map = BTreeMap::new();
    let mut perf_monitor = PerfMonitor::new();
    perf_monitor.start();
    for i in 0..args.total_rows {
        map.insert(i, i);
    }

    match &args.mode[..] {
        "seq" => {
            for i in 0..args.total_rows {
                map.insert(i, i);
            }
        }
        "rand" => {
            let between = Uniform::from(0..args.total_rows);
            let mut thd_rng = rand::thread_rng();
            for i in 0..args.total_rows {
                let k = between.sample(&mut thd_rng);
                map.insert(k, i);
            }
        }
        _ => panic!("unknown mode"),
    }
    let perf_stats = perf_monitor.stop();

    let qps = args.total_rows as f64 * 1_000_000_000f64 / perf_stats.dur.as_nanos() as f64;
    let op_nanos = perf_stats.dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
    println!(
        "stdmap {} insert: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
        args.mode,
        args.threads,
        perf_stats.dur.as_millis(),
        args.total_rows,
        qps,
        op_nanos
    );

    perf_monitor.start();

    match &args.search_mode[..] {
        "seq" => {
            for i in 0..args.total_rows {
                map.get(&i);
            }
        }
        "rand" => {
            let between = Uniform::from(0..args.total_rows);
            let mut thd_rng = rand::thread_rng();
            for _ in 0..args.total_rows {
                let k = between.sample(&mut thd_rng);
                map.get(&k);
            }
        }
        _ => panic!("unknown search mode"),
    }
    let perf_stats = perf_monitor.stop();

    let qps = args.total_rows as f64 * 1_000_000_000f64 / perf_stats.dur.as_nanos() as f64;
    let op_nanos = perf_stats.dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
    println!(
        "stdmap {} lookup: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
        args.mode,
        args.threads,
        perf_stats.dur.as_millis(),
        args.total_rows,
        qps,
        op_nanos
    );
}

async fn single_thread_bench_bplustree(args: &Args) {
    // let tree = bplustree::BPlusTree::new();
    let tree = bplustree::GenericBPlusTree::<u64, u64, 32768, 32768>::new();
    let mut perf_monitor = PerfMonitor::new();
    perf_monitor.start();
    for i in 0..args.total_rows {
        tree.insert(i, i);
    }

    match &args.mode[..] {
        "seq" => {
            for i in 0..args.total_rows {
                tree.insert(i, i);
            }
        }
        "rand" => {
            let between = Uniform::from(0..args.total_rows);
            let mut thd_rng = rand::thread_rng();
            for i in 0..args.total_rows {
                let k = between.sample(&mut thd_rng);
                tree.insert(k, i);
            }
        }
        _ => panic!("unknown mode"),
    }
    let perf_stats = perf_monitor.stop();

    let qps = args.total_rows as f64 * 1_000_000_000f64 / perf_stats.dur.as_nanos() as f64;
    let op_nanos = perf_stats.dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
    println!(
        "bplustree {} insert: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
        args.mode,
        args.threads,
        perf_stats.dur.as_millis(),
        args.total_rows,
        qps,
        op_nanos
    );

    perf_monitor.start();

    match &args.search_mode[..] {
        "seq" => {
            for i in 0..args.total_rows {
                tree.lookup(&i, |value| *value);
            }
        }
        "rand" => {
            let between = Uniform::from(0..args.total_rows);
            let mut thd_rng = rand::thread_rng();
            for _ in 0..args.total_rows {
                let k = between.sample(&mut thd_rng);
                tree.lookup(&k, |value| *value);
            }
        }
        _ => panic!("unknown search mode"),
    }
    let perf_stats = perf_monitor.stop();

    let qps = args.total_rows as f64 * 1_000_000_000f64 / perf_stats.dur.as_nanos() as f64;
    let op_nanos = perf_stats.dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
    println!(
        "bplustree {} lookup: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
        args.mode,
        args.threads,
        perf_stats.dur.as_millis(),
        args.total_rows,
        qps,
        op_nanos
    );
}

struct PerfMonitor {
    cache_refs: Option<PerfCounter>,
    cache_misses: Option<PerfCounter>,
    cycles: Option<PerfCounter>,
    instructions: Option<PerfCounter>,
    branch_misses: Option<PerfCounter>,
    start: Option<Instant>,
}

impl PerfMonitor {
    #[inline]
    pub fn new() -> PerfMonitor {
        let pid = std::process::id() as i32;
        let cache_refs = Builder::from_hardware_event(Hardware::CacheReferences)
            .for_pid(pid)
            .finish()
            .ok();
        let cache_misses = Builder::from_hardware_event(Hardware::CacheMisses)
            .for_pid(pid)
            .finish()
            .ok();
        let cycles = Builder::from_hardware_event(Hardware::CPUCycles)
            .for_pid(pid)
            .finish()
            .ok();
        let instructions = Builder::from_hardware_event(Hardware::Instructions)
            .for_pid(pid)
            .finish()
            .ok();
        let branch_misses = Builder::from_hardware_event(Hardware::BranchMisses)
            .for_pid(pid)
            .finish()
            .ok();
        PerfMonitor {
            cache_refs,
            cache_misses,
            cycles,
            instructions,
            branch_misses,
            start: None,
        }
    }

    #[inline]
    pub fn start(&mut self) {
        if let Some(cache_refs) = self.cache_refs.as_mut() {
            cache_refs.start().unwrap();
        }
        if let Some(cache_misses) = self.cache_misses.as_mut() {
            cache_misses.start().unwrap();
        }
        if let Some(cycles) = self.cycles.as_mut() {
            cycles.start().unwrap();
        }
        if let Some(instructions) = self.instructions.as_mut() {
            instructions.start().unwrap();
        }
        if let Some(branch_misses) = self.branch_misses.as_mut() {
            branch_misses.start().unwrap();
        }
        self.start = Some(Instant::now());
    }

    #[inline]
    pub fn stop(&mut self) -> PerfStats {
        if let Some(cache_refs) = self.cache_refs.as_mut() {
            cache_refs.stop().unwrap();
        }
        if let Some(cache_misses) = self.cache_misses.as_mut() {
            cache_misses.stop().unwrap();
        }
        if let Some(cycles) = self.cycles.as_mut() {
            cycles.stop().unwrap();
        }
        if let Some(instructions) = self.instructions.as_mut() {
            instructions.stop().unwrap();
        }
        if let Some(branch_misses) = self.branch_misses.as_mut() {
            branch_misses.stop().unwrap();
        }
        let dur = self.start.take().unwrap().elapsed();
        PerfStats {
            cache_refs: self
                .cache_refs
                .as_mut()
                .and_then(|p| p.read().ok())
                .unwrap_or_default(),
            cache_misses: self
                .cache_misses
                .as_mut()
                .and_then(|p| p.read().ok())
                .unwrap_or_default(),
            cycles: self
                .cycles
                .as_mut()
                .and_then(|p| p.read().ok())
                .unwrap_or_default(),
            instructions: self
                .instructions
                .as_mut()
                .and_then(|p| p.read().ok())
                .unwrap_or_default(),
            branch_misses: self
                .branch_misses
                .as_mut()
                .and_then(|p| p.read().ok())
                .unwrap_or_default(),
            dur,
        }
    }
}

struct PerfStats {
    cache_refs: u64,
    cache_misses: u64,
    cycles: u64,
    instructions: u64,
    branch_misses: u64,
    dur: Duration,
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "1GiB", value_parser = parse_byte_size)]
    mem_size: usize,

    /// Total rows
    #[arg(long, default_value = "1000000")]
    total_rows: u64,

    /// query thread count
    #[arg(long, default_value = "1")]
    threads: usize,

    #[arg(long, default_value = "rand")]
    mode: String,

    #[arg(long, default_value = "seq")]
    search_mode: String,

    #[arg(long)]
    compact: bool,
}

#[inline]
fn parse_byte_size(input: &str) -> Result<usize, ParseError> {
    Byte::parse_str(input, true).map(|b| b.as_u64() as usize)
}
