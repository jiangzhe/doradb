use byte_unit::{Byte, ParseError};
use clap::Parser;
use doradb_storage::buffer::{BufferPool, FixedBufferPool};
use doradb_storage::index::{BTree, BTreeCompactConfig, BTreeU64};
use doradb_storage::lifetime::StaticLifetime;
use rand_distr::{Distribution, Uniform};
use std::collections::BTreeMap;

use std::time::Instant;

fn main() {
    let args = Args::parse();
    smol::block_on(async {
        let args = args.clone();

        single_thread(&args).await;
    });
}

async fn single_thread(args: &Args) {
    if args.bitflag & 1 == 0 {
        single_thread_bench_btree(args).await;
    }

    if args.bitflag & 2 == 0 {
        single_thread_bench_stdmap(args).await;
    }

    if args.bitflag & 4 == 0 {
        single_thread_bench_bplustree(args).await;
    }
}

async fn single_thread_bench_btree(args: &Args) {
    let pool = FixedBufferPool::with_capacity_static(args.mem_size).unwrap();
    {
        let tree = BTree::new(pool, args.hints_enabled, 1).await;

        let start = Instant::now();
        match &args.mode[..] {
            "seq" => {
                for i in 0..args.total_rows {
                    // trick to use lower 4 bytes as head
                    tree.insert(&i.to_be_bytes(), BTreeU64::from(i), 100).await;
                }
            }
            "rand" => {
                let between = Uniform::new(0, args.total_rows).unwrap();
                let mut thd_rng = rand::rng();
                for i in 0..args.total_rows {
                    let k = between.sample(&mut thd_rng);
                    tree.insert(&k.to_be_bytes(), BTreeU64::from(i), 100).await;
                }
            }
            _ => panic!("unknown mode"),
        }
        let dur = start.elapsed();

        let qps = args.total_rows as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
        let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
        println!(
            "btree {} insert: threads={}, hints={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
            args.mode,
            args.threads,
            args.hints_enabled,
            dur.as_millis(),
            args.total_rows,
            qps,
            op_nanos
        );

        if args.compact {
            let purge_list = tree
                .compact_all::<BTreeU64>(BTreeCompactConfig::new(1.0, 1.0).unwrap())
                .await;
            for g in purge_list {
                pool.deallocate_page(g);
            }
        }
        // let stat = tree.collect_space_statistics().await;
        // println!("compact={}, space stats: {:?}", args.compact, stat);

        if args.insert_only {
            return;
        }

        let start = Instant::now();

        match &args.search_mode[..] {
            "seq" => {
                for i in 0..args.total_rows {
                    tree.lookup_optimistic::<BTreeU64>(&i.to_be_bytes()).await;
                }
            }
            "rand" => {
                let between = Uniform::new(0, args.total_rows).unwrap();
                let mut thd_rng = rand::rng();
                for _ in 0..args.total_rows {
                    let k = between.sample(&mut thd_rng);
                    tree.lookup_optimistic::<BTreeU64>(&k.to_be_bytes()).await;
                }
            }
            _ => panic!("unknown search mode"),
        }

        let dur = start.elapsed();

        let qps = args.total_rows as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
        let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
        println!(
            "btree {} lookup: threads={}, hints={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
            args.mode,
            args.threads,
            args.hints_enabled,
            dur.as_millis(),
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
    let start = Instant::now();
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
            let between = Uniform::new(0, args.total_rows).unwrap();
            let mut thd_rng = rand::rng();
            for i in 0..args.total_rows {
                let k = between.sample(&mut thd_rng);
                map.insert(k, i);
            }
        }
        _ => panic!("unknown mode"),
    }
    let dur = start.elapsed();

    let qps = args.total_rows as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
    let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
    println!(
        "stdmap {} insert: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
        args.mode,
        args.threads,
        dur.as_millis(),
        args.total_rows,
        qps,
        op_nanos
    );

    if args.insert_only {
        return;
    }
    let start = Instant::now();

    match &args.search_mode[..] {
        "seq" => {
            for i in 0..args.total_rows {
                map.get(&i);
            }
        }
        "rand" => {
            let between = Uniform::new(0, args.total_rows).unwrap();
            let mut thd_rng = rand::rng();
            for _ in 0..args.total_rows {
                let k = between.sample(&mut thd_rng);
                map.get(&k);
            }
        }
        _ => panic!("unknown search mode"),
    }
    let dur = start.elapsed();

    let qps = args.total_rows as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
    let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
    println!(
        "stdmap {} lookup: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
        args.mode,
        args.threads,
        dur.as_millis(),
        args.total_rows,
        qps,
        op_nanos
    );
}

async fn single_thread_bench_bplustree(args: &Args) {
    let tree = bplustree::GenericBPlusTree::<[u8; 8], u64, 32768, 32768>::new();
    let start = Instant::now();
    for i in 0..args.total_rows {
        tree.insert(i.to_be_bytes(), i);
    }

    match &args.mode[..] {
        "seq" => {
            for i in 0..args.total_rows {
                tree.insert(i.to_be_bytes(), i);
            }
        }
        "rand" => {
            let between = Uniform::new(0, args.total_rows).unwrap();
            let mut thd_rng = rand::rng();
            for i in 0..args.total_rows {
                let k = between.sample(&mut thd_rng);
                tree.insert(k.to_be_bytes(), i);
            }
        }
        _ => panic!("unknown mode"),
    }
    let dur = start.elapsed();

    let qps = args.total_rows as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
    let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
    println!(
        "bplustree {} insert: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
        args.mode,
        args.threads,
        dur.as_millis(),
        args.total_rows,
        qps,
        op_nanos
    );

    if args.insert_only {
        return;
    }
    let start = Instant::now();

    match &args.search_mode[..] {
        "seq" => {
            for i in 0..args.total_rows {
                tree.lookup(&i.to_be_bytes(), |value| *value);
            }
        }
        "rand" => {
            let between = Uniform::new(0, args.total_rows).unwrap();
            let mut thd_rng = rand::rng();
            for _ in 0..args.total_rows {
                let k = between.sample(&mut thd_rng);
                tree.lookup(&k.to_be_bytes(), |value| *value);
            }
        }
        _ => panic!("unknown search mode"),
    }
    let dur = start.elapsed();

    let qps = args.total_rows as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
    let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
    println!(
        "bplustree {} lookup: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
        args.mode,
        args.threads,
        dur.as_millis(),
        args.total_rows,
        qps,
        op_nanos
    );
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

    #[arg(long, default_value = "seq")]
    mode: String,

    #[arg(long, default_value = "seq")]
    search_mode: String,

    #[arg(long)]
    compact: bool,

    /// bitflag: 1. disable btree; 2, disable stdmap; 4, disable bplustree
    #[arg(long, default_value = "0")]
    bitflag: u8,

    #[arg(long)]
    insert_only: bool,

    #[arg(long)]
    hints_enabled: bool,
}

#[inline]
fn parse_byte_size(input: &str) -> Result<usize, ParseError> {
    Byte::parse_str(input, true).map(|b| b.as_u64() as usize)
}
