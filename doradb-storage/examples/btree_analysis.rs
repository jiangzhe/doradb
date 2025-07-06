use byte_unit::{Byte, ParseError};
use clap::Parser;
use doradb_storage::buffer::{BufferPool, FixedBufferPool};
use doradb_storage::index::{BTree, BTreeCompactConfig};
use doradb_storage::lifetime::StaticLifetime;
use rand::distributions::{Distribution, Uniform};

use std::time::Instant;

fn main() {
    smol::block_on(async {
        let args = Args::parse();

        single_thread_bench_btree(&args).await;
    });
}

async fn single_thread_bench_btree(args: &Args) {
    let pool = FixedBufferPool::with_capacity_static(args.mem_size).unwrap();
    {
        let tree = BTree::new(pool, 1).await;

        let start = Instant::now();
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
        let dur = start.elapsed();

        let qps = args.total_rows as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
        let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
        println!(
            "btree {} insert: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
            args.mode,
            args.threads,
            dur.as_millis(),
            args.total_rows,
            qps,
            op_nanos
        );

        if args.compact {
            let purge_list = tree
                .compact_all::<u64>(BTreeCompactConfig::new(1.0, 1.0).unwrap())
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

        let dur = start.elapsed();

        let qps = args.total_rows as f64 * 1_000_000_000f64 / dur.as_nanos() as f64;
        let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / args.total_rows as f64;
        println!(
            "btree {} lookup: threads={}, dur={}ms, total_count={}, qps={:.2}, op={:.2}ns",
            args.mode,
            args.threads,
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

    #[arg(long)]
    insert_only: bool,
}

#[inline]
fn parse_byte_size(input: &str) -> Result<usize, ParseError> {
    Byte::parse_str(input, true).map(|b| b.as_u64() as usize)
}
