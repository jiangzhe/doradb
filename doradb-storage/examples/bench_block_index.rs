use clap::Parser;
use doradb_storage::buffer::EvictableBufferPoolConfig;
use doradb_storage::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
};
use doradb_storage::engine::EngineConfig;
use doradb_storage::index::{BlockIndex, RowLocation};
use doradb_storage::trx::sys_conf::TrxSysConfig;
use doradb_storage::value::ValKind;
use parking_lot::RwLock;

use rand::RngCore;
use semistr::SemiStr;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

fn main() {
    let args = Args::parse();
    smol::block_on(async {
        let args = args.clone();
        let engine = EngineConfig::default()
            .meta_buffer(64usize * 1024 * 1024)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .max_mem_size(2usize * 1024 * 1024 * 1024)
                    .max_file_size(3usize * 1024 * 1024 * 1024)
                    .file_path("databuffer_bench1.bin"),
            )
            .trx(TrxSysConfig::default().skip_recovery(true))
            .build()
            .await
            .unwrap();
        {
            let metadata = Arc::new(TableMetadata::new(
                vec![ColumnSpec {
                    column_name: SemiStr::new("id"),
                    column_type: ValKind::I32,
                    column_attributes: ColumnAttributes::INDEX,
                }],
                vec![IndexSpec::new(
                    "idx_tb1_id",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            ));
            let table_id = 101;
            let uninit_table_file = engine
                .table_fs
                .create_table_file(table_id, metadata, true)
                .unwrap();
            let (table_file, _) = uninit_table_file.commit(1, false).await.unwrap();
            let blk_idx = BlockIndex::new(
                engine.meta_pool,
                table_id,
                table_file.active_root().pivot_row_id,
                table_file.active_root_ptr(),
                Arc::clone(&table_file),
            )
            .await;
            let blk_idx = Box::leak(Box::new(blk_idx));
            blk_idx.enable_page_committer(engine.trx_sys);

            for _ in 0..args.pages {
                let _ = blk_idx
                    .get_insert_page(engine.mem_pool, args.rows_per_page)
                    .await;
            }
            let start = Instant::now();

            let stop = Arc::new(AtomicBool::new(false));
            let mut handles = vec![];
            for _ in 0..args.threads {
                let args = args.clone();
                let stop = Arc::clone(&stop);
                let blk_idx = &*blk_idx;
                let handle = std::thread::spawn(move || {
                    let ex = smol::LocalExecutor::new();
                    smol::block_on(ex.run(worker(args, blk_idx, stop)))
                });
                handles.push(handle);
            }
            let mut total_count = 0;
            let mut total_sum_page_id = 0;
            for h in handles {
                let (count, sum_page_id) = h.join().unwrap();
                total_count += count;
                total_sum_page_id += sum_page_id;
            }
            let dur = start.elapsed();

            let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / total_count as f64;
            println!(
                "block_index: threads={}, dur={}ms, total_count={}, sum_page_id={}, op={:.2}ns",
                args.threads,
                dur.as_millis(),
                total_count,
                total_sum_page_id,
                op_nanos
            );
        }
        drop(engine);

        let _ = std::fs::remove_file("databuffer_bench1.bin");
        remove_files("*.tbl");
    });

    bench_btreemap(args);
}

fn bench_btreemap(args: Args) {
    let btreemap = RwLock::new(BTreeMap::new());
    let mut page_id = 0u64;
    {
        let mut g = btreemap.write();
        for i in 0..args.pages {
            let row_id = (i * args.rows_per_page) as u64;
            g.insert(row_id, page_id);
            page_id += 1;
        }
    }

    let btreemap = Box::leak(Box::new(btreemap));
    {
        let start = Instant::now();
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = vec![];
        for _ in 0..args.threads {
            let args = args.clone();
            let stop = Arc::clone(&stop);
            let handle = std::thread::spawn(|| worker_btreemap(args, btreemap, stop));
            handles.push(handle);
        }
        let mut total_count = 0;
        let mut total_sum_page_id = 0;
        for h in handles {
            let (count, sum_page_id) = h.join().unwrap();
            total_count += count;
            total_sum_page_id += sum_page_id;
        }
        let dur = start.elapsed();
        let op_nanos = dur.as_nanos() as f64 * args.threads as f64 / total_count as f64;
        println!(
            "btreemap: threads={}, dur={}ms, total_count={}, sum_page_id={}, op={:.2}ns",
            args.threads,
            dur.as_millis(),
            total_count,
            total_sum_page_id,
            op_nanos
        );
    }
    unsafe {
        drop(Box::from_raw(
            btreemap as *const _ as *mut RwLock<BTreeMap<u64, u64>>,
        ));
    }
}

async fn worker(args: Args, blk_idx: &'static BlockIndex, stop: Arc<AtomicBool>) -> (usize, u64) {
    let max_row_id = (args.pages * args.rows_per_page) as u64;
    let mut rng = rand::rng();
    // rng.next_u64() as usize % max_row_id;
    let mut count = 0usize;
    let mut sum_page_id = 0u64;
    for _ in 0..args.count {
        let row_id = rng.next_u64() % max_row_id;
        let res = blk_idx.find_row(row_id).await;
        match res {
            RowLocation::RowPage(page_id) => {
                count += 1;
                sum_page_id += page_id;
            }
            _ => panic!("invalid search result"),
        }
        if stop.load(Ordering::Relaxed) {
            return (count, sum_page_id);
        }
    }
    stop.store(true, Ordering::SeqCst);
    (count, sum_page_id)
}

fn worker_btreemap(
    args: Args,
    btreemap: &'static RwLock<BTreeMap<u64, u64>>,
    stop: Arc<AtomicBool>,
) -> (usize, u64) {
    let max_row_id = (args.pages * args.rows_per_page) as u64;
    let mut rng = rand::rng();
    // rng.next_u64() as usize % max_row_id;
    let mut count = 0usize;
    let mut sum_page_id = 0u64;
    for _ in 0..args.count {
        let row_id = rng.next_u64() % max_row_id;
        {
            let g = btreemap.read();
            let mut res = g.range(row_id..);
            match res.next() {
                Some((_, v)) => {
                    count += 1;
                    sum_page_id += *v;
                }
                _ => {
                    // panic!("invalid search result {}", row_id)
                    count += 1;
                }
            }
        }

        if stop.load(Ordering::Relaxed) {
            return (count, sum_page_id);
        }
    }
    stop.store(true, Ordering::SeqCst);
    (count, sum_page_id)
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// path of redo log file
    #[arg(long, default_value = "25000")]
    pages: usize,

    /// Rows per page
    #[arg(long, default_value = "400")]
    rows_per_page: usize,

    /// query thread count
    #[arg(long, default_value = "1")]
    threads: usize,

    // #[arg(long, default_value = "spin", value_parser = LatchFallbackMode::from_str)]
    // latch_mode: LatchFallbackMode,
    /// query count per thread
    #[arg(long, default_value = "1000000")]
    count: usize,
}

fn remove_files(file_pattern: &str) {
    let files = glob::glob(file_pattern);
    if files.is_err() {
        return;
    }
    for f in files.unwrap() {
        if f.is_err() {
            continue;
        }
        let fp = f.unwrap();
        let _ = std::fs::remove_file(&fp);
    }
}
