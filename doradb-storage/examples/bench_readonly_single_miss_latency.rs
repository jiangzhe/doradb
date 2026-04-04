use clap::Parser;
use doradb_storage::buffer::frame::BufferFrame;
use doradb_storage::buffer::page::{PAGE_SIZE, Page};
use doradb_storage::buffer::{BufferPoolStats, PoolRole, ReadonlyBufferPool};
use doradb_storage::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
use doradb_storage::conf::{
    EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig,
};
use doradb_storage::error::FileKind;
use doradb_storage::file::BlockID;
use doradb_storage::io::{AIOBuf, DirectBuf, IOBackendStats};
use doradb_storage::quiescent::QuiescentBox;
use doradb_storage::value::ValKind;
use rand::RngCore;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{Builder, TempDir};

const DEFAULT_PAGES: usize = 8192;
const DEFAULT_WARM_READS: usize = 1_000_000;
const DEFAULT_CACHE_BYTES: usize = 640usize * 1024 * 1024;
const MIN_READONLY_POOL_PAGES: usize = 256;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Readonly buffer-pool serialized single-miss latency benchmark"
)]
struct Args {
    /// Number of persisted blocks written to the table file.
    #[arg(long, default_value_t = DEFAULT_PAGES)]
    pages: usize,
    /// Number of random warm-hit control reads after the full resident set is loaded.
    #[arg(long, default_value_t = DEFAULT_WARM_READS)]
    warm_reads: usize,
    /// Capacity of the global readonly pool in bytes.
    #[arg(long, default_value_t = DEFAULT_CACHE_BYTES)]
    cache_bytes: usize,
    /// Parent directory used for the temporary benchmark storage root.
    ///
    /// If omitted, the system temp directory is used.
    #[arg(long)]
    temp_dir: Option<PathBuf>,
}

fn make_metadata() -> Arc<TableMetadata> {
    Arc::new(TableMetadata::new(
        vec![ColumnSpec::new(
            "c0",
            ValKind::U32,
            ColumnAttributes::empty(),
        )],
        vec![],
    ))
}

async fn write_pages(table_file: &Arc<doradb_storage::file::table_file::TableFile>, pages: usize) {
    for page_id in 0..pages {
        let mut buf = DirectBuf::zeroed(PAGE_SIZE);
        let bytes = buf.as_bytes_mut();
        bytes.fill(0);
        bytes[0..8].copy_from_slice(&(page_id as u64).to_le_bytes());
        table_file
            .write_block(BlockID::from(page_id), buf)
            .await
            .unwrap();
    }
}

const fn readonly_frame_bytes() -> usize {
    std::mem::size_of::<BufferFrame>() + std::mem::size_of::<Page>()
}

fn required_cache_bytes(pages: usize) -> Option<usize> {
    pages
        .max(MIN_READONLY_POOL_PAGES)
        .checked_mul(readonly_frame_bytes())
}

fn format_pool_stats(stats: BufferPoolStats) -> String {
    format!(
        "cache_hits={} cache_misses={} miss_joins={} queued_reads={} running_reads={} completed_reads={} read_errors={} queued_writes={} running_writes={} completed_writes={} write_errors={}",
        stats.cache_hits,
        stats.cache_misses,
        stats.miss_joins,
        stats.queued_reads,
        stats.running_reads,
        stats.completed_reads,
        stats.read_errors,
        stats.queued_writes,
        stats.running_writes,
        stats.completed_writes,
        stats.write_errors
    )
}

fn format_backend_stats(stats: IOBackendStats) -> String {
    format!(
        "submit_and_wait_calls={} submitted_ops={} submit_and_wait_nanos={} wait_completions={}",
        stats.submit_and_wait_calls,
        stats.submitted_ops,
        stats.submit_and_wait_nanos,
        stats.wait_completions,
    )
}

fn print_phase(
    phase: &str,
    reads: usize,
    elapsed: Duration,
    allocated: usize,
    checksum: u64,
    pool_stats: BufferPoolStats,
    backend_stats: IOBackendStats,
) {
    let ns_per_read = elapsed.as_nanos() as f64 / reads.max(1) as f64;
    println!(
        "phase={} reads={} allocated={} elapsed_ms={} ns_per_read={:.2} checksum={} pool_stats=\"{}\" backend_stats=\"{}\"",
        phase,
        reads,
        allocated,
        elapsed.as_millis(),
        ns_per_read,
        checksum,
        format_pool_stats(pool_stats),
        format_backend_stats(backend_stats),
    );
}

fn main() {
    let args = Args::parse();
    if args.pages == 0 {
        eprintln!("pages must be greater than zero");
        std::process::exit(2);
    }

    let required_cache_bytes = required_cache_bytes(args.pages).unwrap_or_else(|| {
        eprintln!(
            "required cache size overflow: pages={} frame_bytes={}",
            args.pages,
            readonly_frame_bytes(),
        );
        std::process::exit(2);
    });
    if args.cache_bytes < required_cache_bytes {
        eprintln!(
            "cache_bytes={} is too small for a true warm-hit control; required_cache_bytes={} pages={} frame_bytes={}",
            args.cache_bytes,
            required_cache_bytes,
            args.pages,
            readonly_frame_bytes(),
        );
        std::process::exit(2);
    }
    if let Some(temp_dir) = &args.temp_dir
        && !temp_dir.is_dir()
    {
        eprintln!(
            "temp_dir={} is not an existing directory",
            temp_dir.display(),
        );
        std::process::exit(2);
    }

    smol::block_on(async move {
        let temp_dir = match &args.temp_dir {
            Some(temp_dir) => Builder::new()
                .prefix("doradb-storage-bench.")
                .tempdir_in(temp_dir)
                .unwrap(),
            None => TempDir::new().unwrap(),
        };
        let engine = EngineConfig::default()
            .storage_root(temp_dir.path())
            .file(
                FileSystemConfig::default()
                    .data_dir(".")
                    .readonly_buffer_size(args.cache_bytes),
            )
            .meta_buffer(32usize * 1024 * 1024)
            .index_buffer(64usize * 1024 * 1024)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(64usize * 1024 * 1024)
                    .max_file_size(128usize * 1024 * 1024),
            )
            .trx(TrxSysConfig::default().skip_recovery(true))
            .build()
            .await
            .unwrap();
        let table_file = engine
            .table_fs
            .create_table_file(901, make_metadata(), false)
            .unwrap();
        let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
        drop(old_root);

        write_pages(&table_file, args.pages).await;

        let pool = QuiescentBox::new(ReadonlyBufferPool::from_table_file(
            901,
            FileKind::TableFile,
            Arc::clone(&table_file),
            engine.disk_pool.clone_inner(),
        ));

        println!(
            "readonly-single-miss-latency storage_root={} pages={} page_size={} dataset_bytes={} cache_bytes={} required_cache_bytes={} frame_bytes={}",
            temp_dir.path().display(),
            args.pages,
            PAGE_SIZE,
            args.pages * PAGE_SIZE,
            args.cache_bytes,
            required_cache_bytes,
            readonly_frame_bytes(),
        );

        let pool_guard = pool.pool_guard();
        let cold_pool_start = pool.global_stats();
        let cold_backend_start = engine.table_fs.io_backend_stats();
        let cold_start = std::time::Instant::now();
        let mut cold_checksum = 0u64;
        for page_id in 0..args.pages {
            let g = pool
                .read_block(&pool_guard, BlockID::from(page_id))
                .await
                .expect("buffer-pool read failed in benchmark");
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&g.page()[0..8]);
            cold_checksum ^= u64::from_le_bytes(arr);
            drop(g);
        }
        let cold_elapsed = cold_start.elapsed();
        let cold_pool_end = pool.global_stats();
        let cold_backend_end = engine.table_fs.io_backend_stats();
        print_phase(
            "cold_miss",
            args.pages,
            cold_elapsed,
            engine.disk_pool.allocated(),
            cold_checksum,
            cold_pool_end.delta_since(cold_pool_start),
            cold_backend_end.delta_since(cold_backend_start),
        );

        let warm_pool_start = cold_pool_end;
        let warm_backend_start = cold_backend_end;
        let warm_start = std::time::Instant::now();
        let mut warm_checksum = 0u64;
        let mut rng = rand::rng();
        for _ in 0..args.warm_reads {
            let page_id = rng.next_u64() % args.pages as u64;
            let g = pool
                .read_block(&pool_guard, BlockID::from(page_id))
                .await
                .expect("buffer-pool read failed in benchmark");
            warm_checksum ^= g.page()[0] as u64;
            drop(g);
        }
        let warm_elapsed = warm_start.elapsed();
        let warm_pool_end = pool.global_stats();
        let warm_backend_end = engine.table_fs.io_backend_stats();
        print_phase(
            "warm_hit",
            args.warm_reads,
            warm_elapsed,
            engine.disk_pool.allocated(),
            warm_checksum,
            warm_pool_end.delta_since(warm_pool_start),
            warm_backend_end.delta_since(warm_backend_start),
        );

        drop(pool);
        drop(table_file);
        drop(engine);
    });
}
