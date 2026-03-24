use clap::Parser;
use doradb_storage::buffer::guard::PageGuard;
use doradb_storage::buffer::page::{PAGE_SIZE, Page, PageID};
use doradb_storage::buffer::{BufferPool, EvictableBufferPoolConfig, PoolRole, ReadonlyBufferPool};
use doradb_storage::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
use doradb_storage::engine::EngineConfig;
use doradb_storage::error::PersistedFileKind;
use doradb_storage::file::table_fs::TableFileSystemConfig;
use doradb_storage::io::{AIOBuf, DirectBuf};
use doradb_storage::latch::LatchFallbackMode;
use doradb_storage::quiescent::QuiescentBox;
use doradb_storage::trx::sys_conf::TrxSysConfig;
use doradb_storage::value::ValKind;
use rand::RngCore;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

#[derive(Debug, Parser)]
#[command(version, about = "Readonly buffer-pool cold/warm benchmark")]
struct Args {
    /// Number of pages written to table file.
    #[arg(long, default_value_t = 8192)]
    pages: usize,
    /// Number of random reads in warm phase.
    #[arg(long, default_value_t = 1_000_000)]
    warm_reads: usize,
    /// Capacity of global readonly pool in bytes.
    #[arg(long, default_value_t = 256usize * 1024 * 1024)]
    cache_bytes: usize,
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
        table_file.write_page(page_id as PageID, buf).await.unwrap();
    }
}

fn main() {
    let args = Args::parse();
    smol::block_on(async move {
        let temp_dir = TempDir::new().unwrap();
        let engine = EngineConfig::default()
            .storage_root(temp_dir.path())
            .file(
                TableFileSystemConfig::default()
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
            PersistedFileKind::TableFile,
            Arc::clone(&table_file),
            engine.disk_pool.clone_inner(),
        ));
        let pool_guard = (*pool).pool_guard();

        let cold_start = Instant::now();
        let mut cold_checksum = 0u64;
        for page_id in 0..args.pages as PageID {
            let g = pool
                .get_page::<Page>(&pool_guard, page_id, LatchFallbackMode::Shared)
                .await
                .expect("buffer-pool read failed in test")
                .lock_shared_async()
                .await
                .unwrap();
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&g.page()[0..8]);
            cold_checksum ^= u64::from_le_bytes(arr);
            drop(g);
        }
        let cold_elapsed = cold_start.elapsed();
        let cold_per_read_ns = cold_elapsed.as_nanos() as f64 / args.pages.max(1) as f64;

        let warm_start = Instant::now();
        let mut warm_checksum = 0u64;
        let mut rng = rand::rng();
        for _ in 0..args.warm_reads {
            let page_id = (rng.next_u64() % args.pages as u64) as PageID;
            let g = pool
                .get_page::<Page>(&pool_guard, page_id, LatchFallbackMode::Shared)
                .await
                .expect("buffer-pool read failed in test")
                .lock_shared_async()
                .await
                .unwrap();
            warm_checksum ^= g.page()[0] as u64;
            drop(g);
        }
        let warm_elapsed = warm_start.elapsed();
        let warm_per_read_ns = warm_elapsed.as_nanos() as f64 / args.warm_reads.max(1) as f64;

        println!(
            "readonly-buffer-bench pages={} page_size={} cold_reads={} warm_reads={} allocated={} cold_ms={} warm_ms={} cold_ns_per_read={:.2} warm_ns_per_read={:.2} cold_checksum={} warm_checksum={}",
            args.pages,
            PAGE_SIZE,
            args.pages,
            args.warm_reads,
            engine.disk_pool.allocated(),
            cold_elapsed.as_millis(),
            warm_elapsed.as_millis(),
            cold_per_read_ns,
            warm_per_read_ns,
            cold_checksum,
            warm_checksum
        );

        drop(pool_guard);
        drop(pool);
        drop(table_file);
        drop(engine);
    });
}
