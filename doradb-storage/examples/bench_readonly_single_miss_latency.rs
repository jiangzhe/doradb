use clap::Parser;
use doradb_storage::buffer::frame::BufferFrame;
use doradb_storage::buffer::page::Page;
use doradb_storage::buffer::{BufferPoolStats, PoolRole};
use doradb_storage::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
};
use doradb_storage::conf::{
    EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig,
};
use doradb_storage::io::IOBackendStats;
use doradb_storage::row::ops::{SelectKey, SelectMvcc};
use doradb_storage::session::Session;
use doradb_storage::table::{CheckpointOutcome, Table, TableAccess, TablePersistence};
use doradb_storage::value::{Val, ValKind};
use rand::RngCore;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{Builder, TempDir};

const DEFAULT_ROWS: usize = 8192;
const DEFAULT_WARM_READS: usize = 1_000_000;
const DEFAULT_CACHE_BYTES: usize = 640usize * 1024 * 1024;
const READ_SET: [usize; 1] = [0];

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Public persisted-row lookup cold/warm latency benchmark"
)]
struct Args {
    /// Number of persisted rows written to the benchmark table.
    #[arg(long, default_value_t = DEFAULT_ROWS)]
    rows: usize,
    /// Number of random warm lookup reads after the persisted row set is loaded.
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

const fn readonly_frame_bytes() -> usize {
    std::mem::size_of::<BufferFrame>() + std::mem::size_of::<Page>()
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
    if args.rows == 0 {
        eprintln!("rows must be greater than zero");
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
            .trx(TrxSysConfig::default())
            .build()
            .await
            .unwrap();

        let mut ddl_session = engine.try_new_session().unwrap();
        let table_id = ddl_session
            .create_table(
                TableSpec::new(vec![ColumnSpec::new(
                    "id",
                    ValKind::I32,
                    ColumnAttributes::empty(),
                )]),
                vec![IndexSpec::new(
                    "idx_pk",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            )
            .await
            .unwrap();
        drop(ddl_session);

        let table = engine.catalog().get_table(table_id).await.unwrap();
        let mut write_session = engine.try_new_session().unwrap();
        insert_rows(&table, &mut write_session, args.rows).await;
        table.freeze(&write_session, usize::MAX).await;
        assert!(matches!(
            table.checkpoint(&mut write_session).await.unwrap(),
            CheckpointOutcome::Published { .. }
        ));
        drop(write_session);

        let keys = build_keys(args.rows);

        println!(
            "persisted-row-lookup-latency storage_root={} rows={} cache_bytes={} frame_bytes={}",
            temp_dir.path().display(),
            args.rows,
            args.cache_bytes,
            readonly_frame_bytes(),
        );

        let mut session = engine.try_new_session().unwrap();

        let cold_pool_start = engine.disk_pool.stats();
        let cold_backend_start = engine.table_fs.io_backend_stats();
        let cold_start = std::time::Instant::now();
        let mut cold_checksum = 0u64;
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        for (idx, key) in keys.iter().enumerate() {
            let stmt = trx.start_stmt();
            let res = table
                .accessor()
                .index_lookup_unique_mvcc(stmt.ctx(), key, &READ_SET)
                .await
                .unwrap();
            let vals = match res {
                SelectMvcc::Found(vals) => vals,
                SelectMvcc::NotFound => panic!("cold benchmark key unexpectedly missing"),
            };
            black_box(vals);
            cold_checksum ^= idx as u64;
            trx = stmt.succeed();
        }
        trx.commit().await.unwrap();
        let cold_elapsed = cold_start.elapsed();
        let cold_pool_end = engine.disk_pool.stats();
        let cold_backend_end = engine.table_fs.io_backend_stats();
        print_phase(
            "cold_select",
            args.rows,
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
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        for _ in 0..args.warm_reads {
            let idx = (rng.next_u64() % args.rows as u64) as usize;
            let key = &keys[idx];
            let stmt = trx.start_stmt();
            let res = table
                .accessor()
                .index_lookup_unique_mvcc(stmt.ctx(), key, &READ_SET)
                .await
                .unwrap();
            let vals = match res {
                SelectMvcc::Found(vals) => vals,
                SelectMvcc::NotFound => panic!("warm benchmark key unexpectedly missing"),
            };
            black_box(vals);
            warm_checksum ^= idx as u64;
            trx = stmt.succeed();
        }
        trx.commit().await.unwrap();
        let warm_elapsed = warm_start.elapsed();
        let warm_pool_end = engine.disk_pool.stats();
        let warm_backend_end = engine.table_fs.io_backend_stats();
        print_phase(
            "warm_select",
            args.warm_reads,
            warm_elapsed,
            engine.disk_pool.allocated(),
            warm_checksum,
            warm_pool_end.delta_since(warm_pool_start),
            warm_backend_end.delta_since(warm_backend_start),
        );

        drop(session);
        drop(table);
        drop(engine);
    });
}

fn build_keys(rows: usize) -> Vec<SelectKey> {
    (0..rows)
        .map(|idx| SelectKey::new(0, vec![Val::from(idx as i32)]))
        .collect()
}

async fn insert_rows(table: &Arc<Table>, session: &mut Session, rows: usize) {
    let mut trx = session.try_begin_trx().unwrap().unwrap();
    for idx in 0..rows {
        let mut stmt = trx.start_stmt();
        let (ctx, effects) = stmt.ctx_and_effects_mut();
        let res = table
            .accessor()
            .insert_mvcc(ctx, effects, vec![Val::from(idx as i32)])
            .await;
        assert!(res.is_ok());
        trx = stmt.succeed();
    }
    trx.commit().await.unwrap();
}
