use clap::Parser;
use doradb_storage::buffer::{GlobalReadonlyBufferPool, PoolGuard, PoolRole, ReadonlyBufferPool};
use doradb_storage::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
};
use doradb_storage::conf::{
    EngineConfig, EvictableBufferPoolConfig, TableFileSystemConfig, TrxSysConfig,
};
use doradb_storage::engine::Engine;
use doradb_storage::error::PersistedFileKind;
use doradb_storage::index::ColumnBlockIndex;
use doradb_storage::quiescent::QuiescentBox;
use doradb_storage::row::RowID;
use doradb_storage::row::ops::{DeleteMvcc, InsertMvcc, SelectKey};
use doradb_storage::session::Session;
use doradb_storage::table::{Table, TablePersistence};
use doradb_storage::value::{Val, ValKind};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[derive(Clone, Parser)]
struct Args {
    #[arg(long, default_value_t = 4096)]
    rows: usize,
    #[arg(long, default_value_t = 20_000)]
    iterations: usize,
    #[arg(long, default_value_t = 4)]
    sparse_stride: usize,
    #[arg(long, default_value_t = default_threads())]
    threads: usize,
}

struct BenchmarkCase {
    label: &'static str,
    _temp_dir: TempDir,
    _engine: Engine,
    _table: Arc<Table>,
    disk_pool: ReadonlyBufferPool,
    disk_pool_guard: PoolGuard,
    _global_pool: QuiescentBox<GlobalReadonlyBufferPool>,
    root_page_id: u64,
    end_row_id: RowID,
    row_ids: Vec<RowID>,
}

fn main() {
    let args = Args::parse();
    smol::block_on(async move {
        let dense = build_case("dense", args.rows, false, args.sparse_stride).await;
        let sparse = build_case("sparse", args.rows, true, args.sparse_stride).await;
        bench_case(&dense, args.threads, args.iterations);
        bench_case(&sparse, args.threads, args.iterations);
        dense.shutdown();
        sparse.shutdown();
    });
}

fn default_threads() -> usize {
    thread::available_parallelism()
        .map(|n| n.get().min(4))
        .unwrap_or(4)
}

async fn build_case(
    label: &'static str,
    rows: usize,
    sparse: bool,
    sparse_stride: usize,
) -> BenchmarkCase {
    assert!(rows >= 4);
    assert!(sparse_stride >= 2);

    let temp_dir = TempDir::new().unwrap();
    let engine = EngineConfig::default()
        .storage_root(temp_dir.path())
        .data_buffer(
            EvictableBufferPoolConfig::default()
                .role(PoolRole::Mem)
                .max_mem_size(64u64 * 1024 * 1024)
                .max_file_size(128u64 * 1024 * 1024),
        )
        .trx(
            TrxSysConfig::default()
                .log_file_stem("bench_column_runtime_lookup")
                .skip_recovery(true),
        )
        .file(
            TableFileSystemConfig::default()
                .io_depth(16)
                .readonly_buffer_size(128 * 1024 * 1024)
                .data_dir("."),
        )
        .build()
        .await
        .unwrap();

    let mut ddl_session = engine.try_new_session().unwrap();
    let table_id = ddl_session
        .create_table(
            TableSpec::new(vec![
                ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
            ]),
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
    let mut session = engine.try_new_session().unwrap();
    insert_rows(&table, &mut session, rows).await;
    if sparse {
        delete_rows(&table, &mut session, rows, sparse_stride).await;
    }
    table.freeze(&session, usize::MAX).await;
    table.data_checkpoint(&mut session).await.unwrap();

    let global_pool = QuiescentBox::new(
        GlobalReadonlyBufferPool::with_capacity(PoolRole::Disk, 64 * 1024 * 1024).unwrap(),
    );
    let disk_pool = ReadonlyBufferPool::from_table_file(
        table.table_id(),
        PersistedFileKind::TableFile,
        Arc::clone(table.file()),
        global_pool.guard(),
    );

    let active_root = table.file().active_root();
    let root_page_id = active_root.column_block_index_root;
    let end_row_id = active_root.pivot_row_id;
    let disk_pool_guard = disk_pool.pool_guard();
    let index = ColumnBlockIndex::new(root_page_id, end_row_id, &disk_pool, &disk_pool_guard);

    let mut row_ids = Vec::new();
    for row_id in 0..rows as RowID {
        if sparse && (row_id as usize).is_multiple_of(sparse_stride) {
            continue;
        }
        if index.locate_block(row_id).await.unwrap().is_some() {
            row_ids.push(row_id);
        }
    }
    assert!(!row_ids.is_empty());

    BenchmarkCase {
        label,
        _temp_dir: temp_dir,
        _engine: engine,
        _table: table,
        _global_pool: global_pool,
        disk_pool,
        disk_pool_guard,
        root_page_id,
        end_row_id,
        row_ids,
    }
}

async fn insert_rows(table: &Arc<Table>, session: &mut Session, rows: usize) {
    let mut trx = session.try_begin_trx().unwrap().unwrap();
    for idx in 0..rows {
        let name = format!("name-{idx}");
        let mut stmt = trx.start_stmt();
        let res = stmt
            .insert_row(table, vec![Val::from(idx as i32), Val::from(&name[..])])
            .await;
        assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
        trx = stmt.succeed();
    }
    trx.commit().await.unwrap();
}

async fn delete_rows(table: &Arc<Table>, session: &mut Session, rows: usize, stride: usize) {
    let mut trx = session.try_begin_trx().unwrap().unwrap();
    for idx in (0..rows).step_by(stride) {
        let key = SelectKey::new(0, vec![Val::from(idx as i32)]);
        let mut stmt = trx.start_stmt();
        let res = stmt.delete_row(table, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
        trx = stmt.succeed();
    }
    trx.commit().await.unwrap();
}

impl BenchmarkCase {
    fn shutdown(self) {
        let BenchmarkCase {
            label: _,
            _temp_dir,
            _engine,
            _table,
            disk_pool,
            disk_pool_guard,
            _global_pool,
            root_page_id: _,
            end_row_id: _,
            row_ids: _,
        } = self;
        drop(disk_pool);
        drop(disk_pool_guard);
        drop(_global_pool);
        drop(_table);
        drop(_engine);
        drop(_temp_dir);
    }
}

#[derive(Clone, Copy)]
enum BenchPath {
    Locate,
    Resolved,
}

fn bench_case(case: &BenchmarkCase, threads: usize, iterations_per_thread: usize) {
    let locate = bench_parallel(case, threads, iterations_per_thread, BenchPath::Locate);
    let resolved = bench_parallel(case, threads, iterations_per_thread, BenchPath::Resolved);
    print_result(
        case.label,
        "locate",
        case.row_ids.len(),
        threads,
        iterations_per_thread,
        locate,
    );
    print_result(
        case.label,
        "resolved",
        case.row_ids.len(),
        threads,
        iterations_per_thread,
        resolved,
    );
}

fn bench_parallel(
    case: &BenchmarkCase,
    threads: usize,
    iterations_per_thread: usize,
    path: BenchPath,
) -> Duration {
    assert!(threads > 0);
    let start = Instant::now();
    thread::scope(|scope| {
        for worker_idx in 0..threads {
            let disk_pool = case.disk_pool.clone();
            let disk_pool_guard = case.disk_pool_guard.clone();
            let row_ids = &case.row_ids;
            let root_page_id = case.root_page_id;
            let end_row_id = case.end_row_id;
            scope.spawn(move || {
                smol::block_on(async move {
                    let index = ColumnBlockIndex::new(
                        root_page_id,
                        end_row_id,
                        &disk_pool,
                        &disk_pool_guard,
                    );
                    for step in 0..iterations_per_thread {
                        let row_id = row_ids[(worker_idx + step * threads) % row_ids.len()];
                        match path {
                            BenchPath::Locate => {
                                let entry = index.locate_block(row_id).await.unwrap().unwrap();
                                black_box(entry.block_id());
                            }
                            BenchPath::Resolved => {
                                let resolved =
                                    index.locate_and_resolve_row(row_id).await.unwrap().unwrap();
                                black_box((resolved.block_id(), resolved.row_idx()));
                            }
                        }
                    }
                });
            });
        }
    });
    start.elapsed()
}

fn print_result(
    label: &str,
    path: &str,
    live_rows: usize,
    threads: usize,
    iterations_per_thread: usize,
    elapsed: Duration,
) {
    let total_ops = threads * iterations_per_thread;
    let nanos = elapsed.as_nanos() as f64 / total_ops as f64;
    let qps = total_ops as f64 * 1_000_000_000f64 / elapsed.as_nanos() as f64;
    println!(
        "{label:>6} {path:>8}: live_rows={live_rows}, threads={threads}, iterations/thread={iterations_per_thread}, total_ops={total_ops}, elapsed={}ms, op={nanos:.1}ns, qps={qps:.2}",
        elapsed.as_millis(),
    );
}
