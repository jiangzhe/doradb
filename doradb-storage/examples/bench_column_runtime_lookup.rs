use clap::Parser;
use doradb_storage::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
};
use doradb_storage::conf::{
    EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig,
};
use doradb_storage::engine::{Engine, EngineRef};
use doradb_storage::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, SelectMvcc};
use doradb_storage::session::Session;
use doradb_storage::table::{CheckpointOutcome, Table, TableAccess, TablePersistence};
use doradb_storage::value::{Val, ValKind};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const READ_SET: [usize; 2] = [0, 1];

#[derive(Clone, Parser)]
#[command(about = "Persisted row lookup benchmark via public table/session APIs")]
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
    engine_ref: EngineRef,
    table: Arc<Table>,
    keys: Vec<SelectKey>,
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
                .role(doradb_storage::buffer::PoolRole::Mem)
                .max_mem_size(64u64 * 1024 * 1024)
                .max_file_size(128u64 * 1024 * 1024),
        )
        .trx(TrxSysConfig::default().log_file_stem("bench_column_runtime_lookup"))
        .file(
            FileSystemConfig::default()
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
    assert!(matches!(
        table.checkpoint(&mut session).await.unwrap(),
        CheckpointOutcome::Published { .. }
    ));
    drop(session);

    let keys = live_keys(rows, sparse, sparse_stride);
    assert!(!keys.is_empty());

    BenchmarkCase {
        label,
        engine_ref: engine.new_ref().unwrap(),
        _temp_dir: temp_dir,
        _engine: engine,
        table,
        keys,
    }
}

fn live_keys(rows: usize, sparse: bool, sparse_stride: usize) -> Vec<SelectKey> {
    let mut keys = Vec::with_capacity(rows);
    for idx in 0..rows {
        if sparse && idx.is_multiple_of(sparse_stride) {
            continue;
        }
        keys.push(SelectKey::new(0, vec![Val::from(idx as i32)]));
    }
    keys
}

async fn insert_rows(table: &Arc<Table>, session: &mut Session, rows: usize) {
    let mut trx = session.try_begin_trx().unwrap().unwrap();
    for idx in 0..rows {
        let name = format!("name-{idx}");
        let mut stmt = trx.start_stmt();
        let (ctx, effects) = stmt.ctx_and_effects_mut();
        let res = table
            .accessor()
            .insert_mvcc(
                ctx,
                effects,
                vec![Val::from(idx as i32), Val::from(&name[..])],
            )
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
        let (ctx, effects) = stmt.ctx_and_effects_mut();
        let res = table
            .accessor()
            .delete_unique_mvcc(ctx, effects, &key, false)
            .await;
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
            engine_ref,
            table,
            keys: _,
        } = self;
        drop(table);
        drop(engine_ref);
        drop(_engine);
        drop(_temp_dir);
    }
}

fn bench_case(case: &BenchmarkCase, threads: usize, iterations_per_thread: usize) {
    let elapsed = bench_parallel(case, threads, iterations_per_thread);
    print_result(
        case.label,
        case.keys.len(),
        threads,
        iterations_per_thread,
        elapsed,
    );
}

fn bench_parallel(case: &BenchmarkCase, threads: usize, iterations_per_thread: usize) -> Duration {
    assert!(threads > 0);
    let start = Instant::now();
    thread::scope(|scope| {
        for worker_idx in 0..threads {
            let engine_ref = case.engine_ref.clone();
            let table = Arc::clone(&case.table);
            let keys = &case.keys;
            scope.spawn(move || {
                smol::block_on(async move {
                    let mut session = engine_ref.try_new_session().unwrap();
                    let mut trx = session.try_begin_trx().unwrap().unwrap();
                    for step in 0..iterations_per_thread {
                        let key = &keys[(worker_idx + step * threads) % keys.len()];
                        let stmt = trx.start_stmt();
                        let res = table
                            .accessor()
                            .index_lookup_unique_mvcc(stmt.ctx(), key, &READ_SET)
                            .await
                            .unwrap();
                        let vals = match res {
                            SelectMvcc::Found(vals) => vals,
                            SelectMvcc::NotFound => panic!("benchmark key unexpectedly missing"),
                        };
                        black_box(vals);
                        trx = stmt.succeed();
                    }
                    trx.commit().await.unwrap();
                    drop(session);
                });
            });
        }
    });
    start.elapsed()
}

fn print_result(
    label: &str,
    live_rows: usize,
    threads: usize,
    iterations_per_thread: usize,
    elapsed: Duration,
) {
    let total_ops = threads * iterations_per_thread;
    let nanos = elapsed.as_nanos() as f64 / total_ops as f64;
    let qps = total_ops as f64 * 1_000_000_000f64 / elapsed.as_nanos() as f64;
    println!(
        "{label:>6} select_mvcc: live_rows={live_rows}, threads={threads}, iterations/thread={iterations_per_thread}, total_ops={total_ops}, elapsed={}ms, op={nanos:.1}ns, qps={qps:.2}",
        elapsed.as_millis(),
    );
}
