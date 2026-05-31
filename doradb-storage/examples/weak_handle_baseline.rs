// Baseline for public runtime handle boundaries that RFC-0019 phases can affect.
// Tasks that change session, transaction, statement, table lookup, or lifecycle
// admission paths should respect this example by running it before and after the
// task and comparing `baseline.csv` for performance impact.

use doradb_storage::{
    ColumnAttributes, ColumnSpec, EngineConfig, EvictableBufferPoolConfig, FileSystemConfig,
    IndexAttributes, IndexKey, IndexSpec, Result as StorageResult, SelectKey, Table, TableSpec,
    TrxSysConfig, UpdateCol, Val, ValKind,
};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const DEFAULT_ITERATIONS: usize = 1000;
const DEFAULT_SCAN_ROWS: usize = 10_000;
const DEFAULT_POOL_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_OUT_DIR: &str = "target/weak-handle-baseline";

type ToolResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Args {
    iterations: usize,
    scan_rows: usize,
    out_dir: PathBuf,
}

struct BenchRow {
    operation: &'static str,
    iterations: usize,
    elapsed: Duration,
}

impl BenchRow {
    #[inline]
    fn avg_ns(&self) -> u128 {
        self.elapsed.as_nanos() / self.iterations as u128
    }
}

fn usage() -> &'static str {
    "Usage: cargo run -p doradb-storage --example weak_handle_baseline -- [--iterations <n>] [--scan-rows <n>] [--out-dir <path>]\n\
\n\
Measures current public operation boundaries that RFC-0019 weak-handle phases may affect.\n\
Defaults: --iterations 1000 --scan-rows 10000 --out-dir target/weak-handle-baseline\n\
Output: <out-dir>/baseline.csv"
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> ToolResult<()> {
    let args = parse_args()?;
    let rows = smol::block_on(run_baseline(&args))?;
    let report_path = write_csv(&args.out_dir, &rows)?;
    print_report(&rows, &report_path);
    Ok(())
}

fn parse_args() -> ToolResult<Args> {
    let mut iterations = DEFAULT_ITERATIONS;
    let mut scan_rows = DEFAULT_SCAN_ROWS;
    let mut out_dir = PathBuf::from(DEFAULT_OUT_DIR);
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--iterations" | "--iters" => {
                let value = args.next().ok_or("missing value for --iterations")?;
                iterations = parse_positive_usize("--iterations", &value)?;
            }
            "--scan-rows" => {
                let value = args.next().ok_or("missing value for --scan-rows")?;
                scan_rows = parse_positive_usize("--scan-rows", &value)?;
            }
            "--out-dir" => {
                let value = args.next().ok_or("missing value for --out-dir")?;
                out_dir = PathBuf::from(value);
            }
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {arg}\n{}", usage()).into()),
        }
    }

    Ok(Args {
        iterations,
        scan_rows,
        out_dir,
    })
}

fn parse_positive_usize(flag: &'static str, value: &str) -> ToolResult<usize> {
    let parsed = value.parse::<usize>()?;
    if parsed == 0 {
        return Err(format!("{flag} must be greater than zero").into());
    }
    Ok(parsed)
}

async fn run_baseline(args: &Args) -> StorageResult<Vec<BenchRow>> {
    let temp_dir = TempDir::new()?;
    let engine = baseline_engine_config(temp_dir.path()).build().await?;
    let table = create_baseline_table(&engine).await?;
    let table_id = table.table_id();
    let setup_rows = args.scan_rows.max(args.iterations);

    {
        let mut session = engine.new_session()?;
        insert_range(&mut session, &table, 0, setup_rows).await?;
        insert_range(&mut session, &table, 20_000_000, args.iterations).await?;
        insert_range(&mut session, &table, 30_000_000, args.iterations).await?;
    }

    let mut rows = Vec::new();
    rows.push(measure_session_begin(&engine, args.iterations).await?);
    rows.push(measure_statement_exec(&engine, args.iterations).await?);
    rows.push(measure_table_lookup(&engine, table_id, args.iterations).await?);
    rows.push(measure_point_lookup(&engine, &table, args.iterations).await?);
    rows.push(measure_insert(&engine, &table, args.iterations).await?);
    rows.push(measure_update(&engine, &table, args.iterations).await?);
    rows.push(measure_delete(&engine, &table, args.iterations).await?);
    rows.push(measure_table_scan(&engine, &table, args.iterations, setup_rows).await?);

    drop(table);
    engine.shutdown()?;
    Ok(rows)
}

fn baseline_engine_config(root: &Path) -> EngineConfig {
    EngineConfig::default()
        .storage_root(root)
        .meta_buffer(DEFAULT_POOL_BYTES)
        .index_buffer(DEFAULT_POOL_BYTES)
        .index_max_file_size(128usize * 1024 * 1024)
        .data_buffer(
            EvictableBufferPoolConfig::default()
                .max_mem_size(DEFAULT_POOL_BYTES)
                .max_file_size(128usize * 1024 * 1024),
        )
        .file(FileSystemConfig::default().readonly_buffer_size(DEFAULT_POOL_BYTES))
        .trx(TrxSysConfig::default())
}

async fn create_baseline_table(engine: &doradb_storage::Engine) -> StorageResult<Arc<Table>> {
    let mut session = engine.new_session()?;
    let table_id = session
        .create_table(
            TableSpec::new(vec![
                ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("payload", ValKind::I32, ColumnAttributes::empty()),
            ]),
            vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
        )
        .await?;
    session.get_table(table_id).await
}

async fn insert_range(
    session: &mut doradb_storage::Session,
    table: &Table,
    start: i32,
    count: usize,
) -> StorageResult<()> {
    let mut trx = session.begin_trx()?;
    for offset in 0..count {
        let id = start + offset as i32;
        trx.exec(async |stmt| {
            stmt.table_insert_mvcc(table, vec![Val::from(id), Val::from(id)])
                .await
                .map(|_| ())
        })
        .await?;
    }
    trx.commit().await?;
    Ok(())
}

async fn measure_session_begin(
    engine: &doradb_storage::Engine,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        let trx = session.begin_trx()?;
        elapsed += start.elapsed();
        trx.rollback().await?;
    }
    Ok(BenchRow {
        operation: "session_begin",
        iterations,
        elapsed,
    })
}

async fn measure_statement_exec(
    engine: &doradb_storage::Engine,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        trx.exec(async |_stmt| Ok(())).await?;
        elapsed += start.elapsed();
    }
    trx.rollback().await?;
    Ok(BenchRow {
        operation: "statement_exec",
        iterations,
        elapsed,
    })
}

async fn measure_table_lookup(
    engine: &doradb_storage::Engine,
    table_id: doradb_storage::id::TableID,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let engine_ref = engine.new_ref()?;
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        let table = engine_ref.get_table(table_id).await?;
        elapsed += start.elapsed();
        drop(table);
    }
    drop(engine_ref);
    Ok(BenchRow {
        operation: "table_lookup",
        iterations,
        elapsed,
    })
}

async fn measure_point_lookup(
    engine: &doradb_storage::Engine,
    table: &Table,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let key = SelectKey::new(0, vec![Val::from(0i32)]);
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        let res = trx
            .exec(async |stmt| stmt.table_lookup_unique_mvcc(table, &key, &[0, 1]).await)
            .await?;
        elapsed += start.elapsed();
        assert!(res.is_found(), "baseline point lookup key must exist");
    }
    trx.rollback().await?;
    Ok(BenchRow {
        operation: "point_lookup",
        iterations,
        elapsed,
    })
}

async fn measure_insert(
    engine: &doradb_storage::Engine,
    table: &Table,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let mut elapsed = Duration::ZERO;
    for offset in 0..iterations {
        let id = 10_000_000 + offset as i32;
        let start = Instant::now();
        trx.exec(async |stmt| {
            stmt.table_insert_mvcc(table, vec![Val::from(id), Val::from(id)])
                .await
        })
        .await?;
        elapsed += start.elapsed();
    }
    trx.commit().await?;
    Ok(BenchRow {
        operation: "insert",
        iterations,
        elapsed,
    })
}

async fn measure_update(
    engine: &doradb_storage::Engine,
    table: &Table,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let mut elapsed = Duration::ZERO;
    for offset in 0..iterations {
        let id = 20_000_000 + offset as i32;
        let key = SelectKey::new(0, vec![Val::from(id)]);
        let update = vec![UpdateCol {
            idx: 1,
            val: Val::from(-id),
        }];
        let start = Instant::now();
        let res = trx
            .exec(async |stmt| stmt.table_update_unique_mvcc(table, &key, update).await)
            .await?;
        elapsed += start.elapsed();
        assert!(res.is_updated(), "baseline update key must exist");
    }
    trx.commit().await?;
    Ok(BenchRow {
        operation: "update",
        iterations,
        elapsed,
    })
}

async fn measure_delete(
    engine: &doradb_storage::Engine,
    table: &Table,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let mut elapsed = Duration::ZERO;
    for offset in 0..iterations {
        let id = 30_000_000 + offset as i32;
        let key = SelectKey::new(0, vec![Val::from(id)]);
        let start = Instant::now();
        trx.exec(async |stmt| stmt.table_delete_unique_mvcc(table, &key, false).await)
            .await?;
        elapsed += start.elapsed();
    }
    trx.commit().await?;
    Ok(BenchRow {
        operation: "delete",
        iterations,
        elapsed,
    })
}

async fn measure_table_scan(
    engine: &doradb_storage::Engine,
    table: &Table,
    iterations: usize,
    expected_rows: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let mut seen = 0usize;
        let start = Instant::now();
        trx.exec(async |stmt| {
            stmt.table_scan_mvcc(table, &[0, 1], |_| {
                seen += 1;
                true
            })
            .await
        })
        .await?;
        elapsed += start.elapsed();
        assert!(
            seen >= expected_rows,
            "baseline scan should see at least the setup rows"
        );
    }
    trx.rollback().await?;
    Ok(BenchRow {
        operation: "table_scan",
        iterations,
        elapsed,
    })
}

fn write_csv(out_dir: &Path, rows: &[BenchRow]) -> ToolResult<PathBuf> {
    fs::create_dir_all(out_dir)?;
    let path = out_dir.join("baseline.csv");
    let mut out = String::from("operation,iterations,elapsed_ns,avg_ns\n");
    for row in rows {
        out.push_str(&format!(
            "{},{},{},{}\n",
            row.operation,
            row.iterations,
            row.elapsed.as_nanos(),
            row.avg_ns()
        ));
    }
    fs::write(&path, out)?;
    Ok(path)
}

fn print_report(rows: &[BenchRow], report_path: &Path) {
    println!("operation,iterations,elapsed_ns,avg_ns");
    for row in rows {
        println!(
            "{},{},{},{}",
            row.operation,
            row.iterations,
            row.elapsed.as_nanos(),
            row.avg_ns()
        );
    }
    println!("wrote {}", report_path.display());
}
