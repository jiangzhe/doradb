// Baseline for public runtime handle boundaries that RFC-0019 phases can affect.
// Tasks that change session, transaction, statement, table lookup, or lifecycle
// admission paths should respect this example by running it before and after the
// task and comparing `baseline.csv` for performance impact.

use doradb_storage::id::TableID;
use doradb_storage::{
    ColumnAttributes, ColumnSpec, Engine, EngineConfig, EvictableBufferPoolConfig,
    FileSystemConfig, IndexAttributes, IndexKey, IndexSpec, Result as StorageResult, SelectKey,
    TableSpec, TrxSysConfig, UpdateCol, Val, ValKind,
};
use futures::executor;
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::result::Result;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const DEFAULT_ITERATIONS: usize = 1000;
const DEFAULT_SCAN_ROWS: usize = 10_000;
const DEFAULT_POOL_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_OUT_DIR: &str = "target/weak-handle-baseline";

type ToolResult<T> = Result<T, Box<dyn Error>>;

struct Args {
    iterations: usize,
    scan_rows: usize,
    out_dir: PathBuf,
    only: Option<BenchOperation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchOperation {
    SessionBegin,
    StatementExec,
    FirstResolutionEmptyScan,
    CachedResolutionEmptyScan,
    PointLookup,
    Insert,
    Update,
    Delete,
    TableScan,
}

impl BenchOperation {
    const ALL: [BenchOperation; 9] = [
        BenchOperation::SessionBegin,
        BenchOperation::StatementExec,
        BenchOperation::FirstResolutionEmptyScan,
        BenchOperation::CachedResolutionEmptyScan,
        BenchOperation::PointLookup,
        BenchOperation::Insert,
        BenchOperation::Update,
        BenchOperation::Delete,
        BenchOperation::TableScan,
    ];

    #[inline]
    fn name(self) -> &'static str {
        match self {
            BenchOperation::SessionBegin => "session_begin",
            BenchOperation::StatementExec => "statement_exec",
            BenchOperation::FirstResolutionEmptyScan => "first_resolution_empty_scan",
            BenchOperation::CachedResolutionEmptyScan => "cached_resolution_empty_scan",
            BenchOperation::PointLookup => "point_lookup",
            BenchOperation::Insert => "insert",
            BenchOperation::Update => "update",
            BenchOperation::Delete => "delete",
            BenchOperation::TableScan => "table_scan",
        }
    }

    #[inline]
    fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|operation| operation.name() == value)
    }

    #[inline]
    fn valid_names() -> String {
        Self::ALL
            .into_iter()
            .map(BenchOperation::name)
            .collect::<Vec<_>>()
            .join(", ")
    }
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
    "Usage: cargo run --example weak_handle_baseline -- [--iterations <n>] [--scan-rows <n>] [--out-dir <path>] [--only <operation>]\n\
\n\
Measures current public operation boundaries that RFC-0019 weak-handle phases may affect.\n\
Defaults: --iterations 1000 --scan-rows 10000 --out-dir target/weak-handle-baseline\n\
Output: <out-dir>/baseline.csv"
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        exit(1);
    }
}

fn run() -> ToolResult<()> {
    let args = parse_args()?;
    let rows = executor::block_on(run_baseline(&args))?;
    let report_path = write_csv(&args.out_dir, &rows)?;
    print_report(&rows, &report_path);
    Ok(())
}

fn parse_args() -> ToolResult<Args> {
    let mut iterations = DEFAULT_ITERATIONS;
    let mut scan_rows = DEFAULT_SCAN_ROWS;
    let mut out_dir = PathBuf::from(DEFAULT_OUT_DIR);
    let mut only = None;
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
            "--only" => {
                let value = args.next().ok_or("missing value for --only")?;
                only = Some(BenchOperation::parse(&value).ok_or_else(|| {
                    format!(
                        "unknown --only operation: {value}\nvalid operations: {}",
                        BenchOperation::valid_names()
                    )
                })?);
            }
            "--help" | "-h" => {
                println!("{}", usage());
                println!("Valid --only operations: {}", BenchOperation::valid_names());
                exit(0);
            }
            _ => return Err(format!("unknown argument: {arg}\n{}", usage()).into()),
        }
    }

    Ok(Args {
        iterations,
        scan_rows,
        out_dir,
        only,
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
    let temp_dir = TempDir::new().expect("create weak-handle benchmark temp directory");
    let engine = Engine::bootstrap(baseline_engine_config(temp_dir.path())).await?;
    let setup_rows = args.scan_rows.max(args.iterations);
    let resolution_table_id = if should_run(args, BenchOperation::FirstResolutionEmptyScan)
        || should_run(args, BenchOperation::CachedResolutionEmptyScan)
    {
        Some(create_baseline_table(&engine).await?)
    } else {
        None
    };
    let data_table_id = if needs_data_table(args) {
        Some(create_baseline_table(&engine).await?)
    } else {
        None
    };

    if let Some(table_id) = data_table_id {
        let mut session = engine.new_session()?;
        if should_run(args, BenchOperation::PointLookup)
            || should_run(args, BenchOperation::TableScan)
        {
            let count = if should_run(args, BenchOperation::TableScan) {
                setup_rows
            } else {
                1
            };
            insert_range(&mut session, table_id, 0, count).await?;
        }
        if should_run(args, BenchOperation::Update) {
            insert_range(&mut session, table_id, 20_000_000, args.iterations).await?;
        }
        if should_run(args, BenchOperation::Delete) {
            insert_range(&mut session, table_id, 30_000_000, args.iterations).await?;
        }
        session.close().await?;
    }

    let mut rows = Vec::new();
    if should_run(args, BenchOperation::SessionBegin) {
        rows.push(measure_session_begin(&engine, args.iterations).await?);
    }
    if should_run(args, BenchOperation::StatementExec) {
        rows.push(measure_statement_exec(&engine, args.iterations).await?);
    }
    if should_run(args, BenchOperation::FirstResolutionEmptyScan) {
        rows.push(
            measure_first_resolution_empty_scan(
                &engine,
                resolution_table_id.expect("resolution table required"),
                args.iterations,
            )
            .await?,
        );
    }
    if should_run(args, BenchOperation::CachedResolutionEmptyScan) {
        rows.push(
            measure_cached_resolution_empty_scan(
                &engine,
                resolution_table_id.expect("resolution table required"),
                args.iterations,
            )
            .await?,
        );
    }
    if should_run(args, BenchOperation::PointLookup) {
        rows.push(
            measure_point_lookup(
                &engine,
                data_table_id.expect("data table required"),
                args.iterations,
            )
            .await?,
        );
    }
    if should_run(args, BenchOperation::Insert) {
        rows.push(
            measure_insert(
                &engine,
                data_table_id.expect("data table required"),
                args.iterations,
            )
            .await?,
        );
    }
    if should_run(args, BenchOperation::Update) {
        rows.push(
            measure_update(
                &engine,
                data_table_id.expect("data table required"),
                args.iterations,
            )
            .await?,
        );
    }
    if should_run(args, BenchOperation::Delete) {
        rows.push(
            measure_delete(
                &engine,
                data_table_id.expect("data table required"),
                args.iterations,
            )
            .await?,
        );
    }
    if should_run(args, BenchOperation::TableScan) {
        rows.push(
            measure_table_scan(
                &engine,
                data_table_id.expect("data table required"),
                args.iterations,
                setup_rows,
            )
            .await?,
        );
    }

    engine.shutdown()?;
    Ok(rows)
}

fn should_run(args: &Args, operation: BenchOperation) -> bool {
    args.only.is_none_or(|only| only == operation)
}

fn needs_data_table(args: &Args) -> bool {
    [
        BenchOperation::PointLookup,
        BenchOperation::Insert,
        BenchOperation::Update,
        BenchOperation::Delete,
        BenchOperation::TableScan,
    ]
    .into_iter()
    .any(|operation| should_run(args, operation))
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

async fn create_baseline_table(engine: &doradb_storage::Engine) -> StorageResult<TableID> {
    let mut session = engine.new_session()?;
    let table_id = session
        .create_table(
            TableSpec::new(vec![
                ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("payload", ValKind::I32, ColumnAttributes::empty()),
            ]),
            vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)],
        )
        .await?;
    session.close().await?;
    Ok(table_id)
}

async fn insert_range(
    session: &mut doradb_storage::Session,
    table_id: TableID,
    start: i32,
    count: usize,
) -> StorageResult<()> {
    let mut trx = session.begin_trx()?;
    for offset in 0..count {
        let id = start + offset as i32;
        trx.exec(async |stmt| {
            stmt.table_insert_mvcc(table_id, vec![Val::from(id), Val::from(id)])
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
    session.close().await?;
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
    session.close().await?;
    Ok(BenchRow {
        operation: "statement_exec",
        iterations,
        elapsed,
    })
}

/// Measures first table-id resolution through an otherwise empty read statement.
///
/// The timed window excludes session/transaction creation, but includes the
/// public statement boundary, statement read lock, table lifecycle/layout checks,
/// and empty MVCC scan scaffolding.
async fn measure_first_resolution_empty_scan(
    engine: &doradb_storage::Engine,
    table_id: TableID,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let read_set = [];
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let mut session = engine.new_session()?;
        let mut trx = session.begin_trx()?;
        let start = Instant::now();
        trx.exec(async |stmt| stmt.table_scan_mvcc(table_id, &read_set, |_| true).await)
            .await?;
        elapsed += start.elapsed();
        trx.rollback().await?;
        session.close().await?;
    }
    Ok(BenchRow {
        operation: "first_resolution_empty_scan",
        iterations,
        elapsed,
    })
}

/// Measures cached table-id resolution through an otherwise empty read statement.
///
/// This row is not raw table-cache lookup cost. It keeps the same transaction
/// warm so the table cache is hot, then times the full read-statement path.
async fn measure_cached_resolution_empty_scan(
    engine: &doradb_storage::Engine,
    table_id: TableID,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let read_set = [];
    trx.exec(async |stmt| stmt.table_scan_mvcc(table_id, &read_set, |_| true).await)
        .await?;
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        trx.exec(async |stmt| stmt.table_scan_mvcc(table_id, &read_set, |_| true).await)
            .await?;
        elapsed += start.elapsed();
    }
    trx.rollback().await?;
    session.close().await?;
    Ok(BenchRow {
        operation: "cached_resolution_empty_scan",
        iterations,
        elapsed,
    })
}

async fn measure_point_lookup(
    engine: &doradb_storage::Engine,
    table_id: TableID,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let key = SelectKey::new(0, vec![Val::from(0i32)]);
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        let res = trx
            .exec(async |stmt| {
                stmt.table_lookup_unique_mvcc(table_id, key.index_no, &key.vals, &[0, 1])
                    .await
            })
            .await?;
        elapsed += start.elapsed();
        assert!(res.is_found(), "baseline point lookup key must exist");
    }
    trx.rollback().await?;
    session.close().await?;
    Ok(BenchRow {
        operation: "point_lookup",
        iterations,
        elapsed,
    })
}

async fn measure_insert(
    engine: &doradb_storage::Engine,
    table_id: TableID,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let mut elapsed = Duration::ZERO;
    for offset in 0..iterations {
        let id = 10_000_000 + offset as i32;
        let start = Instant::now();
        trx.exec(async |stmt| {
            stmt.table_insert_mvcc(table_id, vec![Val::from(id), Val::from(id)])
                .await
        })
        .await?;
        elapsed += start.elapsed();
    }
    trx.commit().await?;
    session.close().await?;
    Ok(BenchRow {
        operation: "insert",
        iterations,
        elapsed,
    })
}

async fn measure_update(
    engine: &doradb_storage::Engine,
    table_id: TableID,
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
            .exec(async |stmt| {
                stmt.table_update_unique_mvcc(table_id, key.index_no, &key.vals, update)
                    .await
            })
            .await?;
        elapsed += start.elapsed();
        assert!(res.is_updated(), "baseline update key must exist");
    }
    trx.commit().await?;
    session.close().await?;
    Ok(BenchRow {
        operation: "update",
        iterations,
        elapsed,
    })
}

async fn measure_delete(
    engine: &doradb_storage::Engine,
    table_id: TableID,
    iterations: usize,
) -> StorageResult<BenchRow> {
    let mut session = engine.new_session()?;
    let mut trx = session.begin_trx()?;
    let mut elapsed = Duration::ZERO;
    for offset in 0..iterations {
        let id = 30_000_000 + offset as i32;
        let key = SelectKey::new(0, vec![Val::from(id)]);
        let start = Instant::now();
        trx.exec(async |stmt| {
            stmt.table_delete_unique_mvcc(table_id, key.index_no, &key.vals)
                .await
        })
        .await?;
        elapsed += start.elapsed();
    }
    trx.commit().await?;
    session.close().await?;
    Ok(BenchRow {
        operation: "delete",
        iterations,
        elapsed,
    })
}

async fn measure_table_scan(
    engine: &doradb_storage::Engine,
    table_id: TableID,
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
            stmt.table_scan_mvcc(table_id, &[0, 1], |_| {
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
    session.close().await?;
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
    print_derived_report(rows);
    println!("wrote {}", report_path.display());
}

fn print_derived_report(rows: &[BenchRow]) {
    let Some(statement_exec) = avg_ns_for(rows, "statement_exec") else {
        return;
    };
    let Some(first_empty_scan) = avg_ns_for(rows, "first_resolution_empty_scan") else {
        return;
    };
    let Some(cached_empty_scan) = avg_ns_for(rows, "cached_resolution_empty_scan") else {
        return;
    };
    let Some(point_lookup) = avg_ns_for(rows, "point_lookup") else {
        return;
    };

    println!("derived_metric,delta_avg_ns");
    println!(
        "first_resolution_miss_delta,{}",
        delta_ns(first_empty_scan, cached_empty_scan)
    );
    println!(
        "cached_empty_scan_over_statement_exec,{}",
        delta_ns(cached_empty_scan, statement_exec)
    );
    println!(
        "point_lookup_over_cached_empty_scan,{}",
        delta_ns(point_lookup, cached_empty_scan)
    );
}

fn avg_ns_for(rows: &[BenchRow], operation: &'static str) -> Option<u128> {
    rows.iter()
        .find(|row| row.operation == operation)
        .map(BenchRow::avg_ns)
}

fn delta_ns(left: u128, right: u128) -> i128 {
    left as i128 - right as i128
}
