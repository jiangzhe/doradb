use crate::cli::{
    IndexMode, InsertConfig, LoadConfig, LogSyncMode, PrepareArgs, WorkloadArgs, WorkloadConfig,
    validate_batch_size, validate_value_size,
};
use crate::error::{BenchError, Result};
use crate::manifest::{
    DefaultsManifest, KeyRange, Manifest, read_manifest, write_manifest, write_manifest_exclusive,
};
use crate::output::{
    BenchmarkResult, InternalStatsSnapshot, OutputConfig, internal_metrics, write_benchmark_outputs,
};
use crate::workload::{
    SessionPlan, build_session_plans, generate_keys, generate_random_read_keys,
    generate_sequential_read_keys, payload_bytes,
};
use doradb_storage::id::TableID;
use doradb_storage::{
    ColumnAttributes, ColumnSpec, Engine, EngineConfig, IndexAttributes, IndexKey, IndexSpec,
    SelectKey, SelectMvcc, Session, TableSpec, TrxSysConfig, Val, ValKind,
};
use easy_parallel::Parallel;
use smol::{Executor, channel};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct WorkerSummary {
    operations: u64,
    inserted_rows: u64,
    found: u64,
    not_found: u64,
    rows_returned: u64,
    failures: u64,
}

impl WorkerSummary {
    fn merge(&mut self, other: Self) {
        self.operations += other.operations;
        self.inserted_rows += other.inserted_rows;
        self.found += other.found;
        self.not_found += other.not_found;
        self.rows_returned += other.rows_returned;
        self.failures += other.failures;
    }
}

/// Prepare a benchmark storage root and manifest.
pub async fn prepare(storage_root: PathBuf, args: PrepareArgs) -> Result<()> {
    prepare_storage_root(&storage_root)?;
    let default_sessions = args.sessions.unwrap_or(args.threads).get();
    let defaults = DefaultsManifest::new(
        args.threads.get(),
        default_sessions,
        args.value_size.get(),
        args.batch_size.get(),
    )?;

    let engine = open_engine(&storage_root, LogSyncMode::Fsync).await?;
    let mut session = engine.new_session()?;
    let table_id = session
        .create_table(benchmark_table_spec(), benchmark_index_specs(args.index))
        .await?;
    session.close().await?;
    engine.shutdown()?;

    let manifest = Manifest::new_with_defaults(table_id.as_u64(), args.index, defaults);
    write_manifest_exclusive(&storage_root, &manifest)?;
    println!(
        "prepared storage_root={} table_id={} index={} threads={} sessions={} value_size={} batch_size={}",
        storage_root.display(),
        table_id,
        args.index,
        manifest.defaults.threads,
        manifest.defaults.sessions,
        manifest.defaults.value_size,
        manifest.defaults.batch_size
    );
    Ok(())
}

/// Run the selected benchmark workload.
pub async fn run_workload(
    storage_root: PathBuf,
    args: WorkloadArgs,
    command_context: &str,
) -> Result<()> {
    let mut manifest = read_manifest(&storage_root)?;
    let config = args.resolve(
        storage_root,
        manifest.index,
        manifest.defaults.run_defaults(),
    )?;
    validate_load_config(&config)?;
    manifest.validate_workload_compatible(config.workload())?;
    let execution_range = execution_range(&manifest, &config)?;
    let loaded_range = output_loaded_range(&manifest, &config)?;
    let table_id = TableID::new(manifest.table_id);

    let engine = open_engine(&config.storage_root, config.log_sync).await?;
    let stats_state = if config.include_stats {
        let stats_session = engine.new_session()?;
        let before = InternalStatsSnapshot::capture(&stats_session)?;
        Some((stats_session, before))
    } else {
        None
    };
    let started = Instant::now();
    let worker_result = run_workers(&engine, &config, table_id, execution_range, loaded_range);
    let elapsed = started.elapsed();
    let metrics = if let Some((mut stats_session, before)) = stats_state {
        let after = InternalStatsSnapshot::capture(&stats_session)?;
        stats_session.close().await?;
        internal_metrics(&before, &after)
    } else {
        Vec::new()
    };
    engine.shutdown()?;

    let summary = worker_result?;

    let result = BenchmarkResult::new(
        summary.operations,
        summary.inserted_rows,
        summary.found,
        summary.not_found,
        summary.rows_returned,
        elapsed,
        summary.failures,
    );
    let output_config = OutputConfig {
        workload: config.workload(),
        storage_root: config.storage_root.clone(),
        num: config.operation_count(),
        value_size: config.value_size,
        batch_size: config.batch_size,
        rand: output_rand(&config),
        seed: output_seed(&config),
        index: config.index,
        loaded_key_start: loaded_range.start,
        loaded_key_end: loaded_range.end()?,
        threads: config.threads,
        sessions: config.sessions,
        log_sync: config.log_sync,
        include_stats: config.include_stats,
        table_id: manifest.table_id,
    };
    write_benchmark_outputs(&output_config, &metrics, &result, command_context)?;

    if let WorkloadConfig::InsertSeq(insert) | WorkloadConfig::InsertRand(insert) = &config.workload
    {
        manifest.record_insert_success(insert.num)?;
        write_manifest(&config.storage_root, &manifest)?;
    }
    Ok(())
}

/// Clean benchmark artifacts from a prepared storage root.
pub async fn cleanup(storage_root: PathBuf) -> Result<()> {
    let _manifest = read_manifest(&storage_root)?;
    fs::remove_dir_all(&storage_root).map_err(|err| {
        BenchError::message(format!(
            "failed to remove storage root {}: {err}",
            storage_root.display()
        ))
    })?;
    println!("removed storage_root={}", storage_root.display());
    Ok(())
}

fn prepare_storage_root(storage_root: &Path) -> Result<()> {
    if storage_root.exists() {
        return Err(BenchError::message(format!(
            "--root {} must not exist for prepare",
            storage_root.display()
        )));
    }
    fs::create_dir_all(storage_root).map_err(|err| {
        BenchError::message(format!(
            "failed to create storage root {}: {err}",
            storage_root.display()
        ))
    })?;
    Ok(())
}

async fn open_engine(storage_root: &Path, log_sync: LogSyncMode) -> Result<Engine> {
    Ok(EngineConfig::default()
        .storage_root(storage_root)
        .trx(TrxSysConfig::default().log_sync(log_sync.as_storage()))
        .build()
        .await?)
}

fn benchmark_table_spec() -> TableSpec {
    TableSpec::new(vec![
        ColumnSpec::new("logical_key", ValKind::U64, ColumnAttributes::empty()),
        ColumnSpec::new("payload", ValKind::VarByte, ColumnAttributes::empty()),
    ])
}

fn benchmark_index_specs(index: IndexMode) -> Vec<IndexSpec> {
    match index {
        IndexMode::None => Vec::new(),
        IndexMode::Unique => vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)],
        IndexMode::NonUnique => {
            vec![IndexSpec::new(
                vec![IndexKey::new(0)],
                IndexAttributes::empty(),
            )]
        }
    }
}

fn validate_load_config(config: &LoadConfig) -> Result<()> {
    if config.threads == 0 || config.sessions == 0 {
        return Err(BenchError::message(
            "threads and sessions must both be positive",
        ));
    }
    if config.threads > config.sessions {
        return Err(BenchError::message(format!(
            "--threads ({}) must not exceed --sessions ({})",
            config.threads, config.sessions
        )));
    }
    validate_value_size(config.value_size)?;
    validate_batch_size(config.batch_size)?;
    Ok(())
}

fn execution_range(manifest: &Manifest, config: &LoadConfig) -> Result<KeyRange> {
    match &config.workload {
        WorkloadConfig::InsertSeq(insert) | WorkloadConfig::InsertRand(insert) => {
            manifest.key_range(insert.num)
        }
        WorkloadConfig::LookupSeq { .. }
        | WorkloadConfig::LookupRand { .. }
        | WorkloadConfig::TableScan { .. }
        | WorkloadConfig::IndexScan { .. } => Ok(KeyRange {
            start: 0,
            len: config.operation_count(),
        }),
    }
}

fn output_loaded_range(manifest: &Manifest, config: &LoadConfig) -> Result<KeyRange> {
    match &config.workload {
        WorkloadConfig::InsertSeq(insert) | WorkloadConfig::InsertRand(insert) => Ok(KeyRange {
            start: 0,
            len: manifest.key_range(insert.num)?.end()?,
        }),
        WorkloadConfig::LookupSeq { .. }
        | WorkloadConfig::LookupRand { .. }
        | WorkloadConfig::TableScan { .. }
        | WorkloadConfig::IndexScan { .. } => manifest.loaded_key_range(),
    }
}

fn output_rand(config: &LoadConfig) -> bool {
    matches!(
        config.workload,
        WorkloadConfig::InsertRand(_)
            | WorkloadConfig::LookupRand { .. }
            | WorkloadConfig::IndexScan { .. }
    )
}

fn output_seed(config: &LoadConfig) -> u64 {
    match &config.workload {
        WorkloadConfig::InsertSeq(insert) | WorkloadConfig::InsertRand(insert) => insert.seed,
        WorkloadConfig::LookupRand { seed, .. } | WorkloadConfig::IndexScan { seed, .. } => *seed,
        WorkloadConfig::LookupSeq { .. } | WorkloadConfig::TableScan { .. } => 0,
    }
}

fn run_workers(
    engine: &Engine,
    config: &LoadConfig,
    table_id: TableID,
    execution_range: KeyRange,
    loaded_range: KeyRange,
) -> Result<WorkerSummary> {
    let session_plans = build_session_plans(execution_range, config.sessions)?;
    let executor = Executor::new();
    let tasks = session_plans
        .into_iter()
        .map(|plan| {
            executor.spawn(execute_session(
                engine,
                config,
                table_id,
                loaded_range,
                plan,
            ))
        })
        .collect();
    let (signal, shutdown) = channel::unbounded::<()>();
    let executor_ref = &executor;
    let shutdown_receiver = shutdown.clone();

    let (_worker_results, summary) = Parallel::new()
        .each(0..config.threads, move |_| {
            let _ = smol::block_on(executor_ref.run(shutdown_receiver.recv()));
        })
        .finish(move || {
            let _signal = signal;
            smol::block_on(collect_session_tasks(tasks))
        });
    summary
}

async fn collect_session_tasks(
    tasks: Vec<smol::Task<Result<WorkerSummary>>>,
) -> Result<WorkerSummary> {
    let mut summary = WorkerSummary::default();
    let mut first_error = None;
    for task in tasks {
        match task.await {
            Ok(session) => summary.merge(session),
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(summary)
}

async fn execute_session(
    engine: &Engine,
    config: &LoadConfig,
    table_id: TableID,
    loaded_range: KeyRange,
    plan: SessionPlan,
) -> Result<WorkerSummary> {
    let mut session = engine.new_session()?;
    let load_result =
        execute_session_workload(&mut session, config, table_id, loaded_range, &plan).await;
    let close_result = session.close().await;
    match (load_result, close_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(err.into()),
    }
}

async fn execute_session_workload(
    session: &mut Session,
    config: &LoadConfig,
    table_id: TableID,
    loaded_range: KeyRange,
    plan: &SessionPlan,
) -> Result<WorkerSummary> {
    match &config.workload {
        WorkloadConfig::InsertSeq(insert) => {
            let keys = generate_keys(false, config.index, insert.seed, plan)?;
            insert_keys(session, config, insert, table_id, &keys).await
        }
        WorkloadConfig::InsertRand(insert) => {
            let keys = generate_keys(true, config.index, insert.seed, plan)?;
            insert_keys(session, config, insert, table_id, &keys).await
        }
        WorkloadConfig::LookupSeq { .. } => {
            let keys = generate_sequential_read_keys(loaded_range, plan)?;
            lookup_keys(session, config.batch_size, table_id, &keys).await
        }
        WorkloadConfig::LookupRand { seed, .. } => {
            let keys = generate_random_read_keys(*seed, loaded_range, plan)?;
            lookup_keys(session, config.batch_size, table_id, &keys).await
        }
        WorkloadConfig::TableScan { .. } => {
            table_scan_iterations(session, config.batch_size, table_id, plan.rows).await
        }
        WorkloadConfig::IndexScan { seed, .. } => {
            let keys = generate_random_read_keys(*seed, loaded_range, plan)?;
            index_scan_keys(session, config.batch_size, table_id, &keys).await
        }
    }
}

async fn insert_keys(
    session: &mut Session,
    config: &LoadConfig,
    insert: &InsertConfig,
    table_id: TableID,
    keys: &[u64],
) -> Result<WorkerSummary> {
    if keys.is_empty() {
        return Ok(WorkerSummary::default());
    }
    let batch_size = effective_batch_size(config.batch_size, keys.len() as u64)?;
    let mut inserted = 0u64;
    for batch in keys.chunks(batch_size) {
        insert_batch(session, table_id, batch, insert.seed, config.value_size).await?;
        inserted += batch.len() as u64;
    }
    Ok(WorkerSummary {
        operations: inserted,
        inserted_rows: inserted,
        found: 0,
        not_found: 0,
        rows_returned: 0,
        failures: 0,
    })
}

async fn insert_batch(
    session: &mut Session,
    table_id: TableID,
    keys: &[u64],
    seed: u64,
    value_size: usize,
) -> Result<()> {
    let mut trx = session.begin_trx()?;
    for key in keys {
        let payload = payload_bytes(*key, seed, value_size);
        let row = vec![Val::from(*key), Val::from(&payload[..])];
        if let Err(err) = trx
            .exec(async |stmt| stmt.table_insert_mvcc(table_id, row).await.map(|_| ()))
            .await
        {
            trx.rollback().await?;
            return Err(err.into());
        }
    }
    trx.commit().await?;
    Ok(())
}

async fn lookup_keys(
    session: &mut Session,
    batch_size: u64,
    table_id: TableID,
    keys: &[u64],
) -> Result<WorkerSummary> {
    if keys.is_empty() {
        return Ok(WorkerSummary::default());
    }
    let batch_size = effective_batch_size(batch_size, keys.len() as u64)?;
    let mut summary = WorkerSummary::default();
    for batch in keys.chunks(batch_size) {
        summary.merge(lookup_key_batch(session, table_id, batch).await?);
    }
    Ok(summary)
}

async fn lookup_key_batch(
    session: &mut Session,
    table_id: TableID,
    keys: &[u64],
) -> Result<WorkerSummary> {
    let mut trx = session.begin_trx()?;
    let mut summary = WorkerSummary::default();
    for key in keys {
        let select_key = SelectKey::new(0, vec![Val::from(*key)]);
        let lookup = trx
            .exec(async |stmt| {
                stmt.table_lookup_unique_mvcc(table_id, &select_key, &[0, 1])
                    .await
            })
            .await;
        match lookup {
            Ok(SelectMvcc::Found(_)) => {
                summary.operations += 1;
                summary.found += 1;
                summary.rows_returned += 1;
            }
            Ok(SelectMvcc::NotFound) => {
                summary.operations += 1;
                summary.not_found += 1;
            }
            Err(err) => {
                trx.rollback().await?;
                return Err(err.into());
            }
        }
    }
    trx.commit().await?;
    Ok(summary)
}

async fn table_scan_iterations(
    session: &mut Session,
    batch_size: u64,
    table_id: TableID,
    iterations: u64,
) -> Result<WorkerSummary> {
    validate_batch_size(batch_size)?;
    if iterations == 0 {
        return Ok(WorkerSummary::default());
    }
    let mut remaining = iterations;
    let mut summary = WorkerSummary::default();
    while remaining > 0 {
        let batch_iterations = batch_size.min(remaining);
        summary.merge(table_scan_batch(session, table_id, batch_iterations).await?);
        remaining -= batch_iterations;
    }
    Ok(summary)
}

async fn table_scan_batch(
    session: &mut Session,
    table_id: TableID,
    iterations: u64,
) -> Result<WorkerSummary> {
    let mut trx = session.begin_trx()?;
    let mut summary = WorkerSummary::default();
    for _ in 0..iterations {
        let scan = trx
            .exec(async |stmt| {
                let mut rows = 0u64;
                stmt.table_scan_mvcc(table_id, &[0, 1], |_| {
                    rows += 1;
                    true
                })
                .await?;
                Ok(rows)
            })
            .await;
        match scan {
            Ok(rows) => {
                summary.operations += 1;
                summary.rows_returned += rows;
            }
            Err(err) => {
                trx.rollback().await?;
                return Err(err.into());
            }
        }
    }
    trx.commit().await?;
    Ok(summary)
}

async fn index_scan_keys(
    session: &mut Session,
    batch_size: u64,
    table_id: TableID,
    keys: &[u64],
) -> Result<WorkerSummary> {
    if keys.is_empty() {
        return Ok(WorkerSummary::default());
    }
    let batch_size = effective_batch_size(batch_size, keys.len() as u64)?;
    let mut summary = WorkerSummary::default();
    for batch in keys.chunks(batch_size) {
        summary.merge(index_scan_key_batch(session, table_id, batch).await?);
    }
    Ok(summary)
}

async fn index_scan_key_batch(
    session: &mut Session,
    table_id: TableID,
    keys: &[u64],
) -> Result<WorkerSummary> {
    let mut trx = session.begin_trx()?;
    let mut summary = WorkerSummary::default();
    for key in keys {
        let select_key = SelectKey::new(0, vec![Val::from(*key)]);
        let scan = trx
            .exec(async |stmt| {
                stmt.table_index_scan_mvcc(table_id, &select_key, &[0, 1])
                    .await
            })
            .await;
        match scan {
            Ok(scan) => {
                let rows = scan.unwrap_rows().len() as u64;
                summary.operations += 1;
                summary.rows_returned += rows;
                if rows == 0 {
                    summary.not_found += 1;
                } else {
                    summary.found += 1;
                }
            }
            Err(err) => {
                trx.rollback().await?;
                return Err(err.into());
            }
        }
    }
    trx.commit().await?;
    Ok(summary)
}

fn effective_batch_size(batch_size: u64, operation_count: u64) -> Result<usize> {
    validate_batch_size(batch_size)?;
    let bounded = batch_size.min(operation_count.max(1));
    usize::try_from(bounded)
        .map_err(|_| BenchError::message("effective batch size exceeds addressable memory"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::MAX_VALUE_SIZE;
    use std::fs::File;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    #[test]
    fn prepare_storage_root_creates_missing_root() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");
        prepare_storage_root(&root).unwrap();
        assert!(root.is_dir());
    }

    #[test]
    fn prepare_storage_root_rejects_empty_existing() {
        let temp = TempDir::new().unwrap();
        assert!(prepare_storage_root(temp.path()).is_err());
    }

    #[test]
    fn prepare_storage_root_rejects_non_empty_existing() {
        let temp = TempDir::new().unwrap();
        File::create(temp.path().join("marker")).unwrap();
        assert!(prepare_storage_root(temp.path()).is_err());
    }

    #[test]
    fn cleanup_rejects_missing_manifest() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");
        fs::create_dir(&root).unwrap();

        assert!(smol::block_on(cleanup(root.clone())).is_err());
        assert!(root.exists());
    }

    #[test]
    fn cleanup_removes_root_after_manifest_validation() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");
        fs::create_dir(&root).unwrap();
        write_manifest(&root, &Manifest::new(42, IndexMode::None)).unwrap();

        smol::block_on(cleanup(root.clone())).unwrap();
        assert!(!root.exists());
    }

    #[test]
    fn schema_index_specs_match_index_mode_without_primary_key() {
        assert!(benchmark_index_specs(IndexMode::None).is_empty());

        let index_specs = benchmark_index_specs(IndexMode::Unique);
        assert_eq!(index_specs.len(), 1);
        assert!(index_specs[0].attributes.contains(IndexAttributes::UK));
        assert!(!index_specs[0].attributes.contains(IndexAttributes::PK));

        let index_specs = benchmark_index_specs(IndexMode::NonUnique);
        assert_eq!(index_specs.len(), 1);
        assert!(index_specs[0].attributes.is_empty());
    }

    #[test]
    fn effective_batch_size_defaults_to_configured_insert_batch_size() {
        let config = test_load_config();
        assert_eq!(effective_batch_size(config.batch_size, 10).unwrap(), 1);
    }

    #[test]
    fn validate_load_config_rejects_value_size_above_row_payload_limit() {
        let mut config = test_load_config();
        config.value_size = MAX_VALUE_SIZE + 1;
        assert!(validate_load_config(&config).is_err());
    }

    #[test]
    fn validate_load_config_rejects_invalid_batch_size() {
        let mut config = test_load_config();
        config.batch_size = 0;
        assert!(validate_load_config(&config).is_err());
    }

    #[test]
    fn validate_load_config_rejects_invalid_thread_session_counts() {
        let mut config = test_load_config();

        config.threads = 0;
        assert!(validate_load_config(&config).is_err());

        config.threads = 2;
        config.sessions = 1;
        assert!(validate_load_config(&config).is_err());
    }

    #[test]
    fn collect_session_tasks_sums_successes() {
        let executor = Executor::new();
        let tasks = vec![
            executor.spawn(async {
                Ok(WorkerSummary {
                    operations: 2,
                    inserted_rows: 2,
                    found: 1,
                    not_found: 0,
                    rows_returned: 1,
                    failures: 0,
                })
            }),
            executor.spawn(async {
                Ok(WorkerSummary {
                    operations: 3,
                    inserted_rows: 0,
                    found: 0,
                    not_found: 1,
                    rows_returned: 4,
                    failures: 1,
                })
            }),
        ];

        let summary = smol::block_on(executor.run(collect_session_tasks(tasks))).unwrap();

        assert_eq!(
            summary,
            WorkerSummary {
                operations: 5,
                inserted_rows: 2,
                found: 1,
                not_found: 1,
                rows_returned: 5,
                failures: 1,
            }
        );
    }

    #[test]
    fn collect_session_tasks_returns_first_error_after_draining_tasks() {
        let executor = Executor::new();
        let drained = Arc::new(AtomicUsize::new(0));
        let drained_task = Arc::clone(&drained);
        let tasks = vec![
            executor.spawn(async { Err(BenchError::message("first")) }),
            executor.spawn(async move {
                drained_task.fetch_add(1, Ordering::SeqCst);
                Err(BenchError::message("second"))
            }),
        ];

        let err = smol::block_on(executor.run(collect_session_tasks(tasks))).unwrap_err();

        assert_eq!(err.to_string(), "first");
        assert_eq!(drained.load(Ordering::SeqCst), 1);
    }

    fn test_load_config() -> LoadConfig {
        LoadConfig {
            storage_root: "root".into(),
            index: IndexMode::None,
            threads: 1,
            sessions: 1,
            value_size: 16,
            batch_size: 1,
            log_sync: LogSyncMode::Fsync,
            include_stats: false,
            workload: WorkloadConfig::InsertSeq(test_insert_config()),
        }
    }

    fn test_insert_config() -> InsertConfig {
        InsertConfig { num: 10, seed: 0 }
    }
}
