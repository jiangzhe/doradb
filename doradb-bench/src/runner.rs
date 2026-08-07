use crate::cli::{IndexMode, LogSyncMode, PrepareArgs, WorkloadArgs};
use crate::error::{BenchError, Result};
use crate::manifest::{
    DefaultsManifest, Manifest, read_manifest, write_manifest, write_manifest_exclusive,
};
use crate::output::{
    BenchmarkResult, InternalStatsSnapshot, OutputConfig, internal_metrics, write_benchmark_outputs,
};
use crate::workload::{
    IndexDdlRunner, IndexScanRunner, IndexStreamRunner, InsertRandRunner, InsertSeqRunner,
    LockTableRunner, LookupRandRunner, LookupSeqRunner, SessionPlan, SessionSummary,
    StmtNoopRunner, TableDdlRunner, TableScanRunner, TrxNoopRunner, WorkloadConfig, WorkloadRunner,
    benchmark_index_specs, benchmark_table_spec, build_session_plans,
};
use doradb_storage::id::TableID;
use doradb_storage::{Engine, EngineConfig, Session, TrxSysConfig};
use easy_parallel::Parallel;
use smol::{Executor, channel};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Prepare a benchmark storage root and manifest.
pub async fn prepare(storage_root: PathBuf, args: PrepareArgs) -> Result<()> {
    prepare_storage_root(&storage_root)?;
    let default_sessions = args.sessions.unwrap_or(args.threads).get();
    let defaults = DefaultsManifest::new(
        args.threads.get(),
        default_sessions,
        args.value_size.get(),
        args.batch_size.get(),
        args.log_sync,
    )?;

    let engine = open_engine(&storage_root, args.log_sync).await?;
    let operation_result = async {
        let mut session = engine.new_session()?;
        let create_result =
            create_prepared_tables(&mut session, args.index, args.tables.get()).await;
        finish_session(session, create_result).await
    }
    .await;
    let table_ids = finish_engine(&engine, operation_result)?;
    let Some((table_id, auxiliary_table_ids)) = table_ids.split_first() else {
        return Err(BenchError::message(
            "prepare did not create a primary benchmark table",
        ));
    };

    let manifest = Manifest::new_with_tables(
        table_id.as_u64(),
        auxiliary_table_ids
            .iter()
            .map(|table_id| table_id.as_u64())
            .collect(),
        args.index,
        defaults,
    );
    write_manifest_exclusive(&storage_root, &manifest)?;
    println!(
        "prepared storage_root={} table_id={} index={} tables={} log_sync={} threads={} sessions={} value_size={} batch_size={}",
        storage_root.display(),
        table_id,
        args.index,
        manifest.table_count(),
        manifest.defaults.log_sync,
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
    macro_rules! run {
        ($runner:ty, $args:expr) => {
            run_typed_workload::<$runner>(&storage_root, &mut manifest, &$args, command_context)
                .await
        };
    }
    match args {
        WorkloadArgs::InsertSeq(args) => run!(InsertSeqRunner, args),
        WorkloadArgs::InsertRand(args) => run!(InsertRandRunner, args),
        WorkloadArgs::LookupSeq(args) => run!(LookupSeqRunner, args),
        WorkloadArgs::LookupRand(args) => run!(LookupRandRunner, args),
        WorkloadArgs::TableScan(args) => run!(TableScanRunner, args),
        WorkloadArgs::IndexScan(args) => run!(IndexScanRunner, args),
        WorkloadArgs::StmtNoop(args) => run!(StmtNoopRunner, args),
        WorkloadArgs::TrxNoop(args) => run!(TrxNoopRunner, args),
        WorkloadArgs::IndexStream(args) => run!(IndexStreamRunner, args),
        WorkloadArgs::TableDdl(args) => run!(TableDdlRunner, args),
        WorkloadArgs::IndexDdl(args) => run!(IndexDdlRunner, args),
        WorkloadArgs::LockTable(args) => run!(LockTableRunner, args),
    }
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

async fn run_typed_workload<R>(
    storage_root: &Path,
    manifest: &mut Manifest,
    args: &<R::Config as WorkloadConfig>::Args,
    command_context: &str,
) -> Result<()>
where
    R: WorkloadRunner,
{
    let config = R::Config::resolve(manifest, args)?;
    let common = *config.common();
    let table_id = TableID::new(manifest.table_id);
    let loaded_range = config.output_loaded_range();

    let engine = open_engine(storage_root, common.log_sync).await?;
    let operation_result = async {
        let stats_state = if common.include_stats {
            let stats_session = engine.new_session()?;
            let before = InternalStatsSnapshot::capture(&stats_session)?;
            Some((stats_session, before))
        } else {
            None
        };
        let started = Instant::now();
        let summary = run_session_workers::<R>(&engine, &config, table_id)?;
        let elapsed = started.elapsed();
        let metrics = if let Some((mut stats_session, before)) = stats_state {
            let after = InternalStatsSnapshot::capture(&stats_session)?;
            stats_session.close().await?;
            internal_metrics(&before, &after)
        } else {
            Vec::new()
        };
        Ok((summary, elapsed, metrics))
    }
    .await;
    let (summary, elapsed, metrics) = finish_engine(&engine, operation_result)?;

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
        workload: R::Config::WORKLOAD,
        storage_root: storage_root.to_path_buf(),
        num: config.operation_count(),
        range: config.scan_range(),
        value_size: common.value_size,
        batch_size: common.batch_size,
        rand: config.random(),
        seed: config.seed(),
        index: manifest.index,
        loaded_key_start: loaded_range.start,
        loaded_key_end: loaded_range.end()?,
        threads: common.threads,
        sessions: common.sessions,
        log_sync: common.log_sync,
        include_stats: common.include_stats,
        table_id: manifest.table_id,
        scope: config.lock_scope(),
        unlock: config.unlock(),
        tables: config.prepared_table_count(),
        scenario: config.lock_scenario(),
        lock_mode: config.lock_mode(),
        width: config.lock_width(),
    };
    write_benchmark_outputs(&output_config, &metrics, &result, command_context)?;

    if config.update_manifest(manifest)? {
        write_manifest(storage_root, manifest)?;
    }
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
    Ok(Engine::bootstrap(
        EngineConfig::default()
            .storage_root(storage_root)
            .trx(TrxSysConfig::default().log_sync(log_sync.as_storage())),
    )
    .await?)
}

async fn create_prepared_tables(
    session: &mut Session,
    index: IndexMode,
    table_count: usize,
) -> Result<Vec<TableID>> {
    let mut table_ids = Vec::with_capacity(table_count);
    for _ in 0..table_count {
        table_ids.push(
            session
                .create_table(benchmark_table_spec(), benchmark_index_specs(index))
                .await?,
        );
    }
    Ok(table_ids)
}

fn finish_engine<T>(engine: &Engine, operation_result: Result<T>) -> Result<T> {
    engine.shutdown();
    operation_result
}

fn run_session_workers<R: WorkloadRunner>(
    engine: &Engine,
    config: &R::Config,
    table_id: TableID,
) -> Result<SessionSummary> {
    let common = config.common();
    let session_plans = build_session_plans(config.execution_range(), common.sessions)?;
    let runner = R::new(config, table_id);
    let executor = Executor::new();
    let tasks = session_plans
        .into_iter()
        .map(|plan| executor.spawn(execute_session(engine, runner.clone(), plan)))
        .collect();
    let (signal, shutdown) = channel::unbounded::<()>();
    let executor_ref = &executor;
    let shutdown_receiver = shutdown.clone();

    let (_worker_results, summary) = Parallel::new()
        .each(0..common.threads, move |_| {
            let _ = smol::block_on(executor_ref.run(shutdown_receiver.recv()));
        })
        .finish(move || {
            let _signal = signal;
            smol::block_on(collect_session_tasks(tasks))
        });
    summary
}

async fn collect_session_tasks(
    tasks: Vec<smol::Task<Result<SessionSummary>>>,
) -> Result<SessionSummary> {
    let mut summary = SessionSummary::default();
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

async fn execute_session<R: WorkloadRunner>(
    engine: &Engine,
    runner: R,
    plan: SessionPlan,
) -> Result<SessionSummary> {
    let mut session = engine.new_session()?;
    let run_result = runner.run(engine, &mut session, &plan).await;
    finish_session(session, run_result).await
}

async fn finish_session<T>(mut session: Session, run_result: Result<T>) -> Result<T> {
    let close_result = session.close().await;
    match (run_result, close_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::IndexMode;
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
    fn finish_engine_shuts_down_after_operation_error() {
        let temp = TempDir::new().unwrap();
        let engine =
            smol::block_on(open_engine(temp.path(), LogSyncMode::Fsync)).expect("open engine");

        let err = finish_engine::<()>(&engine, Err(BenchError::message("operation failed")))
            .expect_err("operation error");

        assert_eq!(err.to_string(), "operation failed");
        assert!(engine.new_session().is_err());
    }

    #[test]
    fn collect_session_tasks_sums_successes() {
        let executor = Executor::new();
        let tasks = vec![
            executor.spawn(async {
                Ok(SessionSummary {
                    operations: 2,
                    inserted_rows: 2,
                    found: 1,
                    not_found: 0,
                    rows_returned: 1,
                    failures: 0,
                })
            }),
            executor.spawn(async {
                Ok(SessionSummary {
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
            SessionSummary {
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
}
