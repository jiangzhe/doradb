use crate::cli::{IndexMode, LoadArgs, LoadConfig, PrepareArgs};
use crate::error::{BenchError, Result};
use crate::manifest::{
    KeyRange, Manifest, read_manifest, write_manifest, write_manifest_exclusive,
};
use crate::output::{
    BenchmarkResult, InternalStatsSnapshot, OutputConfig, internal_metrics, write_benchmark_outputs,
};
use crate::workload::{SessionPlan, WorkerPlan, build_worker_plans, generate_keys, payload_bytes};
use doradb_storage::id::TableID;
use doradb_storage::{
    ColumnAttributes, ColumnSpec, Engine, EngineConfig, IndexAttributes, IndexKey, IndexSpec,
    Session, TableSpec, Val, ValKind,
};
use easy_parallel::Parallel;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const MAX_VALUE_SIZE: usize = u16::MAX as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerSummary {
    inserted: u64,
    failures: u64,
}

/// Prepare a benchmark storage root and manifest.
pub async fn prepare(storage_root: PathBuf, args: PrepareArgs) -> Result<()> {
    prepare_storage_root(&storage_root)?;

    let engine = open_engine(&storage_root).await?;
    let mut session = engine.new_session()?;
    let table_id = session
        .create_table(benchmark_table_spec(), benchmark_index_specs(args.index))
        .await?;
    session.close().await?;
    engine.shutdown()?;

    let manifest = Manifest::new(table_id.as_u64(), args.index);
    write_manifest_exclusive(&storage_root, &manifest)?;
    println!(
        "prepared storage_root={} table_id={} index={}",
        storage_root.display(),
        table_id,
        args.index
    );
    Ok(())
}

/// Run the selected benchmark workload.
pub async fn run_load(storage_root: PathBuf, args: LoadArgs, command_context: &str) -> Result<()> {
    let mut manifest = read_manifest(&storage_root)?;
    let config = args.resolve(storage_root, manifest.index)?;
    validate_load_config(&config)?;
    let key_range = manifest.key_range(config.num)?;
    let table_id = TableID::new(manifest.table_id);

    let engine = open_engine(&config.storage_root).await?;
    let mut stats_session = engine.new_session()?;
    let before = InternalStatsSnapshot::capture(&stats_session)?;
    let started = Instant::now();
    let worker_result = run_workers(&engine, &config, table_id, key_range);
    let elapsed = started.elapsed();
    let after = InternalStatsSnapshot::capture(&stats_session)?;
    stats_session.close().await?;
    engine.shutdown()?;

    let summary = worker_result?;

    let metrics = internal_metrics(&before, &after);
    let result = BenchmarkResult::new(summary.inserted, elapsed, summary.failures);
    let output_config = OutputConfig {
        workload: config.workload,
        storage_root: config.storage_root.clone(),
        num: config.num,
        value_size: config.value_size,
        batch_size: config.batch_size,
        rand: config.rand,
        seed: config.seed,
        index: config.index,
        threads: config.threads,
        sessions: config.sessions,
        table_id: manifest.table_id,
    };
    write_benchmark_outputs(&output_config, &metrics, &result, command_context)?;

    manifest.advance_key_range(config.num)?;
    write_manifest(&config.storage_root, &manifest)
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

async fn open_engine(storage_root: &Path) -> Result<Engine> {
    Ok(EngineConfig::default()
        .storage_root(storage_root)
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
    }
}

fn validate_load_config(config: &LoadConfig) -> Result<()> {
    if config.value_size > MAX_VALUE_SIZE {
        return Err(BenchError::message(format!(
            "--value-size must not exceed {MAX_VALUE_SIZE} bytes"
        )));
    }
    if config.batch_size > usize::MAX as u64 {
        return Err(BenchError::message(
            "--batch-size exceeds addressable memory on this platform",
        ));
    }
    Ok(())
}

fn run_workers(
    engine: &Engine,
    config: &LoadConfig,
    table_id: TableID,
    key_range: KeyRange,
) -> Result<WorkerSummary> {
    let plans = build_worker_plans(key_range, config.sessions, config.threads)?;
    let results = Parallel::new()
        .each(plans, |plan| {
            smol::block_on(execute_worker(engine, config, table_id, plan))
        })
        .run();
    let mut summary = WorkerSummary {
        inserted: 0,
        failures: 0,
    };
    for result in results {
        let worker = result?;
        summary.inserted += worker.inserted;
        summary.failures += worker.failures;
    }
    Ok(summary)
}

async fn execute_worker(
    engine: &Engine,
    config: &LoadConfig,
    table_id: TableID,
    plan: WorkerPlan,
) -> Result<WorkerSummary> {
    let _worker_index = plan.worker_index;
    let mut summary = WorkerSummary {
        inserted: 0,
        failures: 0,
    };
    for session_plan in plan.sessions {
        let session_summary = execute_session(engine, config, table_id, &session_plan).await?;
        summary.inserted += session_summary.inserted;
        summary.failures += session_summary.failures;
    }
    Ok(summary)
}

async fn execute_session(
    engine: &Engine,
    config: &LoadConfig,
    table_id: TableID,
    plan: &SessionPlan,
) -> Result<WorkerSummary> {
    let keys = generate_keys(config.rand, config.index, config.seed, plan)?;
    let mut session = engine.new_session()?;
    let load_result = insert_keys(&mut session, config, table_id, &keys).await;
    let close_result = session.close().await;
    match (load_result, close_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(err.into()),
    }
}

async fn insert_keys(
    session: &mut Session,
    config: &LoadConfig,
    table_id: TableID,
    keys: &[u64],
) -> Result<WorkerSummary> {
    if keys.is_empty() {
        return Ok(WorkerSummary {
            inserted: 0,
            failures: 0,
        });
    }
    let batch_size = effective_batch_size(config, keys.len() as u64)?;
    let mut inserted = 0u64;
    for batch in keys.chunks(batch_size) {
        insert_batch(session, table_id, batch, config.seed, config.value_size).await?;
        inserted += batch.len() as u64;
    }
    Ok(WorkerSummary {
        inserted,
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

fn effective_batch_size(config: &LoadConfig, row_count: u64) -> Result<usize> {
    let bounded = config.batch_size.min(row_count.max(1));
    usize::try_from(bounded)
        .map_err(|_| BenchError::message("effective batch size exceeds addressable memory"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Workload;
    use std::fs::File;
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
    fn schema_uses_unique_secondary_index_without_primary_key() {
        let index_specs = benchmark_index_specs(IndexMode::Unique);
        assert_eq!(index_specs.len(), 1);
        assert!(index_specs[0].attributes.contains(IndexAttributes::UK));
        assert!(!index_specs[0].attributes.contains(IndexAttributes::PK));
    }

    #[test]
    fn effective_batch_size_defaults_to_configured_insert_batch_size() {
        let config = LoadConfig {
            storage_root: "root".into(),
            workload: Workload::Insert,
            num: 10,
            value_size: 16,
            batch_size: 1,
            rand: false,
            seed: 0,
            index: IndexMode::None,
            threads: 1,
            sessions: 1,
        };
        assert_eq!(effective_batch_size(&config, 10).unwrap(), 1);
    }

    #[test]
    fn validate_load_config_rejects_value_size_above_row_payload_limit() {
        let config = LoadConfig {
            storage_root: "root".into(),
            workload: Workload::Insert,
            num: 10,
            value_size: MAX_VALUE_SIZE + 1,
            batch_size: 1,
            rand: false,
            seed: 0,
            index: IndexMode::None,
            threads: 1,
            sessions: 1,
        };
        assert!(validate_load_config(&config).is_err());
    }
}
