use crate::cli::{ReadArgs, SeededReadArgs, WorkerIterationArgs, Workload, validate_batch_size};
use crate::error::{BenchError, Result};
use crate::manifest::{KeyRange, Manifest};
use crate::workload::util::{
    effective_batch_size, generate_random_read_keys, generate_sequential_read_keys,
};
use crate::workload::{CommonConfig, SessionPlan, SessionSummary, WorkloadConfig, WorkloadRunner};
use doradb_storage::id::TableID;
use doradb_storage::{Error as StorageError, SelectKey, SelectMvcc, Session, Val};

/// Resolved sequential-lookup configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LookupSeqConfig {
    common: CommonConfig,
    num: u64,
    loaded_range: KeyRange,
}

impl WorkloadConfig for LookupSeqConfig {
    type Args = ReadArgs;

    const WORKLOAD: Workload = Workload::LookupSeq;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let common = resolve_read_common(manifest, args)?;
        let num = required_operation_count(args, Self::WORKLOAD)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            num,
            loaded_range: manifest.loaded_key_range()?,
        })
    }

    fn common(&self) -> &CommonConfig {
        &self.common
    }

    fn operation_count(&self) -> u64 {
        self.num
    }

    fn output_loaded_range(&self) -> KeyRange {
        self.loaded_range
    }
}

/// Executes sequential unique-index lookups for one session.
#[derive(Clone, Copy)]
pub(crate) struct LookupSeqRunner {
    loaded_range: KeyRange,
    batch_size: u64,
    table_id: TableID,
}

impl WorkloadRunner for LookupSeqRunner {
    type Config = LookupSeqConfig;

    fn new(config: &Self::Config, table_id: TableID) -> Self {
        Self {
            loaded_range: config.loaded_range,
            batch_size: config.common.batch_size,
            table_id,
        }
    }

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        let keys = generate_sequential_read_keys(self.loaded_range, plan)?;
        lookup_keys(session, self.batch_size, self.table_id, &keys).await
    }
}

/// Resolved random-lookup configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LookupRandConfig {
    common: CommonConfig,
    num: u64,
    seed: u64,
    loaded_range: KeyRange,
}

impl WorkloadConfig for LookupRandConfig {
    type Args = SeededReadArgs;

    const WORKLOAD: Workload = Workload::LookupRand;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let common = resolve_read_common(manifest, args.read())?;
        let num = required_operation_count(args.read(), Self::WORKLOAD)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            num,
            seed: args.seed(),
            loaded_range: manifest.loaded_key_range()?,
        })
    }

    fn common(&self) -> &CommonConfig {
        &self.common
    }

    fn operation_count(&self) -> u64 {
        self.num
    }

    fn output_loaded_range(&self) -> KeyRange {
        self.loaded_range
    }

    fn random(&self) -> bool {
        true
    }

    fn seed(&self) -> u64 {
        self.seed
    }
}

/// Executes seeded random unique-index lookups for one session.
#[derive(Clone, Copy)]
pub(crate) struct LookupRandRunner {
    seed: u64,
    loaded_range: KeyRange,
    batch_size: u64,
    table_id: TableID,
}

impl WorkloadRunner for LookupRandRunner {
    type Config = LookupRandConfig;

    fn new(config: &Self::Config, table_id: TableID) -> Self {
        Self {
            seed: config.seed,
            loaded_range: config.loaded_range,
            batch_size: config.common.batch_size,
            table_id,
        }
    }

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        let keys = generate_random_read_keys(self.seed, self.loaded_range, plan)?;
        lookup_keys(session, self.batch_size, self.table_id, &keys).await
    }
}

/// Resolved table-scan configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableScanConfig {
    common: CommonConfig,
    num: u64,
    loaded_range: KeyRange,
}

impl WorkloadConfig for TableScanConfig {
    type Args = ReadArgs;

    const WORKLOAD: Workload = Workload::TableScan;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let common = resolve_read_common(manifest, args)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            num: args.operation_count().unwrap_or(1),
            loaded_range: manifest.loaded_key_range()?,
        })
    }

    fn common(&self) -> &CommonConfig {
        &self.common
    }

    fn operation_count(&self) -> u64 {
        self.num
    }

    fn output_loaded_range(&self) -> KeyRange {
        self.loaded_range
    }
}

/// Executes full table-scan iterations for one session.
#[derive(Clone, Copy)]
pub(crate) struct TableScanRunner {
    batch_size: u64,
    table_id: TableID,
}

impl WorkloadRunner for TableScanRunner {
    type Config = TableScanConfig;

    fn new(config: &Self::Config, table_id: TableID) -> Self {
        Self {
            batch_size: config.common.batch_size,
            table_id,
        }
    }

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        table_scan_iterations(session, self.batch_size, self.table_id, plan.rows).await
    }
}

/// Resolved exact-key index-scan configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexScanConfig {
    common: CommonConfig,
    num: u64,
    seed: u64,
    loaded_range: KeyRange,
}

impl WorkloadConfig for IndexScanConfig {
    type Args = SeededReadArgs;

    const WORKLOAD: Workload = Workload::IndexScan;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let common = resolve_read_common(manifest, args.read())?;
        let num = required_operation_count(args.read(), Self::WORKLOAD)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            num,
            seed: args.seed(),
            loaded_range: manifest.loaded_key_range()?,
        })
    }

    fn common(&self) -> &CommonConfig {
        &self.common
    }

    fn operation_count(&self) -> u64 {
        self.num
    }

    fn output_loaded_range(&self) -> KeyRange {
        self.loaded_range
    }

    fn random(&self) -> bool {
        true
    }

    fn seed(&self) -> u64 {
        self.seed
    }
}

/// Executes seeded exact-key non-unique index scans for one session.
#[derive(Clone, Copy)]
pub(crate) struct IndexScanRunner {
    seed: u64,
    loaded_range: KeyRange,
    batch_size: u64,
    table_id: TableID,
}

impl WorkloadRunner for IndexScanRunner {
    type Config = IndexScanConfig;

    fn new(config: &Self::Config, table_id: TableID) -> Self {
        Self {
            seed: config.seed,
            loaded_range: config.loaded_range,
            batch_size: config.common.batch_size,
            table_id,
        }
    }

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        let keys = generate_random_read_keys(self.seed, self.loaded_range, plan)?;
        index_scan_keys(session, self.batch_size, self.table_id, &keys).await
    }
}

/// Resolved public index-stream configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexStreamConfig {
    common: CommonConfig,
    num: u64,
    loaded_range: KeyRange,
}

impl WorkloadConfig for IndexStreamConfig {
    type Args = WorkerIterationArgs;

    const WORKLOAD: Workload = Workload::IndexStream;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let common = resolve_worker_common(manifest, args)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            num: args.iterations(),
            loaded_range: manifest.loaded_key_range()?,
        })
    }

    fn common(&self) -> &CommonConfig {
        &self.common
    }

    fn operation_count(&self) -> u64 {
        self.num
    }

    fn output_loaded_range(&self) -> KeyRange {
        self.loaded_range
    }
}

/// Executes full public index-stream iterations for one session.
#[derive(Clone, Copy)]
pub(crate) struct IndexStreamRunner {
    table_id: TableID,
}

impl WorkloadRunner for IndexStreamRunner {
    type Config = IndexStreamConfig;

    fn new(_config: &Self::Config, table_id: TableID) -> Self {
        Self { table_id }
    }

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        index_stream_iterations(session, self.table_id, plan.rows).await
    }
}

fn resolve_read_common(manifest: &Manifest, args: &ReadArgs) -> Result<CommonConfig> {
    let common_args = args.common();
    let worker = common_args.worker();
    CommonConfig::resolve(
        &manifest.defaults,
        worker.thread_override(),
        worker.session_override(),
        None,
        common_args.batch_size_override(),
        worker.log_sync(),
        worker.include_stats(),
    )
}

fn resolve_worker_common(manifest: &Manifest, args: &WorkerIterationArgs) -> Result<CommonConfig> {
    let worker = args.worker();
    CommonConfig::resolve(
        &manifest.defaults,
        worker.thread_override(),
        worker.session_override(),
        None,
        None,
        worker.log_sync(),
        worker.include_stats(),
    )
}

fn required_operation_count(args: &ReadArgs, workload: Workload) -> Result<u64> {
    args.operation_count()
        .ok_or_else(|| BenchError::message(format!("{workload} workload requires --num")))
}

async fn lookup_keys(
    session: &mut Session,
    batch_size: u64,
    table_id: TableID,
    keys: &[u64],
) -> Result<SessionSummary> {
    if keys.is_empty() {
        return Ok(SessionSummary::default());
    }
    let batch_size = effective_batch_size(batch_size, keys.len() as u64)?;
    let mut summary = SessionSummary::default();
    for batch in keys.chunks(batch_size) {
        summary.merge(lookup_key_batch(session, table_id, batch).await?);
    }
    Ok(summary)
}

async fn lookup_key_batch(
    session: &mut Session,
    table_id: TableID,
    keys: &[u64],
) -> Result<SessionSummary> {
    let mut trx = session.begin_trx()?;
    let mut summary = SessionSummary::default();
    for key in keys {
        let select_key = SelectKey::new(0, vec![Val::from(*key)]);
        let lookup = trx
            .exec(async |stmt| {
                stmt.table_lookup_unique_mvcc(
                    table_id,
                    select_key.index_no,
                    &select_key.vals,
                    &[0, 1],
                )
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
) -> Result<SessionSummary> {
    validate_batch_size(batch_size)?;
    if iterations == 0 {
        return Ok(SessionSummary::default());
    }
    let mut remaining = iterations;
    let mut summary = SessionSummary::default();
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
) -> Result<SessionSummary> {
    let mut trx = session.begin_trx()?;
    let mut summary = SessionSummary::default();
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
) -> Result<SessionSummary> {
    if keys.is_empty() {
        return Ok(SessionSummary::default());
    }
    let batch_size = effective_batch_size(batch_size, keys.len() as u64)?;
    let mut summary = SessionSummary::default();
    for batch in keys.chunks(batch_size) {
        summary.merge(index_scan_key_batch(session, table_id, batch).await?);
    }
    Ok(summary)
}

async fn index_scan_key_batch(
    session: &mut Session,
    table_id: TableID,
    keys: &[u64],
) -> Result<SessionSummary> {
    let mut trx = session.begin_trx()?;
    let mut summary = SessionSummary::default();
    for key in keys {
        let select_key = SelectKey::new(0, vec![Val::from(*key)]);
        let scan = trx
            .exec(async |stmt| {
                stmt.table_index_lookup_mvcc(
                    table_id,
                    select_key.index_no,
                    &select_key.vals,
                    &[0, 1],
                )
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

async fn index_stream_iterations(
    session: &mut Session,
    table_id: TableID,
    iterations: u64,
) -> Result<SessionSummary> {
    let mut summary = SessionSummary::default();
    for _ in 0..iterations {
        let mut trx = session.begin_trx()?;
        let scan_result = async {
            let mut stream = trx
                .stream_stmt()
                .table_index_scan_mvcc(table_id, 0, .., &[0, 1])
                .await?;
            let mut rows = 0u64;
            while stream.next().await?.is_some() {
                rows += 1;
            }
            Ok::<u64, StorageError>(rows)
        }
        .await;
        let rows = match scan_result {
            Ok(rows) => rows,
            Err(err) => {
                trx.rollback().await?;
                return Err(err.into());
            }
        };
        trx.commit().await?;
        summary.operations += 1;
        summary.rows_returned += rows;
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command, IndexMode, WorkloadArgs};
    use crate::manifest::DefaultsManifest;
    use clap::Parser;

    fn loaded_manifest(index: IndexMode, batch_size: u64) -> Manifest {
        let mut manifest = Manifest::new_with_defaults(
            1,
            index,
            DefaultsManifest::new(2, 4, 128, batch_size).unwrap(),
        );
        manifest.record_insert_success(3).unwrap();
        manifest
    }

    #[test]
    fn lookup_requires_operation_count() {
        let cli =
            Cli::try_parse_from(["doradb-bench", "--root", "root", "run", "lookup-seq"]).unwrap();
        let Command::Run {
            workload: WorkloadArgs::LookupSeq(args),
        } = cli.command
        else {
            panic!("expected lookup-seq workload");
        };
        assert!(LookupSeqConfig::resolve(&loaded_manifest(IndexMode::Unique, 1), &args).is_err());
    }

    #[test]
    fn table_scan_defaults_to_one_and_inherits_batch_size() {
        let cli =
            Cli::try_parse_from(["doradb-bench", "--root", "root", "run", "table-scan"]).unwrap();
        let Command::Run {
            workload: WorkloadArgs::TableScan(args),
        } = cli.command
        else {
            panic!("expected table-scan workload");
        };
        let config = TableScanConfig::resolve(&loaded_manifest(IndexMode::None, 6), &args).unwrap();
        assert_eq!(config.operation_count(), 1);
        assert_eq!(config.common.batch_size, 6);
        assert_eq!(config.loaded_range, KeyRange { start: 0, len: 3 });
    }

    #[test]
    fn seeded_read_config_resolves_seed_and_overrides() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "run",
            "lookup-rand",
            "--num",
            "5",
            "--seed",
            "7",
            "--batch-size",
            "2",
            "--sessions",
            "3",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::LookupRand(args),
        } = cli.command
        else {
            panic!("expected lookup-rand workload");
        };
        let config =
            LookupRandConfig::resolve(&loaded_manifest(IndexMode::Unique, 1), &args).unwrap();
        assert_eq!(config.operation_count(), 5);
        assert_eq!(config.common.threads, 2);
        assert_eq!(config.common.sessions, 3);
        assert_eq!(config.common.batch_size, 2);
        assert!(config.random());
        assert_eq!(config.seed(), 7);
    }

    #[test]
    fn index_stream_defaults_to_one_iteration() {
        let cli =
            Cli::try_parse_from(["doradb-bench", "--root", "root", "run", "index-stream"]).unwrap();
        let Command::Run {
            workload: WorkloadArgs::IndexStream(args),
        } = cli.command
        else {
            panic!("expected index-stream workload");
        };
        let config =
            IndexStreamConfig::resolve(&loaded_manifest(IndexMode::Unique, 1), &args).unwrap();
        assert_eq!(config.operation_count(), 1);
        assert_eq!(config.loaded_range, KeyRange { start: 0, len: 3 });
    }

    #[test]
    fn index_scan_config_enforces_manifest_compatibility() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "run",
            "index-scan",
            "--num",
            "1",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::IndexScan(args),
        } = cli.command
        else {
            panic!("expected index-scan workload");
        };
        assert!(IndexScanConfig::resolve(&loaded_manifest(IndexMode::Unique, 1), &args).is_err());
    }
}
