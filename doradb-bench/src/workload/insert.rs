use crate::cli::{InsertArgs, Workload};
use crate::error::{BenchError, Result};
use crate::fixture::{IndexMode, KeyRange};
use crate::manifest::Manifest;
use crate::measurement::{LatencyDistribution, MeasurementClock};
use crate::workload::util::{effective_batch_size, generate_insert_keys, generate_payload};
use crate::workload::{
    CommonConfig, RunCancellation, SessionPlan, SessionSummary, WorkloadConfig, WorkloadRunner,
};
use doradb_storage::id::{TableID, TrxID};
use doradb_storage::{OperationError, Session, Val};

/// Resolved sequential-insert configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InsertSeqConfig {
    common: CommonConfig,
    index: IndexMode,
    num: u64,
    seed: u64,
    execution_range: KeyRange,
    output_loaded_range: KeyRange,
}

impl WorkloadConfig for InsertSeqConfig {
    type Args = InsertArgs;

    const WORKLOAD: Workload = Workload::InsertSeq;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let (common, num, seed, execution_range, output_loaded_range) =
            resolve_insert_config(manifest, args)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            index: manifest.index,
            num,
            seed,
            execution_range,
            output_loaded_range,
        })
    }

    fn common(&self) -> &CommonConfig {
        &self.common
    }

    fn operation_count(&self) -> u64 {
        self.num
    }

    fn execution_range(&self) -> KeyRange {
        self.execution_range
    }

    fn output_loaded_range(&self) -> KeyRange {
        self.output_loaded_range
    }

    fn seed(&self) -> u64 {
        self.seed
    }

    fn update_manifest(&self, manifest: &mut Manifest, summary: &SessionSummary) -> Result<bool> {
        manifest.record_insert_outcome(self.num, summary.inserted_rows)?;
        Ok(true)
    }
}

/// Executes sequential-key inserts for one session.
#[derive(Clone, Copy)]
pub(crate) struct InsertSeqRunner {
    index: IndexMode,
    seed: u64,
    value_size: usize,
    batch_size: u64,
    table_id: TableID,
}

impl WorkloadRunner for InsertSeqRunner {
    type Config = InsertSeqConfig;

    fn new(config: &Self::Config, table_id: TableID) -> Self {
        Self {
            index: config.index,
            seed: config.seed,
            value_size: config.common.value_size,
            batch_size: config.common.batch_size,
            table_id,
        }
    }

    async fn run(
        &self,
        _engine: &doradb_storage::Engine,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let result = run_insert_operations(
            session,
            InsertOperationSpec {
                table_id: self.table_id,
                random: false,
                index: self.index,
                seed: self.seed,
                value_size: self.value_size,
                batch_size: self.batch_size,
            },
            plan,
            None,
            None,
        )
        .await?;
        Ok(SessionSummary {
            operations: result.operations,
            inserted_rows: result.inserted_rows,
            failures: result
                .duplicate_key
                .checked_add(result.write_conflict)
                .ok_or_else(|| BenchError::message("insert failure overflow"))?,
            ..SessionSummary::default()
        })
    }
}

/// Resolved random-insert configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InsertRandConfig {
    common: CommonConfig,
    index: IndexMode,
    num: u64,
    seed: u64,
    execution_range: KeyRange,
    output_loaded_range: KeyRange,
}

impl WorkloadConfig for InsertRandConfig {
    type Args = InsertArgs;

    const WORKLOAD: Workload = Workload::InsertRand;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let (common, num, seed, execution_range, output_loaded_range) =
            resolve_insert_config(manifest, args)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            index: manifest.index,
            num,
            seed,
            execution_range,
            output_loaded_range,
        })
    }

    fn common(&self) -> &CommonConfig {
        &self.common
    }

    fn operation_count(&self) -> u64 {
        self.num
    }

    fn execution_range(&self) -> KeyRange {
        self.execution_range
    }

    fn output_loaded_range(&self) -> KeyRange {
        self.output_loaded_range
    }

    fn random(&self) -> bool {
        true
    }

    fn seed(&self) -> u64 {
        self.seed
    }

    fn update_manifest(&self, manifest: &mut Manifest, summary: &SessionSummary) -> Result<bool> {
        manifest.record_insert_outcome(self.num, summary.inserted_rows)?;
        Ok(true)
    }
}

/// Executes seeded random-key inserts for one session.
#[derive(Clone, Copy)]
pub(crate) struct InsertRandRunner {
    index: IndexMode,
    seed: u64,
    value_size: usize,
    batch_size: u64,
    table_id: TableID,
}

impl WorkloadRunner for InsertRandRunner {
    type Config = InsertRandConfig;

    fn new(config: &Self::Config, table_id: TableID) -> Self {
        Self {
            index: config.index,
            seed: config.seed,
            value_size: config.common.value_size,
            batch_size: config.common.batch_size,
            table_id,
        }
    }

    async fn run(
        &self,
        _engine: &doradb_storage::Engine,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let result = run_insert_operations(
            session,
            InsertOperationSpec {
                table_id: self.table_id,
                random: true,
                index: self.index,
                seed: self.seed,
                value_size: self.value_size,
                batch_size: self.batch_size,
            },
            plan,
            None,
            None,
        )
        .await?;
        Ok(SessionSummary {
            operations: result.operations,
            inserted_rows: result.inserted_rows,
            failures: result
                .duplicate_key
                .checked_add(result.write_conflict)
                .ok_or_else(|| BenchError::message("insert failure overflow"))?,
            ..SessionSummary::default()
        })
    }
}

/// Result of one session's completely settled insert batches.
pub(crate) struct InsertOperationResult {
    /// Terminal logical insert attempts.
    pub(crate) operations: u64,
    /// Successful row insertions.
    pub(crate) inserted_rows: u64,
    /// Expected duplicate-key outcomes.
    pub(crate) duplicate_key: u64,
    /// Expected write-conflict outcomes.
    pub(crate) write_conflict: u64,
    /// Exact batch-transaction latency samples.
    pub(crate) latency: LatencyDistribution,
    /// Greatest write-bearing batch commit ID.
    pub(crate) latest_write_fence: Option<TrxID>,
}

/// Storage and generation inputs shared by one insert operation core.
#[derive(Clone, Copy)]
pub(crate) struct InsertOperationSpec {
    /// Runtime primary table target.
    pub(crate) table_id: TableID,
    /// Whether keys use the seeded random order.
    pub(crate) random: bool,
    /// Bound primary-table index shape.
    pub(crate) index: IndexMode,
    /// Deterministic payload and key seed.
    pub(crate) seed: u64,
    /// Generated payload bytes.
    pub(crate) value_size: usize,
    /// Maximum operations per transaction.
    pub(crate) batch_size: u64,
}

/// Shared generated-insert core used by legacy and plan dispatch.
pub(crate) async fn run_insert_operations(
    session: &mut Session,
    spec: InsertOperationSpec,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<InsertOperationResult> {
    let keys = generate_insert_keys(spec.random, spec.index, spec.seed, plan)?;
    let mut result = InsertOperationResult {
        operations: 0,
        inserted_rows: 0,
        duplicate_key: 0,
        write_conflict: 0,
        latency: LatencyDistribution::new()?,
        latest_write_fence: None,
    };
    if keys.is_empty() {
        return Ok(result);
    }
    let batch_size = effective_batch_size(spec.batch_size, keys.len() as u64)?;
    for batch in keys.chunks(batch_size) {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let started = clock.map(MeasurementClock::raw);
        let mut trx = session.begin_trx()?;
        let mut batch_inserted = 0u64;
        for key in batch {
            let payload = generate_payload(*key, spec.seed, spec.value_size);
            let row = vec![Val::from(*key), Val::from(&payload[..])];
            match trx
                .exec(async |stmt| stmt.table_insert_mvcc(spec.table_id, row).await.map(|_| ()))
                .await
            {
                Ok(()) => {
                    result.inserted_rows =
                        checked_insert_counter(result.inserted_rows, 1, "inserted row counter")?;
                    batch_inserted =
                        checked_insert_counter(batch_inserted, 1, "batch inserted row counter")?;
                }
                Err(error) => match error.operation_error() {
                    Some(OperationError::DuplicateKey) => {
                        result.duplicate_key = checked_insert_counter(
                            result.duplicate_key,
                            1,
                            "duplicate-key counter",
                        )?;
                    }
                    Some(OperationError::WriteConflict) => {
                        result.write_conflict = checked_insert_counter(
                            result.write_conflict,
                            1,
                            "write-conflict counter",
                        )?;
                    }
                    _ => {
                        let primary = BenchError::from(error);
                        let _ = trx.rollback().await;
                        return Err(primary);
                    }
                },
            }
            result.operations =
                checked_insert_counter(result.operations, 1, "insert operation counter")?;
        }
        let fence = trx.commit().await?;
        if batch_inserted != 0 {
            result.latest_write_fence = Some(
                result
                    .latest_write_fence
                    .map_or(fence, |current| current.max(fence)),
            );
        }
        if let (Some(clock), Some(started)) = (clock, started) {
            let stopped = clock.raw();
            result
                .latency
                .record(clock.raw_delta_nanos(started, stopped)?)?;
        }
    }
    Ok(result)
}

fn resolve_insert_config(
    manifest: &Manifest,
    args: &InsertArgs,
) -> Result<(CommonConfig, u64, u64, KeyRange, KeyRange)> {
    let common_args = args.common();
    let worker = common_args.worker();
    let common = CommonConfig::resolve(
        &manifest.defaults,
        worker.thread_override(),
        worker.session_override(),
        common_args.value_size_override(),
        common_args.batch_size_override(),
        worker.include_stats(),
    )?;
    let num = args.operation_count();
    let execution_range = manifest.key_range(num)?;
    let output_loaded_range = KeyRange {
        start: 0,
        len: execution_range.end()?,
    };
    Ok((
        common,
        num,
        args.seed(),
        execution_range,
        output_loaded_range,
    ))
}

fn checked_insert_counter(current: u64, addition: u64, label: &str) -> Result<u64> {
    current
        .checked_add(addition)
        .ok_or_else(|| BenchError::message(format!("{label} overflow")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command, LogSyncMode, WorkloadArgs};
    use crate::fixture::{benchmark_index_specs, benchmark_table_spec};
    use crate::manifest::DefaultsManifest;
    use clap::Parser;
    use doradb_storage::{Engine, EngineConfig};
    use tempfile::TempDir;

    #[test]
    fn insert_config_inherits_defaults_and_resolves_ranges() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "run",
            "insert-rand",
            "--num",
            "10",
            "--seed",
            "7",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::InsertRand(args),
        } = cli.command.unwrap()
        else {
            panic!("expected insert-rand workload");
        };
        let manifest = Manifest::new_with_defaults(
            1,
            IndexMode::Unique,
            DefaultsManifest::new(2, 4, 256, 8, LogSyncMode::Fsync).unwrap(),
        );
        let config = InsertRandConfig::resolve(&manifest, &args).unwrap();

        assert_eq!(config.common.threads, 2);
        assert_eq!(config.common.sessions, 4);
        assert_eq!(config.common.value_size, 256);
        assert_eq!(config.common.batch_size, 8);
        assert_eq!(config.common.log_sync, LogSyncMode::Fsync);
        assert_eq!(config.execution_range, KeyRange { start: 0, len: 10 });
        assert_eq!(config.output_loaded_range, KeyRange { start: 0, len: 10 });
        assert!(config.random());
        assert_eq!(config.seed(), 7);
    }

    #[test]
    fn insert_config_updates_manifest_after_success() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "run",
            "insert-seq",
            "--num",
            "4",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::InsertSeq(args),
        } = cli.command.unwrap()
        else {
            panic!("expected insert-seq workload");
        };
        let mut manifest = Manifest::new(1, IndexMode::None);
        manifest.record_insert_success(5).unwrap();
        let config = InsertSeqConfig::resolve(&manifest, &args).unwrap();

        assert_eq!(config.execution_range, KeyRange { start: 5, len: 4 });
        assert_eq!(config.output_loaded_range, KeyRange { start: 0, len: 9 });
        assert!(
            config
                .update_manifest(
                    &mut manifest,
                    &SessionSummary {
                        operations: 4,
                        inserted_rows: 4,
                        ..SessionSummary::default()
                    }
                )
                .unwrap()
        );
        assert_eq!(manifest.runtime.next_key, 9);
        assert_eq!(manifest.runtime.rows_inserted, 9);
    }

    #[test]
    fn insert_core_counts_duplicate_key_and_commits_the_reusable_transaction() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(temp.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    benchmark_table_spec(),
                    benchmark_index_specs(IndexMode::Unique),
                )
                .await
                .unwrap();
            let plan = SessionPlan {
                session_index: 0,
                key_start: 10,
                number: 4,
            };
            let keys = generate_insert_keys(true, IndexMode::None, 2, &plan).unwrap();
            assert!(
                keys.iter()
                    .enumerate()
                    .any(|(index, key)| keys[..index].contains(key))
            );

            let result = run_insert_operations(
                &mut session,
                InsertOperationSpec {
                    table_id,
                    random: true,
                    index: IndexMode::None,
                    seed: 2,
                    value_size: 16,
                    batch_size: 4,
                },
                &plan,
                None,
                None,
            )
            .await
            .unwrap();
            assert_eq!(result.operations, 4);
            assert_eq!(result.write_conflict, 0);
            assert!(result.duplicate_key > 0);
            assert_eq!(
                result.operations,
                result.inserted_rows + result.duplicate_key
            );
            assert!(result.latest_write_fence.is_some());
            session.close().await.unwrap();
            engine.shutdown();
        });
    }
}
