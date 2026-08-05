use crate::cli::{IndexMode, InsertArgs, Workload};
use crate::error::Result;
use crate::manifest::{KeyRange, Manifest};
use crate::workload::util::{effective_batch_size, generate_insert_keys, generate_payload};
use crate::workload::{CommonConfig, SessionPlan, SessionSummary, WorkloadConfig, WorkloadRunner};
use doradb_storage::id::TableID;
use doradb_storage::{Session, Val};

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

    fn update_manifest(&self, manifest: &mut Manifest) -> Result<bool> {
        manifest.record_insert_success(self.num)?;
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

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        let keys = generate_insert_keys(false, self.index, self.seed, plan)?;
        insert_keys(
            session,
            self.table_id,
            &keys,
            self.seed,
            self.value_size,
            self.batch_size,
        )
        .await
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

    fn update_manifest(&self, manifest: &mut Manifest) -> Result<bool> {
        manifest.record_insert_success(self.num)?;
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

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        let keys = generate_insert_keys(true, self.index, self.seed, plan)?;
        insert_keys(
            session,
            self.table_id,
            &keys,
            self.seed,
            self.value_size,
            self.batch_size,
        )
        .await
    }
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

async fn insert_keys(
    session: &mut Session,
    table_id: TableID,
    keys: &[u64],
    seed: u64,
    value_size: usize,
    batch_size: u64,
) -> Result<SessionSummary> {
    if keys.is_empty() {
        return Ok(SessionSummary::default());
    }
    let batch_size = effective_batch_size(batch_size, keys.len() as u64)?;
    let mut inserted = 0u64;
    for batch in keys.chunks(batch_size) {
        insert_batch(session, table_id, batch, seed, value_size).await?;
        inserted += batch.len() as u64;
    }
    Ok(SessionSummary {
        operations: inserted,
        inserted_rows: inserted,
        ..SessionSummary::default()
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
        let payload = generate_payload(*key, seed, value_size);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command, LogSyncMode, WorkloadArgs};
    use crate::manifest::DefaultsManifest;
    use clap::Parser;

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
        } = cli.command
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
        } = cli.command
        else {
            panic!("expected insert-seq workload");
        };
        let mut manifest = Manifest::new(1, IndexMode::None);
        manifest.record_insert_success(5).unwrap();
        let config = InsertSeqConfig::resolve(&manifest, &args).unwrap();

        assert_eq!(config.execution_range, KeyRange { start: 5, len: 4 });
        assert_eq!(config.output_loaded_range, KeyRange { start: 0, len: 9 });
        assert!(config.update_manifest(&mut manifest).unwrap());
        assert_eq!(manifest.runtime.next_key, 9);
        assert_eq!(manifest.runtime.rows_inserted, 9);
    }
}
