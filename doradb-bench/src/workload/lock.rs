use crate::cli::{LockTableArgs, TableLockScope, Workload};
use crate::error::{BenchError, Result};
use crate::manifest::{KeyRange, Manifest};
use crate::workload::util::RandomTableIndexGenerator;
use crate::workload::{CommonConfig, SessionPlan, SessionSummary, WorkloadConfig, WorkloadRunner};
use doradb_storage::id::TableID;
use doradb_storage::{Session, TableLockMode};
use std::sync::Arc;

/// Resolved explicit table-lock workload configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LockTableConfig {
    common: CommonConfig,
    num: u64,
    scope: TableLockScope,
    unlock: bool,
    random: bool,
    seed: u64,
    table_ids: Arc<[TableID]>,
    loaded_range: KeyRange,
}

impl WorkloadConfig for LockTableConfig {
    type Args = LockTableArgs;

    const WORKLOAD: Workload = Workload::LockTable;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        validate_lock_options(args.unlock(), args.random(), args.explicit_seed())?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        let worker = args.worker();
        let common = CommonConfig::resolve(
            &manifest.defaults,
            worker.thread_override(),
            worker.session_override(),
            None,
            None,
            worker.include_stats(),
        )?;
        let table_ids = manifest
            .table_ids()
            .into_iter()
            .map(TableID::new)
            .collect::<Vec<_>>();
        if table_ids.is_empty() {
            return Err(BenchError::message(
                "lock-table workload requires at least one prepared table",
            ));
        }
        Ok(Self {
            common,
            num: args.operation_count(),
            scope: args.scope(),
            unlock: args.unlock(),
            random: args.random(),
            seed: args.seed(),
            table_ids: table_ids.into(),
            loaded_range: manifest.allocated_key_range(),
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
        self.random
    }

    fn seed(&self) -> u64 {
        self.seed
    }

    fn lock_scope(&self) -> Option<TableLockScope> {
        Some(self.scope)
    }

    fn unlock(&self) -> Option<bool> {
        Some(self.unlock)
    }

    fn prepared_table_count(&self) -> Option<usize> {
        Some(self.table_ids.len())
    }
}

/// Executes explicit shared table-lock acquisitions for one public session.
#[derive(Clone)]
pub(crate) struct LockTableRunner {
    scope: TableLockScope,
    unlock: bool,
    random: bool,
    seed: u64,
    table_ids: Arc<[TableID]>,
}

impl WorkloadRunner for LockTableRunner {
    type Config = LockTableConfig;

    fn new(config: &Self::Config, _table_id: TableID) -> Self {
        Self {
            scope: config.scope,
            unlock: config.unlock,
            random: config.random,
            seed: config.seed,
            table_ids: Arc::clone(&config.table_ids),
        }
    }

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        if plan.number == 0 {
            return Ok(SessionSummary::default());
        }
        match (self.scope, self.unlock) {
            (TableLockScope::Session, false) => self.run_session_retained(session, plan).await,
            (TableLockScope::Session, true) => self.run_session_paired(session, plan).await,
            (TableLockScope::Transaction, false) => {
                self.run_transaction_retained(session, plan).await
            }
            (TableLockScope::Transaction, true) => self.run_transaction_paired(session, plan).await,
        }
    }
}

impl LockTableRunner {
    async fn run_session_retained(
        &self,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let table_id = self.stable_table(plan)?;
        for _ in 0..plan.number {
            session.lock_table(table_id, TableLockMode::Shared).await?;
        }
        Ok(completed_summary(plan.number))
    }

    async fn run_session_paired(
        &self,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let stable_table = self.stable_table(plan)?;
        let mut random = self.random_generator(plan)?;
        let mut operations = 0;
        for _ in 0..plan.number {
            let table_id = self.next_table(stable_table, random.as_mut())?;
            session.lock_table(table_id, TableLockMode::Shared).await?;
            session.unlock_table(table_id)?;
            operations += 1;
        }
        Ok(completed_summary(operations))
    }

    async fn run_transaction_retained(
        &self,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let table_id = self.stable_table(plan)?;
        let mut trx = session.begin_trx()?;
        for _ in 0..plan.number {
            if let Err(err) = trx.lock_table(table_id, TableLockMode::Shared).await {
                trx.rollback().await?;
                return Err(err.into());
            }
        }
        trx.commit().await?;
        Ok(completed_summary(plan.number))
    }

    async fn run_transaction_paired(
        &self,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let stable_table = self.stable_table(plan)?;
        let mut random = self.random_generator(plan)?;
        let mut operations = 0;
        for _ in 0..plan.number {
            let table_id = self.next_table(stable_table, random.as_mut())?;
            let mut trx = session.begin_trx()?;
            if let Err(err) = trx.lock_table(table_id, TableLockMode::Shared).await {
                trx.rollback().await?;
                return Err(err.into());
            }
            trx.commit().await?;
            operations += 1;
        }
        Ok(completed_summary(operations))
    }

    fn stable_table(&self, plan: &SessionPlan) -> Result<TableID> {
        let table_count = self.table_ids.len();
        if table_count == 0 {
            return Err(BenchError::message(
                "lock-table workload requires at least one prepared table",
            ));
        }
        let table_index = plan.session_index % table_count;
        self.table_ids
            .get(table_index)
            .copied()
            .ok_or_else(|| BenchError::message("prepared table selection is out of bounds"))
    }

    fn random_generator(&self, plan: &SessionPlan) -> Result<Option<RandomTableIndexGenerator>> {
        self.random
            .then(|| RandomTableIndexGenerator::new(self.seed, self.table_ids.len(), plan))
            .transpose()
    }

    fn next_table(
        &self,
        stable_table: TableID,
        random: Option<&mut RandomTableIndexGenerator>,
    ) -> Result<TableID> {
        let Some(random) = random else {
            return Ok(stable_table);
        };
        self.table_ids
            .get(random.next_index())
            .copied()
            .ok_or_else(|| BenchError::message("random prepared table selection is out of bounds"))
    }
}

fn validate_lock_options(unlock: bool, random: bool, explicit_seed: Option<u64>) -> Result<()> {
    if random && !unlock {
        return Err(BenchError::message(
            "lock-table --rand requires paired release with --unlock",
        ));
    }
    if explicit_seed.is_some() && !random {
        return Err(BenchError::message(
            "lock-table --seed requires random selection with --rand",
        ));
    }
    Ok(())
}

fn completed_summary(operations: u64) -> SessionSummary {
    SessionSummary {
        operations,
        ..SessionSummary::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command, IndexMode, LogSyncMode, WorkloadArgs};
    use crate::manifest::DefaultsManifest;
    use clap::Parser;

    #[test]
    fn lock_config_resolves_manifest_pool_and_controls() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "run",
            "lock-table",
            "--num",
            "7",
            "--scope",
            "transaction",
            "--unlock",
            "--rand",
            "--seed",
            "11",
            "--threads",
            "2",
            "--sessions",
            "3",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::LockTable(args),
        } = cli.command
        else {
            panic!("expected lock-table workload");
        };
        let defaults = DefaultsManifest::new(1, 1, 128, 1, LogSyncMode::Fdatasync).unwrap();
        let manifest = Manifest::new_with_tables(1, vec![3, 5], IndexMode::None, defaults);
        let config = LockTableConfig::resolve(&manifest, &args).unwrap();

        assert_eq!(config.operation_count(), 7);
        assert_eq!(config.scope, TableLockScope::Transaction);
        assert!(config.unlock);
        assert!(config.random);
        assert_eq!(config.seed, 11);
        assert_eq!(
            &*config.table_ids,
            &[TableID::new(1), TableID::new(3), TableID::new(5)]
        );
        assert_eq!(config.common.threads, 2);
        assert_eq!(config.common.sessions, 3);
        assert_eq!(config.common.log_sync, LogSyncMode::Fdatasync);
        assert_eq!(config.loaded_range, KeyRange { start: 0, len: 0 });
    }

    #[test]
    fn lock_options_repeat_cli_dependency_validation() {
        assert!(validate_lock_options(false, true, None).is_err());
        assert!(validate_lock_options(true, false, Some(1)).is_err());
        assert!(validate_lock_options(true, true, None).is_ok());
        assert!(validate_lock_options(true, true, Some(1)).is_ok());
    }

    #[test]
    fn stable_table_selection_uses_session_modulo_pool_size() {
        let runner = LockTableRunner {
            scope: TableLockScope::Session,
            unlock: false,
            random: false,
            seed: 0,
            table_ids: vec![TableID::new(2), TableID::new(4)].into(),
        };
        for session_index in 0..6 {
            let plan = SessionPlan {
                session_index,
                key_start: session_index as u64,
                number: 1,
            };
            assert_eq!(
                runner.stable_table(&plan).unwrap(),
                runner.table_ids[session_index % 2]
            );
        }
    }
}
