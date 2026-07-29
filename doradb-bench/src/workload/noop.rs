use crate::cli::{WorkerCountArgs, Workload};
use crate::error::Result;
use crate::manifest::{KeyRange, Manifest};
use crate::workload::{CommonConfig, SessionPlan, SessionSummary, WorkloadConfig, WorkloadRunner};
use doradb_storage::id::TableID;
use doradb_storage::{Error as StorageError, Session};

/// Resolved statement-noop configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StmtNoopConfig {
    common: CommonConfig,
    num: u64,
    loaded_range: KeyRange,
}

impl WorkloadConfig for StmtNoopConfig {
    type Args = WorkerCountArgs;

    const WORKLOAD: Workload = Workload::StmtNoop;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let common = resolve_noop_common(manifest, args)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            num: args.operation_count(),
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
}

/// Executes no-op statements inside one transaction for one session.
#[derive(Clone, Copy)]
pub(crate) struct StmtNoopRunner;

impl WorkloadRunner for StmtNoopRunner {
    type Config = StmtNoopConfig;

    fn new(_config: &Self::Config, _table_id: TableID) -> Self {
        Self
    }

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        if plan.rows == 0 {
            return Ok(SessionSummary::default());
        }
        let mut trx = session.begin_trx()?;
        for _ in 0..plan.rows {
            if let Err(err) = trx.exec(async |_stmt| Ok::<(), StorageError>(())).await {
                trx.rollback().await?;
                return Err(err.into());
            }
        }
        trx.commit().await?;
        Ok(SessionSummary {
            operations: plan.rows,
            ..SessionSummary::default()
        })
    }
}

/// Resolved transaction-noop configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TrxNoopConfig {
    common: CommonConfig,
    num: u64,
    loaded_range: KeyRange,
}

impl WorkloadConfig for TrxNoopConfig {
    type Args = WorkerCountArgs;

    const WORKLOAD: Workload = Workload::TrxNoop;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let common = resolve_noop_common(manifest, args)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            num: args.operation_count(),
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
}

/// Executes no-effect transaction cycles for one session.
#[derive(Clone, Copy)]
pub(crate) struct TrxNoopRunner;

impl WorkloadRunner for TrxNoopRunner {
    type Config = TrxNoopConfig;

    fn new(_config: &Self::Config, _table_id: TableID) -> Self {
        Self
    }

    async fn run(&self, session: &mut Session, plan: &SessionPlan) -> Result<SessionSummary> {
        for _ in 0..plan.rows {
            session.begin_trx()?.commit().await?;
        }
        Ok(SessionSummary {
            operations: plan.rows,
            ..SessionSummary::default()
        })
    }
}

fn resolve_noop_common(manifest: &Manifest, args: &WorkerCountArgs) -> Result<CommonConfig> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command, IndexMode, LogSyncMode, WorkloadArgs};
    use clap::Parser;

    #[test]
    fn noop_config_resolves_worker_controls() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "run",
            "stmt-noop",
            "--num",
            "3",
            "--threads",
            "1",
            "--sessions",
            "2",
            "--log-sync",
            "none",
            "--include-stats",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::StmtNoop(args),
        } = cli.command
        else {
            panic!("expected stmt-noop workload");
        };
        let config = StmtNoopConfig::resolve(&Manifest::new(1, IndexMode::None), &args).unwrap();
        assert_eq!(config.operation_count(), 3);
        assert_eq!(config.common.threads, 1);
        assert_eq!(config.common.sessions, 2);
        assert_eq!(config.common.log_sync, LogSyncMode::None);
        assert!(config.common.include_stats);
        assert_eq!(config.loaded_range, KeyRange { start: 0, len: 0 });
    }
}
