use crate::cli::{IndexMode, WorkerIterationArgs, Workload};
use crate::error::{BenchError, Result};
use crate::manifest::{KeyRange, Manifest};
use crate::workload::{CommonConfig, SessionPlan, SessionSummary, WorkloadConfig, WorkloadRunner};
use doradb_storage::id::TableID;
use doradb_storage::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, Session, TableSpec, ValKind,
};

/// Resolved table-DDL configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableDdlConfig {
    common: CommonConfig,
    num: u64,
    loaded_range: KeyRange,
}

impl WorkloadConfig for TableDdlConfig {
    type Args = WorkerIterationArgs;

    const WORKLOAD: Workload = Workload::TableDdl;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let num = args.iterations();
        validate_ddl_operation_count(num)?;
        let common = resolve_ddl_common(manifest, args)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            num,
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

/// Executes create/drop table cycles for one session.
#[derive(Clone, Copy)]
pub(crate) struct TableDdlRunner;

impl WorkloadRunner for TableDdlRunner {
    type Config = TableDdlConfig;

    fn new(_config: &Self::Config, _table_id: TableID) -> Self {
        Self
    }

    async fn run(
        &self,
        _engine: &doradb_storage::Engine,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let mut summary = SessionSummary::default();
        for _ in 0..plan.number {
            let table_id = session
                .create_table(benchmark_table_spec(), Vec::new())
                .await?;
            summary.operations += 1;
            session.drop_table(table_id).await?;
            summary.operations += 1;
        }
        Ok(summary)
    }
}

/// Resolved index-DDL configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexDdlConfig {
    common: CommonConfig,
    num: u64,
    loaded_range: KeyRange,
}

impl WorkloadConfig for IndexDdlConfig {
    type Args = WorkerIterationArgs;

    const WORKLOAD: Workload = Workload::IndexDdl;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        let num = args.iterations();
        validate_ddl_operation_count(num)?;
        let common = resolve_ddl_common(manifest, args)?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        Ok(Self {
            common,
            num,
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

/// Executes create/drop index cycles for one session.
#[derive(Clone, Copy)]
pub(crate) struct IndexDdlRunner {
    table_id: TableID,
}

impl WorkloadRunner for IndexDdlRunner {
    type Config = IndexDdlConfig;

    fn new(_config: &Self::Config, table_id: TableID) -> Self {
        Self { table_id }
    }

    async fn run(
        &self,
        _engine: &doradb_storage::Engine,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let mut summary = SessionSummary::default();
        for _ in 0..plan.number {
            let index_no = session
                .create_index(self.table_id, benchmark_non_unique_index_spec())
                .await?;
            summary.operations += 1;
            session.drop_index(self.table_id, index_no).await?;
            summary.operations += 1;
        }
        Ok(summary)
    }
}

/// Build the two-column schema shared by prepared and transient benchmark tables.
pub(crate) fn benchmark_table_spec() -> TableSpec {
    TableSpec::new(vec![
        ColumnSpec::new("logical_key", ValKind::U64, ColumnAttributes::empty()),
        ColumnSpec::new("payload", ValKind::VarByte, ColumnAttributes::empty()),
    ])
}

/// Build the prepared benchmark table's configured secondary-index shape.
pub(crate) fn benchmark_index_specs(index: IndexMode) -> Vec<IndexSpec> {
    match index {
        IndexMode::None => Vec::new(),
        IndexMode::Unique => vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)],
        IndexMode::NonUnique => {
            vec![benchmark_non_unique_index_spec()]
        }
    }
}

fn benchmark_non_unique_index_spec() -> IndexSpec {
    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::empty())
}

fn resolve_ddl_common(manifest: &Manifest, args: &WorkerIterationArgs) -> Result<CommonConfig> {
    let worker = args.worker();
    CommonConfig::resolve(
        &manifest.defaults,
        worker.thread_override(),
        worker.session_override(),
        None,
        None,
        worker.include_stats(),
    )
}

fn validate_ddl_operation_count(cycles: u64) -> Result<()> {
    cycles
        .checked_mul(2)
        .ok_or_else(|| BenchError::message("DDL operation count overflow"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command, WorkloadArgs};
    use clap::Parser;

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
    fn ddl_config_defaults_to_one_iteration() {
        let cli =
            Cli::try_parse_from(["doradb-bench", "--root", "root", "run", "table-ddl"]).unwrap();
        let Command::Run {
            workload: WorkloadArgs::TableDdl(args),
        } = cli.command
        else {
            panic!("expected table-ddl workload");
        };
        let config = TableDdlConfig::resolve(&Manifest::new(1, IndexMode::Unique), &args).unwrap();
        assert_eq!(config.operation_count(), 1);
        assert_eq!(config.loaded_range, KeyRange { start: 0, len: 0 });
    }

    #[test]
    fn ddl_config_rejects_operation_count_overflow() {
        for name in ["table-ddl", "index-ddl"] {
            let cli = Cli::try_parse_from([
                "doradb-bench",
                "--root",
                "root",
                "run",
                name,
                "--num",
                &u64::MAX.to_string(),
            ])
            .unwrap();
            let Command::Run { workload } = cli.command else {
                panic!("expected DDL workload");
            };
            let manifest = Manifest::new(1, IndexMode::None);
            let result = match workload {
                WorkloadArgs::TableDdl(args) => {
                    TableDdlConfig::resolve(&manifest, &args).map(|_| ())
                }
                WorkloadArgs::IndexDdl(args) => {
                    IndexDdlConfig::resolve(&manifest, &args).map(|_| ())
                }
                _ => panic!("expected DDL workload"),
            };
            assert!(result.is_err());
        }
    }

    #[test]
    fn index_ddl_config_enforces_manifest_compatibility() {
        let cli =
            Cli::try_parse_from(["doradb-bench", "--root", "root", "run", "index-ddl"]).unwrap();
        let Command::Run {
            workload: WorkloadArgs::IndexDdl(args),
        } = cli.command
        else {
            panic!("expected index-ddl workload");
        };
        assert!(IndexDdlConfig::resolve(&Manifest::new(1, IndexMode::Unique), &args).is_err());
    }
}
