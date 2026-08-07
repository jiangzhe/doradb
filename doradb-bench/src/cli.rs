use crate::error::{BenchError, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use doradb_storage::LogSync;
use serde::{Deserialize, Serialize};
use std::env::var_os;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;

const ROOT_ENV_VAR: &str = "DORADB_BENCH_ROOT";
pub(super) const DEFAULT_VALUE_SIZE: usize = 128;
pub(super) const DEFAULT_BATCH_SIZE: u64 = 1;
pub(super) const MAX_VALUE_SIZE: usize = u16::MAX as usize;

/// Top-level DoraDB benchmark command line parser.
#[derive(Debug, Parser)]
#[command(
    name = "doradb-bench",
    about = "DoraDB-native storage benchmark tool",
    disable_help_subcommand = true
)]
pub struct Cli {
    /// DoraDB storage root; falls back to DORADB_BENCH_ROOT.
    #[arg(long = "root", short = 'r', global = true, value_name = "STORAGE_ROOT")]
    root: Option<PathBuf>,
    /// Lifecycle command to execute.
    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    /// Resolve the benchmark storage root from CLI arguments or the environment.
    pub fn resolve_root_from_env(&self) -> Result<PathBuf> {
        self.resolve_root_with_env(var_os(ROOT_ENV_VAR).map(PathBuf::from))
    }

    fn resolve_root_with_env(&self, env_root: Option<PathBuf>) -> Result<PathBuf> {
        if let Some(root) = &self.root {
            return Ok(root.clone());
        }
        if let Some(root) = env_root.filter(|root| !root.as_os_str().is_empty()) {
            return Ok(root);
        }
        Err(BenchError::message(format!(
            "--root is required when {ROOT_ENV_VAR} is not set"
        )))
    }
}

/// Supported top-level benchmark commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Prepare an empty benchmark storage root and manifest.
    Prepare(PrepareArgs),
    /// Run a measured workload and write benchmark results.
    Run {
        #[command(subcommand)]
        workload: WorkloadArgs,
    },
    /// Remove the prepared benchmark storage root.
    Cleanup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
pub(super) enum IndexMode {
    #[serde(rename = "none")]
    #[value(name = "none")]
    None,
    #[serde(rename = "unique")]
    #[value(name = "unique")]
    Unique,
    #[serde(rename = "non-unique")]
    #[value(name = "non-unique")]
    NonUnique,
}

impl fmt::Display for IndexMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Unique => f.write_str("unique"),
            Self::NonUnique => f.write_str("non-unique"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
pub(super) enum LogSyncMode {
    #[default]
    #[serde(rename = "fsync")]
    #[value(name = "fsync")]
    Fsync,
    #[serde(rename = "fdatasync")]
    #[value(name = "fdatasync")]
    Fdatasync,
    #[serde(rename = "none")]
    #[value(name = "none")]
    None,
}

impl LogSyncMode {
    #[inline]
    pub(super) fn as_storage(self) -> LogSync {
        match self {
            Self::Fsync => LogSync::Fsync,
            Self::Fdatasync => LogSync::Fdatasync,
            Self::None => LogSync::None,
        }
    }
}

impl fmt::Display for LogSyncMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fsync => f.write_str("fsync"),
            Self::Fdatasync => f.write_str("fdatasync"),
            Self::None => f.write_str("none"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Workload {
    InsertSeq,
    InsertRand,
    LookupSeq,
    LookupRand,
    TableScan,
    IndexScan,
    StmtNoop,
    TrxNoop,
    IndexStream,
    TableDdl,
    IndexDdl,
    LockTable,
}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsertSeq => f.write_str("insert-seq"),
            Self::InsertRand => f.write_str("insert-rand"),
            Self::LookupSeq => f.write_str("lookup-seq"),
            Self::LookupRand => f.write_str("lookup-rand"),
            Self::TableScan => f.write_str("table-scan"),
            Self::IndexScan => f.write_str("index-scan"),
            Self::StmtNoop => f.write_str("stmt-noop"),
            Self::TrxNoop => f.write_str("trx-noop"),
            Self::IndexStream => f.write_str("index-stream"),
            Self::TableDdl => f.write_str("table-ddl"),
            Self::IndexDdl => f.write_str("index-ddl"),
            Self::LockTable => f.write_str("lock-table"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub(super) enum TableLockScope {
    #[default]
    #[value(name = "session")]
    Session,
    #[value(name = "transaction")]
    Transaction,
}

impl fmt::Display for TableLockScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session => f.write_str("session"),
            Self::Transaction => f.write_str("transaction"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub(super) enum LockTableScenario {
    #[default]
    Basic,
    NestedCovered,
    Convert,
    Enqueue,
    CancelHead,
    CancelMiddle,
    CancelTail,
    Promote,
    Handoff,
    ScopeClose,
}

impl fmt::Display for LockTableScenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Basic => f.write_str("basic"),
            Self::NestedCovered => f.write_str("nested-covered"),
            Self::Convert => f.write_str("convert"),
            Self::Enqueue => f.write_str("enqueue"),
            Self::CancelHead => f.write_str("cancel-head"),
            Self::CancelMiddle => f.write_str("cancel-middle"),
            Self::CancelTail => f.write_str("cancel-tail"),
            Self::Promote => f.write_str("promote"),
            Self::Handoff => f.write_str("handoff"),
            Self::ScopeClose => f.write_str("scope-close"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub(super) enum LockTableMode {
    #[default]
    Shared,
    Exclusive,
}

impl fmt::Display for LockTableMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shared => f.write_str("shared"),
            Self::Exclusive => f.write_str("exclusive"),
        }
    }
}

/// Arguments for preparing a benchmark storage root.
#[derive(Clone, Debug, Args)]
pub struct PrepareArgs {
    /// Benchmark table index shape.
    #[arg(long, short = 'i', value_enum, default_value_t = IndexMode::None)]
    pub(super) index: IndexMode,
    /// Number of ordinary benchmark tables to prepare.
    #[arg(long, default_value = "1")]
    pub(super) tables: NonZeroUsize,
    /// Redo-log durability sync method persisted for later runs.
    #[arg(long, value_enum, default_value_t = LogSyncMode::Fsync)]
    pub(super) log_sync: LogSyncMode,
    /// Default operating-system worker threads for later runs.
    #[arg(long, short = 't', default_value = "1")]
    pub(super) threads: NonZeroUsize,
    /// Default independent DoraDB public sessions for later runs.
    #[arg(long, short = 's')]
    pub(super) sessions: Option<NonZeroUsize>,
    /// Default generated payload size in bytes for later insert runs.
    #[arg(long, short = 'v', default_value = "128")]
    pub(super) value_size: NonZeroUsize,
    /// Default operations per transaction for later runs.
    #[arg(long, short = 'b', default_value = "1")]
    pub(super) batch_size: NonZeroU64,
}

/// Arguments for measured benchmark workloads.
#[derive(Clone, Debug, Subcommand)]
pub enum WorkloadArgs {
    /// Insert generated rows with sequential logical keys.
    InsertSeq(InsertArgs),
    /// Insert generated rows with pseudo-random logical keys.
    InsertRand(InsertArgs),
    /// Run unique-index point lookups over loaded keys in sequential order.
    LookupSeq(ReadArgs),
    /// Run unique-index point lookups over loaded keys in seeded random order.
    LookupRand(SeededReadArgs),
    /// Run full table-scan iterations over visible rows.
    TableScan(ReadArgs),
    /// Run bounded secondary-index scans over loaded keys.
    IndexScan(IndexScanArgs),
    /// Execute no-op statements inside one transaction per nonempty session.
    StmtNoop(WorkerCountArgs),
    /// Begin and commit transactions without executing statements.
    TrxNoop(WorkerCountArgs),
    /// Run bounded secondary-index scans through the public stream facade.
    IndexStream(IndexStreamArgs),
    /// Create and drop an empty user table per iteration.
    TableDdl(WorkerIterationArgs),
    /// Create and drop a non-unique logical-key index per iteration.
    IndexDdl(WorkerIterationArgs),
    /// Measure public logical table-lock scenarios.
    LockTable(LockTableArgs),
}

#[derive(Clone, Debug, Args)]
pub(super) struct WorkerArgs {
    /// Operating-system worker threads.
    #[arg(long, short = 't')]
    threads: Option<NonZeroUsize>,
    /// Independent DoraDB public sessions.
    #[arg(long, short = 's')]
    sessions: Option<NonZeroUsize>,
    /// Capture and print internal storage-engine stats.
    #[arg(long, default_value_t = false)]
    include_stats: bool,
}

impl WorkerArgs {
    /// Return an explicitly configured executor thread count.
    pub(super) fn thread_override(&self) -> Option<usize> {
        self.threads.map(NonZeroUsize::get)
    }

    /// Return an explicitly configured public session count.
    pub(super) fn session_override(&self) -> Option<usize> {
        self.sessions.map(NonZeroUsize::get)
    }

    /// Return whether internal engine statistics should be captured.
    pub(super) fn include_stats(&self) -> bool {
        self.include_stats
    }
}

/// Worker controls plus a required aggregate operation count.
#[derive(Clone, Debug, Args)]
pub struct WorkerCountArgs {
    #[command(flatten)]
    worker: WorkerArgs,
    /// Total operations across all sessions.
    #[arg(long, short = 'n')]
    num: NonZeroU64,
}

impl WorkerCountArgs {
    /// Return shared worker arguments.
    pub(super) fn worker(&self) -> &WorkerArgs {
        &self.worker
    }

    /// Return the required aggregate operation count.
    pub(super) fn operation_count(&self) -> u64 {
        self.num.get()
    }
}

/// Arguments for explicit shared table-lock workloads.
#[derive(Clone, Debug, Args)]
pub struct LockTableArgs {
    #[command(flatten)]
    count: WorkerCountArgs,
    /// Lock operation scenario.
    #[arg(long, value_enum, default_value_t = LockTableScenario::Basic)]
    scenario: LockTableScenario,
    /// Physical lock mode used by the scenario.
    #[arg(long, value_enum, default_value_t = LockTableMode::Shared)]
    mode: LockTableMode,
    /// Scenario resource, waiter, promotion, or close cardinality.
    #[arg(long, default_value = "1")]
    width: NonZeroUsize,
    /// Lock ownership scope.
    #[arg(long, value_enum, default_value_t = TableLockScope::Session)]
    scope: TableLockScope,
    /// Release each acquired claim inside its measured iteration.
    #[arg(long, default_value_t = false)]
    unlock: bool,
    /// Select a prepared table independently for every iteration.
    #[arg(long, default_value_t = false, requires = "unlock")]
    rand: bool,
    /// Reproducibility seed for random table selection.
    #[arg(long, requires = "rand")]
    seed: Option<u64>,
}

impl LockTableArgs {
    /// Return shared worker arguments.
    pub(super) fn worker(&self) -> &WorkerArgs {
        self.count.worker()
    }

    /// Return the required aggregate lock iteration count.
    pub(super) fn operation_count(&self) -> u64 {
        self.count.operation_count()
    }

    pub(super) fn scenario(&self) -> LockTableScenario {
        self.scenario
    }

    pub(super) fn mode(&self) -> LockTableMode {
        self.mode
    }

    pub(super) fn width(&self) -> usize {
        self.width.get()
    }

    /// Return the configured lock ownership scope.
    pub(super) fn scope(&self) -> TableLockScope {
        self.scope
    }

    /// Return whether every acquired claim is released inside its iteration.
    pub(super) fn unlock(&self) -> bool {
        self.unlock
    }

    /// Return whether table targets are selected randomly.
    pub(super) fn random(&self) -> bool {
        self.rand
    }

    /// Return whether a seed was explicitly supplied.
    pub(super) fn explicit_seed(&self) -> Option<u64> {
        self.seed
    }

    /// Return the resolved table-selection seed.
    pub(super) fn seed(&self) -> u64 {
        self.seed.unwrap_or(0)
    }
}

/// Worker controls plus an optional aggregate iteration count.
#[derive(Clone, Debug, Args)]
pub struct WorkerIterationArgs {
    #[command(flatten)]
    worker: WorkerArgs,
    /// Total iterations across all sessions.
    #[arg(long, short = 'n')]
    num: Option<NonZeroU64>,
}

impl WorkerIterationArgs {
    /// Return shared worker arguments.
    pub(super) fn worker(&self) -> &WorkerArgs {
        &self.worker
    }

    /// Return the aggregate iteration count, defaulting to one.
    pub(super) fn iterations(&self) -> u64 {
        self.num.map_or(1, NonZeroU64::get)
    }
}

#[derive(Clone, Debug, Args)]
pub(super) struct LoadCommonArgs {
    #[command(flatten)]
    worker: WorkerArgs,
    /// Generated payload size in bytes.
    #[arg(long, short = 'v')]
    value_size: Option<NonZeroUsize>,
    /// Operations per transaction.
    #[arg(long, short = 'b')]
    batch_size: Option<NonZeroU64>,
}

impl LoadCommonArgs {
    /// Return shared worker arguments.
    pub(super) fn worker(&self) -> &WorkerArgs {
        &self.worker
    }

    /// Return an explicitly configured generated payload size.
    pub(super) fn value_size_override(&self) -> Option<usize> {
        self.value_size.map(NonZeroUsize::get)
    }

    /// Return an explicitly configured transaction batch size.
    pub(super) fn batch_size_override(&self) -> Option<u64> {
        self.batch_size.map(NonZeroU64::get)
    }
}

#[derive(Clone, Debug, Args)]
pub(super) struct ReadCommonArgs {
    #[command(flatten)]
    worker: WorkerArgs,
    /// Read operations per transaction.
    #[arg(long, short = 'b')]
    batch_size: Option<NonZeroU64>,
}

impl ReadCommonArgs {
    /// Return shared worker arguments.
    pub(super) fn worker(&self) -> &WorkerArgs {
        &self.worker
    }

    /// Return an explicitly configured read batch size.
    pub(super) fn batch_size_override(&self) -> Option<u64> {
        self.batch_size.map(NonZeroU64::get)
    }
}

/// Arguments for insert workloads.
#[derive(Clone, Debug, Args)]
pub struct InsertArgs {
    #[command(flatten)]
    common: LoadCommonArgs,
    /// Total rows inserted across all sessions.
    #[arg(long, short = 'n')]
    num: NonZeroU64,
    /// Reproducibility seed.
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

impl InsertArgs {
    /// Return shared insert arguments.
    pub(super) fn common(&self) -> &LoadCommonArgs {
        &self.common
    }

    /// Return the aggregate row count.
    pub(super) fn operation_count(&self) -> u64 {
        self.num.get()
    }

    /// Return the deterministic generator seed.
    pub(super) fn seed(&self) -> u64 {
        self.seed
    }
}

/// Arguments shared by read workloads.
#[derive(Clone, Debug, Args)]
pub struct ReadArgs {
    #[command(flatten)]
    common: ReadCommonArgs,
    /// Total read requests or scan iterations across all sessions.
    #[arg(long, short = 'n')]
    num: Option<NonZeroU64>,
}

impl ReadArgs {
    /// Return shared read arguments.
    pub(super) fn common(&self) -> &ReadCommonArgs {
        &self.common
    }

    /// Return the optional aggregate read or scan count.
    pub(super) fn operation_count(&self) -> Option<u64> {
        self.num.map(NonZeroU64::get)
    }
}

/// Arguments for seeded read workloads.
#[derive(Clone, Debug, Args)]
pub struct SeededReadArgs {
    #[command(flatten)]
    read: ReadArgs,
    /// Reproducibility seed.
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

impl SeededReadArgs {
    /// Return shared read arguments.
    pub(super) fn read(&self) -> &ReadArgs {
        &self.read
    }

    /// Return the deterministic read generator seed.
    pub(super) fn seed(&self) -> u64 {
        self.seed
    }
}

/// Arguments for materialized secondary-index range scans.
#[derive(Clone, Debug, Args)]
pub struct IndexScanArgs {
    #[command(flatten)]
    read: SeededReadArgs,
    /// Logical-key values per scan; defaults to the full loaded range.
    #[arg(long)]
    range: Option<NonZeroU64>,
}

impl IndexScanArgs {
    /// Return shared seeded read arguments.
    pub(super) fn read(&self) -> &ReadArgs {
        self.read.read()
    }

    /// Return the deterministic range-selection seed.
    pub(super) fn seed(&self) -> u64 {
        self.read.seed()
    }

    /// Return an explicitly configured logical-key range length.
    pub(super) fn range_override(&self) -> Option<u64> {
        self.range.map(NonZeroU64::get)
    }
}

/// Arguments for streaming secondary-index range scans.
#[derive(Clone, Debug, Args)]
pub struct IndexStreamArgs {
    #[command(flatten)]
    iterations: WorkerIterationArgs,
    /// Logical-key values per stream; defaults to the full loaded range.
    #[arg(long)]
    range: Option<NonZeroU64>,
    /// Reproducibility seed for range selection.
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

impl IndexStreamArgs {
    /// Return shared worker arguments.
    pub(super) fn worker(&self) -> &WorkerArgs {
        self.iterations.worker()
    }

    /// Return the aggregate iteration count.
    pub(super) fn iterations(&self) -> u64 {
        self.iterations.iterations()
    }

    /// Return the deterministic range-selection seed.
    pub(super) fn seed(&self) -> u64 {
        self.seed
    }

    /// Return an explicitly configured logical-key range length.
    pub(super) fn range_override(&self) -> Option<u64> {
        self.range.map(NonZeroU64::get)
    }
}

pub(super) fn validate_workers(threads: usize, sessions: usize) -> Result<()> {
    if threads == 0 || sessions == 0 {
        return Err(BenchError::message(
            "threads and sessions must both be positive",
        ));
    }
    if threads > sessions {
        return Err(BenchError::message(format!(
            "--threads ({threads}) must not exceed --sessions ({sessions})"
        )));
    }
    Ok(())
}

pub(super) fn validate_value_size(value_size: usize) -> Result<()> {
    if value_size == 0 {
        return Err(BenchError::message("--value-size must be positive"));
    }
    if value_size > MAX_VALUE_SIZE {
        return Err(BenchError::message(format!(
            "--value-size must not exceed {MAX_VALUE_SIZE} bytes"
        )));
    }
    Ok(())
}

pub(super) fn validate_batch_size(batch_size: u64) -> Result<()> {
    if batch_size == 0 {
        return Err(BenchError::message("--batch-size must be positive"));
    }
    if batch_size > usize::MAX as u64 {
        return Err(BenchError::message(
            "--batch-size exceeds addressable memory on this platform",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_insert_seq_workload_subcommand() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "run",
            "insert-seq",
            "--num",
            "1",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::Run { .. }));
        assert_eq!(
            cli.resolve_root_with_env(None).unwrap(),
            PathBuf::from("root")
        );
    }

    #[test]
    fn parse_global_root_after_nested_command() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert-seq",
            "--root",
            "root",
            "--num",
            "1",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::Run { .. }));
        assert_eq!(
            cli.resolve_root_with_env(None).unwrap(),
            PathBuf::from("root")
        );
    }

    #[test]
    fn reject_removed_warmup_command() {
        let err = Cli::try_parse_from([
            "doradb-bench",
            "warmup",
            "insert-seq",
            "--root",
            "root",
            "--num",
            "1",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn reject_removed_workload_option() {
        let err = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "--root",
            "root",
            "--workload",
            "fillseq",
            "--num",
            "1",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn reject_removed_file_options() {
        for removed in ["--state-file", "--output", "--storage-root"] {
            let err = Cli::try_parse_from(["doradb-bench", "run", removed, "x"]).unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn parse_insert_short_flags() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert-rand",
            "-r",
            "root",
            "-n",
            "10",
            "-v",
            "32",
            "-b",
            "4",
            "-t",
            "2",
            "-s",
            "4",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::InsertRand(insert),
        } = cli.command
        else {
            panic!("expected run command");
        };
        assert_eq!(insert.operation_count(), 10);
        assert_eq!(insert.common().value_size_override(), Some(32));
        assert_eq!(insert.common().batch_size_override(), Some(4));
        assert_eq!(insert.common().worker().thread_override(), Some(2));
        assert_eq!(insert.common().worker().session_override(), Some(4));
    }

    #[test]
    fn prepare_defaults_index_tables_and_log_sync() {
        let cli = Cli::try_parse_from(["doradb-bench", "--root", "root", "prepare"]).unwrap();
        let Command::Prepare(args) = cli.command else {
            panic!("expected prepare command");
        };
        assert_eq!(args.index, IndexMode::None);
        assert_eq!(args.tables.get(), 1);
        assert_eq!(args.log_sync, LogSyncMode::Fsync);
    }

    #[test]
    fn prepare_rejects_invalid_table_counts() {
        for tables in ["0", "invalid"] {
            assert!(
                Cli::try_parse_from([
                    "doradb-bench",
                    "--root",
                    "root",
                    "prepare",
                    "--tables",
                    tables,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn prepare_parses_topology_durability_and_worker_defaults() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "prepare",
            "--index",
            "non-unique",
            "--tables",
            "3",
            "--log-sync",
            "fdatasync",
            "--threads",
            "2",
            "--sessions",
            "4",
            "--value-size",
            "256",
            "--batch-size",
            "8",
        ])
        .unwrap();
        let Command::Prepare(args) = cli.command else {
            panic!("expected prepare command");
        };
        assert_eq!(args.index, IndexMode::NonUnique);
        assert_eq!(args.tables.get(), 3);
        assert_eq!(args.log_sync, LogSyncMode::Fdatasync);
        assert_eq!(args.threads.get(), 2);
        assert_eq!(args.sessions.unwrap().get(), 4);
        assert_eq!(args.value_size.get(), 256);
        assert_eq!(args.batch_size.get(), 8);
    }

    #[test]
    fn parse_read_workloads() {
        let cases = vec![
            (
                vec![
                    "doradb-bench",
                    "run",
                    "lookup-seq",
                    "--root",
                    "root",
                    "--num",
                    "3",
                    "--batch-size",
                    "2",
                ],
                Workload::LookupSeq,
            ),
            (
                vec![
                    "doradb-bench",
                    "run",
                    "lookup-rand",
                    "--root",
                    "root",
                    "--num",
                    "3",
                    "--batch-size",
                    "2",
                    "--seed",
                    "7",
                ],
                Workload::LookupRand,
            ),
            (
                vec![
                    "doradb-bench",
                    "run",
                    "table-scan",
                    "--root",
                    "root",
                    "--batch-size",
                    "2",
                ],
                Workload::TableScan,
            ),
            (
                vec![
                    "doradb-bench",
                    "run",
                    "index-scan",
                    "--root",
                    "root",
                    "--num",
                    "3",
                    "--batch-size",
                    "2",
                    "--range",
                    "2",
                ],
                Workload::IndexScan,
            ),
        ];

        for (args, workload) in cases {
            let cli = Cli::try_parse_from(args).unwrap();
            let Command::Run { workload: load } = cli.command else {
                panic!("expected run command");
            };
            assert_eq!(parsed_workload(&load), workload);
            let batch_size = match &load {
                WorkloadArgs::LookupSeq(args) | WorkloadArgs::TableScan(args) => {
                    args.common().batch_size_override()
                }
                WorkloadArgs::LookupRand(args) => args.read().common().batch_size_override(),
                WorkloadArgs::IndexScan(args) => args.read().common().batch_size_override(),
                _ => panic!("expected read workload"),
            };
            assert_eq!(batch_size, Some(2));
        }
    }

    #[test]
    fn parse_coordinator_and_ddl_workloads() {
        let cases = [
            ("stmt-noop", Some("3"), Workload::StmtNoop, 3),
            ("trx-noop", Some("3"), Workload::TrxNoop, 3),
            ("index-stream", None, Workload::IndexStream, 1),
            ("table-ddl", None, Workload::TableDdl, 1),
            ("index-ddl", Some("2"), Workload::IndexDdl, 2),
        ];

        for (name, num, expected, expected_num) in cases {
            let mut args = vec![
                "doradb-bench",
                "run",
                name,
                "--root",
                "root",
                "--threads",
                "1",
                "--sessions",
                "2",
                "--include-stats",
            ];
            if let Some(num) = num {
                args.extend(["--num", num]);
            }
            let cli = Cli::try_parse_from(args).unwrap();
            let Command::Run { workload } = cli.command else {
                panic!("expected run command");
            };
            assert_eq!(parsed_workload(&workload), expected);
            assert_eq!(parsed_operation_count(&workload), Some(expected_num));
            let worker = parsed_worker(&workload);
            assert_eq!(worker.thread_override(), Some(1));
            assert_eq!(worker.session_override(), Some(2));
            assert!(worker.include_stats());
        }
    }

    #[test]
    fn parse_lock_table_controls_and_defaults() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "lock-table",
            "--root",
            "root",
            "--num",
            "7",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::LockTable(args),
        } = cli.command
        else {
            panic!("expected lock-table command");
        };
        assert_eq!(args.operation_count(), 7);
        assert_eq!(args.scope(), TableLockScope::Session);
        assert_eq!(args.scenario(), LockTableScenario::Basic);
        assert_eq!(args.mode(), LockTableMode::Shared);
        assert_eq!(args.width(), 1);
        assert!(!args.unlock());
        assert!(!args.random());
        assert_eq!(args.seed(), 0);

        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "lock-table",
            "--root",
            "root",
            "--num",
            "7",
            "--scope",
            "transaction",
            "--unlock",
            "--rand",
            "--seed",
            "9",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::LockTable(args),
        } = cli.command
        else {
            panic!("expected lock-table command");
        };
        assert_eq!(args.scope(), TableLockScope::Transaction);
        assert!(args.unlock());
        assert!(args.random());
        assert_eq!(args.seed(), 9);

        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "lock-table",
            "--root",
            "root",
            "--num",
            "7",
            "--scenario",
            "scope-close",
            "--mode",
            "exclusive",
            "--width",
            "4",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::LockTable(args),
        } = cli.command
        else {
            panic!("expected lock-table command");
        };
        assert_eq!(args.scenario(), LockTableScenario::ScopeClose);
        assert_eq!(args.mode(), LockTableMode::Exclusive);
        assert_eq!(args.width(), 4);
    }

    #[test]
    fn lock_table_enforces_random_dependencies_and_relevant_controls() {
        for args in [
            &[
                "doradb-bench",
                "run",
                "lock-table",
                "--root",
                "root",
                "--num",
                "1",
                "--rand",
            ][..],
            &[
                "doradb-bench",
                "run",
                "lock-table",
                "--root",
                "root",
                "--num",
                "1",
                "--seed",
                "1",
            ][..],
        ] {
            let err = Cli::try_parse_from(args).unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        }

        for option in [
            "--tables",
            "--index",
            "--batch-size",
            "--value-size",
            "--range",
        ] {
            let err = Cli::try_parse_from([
                "doradb-bench",
                "run",
                "lock-table",
                "--root",
                "root",
                "--num",
                "1",
                option,
                "1",
            ])
            .unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn run_workloads_reject_prepare_owned_log_sync() {
        let cases = [
            ("insert-seq", true),
            ("insert-rand", true),
            ("lookup-seq", true),
            ("lookup-rand", true),
            ("table-scan", false),
            ("index-scan", true),
            ("stmt-noop", true),
            ("trx-noop", true),
            ("index-stream", false),
            ("table-ddl", false),
            ("index-ddl", false),
            ("lock-table", true),
        ];
        for (workload, needs_num) in cases {
            let mut args = vec!["doradb-bench", "run", workload, "--root", "root"];
            if needs_num {
                args.extend(["--num", "1"]);
            }
            args.extend(["--log-sync", "none"]);
            let err = Cli::try_parse_from(args).unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn new_workloads_validate_counts_and_reject_irrelevant_controls() {
        for name in ["stmt-noop", "trx-noop", "lock-table"] {
            let err =
                Cli::try_parse_from(["doradb-bench", "run", name, "--root", "root"]).unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        }
        for name in [
            "stmt-noop",
            "trx-noop",
            "index-stream",
            "table-ddl",
            "index-ddl",
        ] {
            let err =
                Cli::try_parse_from(["doradb-bench", "run", name, "--root", "root", "--num", "0"])
                    .unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);

            let irrelevant = if name == "index-stream" {
                &["--batch-size", "--value-size", "--rand"][..]
            } else {
                &[
                    "--batch-size",
                    "--value-size",
                    "--seed",
                    "--range",
                    "--rand",
                ][..]
            };
            for &option in irrelevant {
                let mut args = vec![
                    "doradb-bench",
                    "run",
                    name,
                    "--root",
                    "root",
                    "--num",
                    "1",
                    option,
                ];
                if option != "--rand" {
                    args.push("2");
                }
                let err = Cli::try_parse_from(args).unwrap_err();
                assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
            }
        }

        for name in ["index-scan", "index-stream"] {
            let err = Cli::try_parse_from([
                "doradb-bench",
                "run",
                name,
                "--root",
                "root",
                "--num",
                "1",
                "--range",
                "0",
            ])
            .unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
        }
    }

    #[test]
    fn run_workloads_accept_include_stats() {
        let cases = vec![
            vec![
                "doradb-bench",
                "run",
                "insert-seq",
                "--root",
                "root",
                "--num",
                "3",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "insert-rand",
                "--root",
                "root",
                "--num",
                "3",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "lookup-seq",
                "--root",
                "root",
                "--num",
                "3",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "lookup-rand",
                "--root",
                "root",
                "--num",
                "3",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "table-scan",
                "--root",
                "root",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "index-scan",
                "--root",
                "root",
                "--num",
                "3",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "stmt-noop",
                "--root",
                "root",
                "--num",
                "3",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "trx-noop",
                "--root",
                "root",
                "--num",
                "3",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "index-stream",
                "--root",
                "root",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "table-ddl",
                "--root",
                "root",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "index-ddl",
                "--root",
                "root",
                "--include-stats",
            ],
            vec![
                "doradb-bench",
                "run",
                "lock-table",
                "--root",
                "root",
                "--num",
                "3",
                "--include-stats",
            ],
        ];

        for args in cases {
            let cli = Cli::try_parse_from(args).unwrap();
            let Command::Run { workload } = cli.command else {
                panic!("expected run command");
            };
            assert!(parsed_worker(&workload).include_stats());
        }
    }

    #[test]
    fn read_workloads_reject_value_size() {
        let err = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "lookup-seq",
            "--root",
            "root",
            "--num",
            "3",
            "--value-size",
            "16",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn reject_run_level_index_option() {
        let err = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert-seq",
            "--root",
            "root",
            "--num",
            "1",
            "--index",
            "unique",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn reject_removed_insert_workload_and_rand_flag() {
        let err = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert",
            "--root",
            "root",
            "--num",
            "1",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);

        let err = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert-seq",
            "--root",
            "root",
            "--num",
            "1",
            "--rand",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn resolve_root_uses_env_fallback() {
        let cli = Cli::try_parse_from(["doradb-bench", "run", "insert-seq", "--num", "1"]).unwrap();
        assert_eq!(
            cli.resolve_root_with_env(Some(PathBuf::from("env-root")))
                .unwrap(),
            PathBuf::from("env-root")
        );
    }

    #[test]
    fn resolve_root_prefers_cli_over_env() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "cli-root",
            "run",
            "insert-seq",
            "--num",
            "1",
        ])
        .unwrap();
        assert_eq!(
            cli.resolve_root_with_env(Some(PathBuf::from("env-root")))
                .unwrap(),
            PathBuf::from("cli-root")
        );
    }

    #[test]
    fn resolve_root_rejects_missing_root_and_empty_env() {
        let cli = Cli::try_parse_from(["doradb-bench", "run", "insert-seq", "--num", "1"]).unwrap();
        assert!(cli.resolve_root_with_env(None).is_err());
        assert!(cli.resolve_root_with_env(Some(PathBuf::new())).is_err());
    }

    fn parsed_workload(args: &WorkloadArgs) -> Workload {
        match args {
            WorkloadArgs::InsertSeq(_) => Workload::InsertSeq,
            WorkloadArgs::InsertRand(_) => Workload::InsertRand,
            WorkloadArgs::LookupSeq(_) => Workload::LookupSeq,
            WorkloadArgs::LookupRand(_) => Workload::LookupRand,
            WorkloadArgs::TableScan(_) => Workload::TableScan,
            WorkloadArgs::IndexScan(_) => Workload::IndexScan,
            WorkloadArgs::StmtNoop(_) => Workload::StmtNoop,
            WorkloadArgs::TrxNoop(_) => Workload::TrxNoop,
            WorkloadArgs::IndexStream(_) => Workload::IndexStream,
            WorkloadArgs::TableDdl(_) => Workload::TableDdl,
            WorkloadArgs::IndexDdl(_) => Workload::IndexDdl,
            WorkloadArgs::LockTable(_) => Workload::LockTable,
        }
    }

    fn parsed_worker(args: &WorkloadArgs) -> &WorkerArgs {
        match args {
            WorkloadArgs::InsertSeq(args) | WorkloadArgs::InsertRand(args) => {
                args.common().worker()
            }
            WorkloadArgs::LookupSeq(args) | WorkloadArgs::TableScan(args) => args.common().worker(),
            WorkloadArgs::LookupRand(args) => args.read().common().worker(),
            WorkloadArgs::IndexScan(args) => args.read().common().worker(),
            WorkloadArgs::StmtNoop(args) | WorkloadArgs::TrxNoop(args) => args.worker(),
            WorkloadArgs::IndexStream(args) => args.worker(),
            WorkloadArgs::TableDdl(args) | WorkloadArgs::IndexDdl(args) => args.worker(),
            WorkloadArgs::LockTable(args) => args.worker(),
        }
    }

    fn parsed_operation_count(args: &WorkloadArgs) -> Option<u64> {
        match args {
            WorkloadArgs::InsertSeq(args) | WorkloadArgs::InsertRand(args) => {
                Some(args.operation_count())
            }
            WorkloadArgs::LookupSeq(args) | WorkloadArgs::TableScan(args) => args.operation_count(),
            WorkloadArgs::LookupRand(args) => args.read().operation_count(),
            WorkloadArgs::IndexScan(args) => args.read().operation_count(),
            WorkloadArgs::StmtNoop(args) | WorkloadArgs::TrxNoop(args) => {
                Some(args.operation_count())
            }
            WorkloadArgs::IndexStream(args) => Some(args.iterations()),
            WorkloadArgs::TableDdl(args) | WorkloadArgs::IndexDdl(args) => Some(args.iterations()),
            WorkloadArgs::LockTable(args) => Some(args.operation_count()),
        }
    }
}
