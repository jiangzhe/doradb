use crate::error::{BenchError, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use doradb_storage::LogSync;
use serde::{Deserialize, Serialize};
use std::env::var_os;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;

const ROOT_ENV_VAR: &str = "DORADB_BENCH_ROOT";

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
pub(super) enum LogSyncMode {
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
        }
    }
}

/// Arguments for preparing a benchmark storage root.
#[derive(Clone, Debug, Args)]
pub struct PrepareArgs {
    /// Benchmark table index shape.
    #[arg(long, short = 'i', value_enum)]
    pub(super) index: IndexMode,
    /// Default operating-system worker threads for later runs.
    #[arg(long, short = 't', default_value = "1")]
    pub(super) threads: NonZeroUsize,
    /// Default independent DoraDB public sessions for later runs.
    #[arg(long, short = 's')]
    pub(super) sessions: Option<NonZeroUsize>,
}

/// Arguments for measured benchmark workloads.
#[derive(Clone, Debug, Subcommand)]
pub enum WorkloadArgs {
    /// Insert generated rows with sequential logical keys.
    InsertSeq(InsertArgs),
    /// Insert generated rows with pseudo-random logical keys.
    InsertRand(InsertArgs),
    /// Run unique-index point lookups over loaded keys in sequential order.
    LookupSeq(ReadCountArgs),
    /// Run unique-index point lookups over loaded keys in seeded random order.
    LookupRand(SeededReadArgs),
    /// Run full table-scan iterations over visible rows.
    TableScan(TableScanArgs),
    /// Run exact-key non-unique secondary-index scans over loaded keys.
    IndexScan(SeededReadArgs),
}

impl WorkloadArgs {
    pub(super) fn resolve(
        &self,
        storage_root: PathBuf,
        manifest_index: IndexMode,
        default_threads: usize,
        default_sessions: usize,
    ) -> Result<LoadConfig> {
        match self {
            WorkloadArgs::InsertSeq(args) => args.resolve_insert_seq(
                storage_root,
                manifest_index,
                default_threads,
                default_sessions,
            ),
            WorkloadArgs::InsertRand(args) => args.resolve_insert_rand(
                storage_root,
                manifest_index,
                default_threads,
                default_sessions,
            ),
            WorkloadArgs::LookupSeq(args) => args.resolve(
                storage_root,
                manifest_index,
                default_threads,
                default_sessions,
            ),
            WorkloadArgs::LookupRand(args) => args.resolve_lookup_rand(
                storage_root,
                manifest_index,
                default_threads,
                default_sessions,
            ),
            WorkloadArgs::TableScan(args) => args.resolve(
                storage_root,
                manifest_index,
                default_threads,
                default_sessions,
            ),
            WorkloadArgs::IndexScan(args) => args.resolve_index_scan(
                storage_root,
                manifest_index,
                default_threads,
                default_sessions,
            ),
        }
    }
}

#[derive(Clone, Debug, Args)]
struct LoadCommonArgs {
    /// Operating-system worker threads.
    #[arg(long, short = 't')]
    threads: Option<NonZeroUsize>,
    /// Independent DoraDB public sessions.
    #[arg(long, short = 's')]
    sessions: Option<NonZeroUsize>,
    /// Redo-log durability sync method.
    #[arg(long, value_enum, default_value_t = LogSyncMode::Fsync)]
    log_sync: LogSyncMode,
}

#[derive(Clone, Debug, Args)]
pub struct InsertArgs {
    #[command(flatten)]
    common: LoadCommonArgs,
    /// Total rows inserted across all sessions.
    #[arg(long, short = 'n')]
    num: NonZeroU64,
    /// Generated payload size in bytes.
    #[arg(long, short = 'v', default_value = "128")]
    value_size: NonZeroUsize,
    /// Rows per transaction commit.
    #[arg(long, short = 'b', default_value = "1")]
    batch_size: NonZeroU64,
    /// Reproducibility seed.
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

impl InsertArgs {
    fn resolve_insert_seq(
        &self,
        storage_root: PathBuf,
        manifest_index: IndexMode,
        default_threads: usize,
        default_sessions: usize,
    ) -> Result<LoadConfig> {
        self.resolve_insert(
            storage_root,
            manifest_index,
            default_threads,
            default_sessions,
            false,
        )
    }

    fn resolve_insert_rand(
        &self,
        storage_root: PathBuf,
        manifest_index: IndexMode,
        default_threads: usize,
        default_sessions: usize,
    ) -> Result<LoadConfig> {
        self.resolve_insert(
            storage_root,
            manifest_index,
            default_threads,
            default_sessions,
            true,
        )
    }

    fn resolve_insert(
        &self,
        storage_root: PathBuf,
        manifest_index: IndexMode,
        default_threads: usize,
        default_sessions: usize,
        random: bool,
    ) -> Result<LoadConfig> {
        let workers = resolve_workers(&self.common, default_threads, default_sessions)?;
        let insert = InsertConfig {
            num: self.num.get(),
            value_size: self.value_size.get(),
            batch_size: self.batch_size.get(),
            seed: self.seed,
        };
        let workload = if random {
            WorkloadConfig::InsertRand(insert)
        } else {
            WorkloadConfig::InsertSeq(insert)
        };
        Ok(LoadConfig {
            storage_root,
            index: manifest_index,
            threads: workers.threads,
            sessions: workers.sessions,
            log_sync: self.common.log_sync,
            workload,
        })
    }
}

#[derive(Clone, Debug, Args)]
pub struct ReadCountArgs {
    #[command(flatten)]
    common: LoadCommonArgs,
    /// Total read requests across all sessions.
    #[arg(long, short = 'n')]
    num: NonZeroU64,
}

impl ReadCountArgs {
    fn resolve(
        &self,
        storage_root: PathBuf,
        manifest_index: IndexMode,
        default_threads: usize,
        default_sessions: usize,
    ) -> Result<LoadConfig> {
        let workers = resolve_workers(&self.common, default_threads, default_sessions)?;
        Ok(LoadConfig {
            storage_root,
            index: manifest_index,
            threads: workers.threads,
            sessions: workers.sessions,
            log_sync: self.common.log_sync,
            workload: WorkloadConfig::LookupSeq {
                num: self.num.get(),
            },
        })
    }
}

#[derive(Clone, Debug, Args)]
pub struct SeededReadArgs {
    #[command(flatten)]
    common: LoadCommonArgs,
    /// Total read requests across all sessions.
    #[arg(long, short = 'n')]
    num: NonZeroU64,
    /// Reproducibility seed.
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

impl SeededReadArgs {
    fn resolve_lookup_rand(
        &self,
        storage_root: PathBuf,
        manifest_index: IndexMode,
        default_threads: usize,
        default_sessions: usize,
    ) -> Result<LoadConfig> {
        let workers = resolve_workers(&self.common, default_threads, default_sessions)?;
        Ok(LoadConfig {
            storage_root,
            index: manifest_index,
            threads: workers.threads,
            sessions: workers.sessions,
            log_sync: self.common.log_sync,
            workload: WorkloadConfig::LookupRand {
                num: self.num.get(),
                seed: self.seed,
            },
        })
    }

    fn resolve_index_scan(
        &self,
        storage_root: PathBuf,
        manifest_index: IndexMode,
        default_threads: usize,
        default_sessions: usize,
    ) -> Result<LoadConfig> {
        let workers = resolve_workers(&self.common, default_threads, default_sessions)?;
        Ok(LoadConfig {
            storage_root,
            index: manifest_index,
            threads: workers.threads,
            sessions: workers.sessions,
            log_sync: self.common.log_sync,
            workload: WorkloadConfig::IndexScan {
                num: self.num.get(),
                seed: self.seed,
            },
        })
    }
}

#[derive(Clone, Debug, Args)]
pub struct TableScanArgs {
    #[command(flatten)]
    common: LoadCommonArgs,
    /// Full table-scan iterations across all sessions.
    #[arg(long, short = 'n', default_value = "1")]
    num: NonZeroU64,
}

impl TableScanArgs {
    fn resolve(
        &self,
        storage_root: PathBuf,
        manifest_index: IndexMode,
        default_threads: usize,
        default_sessions: usize,
    ) -> Result<LoadConfig> {
        let workers = resolve_workers(&self.common, default_threads, default_sessions)?;
        Ok(LoadConfig {
            storage_root,
            index: manifest_index,
            threads: workers.threads,
            sessions: workers.sessions,
            log_sync: self.common.log_sync,
            workload: WorkloadConfig::TableScan {
                num: self.num.get(),
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerSettings {
    threads: usize,
    sessions: usize,
}

fn resolve_workers(
    common: &LoadCommonArgs,
    default_threads: usize,
    default_sessions: usize,
) -> Result<WorkerSettings> {
    let threads = common.threads.map_or(default_threads, NonZeroUsize::get);
    let sessions = match (common.threads, common.sessions) {
        (_, Some(sessions)) => sessions.get(),
        (Some(threads), None) => threads.get(),
        (None, None) => default_sessions,
    };
    validate_workers(threads, sessions)?;
    Ok(WorkerSettings { threads, sessions })
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LoadConfig {
    pub(super) storage_root: PathBuf,
    pub(super) index: IndexMode,
    pub(super) threads: usize,
    pub(super) sessions: usize,
    pub(super) log_sync: LogSyncMode,
    pub(super) workload: WorkloadConfig,
}

impl LoadConfig {
    pub(super) fn workload(&self) -> Workload {
        self.workload.workload()
    }

    pub(super) fn operation_count(&self) -> u64 {
        self.workload.operation_count()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum WorkloadConfig {
    InsertSeq(InsertConfig),
    InsertRand(InsertConfig),
    LookupSeq { num: u64 },
    LookupRand { num: u64, seed: u64 },
    TableScan { num: u64 },
    IndexScan { num: u64, seed: u64 },
}

impl WorkloadConfig {
    fn workload(&self) -> Workload {
        match self {
            Self::InsertSeq(_) => Workload::InsertSeq,
            Self::InsertRand(_) => Workload::InsertRand,
            Self::LookupSeq { .. } => Workload::LookupSeq,
            Self::LookupRand { .. } => Workload::LookupRand,
            Self::TableScan { .. } => Workload::TableScan,
            Self::IndexScan { .. } => Workload::IndexScan,
        }
    }

    fn operation_count(&self) -> u64 {
        match self {
            Self::InsertSeq(config) | Self::InsertRand(config) => config.num,
            Self::LookupSeq { num }
            | Self::LookupRand { num, .. }
            | Self::TableScan { num }
            | Self::IndexScan { num, .. } => *num,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct InsertConfig {
    pub(super) num: u64,
    pub(super) value_size: usize,
    pub(super) batch_size: u64,
    pub(super) seed: u64,
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
    fn resolve_sessions_default_and_thread_limit() {
        let args = WorkloadArgs::InsertSeq(InsertArgs {
            common: LoadCommonArgs {
                threads: Some(NonZeroUsize::new(2).unwrap()),
                sessions: None,
                log_sync: LogSyncMode::Fsync,
            },
            num: NonZeroU64::new(1).unwrap(),
            value_size: NonZeroUsize::new(128).unwrap(),
            batch_size: NonZeroU64::new(1).unwrap(),
            seed: 0,
        });
        let config = args
            .resolve(PathBuf::from("root"), IndexMode::None, 1, 1)
            .unwrap();
        assert_eq!(config.sessions, 2);

        let WorkloadArgs::InsertSeq(mut insert) = args else {
            panic!("expected insert-seq workload");
        };
        insert.common.sessions = Some(NonZeroUsize::new(1).unwrap());
        assert!(
            WorkloadArgs::InsertSeq(insert)
                .resolve(PathBuf::from("root"), IndexMode::None, 1, 1)
                .is_err()
        );
    }

    #[test]
    fn resolve_run_worker_defaults_from_manifest() {
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
        let Command::Run { workload } = cli.command else {
            panic!("expected run command");
        };
        let config = workload
            .resolve(PathBuf::from("root"), IndexMode::None, 2, 4)
            .unwrap();
        assert_eq!(config.threads, 2);
        assert_eq!(config.sessions, 4);
    }

    #[test]
    fn resolve_run_sessions_only_uses_manifest_threads() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert-seq",
            "--root",
            "root",
            "--num",
            "1",
            "--sessions",
            "3",
        ])
        .unwrap();
        let Command::Run { workload } = cli.command else {
            panic!("expected run command");
        };
        let config = workload
            .resolve(PathBuf::from("root"), IndexMode::None, 2, 4)
            .unwrap();
        assert_eq!(config.threads, 2);
        assert_eq!(config.sessions, 3);
    }

    #[test]
    fn resolve_insert_seq_defaults_to_single_row_batches() {
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
        let Command::Run { workload } = cli.command else {
            panic!("expected run command");
        };
        let config = workload
            .resolve(PathBuf::from("root"), IndexMode::None, 1, 1)
            .unwrap();
        assert_eq!(config.workload(), Workload::InsertSeq);
        let WorkloadConfig::InsertSeq(insert) = config.workload else {
            panic!("expected insert-seq workload");
        };
        assert_eq!(insert.batch_size, 1);
        assert_eq!(config.log_sync, LogSyncMode::Fsync);
    }

    #[test]
    fn resolve_insert_rand_workload() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert-rand",
            "--root",
            "root",
            "--num",
            "1",
            "--seed",
            "7",
        ])
        .unwrap();
        let Command::Run { workload } = cli.command else {
            panic!("expected run command");
        };
        let config = workload
            .resolve(PathBuf::from("root"), IndexMode::None, 1, 1)
            .unwrap();
        assert_eq!(config.workload(), Workload::InsertRand);
        let WorkloadConfig::InsertRand(insert) = config.workload else {
            panic!("expected insert-rand workload");
        };
        assert_eq!(insert.seed, 7);
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
            "--log-sync",
            "fdatasync",
        ])
        .unwrap();
        let Command::Run { workload } = cli.command else {
            panic!("expected run command");
        };
        let config = workload
            .resolve(PathBuf::from("root"), IndexMode::Unique, 1, 1)
            .unwrap();
        assert_eq!(config.storage_root, PathBuf::from("root"));
        let WorkloadConfig::InsertRand(insert) = config.workload else {
            panic!("expected insert-rand workload");
        };
        assert_eq!(insert.num, 10);
        assert_eq!(insert.value_size, 32);
        assert_eq!(insert.batch_size, 4);
        assert_eq!(config.index, IndexMode::Unique);
        assert_eq!(config.threads, 2);
        assert_eq!(config.sessions, 4);
        assert_eq!(config.log_sync, LogSyncMode::Fdatasync);
    }

    #[test]
    fn prepare_requires_index_and_parses_worker_defaults() {
        let err = Cli::try_parse_from(["doradb-bench", "--root", "root", "prepare"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);

        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "prepare",
            "--index",
            "non-unique",
            "--threads",
            "2",
            "--sessions",
            "4",
        ])
        .unwrap();
        let Command::Prepare(args) = cli.command else {
            panic!("expected prepare command");
        };
        assert_eq!(args.index, IndexMode::NonUnique);
        assert_eq!(args.threads.get(), 2);
        assert_eq!(args.sessions.unwrap().get(), 4);
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
                    "--seed",
                    "7",
                ],
                Workload::LookupRand,
            ),
            (
                vec!["doradb-bench", "run", "table-scan", "--root", "root"],
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
                ],
                Workload::IndexScan,
            ),
        ];

        for (args, workload) in cases {
            let cli = Cli::try_parse_from(args).unwrap();
            let Command::Run { workload: load } = cli.command else {
                panic!("expected run command");
            };
            let config = load
                .resolve(PathBuf::from("root"), IndexMode::Unique, 1, 1)
                .unwrap();
            assert_eq!(config.workload(), workload);
            assert!(config.operation_count() > 0);
        }
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
}
