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
    Run(LoadArgs),
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
}

impl fmt::Display for IndexMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Unique => f.write_str("unique"),
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
    Insert,
}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert => f.write_str("insert"),
        }
    }
}

/// Arguments for preparing a benchmark storage root.
#[derive(Clone, Debug, Args)]
pub struct PrepareArgs {
    /// Benchmark table index shape.
    #[arg(long, short = 'i', value_enum, default_value_t = IndexMode::None)]
    pub(super) index: IndexMode,
}

/// Arguments for measured benchmark runs.
#[derive(Clone, Debug, Args)]
pub struct LoadArgs {
    #[command(subcommand)]
    workload: LoadWorkload,
}

impl LoadArgs {
    pub(super) fn resolve(
        &self,
        storage_root: PathBuf,
        manifest_index: IndexMode,
    ) -> Result<LoadConfig> {
        match &self.workload {
            LoadWorkload::Insert(args) => args.resolve(storage_root, manifest_index),
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
enum LoadWorkload {
    /// Insert generated rows into the prepared benchmark table.
    Insert(InsertArgs),
}

#[derive(Clone, Debug, Args)]
struct LoadCommonArgs {
    /// Validate the prepared benchmark index shape.
    #[arg(long, short = 'i', value_enum)]
    index: Option<IndexMode>,
    /// Operating-system worker threads.
    #[arg(long, short = 't', default_value = "1")]
    threads: NonZeroUsize,
    /// Independent DoraDB public sessions; defaults to --threads.
    #[arg(long, short = 's')]
    sessions: Option<NonZeroUsize>,
    /// Redo-log durability sync method.
    #[arg(long, value_enum, default_value_t = LogSyncMode::Fsync)]
    log_sync: LogSyncMode,
}

#[derive(Clone, Debug, Args)]
struct InsertArgs {
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
    /// Generate logical keys in pseudo-random order.
    #[arg(long)]
    rand: bool,
    /// Reproducibility seed.
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

impl InsertArgs {
    fn resolve(&self, storage_root: PathBuf, manifest_index: IndexMode) -> Result<LoadConfig> {
        let index = self.common.index.unwrap_or(manifest_index);
        if index != manifest_index {
            return Err(BenchError::message(format!(
                "--index {index} does not match prepared benchmark index {manifest_index}"
            )));
        }
        let threads = self.common.threads.get();
        let sessions = self.common.sessions.unwrap_or(self.common.threads).get();
        if threads > sessions {
            return Err(BenchError::message(format!(
                "--threads ({threads}) must not exceed --sessions ({sessions})"
            )));
        }
        Ok(LoadConfig {
            storage_root,
            workload: Workload::Insert,
            num: self.num.get(),
            value_size: self.value_size.get(),
            batch_size: self.batch_size.get(),
            rand: self.rand,
            seed: self.seed,
            index,
            threads,
            sessions,
            log_sync: self.common.log_sync,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LoadConfig {
    pub(super) storage_root: PathBuf,
    pub(super) workload: Workload,
    pub(super) num: u64,
    pub(super) value_size: usize,
    pub(super) batch_size: u64,
    pub(super) rand: bool,
    pub(super) seed: u64,
    pub(super) index: IndexMode,
    pub(super) threads: usize,
    pub(super) sessions: usize,
    pub(super) log_sync: LogSyncMode,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_insert_workload_subcommand() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "run",
            "insert",
            "--num",
            "1",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::Run(_)));
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
            "insert",
            "--root",
            "root",
            "--num",
            "1",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::Run(_)));
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
            "insert",
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
        let args = LoadArgs {
            workload: LoadWorkload::Insert(InsertArgs {
                common: LoadCommonArgs {
                    index: None,
                    threads: NonZeroUsize::new(2).unwrap(),
                    sessions: None,
                    log_sync: LogSyncMode::Fsync,
                },
                num: NonZeroU64::new(1).unwrap(),
                value_size: NonZeroUsize::new(128).unwrap(),
                batch_size: NonZeroU64::new(1).unwrap(),
                rand: false,
                seed: 0,
            }),
        };
        let config = args
            .resolve(PathBuf::from("root"), IndexMode::None)
            .unwrap();
        assert_eq!(config.sessions, 2);

        let LoadWorkload::Insert(mut insert) = args.workload;
        insert.common.sessions = Some(NonZeroUsize::new(1).unwrap());
        assert!(
            LoadArgs {
                workload: LoadWorkload::Insert(insert),
            }
            .resolve(PathBuf::from("root"), IndexMode::None)
            .is_err()
        );
    }

    #[test]
    fn resolve_insert_defaults_to_single_row_batches() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert",
            "--root",
            "root",
            "--num",
            "1",
        ])
        .unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        let config = args
            .resolve(PathBuf::from("root"), IndexMode::None)
            .unwrap();
        assert_eq!(config.workload, Workload::Insert);
        assert_eq!(config.batch_size, 1);
        assert_eq!(config.log_sync, LogSyncMode::Fsync);
        assert!(!config.rand);
    }

    #[test]
    fn parse_insert_random_flag() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert",
            "--root",
            "root",
            "--num",
            "1",
            "--rand",
        ])
        .unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        let config = args
            .resolve(PathBuf::from("root"), IndexMode::None)
            .unwrap();
        assert!(config.rand);
    }

    #[test]
    fn parse_insert_short_flags() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "run",
            "insert",
            "-r",
            "root",
            "-n",
            "10",
            "-v",
            "32",
            "-b",
            "4",
            "-i",
            "unique",
            "-t",
            "2",
            "-s",
            "4",
            "--log-sync",
            "fdatasync",
        ])
        .unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        let config = args
            .resolve(PathBuf::from("root"), IndexMode::Unique)
            .unwrap();
        assert_eq!(config.storage_root, PathBuf::from("root"));
        assert_eq!(config.num, 10);
        assert_eq!(config.value_size, 32);
        assert_eq!(config.batch_size, 4);
        assert_eq!(config.index, IndexMode::Unique);
        assert_eq!(config.threads, 2);
        assert_eq!(config.sessions, 4);
        assert_eq!(config.log_sync, LogSyncMode::Fdatasync);
    }

    #[test]
    fn resolve_root_uses_env_fallback() {
        let cli = Cli::try_parse_from(["doradb-bench", "run", "insert", "--num", "1"]).unwrap();
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
            "insert",
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
        let cli = Cli::try_parse_from(["doradb-bench", "run", "insert", "--num", "1"]).unwrap();
        assert!(cli.resolve_root_with_env(None).is_err());
        assert!(cli.resolve_root_with_env(Some(PathBuf::new())).is_err());
    }
}
