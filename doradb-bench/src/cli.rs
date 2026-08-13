use crate::error::{BenchError, Result};
use clap::Parser;
use std::path::PathBuf;

pub(super) const MAX_VALUE_SIZE: usize = u16::MAX as usize;

/// Top-level DoraDB benchmark command line parser.
#[derive(Debug, Parser)]
#[command(
    name = "doradb-bench",
    about = "Execute a strict DoraDB storage benchmark plan",
    disable_help_subcommand = true
)]
pub struct Cli {
    /// DoraDB storage root; may be supplied by DORADB_BENCH_ROOT.
    #[arg(
        long = "root",
        short = 'r',
        env = "DORADB_BENCH_ROOT",
        value_name = "STORAGE_ROOT"
    )]
    pub root: PathBuf,
    /// Execute a strict TOML benchmark plan directly.
    #[arg(long, short = 'p', value_name = "PLAN_FILE")]
    pub plan: PathBuf,
}

/// Validate executor and public-session counts.
pub(crate) fn validate_workers(threads: usize, sessions: usize) -> Result<()> {
    if threads == 0 {
        return Err(BenchError::message("threads must be positive"));
    }
    if sessions == 0 {
        return Err(BenchError::message("sessions must be positive"));
    }
    if threads > sessions {
        return Err(BenchError::message(format!(
            "threads ({threads}) must not exceed sessions ({sessions})"
        )));
    }
    Ok(())
}

/// Validate the generated payload size.
pub(crate) fn validate_value_size(value_size: usize) -> Result<()> {
    if value_size > MAX_VALUE_SIZE {
        return Err(BenchError::message(format!(
            "value size must not exceed {MAX_VALUE_SIZE} bytes"
        )));
    }
    Ok(())
}

/// Validate a transaction batch size.
pub(crate) fn validate_batch_size(batch_size: u64) -> Result<()> {
    if batch_size == 0 {
        return Err(BenchError::message("batch size must be positive"));
    }
    usize::try_from(batch_size)
        .map_err(|_| BenchError::message("batch size exceeds addressable memory"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[test]
    fn plan_is_the_only_execution_surface() {
        Cli::command().debug_assert();
        let plan =
            Cli::try_parse_from(["doradb-bench", "--root", "root", "--plan", "p.toml"]).unwrap();
        assert_eq!(plan.root, PathBuf::from("root"));
        assert_eq!(plan.plan, PathBuf::from("p.toml"));

        assert!(Cli::try_parse_from(["doradb-bench", "--root", "root"]).is_err());
        assert!(Cli::try_parse_from(["doradb-bench", "--plan", "p.toml"]).is_err());
        assert!(Cli::try_parse_from(["doradb-bench", "--root", "root", "cleanup"]).is_err());
        assert!(Cli::try_parse_from(["doradb-bench", "--root", "root", "prepare"]).is_err());
        assert!(Cli::try_parse_from(["doradb-bench", "--root", "root", "run"]).is_err());
    }

    #[test]
    fn short_options_select_plan_execution() {
        let plan = Cli::try_parse_from(["doradb-bench", "-r", "root", "-p", "p.toml"]).unwrap();
        assert_eq!(plan.root, PathBuf::from("root"));
        assert_eq!(plan.plan, PathBuf::from("p.toml"));
    }

    #[test]
    fn worker_threads_must_not_exceed_sessions() {
        validate_workers(1, 1).unwrap();
        validate_workers(1, 2).unwrap();
        assert_eq!(
            validate_workers(2, 1).unwrap_err().to_string(),
            "threads (2) must not exceed sessions (1)"
        );
    }
}
