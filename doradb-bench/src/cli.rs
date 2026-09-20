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
    use clap::error::ErrorKind;
    use clap::{CommandFactory, Parser};

    /// Purpose: Accept explicit plan execution and reject missing required inputs or legacy
    /// subcommands.
    /// Expected: Long and short options preserve root and plan paths; missing plan or root without
    /// an environment fallback, and cleanup/prepare/run subcommands fail parsing.
    #[test]
    fn plan_is_the_only_execution_surface() {
        Cli::command().debug_assert();
        for (name, root_option, plan_option) in [
            ("long-options", "--root", "--plan"),
            ("short-options", "-r", "-p"),
        ] {
            let plan =
                Cli::try_parse_from(["doradb-bench", root_option, "root", plan_option, "p.toml"])
                    .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(plan.root, PathBuf::from("root"), "{name}");
            assert_eq!(plan.plan, PathBuf::from("p.toml"), "{name}");
        }

        assert!(Cli::try_parse_from(["doradb-bench", "--root", "root"]).is_err());
        let missing_root = Cli::command()
            .mut_arg("root", |arg| arg.env(None::<&str>))
            .try_get_matches_from(["doradb-bench", "--plan", "p.toml"])
            .unwrap_err();
        assert_eq!(missing_root.kind(), ErrorKind::MissingRequiredArgument);
        assert!(Cli::try_parse_from(["doradb-bench", "--root", "root", "cleanup"]).is_err());
        assert!(Cli::try_parse_from(["doradb-bench", "--root", "root", "prepare"]).is_err());
        assert!(Cli::try_parse_from(["doradb-bench", "--root", "root", "run"]).is_err());
    }

    /// Purpose: Validate worker counts at equal, smaller, and excessive thread-to-session ratios.
    /// Expected: One thread with one or two sessions is accepted; two threads with one session
    /// returns the exact ratio error.
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
