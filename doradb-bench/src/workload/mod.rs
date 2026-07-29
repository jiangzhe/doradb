use crate::cli::{
    LogSyncMode, Workload, validate_batch_size, validate_value_size, validate_workers,
};
use crate::error::Result;
use crate::manifest::{DefaultsManifest, KeyRange, Manifest};
use doradb_storage::Session;
use doradb_storage::id::TableID;
use std::future::Future;

mod ddl;
mod insert;
mod noop;
mod read;
mod util;

pub(super) use ddl::{IndexDdlRunner, TableDdlRunner, benchmark_index_specs, benchmark_table_spec};
pub(super) use insert::{InsertRandRunner, InsertSeqRunner};
pub(super) use noop::{StmtNoopRunner, TrxNoopRunner};
pub(super) use read::{
    IndexScanRunner, IndexStreamRunner, LookupRandRunner, LookupSeqRunner, TableScanRunner,
};
pub(super) use util::build_session_plans;

/// Executes one workload's assigned operations through one public session.
pub(super) trait WorkloadRunner: Clone + Send + Sync {
    /// Resolved configuration for this workload.
    type Config: WorkloadConfig;

    /// Build the workload runner from resolved workload state.
    fn new(config: &Self::Config, table_id: TableID) -> Self;

    /// Run the operations assigned by `plan` without opening or closing `session`.
    fn run<'a>(
        &'a self,
        session: &'a mut Session,
        plan: &'a SessionPlan,
    ) -> impl Future<Output = Result<SessionSummary>> + Send + 'a;
}

/// Resolves and describes one workload independently of CLI dispatch.
pub(super) trait WorkloadConfig: Sized {
    /// Concrete parsed CLI arguments for this workload.
    type Args;

    /// Workload identity used by compatibility checks and benchmark output.
    const WORKLOAD: Workload;

    /// Resolve manifest defaults and runtime ranges from parsed CLI arguments.
    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self>;

    /// Return settings shared by central workload execution.
    fn common(&self) -> &CommonConfig;

    /// Return the aggregate request, row, or iteration count.
    fn operation_count(&self) -> u64;

    /// Return the aggregate range partitioned into session plans.
    fn execution_range(&self) -> KeyRange {
        KeyRange {
            start: 0,
            len: self.operation_count(),
        }
    }

    /// Return the loaded range reported in benchmark output.
    fn output_loaded_range(&self) -> KeyRange;

    /// Whether output should identify this workload as randomized.
    fn random(&self) -> bool {
        false
    }

    /// Return the deterministic seed reported in benchmark output.
    fn seed(&self) -> u64 {
        0
    }

    /// Apply a successful workload's runtime manifest changes.
    fn update_manifest(&self, _manifest: &mut Manifest) -> Result<bool> {
        Ok(false)
    }
}

/// Resolved controls shared by every workload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CommonConfig {
    /// Operating-system executor threads.
    pub(super) threads: usize,
    /// Independent public database sessions.
    pub(super) sessions: usize,
    /// Generated payload size reported for this run.
    pub(super) value_size: usize,
    /// Transaction batch size reported for this run.
    pub(super) batch_size: u64,
    /// Redo-log durability mode.
    pub(super) log_sync: LogSyncMode,
    /// Whether internal engine statistics are captured.
    pub(super) include_stats: bool,
}

impl CommonConfig {
    /// Resolve shared controls from manifest defaults and CLI overrides.
    pub(super) fn resolve(
        defaults: &DefaultsManifest,
        thread_override: Option<usize>,
        session_override: Option<usize>,
        value_size_override: Option<usize>,
        batch_size_override: Option<u64>,
        log_sync: LogSyncMode,
        include_stats: bool,
    ) -> Result<Self> {
        let threads = thread_override.unwrap_or(defaults.threads);
        let sessions = match (thread_override, session_override) {
            (_, Some(sessions)) => sessions,
            (Some(threads), None) => threads,
            (None, None) => defaults.sessions,
        };
        let value_size = value_size_override.unwrap_or(defaults.value_size);
        let batch_size = batch_size_override.unwrap_or(defaults.batch_size);
        validate_workers(threads, sessions)?;
        validate_value_size(value_size)?;
        validate_batch_size(batch_size)?;
        Ok(Self {
            threads,
            sessions,
            value_size,
            batch_size,
            log_sync,
            include_stats,
        })
    }
}

/// Deterministic operation assignment for one benchmark session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SessionPlan {
    /// Zero-based session position in this benchmark run.
    pub(super) session_index: usize,
    /// First logical key or request offset assigned to this session.
    pub(super) key_start: u64,
    /// Number of rows, requests, or iterations assigned to this session.
    pub(super) rows: u64,
}

/// Additive result counters produced by one benchmark session.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct SessionSummary {
    /// Completed logical operations.
    pub(super) operations: u64,
    /// Successfully inserted rows.
    pub(super) inserted_rows: u64,
    /// Read operations that found rows.
    pub(super) found: u64,
    /// Read operations that found no rows.
    pub(super) not_found: u64,
    /// Rows emitted by successful read operations.
    pub(super) rows_returned: u64,
    /// Failures retained for output compatibility.
    pub(super) failures: u64,
}

impl SessionSummary {
    /// Add another session's counters into this summary.
    pub(super) fn merge(&mut self, other: Self) {
        self.operations += other.operations;
        self.inserted_rows += other.inserted_rows;
        self.found += other.found;
        self.not_found += other.not_found;
        self.rows_returned += other.rows_returned;
        self.failures += other.failures;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::MAX_VALUE_SIZE;

    fn defaults() -> DefaultsManifest {
        DefaultsManifest::new(2, 4, 256, 8).unwrap()
    }

    #[test]
    fn common_config_inherits_manifest_defaults() {
        let config = CommonConfig::resolve(
            &defaults(),
            None,
            None,
            None,
            None,
            LogSyncMode::Fsync,
            false,
        )
        .unwrap();
        assert_eq!(config.threads, 2);
        assert_eq!(config.sessions, 4);
        assert_eq!(config.value_size, 256);
        assert_eq!(config.batch_size, 8);
    }

    #[test]
    fn common_config_threads_override_defaults_sessions() {
        let config = CommonConfig::resolve(
            &defaults(),
            Some(3),
            None,
            None,
            None,
            LogSyncMode::None,
            true,
        )
        .unwrap();
        assert_eq!(config.threads, 3);
        assert_eq!(config.sessions, 3);
        assert_eq!(config.log_sync, LogSyncMode::None);
        assert!(config.include_stats);
    }

    #[test]
    fn common_config_rejects_invalid_resolved_controls() {
        assert!(
            CommonConfig::resolve(
                &defaults(),
                Some(2),
                Some(1),
                None,
                None,
                LogSyncMode::Fsync,
                false,
            )
            .is_err()
        );
        assert!(
            CommonConfig::resolve(
                &defaults(),
                None,
                None,
                Some(MAX_VALUE_SIZE + 1),
                None,
                LogSyncMode::Fsync,
                false,
            )
            .is_err()
        );
        assert!(
            CommonConfig::resolve(
                &defaults(),
                None,
                None,
                None,
                Some(0),
                LogSyncMode::Fsync,
                false,
            )
            .is_err()
        );
    }
}
