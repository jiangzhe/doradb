use super::consts::{
    DEFAULT_RECOVERY_ACTIVE_PAGES_PER_TASK, DEFAULT_RECOVERY_DISABLE_DML_VALIDATION,
    DEFAULT_RECOVERY_IO_DEPTH, DEFAULT_RECOVERY_MAX_BATCH_OPS, DEFAULT_RECOVERY_MAX_RECYCLED_BYTES,
    DEFAULT_RECOVERY_TARGET_BATCH_BYTES, DEFAULT_RECOVERY_TASKS_PER_WORKER,
};
use crate::error::{ConfigError, ConfigResult};
use error_stack::Report;

/// Startup recovery I/O, validation, and bounded hot-row replay settings.
///
/// Engine configuration validation resolves automatic limits using the configured
/// worker count. Settings remain fixed during startup and do not affect storage
/// formats. Batch targets and idle capacity caps bound transport storage, excluding
/// whole-group input, insertion history, recovered pages, and allocator overhead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryConfig {
    /// Positive direct-IO read-ahead depth used by startup redo recovery.
    pub io_depth: usize,
    /// Disable DML shape, type, and nullability checks in catalog and row replay.
    pub disable_dml_validation: bool,
    /// Maximum submitted batches whose completions have not been collected.
    ///
    /// `None` selects twice the pool worker count; explicit limits must be positive.
    /// Engine configuration validation replaces `None` with the effective limit.
    pub max_in_flight_batches: Option<usize>,
    /// Maximum pages with pending or submitted replay work.
    ///
    /// `None` selects four times the effective batch-submission limit, including
    /// an explicit override. Validation replaces `None` with the positive limit.
    pub max_active_pages: Option<usize>,
    /// Positive maximum number of operations admitted in one replay batch.
    pub max_batch_ops: usize,
    /// Positive used-storage flush target; larger single operations run alone.
    pub target_batch_bytes: usize,
    /// Maximum aggregate idle vector capacity; zero disables batch recycling.
    pub max_recycled_bytes: usize,
}

impl Default for RecoveryConfig {
    #[inline]
    fn default() -> Self {
        Self {
            io_depth: DEFAULT_RECOVERY_IO_DEPTH,
            disable_dml_validation: DEFAULT_RECOVERY_DISABLE_DML_VALIDATION,
            max_in_flight_batches: None,
            max_active_pages: None,
            max_batch_ops: DEFAULT_RECOVERY_MAX_BATCH_OPS,
            target_batch_bytes: DEFAULT_RECOVERY_TARGET_BATCH_BYTES,
            max_recycled_bytes: DEFAULT_RECOVERY_MAX_RECYCLED_BYTES,
        }
    }
}

impl RecoveryConfig {
    /// Set startup recovery's positive direct-IO read-ahead depth.
    #[inline]
    pub fn io_depth(mut self, io_depth: usize) -> Self {
        self.io_depth = io_depth;
        self
    }

    /// Disable recovery/no-transaction DML shape, type, and nullability validation.
    #[inline]
    pub fn disable_dml_validation(mut self, disable: bool) -> Self {
        self.disable_dml_validation = disable;
        self
    }

    /// Set a positive outstanding-batch limit, or `None` for automatic sizing.
    #[inline]
    pub fn max_in_flight_batches(mut self, limit: Option<usize>) -> Self {
        self.max_in_flight_batches = limit;
        self
    }

    /// Set a positive active-page limit, or `None` for automatic sizing.
    #[inline]
    pub fn max_active_pages(mut self, limit: Option<usize>) -> Self {
        self.max_active_pages = limit;
        self
    }

    /// Set the positive operation limit for one replay batch.
    #[inline]
    pub fn max_batch_ops(mut self, limit: usize) -> Self {
        self.max_batch_ops = limit;
        self
    }

    /// Set the positive used-storage target for page-batch flushing.
    #[inline]
    pub fn target_batch_bytes(mut self, bytes: usize) -> Self {
        self.target_batch_bytes = bytes;
        self
    }

    /// Set the idle capacity cap; zero disables recycling.
    #[inline]
    pub fn max_recycled_bytes(mut self, bytes: usize) -> Self {
        self.max_recycled_bytes = bytes;
        self
    }

    /// Validate limits and resolve automatic sizing using validated pool sizing.
    #[inline]
    pub(crate) fn validate(&mut self, worker_threads: usize) -> ConfigResult<()> {
        if self.io_depth == 0 {
            return Err(Report::new(ConfigError::InvalidIoDepth)
                .attach("config_field=recovery.io_depth, actual=0, min_supported=1"));
        }
        let tasks = self
            .max_in_flight_batches
            .unwrap_or_else(|| worker_threads.saturating_mul(DEFAULT_RECOVERY_TASKS_PER_WORKER));
        let pages = self
            .max_active_pages
            .unwrap_or_else(|| tasks.saturating_mul(DEFAULT_RECOVERY_ACTIVE_PAGES_PER_TASK));
        for (field, value) in [
            ("max_in_flight_batches", tasks),
            ("max_active_pages", pages),
            ("max_batch_ops", self.max_batch_ops),
            ("target_batch_bytes", self.target_batch_bytes),
        ] {
            if value == 0 {
                return Err(
                    Report::new(ConfigError::InvalidRecoveryLimit).attach(format!(
                        "config_field=recovery.{field}, actual={value}, min_supported=1"
                    )),
                );
            }
        }
        self.max_in_flight_batches = Some(tasks);
        self.max_active_pages = Some(pages);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use crate::conf::{EngineConfig, ThreadPoolConfig};
    use tempfile::TempDir;

    #[test]
    fn defaults_and_automatic_limits_preserve_replay_sizing() {
        let default = RecoveryConfig::default();
        assert_eq!(default.io_depth, 32);
        assert!(!default.disable_dml_validation);
        assert_eq!(default.max_in_flight_batches, None);
        assert_eq!(default.max_active_pages, None);
        assert_eq!(default.max_batch_ops, 256);
        assert_eq!(default.target_batch_bytes, 256 * 1024);
        assert_eq!(default.max_recycled_bytes, 16 * 1024 * 1024);

        for (workers, tasks, pages, expected_tasks, expected_pages) in [
            (1, None, None, 2, 8),
            (2, None, None, 4, 16),
            (7, None, None, 14, 56),
            (2, Some(3), None, 3, 12),
            (2, None, Some(1), 4, 1),
            (2, Some(1), Some(9), 1, 9),
            (usize::MAX, None, None, usize::MAX, usize::MAX),
            (2, Some(usize::MAX), None, usize::MAX, usize::MAX),
        ] {
            let config = EngineConfig::default()
                .thread_pool(ThreadPoolConfig::default().worker_threads(workers))
                .recovery(
                    default
                        .clone()
                        .max_in_flight_batches(tasks)
                        .max_active_pages(pages),
                )
                .validate_inner()
                .unwrap();
            assert_eq!(config.recovery.max_in_flight_batches, Some(expected_tasks));
            assert_eq!(config.recovery.max_active_pages, Some(expected_pages));
            let recovery = config.recovery.clone();
            assert_eq!(config.validate_inner().unwrap().recovery, recovery);
        }
    }

    #[test]
    fn positive_limits_and_explicit_auto_reset_are_supported() {
        let mut config = RecoveryConfig::default()
            .io_depth(1)
            .disable_dml_validation(true)
            .max_in_flight_batches(Some(7))
            .max_active_pages(Some(9))
            .max_batch_ops(1)
            .target_batch_bytes(1)
            .max_recycled_bytes(0);
        config.validate(2).unwrap();
        assert_eq!(config.io_depth, 1);
        assert!(config.disable_dml_validation);
        assert_eq!(config.max_batch_ops, 1);
        assert_eq!(config.target_batch_bytes, 1);
        assert_eq!(config.max_recycled_bytes, 0);
        config = config.max_in_flight_batches(None).max_active_pages(None);
        config.validate(3).unwrap();
        assert_eq!(config.max_in_flight_batches, Some(6));
        assert_eq!(config.max_active_pages, Some(24));
    }

    #[test]
    fn invalid_limits_fail_before_bootstrap_creates_storage() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("must-not-be-created");
        for (field, config, expected) in [
            (
                "io_depth",
                RecoveryConfig::default().io_depth(0),
                ConfigError::InvalidIoDepth,
            ),
            (
                "max_in_flight_batches",
                RecoveryConfig::default().max_in_flight_batches(Some(0)),
                ConfigError::InvalidRecoveryLimit,
            ),
            (
                "max_active_pages",
                RecoveryConfig::default().max_active_pages(Some(0)),
                ConfigError::InvalidRecoveryLimit,
            ),
            (
                "max_batch_ops",
                RecoveryConfig::default().max_batch_ops(0),
                ConfigError::InvalidRecoveryLimit,
            ),
            (
                "target_batch_bytes",
                RecoveryConfig::default().target_batch_bytes(0),
                ConfigError::InvalidRecoveryLimit,
            ),
        ] {
            let error = match smol::block_on(Engine::bootstrap(
                EngineConfig::default().storage_root(&root).recovery(config),
            )) {
                Ok(_) => panic!("recovery.{field}=0 must fail bootstrap"),
                Err(error) => error,
            };
            assert_eq!(
                error.report().downcast_ref::<ConfigError>(),
                Some(&expected)
            );
            let diagnostic = format!("{error:?}");
            assert!(
                diagnostic.contains(&format!("recovery.{field}")),
                "{diagnostic}"
            );
            assert!(diagnostic.contains("actual=0"), "{diagnostic}");
            assert!(!root.exists());
        }
    }
}
