use crate::error::{ConfigError, ConfigResult};
use error_stack::Report;

/// Immutable per-index scratch and parallel extraction limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HotIndexBuildConfig {
    /// Maximum accounted bulk scratch bytes retained by one build.
    /// Bookkeeping, bounded worker temporaries, and capture-validation metadata
    /// are excluded; this is not a limit on total process memory.
    pub max_scratch_bytes: usize,
    /// Worker limit; `None` selects the engine pool size during validation.
    pub max_workers: Option<usize>,
    /// Soft page target per run, subject to a four-runs-per-worker cap.
    pub target_pages_per_run: usize,
}

impl Default for HotIndexBuildConfig {
    fn default() -> Self {
        Self {
            max_scratch_bytes: 256 * 1024 * 1024,
            max_workers: None,
            target_pages_per_run: 128,
        }
    }
}

impl HotIndexBuildConfig {
    /// Set the per-build bulk scratch ceiling in bytes.
    pub fn max_scratch_bytes(mut self, bytes: usize) -> Self {
        self.max_scratch_bytes = bytes;
        self
    }

    /// Set the extraction worker limit, or select automatic sizing.
    pub fn max_workers(mut self, workers: Option<usize>) -> Self {
        self.max_workers = workers;
        self
    }

    /// Set the soft page target for each sorted run.
    pub fn target_pages_per_run(mut self, pages: usize) -> Self {
        self.target_pages_per_run = pages;
        self
    }

    /// Validate allocation arithmetic and normalize the automatic worker limit.
    pub(crate) fn validate(&mut self, pool_workers: usize) -> ConfigResult<()> {
        let workers = self.max_workers.unwrap_or(pool_workers);
        for (field, valid) in [
            (
                "max_scratch_bytes",
                (1..=isize::MAX as usize).contains(&self.max_scratch_bytes),
            ),
            (
                "max_workers",
                workers > 0 && workers <= pool_workers && workers.checked_mul(4).is_some(),
            ),
            ("target_pages_per_run", self.target_pages_per_run > 0),
        ] {
            if !valid {
                return Err(Report::new(ConfigError::InvalidHotIndexBuildLimit).attach(format!(
                    "config_field=hot_index_build.{field}, config={self:?}, pool_workers={pool_workers}"
                )));
            }
        }
        self.max_workers = Some(workers);
        Ok(())
    }
}

/// Resident input, packing, and unsettled output limits for one cold index build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColdIndexBuildConfig {
    /// Maximum admitted scratch, including retained input and storage-owned buffers.
    pub max_scratch_bytes: usize,
    /// Maximum leaf workers; `None` selects the engine pool size.
    pub max_workers: Option<usize>,
    /// Maximum queued leaf buffers, excluding worker-held buffers.
    pub max_ready_buffers: usize,
    /// Maximum accepted unsettled writes, including one parent/control slot.
    pub max_in_flight_writes: usize,
}

impl Default for ColdIndexBuildConfig {
    fn default() -> Self {
        Self {
            max_scratch_bytes: 256 * 1024 * 1024,
            max_workers: None,
            max_ready_buffers: 8,
            max_in_flight_writes: 32,
        }
    }
}

impl ColdIndexBuildConfig {
    /// Set the resident scratch ceiling.
    #[inline]
    pub fn max_scratch_bytes(mut self, bytes: usize) -> Self {
        self.max_scratch_bytes = bytes;
        self
    }

    /// Set the construction worker allowance.
    #[inline]
    pub fn max_workers(mut self, workers: Option<usize>) -> Self {
        self.max_workers = workers;
        self
    }

    /// Set queued leaf-buffer capacity.
    #[inline]
    pub fn max_ready_buffers(mut self, buffers: usize) -> Self {
        self.max_ready_buffers = buffers;
        self
    }

    /// Set total accepted-write capacity, including parent progress.
    #[inline]
    pub fn max_in_flight_writes(mut self, writes: usize) -> Self {
        self.max_in_flight_writes = writes;
        self
    }

    /// Normalize limits before any engine filesystem effect.
    pub(crate) fn validate(&mut self, pool_workers: usize) -> ConfigResult<()> {
        let workers = self.max_workers.unwrap_or(pool_workers);
        for (field, value, valid) in [
            (
                "max_scratch_bytes",
                self.max_scratch_bytes,
                (1..=isize::MAX as usize).contains(&self.max_scratch_bytes),
            ),
            (
                "max_workers",
                workers,
                workers > 0
                    && workers <= pool_workers
                    && workers <= isize::MAX as usize / (4 * 65536),
            ),
            (
                "max_ready_buffers",
                self.max_ready_buffers,
                self.max_ready_buffers > 0 && self.max_ready_buffers <= isize::MAX as usize / 65536,
            ),
            (
                "max_in_flight_writes",
                self.max_in_flight_writes,
                self.max_in_flight_writes >= 2
                    && self.max_in_flight_writes <= isize::MAX as usize / 65536,
            ),
        ] {
            if !valid {
                return Err(Report::new(ConfigError::InvalidColdIndexBuildLimit).attach(format!("config_field=cold_index_build.{field}, value={value}, pool_workers={pool_workers}")));
            }
        }
        if self
            .max_ready_buffers
            .checked_add(self.max_in_flight_writes)
            .and_then(|n| n.checked_add(4))
            .and_then(|n| n.checked_mul(65536 + 4096))
            .is_none_or(|bytes| bytes > isize::MAX as usize)
        {
            return Err(Report::new(ConfigError::InvalidColdIndexBuildLimit).attach(format!("config_field=cold_index_build.output_limits, max_ready_buffers={}, max_in_flight_writes={}", self.max_ready_buffers, self.max_in_flight_writes)));
        }
        self.max_workers = Some(workers);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::EngineConfig;
    use tempfile::TempDir;

    /// Purpose: Reject invalid cold build limits before engine filesystem effects.
    /// Expected: All effective defaults survive validation and invalid fields retain the cold-specific typed cause.
    #[test]
    fn cold_limits_and_pure_validation() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("uncreated");
        let config = EngineConfig::default()
            .storage_root(&root)
            .validate_inner()
            .unwrap();
        assert_eq!(
            config.cold_index_build,
            ColdIndexBuildConfig {
                max_workers: Some(2),
                ..ColdIndexBuildConfig::default()
            }
        );
        assert!(!root.exists());
        for invalid in [
            ColdIndexBuildConfig::default().max_scratch_bytes(0),
            ColdIndexBuildConfig::default().max_scratch_bytes(usize::MAX),
            ColdIndexBuildConfig::default().max_workers(Some(0)),
            ColdIndexBuildConfig::default().max_workers(Some(3)),
            ColdIndexBuildConfig::default().max_ready_buffers(0),
            ColdIndexBuildConfig::default().max_ready_buffers(usize::MAX),
            ColdIndexBuildConfig::default().max_in_flight_writes(1),
            ColdIndexBuildConfig::default().max_in_flight_writes(usize::MAX),
        ] {
            let error = EngineConfig::default()
                .storage_root(&root)
                .cold_index_build(invalid)
                .validate_inner()
                .unwrap_err();
            assert_eq!(
                error.current_context(),
                &ConfigError::InvalidColdIndexBuildLimit
            );
            assert!(!root.exists());
        }
    }

    /// Purpose: Normalize automatic workers and reject unusable or overflowing build limits.
    /// Expected: Defaults and explicit limits survive pure validation; invalid limits retain their typed cause.
    #[test]
    fn limits_and_pure_validation() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("uncreated");
        let config = EngineConfig::default()
            .storage_root(&root)
            .validate_inner()
            .unwrap();
        assert!(!root.exists());
        assert_eq!(
            config.hot_index_build,
            HotIndexBuildConfig {
                max_scratch_bytes: 256 * 1024 * 1024,
                max_workers: Some(2),
                target_pages_per_run: 128,
            }
        );
        let mut explicit = HotIndexBuildConfig::default()
            .max_workers(Some(1))
            .target_pages_per_run(usize::MAX);
        explicit.validate(2).unwrap();
        assert_eq!(explicit.max_workers, Some(1));
        for invalid in [
            HotIndexBuildConfig::default().max_scratch_bytes(0),
            HotIndexBuildConfig::default().max_scratch_bytes(usize::MAX),
            HotIndexBuildConfig::default().max_workers(Some(0)),
            HotIndexBuildConfig::default().max_workers(Some(3)),
            HotIndexBuildConfig::default().target_pages_per_run(0),
        ] {
            let error = EngineConfig::default()
                .storage_root(&root)
                .hot_index_build(invalid)
                .validate_inner()
                .unwrap_err();
            assert_eq!(
                error.current_context(),
                &ConfigError::InvalidHotIndexBuildLimit
            );
            assert!(!root.exists());
        }
        assert!(HotIndexBuildConfig::default().validate(usize::MAX).is_err());
    }
}
