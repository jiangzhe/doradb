use crate::error::{ConfigError, ConfigResult};
use crate::index::build::disk_builder::DiskBuildAdmission;
use error_stack::Report;

/// Maximum resident input or hot-build scratch allowance: 64 GiB.
pub(super) const MAX_INPUT_BYTES: usize = 64 * 1024 * 1024 * 1024;
/// Maximum per-worker or assembler scratch allowance: 1 GiB.
pub(super) const MAX_LOCAL_SCRATCH_BYTES: usize = 1024 * 1024 * 1024;
/// Maximum workers used by one index build.
pub(super) const MAX_BUILD_WORKERS: usize = 256;
/// Maximum queued output buffers for one index build.
pub(super) const MAX_READY_BUFFERS: usize = 256;
/// Maximum unsettled writes for one index build.
pub(super) const MAX_IN_FLIGHT_WRITES: usize = 256;
const MAX_PAGES_PER_RUN: usize = 8192;

/// Immutable per-index scratch and parallel extraction limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HotIndexBuildConfig {
    /// Maximum accounted bulk scratch bytes retained by one build, up to 64 GiB.
    /// Bookkeeping, bounded worker temporaries, and capture-validation metadata
    /// are excluded; this is not a limit on total process memory.
    pub max_scratch_bytes: usize,
    /// Worker limit, up to 256; `None` selects the pool size capped at 256.
    pub max_workers: Option<usize>,
    /// Soft page target per run, from 1 to 8192, subject to a four-runs-per-worker cap.
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

    /// Validate supported limits and normalize the automatic worker limit.
    pub(crate) fn validate(&mut self, pool_workers: usize) -> ConfigResult<()> {
        let maximum_workers = pool_workers.min(MAX_BUILD_WORKERS);
        let workers = self.max_workers.unwrap_or(maximum_workers);
        for (field, value, maximum) in [
            ("max_scratch_bytes", self.max_scratch_bytes, MAX_INPUT_BYTES),
            ("max_workers", workers, maximum_workers),
            (
                "target_pages_per_run",
                self.target_pages_per_run,
                MAX_PAGES_PER_RUN,
            ),
        ] {
            if !(1..=maximum).contains(&value) {
                return Err(Report::new(ConfigError::InvalidHotIndexBuildLimit).attach(format!(
                    "config_field=hot_index_build.{field}, value={value}, supported=1..={maximum}, pool_workers={pool_workers}"
                )));
            }
        }
        self.max_workers = Some(workers);
        Ok(())
    }
}

/// Independent memory and concurrency limits for cold CREATE extraction and construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColdIndexBuildConfig {
    /// Captured sources, resident runs, extraction/sorting and run-planning metadata, up to 64 GiB.
    pub max_input_bytes: usize,
    /// Extraction and packing scratch per worker slot, including queued merge streams, up to 1 GiB.
    pub worker_scratch_bytes: usize,
    /// Coordinator, parent packing, construction metadata and sealed-tree readers, up to 1 GiB.
    pub assembler_scratch_bytes: usize,
    /// Maximum extraction or construction workers, up to 256; `None` selects the pool size capped at 256.
    pub max_workers: Option<usize>,
    /// Maximum queued output buffers, from 1 to 256.
    pub max_ready_buffers: usize,
    /// Maximum accepted unsettled writes, from 2 to 256, including parent progress.
    pub max_in_flight_writes: usize,
}

impl Default for ColdIndexBuildConfig {
    fn default() -> Self {
        Self {
            max_input_bytes: 256 * 1024 * 1024,
            worker_scratch_bytes: 4 * 1024 * 1024,
            assembler_scratch_bytes: 12 * 1024 * 1024,
            max_workers: None,
            max_ready_buffers: 8,
            max_in_flight_writes: 32,
        }
    }
}

impl ColdIndexBuildConfig {
    /// Set the resident input and run-planning allowance.
    #[inline]
    pub fn max_input_bytes(mut self, bytes: usize) -> Self {
        self.max_input_bytes = bytes;
        self
    }

    /// Set extraction and construction scratch capacity per worker slot.
    #[inline]
    pub fn worker_scratch_bytes(mut self, bytes: usize) -> Self {
        self.worker_scratch_bytes = bytes;
        self
    }

    /// Set shared coordination, parent packing and sealed-reader capacity.
    #[inline]
    pub fn assembler_scratch_bytes(mut self, bytes: usize) -> Self {
        self.assembler_scratch_bytes = bytes;
        self
    }

    /// Set the worker allowance, or select automatic sizing.
    #[inline]
    pub fn max_workers(mut self, workers: Option<usize>) -> Self {
        self.max_workers = workers;
        self
    }

    /// Set queued output capacity.
    #[inline]
    pub fn max_ready_buffers(mut self, buffers: usize) -> Self {
        self.max_ready_buffers = buffers;
        self
    }

    /// Set accepted-write capacity, including parent progress.
    #[inline]
    pub fn max_in_flight_writes(mut self, writes: usize) -> Self {
        self.max_in_flight_writes = writes;
        self
    }

    /// Return the checked aggregate allowance, including derived output capacity.
    /// This is a bound on accounted allocations, not process memory or an extra quota.
    pub fn total_memory_bytes(&self, pool_workers: usize) -> Option<usize> {
        let workers = self
            .max_workers
            .unwrap_or(pool_workers.min(MAX_BUILD_WORKERS));
        self.worker_scratch_bytes
            .checked_mul(workers)?
            .checked_add(self.assembler_scratch_bytes)?
            .checked_add(self.max_input_bytes)?
            .checked_add(DiskBuildAdmission::output_bytes(
                workers,
                self.max_ready_buffers,
                self.max_in_flight_writes,
            )?)
    }

    /// Validate independent component limits and normalize workers before filesystem effects.
    pub(crate) fn validate(&mut self, pool_workers: usize) -> ConfigResult<()> {
        let workers = self
            .max_workers
            .unwrap_or(pool_workers.min(MAX_BUILD_WORKERS));
        for (field, value, minimum, maximum) in [
            ("max_input_bytes", self.max_input_bytes, 1, MAX_INPUT_BYTES),
            (
                "worker_scratch_bytes",
                self.worker_scratch_bytes,
                DiskBuildAdmission::minimum_worker_bytes(),
                MAX_LOCAL_SCRATCH_BYTES,
            ),
            (
                "assembler_scratch_bytes",
                self.assembler_scratch_bytes,
                DiskBuildAdmission::minimum_assembler_bytes(),
                MAX_LOCAL_SCRATCH_BYTES,
            ),
            (
                "max_workers",
                workers,
                1,
                pool_workers.min(MAX_BUILD_WORKERS),
            ),
            (
                "max_ready_buffers",
                self.max_ready_buffers,
                1,
                MAX_READY_BUFFERS,
            ),
            (
                "max_in_flight_writes",
                self.max_in_flight_writes,
                2,
                MAX_IN_FLIGHT_WRITES,
            ),
        ] {
            if !(minimum..=maximum).contains(&value) {
                return Err(Report::new(ConfigError::InvalidColdIndexBuildLimit).attach(format!(
                    "config_field=cold_index_build.{field}, value={value}, supported={minimum}..={maximum}, pool_workers={pool_workers}"
                )));
            }
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
            ColdIndexBuildConfig::default().max_input_bytes(0),
            ColdIndexBuildConfig::default().max_input_bytes(MAX_INPUT_BYTES + 1),
            ColdIndexBuildConfig::default().worker_scratch_bytes(1),
            ColdIndexBuildConfig::default().worker_scratch_bytes(MAX_LOCAL_SCRATCH_BYTES + 1),
            ColdIndexBuildConfig::default().assembler_scratch_bytes(1),
            ColdIndexBuildConfig::default().assembler_scratch_bytes(MAX_LOCAL_SCRATCH_BYTES + 1),
            ColdIndexBuildConfig::default().max_workers(Some(0)),
            ColdIndexBuildConfig::default().max_workers(Some(3)),
            ColdIndexBuildConfig::default().max_ready_buffers(0),
            ColdIndexBuildConfig::default().max_ready_buffers(MAX_READY_BUFFERS + 1),
            ColdIndexBuildConfig::default().max_in_flight_writes(1),
            ColdIndexBuildConfig::default().max_in_flight_writes(MAX_IN_FLIGHT_WRITES + 1),
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

    /// Purpose: Keep cold input and local scratch minima independent of output concurrency.
    /// Expected: Exact component minima and one-byte input pass across supported output concurrency; reducing either scratch minimum fails before filesystem effects.
    #[test]
    fn cold_component_minima_are_independent() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("uncreated");
        for (ready, writes) in [
            (1, 2),
            (3, 2),
            (8, 32),
            (MAX_READY_BUFFERS, MAX_IN_FLIGHT_WRITES),
        ] {
            let worker = DiskBuildAdmission::minimum_worker_bytes();
            let assembler = DiskBuildAdmission::minimum_assembler_bytes();
            let config = ColdIndexBuildConfig::default()
                .max_input_bytes(1)
                .worker_scratch_bytes(worker)
                .assembler_scratch_bytes(assembler)
                .max_ready_buffers(ready)
                .max_in_flight_writes(writes);
            let validated = EngineConfig::default()
                .storage_root(&root)
                .cold_index_build(config)
                .validate_inner()
                .unwrap()
                .cold_index_build;
            assert_eq!(validated.max_input_bytes, 1);
            assert_eq!(validated.worker_scratch_bytes, worker);
            assert_eq!(validated.assembler_scratch_bytes, assembler);
            assert_eq!(validated.max_workers, Some(2));
            for (field, invalid) in [
                (
                    "worker_scratch_bytes",
                    config.worker_scratch_bytes(worker - 1),
                ),
                (
                    "assembler_scratch_bytes",
                    config.assembler_scratch_bytes(assembler - 1),
                ),
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
                assert!(format!("{error:?}").contains(&format!("cold_index_build.{field}")));
            }
            assert!(!root.exists());
        }
    }

    /// Purpose: Report independent cold allowances without an aggregate admission quota.
    /// Expected: Input and assembler changes add directly to the total, component maxima validate, and worker counts stay bounded by both the pool and build limit.
    #[test]
    fn cold_aggregate_capacity_and_upper_bounds() {
        let config = ColdIndexBuildConfig::default();
        let total = config.total_memory_bytes(2).unwrap();
        assert!(total > (256 + 8 + 12) * 1024 * 1024);
        assert_eq!(
            config
                .max_input_bytes(config.max_input_bytes + 7)
                .total_memory_bytes(2),
            Some(total + 7)
        );
        assert_eq!(
            config
                .assembler_scratch_bytes(config.assembler_scratch_bytes + 11)
                .total_memory_bytes(2),
            Some(total + 11)
        );
        let mut maximum = ColdIndexBuildConfig::default()
            .max_input_bytes(MAX_INPUT_BYTES)
            .worker_scratch_bytes(MAX_LOCAL_SCRATCH_BYTES)
            .assembler_scratch_bytes(MAX_LOCAL_SCRATCH_BYTES)
            .max_ready_buffers(MAX_READY_BUFFERS)
            .max_in_flight_writes(MAX_IN_FLIGHT_WRITES);
        maximum.validate(MAX_BUILD_WORKERS + 1).unwrap();
        assert_eq!(maximum.max_workers, Some(MAX_BUILD_WORKERS));
        assert!(maximum.total_memory_bytes(MAX_BUILD_WORKERS).is_some());
        let error = maximum
            .max_workers(Some(MAX_BUILD_WORKERS + 1))
            .validate(MAX_BUILD_WORKERS + 1)
            .unwrap_err();
        assert!(format!("{error:?}").contains("cold_index_build.max_workers"));
    }

    /// Purpose: Normalize automatic workers and enforce supported hot-build limits.
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
            .max_scratch_bytes(MAX_INPUT_BYTES)
            .target_pages_per_run(MAX_PAGES_PER_RUN);
        explicit.validate(2).unwrap();
        assert_eq!(explicit.max_workers, Some(1));
        for invalid in [
            HotIndexBuildConfig::default().max_scratch_bytes(0),
            HotIndexBuildConfig::default().max_scratch_bytes(MAX_INPUT_BYTES + 1),
            HotIndexBuildConfig::default().max_workers(Some(0)),
            HotIndexBuildConfig::default().max_workers(Some(3)),
            HotIndexBuildConfig::default().target_pages_per_run(0),
            HotIndexBuildConfig::default().target_pages_per_run(MAX_PAGES_PER_RUN + 1),
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
        let mut automatic = HotIndexBuildConfig::default();
        automatic.validate(MAX_BUILD_WORKERS + 1).unwrap();
        assert_eq!(automatic.max_workers, Some(MAX_BUILD_WORKERS));
        assert!(
            automatic
                .max_workers(Some(MAX_BUILD_WORKERS + 1))
                .validate(MAX_BUILD_WORKERS + 1)
                .is_err()
        );
    }
}
