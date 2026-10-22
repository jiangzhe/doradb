use super::index_build::{
    MAX_BUILD_WORKERS, MAX_IN_FLIGHT_WRITES, MAX_INPUT_BYTES, MAX_LOCAL_SCRATCH_BYTES,
    MAX_READY_BUFFERS,
};
use crate::error::{ConfigError, ConfigResult};
use crate::index::build::checkpoint::CheckpointAdmission;
use error_stack::Report;

/// Independent memory and concurrency limits for secondary-index checkpointing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointIndexBuildConfig {
    /// Captured sources, resident runs, extraction/sorting and run-planning metadata, up to 64 GiB.
    pub max_input_bytes: usize,
    /// Scratch and pending-output quota for each active or unassembled worker, up to 1 GiB.
    pub worker_scratch_bytes: usize,
    /// Shared planner, assembler, retained fences and construction bookkeeping, up to 1 GiB.
    pub assembler_scratch_bytes: usize,
    /// Maximum active or unassembled workers, up to 256; `None` selects the pool size capped at 256.
    pub max_workers: Option<usize>,
    /// Maximum queued output buffers, from 1 to 256.
    pub max_ready_buffers: usize,
    /// Maximum accepted unsettled writes, from 2 to 256, including parent progress.
    pub max_in_flight_writes: usize,
}

impl Default for CheckpointIndexBuildConfig {
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

impl CheckpointIndexBuildConfig {
    /// Set the resident input and run-planning allowance.
    #[inline]
    pub fn max_input_bytes(mut self, bytes: usize) -> Self {
        self.max_input_bytes = bytes;
        self
    }

    /// Set each worker's scratch and pending-output allowance.
    #[inline]
    pub fn worker_scratch_bytes(mut self, bytes: usize) -> Self {
        self.worker_scratch_bytes = bytes;
        self
    }

    /// Set shared planning, assembly and bookkeeping capacity.
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
            .checked_add(CheckpointAdmission::output_bytes(
                workers,
                self.worker_scratch_bytes,
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
                CheckpointAdmission::minimum_worker_bytes(),
                MAX_LOCAL_SCRATCH_BYTES,
            ),
            (
                "assembler_scratch_bytes",
                self.assembler_scratch_bytes,
                CheckpointAdmission::minimum_assembler_bytes(),
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
                return Err(Report::new(ConfigError::InvalidCheckpointIndexBuildLimit).attach(format!(
                    "config_field=checkpoint_index_build.{field}, value={value}, supported={minimum}..={maximum}, pool_workers={pool_workers}"
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

    /// Purpose: Validate component minima independently of captured input and output concurrency.
    /// Expected: Exact local minima and a one-byte input budget pass; reducing either scratch minimum fails without filesystem effects or changes to CREATE limits.
    #[test]
    fn independent_limits_and_minima() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("uncreated");
        let worker = CheckpointAdmission::minimum_worker_bytes();
        let assembler = CheckpointAdmission::minimum_assembler_bytes();
        for (workers, ready, writes) in [
            (1, 1, 2),
            (2, 8, 32),
            (2, MAX_READY_BUFFERS, MAX_IN_FLIGHT_WRITES),
        ] {
            let config = CheckpointIndexBuildConfig::default()
                .max_input_bytes(1)
                .worker_scratch_bytes(worker)
                .assembler_scratch_bytes(assembler)
                .max_workers(Some(workers))
                .max_ready_buffers(ready)
                .max_in_flight_writes(writes);
            let validated = EngineConfig::default()
                .storage_root(&root)
                .checkpoint_index_build(config)
                .validate_inner()
                .unwrap();
            assert_eq!(validated.checkpoint_index_build, config);
            assert_eq!(
                validated.cold_index_build.max_input_bytes,
                256 * 1024 * 1024
            );
            assert_eq!(validated.cold_index_build.max_workers, Some(2));
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
                    .checkpoint_index_build(invalid)
                    .validate_inner()
                    .unwrap_err();
                assert_eq!(
                    error.current_context(),
                    &ConfigError::InvalidCheckpointIndexBuildLimit
                );
                assert!(format!("{error:?}").contains(&format!("checkpoint_index_build.{field}")));
            }
            assert!(!root.exists());
        }
    }

    /// Purpose: Reject unusable checkpoint limits before engine filesystem effects.
    /// Expected: Defaults normalize pool-sized workers; invalid fields retain the checkpoint-specific typed cause and field diagnostic.
    #[test]
    fn defaults_and_invalid_fields() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("uncreated");
        let validated = EngineConfig::default()
            .storage_root(&root)
            .validate_inner()
            .unwrap();
        assert_eq!(
            validated.checkpoint_index_build,
            CheckpointIndexBuildConfig {
                max_workers: Some(2),
                ..CheckpointIndexBuildConfig::default()
            }
        );
        for (field, invalid) in [
            (
                "max_input_bytes",
                CheckpointIndexBuildConfig::default().max_input_bytes(0),
            ),
            (
                "max_input_bytes",
                CheckpointIndexBuildConfig::default().max_input_bytes(MAX_INPUT_BYTES + 1),
            ),
            (
                "worker_scratch_bytes",
                CheckpointIndexBuildConfig::default()
                    .worker_scratch_bytes(MAX_LOCAL_SCRATCH_BYTES + 1),
            ),
            (
                "assembler_scratch_bytes",
                CheckpointIndexBuildConfig::default()
                    .assembler_scratch_bytes(MAX_LOCAL_SCRATCH_BYTES + 1),
            ),
            (
                "max_workers",
                CheckpointIndexBuildConfig::default().max_workers(Some(0)),
            ),
            (
                "max_workers",
                CheckpointIndexBuildConfig::default().max_workers(Some(3)),
            ),
            (
                "max_ready_buffers",
                CheckpointIndexBuildConfig::default().max_ready_buffers(0),
            ),
            (
                "max_ready_buffers",
                CheckpointIndexBuildConfig::default().max_ready_buffers(MAX_READY_BUFFERS + 1),
            ),
            (
                "max_in_flight_writes",
                CheckpointIndexBuildConfig::default().max_in_flight_writes(1),
            ),
            (
                "max_in_flight_writes",
                CheckpointIndexBuildConfig::default()
                    .max_in_flight_writes(MAX_IN_FLIGHT_WRITES + 1),
            ),
        ] {
            let error = EngineConfig::default()
                .storage_root(&root)
                .checkpoint_index_build(invalid)
                .validate_inner()
                .unwrap_err();
            assert_eq!(
                error.current_context(),
                &ConfigError::InvalidCheckpointIndexBuildLimit
            );
            assert!(format!("{error:?}").contains(&format!("checkpoint_index_build.{field}")));
            assert!(!root.exists());
        }
    }

    /// Purpose: Report independent checkpoint allowances and validate their supported upper bounds.
    /// Expected: Component maxima validate and contribute to the total, while automatic and explicit workers respect the build limit.
    #[test]
    fn aggregate_capacity_and_upper_bounds() {
        let mut config = CheckpointIndexBuildConfig::default();
        let total = config.total_memory_bytes(2).unwrap();
        assert!(total > (256 + 16 + 12) * 1024 * 1024 + 52 * 65536 + 32 * 1024);
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
        config.validate(2).unwrap();
        assert_eq!(config.total_memory_bytes(8), config.total_memory_bytes(2));
        let mut maximum = CheckpointIndexBuildConfig::default()
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
        assert!(format!("{error:?}").contains("checkpoint_index_build.max_workers"));
    }
}
