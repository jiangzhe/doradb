//! Storage diagnostics and optional component-owned profiling.
//!
//! Stats APIs and measurement types require the default-enabled `profiling`
//! feature. Maintenance operations remain available without it, with operational
//! outcomes but no measurement fields. Disabled builds do not synthesize stats.
//!
//! Profiling arithmetic assumes counts, sizes, and timings fit their numeric types.
//! Duration differences saturate at zero.
//!
//! Enabled timestamps share Quanta 0.13 calibration, initialized before reported
//! bootstrap time. The first profiled startup may pay calibration outside that
//! interval. Operational deadlines use the standard clock. Process CPU and Linux
//! RSS probes run only at an explicit caller's request; no engine sampler starts.
mod buffer;
mod checkpoint;
mod cleanup;
pub(crate) mod clock;
mod index_build;
mod io;
mod lock;
mod metrics;
mod process;
mod recovery;
mod runtime;
mod transaction;

pub use buffer::{BufferPoolCounters, BufferPoolRuntimeStats, BufferPoolStats};
pub use checkpoint::{
    CatalogCheckpointReport, CatalogTableCheckpointChange, CatalogTableCheckpointIoStats,
};
pub use cleanup::{MemIndexCleanupStats, SecondaryMemIndexCleanupIndexStats};
pub use index_build::{
    ColdBuildMeasurements, CreateIndexMeasurements, HotExtractionMeasurements,
    HotIndexMeasurements, IndexBuildStats, MergeMeasurements, RecoveryHotIndexMeasurements,
};
pub use io::{IoBackendStats, StorageIoStats};
pub use lock::LogicalLockStats;
pub use metrics::{InternalMetric, InternalMetricKind, InternalMetricUnit, InternalStatsSnapshot};
pub use process::{ProcessRssSampler, SampledProcessRss, process_cpu_nanos};
pub use recovery::{
    RecoveryMeasurements, RecoveryPhaseMeasurements, RecoveryPhaseTimings,
    RecoveryRedoMeasurements, RecoveryRedoMetrics, RecoveryReport, RecoveryWorkCounts,
};
pub use runtime::{MandatoryRuntimeStats, MandatoryTaskStats};
pub use transaction::TransactionSystemStats;

pub(crate) use buffer::{
    BufferPoolStatsHandle, SharedPoolEvictorStatsHandle, buffer_pool_runtime_stats_snapshot,
};
pub(crate) use checkpoint::{
    CatalogCheckpointMeasurement, CheckpointLwcProfile, MeasurableMutableCowFile,
};
pub(crate) use index_build::{
    ColdHotMeasurements, HotExtractionProfile, HotExtractionWorkerProfile, HotPackedLevel,
    HotPackedMeasurements, IndexBuildProfiler, MergeWorkerProfile, PageMeasurement,
    ParentPlanningProfile,
};
pub(crate) use io::{
    BackendStats, BackendStatsHandle, StorageServiceStats, StorageServiceStatsHandle,
    storage_io_stats_snapshot,
};
pub(crate) use lock::{
    FamilyLockStats, LockManagerStats, add, decrement_current, increment_current,
};
pub(crate) use recovery::{RecoveryHotIndexReport, RowReplayCounts};
pub(crate) use runtime::MandatoryTaskCounters;
pub(crate) use transaction::{RedoLogStats, TrxSysStats, transaction_system_stats_snapshot};

use crate::error::RuntimeError;
#[cfg(test)]
pub(crate) use buffer::SharedPoolEvictorStats;
use error_stack::Report;
use std::time::Duration;

fn measurement_error(message: impl Into<String>) -> Report<RuntimeError> {
    Report::new(RuntimeError::ProfilingMeasurement).attach(message.into())
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos() as u64
}
