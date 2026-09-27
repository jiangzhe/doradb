//! Optional storage profiling, compiled only with the default-enabled `profiling` feature.
//!
//! Disabling the feature removes hot-index-build clocks, records, peak tracking,
//! and publication. Required build resource accounting is independent of profiling.
mod hot_index_build;

pub use hot_index_build::{HotBuildMeasurements, HotIndexBuildStats};
pub(crate) use hot_index_build::{HotBuildProfile, HotBuildWorkerProfile, HotIndexBuildProfiler};
