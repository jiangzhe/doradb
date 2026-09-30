//! Profiling timestamps share Quanta's process-wide calibration.
//! Operational deadlines must continue to use `std::time::Instant`.
pub(crate) use quanta::Instant;

/// Initialize the shared clock before entering the first reported interval.
pub(crate) fn initialize() {
    let _ = Instant::now();
}
