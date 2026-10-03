//! Concrete payload and transaction helpers shared by update and upsert workloads.

use crate::error::{BenchError, Result};
use crate::measurement::{LatencyDistribution, MeasurementClock, WorkloadCounters};
use crate::plan_executor::{SessionMeasurement, SessionOutcome};
use crate::workload::util::{generate_payload, merge_measurement};
use doradb_storage::{ErrorKind, Transaction};

const UPDATE_PAYLOAD_SALT: u64 = 0x8c67_29db_4f15_a3e1;

/// Committed mutation counters and transaction latency.
pub(crate) struct MutationSessionOutcome {
    /// Counters and latency from successfully committed batches.
    pub(crate) measurement: SessionMeasurement,
}

impl SessionOutcome for MutationSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: SessionMeasurement {
                counters: WorkloadCounters::default(),
                latency: LatencyDistribution::new()?,
            },
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

/// Settle a batch and publish its counters and latency only after successful commit.
pub(crate) async fn settle_mutation(
    trx: Transaction,
    result: Result<WorkloadCounters>,
    measurement: &mut SessionMeasurement,
    clock: Option<&MeasurementClock>,
    started: Option<u64>,
) -> Result<()> {
    let result = result.and_then(|batch| {
        let mut counters = measurement.counters;
        counters.merge(batch)?;
        Ok(counters)
    });
    let counters = match result {
        Ok(counters) => counters,
        Err(error) => return Err(cleanup_error(error, trx.rollback().await)),
    };
    trx.commit().await?;
    let ended = clock.map(MeasurementClock::raw);
    measurement.counters = counters;
    if let (Some(clock), Some(started), Some(ended)) = (clock, started, ended) {
        measurement
            .latency
            .record(clock.raw_delta_nanos(started, ended)?)?;
    }
    Ok(())
}

/// Preserve the initiating error unless cleanup discovers a fatal engine failure.
pub(crate) fn cleanup_error(
    primary: BenchError,
    cleanup: doradb_storage::Result<()>,
) -> BenchError {
    match cleanup {
        Err(error) if error.is_kind(ErrorKind::Fatal) => BenchError::Storage(error),
        _ => primary,
    }
}

/// Generate deterministic bytes, switching variants when the current payload matches.
pub(crate) fn changed_payload(
    offset: u64,
    seed: u64,
    value_size: usize,
    preferred_variant: bool,
    current: Option<&[u8]>,
) -> Vec<u8> {
    let preferred = generate_update_payload(offset, seed, value_size, preferred_variant);
    if current == Some(preferred.as_slice()) {
        generate_update_payload(offset, seed, value_size, !preferred_variant)
    } else {
        preferred
    }
}

/// Generate the established update payload bytes; callers validate a positive size.
fn generate_update_payload(
    base_offset: u64,
    seed: u64,
    value_size: usize,
    variant: bool,
) -> Vec<u8> {
    let mut payload = generate_payload(
        base_offset,
        seed ^ UPDATE_PAYLOAD_SALT ^ u64::from(variant),
        value_size,
    );
    payload[0] = u8::from(variant);
    payload
}
