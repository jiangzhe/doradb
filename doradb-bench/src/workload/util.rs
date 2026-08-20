use crate::cli::validate_batch_size;
use crate::error::{BenchError, Result};
use crate::fixture::{
    FixtureBinding, FixturePlanEffect, FixtureRuntimeEffect, IndexMode, KeyRange, PrimaryBinding,
};
use crate::measurement::{ExpectedOutcomeCounters, LatencyDistribution, WorkloadCounters};
use crate::plan_executor::SessionMeasurement;
use crate::workload::SessionPlan;
use doradb_storage::id::TableID;
use std::sync::Arc;

const SPLITMIX_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const PAYLOAD_SALT: u64 = 0x4d2b_f14a_17b8_7d83;
const RANDOM_KEY_SALT: u64 = 0xd1b5_4a32_d192_ed03;
const UNIQUE_KEY_SALT: u64 = 0x94d0_49bb_1331_11eb;
const READ_RANDOM_SALT: u64 = 0x3f8a_e42c_6d13_0b57;
const READ_RANGE_SALT: u64 = 0xa76f_c213_9b48_05de;
const TABLE_LOCK_SALT: u64 = 0x6b18_d62f_8f73_a945;

/// Deterministic prepared-table indexes for one session's lock iterations.
pub(super) struct RandomTableIndexGenerator {
    state: u64,
    table_count: u64,
}

impl RandomTableIndexGenerator {
    /// Build a generator for one session plan and prepared table pool.
    pub(super) fn new(seed: u64, table_count: usize, plan: &SessionPlan) -> Result<Self> {
        if table_count == 0 {
            return Err(BenchError::message(
                "lock-table workload requires at least one prepared table",
            ));
        }
        let table_count = u64::try_from(table_count)
            .map_err(|_| BenchError::message("prepared table count exceeds u64"))?;
        Ok(Self {
            state: seed_state(
                seed,
                plan.key_start,
                plan.session_index as u64,
                TABLE_LOCK_SALT,
            ),
            table_count,
        })
    }

    /// Generate the next in-bounds prepared-table index.
    pub(super) fn next_index(&mut self) -> usize {
        (splitmix64(&mut self.state) % self.table_count) as usize
    }
}

/// Deterministic random ranges for one session's scan iterations.
pub(super) struct RandomScanRangeGenerator {
    state: u64,
    loaded_range: KeyRange,
    range_len: u64,
    valid_starts: u64,
}

impl RandomScanRangeGenerator {
    /// Build a generator for one session plan.
    pub(super) fn new(
        seed: u64,
        loaded_range: KeyRange,
        range_len: u64,
        plan: &SessionPlan,
    ) -> Result<Self> {
        validate_loaded_range(loaded_range)?;
        if range_len == 0 {
            return Err(BenchError::message("--range must be positive"));
        }
        if range_len > loaded_range.len {
            return Err(BenchError::message(format!(
                "--range ({range_len}) must not exceed loaded key range length ({})",
                loaded_range.len
            )));
        }
        loaded_range.end()?;
        Ok(Self {
            state: seed_state(
                seed,
                plan.key_start,
                plan.session_index as u64,
                READ_RANGE_SALT,
            ),
            loaded_range,
            range_len,
            valid_starts: loaded_range.len - range_len + 1,
        })
    }

    /// Generate the next half-open logical-key range.
    pub(super) fn next_range(&mut self) -> Result<KeyRange> {
        let offset = splitmix64(&mut self.state) % self.valid_starts;
        let start = self
            .loaded_range
            .start
            .checked_add(offset)
            .ok_or_else(|| BenchError::message("random scan range start overflow"))?;
        let range = KeyRange {
            start,
            len: self.range_len,
        };
        range.end()?;
        Ok(range)
    }
}

/// Partition one aggregate operation range into deterministic session plans.
pub(crate) fn build_session_plans(range: KeyRange, sessions: usize) -> Result<Vec<SessionPlan>> {
    if sessions == 0 {
        return Err(BenchError::message("sessions must be positive"));
    }
    let mut session_plans = Vec::with_capacity(sessions);
    let mut key_start = range.start;
    for session_index in 0..sessions {
        let number = partition_count(range.len, sessions, session_index);
        session_plans.push(SessionPlan {
            session_index,
            key_start,
            number,
        });
        key_start = key_start
            .checked_add(number)
            .ok_or_else(|| BenchError::message("session key range overflow"))?;
    }
    Ok(session_plans)
}

/// Partition a zero-based aggregate operation count across public sessions.
pub(super) fn operation_plans(num: u64, sessions: usize) -> Result<Vec<SessionPlan>> {
    build_session_plans(KeyRange { start: 0, len: num }, sessions)
}

/// Require a workload that consumes no runtime fixture binding.
pub(super) fn require_no_binding(binding: FixtureBinding, identity: &str) -> Result<()> {
    if matches!(binding, FixtureBinding::None) {
        Ok(())
    } else {
        Err(BenchError::message(format!(
            "{identity} received an unexpected fixture binding"
        )))
    }
}

/// Extract the typed primary-table binding required by a workload.
pub(super) fn require_primary(binding: FixtureBinding, identity: &str) -> Result<PrimaryBinding> {
    let FixtureBinding::Primary(primary) = binding else {
        return Err(BenchError::message(format!(
            "{identity} has no primary fixture binding"
        )));
    };
    Ok(primary)
}

/// Extract the ordered table-pool binding required by a workload.
pub(super) fn require_table_pool(
    binding: FixtureBinding,
    identity: &str,
) -> Result<Arc<[TableID]>> {
    let FixtureBinding::TablePool(table_ids) = binding else {
        return Err(BenchError::message(format!(
            "{identity} has no table-pool fixture binding"
        )));
    };
    Ok(table_ids)
}

/// Checked merge of the measurement fields common to typed outcomes.
pub(super) fn merge_measurement(
    aggregate: &mut SessionMeasurement,
    other: SessionMeasurement,
) -> Result<()> {
    aggregate.counters.merge(other.counters)?;
    aggregate.latency.merge(&other.latency)
}

/// Verify the exact latency sample count for one workload execution.
pub(super) fn verify_samples(
    identity: &str,
    latency: &LatencyDistribution,
    expected_samples: u64,
) -> Result<()> {
    if latency.sample_count() == expected_samples {
        Ok(())
    } else {
        Err(BenchError::message(format!(
            "{identity} latency sample count ({}) does not match expected samples ({expected_samples})",
            latency.sample_count()
        )))
    }
}

/// Verify counters for a workload with only a logical operation count.
pub(super) fn verify_simple_counters(
    identity: &str,
    counters: WorkloadCounters,
    operations: u64,
) -> Result<()> {
    if counters.operations != operations
        || counters.inserted_rows != 0
        || counters.updated_rows != 0
        || counters.found != 0
        || counters.not_found != 0
        || counters.rows_returned != 0
        || counters.expected_outcomes != ExpectedOutcomeCounters::default()
    {
        Err(BenchError::message(format!(
            "{identity} produced invalid counters"
        )))
    } else {
        Ok(())
    }
}

/// Reject write counters from a read-only workload.
pub(super) fn verify_no_write_counters(identity: &str, counters: WorkloadCounters) -> Result<()> {
    if counters.inserted_rows != 0
        || counters.updated_rows != 0
        || counters.expected_outcomes != ExpectedOutcomeCounters::default()
    {
        Err(BenchError::message(format!(
            "{identity} produced unexpected write counters"
        )))
    } else {
        Ok(())
    }
}

/// Verify the shared scan/stream counter equations.
pub(super) fn verify_read_shape(
    identity: &str,
    counters: WorkloadCounters,
    operations: u64,
    classified: bool,
) -> Result<()> {
    verify_no_write_counters(identity, counters)?;
    let classification_valid = if classified {
        counters.found.checked_add(counters.not_found) == Some(operations)
    } else {
        counters.found == 0 && counters.not_found == 0
    };
    if counters.operations != operations || !classification_valid {
        Err(BenchError::message(format!(
            "{identity} produced invalid read counters"
        )))
    } else {
        Ok(())
    }
}

/// Verify and return the absence of a fixture transition.
pub(super) fn verify_no_effect(planned_effect: &FixturePlanEffect) -> Result<FixtureRuntimeEffect> {
    if matches!(planned_effect, FixturePlanEffect::None) {
        Ok(FixtureRuntimeEffect::None)
    } else {
        Err(BenchError::message(
            "no-effect workload received a fixture transition",
        ))
    }
}

/// Bound a configured transaction batch size by an operation count.
pub(super) fn effective_batch_size(batch_size: u64, operation_count: u64) -> Result<usize> {
    validate_batch_size(batch_size)?;
    let bounded = batch_size.min(operation_count.max(1));
    usize::try_from(bounded)
        .map_err(|_| BenchError::message("effective batch size exceeds addressable memory"))
}

/// Generate insert keys according to the workload's ordering and index mode.
pub(super) fn generate_insert_keys(
    random: bool,
    index: IndexMode,
    seed: u64,
    plan: &SessionPlan,
) -> Result<Vec<u64>> {
    if !random {
        return sequential_keys(plan);
    }
    match index {
        IndexMode::None | IndexMode::NonUnique => random_keys_with_replacement(seed, plan),
        IndexMode::Unique => unique_random_keys(seed, plan),
    }
}

/// Generate a deterministic payload for one logical key.
pub(super) fn generate_payload(key: u64, seed: u64, value_size: usize) -> Vec<u8> {
    let mut state = seed_state(seed, key, value_size as u64, PAYLOAD_SALT);
    let mut payload = Vec::with_capacity(value_size);
    while payload.len() < value_size {
        let bytes = splitmix64(&mut state).to_le_bytes();
        let remaining = value_size - payload.len();
        let take = remaining.min(bytes.len());
        payload.extend_from_slice(&bytes[..take]);
    }
    payload
}

/// Generate sequential read keys that wrap over the loaded range.
pub(super) fn generate_sequential_read_keys(
    loaded_range: KeyRange,
    plan: &SessionPlan,
) -> Result<Vec<u64>> {
    validate_loaded_range(loaded_range)?;
    let requests = usize::try_from(plan.number)
        .map_err(|_| BenchError::message("session request count exceeds addressable memory"))?;
    let mut keys = Vec::with_capacity(requests);
    for offset in 0..plan.number {
        let request_offset = plan
            .key_start
            .checked_add(offset)
            .ok_or_else(|| BenchError::message("sequential read request overflow"))?;
        keys.push(key_at_loaded_offset(
            loaded_range,
            request_offset % loaded_range.len,
        )?);
    }
    Ok(keys)
}

/// Generate deterministic random read keys within the loaded range.
pub(super) fn generate_random_read_keys(
    seed: u64,
    loaded_range: KeyRange,
    plan: &SessionPlan,
) -> Result<Vec<u64>> {
    validate_loaded_range(loaded_range)?;
    let requests = usize::try_from(plan.number)
        .map_err(|_| BenchError::message("session request count exceeds addressable memory"))?;
    let mut state = seed_state(
        seed,
        plan.key_start,
        plan.session_index as u64,
        READ_RANDOM_SALT,
    );
    let mut keys = Vec::with_capacity(requests);
    for _ in 0..plan.number {
        keys.push(key_at_loaded_offset(
            loaded_range,
            splitmix64(&mut state) % loaded_range.len,
        )?);
    }
    Ok(keys)
}

fn partition_count(total: u64, parts: usize, index: usize) -> u64 {
    let parts_u64 = parts as u64;
    let base = total / parts_u64;
    let remainder = total % parts_u64;
    base + u64::from((index as u64) < remainder)
}

fn bounded_random(state: &mut u64, upper: usize) -> usize {
    debug_assert!(upper > 0);
    (splitmix64(state) % upper as u64) as usize
}

fn seed_state(seed: u64, first: u64, second: u64, salt: u64) -> u64 {
    let mut state = seed ^ first.rotate_left(17) ^ second.rotate_left(31) ^ salt;
    splitmix64(&mut state)
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(SPLITMIX_GAMMA);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn sequential_keys(plan: &SessionPlan) -> Result<Vec<u64>> {
    let rows = usize::try_from(plan.number)
        .map_err(|_| BenchError::message("session row count exceeds addressable memory"))?;
    let mut keys = Vec::with_capacity(rows);
    for offset in 0..plan.number {
        keys.push(
            plan.key_start
                .checked_add(offset)
                .ok_or_else(|| BenchError::message("sequential key overflow"))?,
        );
    }
    Ok(keys)
}

fn random_keys_with_replacement(seed: u64, plan: &SessionPlan) -> Result<Vec<u64>> {
    let rows = usize::try_from(plan.number)
        .map_err(|_| BenchError::message("session row count exceeds addressable memory"))?;
    if rows == 0 {
        return Ok(Vec::new());
    }
    let mut state = seed_state(
        seed,
        plan.key_start,
        plan.session_index as u64,
        RANDOM_KEY_SALT,
    );
    let mut keys = Vec::with_capacity(rows);
    for _ in 0..plan.number {
        let offset = splitmix64(&mut state) % plan.number;
        keys.push(
            plan.key_start
                .checked_add(offset)
                .ok_or_else(|| BenchError::message("random key overflow"))?,
        );
    }
    Ok(keys)
}

fn unique_random_keys(seed: u64, plan: &SessionPlan) -> Result<Vec<u64>> {
    let mut keys = sequential_keys(plan)?;
    let mut state = seed_state(
        seed,
        plan.key_start,
        plan.session_index as u64,
        UNIQUE_KEY_SALT,
    );
    for idx in (1..keys.len()).rev() {
        let swap_idx = bounded_random(&mut state, idx + 1);
        keys.swap(idx, swap_idx);
    }
    Ok(keys)
}

fn validate_loaded_range(loaded_range: KeyRange) -> Result<()> {
    if loaded_range.len == 0 {
        return Err(BenchError::message("loaded key range is empty"));
    }
    Ok(())
}

fn key_at_loaded_offset(loaded_range: KeyRange, offset: u64) -> Result<u64> {
    loaded_range
        .start
        .checked_add(offset)
        .ok_or_else(|| BenchError::message("loaded key overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn loaded_range() -> KeyRange {
        KeyRange { start: 0, len: 3 }
    }

    #[test]
    fn shared_measurement_merge_is_checked_and_additive() {
        let mut aggregate = SessionMeasurement {
            counters: WorkloadCounters {
                operations: 2,
                updated_rows: 2,
                ..WorkloadCounters::default()
            },
            latency: LatencyDistribution::new().unwrap(),
        };
        aggregate.latency.record(10).unwrap();
        let mut other = SessionMeasurement {
            counters: WorkloadCounters {
                operations: 3,
                updated_rows: 3,
                found: 1,
                ..WorkloadCounters::default()
            },
            latency: LatencyDistribution::new().unwrap(),
        };
        other.latency.record(20).unwrap();

        merge_measurement(&mut aggregate, other).unwrap();
        assert_eq!(aggregate.counters.operations, 5);
        assert_eq!(aggregate.counters.updated_rows, 5);
        assert_eq!(aggregate.counters.found, 1);
        assert_eq!(aggregate.latency.sample_count(), 2);
    }

    #[test]
    fn shared_verification_helpers_reject_wrong_shapes() {
        let latency = LatencyDistribution::new().unwrap();
        verify_samples("test", &latency, 0).unwrap();
        assert!(verify_samples("test", &latency, 1).is_err());

        let counters = WorkloadCounters {
            operations: 2,
            ..WorkloadCounters::default()
        };
        verify_simple_counters("test", counters, 2).unwrap();
        assert!(verify_simple_counters("test", counters, 3).is_err());
        assert!(verify_read_shape("test", counters, 2, false).is_ok());
        assert!(verify_read_shape("test", counters, 2, true).is_err());
        let updated = WorkloadCounters {
            operations: 2,
            updated_rows: 1,
            ..WorkloadCounters::default()
        };
        assert!(verify_simple_counters("test", updated, 2).is_err());
        assert!(verify_read_shape("test", updated, 2, false).is_err());

        assert!(verify_no_effect(&FixturePlanEffect::None).is_ok());
        assert!(
            verify_no_effect(&FixturePlanEffect::CreateTables {
                shape: crate::fixture::PrimaryTableShape {
                    index: IndexMode::None,
                },
                table_count: 1,
            })
            .is_err()
        );
    }

    #[test]
    fn shared_binding_helpers_enforce_binding_shapes() {
        require_no_binding(FixtureBinding::None, "none").unwrap();
        assert!(require_primary(FixtureBinding::None, "primary").is_err());
        assert!(require_table_pool(FixtureBinding::None, "pool").is_err());

        let table_ids: Arc<[TableID]> = vec![TableID::new(7), TableID::new(8)].into();
        assert_eq!(
            &*require_table_pool(FixtureBinding::TablePool(Arc::clone(&table_ids)), "pool")
                .unwrap(),
            &*table_ids
        );
    }

    #[test]
    fn partition_rows_across_sessions() {
        let plans = build_session_plans(
            KeyRange {
                start: 100,
                len: 10,
            },
            4,
        )
        .unwrap();
        assert_eq!(plans.len(), 4);
        assert_eq!(plans[0].number, 3);
        assert_eq!(plans[1].number, 3);
        assert_eq!(plans[2].number, 2);
        assert_eq!(plans[3].number, 2);
        assert_eq!(plans[0].key_start, 100);
        assert_eq!(plans[1].key_start, 103);
        assert_eq!(plans[2].key_start, 106);
        assert_eq!(plans[3].key_start, 108);
    }

    #[test]
    fn reject_zero_sessions() {
        assert!(
            build_session_plans(
                KeyRange {
                    start: 100,
                    len: 10,
                },
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn effective_batch_size_uses_configured_limit() {
        assert_eq!(effective_batch_size(3, 10).unwrap(), 3);
        assert_eq!(effective_batch_size(10, 3).unwrap(), 3);
    }

    #[test]
    fn random_insert_none_is_deterministic() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            number: 64,
        };
        let first = generate_insert_keys(true, IndexMode::None, 42, &plan).unwrap();
        let second = generate_insert_keys(true, IndexMode::None, 42, &plan).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn random_insert_none_can_generate_duplicates() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            number: 64,
        };
        let keys = generate_insert_keys(true, IndexMode::None, 2, &plan).unwrap();
        let unique: HashSet<_> = keys.iter().copied().collect();
        assert!(unique.len() < keys.len());
    }

    #[test]
    fn random_insert_non_unique_can_generate_duplicates() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            number: 64,
        };
        let keys = generate_insert_keys(true, IndexMode::NonUnique, 2, &plan).unwrap();
        let unique: HashSet<_> = keys.iter().copied().collect();
        assert!(unique.len() < keys.len());
    }

    #[test]
    fn random_insert_none_rejects_key_overflow() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: u64::MAX,
            number: 2,
        };
        assert!(generate_insert_keys(true, IndexMode::None, 0, &plan).is_err());
    }

    #[test]
    fn random_insert_unique_is_seeded_duplicate_free_coverage() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            number: 64,
        };
        let first = generate_insert_keys(true, IndexMode::Unique, 42, &plan).unwrap();
        let second = generate_insert_keys(true, IndexMode::Unique, 42, &plan).unwrap();
        let different = generate_insert_keys(true, IndexMode::Unique, 43, &plan).unwrap();
        assert_eq!(first, second);
        assert_ne!(first, different);

        let unique: HashSet<_> = first.iter().copied().collect();
        assert_eq!(unique.len(), first.len());
        for key in 10..74 {
            assert!(unique.contains(&key));
        }
    }

    #[test]
    fn random_table_indexes_are_seeded_bounded_and_with_replacement() {
        let plan = SessionPlan {
            session_index: 2,
            key_start: 7,
            number: 64,
        };
        let generate = |seed| {
            let mut generator = RandomTableIndexGenerator::new(seed, 3, &plan).unwrap();
            (0..plan.number)
                .map(|_| generator.next_index())
                .collect::<Vec<_>>()
        };
        let first = generate(11);
        let second = generate(11);
        let different = generate(12);
        assert_eq!(first, second);
        assert_ne!(first, different);
        assert!(first.iter().all(|index| *index < 3));
        assert!(first.iter().copied().collect::<HashSet<_>>().len() < first.len());
    }

    #[test]
    fn random_table_indexes_support_one_table() {
        let plan = SessionPlan {
            session_index: 3,
            key_start: 9,
            number: 4,
        };
        let mut generator = RandomTableIndexGenerator::new(5, 1, &plan).unwrap();
        assert!((0..plan.number).all(|_| generator.next_index() == 0));
    }

    #[test]
    fn sequential_insert_uses_ordered_keys() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            number: 4,
        };
        assert_eq!(
            generate_insert_keys(false, IndexMode::None, 42, &plan).unwrap(),
            vec![10, 11, 12, 13]
        );
    }

    #[test]
    fn payload_generation_is_deterministic_and_sized() {
        let first = generate_payload(7, 11, 31);
        let second = generate_payload(7, 11, 31);
        assert_eq!(first, second);
        assert_eq!(first.len(), 31);
        assert_ne!(first, generate_payload(8, 11, 31));
    }

    #[test]
    fn sequential_reads_wrap_over_loaded_range() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 0,
            number: 8,
        };
        assert_eq!(
            generate_sequential_read_keys(loaded_range(), &plan).unwrap(),
            vec![0, 1, 2, 0, 1, 2, 0, 1]
        );
    }

    #[test]
    fn sequential_reads_use_plan_start_as_request_offset() {
        let plan = SessionPlan {
            session_index: 1,
            key_start: 4,
            number: 4,
        };
        assert_eq!(
            generate_sequential_read_keys(loaded_range(), &plan).unwrap(),
            vec![1, 2, 0, 1]
        );
    }

    #[test]
    fn random_reads_are_seeded_and_bounded() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 0,
            number: 16,
        };
        let first = generate_random_read_keys(11, loaded_range(), &plan).unwrap();
        let second = generate_random_read_keys(11, loaded_range(), &plan).unwrap();
        let different = generate_random_read_keys(12, loaded_range(), &plan).unwrap();
        assert_eq!(first, second);
        assert_ne!(first, different);
        assert!(first.iter().all(|key| *key < loaded_range().len));
    }

    #[test]
    fn random_scan_ranges_are_seeded_and_bounded() {
        let loaded_range = KeyRange { start: 10, len: 8 };
        let plan = SessionPlan {
            session_index: 1,
            key_start: 4,
            number: 16,
        };
        let mut first = RandomScanRangeGenerator::new(11, loaded_range, 3, &plan).unwrap();
        let mut second = RandomScanRangeGenerator::new(11, loaded_range, 3, &plan).unwrap();
        let mut different = RandomScanRangeGenerator::new(12, loaded_range, 3, &plan).unwrap();
        let first_ranges = (0..plan.number)
            .map(|_| first.next_range().unwrap())
            .collect::<Vec<_>>();
        let second_ranges = (0..plan.number)
            .map(|_| second.next_range().unwrap())
            .collect::<Vec<_>>();
        let different_ranges = (0..plan.number)
            .map(|_| different.next_range().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(first_ranges, second_ranges);
        assert_ne!(first_ranges, different_ranges);
        assert!(first_ranges.iter().all(|range| {
            range.len == 3
                && range.start >= loaded_range.start
                && range.end().unwrap() <= loaded_range.end().unwrap()
        }));
        assert!(
            first_ranges
                .iter()
                .any(|range| range.end().unwrap() == loaded_range.end().unwrap())
        );
    }

    #[test]
    fn full_random_scan_range_has_one_valid_start() {
        let loaded_range = KeyRange { start: 10, len: 8 };
        let plan = SessionPlan {
            session_index: 2,
            key_start: 7,
            number: 2,
        };
        let mut ranges = RandomScanRangeGenerator::new(19, loaded_range, 8, &plan).unwrap();
        assert_eq!(ranges.next_range().unwrap(), loaded_range);
        assert_eq!(ranges.next_range().unwrap(), loaded_range);
    }

    #[test]
    fn random_scan_range_rejects_invalid_lengths() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 0,
            number: 1,
        };
        assert!(RandomScanRangeGenerator::new(0, loaded_range(), 0, &plan).is_err());
        assert!(RandomScanRangeGenerator::new(0, loaded_range(), 4, &plan).is_err());
    }

    #[test]
    fn read_generation_rejects_empty_loaded_range() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 0,
            number: 1,
        };
        assert!(generate_sequential_read_keys(KeyRange { start: 0, len: 0 }, &plan).is_err());
        assert!(generate_random_read_keys(0, KeyRange { start: 0, len: 0 }, &plan).is_err());
    }
}
