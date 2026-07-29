use crate::cli::{IndexMode, validate_batch_size};
use crate::error::{BenchError, Result};
use crate::manifest::KeyRange;
use crate::workload::SessionPlan;

const SPLITMIX_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const PAYLOAD_SALT: u64 = 0x4d2b_f14a_17b8_7d83;
const RANDOM_KEY_SALT: u64 = 0xd1b5_4a32_d192_ed03;
const UNIQUE_KEY_SALT: u64 = 0x94d0_49bb_1331_11eb;
const READ_RANDOM_SALT: u64 = 0x3f8a_e42c_6d13_0b57;

/// Partition one aggregate operation range into deterministic session plans.
pub(crate) fn build_session_plans(range: KeyRange, sessions: usize) -> Result<Vec<SessionPlan>> {
    if sessions == 0 {
        return Err(BenchError::message("sessions must be positive"));
    }
    let mut session_plans = Vec::with_capacity(sessions);
    let mut key_start = range.start;
    for session_index in 0..sessions {
        let rows = partition_count(range.len, sessions, session_index);
        session_plans.push(SessionPlan {
            session_index,
            key_start,
            rows,
        });
        key_start = key_start
            .checked_add(rows)
            .ok_or_else(|| BenchError::message("session key range overflow"))?;
    }
    Ok(session_plans)
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
    let requests = usize::try_from(plan.rows)
        .map_err(|_| BenchError::message("session request count exceeds addressable memory"))?;
    let mut keys = Vec::with_capacity(requests);
    for offset in 0..plan.rows {
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
    let requests = usize::try_from(plan.rows)
        .map_err(|_| BenchError::message("session request count exceeds addressable memory"))?;
    let mut state = seed_state(
        seed,
        plan.key_start,
        plan.session_index as u64,
        READ_RANDOM_SALT,
    );
    let mut keys = Vec::with_capacity(requests);
    for _ in 0..plan.rows {
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
    let rows = usize::try_from(plan.rows)
        .map_err(|_| BenchError::message("session row count exceeds addressable memory"))?;
    let mut keys = Vec::with_capacity(rows);
    for offset in 0..plan.rows {
        keys.push(
            plan.key_start
                .checked_add(offset)
                .ok_or_else(|| BenchError::message("sequential key overflow"))?,
        );
    }
    Ok(keys)
}

fn random_keys_with_replacement(seed: u64, plan: &SessionPlan) -> Result<Vec<u64>> {
    let rows = usize::try_from(plan.rows)
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
    for _ in 0..plan.rows {
        let offset = splitmix64(&mut state) % plan.rows;
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
        assert_eq!(plans[0].rows, 3);
        assert_eq!(plans[1].rows, 3);
        assert_eq!(plans[2].rows, 2);
        assert_eq!(plans[3].rows, 2);
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
            rows: 64,
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
            rows: 64,
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
            rows: 64,
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
            rows: 2,
        };
        assert!(generate_insert_keys(true, IndexMode::None, 0, &plan).is_err());
    }

    #[test]
    fn random_insert_unique_is_seeded_duplicate_free_coverage() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            rows: 64,
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
    fn sequential_insert_uses_ordered_keys() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            rows: 4,
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

    fn loaded_range() -> KeyRange {
        KeyRange { start: 0, len: 3 }
    }

    #[test]
    fn sequential_reads_wrap_over_loaded_range() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 0,
            rows: 8,
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
            rows: 4,
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
            rows: 16,
        };
        let first = generate_random_read_keys(11, loaded_range(), &plan).unwrap();
        let second = generate_random_read_keys(11, loaded_range(), &plan).unwrap();
        let different = generate_random_read_keys(12, loaded_range(), &plan).unwrap();
        assert_eq!(first, second);
        assert_ne!(first, different);
        assert!(first.iter().all(|key| *key < loaded_range().len));
    }

    #[test]
    fn read_generation_rejects_empty_loaded_range() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 0,
            rows: 1,
        };
        assert!(generate_sequential_read_keys(KeyRange { start: 0, len: 0 }, &plan).is_err());
        assert!(generate_random_read_keys(0, KeyRange { start: 0, len: 0 }, &plan).is_err());
    }
}
