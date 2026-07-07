use crate::cli::IndexMode;
use crate::error::{BenchError, Result};
use crate::workload::{SessionPlan, bounded_random, seed_state, splitmix64};

const PAYLOAD_SALT: u64 = 0x4d2b_f14a_17b8_7d83;
const RANDOM_KEY_SALT: u64 = 0xd1b5_4a32_d192_ed03;
const UNIQUE_KEY_SALT: u64 = 0x94d0_49bb_1331_11eb;

pub(super) fn generate_keys(
    rand: bool,
    index: IndexMode,
    seed: u64,
    plan: &SessionPlan,
) -> Result<Vec<u64>> {
    if !rand {
        return sequential_keys(plan);
    }
    match index {
        IndexMode::None | IndexMode::NonUnique => random_keys_with_replacement(seed, plan),
        IndexMode::Unique => unique_random_keys(seed, plan),
    }
}

pub(super) fn payload_bytes(key: u64, seed: u64, value_size: usize) -> Vec<u8> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn random_insert_none_is_deterministic() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            rows: 64,
        };
        let first = generate_keys(true, IndexMode::None, 42, &plan).unwrap();
        let second = generate_keys(true, IndexMode::None, 42, &plan).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn random_insert_none_can_generate_duplicates() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            rows: 64,
        };
        let keys = generate_keys(true, IndexMode::None, 2, &plan).unwrap();
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
        let keys = generate_keys(true, IndexMode::NonUnique, 2, &plan).unwrap();
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
        assert!(generate_keys(true, IndexMode::None, 0, &plan).is_err());
    }

    #[test]
    fn random_insert_unique_is_seeded_duplicate_free_coverage() {
        let plan = SessionPlan {
            session_index: 0,
            key_start: 10,
            rows: 64,
        };
        let first = generate_keys(true, IndexMode::Unique, 42, &plan).unwrap();
        let second = generate_keys(true, IndexMode::Unique, 42, &plan).unwrap();
        let different = generate_keys(true, IndexMode::Unique, 43, &plan).unwrap();
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
            generate_keys(false, IndexMode::None, 42, &plan).unwrap(),
            vec![10, 11, 12, 13]
        );
    }

    #[test]
    fn payload_generation_is_deterministic_and_sized() {
        let first = payload_bytes(7, 11, 31);
        let second = payload_bytes(7, 11, 31);
        assert_eq!(first, second);
        assert_eq!(first.len(), 31);
        assert_ne!(first, payload_bytes(8, 11, 31));
    }
}
