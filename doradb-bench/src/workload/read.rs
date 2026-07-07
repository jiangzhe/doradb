use crate::error::{BenchError, Result};
use crate::manifest::KeyRange;
use crate::workload::{SessionPlan, seed_state, splitmix64};

const READ_RANDOM_SALT: u64 = 0x3f8a_e42c_6d13_0b57;

pub(super) fn generate_sequential_keys(
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

pub(super) fn generate_random_keys(
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
            generate_sequential_keys(loaded_range(), &plan).unwrap(),
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
            generate_sequential_keys(loaded_range(), &plan).unwrap(),
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
        let first = generate_random_keys(11, loaded_range(), &plan).unwrap();
        let second = generate_random_keys(11, loaded_range(), &plan).unwrap();
        let different = generate_random_keys(12, loaded_range(), &plan).unwrap();
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
        assert!(generate_sequential_keys(KeyRange { start: 0, len: 0 }, &plan).is_err());
        assert!(generate_random_keys(0, KeyRange { start: 0, len: 0 }, &plan).is_err());
    }
}
