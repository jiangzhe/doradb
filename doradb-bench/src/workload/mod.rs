use crate::cli::IndexMode;
use crate::error::{BenchError, Result};
use crate::manifest::KeyRange;

mod insert;
mod read;

const SPLITMIX_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SessionPlan {
    pub(super) session_index: usize,
    pub(super) key_start: u64,
    pub(super) rows: u64,
}

pub(super) fn build_session_plans(range: KeyRange, sessions: usize) -> Result<Vec<SessionPlan>> {
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

pub(super) fn generate_keys(
    rand: bool,
    index: IndexMode,
    seed: u64,
    plan: &SessionPlan,
) -> Result<Vec<u64>> {
    insert::generate_keys(rand, index, seed, plan)
}

pub(super) fn generate_sequential_read_keys(
    loaded_range: KeyRange,
    plan: &SessionPlan,
) -> Result<Vec<u64>> {
    read::generate_sequential_keys(loaded_range, plan)
}

pub(super) fn generate_random_read_keys(
    seed: u64,
    loaded_range: KeyRange,
    plan: &SessionPlan,
) -> Result<Vec<u64>> {
    read::generate_random_keys(seed, loaded_range, plan)
}

pub(super) fn payload_bytes(key: u64, seed: u64, value_size: usize) -> Vec<u8> {
    insert::payload_bytes(key, seed, value_size)
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
