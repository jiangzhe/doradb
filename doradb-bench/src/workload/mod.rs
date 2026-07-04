use crate::cli::IndexMode;
use crate::error::{BenchError, Result};
use crate::manifest::KeyRange;

mod insert;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SessionPlan {
    pub(super) session_index: usize,
    pub(super) key_start: u64,
    pub(super) rows: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WorkerPlan {
    pub(super) worker_index: usize,
    pub(super) sessions: Vec<SessionPlan>,
}

pub(super) fn build_worker_plans(
    range: KeyRange,
    sessions: usize,
    threads: usize,
) -> Result<Vec<WorkerPlan>> {
    if threads == 0 || sessions == 0 {
        return Err(BenchError::message(
            "threads and sessions must both be positive",
        ));
    }
    if threads > sessions {
        return Err(BenchError::message(
            "threads must not exceed sessions when building worker plans",
        ));
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

    let mut plans = Vec::with_capacity(threads);
    let mut next_session = 0usize;
    for worker_index in 0..threads {
        let session_count = partition_count(sessions as u64, threads, worker_index) as usize;
        let worker_sessions = session_plans[next_session..next_session + session_count].to_vec();
        plans.push(WorkerPlan {
            worker_index,
            sessions: worker_sessions,
        });
        next_session += session_count;
    }
    Ok(plans)
}

pub(super) fn generate_keys(
    rand: bool,
    index: IndexMode,
    seed: u64,
    plan: &SessionPlan,
) -> Result<Vec<u64>> {
    insert::generate_keys(rand, index, seed, plan)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_rows_across_sessions_and_workers() {
        let plans = build_worker_plans(
            KeyRange {
                start: 100,
                len: 10,
            },
            4,
            2,
        )
        .unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].sessions.len(), 2);
        assert_eq!(plans[1].sessions.len(), 2);
        let sessions: Vec<_> = plans
            .iter()
            .flat_map(|worker| worker.sessions.iter())
            .collect();
        assert_eq!(sessions[0].rows, 3);
        assert_eq!(sessions[1].rows, 3);
        assert_eq!(sessions[2].rows, 2);
        assert_eq!(sessions[3].rows, 2);
        assert_eq!(sessions[0].key_start, 100);
        assert_eq!(sessions[1].key_start, 103);
        assert_eq!(sessions[2].key_start, 106);
        assert_eq!(sessions[3].key_start, 108);
    }
}
