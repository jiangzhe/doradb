use super::merge::{HotEntryRef, execution_error, observe_stop};
use super::{BudgetedVec, SortedRuns};
use crate::error::{RuntimeError, RuntimeOrFatalResult};
#[cfg(feature = "profiling")]
use crate::profiling::clock::Instant;
use error_stack::ResultExt;
use std::cmp::Ordering;
use std::sync::atomic::AtomicBool;

/// A rank's exact prefixes and immediate total-order neighbors.
pub(super) struct MergeCut {
    /// Number of entries consumed by this cut.
    pub(super) rank: usize,
    /// Per-run prefix counts with allocation-lifetime admission.
    pub(super) positions: BudgetedVec<usize>,
    /// Maximum consumed entry, absent at rank zero.
    pub(super) left: Option<HotEntryRef>,
    /// Minimum remaining entry, absent at the final rank.
    pub(super) right: Option<HotEntryRef>,
    /// Elapsed duration of the synchronous search.
    #[cfg(feature = "profiling")]
    pub(super) elapsed_nanos: u64,
}

/// Select an array position directly, without search or merge workspace.
pub(super) fn direct(runs: &SortedRuns, rank: usize) -> RuntimeOrFatalResult<MergeCut> {
    let entries = runs
        .single_run()
        .unwrap_or_else(|| unreachable!("direct cut requires one run"));
    assert!(
        rank <= entries.len(),
        "direct cut exceeds resident run: rank={rank}"
    );
    let mut positions = BudgetedVec::new(&runs.budget);
    positions
        .ensure_capacity(1, "merge boundary positions")
        .change_context(RuntimeError::IndexAccess)?;
    positions.push_reserved(rank);
    Ok(MergeCut {
        rank,
        positions,
        left: rank.checked_sub(1).map(|p| HotEntryRef::new(runs, 0, p)),
        right: (rank < entries.len()).then(|| HotEntryRef::new(runs, 0, rank)),
        #[cfg(feature = "profiling")]
        elapsed_nanos: 0,
    })
}

/// Independently select a prefix from zero in one finite synchronous search.
pub(super) fn co_rank(
    runs: &SortedRuns,
    rank: usize,
    stop: &AtomicBool,
) -> RuntimeOrFatalResult<MergeCut> {
    #[cfg(feature = "profiling")]
    let started = Instant::now();
    let mut positions = BudgetedVec::new(&runs.budget);
    positions
        .ensure_capacity(runs.runs().len(), "merge boundary positions")
        .change_context(RuntimeError::IndexAccess)?;
    for _ in runs.runs() {
        positions
            .push(0, "merge boundary positions")
            .change_context(RuntimeError::IndexAccess)?;
    }
    let mut remaining = rank;
    while remaining != 0 {
        observe_stop(stop)?;
        let active = runs
            .runs()
            .iter()
            .zip(positions.iter())
            .filter(|(run, position)| **position < run.entries().len())
            .count();
        if active == 0 {
            return Err(execution_error("co-rank exceeds input length"));
        }
        let step = remaining.div_ceil(active);
        let mut selected = None;
        for (run, (&position, source)) in positions.iter().zip(runs.runs()).enumerate() {
            let count = step.min(source.entries().len() - position);
            if count != 0 {
                let candidate = (run, position + count - 1);
                if selected
                    .is_none_or(|(best, _)| runs.compare(candidate, best) == Some(Ordering::Less))
                {
                    selected = Some((candidate, count));
                }
            }
        }
        let ((run, _), count) = selected
            .unwrap_or_else(|| unreachable!("positive co-rank remainder has an active run"));
        positions[run] += count;
        remaining -= count;
    }
    let (left, right) = neighbors(runs, &positions);
    Ok(MergeCut {
        rank,
        positions,
        left,
        right,
        #[cfg(feature = "profiling")]
        elapsed_nanos: started.elapsed().as_nanos() as u64,
    })
}

/// Construct a global endpoint in O(K), without searching or submitting a job.
pub(super) fn endpoint(runs: &SortedRuns, rank: usize) -> RuntimeOrFatalResult<MergeCut> {
    let mut positions = BudgetedVec::new(&runs.budget);
    positions
        .ensure_capacity(runs.runs().len(), "merge boundary positions")
        .change_context(RuntimeError::IndexAccess)?;
    for run in runs.runs() {
        positions
            .push(
                if rank == 0 { 0 } else { run.entries().len() },
                "merge boundary positions",
            )
            .change_context(RuntimeError::IndexAccess)?;
    }
    let (left, right) = neighbors(runs, &positions);
    Ok(MergeCut {
        rank,
        positions,
        left,
        right,
        #[cfg(feature = "profiling")]
        elapsed_nanos: 0,
    })
}

fn neighbors(runs: &SortedRuns, positions: &[usize]) -> (Option<HotEntryRef>, Option<HotEntryRef>) {
    let mut left: Option<HotEntryRef> = None;
    let mut right: Option<HotEntryRef> = None;
    for (run, &position) in positions.iter().enumerate() {
        if position != 0 {
            let entry = HotEntryRef::new(runs, run, position - 1);
            if left.is_none_or(|old| entry.compare(runs, old) == Ordering::Greater) {
                left = Some(entry);
            }
        }
        if position != runs.runs()[run].entries().len() {
            let entry = HotEntryRef::new(runs, run, position);
            if right.is_none_or(|old| entry.compare(runs, old) == Ordering::Less) {
                right = Some(entry);
            }
        }
    }
    (left, right)
}

/// Check coverage and monotonicity before any partition consumer is admitted.
pub(super) fn verify(runs: &SortedRuns, cuts: &[MergeCut]) -> RuntimeOrFatalResult<()> {
    for (index, cut) in cuts.iter().enumerate() {
        if cut.positions.len() != runs.runs().len() {
            return Err(execution_error("co-rank vector length mismatch"));
        }
        let mut sum = 0usize;
        for (run, &position) in cut.positions.iter().enumerate() {
            if position > runs.runs()[run].entries().len()
                || (index != 0 && position < cuts[index - 1].positions[run])
            {
                return Err(execution_error(
                    "co-rank prefix outside monotone run bounds",
                ));
            }
            sum = sum
                .checked_add(position)
                .ok_or_else(|| execution_error("co-rank sum overflow"))?;
        }
        if sum != cut.rank || (index != 0 && cut.rank <= cuts[index - 1].rank) {
            return Err(execution_error("co-rank coverage mismatch"));
        }
        if let (Some(left), Some(right)) = (cut.left, cut.right)
            && left.compare(runs, right) != Ordering::Less
        {
            return Err(execution_error("co-rank neighbors are not ordered"));
        }
    }
    Ok(())
}
