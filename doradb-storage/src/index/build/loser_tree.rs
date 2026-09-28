use super::merge::HotEntryRef;
use super::{BudgetedVec, MemoryBudget, SortedHotRuns};
use crate::error::ResourceResult;
use std::cmp::Ordering;

const EMPTY: usize = usize::MAX;

/// One partition's live suffix in a resident run.
#[derive(Clone, Copy)]
struct Cursor {
    position: usize,
    end: usize,
}

/// Tournament losers are retained between pulls; only the winner's path changes.
pub(super) struct LoserTree {
    cursors: BudgetedVec<Cursor>,
    losers: BudgetedVec<usize>,
    winner: usize,
    leaves: usize,
}

impl LoserTree {
    /// Admit all cursor and tournament storage before advancing any entry.
    pub(super) fn new(
        runs: &SortedHotRuns,
        start: &[usize],
        end: &[usize],
        budget: &MemoryBudget,
    ) -> ResourceResult<Self> {
        let mut cursors = BudgetedVec::new(budget);
        cursors.ensure_capacity(start.len(), "merge cursors")?;
        for (&position, &end) in start.iter().zip(end) {
            cursors.push(Cursor { position, end }, "merge cursors")?;
        }
        // A run vector cannot exceed isize::MAX elements, so rounding its
        // length to a power of two is representable in usize.
        let leaves = start.len().next_power_of_two();
        let mut losers = BudgetedVec::new(budget);
        losers.ensure_capacity(leaves, "merge loser tree")?;
        for _ in 0..leaves {
            losers.push(EMPTY, "merge loser tree")?;
        }
        let mut tree = Self {
            cursors,
            losers,
            winner: EMPTY,
            leaves,
        };
        tree.winner = tree.build(runs, 1);
        Ok(tree)
    }

    fn build(&mut self, runs: &SortedHotRuns, node: usize) -> usize {
        if node >= self.leaves {
            let run = node - self.leaves;
            return if run < self.cursors.len() && self.cursors[run].position < self.cursors[run].end
            {
                run
            } else {
                EMPTY
            };
        }
        let left = self.build(runs, node * 2);
        let right = self.build(runs, node * 2 + 1);
        let (winner, loser) = self.match_pair(runs, left, right);
        self.losers[node] = loser;
        winner
    }

    #[inline]
    fn match_pair(&self, runs: &SortedHotRuns, left: usize, right: usize) -> (usize, usize) {
        if right == EMPTY
            || (left != EMPTY
                && runs.compare(
                    (left, self.cursors[left].position),
                    (right, self.cursors[right].position),
                ) == Some(Ordering::Less))
        {
            (left, right)
        } else {
            (right, left)
        }
    }

    /// Advance exactly one head and replay its O(log K) tournament path.
    #[inline]
    pub(super) fn pop(&mut self, runs: &SortedHotRuns) -> Option<HotEntryRef> {
        let run = self.winner;
        if run == EMPTY {
            return None;
        }
        let cursor = &mut self.cursors[run];
        let entry = HotEntryRef::new(runs, run, cursor.position);
        cursor.position += 1;
        let mut winner = if cursor.position == cursor.end {
            EMPTY
        } else {
            run
        };
        let mut node = self.leaves.midpoint(run);
        while node != 0 {
            let (next, loser) = self.match_pair(runs, winner, self.losers[node]);
            self.losers[node] = loser;
            winner = next;
            node /= 2;
        }
        self.winner = winner;
        Some(entry)
    }
}
