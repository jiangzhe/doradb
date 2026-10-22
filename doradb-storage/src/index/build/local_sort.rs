//! Shared bounded acceptance and collection for source-specific local-sort jobs.
use super::{BudgetedVec, MemoryBudget};
use crate::completion::Completion;
use crate::error::{CompletionResult, ResourceResult};
use crate::runtime::thread_pool::ThreadPool;
use std::future::Future;
use std::sync::Arc;

/// Completion slots retain accepted jobs, including completed uncollected results.
/// Source adapters own validation, error policy, and reusable workspaces.
pub(crate) struct LocalSortJobs<T> {
    /// Accepted completions indexed by logical source group.
    pub(super) slots: BudgetedVec<Option<Arc<Completion<T>>>>,
    /// Count of groups accepted by the engine pool.
    pub(super) submitted: usize,
    /// Count of logical-head completions already consumed.
    pub(super) collected: usize,
    workers: usize,
}

impl<T: Send + 'static> LocalSortJobs<T> {
    /// Admit the full compact ledger before accepting any source worker.
    pub(crate) fn new(
        budget: &MemoryBudget,
        groups: usize,
        workers: usize,
        purpose: &'static str,
    ) -> ResourceResult<Self> {
        assert!(workers > 0, "local-sort scheduling requires a worker");
        let mut slots = BudgetedVec::new(budget);
        slots.ensure_capacity(groups, purpose)?;
        for _ in 0..groups {
            slots.push_reserved(None);
        }
        Ok(Self {
            slots,
            submitted: 0,
            collected: 0,
            workers,
        })
    }

    /// Number of exact planned source groups, including empty results.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    /// Next logical group awaiting collection.
    #[inline]
    pub(crate) fn collected(&self) -> usize {
        self.collected
    }

    /// Next logical group not yet accepted by the pool.
    #[inline]
    pub(crate) fn submitted(&self) -> usize {
        self.submitted
    }

    /// Whether a result or accepted worker still needs collection.
    #[inline]
    pub(crate) fn pending(&self) -> bool {
        self.collected < self.submitted
    }

    /// Completed-but-uncollected results consume the same allowance as running jobs.
    #[inline]
    pub(crate) fn can_submit(&self) -> bool {
        self.submitted < self.len() && self.submitted - self.collected < self.workers
    }

    /// Accept a finite source job into its already admitted logical slot.
    pub(crate) fn submit(
        &mut self,
        pool: &ThreadPool,
        job: impl Future<Output = T> + Send + 'static,
    ) {
        assert!(
            self.can_submit(),
            "local-sort dispatch exceeds admitted worker allowance"
        );
        self.slots[self.submitted] = Some(pool.submit_async(job));
        self.submitted += 1;
    }

    /// Collect the logical head without surrendering its slot across an await.
    /// Pool admission accepts the obligation; supervised completion is the
    /// authoritative result even during poison or shutdown. The source adapter
    /// stops admission and calls this until all accepted jobs settle. Cancelling
    /// this borrowed wait leaves the same completion owned by this ledger.
    pub(crate) async fn collect(&mut self) -> CompletionResult<T> {
        assert!(
            self.pending(),
            "local-sort collection requires an accepted group"
        );
        let result = self.slots[self.collected]
            .as_ref()
            .unwrap_or_else(|| unreachable!("local-sort accepted group retains completion"))
            .wait_take_result()
            .await;
        self.slots[self.collected] = None;
        self.collected += 1;
        result
    }
}
