//! Detached allocation tracking and caller-driven reclamation.
use super::merge::execution_error;
use super::{BudgetedVec, MemoryBudget};
use crate::buffer::guard::PageExclusiveGuard;
use crate::buffer::{BufferPool, PoolGuard};
use crate::error::{
    FatalError, FatalResult, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult,
    SharedFatalError,
};
use crate::id::PageID;
use crate::index::btree::BTreeNode;
use crate::latch::LatchFallbackMode;
use crate::poison::EnginePoisoner;
use crate::quiescent::QuiescentGuard;
use crate::runtime::yield_now;
use error_stack::ResultExt;
use event_listener::{Event, listener};
use parking_lot::Mutex;
use std::sync::Arc;

#[cfg(test)]
pub(super) use tests::{
    Fault as TestFault, Point as TestPoint, gate as test_gate, packed as test_packed,
    recover as test_recover, remaining as test_remaining,
};

#[cfg(test)]
pub(crate) use tests::{
    Fault as BuildPageFault, Point as BuildPagePoint, gate as gate_build_pages,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Decision {
    Pending,
    Abort,
    Installed,
}

struct PageTracker {
    pages: BudgetedVec<Option<PageID>>,
    producers: usize,
    decision: Decision,
}

struct Staging<P: 'static> {
    page_tracker: Mutex<PageTracker>,
    changed: Event,
    pool: QuiescentGuard<P>,
    guard: PoolGuard,
    poisoner: QuiescentGuard<EnginePoisoner>,
    #[cfg(test)]
    hooks: tests::Hooks,
}

impl<P: 'static> Staging<P> {
    fn abort(&self) {
        let mut page_tracker = self.page_tracker.lock();
        if page_tracker.decision == Decision::Pending {
            page_tracker.decision = Decision::Abort;
        }
        drop(page_tracker);
        self.changed.notify(usize::MAX);
    }

    async fn wait_until_reclaimable(&self) {
        // This is an obligation-drain wait: the decision owner and accepted
        // producers supply progress and notify after changing this predicate.
        // Cleanup ownership is handed to the caller before allocation. That
        // caller must run it through poison and before storage shutdown; no new
        // task admission is needed. Cancelling run() leaves progress in this
        // tracker, and the caller must retain the cleanup object and resume it.
        loop {
            listener!(self.changed => changed);
            let done = {
                let page_tracker = self.page_tracker.lock();
                page_tracker.decision != Decision::Pending && page_tracker.producers == 0
            };
            if done {
                break;
            }
            changed.await;
        }
    }
}

impl<P: BufferPool + 'static> Staging<P> {
    async fn reclaim_pages(&self) -> RuntimeOrFatalResult<()> {
        self.wait_until_reclaimable().await;
        let len = self.page_tracker.lock().pages.len();
        for slot in 0..len {
            let page_id = self.page_tracker.lock().pages[slot];
            if let Some(page_id) = page_id {
                #[cfg(test)]
                self.hooks.before(tests::Point::Reclaim).await?;
                let page = self
                    .pool
                    .get_page::<BTreeNode>(&self.guard, page_id, LatchFallbackMode::Exclusive)
                    .await
                    .map_err(Into::into)?
                    .lock_exclusive_async()
                    .await
                    .unwrap_or_else(|| unreachable!("exclusive staged reopen owns the page latch"));
                self.pool.deallocate_page(page);
                self.page_tracker.lock().pages[slot] = None;
            }
            if slot % 32 == 0 {
                yield_now().await;
            }
        }
        self.page_tracker.lock().pages.clear();
        Ok(())
    }
}

/// One abort-on-drop decision owner, independent of borrowed observer futures.
pub(super) struct StagedPageOwner<P: 'static> {
    state: Arc<Staging<P>>,
}

impl<P: BufferPool + 'static> StagedPageOwner<P> {
    /// Hand off cleanup ownership before allocating any detached page.
    pub(super) fn new(
        pool: QuiescentGuard<P>,
        guard: PoolGuard,
        poisoner: QuiescentGuard<EnginePoisoner>,
        budget: &MemoryBudget,
    ) -> (Self, StagedPageCleanup<P>) {
        let state = Arc::new(Staging {
            page_tracker: Mutex::new(PageTracker {
                pages: BudgetedVec::new(budget),
                producers: 0,
                decision: Decision::Pending,
            }),
            changed: Event::new(),
            pool,
            guard,
            poisoner,
            #[cfg(test)]
            hooks: tests::Hooks::default(),
        });
        let cleanup = StagedPageCleanup {
            state: state.clone(),
            outcome: None,
        };
        (Self { state }, cleanup)
    }

    /// Borrow the pool retaining every detached page through installation.
    #[inline]
    pub(super) fn pool(&self) -> &QuiescentGuard<P> {
        &self.state.pool
    }

    /// Borrow the exact pool guard used by construction and installation.
    #[inline]
    pub(super) fn guard(&self) -> &PoolGuard {
        &self.state.guard
    }

    /// Retain a producer before submitting work that can allocate pages.
    pub(super) fn producer(&self) -> PageProducer<P> {
        let mut page_tracker = self.state.page_tracker.lock();
        assert!(
            page_tracker.decision == Decision::Pending,
            "packed producer admitted after terminal decision"
        );
        page_tracker.producers += 1;
        PageProducer(self.state.clone())
    }

    /// Close construction; accepted producers release their leases before cleanup.
    pub(super) fn abort(&self) {
        self.state.abort();
    }

    /// Disarm descendant reclamation at the synchronous root-transfer edge.
    pub(super) fn transferred(&self) {
        let mut page_tracker = self.state.page_tracker.lock();
        assert!(
            page_tracker.decision == Decision::Pending && page_tracker.producers == 0,
            "packed root transfer requires settled producers and a pending decision"
        );
        page_tracker.pages.clear();
        page_tracker.decision = Decision::Installed;
        drop(page_tracker);
        self.state.changed.notify(usize::MAX);
    }
}

impl<P: 'static> Drop for StagedPageOwner<P> {
    fn drop(&mut self) {
        self.state.abort();
    }
}

/// Caller-owned cleanup obligation, independent of the build and installation destination.
///
/// Retain this object before construction starts. After installation, abort, or
/// dropping the build/ready owner, run it to completion before reporting the
/// operation complete or tearing down storage. Dropping this object does not
/// execute cleanup; the integration caller owns scheduling and cancellation.
#[must_use = "the caller must retain and run staged-page cleanup"]
pub(crate) struct StagedPageCleanup<P: 'static> {
    state: Arc<Staging<P>>,
    outcome: Option<Result<(), SharedFatalError>>,
}

impl<P: BufferPool + 'static> StagedPageCleanup<P> {
    /// Reclaim after the terminal decision and all producers have settled.
    ///
    /// This future can run inline or in a caller-owned task without pool
    /// admission. Cancelling its borrow preserves reclamation progress; retain
    /// this object and call run again. Successful installation disarms page
    /// reclamation. Reopen errors are cached; deallocation invariant panics unwind
    /// directly, and callers must abandon the failed cleanup object without retry.
    pub(crate) async fn run(&mut self) -> FatalResult<()> {
        if self.outcome.is_none() {
            self.outcome = Some(self.reclaim_or_poison().await);
        }
        self.outcome
            .as_ref()
            .unwrap_or_else(|| unreachable!("staged cleanup retains its terminal outcome"))
            .clone()
            .map_err(SharedFatalError::into_report)?;
        if let Some(error) = self.state.poisoner.poison_error() {
            return Err(error);
        }
        Ok(())
    }

    async fn reclaim_or_poison(&self) -> Result<(), SharedFatalError> {
        let failure = match self.state.reclaim_pages().await {
            Ok(()) => return Ok(()),
            Err(RuntimeOrFatalError::Runtime(report)) => {
                report.change_context(FatalError::PurgeDeallocate)
            }
            Err(RuntimeOrFatalError::Fatal(report)) => report,
        }
        .attach("operation=hot_packed_build, phase=staged_cleanup");
        Err(self.state.poisoner.poison(failure))
    }
}

/// Retains construction admission until every accepted producer releases it.
pub(super) struct PageProducer<P: 'static>(Arc<Staging<P>>);

impl<P: BufferPool + 'static> PageProducer<P> {
    /// Reserve a tracking slot before allocation; registration cannot grow or await.
    pub(super) async fn allocate(
        &self,
        _height: u16,
    ) -> RuntimeOrFatalResult<PageExclusiveGuard<BTreeNode>> {
        let slot = {
            let mut page_tracker = self.0.page_tracker.lock();
            if page_tracker.decision != Decision::Pending {
                return Err(execution_error("packed allocation after abort"));
            }
            let slot = page_tracker.pages.len();
            page_tracker
                .pages
                .push(None, "staged page tracking")
                .change_context(RuntimeError::IndexAccess)?;
            slot
        };
        let page = self
            .0
            .pool
            .allocate_page::<BTreeNode>(&self.0.guard)
            .await
            .map_err(Into::into)?;
        self.0.page_tracker.lock().pages[slot] = Some(page.page_id());
        #[cfg(test)]
        self.0
            .hooks
            .before(tests::Point::Allocated(_height))
            .await?;
        Ok(page)
    }
}

impl<P: 'static> Drop for PageProducer<P> {
    fn drop(&mut self) {
        self.0.page_tracker.lock().producers -= 1;
        self.0.changed.notify(usize::MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Semantic stage boundary controlled by allocation and cleanup tests.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(crate) enum Point {
        Allocated(u16),
        Packed(u16),
        Reclaim,
    }

    /// Outcome injected after a test-controlled stage boundary.
    #[derive(Clone, Copy)]
    pub(crate) enum Fault {
        None,
        Runtime,
        Panic,
    }

    #[derive(Clone)]
    struct Gate {
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        fault: Fault,
    }

    /// Build-local deterministic stage gates, compiled only for tests.
    #[derive(Default)]
    pub(crate) struct Hooks {
        counts: Mutex<BTreeMap<Point, usize>>,
        gates: Mutex<BTreeMap<(Point, usize), Gate>>,
    }

    impl Hooks {
        /// Observe the requested semantic predicate before injecting an outcome.
        pub(super) async fn before(&self, point: Point) -> RuntimeOrFatalResult<()> {
            let ordinal = {
                let mut counts = self.counts.lock();
                let count = counts.entry(point).or_default();
                *count += 1;
                *count
            };
            let gate = self.gates.lock().remove(&(point, ordinal));
            if let Some(gate) = gate {
                gate.entered.send(()).unwrap();
                gate.release.recv_async().await.unwrap();
                match gate.fault {
                    Fault::None => (),
                    Fault::Runtime => {
                        return Err(super::execution_error("injected staged failure"));
                    }
                    Fault::Panic => panic!("injected staged panic"),
                }
            }
            Ok(())
        }
    }

    /// Pause an allocation producer or caller-driven cleanup at a semantic edge.
    pub(crate) fn gate<P>(
        cleanup: &StagedPageCleanup<P>,
        point: Point,
        ordinal: usize,
        fault: Fault,
    ) -> (flume::Receiver<()>, flume::Sender<()>) {
        let (entered, receive) = flume::bounded(1);
        let (release, wait) = flume::bounded(1);
        cleanup.state.hooks.gates.lock().insert(
            (point, ordinal),
            Gate {
                entered,
                release: wait,
                fault,
            },
        );
        (receive, release)
    }

    /// Read the tracked unreclaimed page IDs, independent of descriptors.
    pub(crate) fn remaining<P>(cleanup: &StagedPageCleanup<P>) -> Vec<PageID> {
        cleanup
            .state
            .page_tracker
            .lock()
            .pages
            .iter()
            .flatten()
            .copied()
            .collect()
    }

    /// Reclaim test pages after an injected fault known to precede pool access.
    pub(crate) async fn recover<P: BufferPool>(cleanup: &StagedPageCleanup<P>) {
        cleanup.state.reclaim_pages().await.unwrap();
    }

    /// Signal completed page materialization after its page latch has been released.
    pub(crate) async fn packed<P>(
        producer: &PageProducer<P>,
        height: u16,
    ) -> RuntimeOrFatalResult<()> {
        producer.0.hooks.before(Point::Packed(height)).await
    }
}
