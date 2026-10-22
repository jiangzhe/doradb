//! Retained write completions shared by CREATE and checkpoint construction.
use super::MemoryReservation;
use crate::error::CompletionResult;
use futures::future::{BoxFuture, pending};
use futures::stream::FuturesUnordered;
use futures::{Future, FutureExt, StreamExt};

/// Bounded accepted writes observed as they complete, independent of submission order.
pub(super) struct PendingWrites<K> {
    futures: FuturesUnordered<BoxFuture<'static, (K, CompletionResult<()>)>>,
    limit: usize,
    _allocation: MemoryReservation,
}

impl<K> PendingWrites<K> {
    /// Retain a single pre-admitted bookkeeping allowance before any write is submitted.
    pub(super) fn new(limit: usize, allocation: MemoryReservation) -> Self {
        // Validated concurrency and the caller's reservation establish this contract.
        assert!(
            limit > 0 && completion_bytes(limit).is_some_and(|bytes| allocation.bytes() >= bytes),
            "pending write collection requires admitted capacity for {limit} writes"
        );
        Self {
            futures: FuturesUnordered::new(),
            limit,
            _allocation: allocation,
        }
    }

    /// Retain an accepted write and the identity needed by its completion owner.
    pub(super) fn push(
        &mut self,
        key: K,
        wait: impl Future<Output = CompletionResult<()>> + Send + 'static,
    ) where
        K: Send + 'static,
    {
        // Callers collect a completion before accepting beyond their configured limit.
        assert!(
            self.len() < self.limit,
            "pending write collection exceeds configured limit {}",
            self.limit
        );
        self.futures.push(async move { (key, wait.await) }.boxed());
    }

    /// Number of accepted writes whose terminal results remain unobserved.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.futures.len()
    }

    /// Whether every accepted write has been observed.
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.futures.is_empty()
    }

    /// Observe any terminal result, remaining pending when no writes are retained.
    /// Storage publishes completion and drains accepted writes during poison/shutdown;
    /// the caller interprets the unchanged result and owns final settlement. Canceling
    /// this borrowed wait leaves every unobserved write retained in the collection.
    pub(super) async fn wait_next(&mut self) -> (K, CompletionResult<()>) {
        if self.is_empty() {
            return pending().await;
        }
        self.futures
            .next()
            .await
            .unwrap_or_else(|| unreachable!("nonempty pending write collection retains its waits"))
    }
}

/// Conservative allowance for completion cells, boxed waits and collection bookkeeping.
/// One reservation covers the collection's lifetime, including its fixed overhead.
#[inline]
pub(super) fn completion_bytes(limit: usize) -> Option<usize> {
    limit.checked_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::completion::Completion;
    use crate::error::{
        CompletionErrorBridge, FatalError, IoError, RuntimeError, RuntimeOrFatalError,
    };
    use crate::index::build::MemoryBudget;
    use error_stack::Report;
    use std::io::ErrorKind;
    use std::sync::Arc;

    fn collection(limit: usize) -> (MemoryBudget, PendingWrites<usize>) {
        let bytes = completion_bytes(limit).unwrap();
        let budget = MemoryBudget::new(bytes);
        let allocation = budget.reserve(bytes, "test write completions").unwrap();
        let writes = PendingWrites::new(limit, allocation);
        (budget, writes)
    }

    fn submit(writes: &mut PendingWrites<usize>, key: usize) -> Arc<Completion<()>> {
        let completion = Arc::new(Completion::new());
        let pending = completion.clone();
        writes.push(key, async move { pending.wait_result().await });
        completion
    }

    /// Purpose: Collect ready writes despite blocked earlier and later submissions, including after canceling a borrowed wait.
    /// Expected: Ready writes free capacity, canceled waits retain their writes, and an empty collection can be refilled without spinning.
    #[test]
    fn ready_writes_survive_wait_cancellation() {
        let (_, mut writes) = collection(3);
        assert!(writes.wait_next().now_or_never().is_none());
        let first = submit(&mut writes, 0);
        let middle = submit(&mut writes, 1);
        let last = submit(&mut writes, 2);
        assert!(writes.wait_next().now_or_never().is_none());
        assert_eq!(writes.len(), 3);
        middle.complete(Ok(()));
        let (key, result) = writes.wait_next().now_or_never().unwrap();
        assert_eq!(key, 1);
        result.unwrap();
        let refill = submit(&mut writes, 3);
        refill.complete(Ok(()));
        let (key, result) = writes.wait_next().now_or_never().unwrap();
        assert_eq!(key, 3);
        result.unwrap();
        assert!(writes.wait_next().now_or_never().is_none());
        first.complete(Ok(()));
        last.complete(Ok(()));
        let mut completed = Vec::new();
        while !writes.is_empty() {
            let (key, result) = writes.wait_next().now_or_never().unwrap();
            result.unwrap();
            completed.push(key);
        }
        completed.sort_unstable();
        assert_eq!(completed, [0, 2]);
        assert!(writes.wait_next().now_or_never().is_none());
        submit(&mut writes, 4).complete(Ok(()));
        let (key, result) = writes.wait_next().now_or_never().unwrap();
        assert_eq!(key, 4);
        result.unwrap();
    }

    /// Purpose: Keep coarse bookkeeping admission independent of write churn and completion order.
    /// Expected: A single allowance remains charged through empty/refill cycles, releases on drop, and rejects insufficient or overflowing capacity.
    #[test]
    fn bookkeeping_reservation_covers_collection_lifetime() {
        let bytes = completion_bytes(3).unwrap();
        assert_eq!(bytes, 3 * 1024);
        assert_eq!(completion_bytes(usize::MAX), None);
        let insufficient = MemoryBudget::new(bytes - 1);
        assert!(
            insufficient
                .reserve(bytes, "test write completions")
                .is_err()
        );
        assert_eq!(insufficient.used(), 0);
        let (budget, mut writes) = collection(3);
        for cycle in 0..20 {
            assert_eq!(budget.used(), bytes);
            for key in 0..3 {
                submit(&mut writes, cycle * 3 + key).complete(Ok(()));
            }
            let mut completed = Vec::new();
            while !writes.is_empty() {
                let (key, result) = writes.wait_next().now_or_never().unwrap();
                result.unwrap();
                completed.push(key);
                assert_eq!(budget.used(), bytes);
            }
            completed.sort_unstable();
            assert_eq!(completed, [cycle * 3, cycle * 3 + 1, cycle * 3 + 2]);
        }
        drop(writes);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Preserve individual success and failure results while draining all sibling writes.
    /// Expected: Every identity is observed once, original error domains and details survive, and failures leave siblings available for settlement.
    #[test]
    fn mixed_results_preserve_completion_authority() {
        let (_, mut writes) = collection(3);
        submit(&mut writes, 0).complete(Ok(()));
        submit(&mut writes, 1).complete(Err(CompletionErrorBridge::capture(
            Report::new(IoError::from(ErrorKind::Other)).attach("write failure"),
        )));
        submit(&mut writes, 2).complete(Err(CompletionErrorBridge::capture(
            Report::new(FatalError::StorageIo).attach("fatal write failure"),
        )));
        let mut seen = [false; 3];
        while !writes.is_empty() {
            let (key, result) = writes.wait_next().now_or_never().unwrap();
            assert!(!seen[key]);
            seen[key] = true;
            match key {
                0 => result.unwrap(),
                1 => {
                    let RuntimeOrFatalError::Runtime(error) = result
                        .unwrap_err()
                        .into_runtime_or_fatal(RuntimeError::IndexAccess)
                    else {
                        panic!("IO write failure must retain its runtime context");
                    };
                    assert!(error.contains::<IoError>());
                    assert!(format!("{error:?}").contains("write failure"));
                }
                2 => {
                    let RuntimeOrFatalError::Fatal(error) = result
                        .unwrap_err()
                        .into_runtime_or_fatal(RuntimeError::IndexAccess)
                    else {
                        panic!("fatal write failure must bypass runtime context");
                    };
                    assert!(format!("{error:?}").contains("fatal write failure"));
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(seen, [true; 3]);
    }
}
