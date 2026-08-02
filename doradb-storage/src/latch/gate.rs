use event_listener::{Event, listener};
use parking_lot::Mutex;

/// Async exclusive admission with explicit release.
///
/// Unlike a mutex guard, acquisition returns no borrowed token. Callers must
/// pair every successful [`Self::acquire`] with exactly one [`Self::release`].
/// This permits a higher-level owned scope to transfer the admission across
/// runtime threads while retaining its own resource-lifetime proof.
pub(crate) struct ExclusiveGate {
    active: Mutex<bool>,
    changed: Event,
}

impl ExclusiveGate {
    /// Create an inactive gate.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            active: Mutex::new(false),
            changed: Event::new(),
        }
    }

    /// Wait for and acquire exclusive admission.
    ///
    /// Cancelling while this future is pending does not change gate state.
    /// Once this method returns, the caller owns admission and must release it.
    pub(crate) async fn acquire(&self) {
        if self.try_acquire() {
            return;
        }
        loop {
            listener!(self.changed => changed);
            if self.try_acquire() {
                return;
            }
            changed.await;
        }
    }

    #[inline]
    fn try_acquire(&self) -> bool {
        let mut active = self.active.lock();
        if *active {
            return false;
        }
        *active = true;
        true
    }

    /// Release one successful acquisition and wake current waiters.
    #[inline]
    pub(crate) fn release(&self) {
        let mut active = self.active.lock();
        assert!(*active, "exclusive gate release without active acquisition");
        *active = false;
        drop(active);
        self.changed.notify(usize::MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exclusive_gate_serializes_acquisitions() {
        smol::block_on(async {
            let gate = ExclusiveGate::new();
            gate.acquire().await;
            let mut waiter = Box::pin(gate.acquire());

            assert!(matches!(
                futures::poll!(waiter.as_mut()),
                std::task::Poll::Pending
            ));

            gate.release();
            waiter.await;
            gate.release();
        });
    }

    #[test]
    fn test_exclusive_gate_pending_cancellation_does_not_consume_admission() {
        smol::block_on(async {
            let gate = ExclusiveGate::new();
            gate.acquire().await;
            let mut cancelled = Box::pin(gate.acquire());

            assert!(matches!(
                futures::poll!(cancelled.as_mut()),
                std::task::Poll::Pending
            ));

            drop(cancelled);
            gate.release();
            gate.acquire().await;
            gate.release();
        });
    }

    #[test]
    #[should_panic(expected = "exclusive gate release without active acquisition")]
    fn test_exclusive_gate_rejects_unmatched_release() {
        ExclusiveGate::new().release();
    }
}
