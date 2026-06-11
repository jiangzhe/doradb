use event_listener::{Event, Listener};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

/// Wrapper on event to notify all waiters when being dropped.
#[repr(transparent)]
pub(crate) struct EventNotifyOnDrop(Event);

impl EventNotifyOnDrop {
    #[inline]
    pub(crate) fn new() -> Self {
        EventNotifyOnDrop(Event::new())
    }
}

impl Default for EventNotifyOnDrop {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for EventNotifyOnDrop {
    #[inline]
    fn drop(&mut self) {
        self.0.notify(usize::MAX);
    }
}

impl Deref for EventNotifyOnDrop {
    type Target = Event;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Epoch-based notifier for waiters interested in any state change.
pub(crate) struct ChangeNotifier {
    event: Event,
    epoch: AtomicU64,
}

impl ChangeNotifier {
    /// Create a notifier with an initial zero epoch.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            event: Event::new(),
            epoch: AtomicU64::new(0),
        }
    }

    /// Returns the current change epoch.
    #[inline]
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Advance the epoch and wake all current waiters.
    #[inline]
    pub(crate) fn notify(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.event.notify(usize::MAX);
    }

    /// Wait until the epoch differs from `observed_epoch`.
    #[inline]
    pub(crate) fn wait_since(&self, observed_epoch: u64) {
        loop {
            if self.epoch() != observed_epoch {
                return;
            }
            let listener = self.event.listen();
            if self.epoch() != observed_epoch {
                return;
            }
            listener.wait();
        }
    }
}

impl Default for ChangeNotifier {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    #[test]
    fn test_event_notify_on_drop() {
        let ev = EventNotifyOnDrop::default();
        let listener = ev.listen();
        drop(ev);
        // event dropped, so listener.wait() will immediately return.
        listener.wait();
    }

    #[test]
    fn test_change_notifier_wait_since_returns_after_prior_notify() {
        let notifier = ChangeNotifier::new();
        let observed_epoch = notifier.epoch();

        notifier.notify();
        notifier.wait_since(observed_epoch);

        assert_ne!(notifier.epoch(), observed_epoch);
    }

    #[test]
    fn test_change_notifier_wait_since_wakes_after_future_notify() {
        let notifier = Arc::new(ChangeNotifier::new());
        let observed_epoch = notifier.epoch();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let waiter = {
            let notifier = Arc::clone(&notifier);
            std::thread::spawn(move || {
                ready_tx.send(()).expect("waiter should report ready");
                notifier.wait_since(observed_epoch);
                done_tx.send(()).expect("waiter should report completion");
            })
        };

        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("waiter should start");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "waiter should block before a notification"
        );

        notifier.notify();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("waiter should wake after a notification");
        waiter.join().expect("waiter thread should finish");
    }

    #[test]
    fn test_change_notifier_epoch_advances_monotonically() {
        let notifier = ChangeNotifier::new();
        let initial = notifier.epoch();

        notifier.notify();
        assert_eq!(notifier.epoch(), initial + 1);

        notifier.notify();
        assert_eq!(notifier.epoch(), initial + 2);
    }
}
