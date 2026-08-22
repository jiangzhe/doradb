use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::error::{FatalError, FatalResult, SharedFatalError};
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use error_stack::Report;
#[cfg(test)]
use event_listener::Listener;
use event_listener::{Event, EventListener};
use futures::FutureExt;
use parking_lot::Mutex;
use std::convert::Infallible;
use std::result::Result as StdResult;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};

enum PoisonAwareListenerState {
    Registered(EventListener),
    RecheckOnly,
}

/// Move-only authority to retry work after a poison-aware wait boundary.
///
/// This token exists to make the safety protocol structural. A caller that
/// observes a transient primary condition, such as a foreign transaction in
/// prepare, must not wait only for that condition: engine poison can make the
/// expected primary notification impossible. The caller therefore receives
/// this opaque token instead of a raw [`EventListener`]. Its only production
/// consumer is [`EnginePoisoner::wait_or_poison`], which couples primary
/// progress with sticky engine-health validation.
///
/// The token has two internal states with the same retry contract:
///
/// - registered: primary completion still owes a notification, so consumption
///   races that notification with engine poison and then rechecks health;
/// - recheck-only: primary completion won listener registration, so no primary
///   wait remains, but consumption must still check poison before retrying.
///
/// The raw listener and state are deliberately private. This type implements
/// neither `Future` nor `Clone`, so it cannot be awaited directly or reused to
/// authorize multiple retries. Dropping it is valid cancellation, but a caller
/// that wants to retry normal work must first consume it through the poisoner.
#[must_use = "poison-aware listeners must be consumed before retrying normal work"]
pub(crate) struct PoisonAwareListener {
    state: PoisonAwareListenerState,
}

impl PoisonAwareListener {
    /// Wraps an owed primary notification without exposing a direct wait path.
    #[inline]
    pub(crate) fn registered(listener: EventListener) -> Self {
        Self {
            state: PoisonAwareListenerState::Registered(listener),
        }
    }

    /// Requires a poison recheck when primary completion won registration.
    #[inline]
    pub(crate) fn recheck_only() -> Self {
        Self {
            state: PoisonAwareListenerState::RecheckOnly,
        }
    }

    /// Waits only for the primary notifier in low-level registration tests.
    ///
    /// Production code must use [`EnginePoisoner::wait_or_poison`]. This escape
    /// hatch exists only for tests of the primary registration protocol itself.
    #[cfg(test)]
    #[inline]
    pub(crate) fn wait_primary_for_test(self) {
        match self.state {
            PoisonAwareListenerState::Registered(listener) => listener.wait(),
            PoisonAwareListenerState::RecheckOnly => {
                panic!("recheck-only poison-aware listener has no primary event")
            }
        }
    }
}

/// Engine-level owner of fatal runtime poison state.
///
/// Poison is an admission barrier, not a shutdown mechanism. It prevents new
/// foreground/system work from entering paths that depend on durable
/// consistency, while the engine owner remains responsible for normal explicit
/// shutdown.
pub(crate) struct EnginePoisoner {
    /// Engine runtime poison flag for fatal storage background or durability failures.
    poisoned: AtomicBool,
    /// First source-bearing fatal failure that poisoned runtime admission.
    poison_reason: Mutex<Option<SharedFatalError>>,
    /// One-shot wake for event waits that must notice engine poison.
    poison_event: Event,
    /// Test-only count of sticky health observations.
    #[cfg(test)]
    health_checks: AtomicUsize,
    /// Test-only count of poison-listener registrations.
    #[cfg(test)]
    listener_registrations: AtomicUsize,
    /// Test-only count of prepare-or-poison helper entries.
    #[cfg(test)]
    prepare_wait_entries: AtomicUsize,
}

impl EnginePoisoner {
    /// Create a healthy engine poison component.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            poisoned: AtomicBool::new(false),
            poison_reason: Mutex::new(None),
            poison_event: Event::new(),
            #[cfg(test)]
            health_checks: AtomicUsize::new(0),
            #[cfg(test)]
            listener_registrations: AtomicUsize::new(0),
            #[cfg(test)]
            prepare_wait_entries: AtomicUsize::new(0),
        }
    }

    /// Returns the first fatal engine poison error, if runtime admission has been poisoned.
    #[inline]
    pub(crate) fn poison_error(&self) -> Option<Report<FatalError>> {
        self.shared_poison_error()
            .map(SharedFatalError::into_report)
    }

    /// Returns the cached shared fatal error without reconstructing its report.
    #[inline]
    pub(crate) fn shared_poison_error(&self) -> Option<SharedFatalError> {
        #[cfg(test)]
        self.health_checks.fetch_add(1, Ordering::Relaxed);
        if !self.poisoned.load(Ordering::Acquire) {
            return None;
        }
        let error = self
            .poison_reason
            .lock()
            .as_ref()
            .cloned()
            .expect("engine poison flag must have a stored fatal error");
        Some(error)
    }

    /// Returns `Err` once a fatal engine failure poisoned runtime admission.
    #[inline]
    pub(crate) fn ensure_healthy(&self) -> FatalResult<()> {
        match self.poison_error() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Registers for the one-shot engine poison event.
    #[inline]
    pub(crate) fn listener(&self) -> EventListener {
        #[cfg(test)]
        self.listener_registrations.fetch_add(1, Ordering::Relaxed);
        self.poison_event.listen()
    }

    /// Waits for primary progress without permitting engine poison to be lost.
    ///
    /// A registered primary listener is raced with the one-shot poison event
    /// using sticky health checks before and after the race. A recheck-only
    /// token performs only the sticky health check required before retry.
    #[inline]
    pub(crate) async fn wait_or_poison(&self, listener: PoisonAwareListener) -> FatalResult<()> {
        let PoisonAwareListenerState::Registered(primary_listener) = listener.state else {
            return self.ensure_healthy();
        };

        let poison_listener = self.listener();
        self.ensure_healthy()?;
        let primary_wait = primary_listener.fuse();
        let poison_wait = poison_listener.fuse();
        futures::pin_mut!(primary_wait);
        futures::pin_mut!(poison_wait);
        futures::select! {
            () = primary_wait => (),
            () = poison_wait => (),
        }
        self.ensure_healthy()
    }

    /// Records entry into the row prepare-or-poison slow path.
    #[cfg(test)]
    #[inline]
    pub(crate) fn record_prepare_wait_entry(&self) {
        self.prepare_wait_entries.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns health, listener, and prepare-slow-path observation counts.
    #[cfg(test)]
    #[inline]
    pub(crate) fn test_observation_counts(&self) -> (usize, usize, usize) {
        (
            self.health_checks.load(Ordering::Relaxed),
            self.listener_registrations.load(Ordering::Relaxed),
            self.prepare_wait_entries.load(Ordering::Relaxed),
        )
    }

    /// Records one complete fatal report and returns this caller's shared error.
    ///
    /// The first caller wins admission state, while later poison attempts retain
    /// their local failure. The first reason is stored before the atomic flag is
    /// published so a thread that observes `poisoned == true` can immediately
    /// load the published error.
    #[inline]
    pub(crate) fn poison(&self, report: Report<FatalError>) -> SharedFatalError {
        self.publish_shared(SharedFatalError::capture(report))
    }

    /// Publishes an already captured shared Fatal error without reconstructing it.
    #[inline]
    pub(crate) fn poison_shared(&self, local: SharedFatalError) -> SharedFatalError {
        self.publish_shared(local)
    }

    fn publish_shared(&self, local: SharedFatalError) -> SharedFatalError {
        {
            let mut guard = self.poison_reason.lock();
            if guard.is_none() {
                *guard = Some(local.clone());
            }
        }
        let already_poisoned = self.poisoned.swap(true, Ordering::AcqRel);
        if !already_poisoned {
            self.poison_event.notify(usize::MAX);
        }
        local
    }
}

impl Component for EnginePoisoner {
    type Config = ();
    type Owned = Self;
    type Access = QuiescentGuard<Self>;
    type Error = Infallible;

    const NAME: &'static str = "engine_poisoner";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> StdResult<(), Self::Error> {
        registry.register::<Self>(Self::new());
        Ok(())
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {
        // Panic safety: this passive owner remains available through every
        // earlier component hook that can report fatal runtime state.
    }
}

/// Returns a shared healthy poisoner for tests that never publish poison.
#[cfg(test)]
#[inline]
pub(crate) fn healthy_test_poisoner() -> &'static EnginePoisoner {
    use std::sync::OnceLock;

    static POISONER: OnceLock<EnginePoisoner> = OnceLock::new();
    POISONER.get_or_init(EnginePoisoner::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicBool;
    use std::thread::{sleep, spawn, yield_now};
    use std::time::Duration;

    #[test]
    fn test_poison_stores_reason_before_publishing_flag() {
        let poisoner = Arc::new(EnginePoisoner::new());
        let blocked = poisoner.poison_reason.lock();
        let started = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));

        let worker_started = Arc::clone(&started);
        let worker_finished = Arc::clone(&finished);
        let worker_poisoner = Arc::clone(&poisoner);
        let handle = spawn(move || {
            worker_started.store(true, Ordering::Release);
            let err =
                worker_poisoner.poison(Report::new(FatalError::RedoWrite).attach("blocked poison"));
            worker_finished.store(true, Ordering::Release);
            err
        });

        while !started.load(Ordering::Acquire) {
            yield_now();
        }
        for _ in 0..20 {
            assert!(
                !poisoner.poisoned.load(Ordering::Acquire),
                "poison flag must not publish before poison reason is stored"
            );
            assert!(
                !finished.load(Ordering::Acquire),
                "poison call should remain blocked while poison reason lock is held"
            );
            sleep(Duration::from_millis(1));
        }
        assert!(poisoner.poison_error().is_none());

        drop(blocked);

        let err = handle.join().unwrap().into_report();
        assert_eq!(*err.current_context(), FatalError::RedoWrite);
        assert!(poisoner.poisoned.load(Ordering::Acquire));
        assert!(
            poisoner
                .poison_error()
                .as_ref()
                .is_some_and(|err| *err.current_context() == FatalError::RedoWrite)
        );
        assert!(
            poisoner
                .ensure_healthy()
                .as_ref()
                .is_err_and(|err| *err.current_context() == FatalError::RedoWrite)
        );
    }

    #[test]
    fn test_poison_concurrent_callers_share_first_error() {
        let poisoner = Arc::new(EnginePoisoner::new());
        let barrier = Arc::new(Barrier::new(3));

        let worker_a_barrier = Arc::clone(&barrier);
        let worker_a_poisoner = Arc::clone(&poisoner);
        let worker_a = spawn(move || {
            worker_a_barrier.wait();
            worker_a_poisoner.poison(Report::new(FatalError::RedoWrite).attach("writer"))
        });

        let worker_b_barrier = Arc::clone(&barrier);
        let worker_b_poisoner = Arc::clone(&poisoner);
        let worker_b = spawn(move || {
            worker_b_barrier.wait();
            worker_b_poisoner.poison(Report::new(FatalError::RedoSync).attach("sync"))
        });

        barrier.wait();

        let err_a = worker_a.join().unwrap();
        let err_b = worker_b.join().unwrap();
        let stored_error = poisoner
            .poison_reason
            .lock()
            .as_ref()
            .cloned()
            .expect("poisoned engine must retain the first fatal error");
        let stored = poisoner.poison_error().unwrap();
        let stored_reason = *stored.current_context();

        assert!(poisoner.poisoned.load(Ordering::Acquire));
        assert!(
            stored_error.test_identity() == err_a.test_identity()
                || stored_error.test_identity() == err_b.test_identity()
        );
        assert_eq!(err_a.reason(), FatalError::RedoWrite);
        assert_eq!(err_b.reason(), FatalError::RedoSync);
        assert_eq!(stored_error.reason(), stored_reason);
        assert!(
            poisoner
                .ensure_healthy()
                .as_ref()
                .is_err_and(|err| *err.current_context() == stored_reason)
        );
        assert!(matches!(
            stored_reason,
            FatalError::RedoWrite | FatalError::RedoSync
        ));
    }

    #[test]
    fn test_poison_listener_wakes_first_waiters() {
        smol::block_on(async {
            let poisoner = EnginePoisoner::new();
            let listener = poisoner.listener();
            let waiter = smol::spawn(async move {
                listener.await;
            });

            poisoner.ensure_healthy().unwrap();
            let err =
                poisoner.poison(Report::new(FatalError::CheckpointWrite).attach("checkpoint"));
            let first_identity = err.test_identity();
            assert_eq!(err.reason(), FatalError::CheckpointWrite);
            assert_eq!(
                poisoner
                    .poison_reason
                    .lock()
                    .as_ref()
                    .expect("first poison must store its fatal error")
                    .test_identity(),
                first_identity
            );
            waiter.await;
            assert!(
                poisoner
                    .ensure_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::CheckpointWrite)
            );

            let late_listener = poisoner.listener();
            let err = poisoner.poison(Report::new(FatalError::RedoSync).attach("sync"));
            assert_eq!(err.reason(), FatalError::RedoSync);
            assert_eq!(
                poisoner
                    .poison_reason
                    .lock()
                    .as_ref()
                    .expect("later poison must retain the first fatal error")
                    .reason(),
                FatalError::CheckpointWrite
            );
            assert_eq!(
                poisoner
                    .poison_reason
                    .lock()
                    .as_ref()
                    .expect("later poison must retain the first fatal error")
                    .test_identity(),
                first_identity
            );
            drop(late_listener);
            assert!(
                poisoner
                    .ensure_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::CheckpointWrite)
            );
        });
    }

    #[test]
    fn test_recheck_only_token_checks_health_without_registering_listener() {
        smol::block_on(async {
            let poisoner = EnginePoisoner::new();
            let before = poisoner.test_observation_counts();

            poisoner
                .wait_or_poison(PoisonAwareListener::recheck_only())
                .await
                .unwrap();

            let after = poisoner.test_observation_counts();
            assert_eq!(after.0, before.0 + 1, "recheck-only must inspect health");
            assert_eq!(
                after.1, before.1,
                "recheck-only must not register a poison listener"
            );
        });
    }

    #[test]
    fn test_registered_token_races_primary_with_poison_protocol() {
        smol::block_on(async {
            let poisoner = EnginePoisoner::new();
            let primary = Event::new();
            let token = PoisonAwareListener::registered(primary.listen());
            primary.notify(1);
            let before = poisoner.test_observation_counts();

            poisoner.wait_or_poison(token).await.unwrap();

            let after = poisoner.test_observation_counts();
            assert_eq!(
                after.0,
                before.0 + 2,
                "registered wait must check health before and after selection"
            );
            assert_eq!(
                after.1,
                before.1 + 1,
                "registered wait must install the poison side of the race"
            );
        });
    }
}
