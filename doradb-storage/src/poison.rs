use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::error::{FatalError, FatalResult, SharedFatalError};
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use error_stack::Report;
use event_listener::{Event, EventListener};
use parking_lot::Mutex;
use std::convert::Infallible;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicBool, Ordering};

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
}

impl EnginePoisoner {
    /// Create a healthy engine poison component.
    #[inline]
    fn new() -> Self {
        Self {
            poisoned: AtomicBool::new(false),
            poison_reason: Mutex::new(None),
            poison_event: Event::new(),
        }
    }

    /// Returns the first fatal engine poison error, if runtime admission has been poisoned.
    #[inline]
    pub(crate) fn poison_error(&self) -> Option<Report<FatalError>> {
        if !self.poisoned.load(Ordering::Acquire) {
            return None;
        }
        let error = self
            .poison_reason
            .lock()
            .as_ref()
            .cloned()
            .expect("engine poison flag must have a stored fatal error");
        Some(error.into_report())
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
        self.poison_event.listen()
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
    fn shutdown(_component: &Self::Owned) {}
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
}
