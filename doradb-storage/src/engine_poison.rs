use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::error::{FatalError, FatalResult, Result};
use crate::obs;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use crossbeam_utils::CachePadded;
use error_stack::Report;
use event_listener::{Event, EventListener};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// First fatal engine poison reason published to future admission checks.
#[derive(Debug, Clone)]
pub(crate) struct EnginePoisonReason {
    reason: FatalError,
    component: &'static str,
    context: String,
}

impl EnginePoisonReason {
    #[inline]
    fn report(&self) -> Report<FatalError> {
        Report::new(self.reason)
            .attach(format!("component={}", self.component))
            .attach(self.context.clone())
    }
}

/// Engine-level owner of fatal runtime poison state.
///
/// Poison is an admission barrier, not a shutdown mechanism. It prevents new
/// foreground/system work from entering paths that depend on durable
/// consistency, while the engine owner remains responsible for normal explicit
/// shutdown.
pub(crate) struct EnginePoisoner {
    /// Engine-runtime poison flag for fatal storage background or durability failures.
    poisoned: CachePadded<AtomicBool>,
    /// First fatal engine reason and diagnostic context that poisoned runtime admission.
    poison_reason: CachePadded<Mutex<Option<EnginePoisonReason>>>,
    /// One-shot wake for event waits that must notice engine poison.
    poison_event: CachePadded<Event>,
}

impl EnginePoisoner {
    /// Create a healthy engine poison component.
    #[inline]
    fn new() -> Self {
        Self {
            poisoned: CachePadded::new(AtomicBool::new(false)),
            poison_reason: CachePadded::new(Mutex::new(None)),
            poison_event: CachePadded::new(Event::new()),
        }
    }

    /// Returns the first fatal engine poison error, if runtime admission has been poisoned.
    #[inline]
    pub(crate) fn poison_error(&self) -> Option<Report<FatalError>> {
        if !self.poisoned.load(Ordering::Acquire) {
            return None;
        }
        let guard = self.poison_reason.lock();
        debug_assert!(
            guard.is_some(),
            "engine poison flag published before poison reason was stored"
        );
        guard.as_ref().map(EnginePoisonReason::report)
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

    /// Records the first fatal poison reason and returns the published poison error.
    ///
    /// The first caller wins: later poison attempts return the already recorded
    /// reason. The reason and context are stored before the atomic flag is
    /// published so a thread that observes `poisoned == true` can immediately
    /// load a meaningful error.
    #[inline]
    pub(crate) fn poison(
        &self,
        reason: FatalError,
        component: &'static str,
        context: impl Into<String>,
    ) -> Report<FatalError> {
        let attempted_context = context.into();
        {
            let mut guard = self.poison_reason.lock();
            if guard.is_none() {
                *guard = Some(EnginePoisonReason {
                    reason,
                    component,
                    context: attempted_context.clone(),
                });
            }
        }
        let already_poisoned = self.poisoned.swap(true, Ordering::AcqRel);
        let poison = self.poison_error().unwrap_or_else(|| {
            Report::new(FatalError::Poisoned).attach(format!("poisoned_by={reason}"))
        });
        if !already_poisoned {
            obs::error!(
                "event=engine_poison component={} action=poison result=error fatal_reason={:?} context={}",
                component,
                poison.current_context(),
                attempted_context
            );
            self.poison_event.notify(usize::MAX);
        } else if obs::log_enabled!(obs::Level::Debug) {
            obs::debug!(
                "event=engine_poison component={} action=poison result=ignored fatal_reason={:?} published_fatal_reason={:?} context={}",
                component,
                reason,
                poison.current_context(),
                attempted_context
            );
        }
        poison
    }
}

impl Component for EnginePoisoner {
    type Config = ();
    type Owned = Self;
    type Access = QuiescentGuard<Self>;

    const NAME: &'static str = "engine_poisoner";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        registry.register::<Self>(Self::new())
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
            let err = worker_poisoner.poison(FatalError::RedoWrite, "test", "blocked poison");
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

        let err = handle.join().unwrap();
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
            worker_a_poisoner.poison(FatalError::RedoWrite, "test", "writer")
        });

        let worker_b_barrier = Arc::clone(&barrier);
        let worker_b_poisoner = Arc::clone(&poisoner);
        let worker_b = spawn(move || {
            worker_b_barrier.wait();
            worker_b_poisoner.poison(FatalError::RedoSync, "test", "sync")
        });

        barrier.wait();

        let err_a = worker_a.join().unwrap();
        let err_b = worker_b.join().unwrap();
        let stored = poisoner.poison_error().unwrap();
        let err_a_reason = *err_a.current_context();
        let err_b_reason = *err_b.current_context();
        let stored_reason = *stored.current_context();

        assert!(poisoner.poisoned.load(Ordering::Acquire));
        assert_eq!(err_a_reason, err_b_reason);
        assert_eq!(stored_reason, err_a_reason);
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
            let err = poisoner.poison(FatalError::CheckpointWrite, "test", "checkpoint");
            assert_eq!(*err.current_context(), FatalError::CheckpointWrite);
            waiter.await;
            assert!(
                poisoner
                    .ensure_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::CheckpointWrite)
            );

            let late_listener = poisoner.listener();
            let err = poisoner.poison(FatalError::RedoSync, "test", "sync");
            assert_eq!(*err.current_context(), FatalError::CheckpointWrite);
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
