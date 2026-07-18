use crate::error::{IoError, RuntimeError, RuntimeResult};
use crate::obs;
use error_stack::Report;
use std::io::Error as StdIoError;
use std::thread::{self, Builder, JoinHandle};

#[cfg(test)]
pub(crate) use tests::{
    SpawnTestEvent, fail_spawn_named, fail_spawn_named_with_observer, observe_spawn_named,
};

/// Spawns a named thread and logs its start and finish lifecycle.
#[inline]
pub(crate) fn spawn_named<S, F>(name: S, f: F) -> RuntimeResult<JoinHandle<()>>
where
    String: From<S>,
    F: FnOnce() + Send + 'static,
{
    let thread_name = String::from(name);
    let requested_name = thread_name.clone();
    let spawn_error = |err: StdIoError| {
        Report::new(IoError::from(err.kind()))
            .attach(format!("{err}"))
            .change_context(RuntimeError::BackgroundSpawn)
            .attach(format!("thread_name={requested_name}"))
    };
    #[cfg(test)]
    if let Some(err) = tests::injected_spawn_error(&thread_name) {
        return Err(spawn_error(err));
    }
    #[cfg(test)]
    let observer = tests::spawn_observer();
    Builder::new()
        .name(thread_name)
        .spawn(|| {
            let thd = thread::current();
            let worker = thd.name().unwrap_or("unknown");
            #[cfg(test)]
            let _observe_finish =
                observer.map(|observer| tests::observe_worker(worker, observer));
            obs::info!(
                "event=worker_lifecycle component=runtime worker={} thread_id={:?} action=start result=ok",
                worker,
                thd.id()
            );
            f();
            obs::info!(
                "event=worker_lifecycle component=runtime worker={} thread_id={:?} action=finish result=ok",
                worker,
                thd.id()
            );
        })
        .map_err(spawn_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::IoError;
    use std::cell::RefCell;
    use std::io::ErrorKind as IoErrorKind;
    use std::mem::replace;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    thread_local! {
        static SPAWN_TEST_CONTROL: RefCell<SpawnTestControl> = const {
            RefCell::new(SpawnTestControl {
                fail_name: None,
                observer: None,
            })
        };
    }

    pub(super) type SpawnObserver = Arc<dyn Fn(SpawnTestEvent) + Send + Sync>;

    struct SpawnTestControl {
        fail_name: Option<String>,
        observer: Option<SpawnObserver>,
    }

    /// Named-worker lifecycle event for deterministic startup tests.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum SpawnTestEvent {
        Started(String),
        Finished(String),
    }

    /// Scoped, thread-local named-spawn failure injection.
    pub(crate) struct SpawnFailureGuard {
        previous: Option<SpawnTestControl>,
    }

    impl Drop for SpawnFailureGuard {
        #[inline]
        fn drop(&mut self) {
            SPAWN_TEST_CONTROL.with(|control| {
                *control.borrow_mut() = self.previous.take().unwrap_or(SpawnTestControl {
                    fail_name: None,
                    observer: None,
                });
            });
        }
    }

    /// Fail the next matching named spawn performed by the current test thread.
    #[inline]
    pub(crate) fn fail_spawn_named(name: impl Into<String>) -> SpawnFailureGuard {
        install_spawn_test_control(Some(name.into()), None)
    }

    /// Fail one named spawn and observe every worker started by the current scope.
    #[inline]
    pub(crate) fn fail_spawn_named_with_observer(
        name: impl Into<String>,
        observer: impl Fn(SpawnTestEvent) + Send + Sync + 'static,
    ) -> SpawnFailureGuard {
        install_spawn_test_control(Some(name.into()), Some(Arc::new(observer)))
    }

    /// Observe every named worker started by the current test scope.
    #[inline]
    pub(crate) fn observe_spawn_named(
        observer: impl Fn(SpawnTestEvent) + Send + Sync + 'static,
    ) -> SpawnFailureGuard {
        install_spawn_test_control(None, Some(Arc::new(observer)))
    }

    #[inline]
    fn install_spawn_test_control(
        fail_name: Option<String>,
        observer: Option<SpawnObserver>,
    ) -> SpawnFailureGuard {
        let previous = SPAWN_TEST_CONTROL.with(|control| {
            Some(replace(
                &mut *control.borrow_mut(),
                SpawnTestControl {
                    fail_name,
                    observer,
                },
            ))
        });
        SpawnFailureGuard { previous }
    }

    #[inline]
    pub(super) fn injected_spawn_error(name: &str) -> Option<StdIoError> {
        SPAWN_TEST_CONTROL.with(|control| {
            let mut control = control.borrow_mut();
            if control.fail_name.as_deref() == Some(name) {
                control.fail_name.take();
                Some(StdIoError::other("injected named-thread spawn failure"))
            } else {
                None
            }
        })
    }

    #[inline]
    pub(super) fn spawn_observer() -> Option<SpawnObserver> {
        SPAWN_TEST_CONTROL.with(|control| control.borrow().observer.clone())
    }

    #[inline]
    pub(super) fn observe_worker(worker: &str, observer: SpawnObserver) -> ObserveWorkerFinish {
        observer(SpawnTestEvent::Started(worker.to_owned()));
        ObserveWorkerFinish::new(worker.to_owned(), observer)
    }

    pub(super) struct ObserveWorkerFinish {
        worker: String,
        observer: SpawnObserver,
    }

    impl ObserveWorkerFinish {
        #[inline]
        pub(super) fn new(worker: String, observer: SpawnObserver) -> Self {
            ObserveWorkerFinish { worker, observer }
        }
    }

    impl Drop for ObserveWorkerFinish {
        #[inline]
        fn drop(&mut self) {
            (self.observer)(SpawnTestEvent::Finished(self.worker.clone()));
        }
    }

    #[test]
    fn test_spawn_named_runs_and_joins_worker() {
        let ran = Arc::new(AtomicBool::new(false));
        let task_ran = Arc::clone(&ran);

        spawn_named("Thread-Test-Success", move || {
            task_ran.store(true, Ordering::Release);
        })
        .unwrap()
        .join()
        .unwrap();

        assert!(ran.load(Ordering::Acquire));
    }

    #[test]
    fn test_spawn_named_failure_preserves_runtime_io_and_thread_name() {
        let _failure = fail_spawn_named("Thread-Test-Failure");
        let ran = Arc::new(AtomicBool::new(false));
        let task_ran = Arc::clone(&ran);

        let report = spawn_named("Thread-Test-Failure", move || {
            task_ran.store(true, Ordering::Release);
        })
        .unwrap_err();

        assert_eq!(report.current_context(), &RuntimeError::BackgroundSpawn);
        assert_eq!(
            report.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::Other)
        );
        let output = format!("{report:?}");
        assert_eq!(output.matches("thread_name=Thread-Test-Failure").count(), 1);
        assert!(!ran.load(Ordering::Acquire));
    }
}
