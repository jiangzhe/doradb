use crate::completion::Completion;
use crate::component::{
    Component, ComponentRegistry, FirstPanic, ShelfScope, Supplier, panic_payload_description,
};
use crate::conf::ThreadPoolConfig;
use crate::error::{
    ConfigError, ConfigResult, FatalError, RuntimeError, RuntimeResult, SharedFatalError,
};
use crate::obs;
use crate::poison::EnginePoisoner;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use crate::thread;
use error_stack::{Report, ResultExt};
use parking_lot::Mutex;
use std::mem::take;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

trait ThreadPoolJob: Send {
    fn execute(self: Box<Self>, worker: &str, poisoner: &EnginePoisoner);
}

enum ThreadPoolMessage {
    Execute(Box<dyn ThreadPoolJob>),
    Stop,
}

struct ThreadPoolTask<T, F> {
    task: F,
    completion: Arc<Completion<T>>,
}

impl<T, F> ThreadPoolJob for ThreadPoolTask<T, F>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    fn execute(self: Box<Self>, worker: &str, poisoner: &EnginePoisoner) {
        let Self { task, completion } = *self;
        match catch_unwind(AssertUnwindSafe(task)) {
            Ok(output) => completion.complete(Ok(output)),
            Err(payload) => {
                let shared = poison_thread_pool_failure(
                    poisoner,
                    Report::new(FatalError::ThreadPoolTaskPanic).attach(format!(
                        "worker={worker}, panic_payload={}",
                        panic_payload_description(payload.as_ref())
                    )),
                );
                completion.complete(Err(shared.into_completion_bridge()));
            }
        }
    }
}

#[inline]
fn poison_thread_pool_failure(
    poisoner: &EnginePoisoner,
    report: Report<FatalError>,
) -> SharedFatalError {
    obs::error!(
        "event=engine_poison component=thread_pool action=poison result=error error={report:?}"
    );
    poisoner.poison(report)
}

/// Engine-owned executor for finite synchronous CPU computations.
pub(crate) struct ThreadPool {
    worker_threads: usize,
    ingress: flume::Sender<ThreadPoolMessage>,
    poisoner: QuiescentGuard<EnginePoisoner>,
}

impl ThreadPool {
    #[inline]
    fn new(
        config: &ThreadPoolConfig,
        ingress: flume::Sender<ThreadPoolMessage>,
        poisoner: QuiescentGuard<EnginePoisoner>,
    ) -> Self {
        Self {
            worker_threads: config.worker_threads,
            ingress,
            poisoner,
        }
    }

    /// Return the fixed number of CPU workers.
    #[inline]
    pub(crate) const fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    /// Accept one finite synchronous computation for eventual execution.
    ///
    /// The poisoner's atomic healthy path precedes the direct channel send. A
    /// concurrent poison may therefore race that check and admit bounded extra
    /// work, while an observed poison returns its cached Fatal without sending.
    /// Dropping the returned completion never cancels accepted execution.
    pub(crate) fn submit<T, F>(&self, task: F) -> Arc<Completion<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let completion = Arc::new(Completion::new());
        if let Some(shared) = self.poisoner.shared_poison_error() {
            completion.complete(Err(shared.into_completion_bridge()));
            return completion;
        }

        let message = ThreadPoolMessage::Execute(Box::new(ThreadPoolTask {
            task,
            completion: Arc::clone(&completion),
        }));
        if self.ingress.send(message).is_ok() {
            return completion;
        }

        let shared = poison_thread_pool_failure(
            &self.poisoner,
            Report::new(FatalError::ThreadPoolUnavailable)
                .attach("operation=submit_thread_pool_task, ingress=unavailable"),
        );
        completion.complete(Err(shared.into_completion_bridge()));
        completion
    }

    #[inline]
    fn stop_workers(&self, worker_threads: usize) {
        for _ in 0..worker_threads {
            if self.ingress.send(ThreadPoolMessage::Stop).is_err() {
                break;
            }
        }
    }
}

impl Component for ThreadPool {
    type Config = ThreadPoolConfig;
    type Owned = Self;
    type Access = QuiescentGuard<Self>;
    type Error = Report<ConfigError>;

    const NAME: &'static str = "thread_pool";

    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> ConfigResult<()> {
        config.validate()?;
        let poisoner = registry.dependency::<EnginePoisoner>();
        let (sender, receiver) = flume::unbounded();
        registry.register::<Self>(Self::new(&config, sender, poisoner.clone()));
        shelf.put::<ThreadPoolWorkers>(PendingThreadPoolWorkerStartup::new(
            registry.dependency::<Self>(),
            receiver,
            poisoner,
            config.worker_threads,
        ));
        Ok(())
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {
        // Stop signalling and worker joins belong to the adjacent worker owner.
    }
}

impl Supplier<ThreadPoolWorkers> for ThreadPool {
    type Provision = PendingThreadPoolWorkerStartup;
}

/// Deferred fixed-worker startup supplied by the thread-pool core.
pub(crate) struct PendingThreadPoolWorkerStartup {
    pool: QuiescentGuard<ThreadPool>,
    receiver: flume::Receiver<ThreadPoolMessage>,
    poisoner: QuiescentGuard<EnginePoisoner>,
    worker_threads: usize,
}

impl PendingThreadPoolWorkerStartup {
    #[inline]
    fn new(
        pool: QuiescentGuard<ThreadPool>,
        receiver: flume::Receiver<ThreadPoolMessage>,
        poisoner: QuiescentGuard<EnginePoisoner>,
        worker_threads: usize,
    ) -> Self {
        Self {
            pool,
            receiver,
            poisoner,
            worker_threads,
        }
    }

    fn start(self) -> RuntimeResult<ThreadPoolWorkersOwned> {
        let mut pending = PendingThreadPoolWorkers::new(self.pool);
        for worker_idx in 0..self.worker_threads {
            let receiver = self.receiver.clone();
            let poisoner = self.poisoner.clone();
            let worker_name = format!("Thread-Pool-Worker-{}", worker_idx + 1);
            let handle = thread::spawn_named(worker_name, move || {
                run_worker(&receiver, &poisoner);
            })
            .attach("phase=start_thread_pool_worker")?;
            pending.handles.push(handle);
        }
        Ok(pending.into_owned())
    }
}

#[inline]
fn run_worker(receiver: &flume::Receiver<ThreadPoolMessage>, poisoner: &EnginePoisoner) {
    let current = std::thread::current();
    let worker = current.name().unwrap_or("unknown");
    while let Ok(message) = receiver.recv() {
        match message {
            ThreadPoolMessage::Execute(job) => job.execute(worker, poisoner),
            ThreadPoolMessage::Stop => break,
        }
    }
}

struct PendingThreadPoolWorkers {
    pool: QuiescentGuard<ThreadPool>,
    handles: Vec<JoinHandle<()>>,
    armed: bool,
}

impl PendingThreadPoolWorkers {
    #[inline]
    fn new(pool: QuiescentGuard<ThreadPool>) -> Self {
        Self {
            pool,
            handles: Vec::new(),
            armed: true,
        }
    }

    #[inline]
    fn into_owned(mut self) -> ThreadPoolWorkersOwned {
        self.armed = false;
        ThreadPoolWorkersOwned {
            pool: self.pool.clone(),
            handles: Mutex::new(take(&mut self.handles)),
            shutdown_started: AtomicBool::new(false),
        }
    }
}

impl Drop for PendingThreadPoolWorkers {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.pool.stop_workers(self.handles.len());
        let mut panics = FirstPanic::default();
        for handle in take(&mut self.handles) {
            capture_join_panic(&mut panics, handle, "worker_startup_rollback");
        }
        // Preserve the spawn report as the primary startup failure.
        drop(panics);
    }
}

/// Join-handle owner for the engine CPU workers.
pub(crate) struct ThreadPoolWorkers;

impl Component for ThreadPoolWorkers {
    type Config = ();
    type Owned = ThreadPoolWorkersOwned;
    type Access = ();
    type Error = Report<RuntimeError>;

    const NAME: &'static str = "thread_pool_workers";

    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> RuntimeResult<()> {
        let startup = shelf.take::<ThreadPool>();
        registry.register::<Self>(startup.start()?);
        Ok(())
    }

    #[inline]
    fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

    #[inline]
    fn shutdown(component: &Self::Owned) {
        component.shutdown();
    }
}

/// Thread-joining owner that drains accepted CPU tasks during shutdown.
pub(crate) struct ThreadPoolWorkersOwned {
    pool: QuiescentGuard<ThreadPool>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    shutdown_started: AtomicBool,
}

impl ThreadPoolWorkersOwned {
    fn shutdown(&self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut handles = take(&mut *self.handles.lock());
        self.pool.stop_workers(handles.len());
        let mut panics = FirstPanic::default();
        for handle in handles.drain(..) {
            capture_join_panic(&mut panics, handle, "worker_shutdown");
        }
        // Every worker received a FIFO stop and was joined before propagation.
        panics.resume();
    }
}

fn capture_join_panic(panics: &mut FirstPanic, handle: JoinHandle<()>, event: &'static str) {
    let worker = handle.thread().name().unwrap_or("unknown").to_owned();
    if let Err(payload) = handle.join() {
        obs::error!(
            "event={} component=thread_pool worker={} action=join result=panic payload={}",
            event,
            worker,
            panic_payload_description(payload.as_ref())
        );
        panics.capture(payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::RegistryBuilder;
    use crate::error::RuntimeOrFatalError;
    use crate::poison::EnginePoisoner;
    use crate::runtime;
    use crate::thread::{SpawnTestEvent, fail_spawn_named_with_observer, observe_spawn_named};
    use parking_lot::Mutex as ParkingMutex;
    use std::panic;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    struct NonCloneOutput(Box<usize>);

    async fn test_pool(worker_threads: usize) -> (ComponentRegistry, QuiescentGuard<ThreadPool>) {
        let mut builder = RegistryBuilder::new();
        builder.build::<EnginePoisoner>(()).await.unwrap();
        builder
            .build::<ThreadPool>(ThreadPoolConfig::default().worker_threads(worker_threads))
            .await
            .unwrap();
        builder.build::<ThreadPoolWorkers>(()).await.unwrap();
        let registry = builder.finish();
        let pool = registry.dependency::<ThreadPool>();
        (registry, pool)
    }

    #[test]
    fn submission_moves_output_once_from_named_worker() {
        runtime::block_on(async {
            let (_registry, pool) = test_pool(1).await;
            let completion = pool.submit(|| {
                assert_eq!(std::thread::current().name(), Some("Thread-Pool-1"));
                NonCloneOutput(Box::new(17))
            });
            let output = completion.wait_take_result().await.unwrap();
            assert_eq!(*output.0, 17);
        });
    }

    #[test]
    fn detached_submission_still_runs_exactly_once() {
        runtime::block_on(async {
            let (_registry, pool) = test_pool(1).await;
            let runs = Arc::new(AtomicUsize::new(0));
            let task_runs = Arc::clone(&runs);
            drop(pool.submit(move || {
                task_runs.fetch_add(1, Ordering::AcqRel);
            }));
            pool.submit(|| ()).wait_take_result().await.unwrap();
            assert_eq!(runs.load(Ordering::Acquire), 1);
        });
    }

    #[test]
    fn configured_workers_execute_cpu_tasks_in_parallel() {
        runtime::block_on(async {
            let (_registry, pool) = test_pool(2).await;
            let (started_tx, started_rx) = flume::unbounded();
            let (release_tx, release_rx) = flume::unbounded();
            let mut completions = Vec::new();
            for _ in 0..2 {
                let started_tx = started_tx.clone();
                let release_rx = release_rx.clone();
                completions.push(pool.submit(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }));
            }

            let first_started = started_rx.recv_timeout(Duration::from_secs(1)).is_ok();
            let second_started = started_rx.recv_timeout(Duration::from_secs(1)).is_ok();
            release_tx.send(()).unwrap();
            release_tx.send(()).unwrap();
            for completion in completions {
                completion.wait_take_result().await.unwrap();
            }
            assert!(first_started && second_started);
        });
    }

    #[test]
    fn panic_poisons_completion_and_worker_continues() {
        runtime::block_on(async {
            let (registry, pool) = test_pool(1).await;
            let (release_tx, release_rx) = flume::bounded(1);
            let panic_completion = pool.submit(move || -> () {
                release_rx.recv().unwrap();
                panic!("injected CPU panic");
            });
            let queued_completion = pool.submit(|| 23usize);
            release_tx.send(()).unwrap();

            let bridge = panic_completion.wait_take_result().await.unwrap_err();
            let RuntimeOrFatalError::Fatal(report) =
                bridge.into_runtime_or_fatal(RuntimeError::CheckpointExecution)
            else {
                panic!("thread-pool panic must remain Fatal")
            };
            assert_eq!(report.current_context(), &FatalError::ThreadPoolTaskPanic);
            assert_eq!(queued_completion.wait_take_result().await.unwrap(), 23);
            let poison = registry
                .dependency::<EnginePoisoner>()
                .ensure_healthy()
                .unwrap_err();
            assert_eq!(poison.current_context(), &FatalError::ThreadPoolTaskPanic);
        });
    }

    #[test]
    fn detached_panic_poisons_and_releases_task_input_once() {
        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        runtime::block_on(async {
            let (registry, pool) = test_pool(1).await;
            let drops = Arc::new(AtomicUsize::new(0));
            let input = DropCounter(Arc::clone(&drops));
            let (release_tx, release_rx) = flume::bounded(1);
            drop(pool.submit(move || {
                let _input = input;
                release_rx.recv().unwrap();
                panic!("detached injected CPU panic");
            }));
            let queued_completion = pool.submit(|| ());
            release_tx.send(()).unwrap();
            queued_completion.wait_take_result().await.unwrap();

            assert_eq!(drops.load(Ordering::Acquire), 1);
            let poison = registry
                .dependency::<EnginePoisoner>()
                .ensure_healthy()
                .unwrap_err();
            assert_eq!(poison.current_context(), &FatalError::ThreadPoolTaskPanic);
        });
    }

    #[test]
    fn poison_fast_path_reuses_cached_error_without_running_task() {
        runtime::block_on(async {
            let (registry, pool) = test_pool(1).await;
            let poisoner = registry.dependency::<EnginePoisoner>();
            let shared = poisoner.poison(Report::new(FatalError::ThreadPoolTaskPanic));
            let expected_identity = shared.test_identity();
            let runs = Arc::new(AtomicUsize::new(0));
            let task_runs = Arc::clone(&runs);

            let bridge = pool
                .submit(move || {
                    task_runs.fetch_add(1, Ordering::AcqRel);
                })
                .wait_take_result()
                .await
                .unwrap_err();
            assert_eq!(runs.load(Ordering::Acquire), 0);
            let RuntimeOrFatalError::Fatal(report) =
                bridge.into_runtime_or_fatal(RuntimeError::CheckpointExecution)
            else {
                panic!("cached thread-pool poison must remain Fatal")
            };
            assert_eq!(report.current_context(), &FatalError::ThreadPoolTaskPanic);
            assert_eq!(
                poisoner.shared_poison_error().unwrap().test_identity(),
                expected_identity
            );
        });
    }

    #[test]
    fn unavailable_ingress_returns_completed_fatal_handle() {
        runtime::block_on(async {
            let (registry, pool) = test_pool(1).await;
            let outcome = registry.shutdown_all();
            assert!(!outcome.is_degraded());
            let bridge = pool.submit(|| 1usize).wait_take_result().await.unwrap_err();
            let RuntimeOrFatalError::Fatal(report) =
                bridge.into_runtime_or_fatal(RuntimeError::CheckpointExecution)
            else {
                panic!("unavailable thread pool must remain Fatal")
            };
            assert_eq!(report.current_context(), &FatalError::ThreadPoolUnavailable);
            let poison = registry
                .dependency::<EnginePoisoner>()
                .ensure_healthy()
                .unwrap_err();
            assert_eq!(poison.current_context(), &FatalError::ThreadPoolUnavailable);
        });
    }

    #[test]
    fn partial_startup_failure_stops_and_joins_started_workers() {
        runtime::block_on(async {
            let (event_tx, event_rx) = mpsc::channel();
            let _failure = fail_spawn_named_with_observer("Thread-Pool-2", move |event| {
                event_tx.send(event).unwrap();
            });
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<ThreadPool>(ThreadPoolConfig::default().worker_threads(3))
                .await
                .unwrap();
            let error = builder.build::<ThreadPoolWorkers>(()).await.unwrap_err();
            assert_eq!(error.current_context(), &RuntimeError::BackgroundSpawn);
            let events: Vec<_> = event_rx.try_iter().collect();
            assert!(events.contains(&SpawnTestEvent::Started("Thread-Pool-1".to_owned())));
            assert!(events.contains(&SpawnTestEvent::Finished("Thread-Pool-1".to_owned())));
            assert!(!events.contains(&SpawnTestEvent::Started("Thread-Pool-3".to_owned())));
        });
    }

    #[test]
    fn shutdown_joins_every_worker_before_resuming_first_panic() {
        let events = Arc::new(ParkingMutex::new(Vec::new()));
        let observed_events = Arc::clone(&events);
        let observer = observe_spawn_named(move |event| {
            observed_events.lock().push(event.clone());
            match event {
                SpawnTestEvent::Finished(name) if name == "Thread-Pool-1" => {
                    panic::panic_any("first CPU worker panic");
                }
                SpawnTestEvent::Finished(name) if name == "Thread-Pool-2" => {
                    panic::panic_any("second CPU worker panic");
                }
                _ => {}
            }
        });
        let (registry, _pool) = runtime::block_on(test_pool(2));

        let outcome = registry.shutdown_all();
        assert!(outcome.is_degraded());
        let events = events.lock();
        assert!(events.contains(&SpawnTestEvent::Finished("Thread-Pool-1".to_owned())));
        assert!(events.contains(&SpawnTestEvent::Finished("Thread-Pool-2".to_owned())));
        drop(events);
        drop(observer);

        let payload = panic::catch_unwind(AssertUnwindSafe(|| {
            outcome.propagate_or_suppress("thread_pool_worker_test");
        }))
        .unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("first CPU worker panic")
        );
    }
}
