use crate::completion::Completion;
use crate::component::{
    Component, ComponentRegistry, FirstPanic, ShelfScope, Supplier, panic_payload_description,
};
use crate::conf::ThreadPoolConfig;
use crate::error::{ConfigError, ConfigResult, FatalError, RuntimeError, RuntimeResult};
use crate::obs;
use crate::poison::EnginePoisoner;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use crate::{runtime, thread};
use error_stack::{Report, ResultExt};
use event_listener::{Event, listener};
use futures::FutureExt;
use parking_lot::Mutex;
use std::future::Future;
use std::mem::take;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{JoinHandle, current};

#[derive(Clone, Copy, PartialEq, Eq)]
enum PoolPhase {
    Starting,
    Running,
    Draining,
}

struct AdmissionState {
    phase: PoolPhase,
    active: usize,
}

// Shared admission and poison ownership retained by the pool and every job.
struct ThreadPoolShared {
    state: Mutex<AdmissionState>,
    changed: Event,
    poisoner: QuiescentGuard<EnginePoisoner>,
}

impl ThreadPoolShared {
    fn new(poisoner: QuiescentGuard<EnginePoisoner>) -> Self {
        Self {
            state: Mutex::new(AdmissionState {
                phase: PoolPhase::Starting,
                active: 0,
            }),
            changed: Event::new(),
            poisoner,
        }
    }

    fn reserve(self: &Arc<Self>) -> Option<ThreadPoolTaskPermit> {
        let mut state = self.state.lock();
        if state.phase != PoolPhase::Running {
            return None;
        }
        // Each accepted job owns exactly one reservation until cleanup ends.
        assert!(
            state.active < usize::MAX,
            "thread pool active-job accounting overflow"
        );
        state.active += 1;
        Some(ThreadPoolTaskPermit {
            shared: Arc::clone(self),
        })
    }

    fn start(&self) {
        let mut state = self.state.lock();
        assert!(
            state.phase == PoolPhase::Starting,
            "thread pool startup requires Starting admission"
        );
        state.phase = PoolPhase::Running;
    }

    fn close(&self) {
        self.state.lock().phase = PoolPhase::Draining;
        self.changed.notify(usize::MAX);
    }

    fn release(&self) {
        let drained = {
            let mut state = self.state.lock();
            assert!(
                state.active != 0,
                "thread pool active-job accounting underflow"
            );
            state.active -= 1;
            state.active == 0
        };
        if drained {
            self.changed.notify(usize::MAX);
        }
    }

    /// Workers and their live I/O dependencies produce progress. Phase/count
    /// are authoritative; register before rechecking to avoid a lost final wake.
    /// Poison cannot cancel accepted ownership. The job releases its permit
    /// after execution cleanup, while its semantic owner settles publication.
    async fn wait_for_drained_shutdown(&self) {
        loop {
            listener!(self.changed => changed);
            {
                let state = self.state.lock();
                if state.phase == PoolPhase::Draining && state.active == 0 {
                    return;
                }
            }
            changed.await;
        }
    }
}

// Retains shared state and one active job across acceptance and detached execution.
struct ThreadPoolTaskPermit {
    shared: Arc<ThreadPoolShared>,
}

impl Drop for ThreadPoolTaskPermit {
    fn drop(&mut self) {
        self.shared.release();
    }
}

/// Engine-owned executor for finite synchronous and asynchronous jobs.
///
/// Callers bound fan-out and temporary memory and await accepted children before
/// their bootstrap, foreground, or mandatory owner terminates. They retain
/// operation-level cleanup and publication responsibility. Jobs must not retain
/// the engine owner shell in a cycle. Observers retain output-lifetime duties.
/// There is no FIFO scheduling guarantee or global queue capacity.
pub(crate) struct ThreadPool {
    worker_threads: usize,
    executor: async_executor::Executor<'static>,
    shared: Arc<ThreadPoolShared>,
    #[cfg(test)]
    after_reserve: Mutex<Option<tests::SubmissionHook>>,
}

impl ThreadPool {
    #[inline]
    fn new(config: &ThreadPoolConfig, poisoner: QuiescentGuard<EnginePoisoner>) -> Self {
        Self {
            worker_threads: config.worker_threads,
            executor: async_executor::Executor::new(),
            shared: Arc::new(ThreadPoolShared::new(poisoner)),
            #[cfg(test)]
            after_reserve: Mutex::new(None),
        }
    }

    /// Return the fixed number of workers shared by sync and async jobs.
    #[inline]
    pub(crate) const fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    /// Accept one finite synchronous computation for eventual worker execution.
    ///
    /// Jobs must not block on I/O, other jobs, sleeps, or blocking waits.
    /// Dropping the returned completion never cancels accepted execution.
    pub(crate) fn submit<T, F>(&self, job: F) -> Arc<Completion<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.submit_async(async move { job() })
    }

    /// Synchronously accept a finite future without polling it on the caller.
    ///
    /// Jobs may await backend I/O and async latches under their existing
    /// ownership and poison contracts. CPU loops need bounded batches or
    /// `runtime::yield_now()`; an immediately ready await need not yield.
    /// Service loops and unbounded recursive spawning are outside this contract.
    /// Ordinary task errors remain values in `T`.
    ///
    /// Reservation under the admission lock linearizes acceptance against
    /// shutdown. A racing poison may admit finite extra work, but observed
    /// poison rejects without polling. Detached supervision owns every accepted
    /// job through completion and cleanup, independently of its observer.
    pub(crate) fn submit_async<T, F>(&self, future: F) -> Arc<Completion<T>>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let completion = Arc::new(Completion::new());
        if let Some(err) = self.shared.poisoner.shared_poison_error() {
            completion.complete(Err(err.into_completion_bridge()));
            return completion;
        }
        let Some(permit) = self.shared.reserve() else {
            let report = Report::new(FatalError::ThreadPoolUnavailable)
                .attach("operation=submit_thread_pool_task, admission=unavailable");
            obs::error!(
                "event=engine_poison component=thread_pool action=poison result=error error={report:?}"
            );
            let shared = self.shared.poisoner.poison(report);
            completion.complete(Err(shared.into_completion_bridge()));
            return completion;
        };
        #[cfg(test)]
        {
            let hook = self.after_reserve.lock().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        let producer = Arc::clone(&completion);
        self.executor.spawn(async move {
            // Drop the submitted future (including captures retained after its
            // final poll) before publishing completion and releasing admission.
            let outcome = {
                let supervised = AssertUnwindSafe(future).catch_unwind();
                futures::pin_mut!(supervised);
                supervised.await
            };
            match outcome {
                Ok(output) => producer.complete(Ok(output)),
                Err(payload) => {
                    let current = current();
                    let worker = current.name().unwrap_or("unknown");
                    let report = Report::new(FatalError::ThreadPoolTaskPanic).attach(format!(
                        "worker={worker}, panic_payload={}",
                        panic_payload_description(payload.as_ref())
                    ));
                    obs::error!(
                        "event=engine_poison component=thread_pool action=poison result=error error={report:?}"
                    );
                    // The permit already retains poison ownership through the
                    // shared state; no separate guard is needed for this job.
                    let shared = permit.shared.poisoner.poison(report);
                    producer.complete(Err(shared.into_completion_bridge()));
                }
            }
            drop(producer);
            drop(permit);
        }).detach();
        completion
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
        registry.register::<Self>(Self::new(&config, poisoner));
        shelf.put::<ThreadPoolWorkers>(PendingThreadPoolWorkerStartup::new(
            registry.dependency::<Self>(),
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
    worker_threads: usize,
}

impl PendingThreadPoolWorkerStartup {
    #[inline]
    fn new(pool: QuiescentGuard<ThreadPool>, worker_threads: usize) -> Self {
        Self {
            pool,
            worker_threads,
        }
    }

    fn start(self) -> RuntimeResult<ThreadPoolWorkersOwned> {
        let mut pending = PendingThreadPoolWorkers::new(self.pool);
        for worker_idx in 0..self.worker_threads {
            let pool = pending.pool.clone();
            let worker_name = format!("ThreadPoolWorker-{}", worker_idx + 1);
            let handle = thread::spawn_named(worker_name, move || {
                runtime::block_on(pool.executor.run(pool.shared.wait_for_drained_shutdown()));
            })
            .attach("phase=start_thread_pool_worker")?;
            pending.handles.push(handle);
        }
        pending.pool.shared.start();
        Ok(pending.into_owned())
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
        self.pool.shared.close();
        let mut panics = FirstPanic::default();
        for handle in take(&mut self.handles) {
            capture_join_panic(&mut panics, handle, "worker_startup_rollback");
        }
        // Preserve the spawn report as the primary startup failure.
        drop(panics);
    }
}

/// Join-handle owner for the engine pool workers.
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

/// Thread-joining owner that drains accepted jobs during shutdown.
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
        self.pool.shared.close();
        let handles = take(&mut *self.handles.lock());
        let mut panics = FirstPanic::default();
        for handle in handles {
            capture_join_panic(&mut panics, handle, "worker_shutdown");
        }
        // Zero active jobs can wake peers during the last wrapper's final poll.
        // Join every worker before checking final executor bookkeeping.
        if !self.pool.executor.is_empty() {
            let message =
                "thread pool executor must be empty after admission drain and worker joins"
                    .to_owned();
            obs::error!(
                "event=worker_shutdown component=thread_pool action=validate_executor result=panic payload={}",
                message
            );
            panics.capture(Box::new(message));
        }
        // Panic safety: admission is closed, accepted jobs have drained with
        // storage and eviction still live, and every join was attempted before
        // exposing the first join or executor-invariant payload.
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
    use crate::completion::CompletionTake;
    use crate::component::RegistryBuilder;
    use crate::error::{CompletionErrorBridge, RuntimeOrFatalError};
    use crate::thread::{SpawnTestEvent, fail_spawn_named_with_observer, observe_spawn_named};
    use parking_lot::Mutex as ParkingMutex;
    use std::future::{poll_fn, ready};
    use std::panic;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use std::task::Poll;
    use std::thread::scope;
    use std::time::Duration;

    /// Pauses a test submission after acceptance but before detached spawn.
    pub(super) type SubmissionHook = Arc<dyn Fn() + Send + Sync>;

    struct NonCloneOutput(Box<usize>);

    // Counts capture/output destruction independently of observer ownership.
    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

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

    fn submit_computation<T, F>(pool: &ThreadPool, asynchronous: bool, job: F) -> Arc<Completion<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        if asynchronous {
            pool.submit_async(async move {
                runtime::yield_now().await;
                job()
            })
        } else {
            pool.submit(job)
        }
    }

    // Signal only after registering the real pending receive's waker.
    async fn await_release(started: flume::Sender<()>, release: flume::Receiver<()>) {
        let mut started = Some(started);
        let receive = release.recv_async();
        futures::pin_mut!(receive);
        poll_fn(|cx| {
            let result = receive.as_mut().poll(cx);
            if result.is_pending()
                && let Some(started) = started.take()
            {
                started.send(()).unwrap();
            }
            result
        })
        .await
        .unwrap();
    }

    async fn wait_for_closed(pool: &ThreadPool) {
        loop {
            listener!(pool.shared.changed => changed);
            if pool.shared.state.lock().phase == PoolPhase::Draining {
                return;
            }
            changed.await;
        }
    }

    fn assert_fatal(bridge: CompletionErrorBridge, expected: FatalError) {
        let RuntimeOrFatalError::Fatal(report) =
            bridge.into_runtime_or_fatal(RuntimeError::CheckpointExecution)
        else {
            panic!("thread pool failure must remain Fatal")
        };
        assert_eq!(report.current_context(), &expected);
    }

    fn assert_drained(registry: &ComponentRegistry, pool: &ThreadPool) {
        assert!(!registry.shutdown_all().is_degraded());
        assert!(!registry.shutdown_all().is_degraded());
        assert_eq!(pool.shared.state.lock().active, 0);
        assert!(pool.executor.is_empty());
    }

    #[test]
    fn submission_moves_output_once_from_named_worker() {
        runtime::block_on(async {
            let (registry, pool) = test_pool(1).await;
            for asynchronous in [false, true] {
                let completion = submit_computation(&pool, asynchronous, || {
                    assert_eq!(current().name(), Some("ThreadPoolWorker-1"));
                    NonCloneOutput(Box::new(17))
                });
                let output = completion.wait_take_result().await.unwrap();
                assert_eq!(*output.0, 17);
                assert!(matches!(
                    completion.try_take_result(),
                    CompletionTake::Consumed
                ));
            }
            // Keeping a completed observer never holds active admission open.
            let retained = pool.submit_async(ready(NonCloneOutput(Box::new(23))));
            assert_drained(&registry, &pool);
            assert_eq!(*retained.wait_take_result().await.unwrap().0, 23);
            pool.shared.poisoner.ensure_healthy().unwrap();
        });
    }

    #[test]
    fn pending_async_job_yields_the_only_worker() {
        runtime::block_on(async {
            let (registry, pool) = test_pool(1).await;
            let (started_tx, started_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let first = pool.submit_async(async move {
                await_release(started_tx, release_rx).await;
                17
            });
            started_rx.recv_async().await.unwrap();
            let second = pool.submit(|| 23);
            assert_eq!(second.wait_take_result().await.unwrap(), 23);
            assert!(matches!(first.try_take_result(), CompletionTake::Pending));
            release_tx.send(()).unwrap();
            assert_eq!(first.wait_take_result().await.unwrap(), 17);
            assert_drained(&registry, &pool);
        });
    }

    #[test]
    fn configured_workers_bound_parallel_sync_and_async_execution() {
        runtime::block_on(async {
            let (registry, pool) = test_pool(2).await;
            assert_eq!(pool.worker_threads(), 2);
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let (started_tx, started_rx) = flume::unbounded();
            let (release_tx, release_rx) = flume::unbounded();
            let mut completions = Vec::new();
            for asynchronous in [false, true, false, true] {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let started_tx = started_tx.clone();
                let release_rx = release_rx.clone();
                completions.push(submit_computation(&pool, asynchronous, move || {
                    assert!(current().name().unwrap().starts_with("ThreadPoolWorker-"));
                    let count = active.fetch_add(1, Ordering::AcqRel) + 1;
                    peak.fetch_max(count, Ordering::AcqRel);
                    started_tx.send(()).unwrap();
                    // Test-only finite CPU-section gate occupies this worker.
                    release_rx.recv().unwrap();
                    active.fetch_sub(1, Ordering::AcqRel);
                }));
            }
            let first = started_rx.recv_timeout(Duration::from_secs(5));
            let second = started_rx.recv_timeout(Duration::from_secs(5));
            let third = started_rx.try_recv();
            for _ in 0..4 {
                release_tx.send(()).unwrap();
            }
            for completion in completions {
                completion.wait_take_result().await.unwrap();
            }
            assert!(first.is_ok() && second.is_ok());
            assert!(third.is_err());
            assert_eq!(peak.load(Ordering::Acquire), 2);
            assert_drained(&registry, &pool);
        });
    }

    #[test]
    fn detached_jobs_release_inputs_outputs_and_reservations_once() {
        smol::block_on(async {
            let (registry, pool) = test_pool(1).await;
            let (started_tx, started_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let blocker = pool.submit(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.recv().unwrap();
            let inputs = Arc::new(AtomicUsize::new(0));
            let outputs = Arc::new(AtomicUsize::new(0));
            let runs = Arc::new(AtomicUsize::new(0));
            for asynchronous in [false, true] {
                let input = DropCounter(Arc::clone(&inputs));
                let output = DropCounter(Arc::clone(&outputs));
                let runs = Arc::clone(&runs);
                drop(submit_computation(&pool, asynchronous, move || {
                    let _input = input;
                    runs.fetch_add(1, Ordering::AcqRel);
                    output
                }));
            }
            let (pending_tx, pending_rx) = flume::bounded(1);
            let (resume_tx, resume_rx) = flume::bounded(1);
            let input = DropCounter(Arc::clone(&inputs));
            let output = DropCounter(Arc::clone(&outputs));
            let task_runs = Arc::clone(&runs);
            let pending = pool.submit_async(async move {
                let _input = input;
                await_release(pending_tx, resume_rx).await;
                task_runs.fetch_add(1, Ordering::AcqRel);
                output
            });
            release_tx.send(()).unwrap();
            blocker.wait_take_result().await.unwrap();
            pending_rx.recv_async().await.unwrap();
            drop(pending);
            scope(|scope| {
                let shutdown = scope.spawn(|| assert!(!registry.shutdown_all().is_degraded()));
                runtime::block_on(wait_for_closed(&pool));
                assert!(pool.shared.state.lock().active >= 1);
                resume_tx.send(()).unwrap();
                shutdown.join().unwrap();
            });
            assert_drained(&registry, &pool);
            assert_eq!(runs.load(Ordering::Acquire), 3);
            assert_eq!(inputs.load(Ordering::Acquire), 3);
            assert_eq!(outputs.load(Ordering::Acquire), 3);
        });
    }

    #[test]
    fn shutdown_drains_running_queued_and_externally_pending_jobs() {
        let (registry, pool) = runtime::block_on(test_pool(1));
        let (pending_tx, pending_rx) = flume::bounded(1);
        let (resume_tx, resume_rx) = flume::bounded(1);
        let pending = pool.submit_async(async move {
            await_release(pending_tx, resume_rx).await;
            17
        });
        pending_rx.recv().unwrap();
        let (running_tx, running_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let running = pool.submit(move || {
            running_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            23
        });
        running_rx.recv().unwrap();
        let queued = pool.submit_async(ready(29));
        scope(|scope| {
            let shutdown = scope.spawn(|| assert!(!registry.shutdown_all().is_degraded()));
            runtime::block_on(wait_for_closed(&pool));
            assert_eq!(pool.shared.state.lock().active, 3);
            assert!(matches!(queued.try_take_result(), CompletionTake::Pending));
            release_tx.send(()).unwrap();
            assert_eq!(runtime::block_on(running.wait_take_result()).unwrap(), 23);
            assert_eq!(runtime::block_on(queued.wait_take_result()).unwrap(), 29);
            assert!(matches!(pending.try_take_result(), CompletionTake::Pending));
            resume_tx.send(()).unwrap();
            shutdown.join().unwrap();
        });
        assert_eq!(runtime::block_on(pending.wait_take_result()).unwrap(), 17);
        assert_drained(&registry, &pool);
        pool.shared.poisoner.ensure_healthy().unwrap();
    }

    #[test]
    fn future_resources_drop_before_completion_and_permit_release() {
        runtime::block_on(async {
            let (registry, pool) = test_pool(1).await;
            let dropped = Arc::new(AtomicUsize::new(0));
            let input = DropCounter(Arc::clone(&dropped));
            let completion = pool.submit_async(poll_fn(move |_| {
                // A custom future keeps its capture even after returning Ready.
                let _ = &input;
                Poll::Ready(17)
            }));
            assert_eq!(completion.wait_take_result().await.unwrap(), 17);
            assert_eq!(dropped.load(Ordering::Acquire), 1);
            assert_drained(&registry, &pool);
        });
    }

    #[test]
    fn sync_and_suspended_async_panics_poison_and_drain_accepted_siblings() {
        for asynchronous in [false, true] {
            for detached in [false, true] {
                runtime::block_on(async {
                    let (registry, pool) = test_pool(1).await;
                    let inputs = Arc::new(AtomicUsize::new(0));
                    let input = DropCounter(Arc::clone(&inputs));
                    let (started_tx, started_rx) = flume::bounded(1);
                    let (release_tx, release_rx) = flume::bounded(1);
                    let completion = if asynchronous {
                        pool.submit_async(async move {
                            let _input = input;
                            await_release(started_tx, release_rx).await;
                            panic!("injected async panic after suspension");
                        })
                    } else {
                        pool.submit(move || {
                            let _input = input;
                            started_tx.send(()).unwrap();
                            release_rx.recv().unwrap();
                            panic!("injected sync panic");
                        })
                    };
                    started_rx.recv_async().await.unwrap();
                    let (sibling_tx, sibling_rx) = flume::bounded(1);
                    let sibling = pool.submit_async(async move {
                        sibling_rx.recv_async().await.unwrap();
                        23
                    });
                    let poisoned = pool.shared.poisoner.listener();
                    if detached {
                        drop(completion);
                    } else {
                        release_tx.send(()).unwrap();
                        assert_fatal(
                            completion.wait_take_result().await.unwrap_err(),
                            FatalError::ThreadPoolTaskPanic,
                        );
                        assert_eq!(
                            pool.shared
                                .poisoner
                                .ensure_healthy()
                                .unwrap_err()
                                .current_context(),
                            &FatalError::ThreadPoolTaskPanic
                        );
                    }
                    if detached {
                        release_tx.send(()).unwrap();
                        if pool.shared.poisoner.ensure_healthy().is_ok() {
                            poisoned.await;
                        }
                    }
                    assert!(matches!(sibling.try_take_result(), CompletionTake::Pending));
                    sibling_tx.send(()).unwrap();
                    assert_drained(&registry, &pool);
                    assert_eq!(sibling.wait_take_result().await.unwrap(), 23);
                    assert_eq!(inputs.load(Ordering::Acquire), 1);
                    assert_eq!(
                        pool.shared
                            .poisoner
                            .ensure_healthy()
                            .unwrap_err()
                            .current_context(),
                        &FatalError::ThreadPoolTaskPanic
                    );
                });
            }
        }
    }

    #[test]
    fn poison_fast_path_reuses_cached_error_without_polling() {
        runtime::block_on(async {
            let (registry, pool) = test_pool(1).await;
            let shared = pool
                .shared
                .poisoner
                .poison(Report::new(FatalError::ThreadPoolTaskPanic));
            let identity = shared.test_identity();
            for asynchronous in [false, true] {
                let completion =
                    submit_computation(&pool, asynchronous, || panic!("rejected job ran"));
                assert_fatal(
                    completion.wait_take_result().await.unwrap_err(),
                    FatalError::ThreadPoolTaskPanic,
                );
                assert_eq!(
                    pool.shared
                        .poisoner
                        .shared_poison_error()
                        .unwrap()
                        .test_identity(),
                    identity
                );
            }
            assert_drained(&registry, &pool);
        });
    }

    #[test]
    fn reservation_before_spawn_keeps_shutdown_workers_alive() {
        smol::block_on(async {
            let (registry, pool) = test_pool(1).await;
            let (reserved_tx, reserved_rx) = flume::bounded(1);
            let (spawn_tx, spawn_rx) = flume::bounded(1);
            *pool.after_reserve.lock() = Some(Arc::new(move || {
                reserved_tx.send(()).unwrap();
                spawn_rx.recv().unwrap();
            }));
            scope(|scope| {
                let submission = scope.spawn(|| pool.submit_async(async { 17 }));
                reserved_rx.recv().unwrap();
                let (stopped_tx, stopped_rx) = flume::bounded(1);
                let registry = &registry;
                let shutdown = scope.spawn(move || {
                    assert!(!registry.shutdown_all().is_degraded());
                    stopped_tx.send(()).unwrap();
                });
                runtime::block_on(wait_for_closed(&pool));
                assert_eq!(pool.shared.state.lock().active, 1);
                assert!(stopped_rx.try_recv().is_err());
                pool.shared.poisoner.ensure_healthy().unwrap();
                // Losing admission must complete without polling or invoking the hook.
                let rejected = pool.submit_async(async { panic!("closed admission polled a job") });
                assert_fatal(
                    runtime::block_on(rejected.wait_take_result()).unwrap_err(),
                    FatalError::ThreadPoolUnavailable,
                );
                spawn_tx.send(()).unwrap();
                let accepted = submission.join().unwrap();
                shutdown.join().unwrap();
                assert_eq!(runtime::block_on(accepted.wait_take_result()).unwrap(), 17);
            });
            assert_drained(&registry, &pool);
        });
    }

    #[test]
    fn idle_shutdown_is_healthy_and_starting_or_draining_rejects_jobs() {
        runtime::block_on(async {
            let (registry, pool) = test_pool(2).await;
            assert_drained(&registry, &pool);
            pool.shared.poisoner.ensure_healthy().unwrap();
            assert_fatal(
                pool.submit(|| 1).wait_take_result().await.unwrap_err(),
                FatalError::ThreadPoolUnavailable,
            );

            let poisoner = QuiescentBox::new(EnginePoisoner::new());
            let pool = ThreadPool::new(&ThreadPoolConfig::default(), poisoner.guard());
            assert_fatal(
                pool.submit_async(async { panic!("Starting job ran") })
                    .wait_take_result()
                    .await
                    .unwrap_err(),
                FatalError::ThreadPoolUnavailable,
            );
        });
    }

    #[test]
    fn drain_listener_handles_final_release_before_and_after_registration() {
        for release_first in [false, true] {
            runtime::block_on(async {
                let poisoner = QuiescentBox::new(EnginePoisoner::new());
                let shared = Arc::new(ThreadPoolShared::new(poisoner.guard()));
                shared.start();
                let permit = shared.reserve().unwrap();
                shared.close();
                let mut permit = Some(permit);
                if release_first {
                    drop(permit.take());
                }
                let drain = shared.wait_for_drained_shutdown();
                futures::pin_mut!(drain);
                if !release_first {
                    assert!(futures::poll!(drain.as_mut()).is_pending());
                    drop(permit.take());
                }
                drain.await;
            });
        }
    }

    #[test]
    fn startup_failure_stops_and_joins_every_started_worker() {
        for failed_worker in [1, 2] {
            runtime::block_on(async {
                let (event_tx, event_rx) = mpsc::channel();
                let name = format!("ThreadPoolWorker-{failed_worker}");
                let _failure = fail_spawn_named_with_observer(&name, move |event| {
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
                assert!(format!("{error:?}").contains("phase=start_thread_pool_worker"));
                let events: Vec<_> = event_rx.try_iter().collect();
                for index in 1..=3 {
                    let name = format!("ThreadPoolWorker-{index}");
                    assert_eq!(
                        events.contains(&SpawnTestEvent::Started(name.clone())),
                        index < failed_worker
                    );
                    assert_eq!(
                        events.contains(&SpawnTestEvent::Finished(name)),
                        index < failed_worker
                    );
                }
            });
        }
    }

    #[test]
    fn shutdown_joins_every_worker_before_resuming_first_panic() {
        let events = Arc::new(ParkingMutex::new(Vec::new()));
        let observed_events = Arc::clone(&events);
        let observer = observe_spawn_named(move |event| {
            observed_events.lock().push(event.clone());
            match event {
                SpawnTestEvent::Finished(name) if name == "ThreadPoolWorker-1" => {
                    panic::panic_any("first CPU worker panic");
                }
                SpawnTestEvent::Finished(name) if name == "ThreadPoolWorker-2" => {
                    panic::panic_any("second CPU worker panic");
                }
                _ => {}
            }
        });
        let (registry, _pool) = runtime::block_on(test_pool(2));

        let outcome = registry.shutdown_all();
        assert!(outcome.is_degraded());
        let events = events.lock();
        assert!(events.contains(&SpawnTestEvent::Finished("ThreadPoolWorker-1".to_owned())));
        assert!(events.contains(&SpawnTestEvent::Finished("ThreadPoolWorker-2".to_owned())));
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
