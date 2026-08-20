use crate::completion::{Completion, CompletionTake};
use crate::component::{
    Component, ComponentRegistry, FirstPanic, ShelfScope, Supplier, panic_payload_description,
};
use crate::conf::MandatoryRuntimeConfig;
use crate::error::{
    CompletionErrorBridge, CompletionResult, ConfigError, ConfigResult, FatalError, LifecycleError,
    LifecycleOrFatalError, LifecycleOrFatalResult, LifecycleResult, RuntimeError, RuntimeResult,
    SharedFatalError,
};
use crate::id::{SessionOperationKey, TableID};
use crate::obs;
use crate::poison::EnginePoisoner;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use crate::stats::{MandatoryRuntimeStats, MandatoryTaskStats};
use crate::{runtime, thread};
use error_stack::{Report, ResultExt};
use event_listener::{Event, listener};
use futures::FutureExt;
use futures::future::{Either, select};
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::mem::take;
use std::panic::AssertUnwindSafe;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Immutable classification of one mandatory task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MandatoryTaskClass {
    /// Caller-submitted DDL or maintenance operation.
    Operation,
    /// Engine-internal transaction cleanup obligation.
    TransactionCleanup,
}

impl MandatoryTaskClass {
    #[inline]
    const fn label(self) -> &'static str {
        match self {
            Self::Operation => "operation",
            Self::TransactionCleanup => "transaction_cleanup",
        }
    }
}

/// Immutable diagnostic metadata retained by one supervised task.
#[derive(Clone, Debug)]
pub(crate) struct MandatoryTaskMetadata {
    class: MandatoryTaskClass,
    label: &'static str,
    session_operation: Option<SessionOperationKey>,
    table_id: Option<TableID>,
}

impl MandatoryTaskMetadata {
    /// Build caller-operation metadata.
    #[inline]
    pub(crate) const fn operation(
        label: &'static str,
        session_operation: Option<SessionOperationKey>,
    ) -> Self {
        Self {
            class: MandatoryTaskClass::Operation,
            label,
            session_operation,
            table_id: None,
        }
    }

    /// Build caller table-DDL metadata.
    #[inline]
    pub(crate) const fn table_operation(
        label: &'static str,
        session_operation: SessionOperationKey,
        table_id: TableID,
    ) -> Self {
        Self {
            class: MandatoryTaskClass::Operation,
            label,
            session_operation: Some(session_operation),
            table_id: Some(table_id),
        }
    }

    /// Build transaction-cleanup metadata.
    #[inline]
    pub(crate) const fn transaction_cleanup(
        label: &'static str,
        session_operation: Option<SessionOperationKey>,
    ) -> Self {
        Self {
            class: MandatoryTaskClass::TransactionCleanup,
            label,
            session_operation,
            table_id: None,
        }
    }

    #[inline]
    fn diagnostic(&self) -> String {
        format!(
            "task_class={}, task_label={}, session_operation={}, table_id={}",
            self.class.label(),
            self.label,
            self.session_operation
                .map_or_else(|| "none".to_owned(), |key| key.to_string()),
            self.table_id
                .map_or_else(|| "none".to_owned(), |table_id| table_id.to_string())
        )
    }

    #[inline]
    const fn task_class(&self) -> &'static str {
        self.class.label()
    }

    #[inline]
    const fn task_label(&self) -> &'static str {
        self.label
    }

    #[inline]
    const fn session_operation(&self) -> Option<SessionOperationKey> {
        self.session_operation
    }

    #[inline]
    const fn table_id(&self) -> Option<TableID> {
        self.table_id
    }
}

struct OptionalValue<T>(Option<T>);

impl<T> Display for OptionalValue<T>
where
    T: Display,
{
    #[inline]
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            Some(value) => value.fmt(formatter),
            None => formatter.write_str("none"),
        }
    }
}

#[derive(Default)]
struct MandatoryTaskCounters {
    submitted_count: AtomicUsize,
    started_count: AtomicUsize,
    completed_count: AtomicUsize,
    error_count: AtomicUsize,
    panic_count: AtomicUsize,
    detached_observer_count: AtomicUsize,
    admission_wait_nanos: AtomicUsize,
    queue_wait_nanos: AtomicUsize,
    execution_nanos: AtomicUsize,
}

impl MandatoryTaskCounters {
    #[inline]
    fn record_submitted(&self, admission_wait_nanos: usize) {
        self.submitted_count.fetch_add(1, Ordering::Relaxed);
        self.admission_wait_nanos
            .fetch_add(admission_wait_nanos, Ordering::Relaxed);
    }

    #[inline]
    fn record_started(&self, queue_wait_nanos: usize) {
        self.started_count.fetch_add(1, Ordering::Relaxed);
        self.queue_wait_nanos
            .fetch_add(queue_wait_nanos, Ordering::Relaxed);
    }

    #[inline]
    fn record_completed(&self, result: MandatoryTaskResult, execution_nanos: usize) {
        match result {
            MandatoryTaskResult::Ok => {}
            MandatoryTaskResult::Error => {
                self.error_count.fetch_add(1, Ordering::Relaxed);
            }
            MandatoryTaskResult::Panic => {
                self.panic_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.execution_nanos
            .fetch_add(execution_nanos, Ordering::Relaxed);
        self.completed_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn record_observer_detached(&self) {
        self.detached_observer_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn snapshot(&self, active_count: usize) -> MandatoryTaskStats {
        MandatoryTaskStats {
            submitted_count: self.submitted_count.load(Ordering::Relaxed),
            started_count: self.started_count.load(Ordering::Relaxed),
            completed_count: self.completed_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            panic_count: self.panic_count.load(Ordering::Relaxed),
            detached_observer_count: self.detached_observer_count.load(Ordering::Relaxed),
            active_count,
            admission_wait_nanos: self.admission_wait_nanos.load(Ordering::Relaxed),
            queue_wait_nanos: self.queue_wait_nanos.load(Ordering::Relaxed),
            execution_nanos: self.execution_nanos.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy)]
enum MandatoryTaskResult {
    Ok,
    Error,
    Panic,
}

impl MandatoryTaskResult {
    #[inline]
    const fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Panic => "panic",
        }
    }
}

#[derive(Clone, Copy)]
enum PublishedObserver {
    Attached,
    Detached,
}

impl PublishedObserver {
    #[inline]
    const fn label(self) -> &'static str {
        match self {
            Self::Attached => "attached",
            Self::Detached => "detached",
        }
    }
}

struct MandatoryAdmissionState {
    in_use: usize,
    closed: bool,
}

struct MandatoryAdmission {
    limit: usize,
    state: Mutex<MandatoryAdmissionState>,
    changed: Event,
}

impl MandatoryAdmission {
    #[inline]
    fn new(limit: usize) -> Self {
        Self {
            limit,
            state: Mutex::new(MandatoryAdmissionState {
                in_use: 0,
                closed: false,
            }),
            changed: Event::new(),
        }
    }

    async fn acquire(
        &self,
        runtime: QuiescentGuard<MandatoryRuntime>,
    ) -> LifecycleResult<MandatoryPermit> {
        loop {
            listener!(self.changed => changed);
            {
                let mut state = self.state.lock();
                if state.closed {
                    return Err(Report::new(LifecycleError::Shutdown)
                        .attach("mandatory caller admission is closed"));
                }
                if state.in_use < self.limit {
                    state.in_use += 1;
                    return Ok(MandatoryPermit {
                        runtime: Some(runtime),
                    });
                }
            }
            changed.await;
        }
    }

    #[inline]
    fn release(&self) {
        let mut state = self.state.lock();
        assert!(
            state.in_use != 0,
            "mandatory caller permit accounting underflow"
        );
        state.in_use -= 1;
        drop(state);
        self.changed.notify(usize::MAX);
    }

    /// Close caller admission and wake every capacity waiter.
    #[inline]
    pub(crate) fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        drop(state);
        self.changed.notify(usize::MAX);
    }

    async fn drain(&self) {
        loop {
            listener!(self.changed => changed);
            if self.state.lock().in_use == 0 {
                return;
            }
            changed.await;
        }
    }

    #[inline]
    fn inspect(&self) -> (bool, usize) {
        let state = self.state.lock();
        (state.closed, state.in_use)
    }
}

struct MandatoryPermit {
    runtime: Option<QuiescentGuard<MandatoryRuntime>>,
}

impl Drop for MandatoryPermit {
    #[inline]
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.admission.release();
        }
    }
}

struct MandatoryInternalAdmissionState {
    closed: bool,
    active: usize,
}

struct MandatoryInternalAdmission {
    state: Mutex<MandatoryInternalAdmissionState>,
    changed: Event,
}

impl MandatoryInternalAdmission {
    #[inline]
    fn new() -> Self {
        Self {
            state: Mutex::new(MandatoryInternalAdmissionState {
                closed: false,
                active: 0,
            }),
            changed: Event::new(),
        }
    }

    #[inline]
    fn try_acquire(
        &self,
        runtime: QuiescentGuard<MandatoryRuntime>,
    ) -> Option<MandatoryInternalPermit> {
        let mut state = self.state.lock();
        if state.closed {
            return None;
        }
        state.active += 1;
        Some(MandatoryInternalPermit {
            runtime: Some(runtime),
        })
    }

    #[inline]
    fn release(&self) {
        let mut state = self.state.lock();
        assert!(
            state.active != 0,
            "mandatory internal permit accounting underflow"
        );
        state.active -= 1;
        drop(state);
        self.changed.notify(usize::MAX);
    }

    #[inline]
    fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        drop(state);
        self.changed.notify(usize::MAX);
    }

    async fn drain(&self) {
        loop {
            listener!(self.changed => changed);
            if self.state.lock().active == 0 {
                return;
            }
            changed.await;
        }
    }

    #[inline]
    fn inspect(&self) -> (bool, usize) {
        let state = self.state.lock();
        (state.closed, state.active)
    }
}

struct MandatoryInternalPermit {
    runtime: Option<QuiescentGuard<MandatoryRuntime>>,
}

impl Drop for MandatoryInternalPermit {
    #[inline]
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.internal_admission.release();
        }
    }
}

/// Completely caller-prepared execution awaiting mandatory admission.
pub(crate) trait PreparedExecution: Send + 'static {
    /// Terminal output delivered to the sole observer.
    type Output: Send + 'static;
    /// Mandatory owner produced by the synchronous acceptance edge.
    type Accepted: AcceptedExecution<Output = Self::Output>;

    /// Stable diagnostic label.
    const LABEL: &'static str;

    /// Return immutable task diagnostics.
    fn metadata(&self) -> MandatoryTaskMetadata;

    /// Synchronously transfer caller preparation to one mandatory owner.
    fn accept(self) -> Self::Accepted;
}

/// Accepted execution whose resources remain outside its panic-caught future.
pub(crate) trait AcceptedExecution: Send + 'static {
    /// Terminal output delivered to the sole observer.
    type Output: Send + 'static;

    /// Execute while borrowing the retained mandatory owner.
    fn execute(&mut self) -> impl Future<Output = CompletionResult<Self::Output>> + Send;

    /// Release settled resources and publish ordinary terminal state.
    ///
    /// Implementations must not unwind. Every fallible terminal operation must
    /// complete inside [`Self::execute`] and be represented in its result.
    fn finish(&mut self);

    /// Preserve or retain resources after an unexpected execution unwind.
    ///
    /// Implementations must not unwind. This method must settle every
    /// resource using panic-minimal operations and return the completion error.
    fn handle_panic(
        &mut self,
        panic: Box<dyn Any + Send>,
    ) -> impl Future<Output = CompletionErrorBridge> + Send;
}

/// Engine-internal mandatory obligation submitted without caller quota.
pub(crate) trait MandatoryInternalTask: Send + 'static {
    /// Stable diagnostic label.
    const LABEL: &'static str;

    /// Return immutable task diagnostics.
    fn metadata(&self) -> MandatoryTaskMetadata;

    /// Execute while the task object remains available to panic supervision.
    fn run(&mut self) -> impl Future<Output = ()> + Send;

    /// Preserve raw-reference-sensitive resources after an unexpected unwind.
    ///
    /// This runs before engine poison is published. Implementations must leave
    /// the task safe to drop and publish any `FailedRetained` state required
    /// by their domain. Implementations must not unwind.
    fn preserve_after_panic(&mut self);

    /// Wake task-specific waiters after preservation and engine poison.
    ///
    /// Implementations must not unwind.
    fn publish_panic(&mut self, error: CompletionErrorBridge);
}

enum ObservationState {
    Attached,
    Detached,
    Consumed,
}

struct MandatoryCompletion<T> {
    completion: Completion<T>,
    observation: Mutex<ObservationState>,
    metadata: MandatoryTaskMetadata,
    counters: Arc<MandatoryTaskCounters>,
}

impl<T> MandatoryCompletion<T> {
    #[inline]
    fn endpoints(
        metadata: MandatoryTaskMetadata,
        counters: Arc<MandatoryTaskCounters>,
    ) -> (CompletionProducer<T>, CompletionObserver<T>) {
        let inner = Arc::new(Self {
            completion: Completion::new(),
            observation: Mutex::new(ObservationState::Attached),
            metadata,
            counters,
        });
        (
            CompletionProducer {
                inner: Arc::clone(&inner),
            },
            CompletionObserver { inner, armed: true },
        )
    }

    #[inline]
    fn handle_unobserved(&self, result: CompletionResult<T>) {
        match result {
            Ok(value) => drop(value),
            Err(error) => {
                obs::error!(
                    "event=mandatory_completion component=mandatory_runtime action=discard_unobserved result=error task_class={} task_label={} session_operation={} table_id={} error={error:?}",
                    self.metadata.task_class(),
                    self.metadata.task_label(),
                    OptionalValue(self.metadata.session_operation()),
                    OptionalValue(self.metadata.table_id()),
                );
            }
        }
    }
}

struct CompletionProducer<T> {
    inner: Arc<MandatoryCompletion<T>>,
}

impl<T> CompletionProducer<T> {
    #[inline]
    fn metadata(&self) -> &MandatoryTaskMetadata {
        &self.inner.metadata
    }

    #[inline]
    fn counters(&self) -> &Arc<MandatoryTaskCounters> {
        &self.inner.counters
    }

    #[inline]
    fn complete(self, result: CompletionResult<T>) -> PublishedObserver {
        let observation = self.inner.observation.lock();
        match *observation {
            ObservationState::Attached => {
                self.inner.completion.complete(result);
                PublishedObserver::Attached
            }
            ObservationState::Detached => {
                self.inner.completion.complete(result);
                let result = match self.inner.completion.try_take_result() {
                    CompletionTake::Ready(result) => result,
                    CompletionTake::Pending | CompletionTake::Consumed => {
                        unreachable!("detached completion must move its newly published result")
                    }
                };
                drop(observation);
                self.inner.handle_unobserved(result);
                PublishedObserver::Detached
            }
            ObservationState::Consumed => {
                unreachable!("observer cannot consume before producer completion")
            }
        }
    }
}

/// Sole, execution-inert observer for one mandatory caller operation.
pub(crate) struct CompletionObserver<T> {
    inner: Arc<MandatoryCompletion<T>>,
    armed: bool,
}

impl<T> CompletionObserver<T> {
    /// Wait for the mandatory task and return its typed completion transport.
    #[inline]
    pub(crate) async fn wait(mut self) -> CompletionResult<T> {
        let result = self.inner.completion.wait_take_result().await;
        let mut observation = self.inner.observation.lock();
        assert!(
            matches!(*observation, ObservationState::Attached),
            "attached mandatory observer must own the exclusive result"
        );
        *observation = ObservationState::Consumed;
        self.armed = false;
        drop(observation);
        result
    }
}

impl<T> Drop for CompletionObserver<T> {
    #[inline]
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.inner.counters.record_observer_detached();
        let mut observation = self.inner.observation.lock();
        assert!(
            matches!(*observation, ObservationState::Attached),
            "mandatory observer detaches exactly once"
        );
        match self.inner.completion.try_take_result() {
            CompletionTake::Pending => {
                *observation = ObservationState::Detached;
            }
            CompletionTake::Ready(result) => {
                *observation = ObservationState::Consumed;
                drop(observation);
                self.inner.handle_unobserved(result);
            }
            CompletionTake::Consumed => {
                unreachable!("attached observer must own an unconsumed completion")
            }
        }
    }
}

/// One engine-owned asynchronous scheduler for non-cancellable obligations.
pub(crate) struct MandatoryRuntime {
    /// Shared executor driven by every fixed mandatory-runtime runner.
    executor: async_executor::Executor<'static>,
    /// Bounded admission for caller-prepared operations.
    ///
    /// This counter enforces the configured concurrency limit after the
    /// consuming acceptance edge. It excludes cancellable caller preparation
    /// and is closed on engine shutdown or runtime poison.
    admission: MandatoryAdmission,
    /// Independently accounted admission for engine-internal obligations.
    ///
    /// Cleanup already required for correctness bypasses the caller limit and
    /// is submitted synchronously through this admission. It remains open
    /// while redo can produce final cleanup, then closes and drains before the
    /// executor runners stop.
    internal_admission: MandatoryInternalAdmission,
    /// Monotonic diagnostics for accepted caller operations.
    operation_counters: Arc<MandatoryTaskCounters>,
    /// Monotonic diagnostics for internal transaction cleanup.
    transaction_cleanup_counters: Arc<MandatoryTaskCounters>,
    /// One-way stop state shared by all executor runners.
    stopping: AtomicBool,
    /// Wakeup used to stop every runner after both admissions drain.
    stop_event: Event,
    /// Engine-level fatal state used by mandatory panic supervision.
    poisoner: QuiescentGuard<EnginePoisoner>,
}

impl MandatoryRuntime {
    #[inline]
    fn new(config: &MandatoryRuntimeConfig, poisoner: QuiescentGuard<EnginePoisoner>) -> Self {
        Self {
            executor: async_executor::Executor::new(),
            admission: MandatoryAdmission::new(config.concurrency_limit),
            internal_admission: MandatoryInternalAdmission::new(),
            operation_counters: Arc::new(MandatoryTaskCounters::default()),
            transaction_cleanup_counters: Arc::new(MandatoryTaskCounters::default()),
            stopping: AtomicBool::new(false),
            stop_event: Event::new(),
            poisoner,
        }
    }

    #[inline]
    fn poison_mandatory_panic(&self, metadata: &MandatoryTaskMetadata) -> SharedFatalError {
        self.admission.close();
        let report = Report::new(FatalError::MandatoryTaskPanic).attach(metadata.diagnostic());
        obs::error!(
            "event=engine_poison component=mandatory_runtime action=poison result=error error={report:?}"
        );
        self.poisoner.poison(report)
    }

    /// Close caller admission and wake every capacity waiter.
    #[inline]
    pub(crate) fn close_admission(&self) {
        self.admission.close();
    }

    /// Wait until all accepted caller operations have reached terminal handling.
    #[inline]
    pub(crate) async fn drain_callers(&self) {
        self.admission.drain().await;
    }

    /// Return caller-permit and internal-task blocker counts.
    #[inline]
    pub(crate) fn blocker_counts(&self) -> (usize, usize) {
        let (_, callers) = self.admission.inspect();
        let (_, internal) = self.internal_admission.inspect();
        (callers, internal)
    }

    /// Return an independently sampled fixed-class statistics snapshot.
    #[inline]
    pub(crate) fn stats(&self) -> MandatoryRuntimeStats {
        let (_, operation_active) = self.admission.inspect();
        let (_, cleanup_active) = self.internal_admission.inspect();
        MandatoryRuntimeStats {
            operation: self.operation_counters.snapshot(operation_active),
            transaction_cleanup: self.transaction_cleanup_counters.snapshot(cleanup_active),
        }
    }

    async fn run(&self) {
        self.executor.run(self.wait_for_stop()).await;
    }

    async fn wait_for_stop(&self) {
        loop {
            listener!(self.stop_event => stopped);
            if self.stopping.load(Ordering::Acquire) {
                return;
            }
            stopped.await;
        }
    }

    #[inline]
    fn signal_stop(&self) {
        if !self.stopping.swap(true, Ordering::AcqRel) {
            self.stop_event.notify(usize::MAX);
        }
    }
}

impl Component for MandatoryRuntime {
    type Config = MandatoryRuntimeConfig;
    type Owned = Self;
    type Access = QuiescentGuard<Self>;
    type Error = Report<ConfigError>;

    const NAME: &'static str = "mandatory_runtime";

    async fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> ConfigResult<()> {
        config.validate()?;
        let worker_threads = config.worker_threads;
        let poisoner = registry.dependency::<EnginePoisoner>();
        registry.register::<Self>(Self::new(&config, poisoner));
        let runtime = registry.dependency::<Self>();
        shelf.put::<MandatoryRuntimeWorkers>(PendingMandatoryRuntimeWorkerStartup::new(
            runtime,
            worker_threads,
        ));
        Ok(())
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.guard()
    }

    #[inline]
    fn shutdown(_component: &Self::Owned) {
        // Panic safety: caller/internal admission, stop signalling, and runner
        // joins belong to the later `MandatoryRuntimeWorkers` component.
    }
}

impl Supplier<MandatoryRuntimeWorkers> for MandatoryRuntime {
    type Provision = PendingMandatoryRuntimeWorkerStartup;
}

/// Deferred mandatory-runtime runner startup supplied by the runtime core.
pub(crate) struct PendingMandatoryRuntimeWorkerStartup {
    runtime: QuiescentGuard<MandatoryRuntime>,
    worker_threads: usize,
}

impl PendingMandatoryRuntimeWorkerStartup {
    #[inline]
    fn new(runtime: QuiescentGuard<MandatoryRuntime>, worker_threads: usize) -> Self {
        Self {
            runtime,
            worker_threads,
        }
    }

    /// Start every configured runner and retain rollback ownership until done.
    #[inline]
    fn start(self) -> RuntimeResult<MandatoryRuntimeWorkersOwned> {
        let mut pending = PendingMandatoryRuntimeWorkers::new(self.runtime);
        for runner in 0..self.worker_threads {
            let runner_runtime = pending.runtime.clone();
            let handle =
                thread::spawn_named(format!("Mandatory-Runtime-{}", runner + 1), move || {
                    runtime::block_on(runner_runtime.run())
                })
                .attach("phase=start_mandatory_runtime_runner")?;
            pending.handles.push(handle);
        }
        Ok(pending.into_owned())
    }
}

/// Partial worker owner that joins started runners if startup is interrupted.
struct PendingMandatoryRuntimeWorkers {
    runtime: QuiescentGuard<MandatoryRuntime>,
    handles: Vec<JoinHandle<()>>,
    armed: bool,
}

impl PendingMandatoryRuntimeWorkers {
    #[inline]
    fn new(runtime: QuiescentGuard<MandatoryRuntime>) -> Self {
        Self {
            runtime,
            handles: Vec::new(),
            armed: true,
        }
    }

    #[inline]
    fn into_owned(mut self) -> MandatoryRuntimeWorkersOwned {
        self.armed = false;
        MandatoryRuntimeWorkersOwned {
            runtime: self.runtime.clone(),
            handles: Mutex::new(take(&mut self.handles)),
            shutdown_started: AtomicBool::new(false),
        }
    }
}

impl Drop for PendingMandatoryRuntimeWorkers {
    #[inline]
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.runtime.admission.close();
        self.runtime.internal_admission.close();
        self.runtime.signal_stop();
        let mut panics = FirstPanic::default();
        for handle in take(&mut self.handles) {
            let worker = handle.thread().name().unwrap_or("unknown").to_owned();
            if let Err(payload) = handle.join() {
                obs::error!(
                    "event=worker_startup_rollback component=mandatory_runtime worker={} action=join result=panic payload={}",
                    worker,
                    panic_payload_description(payload.as_ref())
                );
                panics.capture(payload);
            }
        }
        // A component-startup error remains the primary diagnostic; dropping
        // the accumulator forgets captured rollback payloads after all joins.
        drop(panics);
    }
}

/// Late component owner for mandatory runtime runner threads.
pub(crate) struct MandatoryRuntimeWorkers;

impl Component for MandatoryRuntimeWorkers {
    type Config = ();
    type Owned = MandatoryRuntimeWorkersOwned;
    type Access = ();
    type Error = Report<RuntimeError>;

    const NAME: &'static str = "mandatory_runtime_workers";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        mut shelf: ShelfScope<'_, Self>,
    ) -> RuntimeResult<()> {
        let startup = shelf.take::<MandatoryRuntime>();
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

/// Thread-joining owner for the mandatory runtime workers.
pub(crate) struct MandatoryRuntimeWorkersOwned {
    runtime: QuiescentGuard<MandatoryRuntime>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    shutdown_started: AtomicBool,
}

impl MandatoryRuntimeWorkersOwned {
    #[inline]
    fn shutdown(&self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }
        // Normal engine shutdown closes caller admission before component
        // teardown. Bootstrap rollback may reach this owner before the engine
        // lifecycle exists, so close it defensively while still requiring every
        // accepted caller to have drained.
        let mut panics = FirstPanic::default();
        self.runtime.admission.close();
        let (_, callers) = self.runtime.admission.inspect();
        if callers != 0 {
            let message = format!(
                "mandatory runner shutdown requires drained caller admission: callers={callers}"
            );
            obs::error!(
                "event=worker_shutdown component=mandatory_runtime worker=all action=validate_callers result=panic payload={}",
                message
            );
            panics.capture(Box::new(message));
            // Accepted callers can still depend on the runners and internal
            // admission remaining live. Report the invariant without starting
            // worker teardown.
            panics.resume();
            return;
        }
        self.runtime.internal_admission.close();
        runtime::block_on(self.runtime.internal_admission.drain());
        self.runtime.signal_stop();
        for handle in take(&mut *self.handles.lock()) {
            let worker = handle.thread().name().unwrap_or("unknown").to_owned();
            if let Err(payload) = handle.join() {
                obs::error!(
                    "event=worker_shutdown component=mandatory_runtime worker={} action=join result=panic payload={}",
                    worker,
                    panic_payload_description(payload.as_ref())
                );
                panics.capture(payload);
            }
        }
        if !self.runtime.executor.is_empty() {
            let message =
                "mandatory executor must be empty after admission drain and runner join".to_owned();
            obs::error!(
                "event=worker_shutdown component=mandatory_runtime worker=all action=validate_executor result=panic payload={}",
                message
            );
            panics.capture(Box::new(message));
        }
        // Panic safety after caller admission is confirmed drained: both
        // admissions are closed, internal work is drained, every runner is
        // signalled and joined, and terminal validation is complete before the
        // first join or executor-invariant payload is resumed.
        panics.resume();
    }
}

impl QuiescentGuard<MandatoryRuntime> {
    /// Submit a fully prepared caller operation.
    pub(crate) async fn submit<E>(
        &self,
        prepared: E,
    ) -> LifecycleOrFatalResult<CompletionObserver<E::Output>>
    where
        E: PreparedExecution,
    {
        let poison_listener = self.poisoner.listener();
        if let Err(error) = self.poisoner.ensure_healthy() {
            self.admission.close();
            return Err(LifecycleOrFatalError::from(
                error.attach("phase=mandatory_admission_health_check"),
            ));
        }
        let admission_started_at = Instant::now();
        let acquire = self.admission.acquire(self.clone());
        futures::pin_mut!(acquire);
        futures::pin_mut!(poison_listener);
        let permit = match select(acquire, poison_listener).await {
            Either::Left((result, _)) => result?,
            Either::Right((_, _)) => {
                self.admission.close();
                return Err(LifecycleOrFatalError::from(
                    self.poisoner
                        .ensure_healthy()
                        .expect_err("poison event requires a published fatal reason")
                        .attach("phase=mandatory_admission_poison_wake"),
                ));
            }
        };
        // Winning admission is the poison-race linearization point. A later
        // poison does not cancel work that is already admitted and accounted.
        let admission_wait_nanos = elapsed_nanos(admission_started_at);
        let metadata = prepared.metadata();
        let (producer, observer) =
            MandatoryCompletion::endpoints(metadata, Arc::clone(&self.operation_counters));

        // No await or expected rejection exists below this ownership edge.
        let queued_at = Instant::now();
        self.operation_counters
            .record_submitted(admission_wait_nanos);
        let accepted = prepared.accept();
        let task_runtime = self.clone();
        self.executor
            .spawn(task_runtime.supervise_accepted(
                accepted,
                producer,
                permit,
                queued_at,
                admission_wait_nanos,
            ))
            .detach();
        Ok(observer)
    }

    /// Synchronously submit an existing internal correctness obligation.
    #[inline]
    pub(crate) fn submit_internal<J>(&self, job: J) -> StdResult<(), J>
    where
        J: MandatoryInternalTask,
    {
        let Some(permit) = self.internal_admission.try_acquire(self.clone()) else {
            return Err(job);
        };
        let metadata = job.metadata();
        let queued_at = Instant::now();
        let task_runtime = self.clone();
        self.transaction_cleanup_counters.record_submitted(0);
        self.executor
            .spawn(task_runtime.supervise_internal(job, metadata, permit, queued_at))
            .detach();
        Ok(())
    }

    /// Supervise one accepted caller operation through terminal publication.
    ///
    /// The accepted owner remains outside the unwind-caught borrowed execution
    /// future. Normal finish and panic policy methods are non-unwinding by
    /// contract. The owner drops before its permit releases caller capacity.
    async fn supervise_accepted<A>(
        self,
        mut accepted: A,
        producer: CompletionProducer<A::Output>,
        permit: MandatoryPermit,
        queued_at: Instant,
        admission_wait_nanos: usize,
    ) where
        A: AcceptedExecution,
    {
        let started_at = Instant::now();
        let queue_wait_nanos = duration_nanos(started_at.duration_since(queued_at));
        let metadata = producer.metadata().clone();
        let counters = Arc::clone(producer.counters());
        counters.record_started(queue_wait_nanos);
        obs::debug!(
            "event=mandatory_task component=mandatory_runtime action=start result=ok task_class={} task_label={} session_operation={} table_id={} admission_wait_nanos={} queue_wait_nanos={}",
            metadata.task_class(),
            metadata.task_label(),
            OptionalValue(metadata.session_operation()),
            OptionalValue(metadata.table_id()),
            admission_wait_nanos,
            queue_wait_nanos,
        );
        let outcome = AssertUnwindSafe(async { accepted.execute().await })
            .catch_unwind()
            .await;
        let (result, completion_result) = match outcome {
            Ok(completion_result) => {
                let task_result = if completion_result.is_ok() {
                    MandatoryTaskResult::Ok
                } else {
                    MandatoryTaskResult::Error
                };
                accepted.finish();
                (task_result, completion_result)
            }
            Err(panic) => {
                let error = accepted.handle_panic(panic).await;
                self.poison_mandatory_panic(&metadata);
                (MandatoryTaskResult::Panic, Err::<A::Output, _>(error))
            }
        };
        let execution_nanos = elapsed_nanos(started_at);
        counters.record_completed(result, execution_nanos);
        // Publish terminal metrics before waking the observer so an immediate
        // statistics snapshot includes the completion that it just consumed.
        let observer = producer.complete(completion_result);
        obs::debug!(
            "event=mandatory_task component=mandatory_runtime action=finish result={} task_class={} task_label={} session_operation={} table_id={} execution_nanos={} observer={}",
            result.label(),
            metadata.task_class(),
            metadata.task_label(),
            OptionalValue(metadata.session_operation()),
            OptionalValue(metadata.table_id()),
            execution_nanos,
            observer.label(),
        );
        drop(accepted);
        drop(permit);
    }

    /// Supervise one engine-internal obligation through terminal handling.
    ///
    /// Only the borrowed run future is unwind-caught. Preservation and panic
    /// publication are non-unwinding by contract. The job drops before its
    /// permit publishes internal-admission drain progress.
    async fn supervise_internal<J>(
        self,
        mut job: J,
        metadata: MandatoryTaskMetadata,
        permit: MandatoryInternalPermit,
        queued_at: Instant,
    ) where
        J: MandatoryInternalTask,
    {
        let started_at = Instant::now();
        let queue_wait_nanos = duration_nanos(started_at.duration_since(queued_at));
        self.transaction_cleanup_counters
            .record_started(queue_wait_nanos);
        obs::debug!(
            "event=mandatory_task component=mandatory_runtime action=start result=ok task_class={} task_label={} session_operation={} table_id={} admission_wait_nanos=0 queue_wait_nanos={}",
            metadata.task_class(),
            metadata.task_label(),
            OptionalValue(metadata.session_operation()),
            OptionalValue(metadata.table_id()),
            queue_wait_nanos,
        );
        let result = if AssertUnwindSafe(async { job.run().await })
            .catch_unwind()
            .await
            .is_err()
        {
            job.preserve_after_panic();
            let fatal = self.poison_mandatory_panic(&metadata);
            job.publish_panic(fatal.into_completion_bridge());
            MandatoryTaskResult::Panic
        } else {
            MandatoryTaskResult::Ok
        };
        let execution_nanos = elapsed_nanos(started_at);
        self.transaction_cleanup_counters
            .record_completed(result, execution_nanos);
        obs::debug!(
            "event=mandatory_task component=mandatory_runtime action=finish result={} task_class={} task_label={} session_operation={} table_id={} execution_nanos={} observer=none",
            result.label(),
            metadata.task_class(),
            metadata.task_label(),
            OptionalValue(metadata.session_operation()),
            OptionalValue(metadata.table_id()),
            execution_nanos,
        );
        drop(job);
        drop(permit);
    }
}

#[inline]
fn elapsed_nanos(started_at: Instant) -> usize {
    duration_nanos(started_at.elapsed())
}

#[inline]
fn duration_nanos(duration: Duration) -> usize {
    duration.as_nanos() as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::RegistryBuilder;
    use crate::conf::MandatoryRuntimeConfig;
    use crate::error::{FatalError, OperationError};
    use crate::thread::{SpawnTestEvent, fail_spawn_named, observe_spawn_named};
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct SyntheticPrepared {
        moves: Arc<AtomicUsize>,
        finishes: Arc<AtomicUsize>,
        fail: bool,
    }

    impl PreparedExecution for SyntheticPrepared {
        type Output = SyntheticOutput;
        type Accepted = SyntheticAccepted;

        const LABEL: &'static str = "synthetic_prepared";

        #[inline]
        fn metadata(&self) -> MandatoryTaskMetadata {
            MandatoryTaskMetadata::operation(Self::LABEL, None)
        }

        #[inline]
        fn accept(self) -> Self::Accepted {
            self.moves.fetch_add(1, Ordering::Relaxed);
            SyntheticAccepted {
                moves: self.moves,
                finishes: self.finishes,
                fail: self.fail,
            }
        }
    }

    struct SyntheticAccepted {
        moves: Arc<AtomicUsize>,
        finishes: Arc<AtomicUsize>,
        fail: bool,
    }

    impl AcceptedExecution for SyntheticAccepted {
        type Output = SyntheticOutput;

        #[inline]
        async fn execute(&mut self) -> CompletionResult<Self::Output> {
            if self.fail {
                Err(CompletionErrorBridge::capture(
                    Report::new(OperationError::TableNotFound).attach("synthetic error"),
                ))
            } else {
                Ok(SyntheticOutput(self.moves.load(Ordering::Relaxed)))
            }
        }

        #[inline]
        fn finish(&mut self) {
            self.finishes.fetch_add(1, Ordering::Relaxed);
        }

        #[inline]
        async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
            CompletionErrorBridge::capture(
                Report::new(FatalError::MandatoryTaskPanic).attach("synthetic panic"),
            )
        }
    }

    #[derive(Debug)]
    struct SyntheticOutput(usize);

    struct ExecutePanicPrepared {
        dropped: Arc<AtomicUsize>,
        finishes: Arc<AtomicUsize>,
        handled: Arc<AtomicUsize>,
    }

    impl PreparedExecution for ExecutePanicPrepared {
        type Output = ();
        type Accepted = ExecutePanicAccepted;

        const LABEL: &'static str = "execute_panic";

        #[inline]
        fn metadata(&self) -> MandatoryTaskMetadata {
            MandatoryTaskMetadata::operation(Self::LABEL, None)
        }

        #[inline]
        fn accept(self) -> Self::Accepted {
            ExecutePanicAccepted {
                dropped: self.dropped,
                finishes: self.finishes,
                handled: self.handled,
            }
        }
    }

    struct ExecutePanicAccepted {
        dropped: Arc<AtomicUsize>,
        finishes: Arc<AtomicUsize>,
        handled: Arc<AtomicUsize>,
    }

    impl Drop for ExecutePanicAccepted {
        #[inline]
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl AcceptedExecution for ExecutePanicAccepted {
        type Output = ();

        #[inline]
        async fn execute(&mut self) -> CompletionResult<Self::Output> {
            panic!("synthetic accepted execution panic");
        }

        #[inline]
        fn finish(&mut self) {
            self.finishes.fetch_add(1, Ordering::Relaxed);
        }

        #[inline]
        async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
            self.handled.fetch_add(1, Ordering::Relaxed);
            CompletionErrorBridge::capture(
                Report::new(FatalError::MandatoryTaskPanic)
                    .attach("synthetic accepted execution panic"),
            )
        }
    }

    struct SyntheticPanicInternal {
        preserved: Arc<AtomicUsize>,
        published: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
        completion: Arc<Completion<()>>,
    }

    impl Drop for SyntheticPanicInternal {
        #[inline]
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl MandatoryInternalTask for SyntheticPanicInternal {
        const LABEL: &'static str = "synthetic_internal_panic";

        #[inline]
        fn metadata(&self) -> MandatoryTaskMetadata {
            MandatoryTaskMetadata::transaction_cleanup(Self::LABEL, None)
        }

        #[inline]
        async fn run(&mut self) {
            panic!("synthetic mandatory internal task panic");
        }

        #[inline]
        fn preserve_after_panic(&mut self) {
            self.preserved.fetch_add(1, Ordering::Relaxed);
        }

        #[inline]
        fn publish_panic(&mut self, error: CompletionErrorBridge) {
            self.published.fetch_add(1, Ordering::Relaxed);
            self.completion.complete(Err(error));
        }
    }

    struct GatedPrepared {
        started: Arc<Completion<()>>,
        release: Arc<Completion<()>>,
    }

    impl PreparedExecution for GatedPrepared {
        type Output = ();
        type Accepted = GatedAccepted;

        const LABEL: &'static str = "gated_operation";

        #[inline]
        fn metadata(&self) -> MandatoryTaskMetadata {
            MandatoryTaskMetadata::operation(Self::LABEL, None)
        }

        #[inline]
        fn accept(self) -> Self::Accepted {
            GatedAccepted {
                started: self.started,
                release: self.release,
            }
        }
    }

    struct GatedAccepted {
        started: Arc<Completion<()>>,
        release: Arc<Completion<()>>,
    }

    impl AcceptedExecution for GatedAccepted {
        type Output = ();

        #[inline]
        async fn execute(&mut self) -> CompletionResult<Self::Output> {
            self.started.complete(Ok(()));
            self.release.wait_result().await
        }

        #[inline]
        fn finish(&mut self) {}

        #[inline]
        async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
            CompletionErrorBridge::capture(
                Report::new(FatalError::MandatoryTaskPanic).attach("gated operation panic"),
            )
        }
    }

    struct SignalInternal {
        completed: Arc<Completion<()>>,
    }

    impl MandatoryInternalTask for SignalInternal {
        const LABEL: &'static str = "signal_internal";

        #[inline]
        fn metadata(&self) -> MandatoryTaskMetadata {
            MandatoryTaskMetadata::transaction_cleanup(Self::LABEL, None)
        }

        #[inline]
        async fn run(&mut self) {
            self.completed.complete(Ok(()));
        }

        #[inline]
        fn preserve_after_panic(&mut self) {}

        #[inline]
        fn publish_panic(&mut self, error: CompletionErrorBridge) {
            self.completed.complete(Err(error));
        }
    }

    struct OverlapRendezvous {
        registrations: AtomicUsize,
        ready: Completion<()>,
    }

    impl OverlapRendezvous {
        #[inline]
        fn new() -> Self {
            Self {
                registrations: AtomicUsize::new(0),
                ready: Completion::new(),
            }
        }

        #[inline]
        async fn arrive(&self) -> CompletionResult<()> {
            let registrations = self.registrations.fetch_add(1, Ordering::AcqRel) + 1;
            assert!(registrations <= 2, "overlap task registered more than once");
            if registrations == 2 {
                self.ready.complete(Ok(()));
            }
            self.ready.wait_result().await
        }
    }

    struct OverlapPrepared {
        rendezvous: Arc<OverlapRendezvous>,
    }

    impl PreparedExecution for OverlapPrepared {
        type Output = ();
        type Accepted = OverlapAccepted;

        const LABEL: &'static str = "overlap_operation";

        #[inline]
        fn metadata(&self) -> MandatoryTaskMetadata {
            MandatoryTaskMetadata::operation(Self::LABEL, None)
        }

        #[inline]
        fn accept(self) -> Self::Accepted {
            OverlapAccepted {
                rendezvous: self.rendezvous,
            }
        }
    }

    struct OverlapAccepted {
        rendezvous: Arc<OverlapRendezvous>,
    }

    impl AcceptedExecution for OverlapAccepted {
        type Output = ();

        #[inline]
        async fn execute(&mut self) -> CompletionResult<Self::Output> {
            self.rendezvous.arrive().await
        }

        #[inline]
        fn finish(&mut self) {}

        #[inline]
        async fn handle_panic(&mut self, _panic: Box<dyn Any + Send>) -> CompletionErrorBridge {
            CompletionErrorBridge::capture(
                Report::new(FatalError::MandatoryTaskPanic).attach("overlap operation panic"),
            )
        }
    }

    #[test]
    fn exclusive_completion_moves_non_clone_output_once() {
        struct NonClone(usize);

        runtime::block_on(async {
            let completion = Completion::new();
            completion.complete(Ok(NonClone(7)));
            assert_eq!(completion.wait_take_result().await.unwrap().0, 7);
            assert!(matches!(
                completion.try_take_result(),
                CompletionTake::Consumed
            ));
        });
    }

    #[test]
    fn mandatory_observer_retains_operation_error() {
        runtime::block_on(async {
            let metadata = MandatoryTaskMetadata::operation("test", None);
            let (producer, observer) = MandatoryCompletion::<()>::endpoints(
                metadata,
                Arc::new(MandatoryTaskCounters::default()),
            );
            let _ = producer.complete(Err::<(), _>(CompletionErrorBridge::capture(
                Report::new(OperationError::TableNotFound).attach("operation=test"),
            )));
            let error = observer.wait().await.unwrap_err();
            assert_eq!(
                error.downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
        });
    }

    #[test]
    fn worker_component_owns_runner_startup() {
        runtime::block_on(async {
            let _failure = fail_spawn_named("Mandatory-Runtime-1");
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<MandatoryRuntime>(
                    MandatoryRuntimeConfig::default()
                        .worker_threads(1)
                        .concurrency_limit(1),
                )
                .await
                .unwrap();

            let report = builder
                .build::<MandatoryRuntimeWorkers>(())
                .await
                .unwrap_err();
            assert_eq!(report.current_context(), &RuntimeError::BackgroundSpawn);
            let output = format!("{report:?}");
            assert!(output.contains("phase=start_mandatory_runtime_runner"));
            assert!(output.contains("thread_name=Mandatory-Runtime-1"));
        });
    }

    #[test]
    fn detached_success_is_dropped_once() {
        struct DropCount(Arc<AtomicUsize>);

        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let counters = Arc::new(MandatoryTaskCounters::default());
        let (producer, observer) = MandatoryCompletion::<DropCount>::endpoints(
            MandatoryTaskMetadata::operation("test", None),
            Arc::clone(&counters),
        );
        drop(observer);
        let _ = producer.complete(Ok(DropCount(Arc::clone(&drops))));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(counters.detached_observer_count.load(Ordering::Relaxed), 1);

        let (producer, observer) = MandatoryCompletion::<DropCount>::endpoints(
            MandatoryTaskMetadata::operation("test", None),
            Arc::clone(&counters),
        );
        assert!(matches!(
            producer.complete(Ok(DropCount(Arc::clone(&drops)))),
            PublishedObserver::Attached
        ));
        drop(observer);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        assert_eq!(counters.detached_observer_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn synthetic_prepared_execution_moves_and_finishes_once() {
        let (registry, mandatory) = runtime::block_on(async {
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<MandatoryRuntime>(
                    MandatoryRuntimeConfig::default()
                        .worker_threads(1)
                        .concurrency_limit(1),
                )
                .await
                .unwrap();
            builder.build::<MandatoryRuntimeWorkers>(()).await.unwrap();
            let registry = builder.finish();
            let mandatory = registry.dependency::<MandatoryRuntime>();
            let moves = Arc::new(AtomicUsize::new(0));
            let finishes = Arc::new(AtomicUsize::new(0));
            assert_eq!(mandatory.stats(), MandatoryRuntimeStats::default());

            let observer = mandatory
                .submit(SyntheticPrepared {
                    moves: Arc::clone(&moves),
                    finishes: Arc::clone(&finishes),
                    fail: false,
                })
                .await
                .unwrap();
            assert_eq!(observer.wait().await.unwrap().0, 1);
            assert_eq!(moves.load(Ordering::Relaxed), 1);
            assert_eq!(finishes.load(Ordering::Relaxed), 1);
            let stats = mandatory.stats();
            assert_eq!(stats.operation.submitted_count, 1);
            assert_eq!(stats.operation.started_count, 1);
            assert_eq!(stats.operation.completed_count, 1);
            assert_eq!(stats.operation.error_count, 0);
            assert_eq!(stats.operation.panic_count, 0);
            assert_eq!(stats.operation.detached_observer_count, 0);
            assert_eq!(stats.transaction_cleanup, MandatoryTaskStats::default());
            mandatory.drain_callers().await;
            assert_eq!(mandatory.stats().operation.active_count, 0);

            (registry, mandatory)
        });
        mandatory.close_admission();
        runtime::block_on(mandatory.drain_callers());
        registry
            .shutdown_all()
            .propagate_or_suppress("mandatory_runtime_test");
    }

    #[test]
    fn mandatory_shutdown_joins_every_runner_before_resuming_first_panic() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed_events = Arc::clone(&events);
        let observer = observe_spawn_named(move |event| {
            observed_events.lock().push(event.clone());
            match event {
                SpawnTestEvent::Finished(name) if name == "Mandatory-Runtime-1" => {
                    panic::panic_any("first mandatory runner panic");
                }
                SpawnTestEvent::Finished(name) if name == "Mandatory-Runtime-2" => {
                    panic::panic_any("second mandatory runner panic");
                }
                _ => {}
            }
        });
        let (registry, mandatory) = runtime::block_on(async {
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<MandatoryRuntime>(
                    MandatoryRuntimeConfig::default()
                        .worker_threads(2)
                        .concurrency_limit(1),
                )
                .await
                .unwrap();
            builder.build::<MandatoryRuntimeWorkers>(()).await.unwrap();
            let registry = builder.finish();
            let mandatory = registry.dependency::<MandatoryRuntime>();
            (registry, mandatory)
        });
        mandatory.close_admission();
        runtime::block_on(mandatory.drain_callers());

        let outcome = registry.shutdown_all();
        assert!(outcome.is_degraded());
        let events = events.lock();
        assert!(events.contains(&SpawnTestEvent::Finished("Mandatory-Runtime-1".to_owned())));
        assert!(events.contains(&SpawnTestEvent::Finished("Mandatory-Runtime-2".to_owned())));
        drop(events);
        drop(observer);

        let payload = panic::catch_unwind(AssertUnwindSafe(|| {
            outcome.propagate_or_suppress("mandatory_multi_runner_test");
        }))
        .unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("first mandatory runner panic")
        );
    }

    #[test]
    fn mandatory_shutdown_preserves_runners_until_callers_drain() {
        let (registry, mandatory) = runtime::block_on(async {
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<MandatoryRuntime>(
                    MandatoryRuntimeConfig::default()
                        .worker_threads(1)
                        .concurrency_limit(1),
                )
                .await
                .unwrap();
            builder.build::<MandatoryRuntimeWorkers>(()).await.unwrap();
            let registry = builder.finish();
            let mandatory = registry.dependency::<MandatoryRuntime>();
            (registry, mandatory)
        });
        let caller = runtime::block_on(mandatory.admission.acquire(mandatory.clone())).unwrap();
        let workers = MandatoryRuntimeWorkersOwned {
            runtime: mandatory.clone(),
            handles: Mutex::new(vec![std::thread::spawn(|| {})]),
            shutdown_started: AtomicBool::new(false),
        };

        let payload = panic::catch_unwind(AssertUnwindSafe(|| workers.shutdown())).unwrap_err();
        let caller_admission = mandatory.admission.inspect();
        let internal_admission = mandatory.internal_admission.inspect();
        let stopping = mandatory.stopping.load(Ordering::Acquire);
        let handle_count = workers.handles.lock().len();

        // Always restore the normal shutdown preconditions before asserting so
        // a regression cannot leave mandatory-runtime runners detached.
        drop(caller);
        for handle in take(&mut *workers.handles.lock()) {
            handle.join().unwrap();
        }
        drop(workers);
        mandatory.close_admission();
        runtime::block_on(mandatory.drain_callers());
        registry
            .shutdown_all()
            .propagate_or_suppress("mandatory_active_caller_shutdown_test");

        assert_eq!(
            payload.downcast_ref::<String>().map(String::as_str),
            Some("mandatory runner shutdown requires drained caller admission: callers=1")
        );
        assert_eq!(caller_admission, (true, 1));
        assert_eq!(internal_admission, (false, 0));
        assert!(!stopping);
        assert_eq!(handle_count, 1);
    }

    #[test]
    fn ordinary_error_and_observer_detach_are_counted_by_outcome() {
        let (registry, mandatory) = runtime::block_on(async {
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<MandatoryRuntime>(
                    MandatoryRuntimeConfig::default()
                        .worker_threads(1)
                        .concurrency_limit(1),
                )
                .await
                .unwrap();
            builder.build::<MandatoryRuntimeWorkers>(()).await.unwrap();
            let registry = builder.finish();
            let mandatory = registry.dependency::<MandatoryRuntime>();
            let moves = Arc::new(AtomicUsize::new(0));
            let finishes = Arc::new(AtomicUsize::new(0));

            let error = mandatory
                .submit(SyntheticPrepared {
                    moves: Arc::clone(&moves),
                    finishes: Arc::clone(&finishes),
                    fail: true,
                })
                .await
                .unwrap()
                .wait()
                .await
                .unwrap_err();
            assert_eq!(
                error.downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
            let stats = mandatory.stats().operation;
            assert_eq!(stats.submitted_count, 1);
            assert_eq!(stats.started_count, 1);
            assert_eq!(stats.completed_count, 1);
            assert_eq!(stats.error_count, 1);
            assert_eq!(stats.panic_count, 0);
            mandatory.drain_callers().await;

            let observer = mandatory
                .submit(SyntheticPrepared {
                    moves,
                    finishes,
                    fail: false,
                })
                .await
                .unwrap();
            drop(observer);
            mandatory.drain_callers().await;

            let stats = mandatory.stats().operation;
            assert_eq!(stats.submitted_count, 2);
            assert_eq!(stats.started_count, 2);
            assert_eq!(stats.completed_count, 2);
            assert_eq!(stats.error_count, 1);
            assert_eq!(stats.panic_count, 0);
            assert_eq!(stats.detached_observer_count, 1);
            assert_eq!(stats.active_count, 0);
            (registry, mandatory)
        });
        mandatory.close_admission();
        runtime::block_on(mandatory.drain_callers());
        registry
            .shutdown_all()
            .propagate_or_suppress("mandatory_runtime_test");
    }

    #[test]
    fn internal_work_progresses_with_one_runner_and_saturated_caller_capacity() {
        let (registry, mandatory) = runtime::block_on(async {
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<MandatoryRuntime>(
                    MandatoryRuntimeConfig::default()
                        .worker_threads(1)
                        .concurrency_limit(1),
                )
                .await
                .unwrap();
            builder.build::<MandatoryRuntimeWorkers>(()).await.unwrap();
            let registry = builder.finish();
            let mandatory = registry.dependency::<MandatoryRuntime>();
            for _ in 0..32 {
                let first_started = Arc::new(Completion::new());
                let first_release = Arc::new(Completion::new());
                let first = mandatory
                    .submit(GatedPrepared {
                        started: Arc::clone(&first_started),
                        release: Arc::clone(&first_release),
                    })
                    .await
                    .unwrap();
                first_started.wait_result().await.unwrap();

                let moves = Arc::new(AtomicUsize::new(0));
                let finishes = Arc::new(AtomicUsize::new(0));
                let waiting = mandatory.submit(SyntheticPrepared {
                    moves: Arc::clone(&moves),
                    finishes: Arc::clone(&finishes),
                    fail: false,
                });
                futures::pin_mut!(waiting);
                assert!(matches!(
                    futures::poll!(waiting.as_mut()),
                    std::task::Poll::Pending
                ));
                assert_eq!(moves.load(Ordering::Relaxed), 0);

                let internal_completed = Arc::new(Completion::new());
                assert!(
                    mandatory
                        .submit_internal(SignalInternal {
                            completed: Arc::clone(&internal_completed),
                        })
                        .is_ok()
                );
                internal_completed.wait_result().await.unwrap();
                mandatory.internal_admission.drain().await;
                assert!(matches!(
                    futures::poll!(waiting.as_mut()),
                    std::task::Poll::Pending
                ));
                assert_eq!(moves.load(Ordering::Relaxed), 0);

                first_release.complete(Ok(()));
                first.wait().await.unwrap();
                let second = waiting.await.unwrap();
                assert_eq!(second.wait().await.unwrap().0, 1);
                mandatory.drain_callers().await;
            }

            let stats = mandatory.stats();
            assert_eq!(stats.operation.submitted_count, 64);
            assert_eq!(stats.operation.completed_count, 64);
            assert_eq!(stats.operation.active_count, 0);
            assert_eq!(stats.transaction_cleanup.submitted_count, 32);
            assert_eq!(stats.transaction_cleanup.completed_count, 32);
            assert_eq!(stats.transaction_cleanup.active_count, 0);
            (registry, mandatory)
        });
        mandatory.close_admission();
        runtime::block_on(mandatory.drain_callers());
        registry
            .shutdown_all()
            .propagate_or_suppress("mandatory_runtime_test");
    }

    #[test]
    fn independent_accepted_tasks_overlap_without_blocking_runners() {
        let (registry, mandatory) = runtime::block_on(async {
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<MandatoryRuntime>(
                    MandatoryRuntimeConfig::default()
                        .worker_threads(2)
                        .concurrency_limit(2),
                )
                .await
                .unwrap();
            builder.build::<MandatoryRuntimeWorkers>(()).await.unwrap();
            let registry = builder.finish();
            let mandatory = registry.dependency::<MandatoryRuntime>();
            for _ in 0..32 {
                let rendezvous = Arc::new(OverlapRendezvous::new());
                let first = mandatory
                    .submit(OverlapPrepared {
                        rendezvous: Arc::clone(&rendezvous),
                    })
                    .await
                    .unwrap();
                let second = mandatory
                    .submit(OverlapPrepared {
                        rendezvous: Arc::clone(&rendezvous),
                    })
                    .await
                    .unwrap();

                first.wait().await.unwrap();
                second.wait().await.unwrap();
                assert_eq!(rendezvous.registrations.load(Ordering::Acquire), 2);
                mandatory.drain_callers().await;
            }
            let stats = mandatory.stats().operation;
            assert_eq!(stats.submitted_count, 64);
            assert_eq!(stats.started_count, 64);
            assert_eq!(stats.completed_count, 64);
            assert_eq!(stats.active_count, 0);
            (registry, mandatory)
        });
        mandatory.close_admission();
        runtime::block_on(mandatory.drain_callers());
        registry
            .shutdown_all()
            .propagate_or_suppress("mandatory_runtime_test");
    }

    #[test]
    fn internal_panic_preserves_poisons_publishes_and_releases() {
        let (registry, mandatory, dropped) = runtime::block_on(async {
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<MandatoryRuntime>(
                    MandatoryRuntimeConfig::default()
                        .worker_threads(1)
                        .concurrency_limit(1),
                )
                .await
                .unwrap();
            builder.build::<MandatoryRuntimeWorkers>(()).await.unwrap();
            let registry = builder.finish();
            let mandatory = registry.dependency::<MandatoryRuntime>();
            let preserved = Arc::new(AtomicUsize::new(0));
            let published = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            let completion = Arc::new(Completion::new());

            assert!(
                mandatory
                    .submit_internal(SyntheticPanicInternal {
                        preserved: Arc::clone(&preserved),
                        published: Arc::clone(&published),
                        dropped: Arc::clone(&dropped),
                        completion: Arc::clone(&completion),
                    })
                    .is_ok(),
                "open internal admission accepts the task"
            );
            let error = completion.wait_result().await.unwrap_err();
            assert_eq!(
                error.downcast_ref::<FatalError>(),
                Some(&FatalError::MandatoryTaskPanic)
            );
            assert_eq!(preserved.load(Ordering::Relaxed), 1);
            assert_eq!(published.load(Ordering::Relaxed), 1);
            mandatory.internal_admission.drain().await;
            let stats = mandatory.stats();
            assert_eq!(stats.operation, MandatoryTaskStats::default());
            assert_eq!(stats.transaction_cleanup.submitted_count, 1);
            assert_eq!(stats.transaction_cleanup.started_count, 1);
            assert_eq!(stats.transaction_cleanup.completed_count, 1);
            assert_eq!(stats.transaction_cleanup.error_count, 0);
            assert_eq!(stats.transaction_cleanup.panic_count, 1);
            assert_eq!(stats.transaction_cleanup.detached_observer_count, 0);
            assert_eq!(stats.transaction_cleanup.active_count, 0);

            (registry, mandatory, dropped)
        });
        mandatory.close_admission();
        runtime::block_on(mandatory.drain_callers());
        registry
            .shutdown_all()
            .propagate_or_suppress("mandatory_runtime_test");
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn accepted_execute_panic_invokes_policy_without_finish() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let finishes = Arc::new(AtomicUsize::new(0));
        let handled = Arc::new(AtomicUsize::new(0));
        let (registry, mandatory) = runtime::block_on(async {
            let mut builder = RegistryBuilder::new();
            builder.build::<EnginePoisoner>(()).await.unwrap();
            builder
                .build::<MandatoryRuntime>(
                    MandatoryRuntimeConfig::default()
                        .worker_threads(1)
                        .concurrency_limit(1),
                )
                .await
                .unwrap();
            builder.build::<MandatoryRuntimeWorkers>(()).await.unwrap();
            let registry = builder.finish();
            let mandatory = registry.dependency::<MandatoryRuntime>();

            let observer = mandatory
                .submit(ExecutePanicPrepared {
                    dropped: Arc::clone(&dropped),
                    finishes: Arc::clone(&finishes),
                    handled: Arc::clone(&handled),
                })
                .await
                .unwrap();
            let error = observer.wait().await.unwrap_err();
            assert_eq!(
                error.downcast_ref::<FatalError>().copied(),
                Some(FatalError::MandatoryTaskPanic)
            );
            assert_eq!(finishes.load(Ordering::Relaxed), 0);
            assert_eq!(handled.load(Ordering::Relaxed), 1);
            let poison = mandatory.poisoner.poison_error().unwrap();
            let poison = format!("{poison:?}");
            assert!(poison.contains("task_class=operation"), "{poison}");
            assert!(poison.contains("task_label=execute_panic"), "{poison}");
            let rejected = mandatory
                .submit(SyntheticPrepared {
                    moves: Arc::new(AtomicUsize::new(0)),
                    finishes: Arc::new(AtomicUsize::new(0)),
                    fail: false,
                })
                .await;
            let error = match rejected {
                Ok(_) => panic!("poisoned mandatory runtime must reject new work"),
                Err(error) => error,
            };
            let LifecycleOrFatalError::Fatal(error) = error else {
                panic!("poisoned mandatory admission must remain Fatal")
            };
            assert_eq!(
                error.downcast_ref::<FatalError>().copied(),
                Some(FatalError::MandatoryTaskPanic)
            );
            assert!(error.downcast_ref::<LifecycleError>().is_none());
            mandatory.drain_callers().await;
            let stats = mandatory.stats().operation;
            assert_eq!(stats.submitted_count, 1);
            assert_eq!(stats.started_count, 1);
            assert_eq!(stats.completed_count, 1);
            assert_eq!(stats.error_count, 0);
            assert_eq!(stats.panic_count, 1);
            assert_eq!(stats.detached_observer_count, 0);
            assert_eq!(stats.active_count, 0);

            (registry, mandatory)
        });
        mandatory.close_admission();
        runtime::block_on(mandatory.drain_callers());
        registry
            .shutdown_all()
            .propagate_or_suppress("mandatory_runtime_test");
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }
}
