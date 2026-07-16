//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands. See
//! `docs/engine-component-lifetime.md` for the runtime-versus-owner lifetime
//! model that this module enforces with the component registry.
use crate::buffer::PoolRole;
use crate::buffer::SharedPoolEvictorWorkers;
use crate::catalog::{Catalog, CatalogConfig};
use crate::component::{
    ComponentRegistry, DiskPoolConfig, EnginePools, IndexPoolConfig, MetaPoolConfig,
    RegistryBuilder,
};
use crate::conf::EngineConfig;
use crate::engine_poison::EnginePoisoner;
use crate::error::{LifecycleError, LifecycleResult, Result};
use crate::file::fs::{FileSystem, FileSystemWorkers};
use crate::id::SessionID;
use crate::lock::LockManager;
use crate::obs;
use crate::quiescent::QuiescentGuard;
use crate::session::{Session, SessionRegistry};
use crate::trx::sys::TransactionSystem;
use crate::{DiskPool, IndexPool, MemPool, MetaPool};
use error_stack::{Report, ResultExt, ensure};
use event_listener::{Event, EventListener, Listener, listener};
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::mem::forget;
use std::ops::Deref;
use std::result;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread::yield_now;

const FIRST_SESSION_ID: SessionID = SessionID::new(1);
// Engine lifecycle admission uses one packed atomic word so admission and
// shutdown closure live in a single CAS domain. The low bits encode
// `EngineLifecycleState`; the remaining high bits count active admissions.
const LIFECYCLE_STATE_BITS: usize = 2;
const LIFECYCLE_STATE_MASK: usize = (1 << LIFECYCLE_STATE_BITS) - 1;
const ONE_ACTIVE_ADMISSION: usize = 1 << LIFECYCLE_STATE_BITS;
const MAX_ACTIVE_ADMISSIONS: usize = usize::MAX >> LIFECYCLE_STATE_BITS;

#[repr(usize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EngineLifecycleState {
    Running = 0,
    ShuttingDown = 1,
    Shutdown = 2,
}

impl EngineLifecycleState {
    #[inline]
    const fn as_usize(self) -> usize {
        self as usize
    }
}

impl TryFrom<usize> for EngineLifecycleState {
    type Error = usize;

    #[inline]
    fn try_from(value: usize) -> result::Result<Self, Self::Error> {
        match value {
            x if x == EngineLifecycleState::Running.as_usize() => Ok(EngineLifecycleState::Running),
            x if x == EngineLifecycleState::ShuttingDown.as_usize() => {
                Ok(EngineLifecycleState::ShuttingDown)
            }
            x if x == EngineLifecycleState::Shutdown.as_usize() => {
                Ok(EngineLifecycleState::Shutdown)
            }
            x => Err(x),
        }
    }
}

struct EngineLifecycle {
    /// Packed lifecycle state and active admission count.
    ///
    /// Bits `[0, LIFECYCLE_STATE_BITS)` store [`EngineLifecycleState`]. The
    /// upper bits store the number of live [`EngineAdmission`] tokens. Keeping
    /// both in one atomic prevents the shutdown close-admission transition from
    /// racing independently with new admission increments.
    state: AtomicUsize,
    admission_released: Event,
    runtime_refs: AtomicUsize,
    runtime_refs_released: Event,
    shutdown_started: Event,
    shutdown_lock: Mutex<()>,
}

impl EngineLifecycle {
    #[inline]
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(EngineLifecycleState::Running.as_usize()),
            admission_released: Event::new(),
            runtime_refs: AtomicUsize::new(0),
            runtime_refs_released: Event::new(),
            shutdown_started: Event::new(),
            shutdown_lock: Mutex::new(()),
        }
    }

    #[inline]
    const fn active_admissions_from_word(word: usize) -> usize {
        word >> LIFECYCLE_STATE_BITS
    }

    #[inline]
    fn state(&self) -> EngineLifecycleState {
        let state = self.state.load(Ordering::Acquire) & LIFECYCLE_STATE_MASK;
        EngineLifecycleState::try_from(state)
            .unwrap_or_else(|state| panic!("invalid engine lifecycle state: {state}"))
    }

    #[inline]
    fn admit(&self) -> LifecycleResult<EngineAdmission<'_>> {
        loop {
            let word = self.state.load(Ordering::Acquire);
            let state = EngineLifecycleState::try_from(word & LIFECYCLE_STATE_MASK)
                .unwrap_or_else(|state| panic!("invalid engine lifecycle state: {state}"));
            ensure!(
                state == EngineLifecycleState::Running,
                LifecycleError::Shutdown
            );
            assert!(
                Self::active_admissions_from_word(word) < MAX_ACTIVE_ADMISSIONS,
                "engine admission count overflow"
            );
            let next = word + ONE_ACTIVE_ADMISSION;
            if self
                .state
                .compare_exchange_weak(word, next, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(EngineAdmission {
                    lifecycle: self,
                    _not_send: PhantomData,
                });
            }
        }
    }

    #[inline]
    fn close_admission(&self) {
        loop {
            let word = self.state.load(Ordering::Acquire);
            let state = EngineLifecycleState::try_from(word & LIFECYCLE_STATE_MASK)
                .unwrap_or_else(|state| panic!("invalid engine lifecycle state: {state}"));
            match state {
                EngineLifecycleState::Running => {
                    // Close future admission by flipping only the lifecycle
                    // state bits to ShuttingDown while preserving the live
                    // admission count in the upper bits.
                    let next = (word & !LIFECYCLE_STATE_MASK)
                        | EngineLifecycleState::ShuttingDown.as_usize();
                    if self
                        .state
                        .compare_exchange_weak(word, next, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        self.shutdown_started.notify(usize::MAX);
                        return;
                    }
                }
                EngineLifecycleState::ShuttingDown | EngineLifecycleState::Shutdown => return,
            }
        }
    }

    #[inline]
    fn wait_for_admissions_drained(&self) {
        loop {
            if Self::active_admissions_from_word(self.state.load(Ordering::Acquire)) == 0 {
                return;
            }
            listener!(self.admission_released => admission_released);
            if Self::active_admissions_from_word(self.state.load(Ordering::Acquire)) == 0 {
                return;
            }
            admission_released.wait();
        }
    }

    #[inline]
    fn retain_runtime_ref(&self) {
        let refs = self.runtime_refs.fetch_add(1, Ordering::AcqRel);
        assert!(refs < usize::MAX, "engine runtime ref count overflow");
    }

    #[inline]
    fn release_runtime_ref(&self) {
        let refs = self.runtime_refs.fetch_sub(1, Ordering::AcqRel);
        assert!(refs > 0, "engine runtime ref count underflow");
        if refs == 1 && self.state() != EngineLifecycleState::Running {
            self.runtime_refs_released.notify(usize::MAX);
        }
    }

    #[inline]
    fn runtime_refs(&self) -> usize {
        self.runtime_refs.load(Ordering::Acquire)
    }

    #[inline]
    fn wait_for_runtime_refs_drained(&self) {
        loop {
            if self.runtime_refs() == 0 {
                return;
            }
            listener!(self.runtime_refs_released => runtime_refs_released);
            if self.runtime_refs() == 0 {
                return;
            }
            runtime_refs_released.wait();
        }
    }

    #[inline]
    fn mark_shutdown(&self) {
        let word = self.state.load(Ordering::Acquire);
        let state = EngineLifecycleState::try_from(word & LIFECYCLE_STATE_MASK)
            .unwrap_or_else(|state| panic!("invalid engine lifecycle state: {state}"));
        debug_assert_eq!(state, EngineLifecycleState::ShuttingDown);
        debug_assert_eq!(Self::active_admissions_from_word(word), 0);
        self.state
            .store(EngineLifecycleState::Shutdown.as_usize(), Ordering::Release);
    }

    #[inline]
    fn release_admission(&self) {
        let word = self.state.fetch_sub(ONE_ACTIVE_ADMISSION, Ordering::AcqRel);
        let active_admissions = Self::active_admissions_from_word(word);
        assert!(active_admissions > 0, "engine admission count underflow");
        if active_admissions == 1 {
            self.admission_released.notify(usize::MAX);
        }
    }

    /// Registers for the transition away from the running state.
    #[inline]
    fn shutdown_listener(&self) -> EventListener {
        self.shutdown_started.listen()
    }
}

/// Short-lived proof that an operation entered while the engine was running.
///
/// The token keeps the engine's active-admission count nonzero until immediate
/// lifecycle validation, local runtime lookup, or strong pinning is complete.
/// Callers must not hold it across user callbacks, statement execution,
/// blocking I/O, registry guard retention, or `.await` points.
pub(crate) struct EngineAdmission<'a> {
    lifecycle: &'a EngineLifecycle,
    _not_send: PhantomData<*mut ()>,
}

impl Drop for EngineAdmission<'_> {
    #[inline]
    fn drop(&mut self) {
        self.lifecycle.release_admission();
    }
}

/// `Engine` is the public owner and session factory for one storage runtime.
///
/// The owner coordinates explicit shutdown and final component teardown. Public
/// [`Session`] values keep weak reachability plus engine-local identity, and
/// operations acquire strong runtime access internally only for the duration of
/// the operation. Runtime internals are not exposed through the public facade.
pub struct Engine {
    // Field order is part of owner teardown: after `Drop::drop` returns, the
    // owner runtime ref is released before component owners are dropped.
    inner: Arc<EngineInner>,
    components: Option<ComponentRegistry>,
}

impl Engine {
    /// Returns the shared engine runtime state.
    #[inline]
    pub(crate) fn inner(&self) -> &Arc<EngineInner> {
        &self.inner
    }

    #[inline]
    fn components(&self) -> &ComponentRegistry {
        self.components
            .as_ref()
            .expect("engine owner keeps component registry until drop")
    }

    /// Create a new session while the engine is still running.
    #[inline]
    pub fn new_session(&self) -> Result<Session> {
        let inner = self.inner();
        Ok(inner.with_admitted_operation(|| {
            let id = inner.next_session_id();
            inner
                .session_registry
                .create_session(inner, EngineRef::new(Arc::clone(inner)), id)
        })?)
    }

    /// Return the shared catalog handle.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "test-only catalog"))]
    pub(crate) fn catalog(&self) -> &Catalog {
        self.inner().catalog()
    }

    /// Return the shared logical lock manager.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "test-only lock_manager"))]
    pub(crate) fn lock_manager(&self) -> &QuiescentGuard<LockManager> {
        self.inner().lock_manager()
    }

    /// Try to clone the crate-private shared runtime handle while the engine is
    /// still running.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "transitional internal runtime handle")
    )]
    pub(crate) fn new_ref(&self) -> Result<EngineRef> {
        let inner = self.inner();
        Ok(inner.with_admitted_operation(|| EngineRef::new(Arc::clone(inner)))?)
    }

    /// Try to complete idempotent engine shutdown without waiting for active work.
    ///
    /// `try_shutdown` rejects new work immediately, drains in-flight admission,
    /// and returns an error with shutdown-busy context if active operations,
    /// active transactions, abandoned transaction cleanup, or internal runtime
    /// pins are still alive.
    ///
    /// This path is valid after storage poison. Poison only blocks admission; it
    /// does not replace the owner-side responsibility to stop background workers
    /// and drop components in the registered order.
    #[inline]
    pub fn try_shutdown(&self) -> Result<()> {
        let inner = self.inner();
        if inner.lifecycle.state() == EngineLifecycleState::Shutdown {
            return Ok(());
        }
        obs::info!(
            "event=engine_lifecycle component=engine action=shutdown_start result=ok mode=try"
        );
        inner.lifecycle.close_admission();
        inner.lifecycle.wait_for_admissions_drained();

        let _shutdown = inner.lifecycle.shutdown_lock.lock();
        if inner.lifecycle.state() == EngineLifecycleState::Shutdown {
            obs::info!(
                "event=engine_lifecycle component=engine action=shutdown_finish result=ok mode=try already_shutdown=true"
            );
            return Ok(());
        }

        let queued_cleanup = self.queue_shutdown_trx_cleanup(inner);
        let active_transactions = inner.session_registry.active_transaction_count();
        let strong_count = Arc::strong_count(inner);
        if strong_count != 1 || active_transactions != 0 || queued_cleanup != 0 {
            let busy = (strong_count - 1)
                .max(active_transactions)
                .max(queued_cleanup);
            obs::warn!(
                "event=engine_lifecycle component=engine action=shutdown_finish result=busy mode=try busy={} strong_refs={} active_transactions={} queued_cleanup={}",
                busy,
                strong_count - 1,
                active_transactions,
                queued_cleanup
            );
            return Err(Report::new(LifecycleError::ShutdownBusy)
                .attach(busy)
                .into());
        }
        self.finish_shutdown_locked(inner);
        obs::info!(
            "event=engine_lifecycle component=engine action=shutdown_finish result=ok mode=try"
        );
        Ok(())
    }

    /// Complete idempotent engine shutdown, waiting for active work to drain.
    ///
    /// Shutdown rejects new work immediately, waits for active operations,
    /// active transactions, abandoned transaction cleanup, and internal runtime
    /// pins to drain, removes idle registry-owned sessions, then dispatches
    /// component shutdown in reverse registration order.
    #[inline]
    pub fn shutdown(&self) -> Result<()> {
        let inner = self.inner();
        if inner.lifecycle.state() == EngineLifecycleState::Shutdown {
            return Ok(());
        }
        obs::info!(
            "event=engine_lifecycle component=engine action=shutdown_start result=ok mode=wait"
        );
        inner.lifecycle.close_admission();
        inner.lifecycle.wait_for_admissions_drained();

        loop {
            inner.lifecycle.wait_for_runtime_refs_drained();

            let _shutdown = inner.lifecycle.shutdown_lock.lock();
            if inner.lifecycle.state() == EngineLifecycleState::Shutdown {
                obs::info!(
                    "event=engine_lifecycle component=engine action=shutdown_finish result=ok mode=wait already_shutdown=true"
                );
                return Ok(());
            }

            let trx_change_epoch = inner.session_registry.trx_change_epoch();
            let queued_cleanup = self.queue_shutdown_trx_cleanup(inner);
            let active_transactions = inner.session_registry.active_transaction_count();
            let strong_count = Arc::strong_count(inner);
            if strong_count == 1 && active_transactions == 0 && queued_cleanup == 0 {
                self.finish_shutdown_locked(inner);
                obs::info!(
                    "event=engine_lifecycle component=engine action=shutdown_finish result=ok mode=wait"
                );
                return Ok(());
            }
            drop(_shutdown);

            let runtime_refs = inner.lifecycle.runtime_refs();
            if queued_cleanup != 0 && runtime_refs == 0 {
                inner
                    .session_registry
                    .wait_for_trx_change_since(trx_change_epoch);
                continue;
            }

            if active_transactions != 0 && queued_cleanup == 0 && runtime_refs == 0 {
                inner
                    .session_registry
                    .wait_for_trx_change_since(trx_change_epoch);
                continue;
            }

            // A weak public handle can briefly upgrade to an `Arc<EngineInner>`
            // while it discovers admission is already closed. That raw Arc is
            // intentionally not a runtime ref, so there is no event to wait on.
            if runtime_refs == 0 {
                yield_now();
            }
        }
    }

    #[inline]
    fn finish_shutdown_locked(&self, inner: &Arc<EngineInner>) {
        // Once no runtime pins remain, no active transaction or operation can
        // still be using idle session state. This releases registry-owned guards
        // before component shutdown.
        inner.session_registry.shutdown_idle();

        self.components().shutdown_all();
        inner.lifecycle.mark_shutdown();
    }

    #[inline]
    fn queue_shutdown_trx_cleanup(&self, inner: &Arc<EngineInner>) -> usize {
        let cleanup = inner.session_registry.collect_shutdown_cleanup();
        if cleanup.is_empty() {
            return 0;
        }
        let engine_ref = EngineRef::new(Arc::clone(inner));
        let len = cleanup.len();
        for (session_id, trx_id, reason) in cleanup {
            inner.trx_sys.request_abandoned_trx_cleanup(
                engine_ref.clone(),
                session_id,
                trx_id,
                reason,
            );
        }
        len
    }
}

impl Drop for Engine {
    #[inline]
    fn drop(&mut self) {
        if let Err(err) = self.try_shutdown() {
            obs::error!(
                "event=engine_lifecycle component=engine action=shutdown_finish result=error mode=drop error={}",
                err
            );
            if err.lifecycle_error() == Some(LifecycleError::ShutdownBusy) {
                // Fatal owner-drop violations still need to stop background
                // workers, but the owner registry cannot be dropped while
                // leaked runtime refs still retain component guards. This keeps
                // worker threads from escaping even after poison, while avoiding
                // quiescent owner teardown that could otherwise wait forever.
                let components = self
                    .components
                    .take()
                    .expect("engine component registry is present until drop");
                components.shutdown_all();
                forget(components);
            }
            panic!("fatal: engine shutdown failed: {err}");
        }

        // Field order releases the owner runtime ref before registry-owned
        // component owners.
    }
}

/// Crate-private cloneable shared runtime handle for the storage engine.
///
/// `EngineRef` intentionally does not own shutdown orchestration. It exposes
/// runtime state needed by sessions and internal subsystems, and reports its
/// clone/drop lifecycle so blocking shutdown can wait for runtime refs without
/// waking on every non-terminal drop.
pub(crate) struct EngineRef(Arc<EngineInner>);

impl EngineRef {
    #[inline]
    fn new(inner: Arc<EngineInner>) -> Self {
        inner.lifecycle.retain_runtime_ref();
        Self(inner)
    }

    /// Downgrade this private runtime pin into weak engine reachability.
    #[inline]
    pub(crate) fn downgrade(&self) -> WeakEngineRef {
        WeakEngineRef(Arc::downgrade(&self.0))
    }

    /// Create a new session while the engine is still running.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "transitional internal runtime handle")
    )]
    pub(crate) fn new_session(&self) -> Result<Session> {
        Ok(self.0.with_admitted_operation(|| {
            let id = self.0.next_session_id();
            self.0
                .session_registry
                .create_session(&self.0, self.clone(), id)
        })?)
    }

    /// Return the shared catalog handle.
    #[inline]
    pub(crate) fn catalog(&self) -> &Catalog {
        &self.0.catalog
    }

    /// Return the shared logical lock manager.
    #[inline]
    pub(crate) fn lock_manager(&self) -> &QuiescentGuard<LockManager> {
        self.0.lock_manager()
    }

    /// Returns the next engine-local session identity.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn next_session_id(&self) -> SessionID {
        self.0.next_session_id()
    }
}

impl Clone for EngineRef {
    #[inline]
    fn clone(&self) -> Self {
        let inner = Arc::clone(&self.0);
        inner.lifecycle.retain_runtime_ref();
        Self(inner)
    }
}

impl Drop for EngineRef {
    #[inline]
    fn drop(&mut self) {
        self.0.lifecycle.release_runtime_ref();
    }
}

impl Deref for EngineRef {
    type Target = EngineInner;

    #[inline]
    fn deref(&self) -> &EngineInner {
        &self.0
    }
}

/// Crate-private weak reachability handle used by public runtime handles.
#[derive(Clone)]
pub(crate) struct WeakEngineRef(Weak<EngineInner>);

impl WeakEngineRef {
    /// Create weak engine reachability from the engine runtime owner.
    #[inline]
    pub(crate) fn new(inner: &Arc<EngineInner>) -> Self {
        Self(Arc::downgrade(inner))
    }

    /// Upgrade weak engine reachability for one admitted public operation.
    #[inline]
    pub(crate) fn upgrade(&self) -> LifecycleResult<EngineRef> {
        self.0.upgrade().map(EngineRef::new).ok_or_else(|| {
            Report::new(LifecycleError::Shutdown).attach("engine is no longer reachable")
        })
    }

    /// Upgrade weak reachability for explicit terminal cleanup.
    ///
    /// This path does not acquire foreground admission: an already-active
    /// transaction must be able to commit or roll back while owner shutdown is
    /// waiting for active transactions to finish before component teardown.
    #[inline]
    pub(crate) fn upgrade_for_terminal(&self) -> LifecycleResult<EngineRef> {
        self.upgrade()
    }

    /// Best-effort upgrade for nonblocking cleanup hints from `Drop`.
    #[inline]
    pub(crate) fn upgrade_for_cleanup(&self) -> Option<EngineRef> {
        self.0.upgrade().map(EngineRef::new)
    }
}

/// Shared crate-private runtime state for an [`Engine`].
///
/// The fields here are the cloneable handles that sessions and other runtime
/// objects may retain. Owner-only teardown state lives on [`Engine`] itself.
pub(crate) struct EngineInner {
    /// Engine-level fatal runtime poison state.
    pub(crate) engine_poisoner: QuiescentGuard<EnginePoisoner>,
    /// Shared catalog handle.
    pub(crate) catalog: QuiescentGuard<Catalog>,
    /// Shared transaction-system handle.
    pub(crate) trx_sys: QuiescentGuard<TransactionSystem>,
    /// Metadata pool used for block-index and catalog tables.
    pub(crate) meta_pool: MetaPool,
    /// Secondary-index pool.
    pub(crate) index_pool: IndexPool,
    /// In-memory row-page pool for table data.
    pub(crate) mem_pool: MemPool,
    /// Table-file subsystem that runs persistent page IO.
    pub(crate) table_fs: QuiescentGuard<FileSystem>,
    /// Global readonly pool for persisted table-file reads.
    pub(crate) disk_pool: DiskPool,
    /// Shared logical metadata and table-data lock manager.
    lock_manager: QuiescentGuard<LockManager>,
    /// Engine-owned strong session-state registry.
    pub(crate) session_registry: SessionRegistry,
    /// Monotonically increasing engine-local session identity source.
    next_session_id: AtomicU64,
    lifecycle: EngineLifecycle,
}

impl EngineInner {
    /// Return the shared catalog handle.
    #[inline]
    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Clone the inner engine buffer-pool handles as one startup/recovery bundle.
    #[inline]
    pub(crate) fn pools(&self) -> EnginePools {
        EnginePools::new(
            self.meta_pool.clone_inner(),
            self.index_pool.clone_inner(),
            self.mem_pool.clone_inner(),
            self.disk_pool.clone_inner(),
        )
    }

    /// Return the shared logical lock manager.
    #[inline]
    pub(crate) fn lock_manager(&self) -> &QuiescentGuard<LockManager> {
        &self.lock_manager
    }

    /// Returns the next engine-local session identity.
    #[inline]
    pub(crate) fn next_session_id(&self) -> SessionID {
        SessionID::new(self.next_session_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Enter one short engine operation while runtime admission is open.
    ///
    /// The returned token is a scoped admission proof only. Drop it before any
    /// user callback, statement execution, blocking I/O, registry guard
    /// retention, or `.await` point.
    #[inline]
    pub(crate) fn acquire_admission(&self) -> LifecycleResult<EngineAdmission<'_>> {
        let admission = self
            .lifecycle
            .admit()
            .attach_with(|| "phase=acquire_engine_lifecycle_admission")?;
        self.engine_poisoner
            .ensure_healthy()
            .change_context(LifecycleError::RuntimeUnavailable)
            .attach_with(|| "phase=check_engine_health")?;
        Ok(admission)
    }

    /// Validate that engine admission is still open without checking storage
    /// poison.
    ///
    /// This is only for cheap lifecycle state queries that must remain
    /// observable after a fatal storage error. Real operations must use
    /// [`Self::acquire_admission`].
    #[inline]
    pub(crate) fn ensure_admission_open_for_query(&self) -> LifecycleResult<()> {
        if self.lifecycle.state() == EngineLifecycleState::Running {
            return Ok(());
        }
        Err(Report::new(LifecycleError::Shutdown).attach("engine admission is closed"))
    }

    /// Returns whether owner-side shutdown has started.
    #[inline]
    pub(crate) fn shutdown_started(&self) -> bool {
        self.lifecycle.state() != EngineLifecycleState::Running
    }

    /// Registers for owner-side shutdown start.
    #[inline]
    pub(crate) fn shutdown_listener(&self) -> EventListener {
        self.lifecycle.shutdown_listener()
    }

    /// Run immediate synchronous work under engine admission.
    ///
    /// Use this helper for lifecycle validation plus local runtime lookup or
    /// strong pinning. The closure must not perform user callbacks, statement
    /// execution, blocking I/O, or async waits.
    #[inline]
    pub(crate) fn with_admitted_operation<T>(&self, f: impl FnOnce() -> T) -> LifecycleResult<T> {
        let _admission = self.acquire_admission()?;
        Ok(f())
    }
}

impl EngineConfig {
    /// Build the storage engine and all registered components.
    #[inline]
    pub async fn build(self) -> Result<Engine> {
        obs::info!("event=engine_lifecycle component=engine action=build_start result=ok");
        self.build_inner()
            .await
            .inspect(|_| {
                obs::info!("event=engine_lifecycle component=engine action=build_finish result=ok");
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=engine_lifecycle component=engine action=build_finish result=error error={}",
                    err
                );
            })
    }

    #[inline]
    async fn build_inner(self) -> Result<Engine> {
        let resolved = self.resolve_storage_paths()?;
        resolved.validate_marker_if_present()?;
        // Startup prefers a small, durable-safety-focused preflight over trying
        // to exhaust every possible path conflict up front. It is acceptable for
        // later setup steps to fail, but those failures must not clobber durable
        // files or persist `storage-layout.toml` before the engine is fully built.
        resolved.ensure_directories()?;

        let file = self.file.data_dir(resolved.data_dir_path());
        let readonly_buffer_size = file.readonly_buffer_size;
        let trx_cfg = self.trx.log_dir(resolved.log_dir_path());
        let catalog_cfg = CatalogConfig::new(trx_cfg.recovery_disable_dml_validation);
        let mut builder = RegistryBuilder::new();
        // Components are registered in one fixed dependency order. Reverse
        // registration order then defines both explicit shutdown order and the
        // final owner drop order.
        builder.build::<EnginePoisoner>(()).await?;
        builder.build::<FileSystem>(file).await?;
        builder
            .build::<DiskPool>(DiskPoolConfig::new(readonly_buffer_size))
            .await?;
        builder
            .build::<MetaPool>(MetaPoolConfig::new(self.meta_buffer.as_u64() as usize))
            .await?;
        builder
            .build::<IndexPool>(IndexPoolConfig::new(
                self.index_buffer.as_u64() as usize,
                resolved.index_swap_file_path(),
                self.index_max_file_size.as_u64() as usize,
            ))
            .await?;
        builder
            .build::<MemPool>(
                self.data_buffer
                    .role(PoolRole::Mem)
                    .data_swap_file(resolved.data_swap_file_path()),
            )
            .await?;
        builder.build::<FileSystemWorkers>(()).await?;
        builder.build::<SharedPoolEvictorWorkers>(()).await?;
        builder.build::<LockManager>(()).await?;
        // Catalog owns user-table runtimes, and those runtimes retain buffer-pool
        // guards for row/index/readonly access. Register catalog after the pools it
        // can pin so reverse shutdown/drop order releases table guards before pool
        // owners are torn down.
        builder.build::<Catalog>(catalog_cfg).await?;
        builder.build::<TransactionSystem>(trx_cfg).await?;

        resolved.persist_marker_if_missing()?;
        let registry = builder.finish()?;
        let engine_poisoner = registry.dependency::<EnginePoisoner>()?;
        let catalog = registry.dependency::<Catalog>()?;
        let trx_sys = registry.dependency::<TransactionSystem>()?;
        let meta_pool = registry.dependency::<MetaPool>()?;
        let index_pool = registry.dependency::<IndexPool>()?;
        let mem_pool = registry.dependency::<MemPool>()?;
        let table_fs = registry.dependency::<FileSystem>()?;
        let disk_pool = registry.dependency::<DiskPool>()?;
        let lock_manager = registry.dependency::<LockManager>()?;
        let engine_inner = EngineInner {
            engine_poisoner,
            catalog,
            trx_sys,
            meta_pool,
            index_pool,
            mem_pool,
            table_fs,
            disk_pool,
            lock_manager,
            session_registry: SessionRegistry::new(),
            next_session_id: AtomicU64::new(FIRST_SESSION_ID.as_u64()),
            lifecycle: EngineLifecycle::new(),
        };
        Ok(Engine {
            inner: Arc::new(engine_inner),
            components: Some(registry),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::test_io_backend_stats_handle_identity as pool_stats_handle_identity;
    use crate::catalog::tests::table1;
    use crate::conf::consts::STORAGE_LAYOUT_FILE_NAME;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
    use crate::error::{
        CompletionErrorKind, ConfigError, Error, ErrorKind, FatalError, LifecycleError,
        OperationError, ResourceError, RuntimeError,
    };
    use crate::file::fs::tests::io_backend_stats_handle_identity as fs_stats_handle_identity;
    use crate::id::{TableID, TrxID};
    use crate::io::{
        IOKind, StdIoResult, StorageBackendFileIdentity, StorageBackendOp, StorageBackendTestHook,
        install_storage_backend_test_hook,
    };
    use crate::lock::tests::{debug_snapshot, try_acquire};
    use crate::lock::{LockMode, LockOwner, LockResource};
    use crate::session::tests::{SessionTestExt, session_registry_len};
    use crate::thread::{SpawnTestEvent, fail_spawn_named_with_observer, observe_spawn_named};
    use crate::trx::tests::add_pseudo_redo_log_entry;
    use smol::Timer;
    use std::fs;
    use std::io::Error as StdIoError;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::AtomicBool;
    use std::sync::{Barrier, mpsc};
    use std::thread::{self, sleep};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    const TEST_POOL_BYTES: usize = 64 * 1024 * 1024;

    fn test_engine_config_for(root: &Path) -> EngineConfig {
        EngineConfig::default()
            .storage_root(root)
            .meta_buffer(TEST_POOL_BYTES)
            .index_buffer(TEST_POOL_BYTES)
            .index_max_file_size(128usize * 1024 * 1024)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(TEST_POOL_BYTES)
                    .max_file_size(128usize * 1024 * 1024),
            )
            .file(FileSystemConfig::default().readonly_buffer_size(TEST_POOL_BYTES))
            .trx(TrxSysConfig::default())
    }

    struct FailInitialRedoHeaderWriteHook {
        redo_path: PathBuf,
        log_started: Arc<AtomicBool>,
        failed: AtomicBool,
    }

    impl FailInitialRedoHeaderWriteHook {
        #[inline]
        fn new(redo_path: PathBuf, log_started: Arc<AtomicBool>) -> Self {
            Self {
                redo_path,
                log_started,
                failed: AtomicBool::new(false),
            }
        }

        #[inline]
        fn failed(&self) -> bool {
            self.failed.load(Ordering::Acquire)
        }
    }

    impl StorageBackendTestHook for FailInitialRedoHeaderWriteHook {
        #[inline]
        fn on_complete(&self, op: StorageBackendOp, res: &mut StdIoResult<usize>) {
            if !self.log_started.load(Ordering::Acquire)
                || op.kind() != IOKind::Write
                || op.offset() != 0
            {
                return;
            }
            let Ok(identity) = StorageBackendFileIdentity::from_path(&self.redo_path) else {
                return;
            };
            if !op.matches_file_identity(identity)
                || self
                    .failed
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                return;
            }
            *res = Err(StdIoError::from_raw_os_error(libc::EIO));
        }
    }

    #[test]
    fn test_engine_worker_spawn_failures_are_runtime_and_failure_atomic() {
        for (worker, phase) in [
            (
                "IO-Thread",
                "component=fs_workers, phase=build_shared_storage_worker",
            ),
            (
                "Shared-Pool-Evictor",
                "component=shared_pool_evictor_workers, phase=build_shared_evictor",
            ),
            ("Log-Thread", "phase=start_transaction_log_worker"),
            ("Purge-Executor-1", "phase=start_transaction_purge_workers"),
            ("Purge-Dispatcher", "phase=start_transaction_purge_workers"),
            (
                "Trx-Cleanup-Thread",
                "phase=start_transaction_cleanup_worker",
            ),
        ] {
            let root = TempDir::new().unwrap();
            let (event_tx, event_rx) = mpsc::channel();
            let _failure = fail_spawn_named_with_observer(worker, move |event| {
                let _ = event_tx.send(event);
            });

            let err = match smol::block_on(test_engine_config_for(root.path()).build()) {
                Ok(_) => panic!("injected worker spawn must fail engine startup"),
                Err(err) => err,
            };

            assert_eq!(err.kind(), ErrorKind::Runtime, "worker={worker}, err={err}");
            assert_eq!(
                err.downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::BackgroundSpawn),
                "worker={worker}, err={err}"
            );
            let output = format!("{err:?}");
            assert!(output.contains(phase), "worker={worker}, report={output}");
            assert_eq!(
                output.matches(&format!("thread_name={worker}")).count(),
                1,
                "worker={worker}, report={output}"
            );

            let mut started = Vec::new();
            let mut finished = Vec::new();
            for event in event_rx.try_iter() {
                match event {
                    SpawnTestEvent::Started(name) => started.push(name),
                    SpawnTestEvent::Finished(name) => finished.push(name),
                }
            }
            started.sort_unstable();
            finished.sort_unstable();
            assert_eq!(
                started, finished,
                "startup returned before reclaiming all workers for failure at {worker}"
            );
            assert!(!started.iter().any(|name| name == worker));
        }
    }

    #[test]
    fn test_initial_redo_header_failure_reclaims_log_worker_before_startup_returns() {
        let root = TempDir::new().unwrap();
        let log_started = Arc::new(AtomicBool::new(false));
        let hook = Arc::new(FailInitialRedoHeaderWriteHook::new(
            root.path().join("redo.log.00000000"),
            Arc::clone(&log_started),
        ));
        let _io_failure = install_storage_backend_test_hook(hook.clone());
        let (event_tx, event_rx) = mpsc::channel();
        let _observer = observe_spawn_named(move |event| {
            if matches!(&event, SpawnTestEvent::Started(name) if name == "Log-Thread") {
                log_started.store(true, Ordering::Release);
            }
            let _ = event_tx.send(event);
        });

        let err = match smol::block_on(test_engine_config_for(root.path()).build()) {
            Ok(_) => panic!("initial redo-header write failure must fail engine startup"),
            Err(err) => err,
        };

        assert!(hook.failed(), "initial redo-header write was not injected");
        assert_eq!(err.kind(), ErrorKind::Fatal);
        assert_eq!(
            err.completion_error(),
            Some(CompletionErrorKind::Fatal(FatalError::RedoWrite))
        );
        let output = format!("{err:?}");
        assert!(
            output.contains("wait for initial redo super-block write"),
            "report={output}"
        );

        let mut started = Vec::new();
        let mut finished = Vec::new();
        for event in event_rx.try_iter() {
            match event {
                SpawnTestEvent::Started(name) => started.push(name),
                SpawnTestEvent::Finished(name) => finished.push(name),
            }
        }
        started.sort_unstable();
        finished.sort_unstable();
        assert_eq!(
            started, finished,
            "startup returned before reclaiming workers after initial redo-header failure"
        );
        for expected in ["IO-Thread", "Log-Thread", "Shared-Pool-Evictor"] {
            assert!(
                started.iter().any(|name| name == expected),
                "expected worker did not start: {expected}, started={started:?}"
            );
        }
        assert!(
            !started
                .iter()
                .any(|name| name.starts_with("Purge-") || name == "Trx-Cleanup-Thread"),
            "transaction workers started before initial redo header became durable: {started:?}"
        );
    }

    #[test]
    fn test_partial_purge_executor_spawn_failure_reclaims_started_executor() {
        let root = TempDir::new().unwrap();
        let (event_tx, event_rx) = mpsc::channel();
        let _failure = fail_spawn_named_with_observer("Purge-Executor-2", move |event| {
            let _ = event_tx.send(event);
        });
        let config =
            test_engine_config_for(root.path()).trx(TrxSysConfig::default().purge_threads(3));

        let err = match smol::block_on(config.build()) {
            Ok(_) => panic!("second purge-executor spawn failure must fail engine startup"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::BackgroundSpawn)
        );
        let output = format!("{err:?}");
        assert!(output.contains("phase=start_transaction_purge_workers"));
        assert_eq!(output.matches("thread_name=Purge-Executor-2").count(), 1);

        let mut started = Vec::new();
        let mut finished = Vec::new();
        for event in event_rx.try_iter() {
            match event {
                SpawnTestEvent::Started(name) => started.push(name),
                SpawnTestEvent::Finished(name) => finished.push(name),
            }
        }
        started.sort_unstable();
        finished.sort_unstable();
        assert_eq!(
            started, finished,
            "partial purge startup returned before reclaiming its first executor"
        );
        assert!(
            started.iter().any(|name| name == "Purge-Executor-1"),
            "first purge executor did not start: {started:?}"
        );
        assert!(
            !started.iter().any(|name| name == "Purge-Executor-2"),
            "injected second purge executor unexpectedly started: {started:?}"
        );
        assert!(
            !started.iter().any(|name| name == "Purge-Dispatcher"),
            "purge dispatcher started after executor startup failed: {started:?}"
        );
    }

    #[test]
    fn test_startup_rollback_join_panic_preserves_primary_runtime_report() {
        let root = TempDir::new().unwrap();
        let _failure = fail_spawn_named_with_observer("Purge-Dispatcher", |event| {
            if event == SpawnTestEvent::Finished("Purge-Executor-1".to_owned()) {
                panic!("injected purge executor join panic");
            }
        });

        let err = match smol::block_on(test_engine_config_for(root.path()).build()) {
            Ok(_) => panic!("injected purge dispatcher spawn must fail engine startup"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::BackgroundSpawn)
        );
        let output = format!("{err:?}");
        assert_eq!(output.matches("thread_name=Purge-Dispatcher").count(), 1);
        assert!(
            output.contains(
                "phase=rollback_purge_dispatcher_spawn, cleanup=join_partial_purge_workers, join_panics=1"
            ),
            "report={output}"
        );
    }

    fn wait_until_shutdown_begins(engine: &Engine) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while engine.inner().lifecycle.state() == EngineLifecycleState::Running {
            assert!(
                Instant::now() < deadline,
                "shutdown did not close admission before timeout"
            );
            yield_now();
        }
    }

    fn wait_until(mut done: impl FnMut() -> bool, message: &'static str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "{message}");
            sleep(Duration::from_millis(1));
        }
    }

    fn lock_entry_count(engine: &Engine, owner: LockOwner) -> usize {
        debug_snapshot(engine.lock_manager())
            .entries
            .iter()
            .filter(|entry| entry.owner == owner)
            .count()
    }

    #[inline]
    fn assert_runtime_unavailable_after_shutdown(err: Error) {
        assert_eq!(err.kind(), ErrorKind::Lifecycle);
        assert_eq!(err.lifecycle_error(), Some(LifecycleError::Shutdown));
    }

    #[test]
    fn test_engine_config() {
        let config = EngineConfig::default();
        let config_str = toml::to_string(&config).unwrap();
        assert!(config_str.contains("storage_root"));
        assert!(config_str.contains("index_swap_file"));
        assert!(config_str.contains("data_swap_file"));
        assert!(config_str.contains("log_write_io_depth"));
        assert!(config_str.contains("recovery_io_depth"));
        assert!(config_str.contains("recovery_disable_dml_validation"));
        assert!(config_str.contains("catalog_checkpoint_scan_io_depth"));
        assert!(config_str.contains("gc_buckets"));
        assert!(config_str.contains("log_dir"));
        assert!(config_str.contains("log_file_stem"));
        assert!(!config_str.contains("max_io_depth"));
    }

    #[test]
    fn test_catalog_checkpoint_scan_io_depth_comes_from_trx_config() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path())
                .trx(
                    TrxSysConfig::default()
                        .log_write_io_depth(2)
                        .recovery_io_depth(3)
                        .catalog_checkpoint_scan_io_depth(4),
                )
                .build()
                .await
                .unwrap();

            let scan_cfg = engine
                .inner()
                .trx_sys
                .catalog_checkpoint_scan_config()
                .unwrap();
            assert_eq!(scan_cfg.read_ahead_depth, 4);
        });
    }

    #[test]
    fn test_session_ids_are_monotonic_across_engine_handles() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let engine_ref = engine.new_ref().unwrap();

            let session1 = engine.new_session().unwrap();
            let session2 = engine_ref.new_session().unwrap();
            let session3 = engine.new_session().unwrap();

            assert_eq!(session1.id(), FIRST_SESSION_ID);
            assert_eq!(session2.id(), SessionID::new(session1.id().as_u64() + 1));
            assert_eq!(session3.id(), SessionID::new(session2.id().as_u64() + 1));
        });
    }

    #[test]
    fn test_engine_lock_manager_is_shared_across_runtime_handles() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let engine_ref = engine.new_ref().unwrap();
            let resource = LockResource::TableMetadata(TableID::new(10));
            let owner = LockOwner::Session(SessionID::new(10));

            assert!(
                try_acquire(engine.lock_manager(), resource, LockMode::Exclusive, owner).unwrap()
            );
            assert!(
                !try_acquire(
                    engine_ref.lock_manager(),
                    resource,
                    LockMode::Shared,
                    LockOwner::Session(SessionID::new(11))
                )
                .unwrap()
            );
            assert_eq!(engine_ref.lock_manager().release_owner(owner), 1);
            assert!(
                try_acquire(
                    engine.lock_manager(),
                    resource,
                    LockMode::Shared,
                    LockOwner::Session(SessionID::new(11))
                )
                .unwrap()
            );
        });
    }

    #[test]
    fn test_session_drop_releases_session_owned_locks() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let session = engine.new_session().unwrap();
            let resource = LockResource::TableData(TableID::new(91_200));

            assert!(
                try_acquire(
                    engine.lock_manager(),
                    resource,
                    LockMode::Exclusive,
                    LockOwner::Session(session.id())
                )
                .unwrap()
            );
            drop(session);

            assert!(
                try_acquire(
                    engine.lock_manager(),
                    resource,
                    LockMode::Shared,
                    LockOwner::Session(SessionID::new(91_201))
                )
                .unwrap()
            );
        });
    }

    #[test]
    fn test_session_drop_releases_session_owned_waiters() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let resource = LockResource::TableMetadata(TableID::new(91_202));
            let blocking_owner = LockOwner::Session(SessionID::new(91_203));
            assert!(
                try_acquire(
                    engine.lock_manager(),
                    resource,
                    LockMode::Exclusive,
                    blocking_owner
                )
                .unwrap()
            );

            let session = engine.new_session().unwrap();
            let waiting_owner = LockOwner::Session(session.id());
            let manager = engine.lock_manager().clone();
            let wait_task = smol::spawn(async move {
                manager
                    .acquire(resource, LockMode::Shared, waiting_owner)
                    .await
            });

            let mut waiter_seen = false;
            for _ in 0..100 {
                waiter_seen = debug_snapshot(engine.lock_manager())
                    .entries
                    .iter()
                    .any(|entry| entry.owner == waiting_owner);
                if waiter_seen {
                    break;
                }
                Timer::after(Duration::from_millis(1)).await;
            }
            assert!(waiter_seen);

            drop(session);
            let err = wait_task.await.unwrap_err();
            assert_eq!(*err.current_context(), OperationError::LockWaiterReleased);
            assert_eq!(engine.lock_manager().release_owner(blocking_owner), 1);
        });
    }

    #[test]
    fn test_engine_shared_storage_runtime_reuses_one_backend_stats_handle() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path())
                .file(
                    FileSystemConfig::default()
                        .io_depth(7)
                        .readonly_buffer_size(TEST_POOL_BYTES),
                )
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(TEST_POOL_BYTES)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .build()
                .await
                .unwrap();

            let table_stats = fs_stats_handle_identity(&engine.inner().table_fs);
            let mem_stats = pool_stats_handle_identity(&engine.inner().mem_pool);
            let index_stats = pool_stats_handle_identity(&engine.inner().index_pool);

            assert_eq!(table_stats, mem_stats);
            assert_eq!(table_stats, index_stats);
        });
    }

    #[test]
    fn test_engine_shared_storage_io_depth_comes_from_file_system_config() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path())
                .file(
                    FileSystemConfig::default()
                        .io_depth(7)
                        .readonly_buffer_size(TEST_POOL_BYTES),
                )
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(TEST_POOL_BYTES)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .build()
                .await
                .unwrap();

            assert_eq!(engine.inner().table_fs.configured_io_depth(), 7);
            assert_eq!(
                engine.inner().mem_pool.io_backend_stats(),
                engine.inner().table_fs.io_backend_stats()
            );
            assert_eq!(
                engine.inner().index_pool.io_backend_stats(),
                engine.inner().table_fs.io_backend_stats()
            );
        });
    }

    #[test]
    fn test_storage_layout_marker_allows_data_swap_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            let engine = test_engine_config_for(root.path())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024)
                        .data_swap_file("alt-data.swp"),
                )
                .build()
                .await
                .unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_storage_layout_marker_allows_index_swap_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            let engine = test_engine_config_for(root.path())
                .index_swap_file("alt-index.swp")
                .build()
                .await
                .unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_engine_startup_creates_default_swap_files() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            assert!(root.path().join("data.swp").exists());
            assert!(root.path().join("index.swp").exists());
        });
    }

    #[test]
    fn test_storage_layout_marker_rejects_data_dir_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            let err = match test_engine_config_for(root.path())
                .file(FileSystemConfig::default().data_dir("data"))
                .build()
                .await
            {
                Ok(_) => panic!("expected storage layout mismatch"),
                Err(err) => err,
            };
            assert!(err.is_kind(ErrorKind::Config));
            assert_eq!(
                err.report().downcast_ref::<ConfigError>().copied(),
                Some(ConfigError::StorageLayoutMismatch)
            );
        });
    }

    #[test]
    fn test_storage_layout_mismatch_does_not_create_new_directories() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            drop(engine);

            let new_data_dir = root.path().join("other-data");
            assert!(!new_data_dir.exists());

            let err = match test_engine_config_for(root.path())
                .file(FileSystemConfig::default().data_dir("other-data"))
                .build()
                .await
            {
                Ok(_) => panic!("expected storage layout mismatch"),
                Err(err) => err,
            };
            assert!(err.is_kind(ErrorKind::Config));
            assert_eq!(
                err.report().downcast_ref::<ConfigError>().copied(),
                Some(ConfigError::StorageLayoutMismatch)
            );
            assert!(!new_data_dir.exists());
        });
    }

    #[test]
    fn test_storage_layout_marker_allows_storage_root_relocation() {
        smol::block_on(async {
            let parent = TempDir::new().unwrap();
            let root_a = parent.path().join("root-a");
            let root_b = parent.path().join("root-b");

            let engine = test_engine_config_for(&root_a).build().await.unwrap();
            drop(engine);

            fs::rename(&root_a, &root_b).unwrap();

            let engine = test_engine_config_for(&root_b).build().await.unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_failed_startup_does_not_persist_storage_layout_marker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let marker_path = root.path().join(STORAGE_LAYOUT_FILE_NAME);

            let err = match EngineConfig::default()
                .storage_root(root.path())
                .meta_buffer(TEST_POOL_BYTES)
                .index_buffer(TEST_POOL_BYTES)
                .file(FileSystemConfig::default().readonly_buffer_size(TEST_POOL_BYTES))
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(1024usize * 1024)
                        .max_file_size(2usize * 1024 * 1024),
                )
                .trx(TrxSysConfig::default())
                .build()
                .await
            {
                Ok(_) => panic!("expected startup failure"),
                Err(err) => err,
            };
            assert_eq!(
                err.resource_error(),
                Some(ResourceError::BufferPoolSizeTooSmall)
            );
            assert!(!marker_path.exists());

            let engine = test_engine_config_for(root.path())
                .file(
                    FileSystemConfig::default()
                        .data_dir("data")
                        .readonly_buffer_size(TEST_POOL_BYTES),
                )
                .build()
                .await
                .unwrap();
            drop(engine);
            assert!(marker_path.exists());
        });
    }

    #[test]
    fn test_invalid_swap_path_startup_does_not_persist_storage_layout_marker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let marker_path = root.path().join(STORAGE_LAYOUT_FILE_NAME);

            let err = match test_engine_config_for(root.path())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(TEST_POOL_BYTES)
                        .max_file_size(128usize * 1024 * 1024)
                        .data_swap_file("catalog.mtb/data.swp"),
                )
                .build()
                .await
            {
                Ok(_) => panic!("expected startup failure"),
                Err(err) => err,
            };
            assert!(err.is_kind(ErrorKind::Config));
            assert_eq!(
                err.report().downcast_ref::<ConfigError>().copied(),
                Some(ConfigError::PathMustNotOverlapReservedLocation)
            );
            assert!(!marker_path.exists());
        });
    }

    #[test]
    fn test_engine_shutdown_is_idempotent_and_rejects_new_work() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();

            engine.shutdown().unwrap();
            engine.shutdown().unwrap();

            let err = match engine.new_session() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_runtime_unavailable_after_shutdown(err);

            let err = match engine.new_ref() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_runtime_unavailable_after_shutdown(err);
        });
    }

    #[test]
    fn test_engine_ref_rejected_once_shutdown_begins() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let engine_ref = engine.new_ref().unwrap();

            let err = match engine.try_shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::ShutdownBusy));
            assert_eq!(err.downcast_ref::<usize>().copied(), Some(1));

            let err = match engine_ref.new_session() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_runtime_unavailable_after_shutdown(err);

            let err = match engine.new_ref() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_runtime_unavailable_after_shutdown(err);

            drop(engine_ref);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_ignores_live_idle_session_handle() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            engine.shutdown().unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);

            let err = match engine.new_session() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_runtime_unavailable_after_shutdown(err);

            let err = match session.begin_trx() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_runtime_unavailable_after_shutdown(err);

            let err = match session
                .lock_table(TableID::new(91_300), LockMode::Shared)
                .await
            {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_runtime_unavailable_after_shutdown(err);

            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_busy_keeps_pinned_idle_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let session_handle = engine.new_session().unwrap();
            let session = session_handle.pin().unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            let err = match engine.try_shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::ShutdownBusy));
            assert_eq!(err.downcast_ref::<usize>().copied(), Some(1));
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            drop(session);
            engine.shutdown().unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);
        });
    }

    #[test]
    fn test_engine_shutdown_busy_until_active_transaction_finishes() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = match engine.try_shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::ShutdownBusy));
            assert_eq!(err.downcast_ref::<usize>().copied(), Some(1));

            trx.rollback().await.unwrap();
            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_rejects_non_terminal_transaction_work() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            let err = match engine.try_shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::ShutdownBusy));
            assert_eq!(err.downcast_ref::<usize>().copied(), Some(1));

            let err = trx
                .lock_table(table_id, LockMode::Shared)
                .await
                .unwrap_err();
            assert_runtime_unavailable_after_shutdown(err);

            trx.rollback().await.unwrap();
            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_transaction_handle_does_not_retain_runtime_ref() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();
            assert_eq!(engine.inner().lifecycle.runtime_refs(), 0);

            let trx = session.begin_trx().unwrap();
            assert_eq!(
                engine.inner().lifecycle.runtime_refs(),
                0,
                "idle public transaction handles must not retain EngineRef"
            );

            trx.rollback().await.unwrap();
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_waits_for_engine_ref_to_drop() {
        let root = TempDir::new().unwrap();
        let engine = smol::block_on(test_engine_config_for(root.path()).build()).unwrap();
        let engine_ref = engine.new_ref().unwrap();
        let (done_tx, done_rx) = mpsc::channel();

        thread::scope(|scope| {
            let shutdown_engine = &engine;
            let shutdown_handle = scope.spawn(move || {
                let result = shutdown_engine
                    .shutdown()
                    .map_err(|err| err.lifecycle_error());
                done_tx.send(result).unwrap();
            });

            wait_until_shutdown_begins(&engine);
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "shutdown must wait while an EngineRef is alive"
            );

            drop(engine_ref);
            let result = done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown should complete after EngineRef drops");
            assert_eq!(result, Ok(()));
            shutdown_handle.join().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_waits_for_pinned_idle_session() {
        let root = TempDir::new().unwrap();
        let engine = smol::block_on(test_engine_config_for(root.path()).build()).unwrap();
        let session_handle = engine.new_session().unwrap();
        let session = session_handle.pin().unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

        thread::scope(|scope| {
            let shutdown_engine = &engine;
            let shutdown_handle = scope.spawn(move || {
                let result = shutdown_engine
                    .shutdown()
                    .map_err(|err| err.lifecycle_error());
                done_tx.send(result).unwrap();
            });

            wait_until_shutdown_begins(&engine);
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "shutdown must wait while a SessionPin is alive"
            );
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            drop(session);
            let result = done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown should complete after SessionPin drops");
            assert_eq!(result, Ok(()));
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);
            shutdown_handle.join().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_wakes_maintenance_progress_wait() {
        let root = TempDir::new().unwrap();
        let engine = smol::block_on(test_engine_config_for(root.path()).build()).unwrap();
        let session = engine.new_session().unwrap();
        let (started_tx, started_rx) = mpsc::channel();

        thread::scope(|scope| {
            let waiter = scope.spawn(move || {
                started_tx.send(()).unwrap();
                let err = smol::block_on(session.wait_for_gc_horizon_after(TrxID::new(u64::MAX)))
                    .unwrap_err();
                assert_eq!(err.lifecycle_error(), Some(LifecycleError::Shutdown));
            });
            started_rx.recv().unwrap();
            engine.shutdown().unwrap();
            waiter.join().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_waits_for_active_transaction_to_finish() {
        let root = TempDir::new().unwrap();
        let engine = smol::block_on(test_engine_config_for(root.path()).build()).unwrap();
        let mut session = engine.new_session().unwrap();
        let trx = session.begin_trx().unwrap();
        let (done_tx, done_rx) = mpsc::channel();

        thread::scope(|scope| {
            let shutdown_engine = &engine;
            let shutdown_handle = scope.spawn(move || {
                let result = shutdown_engine
                    .shutdown()
                    .map_err(|err| err.lifecycle_error());
                done_tx.send(result).unwrap();
            });

            wait_until_shutdown_begins(&engine);
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "shutdown must wait while an active transaction is alive"
            );

            smol::block_on(trx.rollback()).unwrap();
            drop(session);
            let result = done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown should complete after transaction rollback");
            assert_eq!(result, Ok(()));
            shutdown_handle.join().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_waits_for_checked_out_abandoned_transaction_to_return() {
        let root = TempDir::new().unwrap();
        let engine = smol::block_on(test_engine_config_for(root.path()).build()).unwrap();
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        let checkout = trx.checkout().unwrap();
        let (done_tx, done_rx) = mpsc::channel();

        drop(trx);
        assert!(session.in_trx().unwrap());

        thread::scope(|scope| {
            let shutdown_engine = &engine;
            let shutdown_handle = scope.spawn(move || {
                let result = shutdown_engine
                    .shutdown()
                    .map_err(|err| err.lifecycle_error());
                done_tx.send(result).unwrap();
            });

            wait_until_shutdown_begins(&engine);
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "shutdown must wait until checked-out abandoned transaction returns"
            );

            drop(checkout);
            let result = done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown should complete after checkout returns");
            assert_eq!(result, Ok(()));
            shutdown_handle.join().unwrap();
        });
    }

    #[test]
    fn test_session_close_rejects_active_transaction_then_retries_after_rollback() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session.close().await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(
                err.lifecycle_error(),
                Some(LifecycleError::ExistingTransaction)
            );
            assert!(session.in_trx().unwrap());

            trx.rollback().await.unwrap();
            assert!(!session.in_trx().unwrap());
            session.close().await.unwrap();
            session.close().await.unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_session_in_trx_returns_error_after_session_close() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();

            assert!(!session.in_trx().unwrap());
            session.close().await.unwrap();

            let err = session.in_trx().unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(
                err.lifecycle_error(),
                Some(LifecycleError::SessionUnavailable)
            );
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_session_in_trx_returns_error_after_engine_shutdown() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let session = engine.new_session().unwrap();

            assert!(!session.in_trx().unwrap());
            engine.shutdown().unwrap();

            let err = session.in_trx().unwrap_err();
            assert_runtime_unavailable_after_shutdown(err);
        });
    }

    #[test]
    fn test_dropped_active_session_is_removed_after_transaction_terminal() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            drop(session);
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);
            trx.rollback().await.unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_dropped_transaction_handle_cleanup_releases_locks_and_reuses_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let owner = LockOwner::Transaction(trx.trx_id());

            trx.lock_table(table_id, LockMode::Shared).await.unwrap();
            assert!(lock_entry_count(&engine, owner) > 0);

            drop(trx);
            wait_until(
                || session.in_trx().is_ok_and(|in_trx| !in_trx),
                "abandoned transaction cleanup did not return the session to idle",
            );
            assert_eq!(lock_entry_count(&engine, owner), 0);

            let replacement = session.begin_trx().unwrap();
            replacement.rollback().await.unwrap();
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_checked_out_abandoned_cleanup_runs_after_checkout_return() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let owner = LockOwner::Transaction(trx.trx_id());

            trx.lock_table(table_id, LockMode::Shared).await.unwrap();
            assert!(lock_entry_count(&engine, owner) > 0);

            let checkout = trx.checkout().unwrap();
            drop(trx);
            assert!(session.in_trx().unwrap());
            assert!(lock_entry_count(&engine, owner) > 0);

            drop(checkout);
            wait_until(
                || session.in_trx().is_ok_and(|in_trx| !in_trx),
                "checkout return did not schedule abandoned transaction cleanup",
            );
            assert_eq!(lock_entry_count(&engine, owner), 0);

            let replacement = session.begin_trx().unwrap();
            replacement.rollback().await.unwrap();
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_dropped_session_live_transaction_can_commit() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            add_pseudo_redo_log_entry(&mut trx).await;
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            drop(session);
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);
            assert!(trx.commit().await.unwrap() > TrxID::new(0));
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);

            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_dropping_session_then_transaction_removes_abandoned_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            drop(session);
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);
            drop(trx);
            wait_until(
                || session_registry_len(&engine.inner().session_registry) == 0,
                "abandoned session was not removed after transaction cleanup",
            );

            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_admitted_operation_token_releases_before_shutdown_completes() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let admission = engine.inner().acquire_admission().unwrap();
            let (started_tx, started_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();

            thread::scope(|scope| {
                let shutdown_handle = scope.spawn(|| {
                    started_tx.send(()).unwrap();
                    engine.shutdown().unwrap();
                    done_tx.send(()).unwrap();
                });

                started_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("shutdown thread should start");
                assert!(
                    done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                    "shutdown must wait while an admitted operation is active"
                );

                drop(admission);
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("shutdown should complete after admission is released");
                shutdown_handle.join().unwrap();
            });
        });
    }

    #[test]
    fn test_same_session_rejects_overlapping_transactions() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();

            let trx = session.begin_trx().unwrap();
            assert!(session.in_trx().unwrap());
            let err = match session.begin_trx() {
                Ok(_) => panic!("expected existing transaction error"),
                Err(err) => err,
            };
            let kind = err.kind();
            let lifecycle_error = err.lifecycle_error();

            trx.rollback().await.unwrap();
            assert!(!session.in_trx().unwrap());
            assert_eq!(kind, ErrorKind::Lifecycle);
            assert_eq!(lifecycle_error, Some(LifecycleError::ExistingTransaction));
        });
    }

    #[test]
    fn test_same_session_reuse_after_commit() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            add_pseudo_redo_log_entry(&mut trx).await;
            let cts = trx.commit().await.unwrap();
            assert!(cts > TrxID::new(0));
            assert!(!session.in_trx().unwrap());

            let trx = session.begin_trx().unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_same_session_reuse_after_rollback() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();

            let trx = session.begin_trx().unwrap();
            trx.rollback().await.unwrap();
            assert!(!session.in_trx().unwrap());

            let trx = session.begin_trx().unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_stale_transaction_commit_does_not_update_session_state() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();

            let trx = session.begin_trx().unwrap();
            let stale_trx_id = trx.trx_id();
            trx.rollback().await.unwrap();
            assert_eq!(session.last_cts(), TrxID::new(0));

            let replacement = session.begin_trx().unwrap();
            engine.inner().session_registry.finish_trx_commit(
                session.id(),
                stale_trx_id,
                TrxID::new(91_241),
            );

            assert!(session.in_trx().unwrap());
            assert_eq!(session.last_cts(), TrxID::new(0));

            replacement.rollback().await.unwrap();
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_same_session_reuse_after_readonly_commit() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();

            let trx = session.begin_trx().unwrap();
            let cts = trx.commit().await.unwrap();
            assert_eq!(cts, TrxID::new(0));
            assert!(!session.in_trx().unwrap());

            let trx = session.begin_trx().unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_distinct_sessions_can_hold_overlapping_transactions() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session1 = engine.new_session().unwrap();
            let mut session2 = engine.new_session().unwrap();

            let trx1 = session1.begin_trx().unwrap();
            let trx2 = session2.begin_trx().unwrap();

            assert!(session1.in_trx().unwrap());
            assert!(session2.in_trx().unwrap());

            trx1.rollback().await.unwrap();
            trx2.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_drop_engine_without_explicit_shutdown_succeeds_without_extra_refs() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();

            let res = catch_unwind(AssertUnwindSafe(|| drop(engine)));
            assert!(res.is_ok());
        });
    }

    #[test]
    fn test_drop_engine_panics_when_extra_refs_exist() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let leaked_ref = engine.new_ref().unwrap();

            let res = catch_unwind(AssertUnwindSafe(|| drop(engine)));
            assert!(res.is_err());

            drop(leaked_ref);
        });
    }

    #[test]
    fn test_shutdown_race_with_owner_ref_creation() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let barrier = Arc::new(Barrier::new(3));
            let engine = &engine;

            thread::scope(|scope| {
                let shutdown_barrier = Arc::clone(&barrier);
                let shutdown_handle = scope.spawn(move || {
                    shutdown_barrier.wait();
                    engine.try_shutdown()
                });

                let ref_barrier = Arc::clone(&barrier);
                let ref_handle = scope.spawn(move || {
                    ref_barrier.wait();
                    engine.new_ref()
                });

                barrier.wait();

                let shutdown_res = shutdown_handle.join().unwrap();
                let new_ref_res = ref_handle.join().unwrap();

                match (shutdown_res, new_ref_res) {
                    (Ok(()), Err(err))
                        if err.lifecycle_error() == Some(LifecycleError::Shutdown) => {}
                    (Err(err), Ok(engine_ref))
                        if err.lifecycle_error() == Some(LifecycleError::ShutdownBusy) =>
                    {
                        drop(engine_ref);
                        engine.shutdown().unwrap();
                    }
                    (Ok(()), Ok(engine_ref)) => {
                        drop(engine_ref);
                        panic!(
                            "shutdown succeeded but owner-side EngineRef creation also succeeded"
                        );
                    }
                    (Err(err), Ok(engine_ref)) => {
                        drop(engine_ref);
                        panic!("unexpected shutdown result during race: {err:?}");
                    }
                    (Ok(()), Err(err)) => {
                        panic!("unexpected new_ref error after successful shutdown: {err:?}");
                    }
                    (Err(shutdown_err), Err(new_ref_err)) => {
                        panic!(
                            "unexpected shutdown/new_ref race outcome: shutdown={shutdown_err:?}, new_ref={new_ref_err:?}"
                        );
                    }
                }
            });
        });
    }

    #[test]
    fn test_unstarted_transaction_system_shutdown_is_safe() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let log_dir = temp_dir.path().join("log");
            fs::create_dir_all(&log_dir).unwrap();
            let engine = test_engine_config_for(temp_dir.path())
                .file(
                    FileSystemConfig::default()
                        .data_dir("data")
                        .readonly_buffer_size(TEST_POOL_BYTES),
                )
                .trx(TrxSysConfig::default())
                .build()
                .await
                .unwrap();

            let mut config = TrxSysConfig::default()
                .log_dir(&log_dir)
                .log_file_stem("pending-startup-cleanup");
            config.validate().unwrap();
            let (trx_sys, startup) = TransactionSystem::bootstrap(
                config,
                engine.inner().engine_poisoner.clone(),
                engine.inner().pools(),
                engine.inner().table_fs.clone(),
                engine.inner().catalog.clone(),
            )
            .await
            .unwrap();
            drop(startup);
            drop(trx_sys);
        });
    }
}
