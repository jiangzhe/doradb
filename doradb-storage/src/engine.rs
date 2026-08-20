//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands. See
//! `docs/engine-component-lifetime.md` for the runtime-versus-owner lifetime
//! model that this module enforces with the component registry.
use crate::buffer::SharedPoolEvictorWorkers;
#[cfg(test)]
use crate::catalog::index::tests::IndexDdlTestController;
#[cfg(test)]
use crate::catalog::table::tests::TableDdlTestController;
use crate::catalog::{Catalog, CatalogConfig};
use crate::component::{
    ComponentRegistry, ComponentShutdownOutcome, DiskPoolConfig, EnginePools, MetaPoolConfig,
    RegistryBuilder,
};
use crate::conf::{EngineConfig, ValidatedTrxSysConfig};
use crate::error::{
    ConfigError, DiscloseError, DiscloseResultExt, LifecycleError, LifecycleOrFatalError,
    LifecycleOrFatalResult, LifecycleResult, Result,
};
use crate::file::fs::{FileSystem, FileSystemWorkers};
use crate::id::SessionID;
use crate::lock::LockManager;
use crate::obs;
use crate::poison::EnginePoisoner;
use crate::quiescent::QuiescentGuard;
use crate::root::{StorageRootLease, StorageRootLeaseAttempt};
use crate::runtime::block_on;
use crate::runtime::mandatory::{MandatoryRuntime, MandatoryRuntimeWorkers};
use crate::session::{Session, SessionAdmission, SessionCleanupRequest, SessionRegistry};
#[cfg(test)]
use crate::table::tests::MaintenanceTestController;
use crate::trx::SessionOperationState;
use crate::trx::sys::{TransactionPurgeWorkers, TransactionRedoWorkers, TransactionSystem};
use crate::{DiskPool, IndexPool, MemPool, MetaPool};
use error_stack::{Report, ResultExt};
use event_listener::{Event, EventListener, Listener, listener};
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::ops::Deref;
use std::result;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

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

#[derive(Clone, Copy)]
enum ShutdownOrigin {
    Explicit,
    OwnerDrop,
}

impl ShutdownOrigin {
    #[inline]
    const fn label(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::OwnerDrop => "owner_drop",
        }
    }
}

/// Packed engine-wide operation admission and shutdown coordination.
pub(crate) struct EngineLifecycle {
    /// Packed lifecycle state and active admission count.
    ///
    /// Bits `[0, LIFECYCLE_STATE_BITS)` store [`EngineLifecycleState`]. The
    /// upper bits store the number of live [`EngineAdmission`] tokens. Keeping
    /// both in one atomic prevents the shutdown close-admission transition from
    /// racing independently with new admission increments.
    state: AtomicUsize,
    admission_released: Event,
    shutdown_started: Event,
    shutdown_lock: Mutex<()>,
}

impl EngineLifecycle {
    #[inline]
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(EngineLifecycleState::Running.as_usize()),
            admission_released: Event::new(),
            shutdown_started: Event::new(),
            shutdown_lock: Mutex::new(()),
        }
    }

    #[inline]
    const fn active_admissions_from_word(word: usize) -> usize {
        word >> LIFECYCLE_STATE_BITS
    }

    #[inline]
    fn inspect_state(&self) -> EngineLifecycleState {
        let state = self.state.load(Ordering::Acquire) & LIFECYCLE_STATE_MASK;
        EngineLifecycleState::try_from(state)
            .unwrap_or_else(|state| panic!("invalid engine lifecycle state: {state}"))
    }

    /// Acquire one operation-start admission while the engine is running.
    #[inline]
    pub(crate) fn admit(&self) -> LifecycleResult<EngineAdmission<'_>> {
        loop {
            let word = self.state.load(Ordering::Acquire);
            let state = EngineLifecycleState::try_from(word & LIFECYCLE_STATE_MASK)
                .unwrap_or_else(|state| panic!("invalid engine lifecycle state: {state}"));
            if state != EngineLifecycleState::Running {
                return Err(Report::new(LifecycleError::Shutdown).attach(format!(
                    "engine lifecycle admission is closed: state={state:?}"
                )));
            }
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
    pub(crate) fn shutdown_listener(&self) -> EventListener {
        self.shutdown_started.listen()
    }

    /// Returns whether owner-side shutdown has started.
    #[inline]
    pub(crate) fn shutdown_started(&self) -> bool {
        self.inspect_state() != EngineLifecycleState::Running
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
    // Field order is part of owner teardown: shared runtime reachability is
    // released before component owners are dropped.
    inner: Arc<EngineInner>,
    components: Option<ComponentRegistry>,
}

impl Engine {
    /// Bootstrap the storage engine and all registered components.
    #[inline]
    pub async fn bootstrap(config: EngineConfig) -> Result<Self> {
        obs::info!("event=engine_lifecycle component=engine action=build_start result=ok");
        let result = async {
            let config = config.validate_inner().disclose()?;
            let resolved = config
                .resolve_storage_paths()
                .disclose()?
                .prepare_storage_root()
                .disclose()?;
            let lock_path = resolved.lock_path();
            let lease = match StorageRootLease::try_acquire(&resolved).disclose()? {
                StorageRootLeaseAttempt::Acquired(lease) => lease,
                StorageRootLeaseAttempt::Contended {
                    diagnostic,
                    diagnostic_status,
                } => {
                    let report = Report::new(LifecycleError::StorageRootInUse).attach(format!(
                        "operation=acquire_storage_root, storage_root={}, lock_path={}, owner_diagnostic={diagnostic_status}",
                        resolved.storage_root_path().display(),
                        lock_path.display()
                    ));
                    let report = if let Some(diagnostic) = diagnostic {
                        report.attach(format!(
                            "owner_pid={}, owner_acquired_unix_ms={}",
                            diagnostic.pid, diagnostic.acquired_unix_ms
                        ))
                    } else {
                        report
                    };
                    return Err(report.disclose());
                }
            };
            let mut builder = RegistryBuilder::new();
            // Root ownership is registered first so every failure and reverse
            // shutdown path releases it only after all subordinate components stop.
            builder
                .build::<StorageRootLease>(lease)
                .await
                .unwrap_or_else(|never| match never {});
            resolved.cleanup_stale_marker_temps().disclose()?;
            let marker_was_present = resolved.validate_marker_if_present().disclose()?;
            // Startup prefers a small, durable-safety-focused preflight over trying
            // to exhaust every possible path conflict up front. It is acceptable for
            // later setup steps to fail, but those failures must not clobber durable
            // files or persist `storage-layout.toml` before the engine is fully built.
            resolved.ensure_directories().disclose()?;

            let file = config.file.data_dir(resolved.data_dir_path());
            let readonly_buffer_size = file.readonly_buffer_size;
            let file = file.validate().disclose()?;
            let trx_cfg = config.trx.log_dir(resolved.log_dir_path());
            let catalog_cfg = CatalogConfig::new(trx_cfg.recovery_disable_dml_validation);
            let trx_cfg = ValidatedTrxSysConfig::try_new(trx_cfg).disclose()?;
            // Components are registered in one fixed dependency order. Reverse
            // registration order then defines both explicit shutdown order and the
            // final owner drop order.
            builder
                .build::<EnginePoisoner>(())
                .await
                .unwrap_or_else(|never| match never {});
            builder
                .build::<MandatoryRuntime>(config.mandatory_runtime.clone())
                .await
                .disclose()?;
            builder.build::<FileSystem>(file).await.disclose()?;
            builder
                .build::<DiskPool>(DiskPoolConfig::new(readonly_buffer_size))
                .await
                .disclose()?;
            builder
                .build::<MetaPool>(MetaPoolConfig::new(config.meta_buffer.as_u64() as usize))
                .await
                .disclose()?;
            builder
                .build::<IndexPool>(
                    config
                        .index_buffer
                        .swap_file(resolved.index_swap_file_path()),
                )
                .await
                .disclose()?;
            builder
                .build::<MemPool>(
                    config
                        .data_buffer
                        .swap_file(resolved.data_swap_file_path()),
                )
                .await
                .disclose()?;
            builder.build::<FileSystemWorkers>(()).await.disclose()?;
            builder
                .build::<SharedPoolEvictorWorkers>(())
                .await
                .disclose()?;
            builder
                .build::<LockManager>(())
                .await
                .unwrap_or_else(|never| match never {});
            // Catalog owns user-table runtimes, and those runtimes retain buffer-pool
            // guards for row/index/readonly access. Register catalog after the pools it
            // can pin so reverse shutdown/drop order releases table guards before pool
            // owners are torn down.
            builder.build::<Catalog>(catalog_cfg).await.disclose()?;
            builder
                .build::<TransactionSystem>(trx_cfg)
                .await
                .disclose()?;
            builder
                .build::<TransactionPurgeWorkers>(())
                .await
                .disclose()?;
            builder
                .build::<MandatoryRuntimeWorkers>(())
                .await
                .disclose()?;
            builder
                .build::<TransactionRedoWorkers>(())
                .await
                .disclose()?;

            if marker_was_present {
                if !resolved.validate_marker_if_present().disclose()? {
                    return Err(Report::new(ConfigError::StorageLayoutMismatch)
                        .attach(format!(
                            "operation=revalidate_storage_layout_marker, phase=post_component_build, marker_path={}, reason=initially_present_marker_disappeared",
                            resolved.marker_path().display()
                        ))
                        .disclose());
                }
            } else {
                resolved.persist_marker().disclose()?;
            }
            let registry = builder.finish();
            let poisoner = registry.dependency::<EnginePoisoner>();
            let mandatory_runtime = registry.dependency::<MandatoryRuntime>();
            let catalog = registry.dependency::<Catalog>();
            let trx_sys = registry.dependency::<TransactionSystem>();
            let meta_pool = registry.dependency::<MetaPool>();
            let index_pool = registry.dependency::<IndexPool>();
            let mem_pool = registry.dependency::<MemPool>();
            let table_fs = registry.dependency::<FileSystem>();
            let disk_pool = registry.dependency::<DiskPool>();
            let lock_manager = registry.dependency::<LockManager>();
            let session_registry = Arc::new(SessionRegistry::new());
            let lifecycle = Arc::new(EngineLifecycle::new());
            let core = Arc::new(EngineCore {
                poisoner,
                mandatory_runtime,
                catalog,
                trx_sys,
                pools: EnginePools::new(
                    meta_pool.clone_inner(),
                    index_pool.clone_inner(),
                    mem_pool.clone_inner(),
                    disk_pool.clone_inner(),
                ),
                table_fs,
                lock_manager,
                session_registry: Arc::downgrade(&session_registry),
                #[cfg(test)]
                table_ddl_test: TableDdlTestController::default(),
                #[cfg(test)]
                index_ddl_test: IndexDdlTestController::default(),
                #[cfg(test)]
                maintenance_test: MaintenanceTestController::default(),
            });
            let engine_inner = EngineInner {
                core,
                session_registry,
                lifecycle,
                next_session_id: AtomicU64::new(FIRST_SESSION_ID.as_u64()),
            };
            Ok(Engine {
                inner: Arc::new(engine_inner),
                components: Some(registry),
            })
        }
        .await;
        result
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
        self.new_session_inner().disclose()
    }

    #[inline]
    fn new_session_inner(&self) -> LifecycleOrFatalResult<Session> {
        let inner = self.inner();
        inner.with_admitted_operation(|| {
            let id = inner.next_session_id();
            let admission = Arc::new(SessionAdmission::new(Arc::clone(&inner.lifecycle)));
            inner
                .session_registry
                .create_session(Arc::clone(&inner.core), admission, id)
        })
    }

    /// Try to complete idempotent engine shutdown without waiting for active work.
    ///
    /// `try_shutdown` rejects new work immediately, drains in-flight admission,
    /// and returns an error with shutdown-busy context if active operations,
    /// active transactions, abandoned transaction cleanup, or internal runtime
    /// permits are still alive.
    ///
    /// This path is valid after storage poison. Poison only blocks admission; it
    /// does not replace the owner-side responsibility to stop background workers
    /// and drop components in the registered order.
    #[inline]
    pub fn try_shutdown(&self) -> Result<()> {
        self.try_shutdown_inner().disclose()
    }

    #[inline]
    fn try_shutdown_inner(&self) -> LifecycleResult<()> {
        let inner = self.inner();
        if inner.lifecycle.inspect_state() == EngineLifecycleState::Shutdown {
            return Ok(());
        }
        obs::info!(
            "event=engine_lifecycle component=engine action=shutdown_start result=ok mode=try origin=explicit"
        );
        inner.lifecycle.close_admission();
        inner.mandatory_runtime.close_admission();
        inner.lifecycle.wait_for_admissions_drained();

        let _shutdown = inner.lifecycle.shutdown_lock.lock();
        if inner.lifecycle.inspect_state() == EngineLifecycleState::Shutdown {
            obs::info!(
                "event=engine_lifecycle component=engine action=shutdown_finish result=ok mode=try origin=explicit already_shutdown=true"
            );
            return Ok(());
        }

        let blocker = inner.session_registry.first_shutdown_blocker();
        let session_blocker = blocker.as_ref().map_or("none", |blocker| blocker.label());
        let operation_state = blocker
            .as_ref()
            .and_then(|blocker| blocker.operation_state())
            .map_or("none", SessionOperationState::label);
        let observer_count = blocker
            .as_ref()
            .map_or(0, |blocker| blocker.observer_count());
        let has_session_blocker = blocker.is_some();
        let cleanup_queued = self
            .queue_shutdown_operation_cleanup(blocker.and_then(|blocker| blocker.into_cleanup()));
        let (mandatory_callers, mandatory_internal) = inner.mandatory_runtime.blocker_counts();
        if has_session_blocker || mandatory_callers != 0 || mandatory_internal != 0 {
            obs::warn!(
                "event=engine_lifecycle component=engine action=shutdown_finish result=busy mode=try origin=explicit session_blocker={} operation_state={} observer_count={} cleanup_queued={} mandatory_callers={} mandatory_internal={}",
                session_blocker,
                operation_state,
                observer_count,
                cleanup_queued,
                mandatory_callers,
                mandatory_internal
            );
            return Err(Report::new(LifecycleError::ShutdownBusy).attach(format!(
                "origin=explicit, session_blocker={session_blocker}, operation_state={operation_state}, observer_count={observer_count}, cleanup_queued={cleanup_queued}, mandatory_callers={mandatory_callers}, mandatory_internal={mandatory_internal}"
            )));
        }
        let outcome = self.finish_shutdown_locked(inner);
        drop(_shutdown);
        if outcome.is_degraded() {
            obs::error!(
                "event=engine_lifecycle component=engine action=shutdown_finish result=panic mode=try origin=explicit"
            );
        } else {
            obs::info!(
                "event=engine_lifecycle component=engine action=shutdown_finish result=ok mode=try origin=explicit"
            );
        }
        outcome.propagate_or_suppress("engine_try_shutdown");
        Ok(())
    }

    /// Complete idempotent engine shutdown, waiting for active work to drain.
    ///
    /// Shutdown rejects new work immediately, waits for active operations,
    /// active transactions, abandoned transaction cleanup, and internal runtime
    /// permits to drain, removes idle registry-owned sessions, then dispatches
    /// component shutdown in reverse registration order.
    #[inline]
    pub fn shutdown(&self) {
        self.shutdown_inner(ShutdownOrigin::Explicit);
    }

    #[inline]
    fn shutdown_inner(&self, origin: ShutdownOrigin) {
        let inner = self.inner();
        if inner.lifecycle.inspect_state() == EngineLifecycleState::Shutdown {
            return;
        }
        obs::info!(
            "event=engine_lifecycle component=engine action=shutdown_start result=ok mode=wait origin={}",
            origin.label(),
        );
        inner.lifecycle.close_admission();
        inner.mandatory_runtime.close_admission();
        inner.lifecycle.wait_for_admissions_drained();
        block_on(inner.mandatory_runtime.drain_callers());

        loop {
            let _shutdown = inner.lifecycle.shutdown_lock.lock();
            if inner.lifecycle.inspect_state() == EngineLifecycleState::Shutdown {
                obs::info!(
                    "event=engine_lifecycle component=engine action=shutdown_finish result=ok mode=wait origin={} already_shutdown=true",
                    origin.label(),
                );
                return;
            }

            let shutdown_wait = inner.session_registry.first_shutdown_wait();
            if shutdown_wait.is_none() {
                let outcome = self.finish_shutdown_locked(inner);
                drop(_shutdown);
                if outcome.is_degraded() {
                    obs::error!(
                        "event=engine_lifecycle component=engine action=shutdown_finish result=panic mode=wait origin={}",
                        origin.label(),
                    );
                } else {
                    obs::info!(
                        "event=engine_lifecycle component=engine action=shutdown_finish result=ok mode=wait origin={}",
                        origin.label(),
                    );
                }
                outcome.propagate_or_suppress("engine_shutdown");
                return;
            }
            drop(_shutdown);

            if let Some(shutdown_wait) = shutdown_wait {
                self.queue_shutdown_operation_cleanup(shutdown_wait.blocker.into_cleanup());
                shutdown_wait.listener.wait();
            }
        }
    }

    #[inline]
    fn finish_shutdown_locked(&self, inner: &Arc<EngineInner>) -> ComponentShutdownOutcome {
        // Once no registered operation or observer remains, idle session state
        // can release its registry-owned guards before component shutdown.
        inner.session_registry.shutdown_idle();

        let outcome = self.components().shutdown_all();
        inner.lifecycle.mark_shutdown();
        outcome
    }

    /// Queues rollback for one shutdown-discovered abandoned transaction.
    ///
    /// The operation key locates the stable outer entry, while the transaction
    /// id identifies the exact public or private transaction to claim under the
    /// entry mutex. The second identity prevents a stale hint from claiming a
    /// newer private transaction installed under the same operation key. Other
    /// active operation states only block shutdown; accepted mandatory work
    /// already owns its cleanup authority through the stable operation entry.
    #[inline]
    fn queue_shutdown_operation_cleanup(&self, cleanup: Option<SessionCleanupRequest>) -> bool {
        let Some(SessionCleanupRequest {
            runtime,
            operation_key,
            trx_id,
        }) = cleanup
        else {
            return false;
        };
        let trx_sys = runtime.trx_sys.clone();
        trx_sys.request_abandoned_trx_cleanup(runtime, operation_key, trx_id);
        true
    }
}

impl Drop for Engine {
    #[inline]
    fn drop(&mut self) {
        // Implicit owner drop runs the same synchronous drain as explicit
        // shutdown. An unintended drop may therefore block until every
        // foreground operation and engine-owned background task completes.
        self.shutdown_inner(ShutdownOrigin::OwnerDrop);

        // Field order releases shared runtime reachability before registry-owned
        // component owners.
    }
}

/// Immutable component capabilities retained by registered session state.
///
/// The weak registry edge is used only for pointer-exact removal after a
/// session becomes closed and idle. It must never be used for operation
/// resolution.
pub(crate) struct EngineCore {
    /// Engine-level fatal runtime poison state.
    pub(crate) poisoner: QuiescentGuard<EnginePoisoner>,
    /// Engine-owned scheduler for accepted caller and internal obligations.
    pub(crate) mandatory_runtime: QuiescentGuard<MandatoryRuntime>,
    /// Shared catalog handle.
    pub(crate) catalog: QuiescentGuard<Catalog>,
    /// Shared transaction-system handle.
    pub(crate) trx_sys: QuiescentGuard<TransactionSystem>,
    /// Typed pool handles, owner-scoped guards, and session-root factory.
    pub(crate) pools: EnginePools,
    /// Table-file subsystem that runs persistent page IO.
    pub(crate) table_fs: QuiescentGuard<FileSystem>,
    /// Shared logical metadata and table-data lock manager.
    lock_manager: QuiescentGuard<LockManager>,
    /// Cold weak back-reference for pointer-exact idle-session removal.
    pub(crate) session_registry: Weak<SessionRegistry>,
    /// Per-engine table-DDL fault and phase controller.
    #[cfg(test)]
    pub(crate) table_ddl_test: TableDdlTestController,
    /// Per-engine index-DDL fault and phase controller.
    #[cfg(test)]
    pub(crate) index_ddl_test: IndexDdlTestController,
    /// Per-engine maintenance fault and phase controller.
    #[cfg(test)]
    pub(crate) maintenance_test: MaintenanceTestController,
}

impl EngineCore {
    /// Return the shared catalog handle.
    #[inline]
    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Return the shared logical lock manager.
    #[inline]
    pub(crate) fn lock_manager(&self) -> &QuiescentGuard<LockManager> {
        &self.lock_manager
    }
}

/// Owner-facing coordination shell for one [`Engine`].
///
/// Registered sessions retain only [`EngineCore`] and the lifecycle admission
/// gate, so no session-local authority can recover this owner shell.
pub(crate) struct EngineInner {
    /// Shared component capabilities.
    pub(crate) core: Arc<EngineCore>,
    /// Engine-owned strong session-state registry.
    pub(crate) session_registry: Arc<SessionRegistry>,
    /// Shared lifecycle admission and shutdown state.
    lifecycle: Arc<EngineLifecycle>,
    /// Monotonically increasing engine-local session identity source.
    next_session_id: AtomicU64,
}

impl EngineInner {
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
    pub(crate) fn acquire_admission(&self) -> LifecycleOrFatalResult<EngineAdmission<'_>> {
        let admission = self
            .lifecycle
            .admit()
            .attach_with(|| "phase=acquire_engine_lifecycle_admission")?;
        self.poisoner.ensure_healthy().map_err(|error| {
            LifecycleOrFatalError::from(error.attach("phase=check_engine_health"))
        })?;
        Ok(admission)
    }

    /// Run immediate synchronous work under engine admission.
    ///
    /// Use this helper for lifecycle validation plus local runtime lookup or
    /// strong pinning. The closure must not perform user callbacks, statement
    /// execution, blocking I/O, or async waits.
    #[inline]
    pub(crate) fn with_admitted_operation<T>(
        &self,
        f: impl FnOnce() -> T,
    ) -> LifecycleOrFatalResult<T> {
        let _admission = self.acquire_admission()?;
        Ok(f())
    }
}

impl Deref for EngineInner {
    type Target = EngineCore;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::test_io_backend_stats_handle_identity as pool_stats_handle_identity;
    use crate::catalog::tests::table1;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
    use crate::error::{ConfigError, Error, ErrorKind, FatalError, LifecycleError, RuntimeError};
    use crate::file::fs::tests::io_backend_stats_handle_identity as fs_stats_handle_identity;
    use crate::id::{TableID, TrxID};
    use crate::io::{
        IOKind, StdIoResult, StorageBackendFileIdentity, StorageBackendOp, StorageBackendTestHook,
        install_storage_backend_test_hook,
    };
    use crate::lock::tests::{TestLockOwner, debug_snapshot};
    use crate::lock::{LockMode, LockOwner, LockResource, TableLockMode};
    use crate::root::STORAGE_LAYOUT_FILE_NAME;
    use crate::session::tests::{
        SessionTestExt, assert_existing_transaction_error, finish_trx_commit_for_test,
        session_registry_len,
    };
    use crate::thread::{SpawnTestEvent, fail_spawn_named_with_observer, observe_spawn_named};
    use crate::trx::tests::{add_pseudo_redo_log_entry, pending_statement};
    use std::fs;
    use std::io::Error as StdIoError;
    use std::os::unix::fs::symlink;
    use std::panic::{self, AssertUnwindSafe};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::thread::{self, sleep, yield_now};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    const TEST_POOL_BYTES: usize = 64 * 1024 * 1024;

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

    fn test_engine_config_for(root: &Path) -> EngineConfig {
        EngineConfig::default()
            .storage_root(root)
            .meta_buffer(TEST_POOL_BYTES)
            .index_buffer(
                EvictableBufferPoolConfig::default()
                    .swap_file("index.swp")
                    .max_mem_size(TEST_POOL_BYTES)
                    .max_file_size(128usize * 1024 * 1024),
            )
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .max_mem_size(TEST_POOL_BYTES)
                    .max_file_size(128usize * 1024 * 1024),
            )
            .file(FileSystemConfig::default().readonly_buffer_size(TEST_POOL_BYTES))
            .trx(TrxSysConfig::default())
    }

    fn wait_until_shutdown_begins(engine: &Engine) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while engine.inner().lifecycle.inspect_state() == EngineLifecycleState::Running {
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
        debug_snapshot(engine.inner().core.lock_manager())
            .entries
            .iter()
            .filter(|entry| entry.family == owner.family())
            .count()
    }

    #[inline]
    fn assert_runtime_unavailable_after_shutdown(err: Error) {
        assert_eq!(err.kind(), ErrorKind::Lifecycle);
        assert_eq!(
            err.report().downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::Shutdown)
        );
    }

    #[test]
    fn test_engine_lifecycle_rejected_admission_reports_state() {
        let lifecycle = EngineLifecycle::new();
        lifecycle.close_admission();
        let err = match lifecycle.admit() {
            Ok(_) => panic!("admission should be rejected after lifecycle closure"),
            Err(err) => err,
        };
        assert_eq!(err.current_context(), &LifecycleError::Shutdown);
        let output = format!("{err:?}");
        assert!(output.contains("state=ShuttingDown"), "{output}");
    }

    #[test]
    fn test_poisoned_engine_new_session_admission_remains_fatal() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let _ = engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test admission poison"));

            let error = match engine.new_session() {
                Ok(_) => panic!("poisoned engine must reject new sessions"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), ErrorKind::Fatal);
            assert_eq!(
                error.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::RedoWrite)
            );
            assert!(error.report().downcast_ref::<LifecycleError>().is_none());
        });
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
                "Mandatory-Runtime-1",
                "phase=start_mandatory_runtime_runner",
            ),
            (
                "Mandatory-Runtime-2",
                "phase=start_mandatory_runtime_runner",
            ),
        ] {
            let root = TempDir::new().unwrap();
            let (event_tx, event_rx) = mpsc::channel();
            let _failure = fail_spawn_named_with_observer(worker, move |event| {
                let _ = event_tx.send(event);
            });

            let err = match smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))) {
                Ok(_) => panic!("injected worker spawn must fail engine startup"),
                Err(err) => err,
            };

            assert_eq!(err.kind(), ErrorKind::Runtime, "worker={worker}, err={err}");
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
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
            if worker.starts_with("Mandatory-Runtime-") {
                assert!(
                    started.iter().any(|name| name == "Purge-Dispatcher"),
                    "purge did not start before mandatory workers: started={started:?}"
                );
                assert!(
                    !started.iter().any(|name| name == "Log-Thread"),
                    "redo started after mandatory worker startup failed: started={started:?}"
                );
            }
            if worker == "Mandatory-Runtime-2" {
                assert!(
                    started.iter().any(|name| name == "Mandatory-Runtime-1"),
                    "first mandatory runner did not start: started={started:?}"
                );
            }
        }
    }

    #[test]
    fn test_initial_redo_header_failure_reclaims_started_workers_before_startup_returns() {
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

        let err = match smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))) {
            Ok(_) => panic!("initial redo-header write failure must fail engine startup"),
            Err(err) => err,
        };

        assert!(hook.failed(), "initial redo-header write was not injected");
        assert_eq!(err.kind(), ErrorKind::Fatal);
        assert_eq!(
            err.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::RedoWrite)
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
        for expected in [
            "IO-Thread",
            "Log-Thread",
            "Purge-Dispatcher",
            "Purge-Executor-1",
            "Shared-Pool-Evictor",
        ] {
            assert!(
                started.iter().any(|name| name == expected),
                "expected worker did not start: {expected}, started={started:?}"
            );
        }
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

        let err = match smol::block_on(Engine::bootstrap(config)) {
            Ok(_) => panic!("second purge-executor spawn failure must fail engine startup"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
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
        assert!(
            !started.iter().any(|name| name == "Log-Thread"),
            "redo started after purge startup had already failed: {started:?}"
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

        let err = match smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))) {
            Ok(_) => panic!("injected purge dispatcher spawn must fail engine startup"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Runtime);
        assert_eq!(
            err.report().downcast_ref::<RuntimeError>().copied(),
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

    #[test]
    fn test_catalog_checkpoint_scan_io_depth_comes_from_trx_config() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                test_engine_config_for(root.path()).trx(
                    TrxSysConfig::default()
                        .log_write_io_depth(2)
                        .recovery_io_depth(3)
                        .catalog_checkpoint_scan_io_depth(4),
                ),
            )
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
    fn test_session_ids_are_monotonic_across_engine_sessions() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let session1 = engine.new_session().unwrap();
            let session2 = engine.new_session().unwrap();
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
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let runtime = session.engine();
            let resource = LockResource::TableMetadata(TableID::new(10));
            let mut first = TestLockOwner::new(LockOwner::session_explicit(SessionID::new(10)));
            first
                .acquire(
                    engine.inner().core.lock_manager(),
                    resource,
                    LockMode::Exclusive,
                )
                .await
                .unwrap();

            let mut second = TestLockOwner::new(LockOwner::session_explicit(SessionID::new(11)));
            let mut acquire =
                Box::pin(second.acquire(runtime.lock_manager(), resource, LockMode::Shared));
            assert!(matches!(
                futures::poll!(acquire.as_mut()),
                std::task::Poll::Pending
            ));

            first.close(runtime.lock_manager());
            acquire.await.unwrap();
            second.close(engine.inner().core.lock_manager());
        });
    }

    #[test]
    fn test_engine_shared_storage_runtime_reuses_one_backend_stats_handle() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                test_engine_config_for(root.path())
                    .file(
                        FileSystemConfig::default()
                            .io_depth(7)
                            .readonly_buffer_size(TEST_POOL_BYTES),
                    )
                    .data_buffer(
                        EvictableBufferPoolConfig::default()
                            .max_mem_size(TEST_POOL_BYTES)
                            .max_file_size(128usize * 1024 * 1024),
                    ),
            )
            .await
            .unwrap();

            let table_stats = fs_stats_handle_identity(&engine.inner().table_fs);
            let mem_stats = pool_stats_handle_identity(&engine.inner().pools.mem);
            let index_stats = pool_stats_handle_identity(&engine.inner().pools.index);

            assert_eq!(table_stats, mem_stats);
            assert_eq!(table_stats, index_stats);
        });
    }

    #[test]
    fn test_engine_shared_storage_io_depth_comes_from_file_system_config() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                test_engine_config_for(root.path())
                    .file(
                        FileSystemConfig::default()
                            .io_depth(7)
                            .readonly_buffer_size(TEST_POOL_BYTES),
                    )
                    .data_buffer(
                        EvictableBufferPoolConfig::default()
                            .max_mem_size(TEST_POOL_BYTES)
                            .max_file_size(128usize * 1024 * 1024),
                    ),
            )
            .await
            .unwrap();

            assert_eq!(engine.inner().table_fs.configured_io_depth(), 7);
            assert_eq!(
                engine.inner().pools.mem.io_backend_stats(),
                engine.inner().table_fs.io_backend_stats()
            );
            assert_eq!(
                engine.inner().pools.index.io_backend_stats(),
                engine.inner().table_fs.io_backend_stats()
            );
        });
    }

    #[test]
    fn test_storage_layout_marker_allows_data_swap_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            drop(engine);

            let engine = Engine::bootstrap(
                test_engine_config_for(root.path()).data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024)
                        .swap_file("alt-data.swp"),
                ),
            )
            .await
            .unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_storage_layout_marker_allows_index_swap_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            drop(engine);

            let engine = Engine::bootstrap(
                test_engine_config_for(root.path()).index_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(TEST_POOL_BYTES)
                        .max_file_size(128usize * 1024 * 1024)
                        .swap_file("alt-index.swp"),
                ),
            )
            .await
            .unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_engine_startup_creates_default_swap_files() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            drop(engine);

            assert!(root.path().join("data.swp").exists());
            assert!(root.path().join("index.swp").exists());
        });
    }

    #[test]
    fn test_storage_layout_marker_rejects_data_dir_change() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            drop(engine);

            let err = match Engine::bootstrap(
                test_engine_config_for(root.path())
                    .file(FileSystemConfig::default().data_dir("data")),
            )
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
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            drop(engine);

            let new_data_dir = root.path().join("other-data");
            assert!(!new_data_dir.exists());

            let err = match Engine::bootstrap(
                test_engine_config_for(root.path())
                    .file(FileSystemConfig::default().data_dir("other-data")),
            )
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

            let engine = Engine::bootstrap(test_engine_config_for(&root_a))
                .await
                .unwrap();
            drop(engine);

            fs::rename(&root_a, &root_b).unwrap();

            let engine = Engine::bootstrap(test_engine_config_for(&root_b))
                .await
                .unwrap();
            drop(engine);
        });
    }

    #[test]
    fn test_active_engine_excludes_aliases_and_shutdown_releases_root() {
        smol::block_on(async {
            let parent = TempDir::new().unwrap();
            let root = parent.path().join("storage");
            let engine = Engine::bootstrap(test_engine_config_for(&root))
                .await
                .unwrap();
            let alias = parent.path().join("storage-alias");
            symlink(&root, &alias).unwrap();
            let alternate_data = root.join("alternate-data");

            let err = match Engine::bootstrap(
                test_engine_config_for(&alias).file(
                    FileSystemConfig::default()
                        .data_dir("alternate-data")
                        .readonly_buffer_size(TEST_POOL_BYTES),
                ),
            )
            .await
            {
                Ok(_) => panic!("active canonical storage root must reject a second engine"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::StorageRootInUse)
            );
            let output = format!("{err:?}");
            assert!(output.contains("owner_pid="), "{output}");
            assert!(!alternate_data.exists());

            engine.shutdown();
            let replacement = Engine::bootstrap(test_engine_config_for(&root))
                .await
                .unwrap();
            drop(replacement);
            drop(engine);
        });
    }

    #[test]
    fn test_failed_startup_does_not_persist_storage_layout_marker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let marker_path = root.path().join(STORAGE_LAYOUT_FILE_NAME);

            let err = match Engine::bootstrap(
                EngineConfig::default()
                    .storage_root(root.path())
                    .meta_buffer(TEST_POOL_BYTES)
                    .index_buffer(
                        EvictableBufferPoolConfig::default()
                            .swap_file("index.swp")
                            .max_mem_size(TEST_POOL_BYTES)
                            .max_file_size(128usize * 1024 * 1024),
                    )
                    .file(FileSystemConfig::default().readonly_buffer_size(TEST_POOL_BYTES))
                    .data_buffer(
                        EvictableBufferPoolConfig::default()
                            .max_mem_size(1024usize * 1024)
                            .max_file_size(2usize * 1024 * 1024),
                    )
                    .trx(TrxSysConfig::default()),
            )
            .await
            {
                Ok(_) => panic!("expected startup failure"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Config);
            assert_eq!(
                err.report().downcast_ref::<ConfigError>().copied(),
                Some(ConfigError::InvalidBufferPoolConfig)
            );
            assert!(format!("{err:?}").contains("data_buffer"));
            assert!(!marker_path.exists());

            let engine = Engine::bootstrap(
                test_engine_config_for(root.path()).file(
                    FileSystemConfig::default()
                        .data_dir("data")
                        .readonly_buffer_size(TEST_POOL_BYTES),
                ),
            )
            .await
            .unwrap();
            drop(engine);
            assert!(marker_path.exists());
        });
    }

    #[test]
    fn test_readonly_buffer_pool_preflight_reports_config_error() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let marker_path = root.path().join(STORAGE_LAYOUT_FILE_NAME);
            let err = match Engine::bootstrap(
                test_engine_config_for(root.path())
                    .file(FileSystemConfig::default().readonly_buffer_size(1usize)),
            )
            .await
            {
                Ok(_) => panic!("undersized readonly buffer pool should fail"),
                Err(err) => err,
            };

            assert_eq!(err.kind(), ErrorKind::Config);
            assert_eq!(
                err.report().downcast_ref::<ConfigError>().copied(),
                Some(ConfigError::InvalidFixedBufferPoolSize)
            );
            assert!(format!("{err:?}").contains("file.readonly_buffer_size=1"));
            assert!(!marker_path.exists());
        });
    }

    #[test]
    fn test_invalid_swap_path_startup_does_not_persist_storage_layout_marker() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let marker_path = root.path().join(STORAGE_LAYOUT_FILE_NAME);

            let err = match Engine::bootstrap(
                test_engine_config_for(root.path()).data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(TEST_POOL_BYTES)
                        .max_file_size(128usize * 1024 * 1024)
                        .swap_file("catalog.mtb/data.swp"),
                ),
            )
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
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();

            engine.shutdown();
            engine.shutdown();

            let err = match engine.new_session() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_runtime_unavailable_after_shutdown(err);
        });
    }

    #[test]
    fn test_engine_shutdown_contains_purge_finish_panic_and_releases_root() {
        let root = TempDir::new().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let injected = Arc::new(AtomicBool::new(false));
        let observed_events = Arc::clone(&events);
        let observed_injected = Arc::clone(&injected);
        let observer = observe_spawn_named(move |event| {
            observed_events.lock().push(event.clone());
            if event == SpawnTestEvent::Finished("Purge-Dispatcher".to_owned())
                && !observed_injected.swap(true, Ordering::AcqRel)
            {
                panic::panic_any("injected purge dispatcher finish panic");
            }
        });
        let engine =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();

        let payload = panic::catch_unwind(AssertUnwindSafe(|| engine.shutdown())).unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("injected purge dispatcher finish panic")
        );
        assert!(injected.load(Ordering::Acquire));
        assert_eq!(
            engine.inner().lifecycle.inspect_state(),
            EngineLifecycleState::Shutdown
        );

        // A contained payload is consumed once; neither repeated explicit
        // shutdown nor eventual owner drop may replay it.
        engine.shutdown();
        engine.try_shutdown().unwrap();

        let events = events.lock();
        let finish_position = |worker: &str| {
            events
                .iter()
                .position(|event| event == &SpawnTestEvent::Finished(worker.to_owned()))
                .unwrap_or_else(|| panic!("worker did not finish after contained panic: {worker}"))
        };
        let redo_finished = finish_position("Log-Thread");
        let mandatory_1_finished = finish_position("Mandatory-Runtime-1");
        let mandatory_2_finished = finish_position("Mandatory-Runtime-2");
        let purge_dispatcher_finished = finish_position("Purge-Dispatcher");
        let purge_executor_finished = finish_position("Purge-Executor-1");
        let evictor_finished = finish_position("Shared-Pool-Evictor");
        let io_finished = finish_position("IO-Thread");
        assert!(redo_finished < mandatory_1_finished);
        assert!(redo_finished < mandatory_2_finished);
        assert!(mandatory_1_finished < purge_dispatcher_finished);
        assert!(mandatory_2_finished < purge_dispatcher_finished);
        assert!(purge_dispatcher_finished < evictor_finished);
        assert!(purge_executor_finished < evictor_finished);
        assert!(evictor_finished < io_finished);
        drop(events);
        drop(observer);

        // Root-lease shutdown is an active hook, so a replacement engine can
        // start while the degraded terminal owner remains allocated.
        let replacement =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();
        replacement.shutdown();
        drop(replacement);
        drop(engine);
    }

    #[test]
    fn test_engine_owner_drop_suppresses_shutdown_panic_during_outer_unwind() {
        let root = TempDir::new().unwrap();
        let injected = Arc::new(AtomicBool::new(false));
        let observed_injected = Arc::clone(&injected);
        let observer = observe_spawn_named(move |event| {
            if event == SpawnTestEvent::Finished("Purge-Dispatcher".to_owned())
                && !observed_injected.swap(true, Ordering::AcqRel)
            {
                panic::panic_any("injected owner-drop purge finish panic");
            }
        });
        let engine =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();

        let payload = panic::catch_unwind(AssertUnwindSafe(move || {
            let _engine = engine;
            panic::panic_any("outer engine owner panic");
        }))
        .unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("outer engine owner panic")
        );
        assert!(injected.load(Ordering::Acquire));
        drop(observer);

        let replacement =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();
        replacement.shutdown();
    }

    #[test]
    fn test_engine_contains_evictor_and_io_finish_panics_after_stop_signals() {
        for target in ["Shared-Pool-Evictor", "IO-Thread"] {
            let root = TempDir::new().unwrap();
            let events = Arc::new(Mutex::new(Vec::new()));
            let observed_events = Arc::clone(&events);
            let observer = observe_spawn_named(move |event| {
                observed_events.lock().push(event.clone());
                if event == SpawnTestEvent::Finished(target.to_owned()) {
                    panic::panic_any(format!("injected {target} finish panic"));
                }
            });
            let engine =
                smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();

            let payload = panic::catch_unwind(AssertUnwindSafe(|| engine.shutdown())).unwrap_err();
            assert_eq!(
                payload.downcast_ref::<String>().map(String::as_str),
                Some(format!("injected {target} finish panic").as_str())
            );
            assert_eq!(
                engine.inner().lifecycle.inspect_state(),
                EngineLifecycleState::Shutdown
            );
            let events = events.lock();
            assert!(
                events.contains(&SpawnTestEvent::Finished(target.to_owned())),
                "target worker did not finish: {target}"
            );
            if target == "Shared-Pool-Evictor" {
                assert!(
                    events.contains(&SpawnTestEvent::Finished("IO-Thread".to_owned())),
                    "I/O teardown did not continue after evictor join panic"
                );
            }
            drop(events);
            drop(observer);

            let replacement =
                smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();
            replacement.shutdown();
            drop(replacement);
            drop(engine);
        }
    }

    #[test]
    fn test_engine_shutdown_ignores_live_idle_session_handle() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            engine.shutdown();
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
                .lock_table(TableID::new(91_300), TableLockMode::Shared)
                .await
            {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_runtime_unavailable_after_shutdown(err);

            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_engine_shutdown_busy_keeps_pinned_idle_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let session_handle = engine.new_session().unwrap();
            let session = session_handle.pin_observer().unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            let err = match engine.try_shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ShutdownBusy)
            );
            assert_eq!(
                err.report().downcast_ref::<String>().map(String::as_str),
                Some(
                    "origin=explicit, session_blocker=observer, operation_state=none, observer_count=1, cleanup_queued=false, mandatory_callers=0, mandatory_internal=0"
                )
            );
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            drop(session);
            engine.shutdown();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);
        });
    }

    #[test]
    fn test_engine_shutdown_busy_until_active_transaction_finishes() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = match engine.try_shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ShutdownBusy)
            );
            assert_eq!(
                err.report().downcast_ref::<String>().map(String::as_str),
                Some(
                    "origin=explicit, session_blocker=operation, operation_state=voluntary, observer_count=0, cleanup_queued=false, mandatory_callers=0, mandatory_internal=0"
                )
            );

            trx.rollback().await.unwrap();
            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_engine_shutdown_rejects_non_terminal_transaction_work() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            let err = match engine.try_shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ShutdownBusy)
            );
            assert_eq!(
                err.report().downcast_ref::<String>().map(String::as_str),
                Some(
                    "origin=explicit, session_blocker=operation, operation_state=voluntary, observer_count=0, cleanup_queued=false, mandatory_callers=0, mandatory_internal=0"
                )
            );

            let err = trx
                .lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap_err();
            assert_runtime_unavailable_after_shutdown(err);

            trx.rollback().await.unwrap();
            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn test_engine_shutdown_waits_for_pinned_idle_session() {
        let root = TempDir::new().unwrap();
        let engine =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();
        let session_handle = engine.new_session().unwrap();
        let session = session_handle.pin_observer().unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

        thread::scope(|scope| {
            let shutdown_engine = &engine;
            let shutdown_handle = scope.spawn(move || {
                shutdown_engine.shutdown();
                done_tx.send(()).unwrap();
            });

            wait_until_shutdown_begins(&engine);
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "shutdown must wait while a SessionObserverPin is alive"
            );
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            drop(session);
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown should complete after SessionObserverPin drops");
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);
            shutdown_handle.join().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_wakes_maintenance_progress_wait() {
        let root = TempDir::new().unwrap();
        let engine =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();
        let session = engine.new_session().unwrap();
        let (started_tx, started_rx) = mpsc::channel();

        thread::scope(|scope| {
            let waiter = scope.spawn(move || {
                started_tx.send(()).unwrap();
                let err = smol::block_on(session.wait_for_gc_horizon_after(TrxID::new(u64::MAX)))
                    .unwrap_err();
                assert_eq!(
                    err.report().downcast_ref::<LifecycleError>().copied(),
                    Some(LifecycleError::Shutdown)
                );
            });
            started_rx.recv().unwrap();
            engine.shutdown();
            waiter.join().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_waits_for_active_transaction_to_finish() {
        let root = TempDir::new().unwrap();
        let engine =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();
        let mut session = engine.new_session().unwrap();
        let trx = session.begin_trx().unwrap();
        let (done_tx, done_rx) = mpsc::channel();

        thread::scope(|scope| {
            let shutdown_engine = &engine;
            let shutdown_handle = scope.spawn(move || {
                shutdown_engine.shutdown();
                done_tx.send(()).unwrap();
            });

            wait_until_shutdown_begins(&engine);
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "shutdown must wait while an active transaction is alive"
            );

            smol::block_on(trx.rollback()).unwrap();
            drop(session);
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown should complete after transaction rollback");
            shutdown_handle.join().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_waits_for_checked_out_abandoned_transaction_to_return() {
        let root = TempDir::new().unwrap();
        let engine =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        let checkout = trx.checkout().unwrap();
        let (done_tx, done_rx) = mpsc::channel();

        drop(trx);
        assert!(session.in_trx().unwrap());

        thread::scope(|scope| {
            let shutdown_engine = &engine;
            let shutdown_handle = scope.spawn(move || {
                shutdown_engine.shutdown();
                done_tx.send(()).unwrap();
            });

            wait_until_shutdown_begins(&engine);
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "shutdown must wait until checked-out abandoned transaction returns"
            );

            drop(checkout);
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown should complete after checkout returns");
            shutdown_handle.join().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_waits_for_cancelled_statement_return() {
        let root = TempDir::new().unwrap();
        let engine =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        let mut exec = Box::pin(pending_statement(&mut trx));
        smol::block_on(async {
            assert!(matches!(
                futures::poll!(exec.as_mut()),
                std::task::Poll::Pending
            ));
        });
        let (done_tx, done_rx) = mpsc::channel();

        thread::scope(|scope| {
            let shutdown_engine = &engine;
            let shutdown_handle = scope.spawn(move || {
                shutdown_engine.shutdown();
                done_tx.send(()).unwrap();
            });

            wait_until_shutdown_begins(&engine);
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "shutdown must wait until the cancelled statement returns its checkout"
            );

            drop(exec);
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown should complete after statement cancellation cleanup");
            shutdown_handle.join().unwrap();
        });
    }

    #[test]
    fn test_session_close_rejects_active_transaction_then_retries_after_rollback() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session.close().await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );
            assert!(session.in_trx().unwrap());

            trx.rollback().await.unwrap();
            assert!(!session.in_trx().unwrap());
            session.close().await.unwrap();
            session.close().await.unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);
            engine.shutdown();
        });
    }

    #[test]
    fn test_session_in_trx_returns_error_after_session_close() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();

            assert!(!session.in_trx().unwrap());
            session.close().await.unwrap();

            let err = session.in_trx().unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::SessionUnavailable)
            );
            engine.shutdown();
        });
    }

    #[test]
    fn test_session_in_trx_returns_error_after_engine_shutdown() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let session = engine.new_session().unwrap();

            assert!(!session.in_trx().unwrap());
            engine.shutdown();

            let err = session.in_trx().unwrap_err();
            assert_runtime_unavailable_after_shutdown(err);
        });
    }

    #[test]
    fn test_dropped_active_session_is_removed_after_transaction_terminal() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            drop(session);
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);
            trx.rollback().await.unwrap();
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);
            engine.shutdown();
        });
    }

    #[test]
    fn test_dropped_transaction_handle_cleanup_releases_locks_and_reuses_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let mut trx = session.begin_trx().unwrap();
            let owner = LockOwner::transaction(session_id, trx.trx_id());

            trx.lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap();
            assert!(lock_entry_count(&engine, owner) > 0);

            drop(trx);
            wait_until(
                || session.in_trx().is_ok_and(|in_trx| !in_trx),
                "abandoned transaction cleanup did not return the session to idle",
            );
            assert_eq!(lock_entry_count(&engine, owner), 0);

            let replacement = session.begin_trx().unwrap();
            replacement.rollback().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn test_checked_out_abandoned_cleanup_runs_after_checkout_return() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let table_id = table1(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let mut trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();
            let owner = LockOwner::transaction(session_id, trx.trx_id());

            trx.lock_table(table_id, TableLockMode::Shared)
                .await
                .unwrap();
            assert!(lock_entry_count(&engine, owner) > 0);

            let checkout = trx.checkout().unwrap();
            drop(trx);
            assert!(session.in_trx().unwrap());
            assert!(session.list_table_ids().is_ok());
            let err = match session.begin_trx() {
                Ok(_) => panic!("checked-out abandoned transaction must block a replacement"),
                Err(err) => err,
            };
            assert_existing_transaction_error(&err, session.id(), trx_id, "voluntary");
            assert!(lock_entry_count(&engine, owner) > 0);

            drop(checkout);
            wait_until(
                || session.in_trx().is_ok_and(|in_trx| !in_trx),
                "checkout return did not schedule abandoned transaction cleanup",
            );
            assert_eq!(lock_entry_count(&engine, owner), 0);
            assert!(session.list_table_ids().is_ok());

            let replacement = session.begin_trx().unwrap();
            replacement.rollback().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn test_dropped_session_live_transaction_can_commit() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            add_pseudo_redo_log_entry(&mut trx).await;
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);

            drop(session);
            assert_eq!(session_registry_len(&engine.inner().session_registry), 1);
            assert!(trx.commit().await.unwrap() > TrxID::new(0));
            assert_eq!(session_registry_len(&engine.inner().session_registry), 0);

            engine.shutdown();
        });
    }

    #[test]
    fn test_dropping_session_then_transaction_removes_abandoned_session() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
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

            engine.shutdown();
        });
    }

    #[test]
    fn test_admitted_operation_token_releases_before_shutdown_completes() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let admission = engine.inner().acquire_admission().unwrap();
            let (started_tx, started_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();

            thread::scope(|scope| {
                let shutdown_handle = scope.spawn(|| {
                    started_tx.send(()).unwrap();
                    engine.shutdown();
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
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();

            let trx = session.begin_trx().unwrap();
            assert!(session.in_trx().unwrap());
            let err = match session.begin_trx() {
                Ok(_) => panic!("expected existing transaction error"),
                Err(err) => err,
            };
            let kind = err.kind();
            let lifecycle_error = err.report().downcast_ref::<LifecycleError>().copied();

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
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
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
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
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
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();

            let trx = session.begin_trx().unwrap();
            let stale_trx_id = trx.trx_id();
            trx.rollback().await.unwrap();
            assert_eq!(session.last_cts(), TrxID::new(0));

            let replacement = session.begin_trx().unwrap();
            finish_trx_commit_for_test(
                &engine.inner().session_registry,
                session.id(),
                stale_trx_id,
                TrxID::new(91_241),
            );

            assert!(session.in_trx().unwrap());
            assert_eq!(session.last_cts(), TrxID::new(0));

            replacement.rollback().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn test_same_session_reuse_after_readonly_commit() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
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
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();
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
    fn test_drop_engine_without_explicit_shutdown_succeeds() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(test_engine_config_for(root.path()))
                .await
                .unwrap();

            drop(engine);
        });
    }

    #[test]
    fn test_drop_engine_waits_for_active_transaction_to_finish() {
        let root = TempDir::new().unwrap();
        let engine =
            smol::block_on(Engine::bootstrap(test_engine_config_for(root.path()))).unwrap();
        let mut session = engine.new_session().unwrap();
        let trx = session.begin_trx().unwrap();
        let shutdown_started = engine.inner().lifecycle.shutdown_listener();
        let (done_tx, done_rx) = mpsc::channel();

        thread::scope(|scope| {
            let drop_handle = scope.spawn(move || {
                drop(engine);
                done_tx.send(()).unwrap();
            });
            shutdown_started.wait();
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "engine drop must wait while an active transaction is alive"
            );

            smol::block_on(trx.rollback()).unwrap();
            drop(session);
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("engine drop should complete after transaction rollback");
            drop_handle.join().unwrap();
        });
    }

    #[test]
    fn test_unstarted_transaction_system_shutdown_is_safe() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let log_dir = temp_dir.path().join("log");
            fs::create_dir_all(&log_dir).unwrap();
            let engine = Engine::bootstrap(
                test_engine_config_for(temp_dir.path())
                    .file(
                        FileSystemConfig::default()
                            .data_dir("data")
                            .readonly_buffer_size(TEST_POOL_BYTES),
                    )
                    .trx(TrxSysConfig::default()),
            )
            .await
            .unwrap();

            let config = TrxSysConfig::default()
                .log_dir(&log_dir)
                .log_file_stem("pending-startup-cleanup");
            let config = ValidatedTrxSysConfig::try_new(config).unwrap();
            let (trx_sys, startup) = TransactionSystem::bootstrap(
                config,
                engine.inner().poisoner.clone(),
                engine.inner().mandatory_runtime.clone(),
                EnginePools::new(
                    engine.inner().core.pools.meta.clone(),
                    engine.inner().core.pools.index.clone(),
                    engine.inner().core.pools.mem.clone(),
                    engine.inner().core.pools.disk.clone(),
                ),
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
