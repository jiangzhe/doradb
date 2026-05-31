//! Storage engine for DoraDB.
//!
//! This module provides the main entry point of the storage engine,
//! including start, stop, recover, and execute commands. See
//! `docs/engine-component-lifetime.md` for the runtime-versus-owner lifetime
//! model that this module and [`crate::component::ComponentRegistry`] enforce.
use crate::buffer::PoolRole;
use crate::buffer::SharedPoolEvictorWorkers;
use crate::catalog::Catalog;
use crate::component::{
    ComponentRegistry, DiskPoolConfig, IndexPoolConfig, MetaPoolConfig, RegistryBuilder,
};
use crate::conf::EngineConfig;
use crate::error::{LifecycleError, LifecycleResult, OperationError, Result};
use crate::file::fs::{FileSystem, FileSystemWorkers};
use crate::id::{SessionID, TableID};
use crate::lock::LockManager;
use crate::quiescent::QuiescentGuard;
use crate::session::Session;
use crate::trx::sys::TransactionSystem;
use crate::{DiskPool, IndexPool, MemPool, MetaPool};
use error_stack::{Report, ensure};
use event_listener::{Event, Listener, listener};
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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
    fn try_from(value: usize) -> std::result::Result<Self, Self::Error> {
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
    finalize_lock: Mutex<()>,
}

impl EngineLifecycle {
    #[inline]
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(EngineLifecycleState::Running.as_usize()),
            admission_released: Event::new(),
            finalize_lock: Mutex::new(()),
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
                    let next = (word & !LIFECYCLE_STATE_MASK)
                        | EngineLifecycleState::ShuttingDown.as_usize();
                    if self
                        .state
                        .compare_exchange_weak(word, next, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
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

/// Storage engine owner.
///
/// `Engine` owns teardown-only state such as the top-level
/// [`ComponentRegistry`], while [`EngineRef`] and [`Session`] hold only the
/// shared runtime handle in [`EngineInner`]. Explicit shutdown and final owner
/// drop therefore stay with the owner object instead of the cloneable runtime
/// access path.
pub struct Engine {
    inner: Option<Arc<EngineInner>>,
    components: Option<ComponentRegistry>,
}

impl Engine {
    #[inline]
    fn inner(&self) -> &Arc<EngineInner> {
        self.inner
            .as_ref()
            .expect("engine owner keeps runtime handle until drop")
    }

    #[inline]
    fn components(&self) -> &ComponentRegistry {
        self.components
            .as_ref()
            .expect("engine owner keeps component registry until drop")
    }

    #[inline]
    fn release_owned_parts(&mut self) -> (Arc<EngineInner>, ComponentRegistry) {
        let inner = self
            .inner
            .take()
            .expect("engine runtime handle is present until final drop");
        let components = self
            .components
            .take()
            .expect("engine component registry is present until final drop");
        (inner, components)
    }

    /// Create a new session while the engine is still running.
    #[inline]
    pub fn new_session(&self) -> Result<Session> {
        let inner = self.inner();
        inner.with_admitted_operation(|| {
            let id = inner.next_session_id();
            Session::new(EngineRef(Arc::clone(inner)), id)
        })
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

    /// Try to clone the shared runtime handle while the engine is still
    /// running.
    #[inline]
    pub fn new_ref(&self) -> Result<EngineRef> {
        let inner = self.inner();
        inner.with_admitted_operation(|| EngineRef(Arc::clone(inner)))
    }

    /// Start idempotent engine shutdown.
    ///
    /// Shutdown rejects new work immediately, waits for user-owned
    /// [`EngineRef`]s and sessions to drain, then dispatches component shutdown
    /// in reverse registration order.
    ///
    /// This path is valid after storage poison. Poison only blocks admission; it
    /// does not replace the owner-side responsibility to stop background workers
    /// and drop components in the registered order.
    #[inline]
    pub fn shutdown(&self) -> Result<()> {
        self.finalize_shutdown()
    }

    #[inline]
    fn finalize_shutdown(&self) -> Result<()> {
        let inner = self.inner();
        let _finalize = inner.lifecycle.finalize_lock.lock();
        if inner.lifecycle.state() == EngineLifecycleState::Shutdown {
            return Ok(());
        }

        inner.lifecycle.close_admission();
        inner.lifecycle.wait_for_admissions_drained();

        // Any live session/transaction/statement keeps an `EngineRef` alive
        // through `SessionState`. Owner-side `EngineRef` creation and session
        // admission increment `active_admissions`, so once the Running ->
        // ShuttingDown transition completes and the admission count drains, no
        // new owner-created runtime handles can appear before this snapshot.
        // Requiring the last strong reference here gives transaction-system
        // shutdown a clean point where user-originated work has already drained
        // before we start disabling runtime state. This requirement is
        // unchanged for poisoned engines: poison does not drain sessions or old
        // table handles for us.
        let strong_count = Arc::strong_count(inner);
        if strong_count != 1 {
            return Err(Report::new(LifecycleError::ShutdownBusy)
                .attach(strong_count - 1)
                .into());
        }

        self.components().shutdown_all();
        inner.lifecycle.mark_shutdown();
        Ok(())
    }
}

impl Deref for Engine {
    type Target = EngineInner;

    #[inline]
    fn deref(&self) -> &EngineInner {
        self.inner().as_ref()
    }
}

impl Drop for Engine {
    #[inline]
    fn drop(&mut self) {
        if let Err(err) = self.finalize_shutdown() {
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
                std::mem::forget(components);
            }
            panic!("fatal: engine shutdown failed: {err}");
        }

        // Drop the shared runtime handle before registry-owned component
        // owners. That makes the owner/runtime split explicit instead of
        // relying on incidental struct layout.
        let (inner, components) = self.release_owned_parts();
        drop(inner);
        drop(components);
    }
}

/// Cloneable shared runtime handle for the storage engine.
///
/// `EngineRef` intentionally does not own shutdown orchestration. It only
/// exposes the runtime state needed by sessions and internal subsystems.
#[derive(Clone)]
pub struct EngineRef(Arc<EngineInner>);

impl Deref for EngineRef {
    type Target = EngineInner;

    #[inline]
    fn deref(&self) -> &EngineInner {
        &self.0
    }
}

impl EngineRef {
    /// Create a new session while the engine is still running.
    #[inline]
    pub fn new_session(&self) -> Result<Session> {
        self.0.with_admitted_operation(|| {
            let id = self.0.next_session_id();
            Session::new(self.clone(), id)
        })
    }

    /// Return the shared catalog handle.
    #[inline]
    pub(crate) fn catalog(&self) -> &Catalog {
        &self.0.catalog
    }

    /// Get a user-table runtime handle by table id.
    #[inline]
    pub async fn get_table(&self, table_id: TableID) -> Result<Arc<crate::Table>> {
        self.0
            .with_admitted_operation(|| self.0.catalog().get_table_now(table_id))?
            .ok_or_else(|| {
                error_stack::Report::new(OperationError::TableNotFound)
                    .attach(format!("get table: table_id={table_id}"))
                    .into()
            })
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

/// Shared runtime state for an [`Engine`].
///
/// The fields here are the cloneable handles that sessions and other runtime
/// objects may retain. Owner-only teardown state lives on [`Engine`] itself.
pub struct EngineInner {
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
    pub(crate) fn acquire_admission(&self) -> Result<EngineAdmission<'_>> {
        let admission = self.lifecycle.admit()?;
        self.trx_sys.ensure_runtime_healthy()?;
        Ok(admission)
    }

    /// Run immediate synchronous work under engine admission.
    ///
    /// Use this helper for lifecycle validation plus local runtime lookup or
    /// strong pinning. The closure must not perform user callbacks, statement
    /// execution, blocking I/O, or async waits.
    #[inline]
    pub(crate) fn with_admitted_operation<T>(&self, f: impl FnOnce() -> T) -> Result<T> {
        let _admission = self.acquire_admission()?;
        Ok(f())
    }
}

impl EngineConfig {
    /// Build the storage engine and all registered components.
    #[inline]
    pub async fn build(self) -> Result<Engine> {
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
        let mut builder = RegistryBuilder::new();
        // Components are registered in one fixed dependency order. Reverse
        // registration order then defines both explicit shutdown order and the
        // final owner drop order.
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
        builder.build::<Catalog>(()).await?;
        builder.build::<TransactionSystem>(trx_cfg).await?;

        resolved.persist_marker_if_missing()?;
        let registry = builder.finish()?;
        let catalog = registry.dependency::<Catalog>()?;
        let trx_sys = registry.dependency::<TransactionSystem>()?;
        let meta_pool = registry.dependency::<MetaPool>()?;
        let index_pool = registry.dependency::<IndexPool>()?;
        let mem_pool = registry.dependency::<MemPool>()?;
        let table_fs = registry.dependency::<FileSystem>()?;
        let disk_pool = registry.dependency::<DiskPool>()?;
        let lock_manager = registry.dependency::<LockManager>()?;
        let engine_inner = EngineInner {
            catalog,
            trx_sys,
            meta_pool,
            index_pool,
            mem_pool,
            table_fs,
            disk_pool,
            lock_manager,
            next_session_id: AtomicU64::new(FIRST_SESSION_ID.as_u64()),
            lifecycle: EngineLifecycle::new(),
        };
        Ok(Engine {
            inner: Some(Arc::new(engine_inner)),
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
    use crate::error::{ConfigError, ErrorKind, LifecycleError, OperationError, ResourceError};
    use crate::file::fs::tests::io_backend_stats_handle_identity as fs_stats_handle_identity;
    use crate::id::TrxID;
    use crate::lock::tests::{debug_snapshot, try_acquire};
    use crate::lock::{LockMode, LockOwner, LockResource};
    use std::fs;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Barrier, mpsc};
    use std::time::Duration;
    use tempfile::TempDir;

    const TEST_POOL_BYTES: usize = 64 * 1024 * 1024;

    fn test_engine_config_for(root: &std::path::Path) -> EngineConfig {
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

    #[test]
    fn test_engine_config() {
        let config = EngineConfig::default();
        let config_str = toml::to_string(&config).unwrap();
        assert!(config_str.contains("storage_root"));
        assert!(config_str.contains("index_swap_file"));
        assert!(config_str.contains("data_swap_file"));
        assert!(config_str.contains("log_dir"));
        assert!(config_str.contains("log_file_stem"));
        assert!(!config_str.contains("max_io_depth"));
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
            let resource = LockResource::CatalogNamespace;
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
                smol::Timer::after(Duration::from_millis(1)).await;
            }
            assert!(waiter_seen);

            drop(session);
            let err = wait_task.await.unwrap_err();
            assert_eq!(
                err.operation_error(),
                Some(OperationError::LockWaiterReleased)
            );
            assert_eq!(engine.lock_manager().release_owner(blocking_owner), 1);
        });
    }

    #[test]
    fn test_engine_component_order_uses_shared_storage_and_evictor_workers() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();

            assert_eq!(
                engine.components().component_names(),
                vec![
                    "fs",
                    "disk_pool",
                    "meta_pool",
                    "index_pool",
                    "mem_pool",
                    "fs_workers",
                    "shared_pool_evictor_workers",
                    "lock_manager",
                    "catalog",
                    "trx_sys",
                    "trx_sys_workers",
                ]
            );
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

            let table_stats = fs_stats_handle_identity(&engine.table_fs);
            let mem_stats = pool_stats_handle_identity(&engine.mem_pool);
            let index_stats = pool_stats_handle_identity(&engine.index_pool);

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

            assert_eq!(engine.table_fs.configured_io_depth(), 7);
            assert_eq!(
                engine.mem_pool.io_backend_stats(),
                engine.table_fs.io_backend_stats()
            );
            assert_eq!(
                engine.index_pool.io_backend_stats(),
                engine.table_fs.io_backend_stats()
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
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::Shutdown));

            let err = match engine.new_ref() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::Shutdown));
        });
    }

    #[test]
    fn test_engine_ref_rejected_once_shutdown_begins() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let table_id = table1(&engine).await;
            let engine_ref = engine.new_ref().unwrap();

            let err = match engine.shutdown() {
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
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::Shutdown));

            let err = match engine_ref.get_table(table_id).await {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::Shutdown));

            let err = match engine.new_ref() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::Shutdown));

            drop(engine_ref);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_busy_until_refs_drop() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();

            let err = match engine.shutdown() {
                Ok(_) => panic!("expected busy shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::ShutdownBusy));
            assert_eq!(err.downcast_ref::<usize>().copied(), Some(1));

            let err = match engine.new_session() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::Shutdown));

            let err = match session.begin_trx() {
                Ok(_) => panic!("expected shutdown error"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), ErrorKind::Lifecycle);
            assert_eq!(err.lifecycle_error(), Some(LifecycleError::Shutdown));
            assert!(!session.in_trx());

            drop(session);
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

            std::thread::scope(|scope| {
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
            assert!(session.in_trx());
            let err = match session.begin_trx() {
                Ok(_) => panic!("expected existing transaction error"),
                Err(err) => err,
            };
            let kind = err.kind();
            let operation_error = err.operation_error();

            trx.rollback().await.unwrap();
            assert!(!session.in_trx());
            assert_eq!(kind, ErrorKind::Operation);
            assert_eq!(operation_error, Some(OperationError::ExistingTransaction));
        });
    }

    #[test]
    fn test_same_session_reuse_after_commit() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.add_pseudo_redo_log_entry();
            let cts = trx.commit().await.unwrap();
            assert!(cts > TrxID::new(0));
            assert!(!session.in_trx());

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
            assert!(!session.in_trx());

            let trx = session.begin_trx().unwrap();
            trx.rollback().await.unwrap();
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
            assert!(!session.in_trx());

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

            assert!(session1.in_trx());
            assert!(session2.in_trx());

            trx1.rollback().await.unwrap();
            trx2.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_engine_shutdown_keeps_external_table_refs_valid() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = test_engine_config_for(root.path()).build().await.unwrap();
            let table_id = table1(&engine).await;
            let table = engine.catalog().get_table(table_id).await.unwrap();

            engine.shutdown().unwrap();

            assert_eq!(table.table_id(), table_id);
            assert!(engine.catalog().get_table(table_id).await.is_some());
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

            std::thread::scope(|scope| {
                let shutdown_barrier = Arc::clone(&barrier);
                let shutdown_handle = scope.spawn(move || {
                    shutdown_barrier.wait();
                    engine.shutdown()
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

            let (trx_sys, startup) = TrxSysConfig::default()
                .log_dir(&log_dir)
                .log_file_stem("pending-startup-cleanup")
                .prepare(
                    engine.meta_pool.clone_inner(),
                    engine.index_pool.clone_inner(),
                    engine.mem_pool.clone_inner(),
                    engine.table_fs.clone(),
                    engine.disk_pool.clone_inner(),
                    engine.catalog.clone(),
                )
                .await
                .unwrap();
            drop(startup);
            drop(trx_sys);
        });
    }
}
