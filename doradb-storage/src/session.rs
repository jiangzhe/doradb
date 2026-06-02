use crate::buffer::page::VersionedPageID;
use crate::buffer::{BufferPool, PoolGuards, PoolRole};
use crate::catalog::{IndexNo, IndexSpec, TableSpec};
use crate::engine::{EngineInner, EngineRef, WeakEngineRef};
use crate::error::{LifecycleError, OperationError, Result};
use crate::id::{RowID, SessionID, TableID, TrxID};
use crate::lock::{LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource};
use crate::quiescent::QuiescentGuard;
use crate::table::Table;
use crate::trx::ActiveTrx;
use dashmap::DashMap;
use error_stack::Report;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Shared session-level DDL admission context.
pub(crate) struct SessionDdlContext {
    pub(crate) engine: EngineRef,
    pub(crate) pool_guards: PoolGuards,
    pub(crate) owner: LockOwner,
    pub(crate) owner_group: LockOwnerGroup,
}

impl SessionDdlContext {
    #[inline]
    pub(crate) fn new(session: &Session) -> Result<Self> {
        let pin = session.pin("session DDL")?;
        if pin.in_trx()? {
            return Err(Report::new(OperationError::NotSupported)
                .attach("implicit commit due to DDL")
                .into());
        }
        let id = pin.id();
        let pool_guards = pin.state.pool_guards().clone();
        Ok(Self {
            engine: pin.engine,
            pool_guards,
            owner: LockOwner::Session(id),
            owner_group: LockOwnerGroup::Session(id),
        })
    }
}

/// Weak, non-cloneable public session capability bound to one engine instance.
///
/// The engine owns the strong session state in its internal session registry.
/// Public session operations upgrade weak engine reachability internally, pin
/// the registry-owned state for one operation, and release registry/admission
/// guards before async work.
pub struct Session {
    id: SessionID,
    engine: WeakEngineRef,
    closed: AtomicBool,
}

impl Session {
    #[inline]
    pub(crate) fn new(engine: WeakEngineRef, id: SessionID) -> Self {
        Session {
            id,
            engine,
            closed: AtomicBool::new(false),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.id
    }

    /// Returns a pinned operation view of this session.
    #[inline]
    pub(crate) fn pin(&self, operation: &'static str) -> Result<SessionPin> {
        if self.closed.load(Ordering::Acquire) {
            return Err(closed_session_error(self.id, operation));
        }
        let engine = self.engine.upgrade(operation)?;
        let admission = engine.acquire_admission()?;
        let state = engine.session_registry.pin_running(self.id, operation)?;
        drop(admission);
        Ok(SessionPin { engine, state })
    }

    /// Get a user-table runtime handle through this session's engine.
    #[inline]
    pub async fn get_table(&self, table_id: TableID) -> Result<Arc<Table>> {
        let pin = self.pin("get table")?;
        pin.engine.get_table(table_id).await
    }

    /// Returns whether the session currently owns an active transaction.
    #[inline]
    pub fn in_trx(&self) -> Result<bool> {
        const OPERATION: &str = "query session transaction state";
        if self.closed.load(Ordering::Acquire) {
            return Err(closed_session_error(self.id, OPERATION));
        }
        let engine = self.engine.upgrade(OPERATION)?;
        engine.ensure_admission_open_for_query(OPERATION)?;
        let state = engine.session_registry.pin_running(self.id, OPERATION)?;
        state.in_trx(OPERATION)
    }

    /// Checked session transaction state query for internal operation paths.
    #[inline]
    pub(crate) fn checked_in_trx(&self, operation: &'static str) -> Result<bool> {
        self.pin(operation)?.in_trx()
    }

    /// Begin a new transaction if the session is currently idle.
    #[inline]
    pub fn begin_trx(&mut self) -> Result<ActiveTrx> {
        let pin = self.pin("begin transaction")?;
        let engine = pin.engine.clone();
        pin.state
            .begin_trx(|| engine.trx_sys.begin_trx(engine.clone(), &pin.state))
    }

    /// Close this session when it has no active transaction.
    #[inline]
    pub async fn close(&mut self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let engine = self.engine.upgrade("close session")?;
        let _admission = engine.acquire_admission()?;
        engine.session_registry.close_idle(self.id)?;
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    /// Create a new table.
    #[inline]
    pub async fn create_table(
        &mut self,
        table_spec: TableSpec,
        index_specs: Vec<IndexSpec>,
    ) -> Result<TableID> {
        crate::catalog::create_table_for_session(self, table_spec, index_specs).await
    }

    /// Build and publish a new secondary index for an existing user table.
    #[inline]
    pub async fn create_index(
        &mut self,
        table_id: TableID,
        index_spec: IndexSpec,
    ) -> Result<IndexNo> {
        crate::catalog::create_index_for_session(self, table_id, index_spec).await
    }

    /// Logically drop an active secondary index from an existing user table.
    #[inline]
    pub async fn drop_index(&mut self, table_id: TableID, index_no: IndexNo) -> Result<()> {
        crate::catalog::drop_index_for_session(self, table_id, index_no).await
    }

    /// Logically drop an existing user table.
    #[inline]
    pub async fn drop_table(&mut self, table_id: TableID) -> Result<()> {
        crate::catalog::drop_table_for_session(self, table_id).await
    }

    /// Acquires an explicit session-lifetime table lock.
    #[inline]
    pub async fn lock_table(&self, table_id: TableID, mode: LockMode) -> Result<()> {
        mode.validate_explicit_table_lock()?;
        let pin = self.pin("lock explicit table")?;
        let engine = pin.engine;
        engine
            .catalog()
            .validate_user_table_live(table_id, "lock explicit table")
            .await?;
        let lock_manager = engine.lock_manager();
        let owner = LockOwner::Session(self.id());
        let owner_group = LockOwnerGroup::Session(self.id());
        let (mut metadata_guard, mut data_guard) = lock_manager
            .acquire_grouped_table_locks(table_id, mode, owner, owner_group)
            .await?;
        engine
            .catalog()
            .validate_user_table_live(table_id, "lock explicit table")
            .await?;
        if let Some(guard) = data_guard.as_mut() {
            guard.disarm();
        }
        if let Some(guard) = metadata_guard.as_mut() {
            guard.disarm();
        }
        Ok(())
    }

    /// Releases an explicit session-lifetime table lock when no transaction is active.
    #[inline]
    pub fn unlock_table(&self, table_id: TableID) -> Result<()> {
        let pin = self.pin("unlock explicit table")?;
        if pin.in_trx()? {
            return Err(Report::new(OperationError::NotSupported)
                .attach("unlock table while session has an active transaction")
                .into());
        }
        let owner = LockOwner::Session(self.id());
        let lock_manager = pin.engine.lock_manager();
        lock_manager.release(LockResource::TableData(table_id), owner);
        lock_manager.release(LockResource::TableMetadata(table_id), owner);
        Ok(())
    }
}

impl Drop for Session {
    #[inline]
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(engine) = self.engine.upgrade_for_cleanup() {
            engine.session_registry.abandon(self.id);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) trait SessionTestExt {
        fn pool_guards(&self) -> PoolGuards;
        fn engine(&self) -> EngineRef;
        fn last_cts(&self) -> TrxID;
        fn load_active_insert_page(
            &mut self,
            table_id: TableID,
        ) -> Option<(VersionedPageID, RowID)>;
        fn save_active_insert_page(
            &mut self,
            table_id: TableID,
            page_id: VersionedPageID,
            row_id: RowID,
        );
    }

    impl SessionTestExt for Session {
        #[inline]
        fn pool_guards(&self) -> PoolGuards {
            self.pin("test session pool guards")
                .expect("test session must be running")
                .state
                .pool_guards()
                .clone()
        }

        #[inline]
        fn engine(&self) -> EngineRef {
            self.pin("test session engine")
                .expect("test session must be running")
                .engine
        }

        #[inline]
        fn last_cts(&self) -> TrxID {
            let pin = self
                .pin("test session last commit timestamp")
                .expect("test session must be running");
            TrxID::new(pin.state.last_cts.load(Ordering::SeqCst))
        }

        #[inline]
        fn load_active_insert_page(
            &mut self,
            table_id: TableID,
        ) -> Option<(VersionedPageID, RowID)> {
            self.pin("load active insert page")
                .expect("test session must be running")
                .state
                .load_active_insert_page(table_id)
        }

        #[inline]
        fn save_active_insert_page(
            &mut self,
            table_id: TableID,
            page_id: VersionedPageID,
            row_id: RowID,
        ) {
            self.pin("save active insert page")
                .expect("test session must be running")
                .state
                .save_active_insert_page(table_id, page_id, row_id);
        }
    }
}

/// One operation-scoped strong pin of a registry-owned session.
pub(crate) struct SessionPin {
    pub(crate) engine: EngineRef,
    pub(crate) state: Arc<SessionState>,
}

impl SessionPin {
    /// Returns the engine-local session identity.
    #[inline]
    pub(crate) fn id(&self) -> SessionID {
        self.state.id()
    }

    /// Returns a cloned guard bundle for this pinned operation.
    #[inline]
    pub(crate) fn pool_guards(&self) -> PoolGuards {
        self.state.pool_guards().clone()
    }

    /// Returns whether the pinned session is in an active transaction.
    #[inline]
    pub(crate) fn in_trx(&self) -> Result<bool> {
        self.state.in_trx("query pinned session transaction state")
    }
}

/// Engine-owned registry for strong session state.
pub(crate) struct SessionRegistry {
    entries: DashMap<SessionID, Arc<SessionState>>,
}

impl SessionRegistry {
    /// Create an empty session registry.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Insert a new registry-owned session state.
    #[inline]
    pub(crate) fn insert(&self, state: Arc<SessionState>) {
        let old = self.entries.insert(state.id(), state);
        debug_assert!(old.is_none(), "session ids are monotonic and never reused");
    }

    /// Create and insert one registry-owned session state.
    #[inline]
    pub(crate) fn create_session(
        &self,
        engine: &Arc<EngineInner>,
        engine_ref: EngineRef,
        id: SessionID,
    ) -> Session {
        let state = Arc::new(SessionState::new(engine_ref, id));
        self.insert(state);
        Session::new(WeakEngineRef::new(engine), id)
    }

    /// Pin a running session state for one operation.
    #[inline]
    pub(crate) fn pin_running(
        &self,
        id: SessionID,
        operation: &'static str,
    ) -> Result<Arc<SessionState>> {
        let state = self
            .session_state(id)
            .ok_or_else(|| stale_session_error(id, operation))?;
        state.ensure_running(operation)?;
        Ok(state)
    }

    /// Close an idle session and remove it from the registry.
    #[inline]
    pub(crate) fn close_idle(&self, id: SessionID) -> Result<()> {
        self.modify_session_checked(id, "close session", SessionState::close_idle)
    }

    /// Best-effort nonblocking abandonment from public session `Drop`.
    #[inline]
    pub(crate) fn abandon(&self, id: SessionID) {
        self.modify_session(id, SessionState::abandon);
    }

    /// Apply session cleanup after a transaction commits.
    #[inline]
    pub(crate) fn finish_trx_commit(&self, id: SessionID, trx_id: TrxID, cts: TrxID) {
        self.modify_session(id, |state| state.finish_trx_commit(trx_id, cts));
    }

    /// Apply session cleanup after a transaction rolls back.
    #[inline]
    pub(crate) fn finish_trx_rollback(&self, id: SessionID, trx_id: TrxID) {
        self.modify_session(id, |state| state.finish_trx_rollback(trx_id));
    }

    /// Remove idle and abandoned-idle sessions during engine shutdown.
    #[inline]
    pub(crate) fn shutdown_idle(&self) {
        let sessions = self
            .entries
            .iter()
            .map(|entry| (*entry.key(), Arc::clone(entry.value())))
            .collect::<Vec<_>>();
        let removed = sessions
            .into_iter()
            .filter_map(|(id, state)| {
                state
                    .shutdown_removal()
                    .then(|| self.entries.remove(&id).map(|(_, state)| state))
                    .flatten()
            })
            .collect::<Vec<_>>();
        drop(removed);
    }

    /// Returns the number of registry-owned sessions.
    #[inline]
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    fn session_state(&self, id: SessionID) -> Option<Arc<SessionState>> {
        self.entries.get(&id).map(|entry| Arc::clone(entry.value()))
    }

    #[inline]
    fn modify_session(&self, id: SessionID, modify: impl FnOnce(&SessionState) -> SessionRemoval) {
        let Some(state) = self.session_state(id) else {
            return;
        };
        self.apply_removal(id, modify(&state));
    }

    #[inline]
    fn modify_session_checked(
        &self,
        id: SessionID,
        operation: &'static str,
        modify: impl FnOnce(&SessionState) -> Result<SessionRemoval>,
    ) -> Result<()> {
        let state = self
            .session_state(id)
            .ok_or_else(|| stale_session_error(id, operation))?;
        let removal = modify(&state)?;
        self.apply_removal(id, removal);
        Ok(())
    }

    #[inline]
    fn apply_removal(&self, id: SessionID, removal: SessionRemoval) {
        let removed = match removal {
            SessionRemoval::Remove => self.entries.remove(&id).map(|(_, state)| state),
            SessionRemoval::Keep => None,
        };
        drop(removed);
    }
}

/// Shared mutable state referenced by transactions started from one [`Session`].
pub(crate) struct SessionState {
    id: SessionID,
    pool_guards: PoolGuards,
    lock_manager: QuiescentGuard<LockManager>,
    lifecycle: Mutex<SessionLifecycle>,
    last_cts: AtomicU64,
    active_insert_pages: Mutex<HashMap<TableID, (VersionedPageID, RowID)>>,
}

impl SessionState {
    /// Create a new session state and populate its default pool guards.
    #[inline]
    pub(crate) fn new(engine_ref: EngineRef, id: SessionID) -> Self {
        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, engine_ref.meta_pool.pool_guard())
            .push(PoolRole::Index, engine_ref.index_pool.pool_guard())
            .push(PoolRole::Mem, engine_ref.mem_pool.pool_guard())
            .push(PoolRole::Disk, engine_ref.disk_pool.pool_guard())
            .build();
        let lock_manager = engine_ref.lock_manager().clone();
        SessionState {
            id,
            pool_guards,
            lock_manager,
            lifecycle: Mutex::new(SessionLifecycle::RunningIdle),
            last_cts: AtomicU64::new(0),
            active_insert_pages: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.id
    }

    /// Returns the guard bundle owned by this session state.
    #[inline]
    pub fn pool_guards(&self) -> &PoolGuards {
        &self.pool_guards
    }

    #[inline]
    fn ensure_running(&self, operation: &'static str) -> Result<()> {
        match *self.lifecycle.lock() {
            SessionLifecycle::RunningIdle | SessionLifecycle::RunningActive { .. } => Ok(()),
            SessionLifecycle::AbandonedIdle
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::Closed => Err(closed_session_error(self.id, operation)),
        }
    }

    #[inline]
    fn in_trx(&self, operation: &'static str) -> Result<bool> {
        match *self.lifecycle.lock() {
            SessionLifecycle::RunningIdle => Ok(false),
            SessionLifecycle::RunningActive { .. } => Ok(true),
            SessionLifecycle::AbandonedIdle
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::Closed => Err(closed_session_error(self.id, operation)),
        }
    }

    #[inline]
    fn begin_trx(&self, begin: impl FnOnce() -> ActiveTrx) -> Result<ActiveTrx> {
        let mut lifecycle = self.lifecycle.lock();
        match *lifecycle {
            SessionLifecycle::RunningIdle => {
                let trx = begin();
                *lifecycle = SessionLifecycle::RunningActive {
                    trx_id: trx.trx_id(),
                };
                Ok(trx)
            }
            SessionLifecycle::RunningActive { trx_id } => {
                Err(Report::new(OperationError::ExistingTransaction)
                    .attach(format!("session_id={}, trx_id={trx_id}", self.id))
                    .into())
            }
            SessionLifecycle::AbandonedIdle
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::Closed => Err(closed_session_error(self.id, "begin transaction")),
        }
    }

    #[inline]
    fn close_idle(&self) -> Result<SessionRemoval> {
        let mut lifecycle = self.lifecycle.lock();
        match *lifecycle {
            SessionLifecycle::RunningIdle | SessionLifecycle::AbandonedIdle => {
                *lifecycle = SessionLifecycle::Closed;
                self.release_session_locks();
                Ok(SessionRemoval::Remove)
            }
            SessionLifecycle::RunningActive { trx_id } => {
                Err(Report::new(OperationError::ExistingTransaction)
                    .attach(format!(
                        "close session with active transaction: session_id={}, trx_id={trx_id}",
                        self.id
                    ))
                    .into())
            }
            SessionLifecycle::AbandonedActive { trx_id } => Err(Report::new(
                OperationError::ExistingTransaction,
            )
            .attach(format!(
                "close abandoned session with active transaction: session_id={}, trx_id={trx_id}",
                self.id
            ))
            .into()),
            SessionLifecycle::Closed => Ok(SessionRemoval::Remove),
        }
    }

    #[inline]
    fn abandon(&self) -> SessionRemoval {
        let mut lifecycle = self.lifecycle.lock();
        match *lifecycle {
            SessionLifecycle::RunningIdle | SessionLifecycle::AbandonedIdle => {
                *lifecycle = SessionLifecycle::AbandonedIdle;
                self.release_session_locks();
                SessionRemoval::Remove
            }
            SessionLifecycle::RunningActive { trx_id }
            | SessionLifecycle::AbandonedActive { trx_id } => {
                *lifecycle = SessionLifecycle::AbandonedActive { trx_id };
                SessionRemoval::Keep
            }
            SessionLifecycle::Closed => SessionRemoval::Remove,
        }
    }

    /// Finish this session's active-transaction lifecycle after commit.
    #[inline]
    fn finish_trx_commit(&self, trx_id: TrxID, cts: TrxID) -> SessionRemoval {
        self.finish_trx(trx_id, || {
            self.last_cts.store(cts.as_u64(), Ordering::SeqCst);
        })
    }

    /// Finish this session's active-transaction lifecycle after rollback.
    #[inline]
    fn finish_trx_rollback(&self, trx_id: TrxID) -> SessionRemoval {
        self.finish_trx(trx_id, || {})
    }

    #[inline]
    fn finish_trx(&self, trx_id: TrxID, mut on_finish: impl FnMut()) -> SessionRemoval {
        let mut lifecycle = self.lifecycle.lock();
        match *lifecycle {
            SessionLifecycle::RunningActive { trx_id: active } if active == trx_id => {
                on_finish();
                *lifecycle = SessionLifecycle::RunningIdle;
                SessionRemoval::Keep
            }
            SessionLifecycle::AbandonedActive { trx_id: active } if active == trx_id => {
                on_finish();
                *lifecycle = SessionLifecycle::Closed;
                self.release_session_locks();
                SessionRemoval::Remove
            }
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed
            | SessionLifecycle::RunningActive { .. }
            | SessionLifecycle::AbandonedActive { .. } => SessionRemoval::Keep,
        }
    }

    #[inline]
    fn shutdown_removal(&self) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        match *lifecycle {
            SessionLifecycle::RunningIdle | SessionLifecycle::AbandonedIdle => {
                *lifecycle = SessionLifecycle::Closed;
                self.release_session_locks();
                true
            }
            SessionLifecycle::Closed => true,
            SessionLifecycle::RunningActive { .. } | SessionLifecycle::AbandonedActive { .. } => {
                false
            }
        }
    }

    /// Remove and return the cached insert page for a table, if present.
    #[inline]
    pub fn load_active_insert_page(&self, table_id: TableID) -> Option<(VersionedPageID, RowID)> {
        let mut g = self.active_insert_pages.lock();
        g.remove(&table_id)
    }

    /// Cache the active insert page for a table.
    #[inline]
    pub fn save_active_insert_page(
        &self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        let mut g = self.active_insert_pages.lock();
        let res = g.insert(table_id, (page_id, row_id));
        debug_assert!(res.is_none());
    }

    #[inline]
    fn release_session_locks(&self) {
        self.lock_manager.release_owner(LockOwner::Session(self.id));
    }
}

impl Drop for SessionState {
    #[inline]
    fn drop(&mut self) {
        self.release_session_locks();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionLifecycle {
    RunningIdle,
    RunningActive { trx_id: TrxID },
    AbandonedIdle,
    AbandonedActive { trx_id: TrxID },
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionRemoval {
    Keep,
    Remove,
}

/// Transitional session reference retained by public transaction handles.
///
/// This handle deliberately owns engine runtime liveness but not session
/// lifecycle ownership. An active transaction must keep a strong [`EngineRef`]
/// so commit, rollback, logging, lock cleanup, and registry completion can still
/// reach engine runtime state, and so explicit shutdown waits for active
/// transactions to finish. The session state itself stays registry-owned: the
/// weak session reference prevents a transaction from extending public session
/// lifetime, allowing a dropped public session to become abandoned and be
/// removed when this transaction reaches commit or rollback.
pub(crate) struct TrxSessionRef {
    /// Strong runtime handle kept until the transaction reaches a terminal path.
    engine: EngineRef,
    /// Registry lookup key for terminal session cleanup.
    session_id: SessionID,
    /// Active transaction id used to avoid finishing a replaced session state.
    trx_id: TrxID,
    /// Optional reachability for session-local caches, not lifecycle ownership.
    session: Weak<SessionState>,
    /// Cached guards needed by transaction work even if session state is gone.
    pool_guards: PoolGuards,
}

impl TrxSessionRef {
    /// Create a transaction session reference without strongly owning session state.
    #[inline]
    pub(crate) fn new(engine: EngineRef, session: &Arc<SessionState>, trx_id: TrxID) -> Self {
        Self {
            engine,
            session_id: session.id(),
            trx_id,
            session: Arc::downgrade(session),
            pool_guards: session.pool_guards().clone(),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub(crate) fn session_id(&self) -> SessionID {
        self.session_id
    }

    /// Returns the crate-private engine runtime handle.
    #[inline]
    pub(crate) fn engine(&self) -> &EngineRef {
        &self.engine
    }

    /// Returns the cloned session pool guards retained by this transaction.
    #[inline]
    pub(crate) fn pool_guards(&self) -> &PoolGuards {
        &self.pool_guards
    }

    /// Remove and return the cached insert page for a table, if session state remains.
    #[inline]
    pub(crate) fn load_active_insert_page(
        &self,
        table_id: TableID,
    ) -> Option<(VersionedPageID, RowID)> {
        self.session
            .upgrade()
            .and_then(|session| session.load_active_insert_page(table_id))
    }

    /// Cache the active insert page if session state remains.
    #[inline]
    pub(crate) fn save_active_insert_page(
        &self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        if let Some(session) = self.session.upgrade() {
            session.save_active_insert_page(table_id, page_id, row_id);
        }
    }

    /// Mark the owning session committed.
    #[inline]
    pub(crate) fn commit(&self, cts: TrxID) {
        self.engine
            .session_registry
            .finish_trx_commit(self.session_id, self.trx_id, cts);
    }

    /// Mark the owning session rolled back.
    #[inline]
    pub(crate) fn rollback(&self) {
        self.engine
            .session_registry
            .finish_trx_rollback(self.session_id, self.trx_id);
    }
}

#[inline]
fn closed_session_error(id: SessionID, operation: &'static str) -> crate::error::Error {
    Report::new(OperationError::NotSupported)
        .attach(format!(
            "{operation}: session is closed or abandoned: session_id={id}"
        ))
        .into()
}

#[inline]
fn stale_session_error(id: SessionID, operation: &'static str) -> crate::error::Error {
    Report::new(LifecycleError::Shutdown)
        .attach(format!("{operation}: session is missing: session_id={id}"))
        .into()
}
