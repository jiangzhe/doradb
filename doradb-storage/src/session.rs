use crate::buffer::page::VersionedPageID;
use crate::buffer::{BufferPool, PoolGuards, PoolRole};
use crate::catalog::{IndexNo, IndexSpec, TableSpec};
use crate::engine::{EngineInner, EngineRef, WeakEngineRef};
use crate::error::{LifecycleError, OperationError, Result};
use crate::id::{RowID, SessionID, TableID, TrxID};
use crate::lock::{LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource};
use crate::quiescent::QuiescentGuard;
use crate::table::{CheckpointOutcome, CheckpointReadiness, SecondaryMemIndexCleanupStats, Table};
use crate::trx::{
    StartedTransaction, Transaction, TrxCleanupReason, TrxEntry, TrxEntryState,
    transaction_discarded_err,
};
use dashmap::DashMap;
use error_stack::Report;
use event_listener::{Event, Listener, listener};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

/// Shared session-level DDL admission context.
pub(crate) struct SessionDdlContext {
    pub(crate) engine: EngineRef,
    pub(crate) pool_guards: PoolGuards,
    pub(crate) owner: LockOwner,
    pub(crate) owner_group: LockOwnerGroup,
}

impl SessionDdlContext {
    #[inline]
    pub(crate) fn new(session: &SessionPin) -> Result<Self> {
        if session.in_trx("session DDL")? {
            return Err(Report::new(OperationError::NotSupported)
                .attach("implicit commit due to DDL")
                .into());
        }
        let id = session.id();
        let pool_guards = session.pool_guards();
        Ok(Self {
            engine: session.engine.clone(),
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

    /// Begin a new transaction if the session is currently idle.
    #[inline]
    pub fn begin_trx(&mut self) -> Result<Transaction> {
        let session = self.pin("begin transaction")?;
        session.begin_trx("begin transaction")
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
        let session = self.pin("session DDL")?;
        crate::catalog::create_table_for_session(session, table_spec, index_specs).await
    }

    /// Build and publish a new secondary index for an existing user table.
    #[inline]
    pub async fn create_index(
        &mut self,
        table_id: TableID,
        index_spec: IndexSpec,
    ) -> Result<IndexNo> {
        let session = self.pin("session DDL")?;
        crate::catalog::create_index_for_session(session, table_id, index_spec).await
    }

    /// Logically drop an active secondary index from an existing user table.
    #[inline]
    pub async fn drop_index(&mut self, table_id: TableID, index_no: IndexNo) -> Result<()> {
        let session = self.pin("session DDL")?;
        crate::catalog::drop_index_for_session(session, table_id, index_no).await
    }

    /// Logically drop an existing user table.
    #[inline]
    pub async fn drop_table(&mut self, table_id: TableID) -> Result<()> {
        let session = self.pin("session DDL")?;
        crate::catalog::drop_table_for_session(session, table_id).await
    }

    /// Freeze row pages for an existing user table and return approximate non-deleted rows visited.
    #[inline]
    pub async fn freeze_table(&self, table_id: TableID, max_rows: usize) -> Result<usize> {
        let session = self.pin("freeze table")?;
        let table = session.resolve_user_table(table_id, "freeze table").await?;
        table.freeze(session, max_rows).await
    }

    /// Report whether an existing user table checkpoint can safely publish now.
    #[inline]
    pub fn table_checkpoint_readiness(&self, table_id: TableID) -> Result<CheckpointReadiness> {
        let session = self.pin("check checkpoint readiness")?;
        let table =
            session.resolve_existing_user_table_now(table_id, "check checkpoint readiness")?;
        table.checkpoint_readiness(&session)
    }

    /// Persist eligible row-store and cold-delete state for an existing user table.
    #[inline]
    pub async fn checkpoint_table(&mut self, table_id: TableID) -> Result<CheckpointOutcome> {
        let session = self.pin("checkpoint table")?;
        let table = session
            .resolve_existing_user_table(table_id, "checkpoint table")
            .await?;
        table.checkpoint(session).await
    }

    /// Returns total number of hot row pages for an existing user table.
    #[inline]
    pub async fn total_row_pages(&self, table_id: TableID) -> Result<usize> {
        let session = self.pin("count table row pages")?;
        let table = session
            .resolve_user_table(table_id, "count table row pages")
            .await?;
        let guards = session.pool_guards();
        Ok(table.total_row_pages(&guards).await)
    }

    /// Full-scan cleanup for an existing user table's secondary MemIndex entries.
    #[inline]
    pub async fn cleanup_secondary_mem_indexes(
        &mut self,
        table_id: TableID,
        clean_live_entries: bool,
    ) -> Result<SecondaryMemIndexCleanupStats> {
        let session = self.pin("cleanup secondary mem indexes")?;
        let table = session
            .resolve_user_table(table_id, "cleanup secondary mem indexes")
            .await?;
        table
            .cleanup_secondary_mem_indexes(session, clean_live_entries)
            .await
    }

    /// Acquires an explicit session-lifetime table lock.
    #[inline]
    pub async fn lock_table(&self, table_id: TableID, mode: LockMode) -> Result<()> {
        mode.validate_explicit_table_lock()?;
        let session = self.pin("lock explicit table")?;
        session.lock_table(table_id, mode).await
    }

    /// Releases an explicit session-lifetime table lock when no transaction is active.
    #[inline]
    pub fn unlock_table(&self, table_id: TableID) -> Result<()> {
        let session = self.pin("unlock explicit table")?;
        session.unlock_table(table_id)
    }
}

#[inline]
fn table_not_found_error(table_id: TableID, operation: &'static str) -> crate::error::Error {
    Report::new(OperationError::TableNotFound)
        .attach(format!("operation={operation}, table_id={table_id}"))
        .into()
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

    /// Begin a new transaction from this already pinned session operation.
    #[inline]
    pub(crate) fn begin_trx(&self, operation: &'static str) -> Result<Transaction> {
        let engine = &self.engine;
        self.state
            .begin_trx(operation, || engine.trx_sys.begin_trx(engine, &self.state))
    }

    /// Returns whether the pinned session is in an active transaction.
    #[inline]
    pub(crate) fn in_trx(&self, operation: &'static str) -> Result<bool> {
        self.state.in_trx(operation)
    }

    #[inline]
    pub(crate) async fn resolve_user_table(
        &self,
        table_id: TableID,
        operation: &'static str,
    ) -> Result<Arc<Table>> {
        if let Some(table) = self.state.cached_user_table(table_id) {
            table.check_foreground_live(operation)?;
            return Ok(table);
        }
        let table = self
            .engine
            .catalog()
            .validate_user_table_live(table_id, operation)
            .await?;
        self.state.cache_user_table(&table);
        Ok(table)
    }

    #[inline]
    pub(crate) async fn resolve_existing_user_table(
        &self,
        table_id: TableID,
        operation: &'static str,
    ) -> Result<Arc<Table>> {
        if let Some(table) = self.state.cached_user_table(table_id) {
            return Ok(table);
        }
        let table = self
            .engine
            .catalog()
            .get_table(table_id)
            .await
            .ok_or_else(|| table_not_found_error(table_id, operation))?;
        self.state.cache_user_table(&table);
        Ok(table)
    }

    #[inline]
    pub(crate) fn resolve_existing_user_table_now(
        &self,
        table_id: TableID,
        operation: &'static str,
    ) -> Result<Arc<Table>> {
        if let Some(table) = self.state.cached_user_table(table_id) {
            return Ok(table);
        }
        let table = self
            .engine
            .catalog()
            .get_table_now(table_id)
            .ok_or_else(|| table_not_found_error(table_id, operation))?;
        self.state.cache_user_table(&table);
        Ok(table)
    }

    /// Acquires an explicit session-lifetime table lock from this pinned session.
    #[inline]
    pub(crate) async fn lock_table(&self, table_id: TableID, mode: LockMode) -> Result<()> {
        let session_id = self.id();
        let engine = &self.engine;
        engine
            .catalog()
            .validate_user_table_live(table_id, "lock explicit table")
            .await?;
        let lock_manager = engine.lock_manager();
        let owner = LockOwner::Session(session_id);
        let owner_group = LockOwnerGroup::Session(session_id);
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

    /// Releases an explicit session-lifetime table lock from this pinned session.
    #[inline]
    pub(crate) fn unlock_table(&self, table_id: TableID) -> Result<()> {
        if self.in_trx("unlock explicit table")? {
            return Err(Report::new(OperationError::NotSupported)
                .attach("unlock table while session has an active transaction")
                .into());
        }
        let owner = LockOwner::Session(self.id());
        let lock_manager = self.engine.lock_manager();
        lock_manager.release(LockResource::TableData(table_id), owner);
        lock_manager.release(LockResource::TableMetadata(table_id), owner);
        Ok(())
    }
}

/// Engine-owned registry for strong session state.
pub(crate) struct SessionRegistry {
    entries: DashMap<SessionID, Arc<SessionState>>,
    trx_changed: Event,
    trx_change_epoch: AtomicU64,
}

impl SessionRegistry {
    /// Create an empty session registry.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            entries: DashMap::new(),
            trx_changed: Event::new(),
            trx_change_epoch: AtomicU64::new(0),
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
        self.notify_trx_changed();
    }

    /// Apply session cleanup after a transaction commits.
    #[inline]
    pub(crate) fn finish_trx_commit(&self, id: SessionID, trx_id: TrxID, cts: TrxID) {
        self.modify_session(id, |state| state.finish_trx_commit(trx_id, cts));
        self.notify_trx_changed();
    }

    /// Apply session cleanup after a transaction rolls back.
    #[inline]
    pub(crate) fn finish_trx_rollback(&self, id: SessionID, trx_id: TrxID) {
        self.modify_session(id, |state| state.finish_trx_rollback(trx_id));
        self.notify_trx_changed();
    }

    /// Resolve an active transaction by session and transaction id.
    #[inline]
    pub(crate) fn resolve_trx(
        &self,
        session_id: SessionID,
        trx_id: TrxID,
        operation: &'static str,
    ) -> Result<(Arc<TrxEntry>, Arc<SessionState>)> {
        let session = self
            .session_state(session_id)
            .ok_or_else(|| stale_session_error(session_id, operation))?;
        let entry = session.resolve_trx(trx_id, operation)?;
        Ok((entry, session))
    }

    /// Mark a public transaction handle abandoned if it still names the active entry.
    #[inline]
    pub(crate) fn abandon_trx_handle(&self, session_id: SessionID, trx_id: TrxID) -> bool {
        let Some(session) = self.session_state(session_id) else {
            return false;
        };
        let abandoned = session.abandon_trx_handle(trx_id);
        if abandoned {
            self.notify_trx_changed();
        }
        abandoned
    }

    /// Queue cleanup for abandoned transactions found during shutdown scanning.
    #[inline]
    pub(crate) fn collect_shutdown_cleanup(&self) -> Vec<(SessionID, TrxID, TrxCleanupReason)> {
        let sessions = self
            .entries
            .iter()
            .map(|entry| (*entry.key(), Arc::clone(entry.value())))
            .collect::<Vec<_>>();
        let mut cleanup = Vec::new();
        for (session_id, session) in sessions {
            if let Some(trx_id) = session.shutdown_cleanup_candidate() {
                cleanup.push((session_id, trx_id, TrxCleanupReason::ShutdownDrain));
            }
        }
        if !cleanup.is_empty() {
            self.notify_trx_changed();
        }
        cleanup
    }

    /// Count registry-owned transactions that still block owner shutdown.
    #[inline]
    pub(crate) fn active_transaction_count(&self) -> usize {
        self.entries
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .filter(|session| session.has_active_transaction())
            .count()
    }

    /// Returns the current transaction lifecycle change epoch.
    #[inline]
    pub(crate) fn trx_change_epoch(&self) -> u64 {
        self.trx_change_epoch.load(Ordering::Acquire)
    }

    /// Wait for any transaction lifecycle change after the observed epoch.
    #[inline]
    pub(crate) fn wait_for_trx_change_since(&self, observed_epoch: u64) {
        loop {
            if self.trx_change_epoch() != observed_epoch {
                return;
            }
            listener!(self.trx_changed => trx_changed);
            if self.trx_change_epoch() != observed_epoch {
                return;
            }
            trx_changed.wait();
        }
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

    #[inline]
    pub(crate) fn notify_trx_changed(&self) {
        self.trx_change_epoch.fetch_add(1, Ordering::AcqRel);
        self.trx_changed.notify(usize::MAX);
    }
}

/// Shared mutable state referenced by transactions started from one [`Session`].
pub(crate) struct SessionState {
    id: SessionID,
    pool_guards: PoolGuards,
    lock_manager: QuiescentGuard<LockManager>,
    lifecycle: Mutex<SessionLifecycle>,
    last_cts: AtomicU64,
    table_cache: Mutex<HashMap<TableID, Weak<Table>>>,
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
            table_cache: Mutex::new(HashMap::new()),
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
        match &*self.lifecycle.lock() {
            SessionLifecycle::RunningIdle | SessionLifecycle::RunningActive { .. } => Ok(()),
            SessionLifecycle::AbandonedIdle
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::Closed => Err(closed_session_error(self.id, operation)),
        }
    }

    #[inline]
    fn in_trx(&self, operation: &'static str) -> Result<bool> {
        match &*self.lifecycle.lock() {
            SessionLifecycle::RunningIdle => Ok(false),
            SessionLifecycle::RunningActive { entry } => Ok(entry.in_trx()),
            SessionLifecycle::AbandonedIdle
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::Closed => Err(closed_session_error(self.id, operation)),
        }
    }

    #[inline]
    fn active_transaction_err(
        &self,
        operation: &'static str,
        entry: &TrxEntry,
    ) -> crate::error::Error {
        Report::new(OperationError::ExistingTransaction)
            .attach(format!(
                "{operation}: session_id={}, trx_id={}, state={:?}",
                self.id,
                entry.trx_id(),
                entry.inspect_state()
            ))
            .into()
    }

    #[inline]
    fn begin_trx(
        &self,
        operation: &'static str,
        begin: impl FnOnce() -> StartedTransaction,
    ) -> Result<Transaction> {
        let mut lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningIdle => {}
            SessionLifecycle::RunningActive { entry } => {
                return Err(self.active_transaction_err(operation, entry));
            }
            SessionLifecycle::AbandonedIdle
            | SessionLifecycle::AbandonedActive { .. }
            | SessionLifecycle::Closed => {
                return Err(closed_session_error(self.id, operation));
            }
        }
        let trx = begin();
        *lifecycle = SessionLifecycle::RunningActive {
            entry: Arc::clone(&trx.entry),
        };
        Ok(trx.handle)
    }

    #[inline]
    fn close_idle(&self) -> Result<SessionRemoval> {
        let mut lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningIdle | SessionLifecycle::AbandonedIdle => {}
            SessionLifecycle::RunningActive { entry } => {
                return Err(
                    self.active_transaction_err("close session with active transaction", entry)
                );
            }
            SessionLifecycle::AbandonedActive { entry } => {
                return Err(self.active_transaction_err(
                    "close abandoned session with active transaction",
                    entry,
                ));
            }
            SessionLifecycle::Closed => return Ok(SessionRemoval::Remove),
        }
        *lifecycle = SessionLifecycle::Closed;
        self.release_session_locks();
        Ok(SessionRemoval::Remove)
    }

    #[inline]
    fn abandon(&self) -> SessionRemoval {
        let mut lifecycle = self.lifecycle.lock();
        let entry = match &*lifecycle {
            SessionLifecycle::RunningIdle | SessionLifecycle::AbandonedIdle => {
                *lifecycle = SessionLifecycle::AbandonedIdle;
                self.release_session_locks();
                return SessionRemoval::Remove;
            }
            SessionLifecycle::RunningActive { entry }
            | SessionLifecycle::AbandonedActive { entry } => Arc::clone(entry),
            SessionLifecycle::Closed => return SessionRemoval::Remove,
        };
        *lifecycle = SessionLifecycle::AbandonedActive { entry };
        SessionRemoval::Keep
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
        enum Finish {
            Running(Arc<TrxEntry>),
            Abandoned(Arc<TrxEntry>),
            Stale,
        }
        let finish = match &*lifecycle {
            SessionLifecycle::RunningActive { entry } if entry.trx_id() == trx_id => {
                Finish::Running(Arc::clone(entry))
            }
            SessionLifecycle::AbandonedActive { entry } if entry.trx_id() == trx_id => {
                Finish::Abandoned(Arc::clone(entry))
            }
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed
            | SessionLifecycle::RunningActive { .. }
            | SessionLifecycle::AbandonedActive { .. } => Finish::Stale,
        };
        match finish {
            Finish::Running(entry) => {
                on_finish();
                entry.finish(TrxEntryState::Terminal);
                *lifecycle = SessionLifecycle::RunningIdle;
                SessionRemoval::Keep
            }
            Finish::Abandoned(entry) => {
                on_finish();
                entry.finish(TrxEntryState::Terminal);
                *lifecycle = SessionLifecycle::Closed;
                self.release_session_locks();
                SessionRemoval::Remove
            }
            Finish::Stale => SessionRemoval::Keep,
        }
    }

    #[inline]
    fn resolve_trx(&self, trx_id: TrxID, operation: &'static str) -> Result<Arc<TrxEntry>> {
        let lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningActive { entry }
            | SessionLifecycle::AbandonedActive { entry }
                if entry.trx_id() == trx_id =>
            {
                Ok(Arc::clone(entry))
            }
            SessionLifecycle::RunningActive { .. } | SessionLifecycle::AbandonedActive { .. } => {
                Err(transaction_discarded_err(operation))
            }
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed => Err(transaction_discarded_err(operation)),
        }
    }

    #[inline]
    fn abandon_trx_handle(&self, trx_id: TrxID) -> bool {
        let lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningActive { entry }
            | SessionLifecycle::AbandonedActive { entry }
                if entry.trx_id() == trx_id =>
            {
                entry.abandon()
            }
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed
            | SessionLifecycle::RunningActive { .. }
            | SessionLifecycle::AbandonedActive { .. } => false,
        }
    }

    #[inline]
    fn shutdown_cleanup_candidate(&self) -> Option<TrxID> {
        let lifecycle = self.lifecycle.lock();
        match &*lifecycle {
            SessionLifecycle::RunningActive { entry } => matches!(
                entry.inspect_state(),
                TrxEntryState::Abandoned | TrxEntryState::CheckedOutAbandoned
            )
            .then(|| entry.trx_id()),
            SessionLifecycle::AbandonedActive { entry } => entry.abandon().then(|| entry.trx_id()),
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed => None,
        }
    }

    #[inline]
    fn has_active_transaction(&self) -> bool {
        match &*self.lifecycle.lock() {
            SessionLifecycle::RunningActive { entry }
            | SessionLifecycle::AbandonedActive { entry } => entry.in_trx(),
            SessionLifecycle::RunningIdle
            | SessionLifecycle::AbandonedIdle
            | SessionLifecycle::Closed => false,
        }
    }

    #[inline]
    fn shutdown_removal(&self) -> bool {
        let mut lifecycle = self.lifecycle.lock();
        match &*lifecycle {
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

    /// Upgrade a cached user-table runtime if the session weak cache still reaches it.
    #[inline]
    pub(crate) fn cached_user_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        let mut cache = self.table_cache.lock();
        let weak = cache.get(&table_id)?;
        match weak.upgrade() {
            Some(table) => Some(table),
            None => {
                cache.remove(&table_id);
                None
            }
        }
    }

    /// Remember a successfully resolved user-table runtime without extending its lifetime.
    #[inline]
    pub(crate) fn cache_user_table(&self, table: &Arc<Table>) {
        let mut cache = self.table_cache.lock();
        cache.insert(table.table_id(), Arc::downgrade(table));
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

enum SessionLifecycle {
    RunningIdle,
    RunningActive { entry: Arc<TrxEntry> },
    AbandonedIdle,
    AbandonedActive { entry: Arc<TrxEntry> },
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionRemoval {
    Keep,
    Remove,
}

/// Private transaction runtime attachment retained by checked-out transaction work.
///
/// This handle owns engine runtime liveness and session-state reachability for
/// one operation, terminal path, prepared commit handoff, or cleanup path. The
/// public transaction facade never stores it.
pub(crate) struct TrxAttachment {
    /// Strong runtime handle kept until the transaction reaches a terminal path.
    engine: EngineRef,
    /// Registry lookup key for terminal session cleanup.
    session_id: SessionID,
    /// Active transaction id used to avoid finishing a replaced session state.
    trx_id: TrxID,
    /// Claim-local reachability for session-local caches.
    session: Arc<SessionState>,
    /// Cached guards needed by transaction work even if session state is gone.
    pool_guards: PoolGuards,
}

impl TrxAttachment {
    /// Create a transaction runtime attachment without public handle ownership.
    #[inline]
    pub(crate) fn new(engine: EngineRef, session: Arc<SessionState>, trx_id: TrxID) -> Self {
        let pool_guards = session.pool_guards().clone();
        Self {
            engine,
            session_id: session.id(),
            trx_id,
            session,
            pool_guards,
        }
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

    /// Upgrade a cached session-local user-table runtime if it is still alive.
    #[inline]
    pub(crate) fn cached_user_table(&self, table_id: TableID) -> Option<Arc<Table>> {
        self.session.cached_user_table(table_id)
    }

    /// Store a weak session-local table cache entry after successful resolution.
    #[inline]
    pub(crate) fn cache_user_table(&self, table: &Arc<Table>) {
        self.session.cache_user_table(table);
    }

    /// Remove and return the cached insert page for a table, if session state remains.
    #[inline]
    pub(crate) fn load_active_insert_page(
        &self,
        table_id: TableID,
    ) -> Option<(VersionedPageID, RowID)> {
        self.session.load_active_insert_page(table_id)
    }

    /// Cache the active insert page if session state remains.
    #[inline]
    pub(crate) fn save_active_insert_page(
        &self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        self.session
            .save_active_insert_page(table_id, page_id, row_id);
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

    /// Queue rollback cleanup for an abandoned transaction.
    #[inline]
    pub(crate) fn request_abandoned_cleanup(&self, reason: TrxCleanupReason) {
        self.engine.trx_sys.request_abandoned_trx_cleanup(
            self.engine.clone(),
            self.session_id,
            self.trx_id,
            reason,
        );
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::conf::EngineConfig;
    use crate::trx::{MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS, TrxInner};
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Returns the number of registry-owned sessions for tests.
    #[inline]
    pub(crate) fn session_registry_len(registry: &SessionRegistry) -> usize {
        registry.entries.len()
    }

    /// Create one registry-owned transaction with test-controlled ids.
    #[inline]
    pub(crate) fn create_test_transaction(
        registry: &SessionRegistry,
        engine: EngineRef,
        session_id: SessionID,
        trx_id: TrxID,
        sts: TrxID,
        log_no: usize,
        gc_no: usize,
    ) -> (Transaction, Arc<SessionState>) {
        let state = Arc::new(SessionState::new(engine.clone(), session_id));
        let inner = crate::trx::TrxInner::new(trx_id, sts, log_no, gc_no, session_id);
        let entry = TrxEntry::new(inner);
        {
            let mut lifecycle = state.lifecycle.lock();
            *lifecycle = SessionLifecycle::RunningActive { entry };
        }
        registry.insert(Arc::clone(&state));
        (
            Transaction::new(engine.downgrade(), session_id, trx_id, sts),
            state,
        )
    }

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
            let session = self
                .pin("test session last commit timestamp")
                .expect("test session must be running");
            TrxID::new(session.state.last_cts.load(Ordering::SeqCst))
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

    #[test]
    fn test_shutdown_cleanup_candidate_claims_abandoned_active() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let session_id = SessionID::new(1);
            let trx_id = MIN_ACTIVE_TRX_ID;
            let state = SessionState::new(engine.new_ref().unwrap(), session_id);
            let entry = TrxEntry::new(TrxInner::new(trx_id, MIN_SNAPSHOT_TS, 0, 0, session_id));
            {
                let mut lifecycle = state.lifecycle.lock();
                *lifecycle = SessionLifecycle::AbandonedActive {
                    entry: Arc::clone(&entry),
                };
            }

            assert_eq!(state.shutdown_cleanup_candidate(), Some(trx_id));
            assert_eq!(entry.inspect_state(), TrxEntryState::Abandoned);
        });
    }

    #[test]
    fn test_shutdown_cleanup_candidate_skips_unclaimable_abandoned_active() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = EngineConfig::default()
                .storage_root(root.path())
                .build()
                .await
                .unwrap();
            let session_id = SessionID::new(1);
            let trx_id = MIN_ACTIVE_TRX_ID;
            let state = SessionState::new(engine.new_ref().unwrap(), session_id);
            let entry = TrxEntry::new(TrxInner::new(trx_id, MIN_SNAPSHOT_TS, 0, 0, session_id));
            entry.finish(TrxEntryState::Failed);
            {
                let mut lifecycle = state.lifecycle.lock();
                *lifecycle = SessionLifecycle::AbandonedActive {
                    entry: Arc::clone(&entry),
                };
            }

            assert_eq!(state.shutdown_cleanup_candidate(), None);
            assert_eq!(entry.inspect_state(), TrxEntryState::Failed);
        });
    }

    #[test]
    fn test_wait_for_trx_change_since_returns_after_prior_change() {
        let registry = SessionRegistry::new();
        let observed_epoch = registry.trx_change_epoch();

        registry.notify_trx_changed();
        registry.wait_for_trx_change_since(observed_epoch);

        assert_ne!(registry.trx_change_epoch(), observed_epoch);
    }

    #[test]
    fn test_wait_for_trx_change_since_wakes_on_change() {
        let registry = Arc::new(SessionRegistry::new());
        let observed_epoch = registry.trx_change_epoch();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let waiter = {
            let registry = Arc::clone(&registry);
            std::thread::spawn(move || {
                ready_tx.send(()).expect("waiter should report ready");
                registry.wait_for_trx_change_since(observed_epoch);
                done_tx.send(()).expect("waiter should report completion");
            })
        };

        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("waiter should start");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "waiter should block before a transaction change"
        );

        registry.notify_trx_changed();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("waiter should wake after a transaction change");
        waiter.join().expect("waiter thread should finish");
    }
}
