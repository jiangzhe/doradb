use crate::buffer::page::VersionedPageID;
use crate::buffer::{BufferPool, PoolGuards, PoolRole};
use crate::catalog::{IndexNo, IndexSpec, TableSpec};
use crate::engine::EngineRef;
use crate::error::{OperationError, Result};
use crate::id::{RowID, SessionID, TableID, TrxID};
use crate::lock::{FreshLockGuard, LockMode, LockOwner, LockOwnerGroup, LockResource};
use crate::table::Table;
use crate::trx::ActiveTrx;
use error_stack::Report;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Shared session-level DDL admission context.
pub(crate) struct SessionDdlContext {
    pub(crate) engine: EngineRef,
    pub(crate) owner: LockOwner,
    pub(crate) owner_group: LockOwnerGroup,
}

impl SessionDdlContext {
    #[inline]
    pub(crate) fn new(session: &Session) -> Result<Self> {
        if session.in_trx() {
            return Err(Report::new(OperationError::NotSupported)
                .attach("implicit commit due to DDL")
                .into());
        }
        Ok(Self {
            engine: session.engine().clone(),
            owner: LockOwner::Session(session.id()),
            owner_group: LockOwnerGroup::Session(session.id()),
        })
    }
}

/// Per-client execution context bound to one engine instance.
pub struct Session {
    state: Arc<SessionState>,
}

impl Session {
    #[inline]
    pub(crate) fn new(engine_ref: EngineRef, id: SessionID) -> Self {
        Session {
            state: Arc::new(SessionState::new(engine_ref, id)),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.state.id()
    }

    /// Returns the engine handle bound to this session.
    #[inline]
    pub fn engine(&self) -> &EngineRef {
        &self.state.engine_ref
    }

    /// Get a user-table runtime handle through this session's engine.
    #[inline]
    pub async fn get_table(&self, table_id: TableID) -> Result<Arc<Table>> {
        self.state.engine_ref.get_table(table_id).await
    }

    /// Returns the reusable pool guards owned by this session.
    #[inline]
    pub fn pool_guards(&self) -> &PoolGuards {
        self.state.pool_guards()
    }

    /// Returns whether the session currently owns an active transaction.
    #[inline]
    pub fn in_trx(&self) -> bool {
        self.state.in_trx.load(Ordering::Relaxed)
    }

    /// Remove and return the cached insert page for a table, if present.
    #[inline]
    pub fn load_active_insert_page(
        &mut self,
        table_id: TableID,
    ) -> Option<(VersionedPageID, RowID)> {
        self.state.load_active_insert_page(table_id)
    }

    /// Cache the current insert page for a table.
    #[inline]
    pub fn save_active_insert_page(
        &mut self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        self.state
            .save_active_insert_page(table_id, page_id, row_id);
    }

    /// Begin a new transaction if the session is currently idle.
    #[inline]
    pub fn begin_trx(&mut self) -> Result<ActiveTrx> {
        self.state.engine_ref.with_admitted_operation(|| {
            if !self.state.try_enter_trx() {
                return Err(Report::new(OperationError::ExistingTransaction)
                    .attach(format!("session_id={}", self.id()))
                    .into());
            }
            Ok(self
                .state
                .engine_ref
                .trx_sys
                .begin_trx(Arc::clone(&self.state)))
        })?
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
        let engine = self.state.engine().clone();
        engine
            .catalog()
            .validate_user_table_live(table_id, "lock explicit table")
            .await?;
        let lock_manager = engine.lock_manager();
        let owner = LockOwner::Session(self.id());
        let owner_group = LockOwnerGroup::Session(self.id());
        let metadata_resource = LockResource::TableMetadata(table_id);
        let metadata_grant = lock_manager
            .acquire_grouped_with_grant(metadata_resource, LockMode::Shared, owner, owner_group)
            .await?;
        let mut metadata_guard =
            FreshLockGuard::new(lock_manager, metadata_resource, owner, metadata_grant);
        let data_resource = LockResource::TableData(table_id);
        let data_grant = lock_manager
            .acquire_grouped_with_grant(data_resource, mode, owner, owner_group)
            .await?;
        let mut data_guard = FreshLockGuard::new(lock_manager, data_resource, owner, data_grant);
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
        if self.in_trx() {
            return Err(Report::new(OperationError::NotSupported)
                .attach("unlock table while session has an active transaction")
                .into());
        }
        let owner = LockOwner::Session(self.id());
        let lock_manager = self.state.engine().lock_manager();
        lock_manager.release(LockResource::TableData(table_id), owner);
        lock_manager.release(LockResource::TableMetadata(table_id), owner);
        Ok(())
    }
}

/// Shared mutable state referenced by transactions started from one [`Session`].
pub struct SessionState {
    id: SessionID,
    engine_ref: EngineRef,
    pool_guards: PoolGuards,
    in_trx: AtomicBool,
    last_cts: AtomicU64,
    active_insert_pages: Mutex<HashMap<TableID, (VersionedPageID, RowID)>>,
}

impl SessionState {
    /// Create a new session state and populate its default pool guards.
    #[inline]
    pub fn new(engine_ref: EngineRef, id: SessionID) -> Self {
        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, engine_ref.meta_pool.pool_guard())
            .push(PoolRole::Index, engine_ref.index_pool.pool_guard())
            .push(PoolRole::Mem, engine_ref.mem_pool.pool_guard())
            .push(PoolRole::Disk, engine_ref.disk_pool.pool_guard())
            .build();
        SessionState {
            id,
            engine_ref,
            pool_guards,
            in_trx: AtomicBool::new(false),
            last_cts: AtomicU64::new(0),
            active_insert_pages: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the engine-local session identity.
    #[inline]
    pub fn id(&self) -> SessionID {
        self.id
    }

    /// Returns the engine handle for this session state.
    #[inline]
    pub fn engine(&self) -> &EngineRef {
        &self.engine_ref
    }

    /// Returns the guard bundle owned by this session state.
    #[inline]
    pub fn pool_guards(&self) -> &PoolGuards {
        &self.pool_guards
    }

    #[inline]
    fn try_enter_trx(&self) -> bool {
        self.in_trx
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Mark the session transaction as committed at the given CTS.
    #[inline]
    pub fn commit(&self, cts: TrxID) {
        self.last_cts.store(cts.as_u64(), Ordering::SeqCst);
        self.in_trx.store(false, Ordering::SeqCst);
    }

    /// Mark the session transaction as rolled back.
    #[inline]
    pub fn rollback(&self) {
        self.in_trx.store(false, Ordering::SeqCst);
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
}

impl Drop for SessionState {
    #[inline]
    fn drop(&mut self) {
        self.engine_ref
            .lock_manager()
            .release_owner(LockOwner::Session(self.id));
    }
}
