use super::TrxInner;
use crate::catalog::{
    CurrentTableState, ResolvedLiveMetadata, ResolvedVisibleTableMetadata, is_catalog_table,
};
use crate::error::{
    OperationError, OperationOrFatalError, OperationOrFatalResult, OperationResult,
};
use crate::id::{TableID, TrxID};
use crate::lock::{LockMode, LockResource, LockScopeState};
use crate::session::TrxAttachment;
use crate::table::{Table, TableRuntimeLayout};
use error_stack::{Report, ResultExt};
use std::sync::Arc;

/// Schema contract requested by one foreground user-table operation.
#[derive(Clone, Copy)]
pub(crate) enum TableAdmissionRequest {
    TableRead,
    IndexRead { index_no: usize },
    TableWrite,
    IndexWrite { index_no: usize },
}

impl TableAdmissionRequest {
    #[inline]
    fn index_no(self) -> Option<usize> {
        match self {
            Self::TableRead | Self::TableWrite => None,
            Self::IndexRead { index_no } | Self::IndexWrite { index_no } => Some(index_no),
        }
    }

    #[inline]
    fn is_write(self) -> bool {
        matches!(self, Self::TableWrite | Self::IndexWrite { .. })
    }
}

/// Positive transaction-lifetime binding between visible schema and current runtime.
pub(super) struct TransactionTableBinding {
    visible: ResolvedLiveMetadata,
    bound_current_effective_cts: TrxID,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
}

impl TransactionTableBinding {
    #[inline]
    fn validate(
        &self,
        table_id: TableID,
        request: TableAdmissionRequest,
        operation: &'static str,
    ) -> OperationResult<()> {
        if let Some(index_no) = request.index_no() {
            let visible_spec = self
                .visible
                .metadata()
                .idx
                .index_spec(index_no)
                .ok_or_else(|| {
                    Report::new(OperationError::IndexNotFound).attach(format!(
                        "operation={operation}, table_id={table_id}, index_no={index_no}"
                    ))
                })?;
            let current_spec =
                self.layout
                    .metadata()
                    .idx
                    .index_spec(index_no)
                    .ok_or_else(|| {
                        Report::new(OperationError::SchemaChanged).attach(format!(
                            "operation={operation}, table_id={table_id}, index_no={index_no}"
                        ))
                    })?;
            assert_eq!(
                visible_spec, current_spec,
                "stable index specification changed across metadata versions: table_id={table_id}, index_no={index_no}"
            );
            assert!(
                self.layout
                    .secondary_indexes()
                    .get(index_no)
                    .is_some_and(Option::is_some),
                "active current index is missing its runtime: table_id={table_id}, index_no={index_no}"
            );
        }

        if request.is_write() && self.visible.effective_cts() != self.bound_current_effective_cts {
            return Err(Report::new(OperationError::SchemaChanged).attach(format!(
                "operation={operation}, table_id={table_id}, visible_effective_cts={}, current_effective_cts={}",
                self.visible.effective_cts(),
                self.bound_current_effective_cts
            )));
        }
        Ok(())
    }

    #[inline]
    fn operation_parts(&self) -> (Arc<Table>, Arc<TableRuntimeLayout>) {
        (Arc::clone(&self.table), Arc::clone(&self.layout))
    }
}

#[inline]
fn admit_cached_binding(
    inner: &TrxInner,
    table_id: TableID,
    metadata_resource: LockResource,
    request: TableAdmissionRequest,
    operation: &'static str,
) -> OperationResult<Option<(Arc<Table>, Arc<TableRuntimeLayout>)>> {
    let Some(binding) = inner.table_bindings.get(&table_id) else {
        return Ok(None);
    };
    assert!(
        inner
            .checked_lock_state()
            .covers(metadata_resource, LockMode::Shared),
        "transaction table binding requires cached metadata S: table_id={table_id}"
    );
    binding.validate(table_id, request, operation)?;
    Ok(Some(binding.operation_parts()))
}

#[inline]
fn resolve_table_binding(
    attachment: &TrxAttachment,
    sts: TrxID,
    table_id: TableID,
    request: TableAdmissionRequest,
    operation: &'static str,
) -> OperationResult<TransactionTableBinding> {
    let visible = attachment
        .engine()
        .catalog()
        .resolve_user_table_visible(table_id, sts)
        .ok_or_else(|| {
            Report::new(OperationError::TableNotFound)
                .attach(format!("operation={operation}, table_id={table_id}"))
        })?;
    let visible = match visible {
        ResolvedVisibleTableMetadata::Live(visible) => visible,
        ResolvedVisibleTableMetadata::Tombstone { effective_cts } => {
            return Err(Report::new(OperationError::TableNotFound)
                .attach(format!(
                    "operation={operation}, table_id={table_id}, tombstone_effective_cts={effective_cts}"
                )));
        }
    };
    let current = attachment
        .engine()
        .catalog()
        .resolve_user_table_current(table_id)
        .ok_or_else(|| {
            Report::new(OperationError::SchemaChanged)
                .attach(format!("operation={operation}, table_id={table_id}"))
        })?;
    let (bound_current_effective_cts, current_metadata, table) = match current {
        CurrentTableState::Live {
            effective_cts,
            metadata,
            table,
        } => (effective_cts, metadata, table),
        CurrentTableState::Dropped { .. } => {
            return Err(Report::new(OperationError::SchemaChanged)
                .attach(format!("operation={operation}, table_id={table_id}")));
        }
    };
    assert_eq!(
        table.table_id(),
        table_id,
        "current catalog runtime has mismatched table id: requested={table_id}, actual={}",
        table.table_id()
    );
    let layout = table.layout_snapshot();
    assert!(
        Arc::ptr_eq(layout.metadata_arc(), &current_metadata),
        "current catalog metadata and runtime layout diverged: table_id={table_id}"
    );
    let binding = TransactionTableBinding {
        visible,
        bound_current_effective_cts,
        table,
        layout,
    };
    binding.validate(table_id, request, operation)?;
    Ok(binding)
}

#[inline]
async fn install_table_binding(
    inner: &mut TrxInner,
    attachment: &TrxAttachment,
    statement_locks: &mut LockScopeState,
    table_id: TableID,
    metadata_resource: LockResource,
    binding: TransactionTableBinding,
    operation: &'static str,
) -> OperationResult<(Arc<Table>, Arc<TableRuntimeLayout>)> {
    let lock_manager = attachment.engine().lock_manager();
    inner
        .checked_lock_state_mut()
        .acquire(lock_manager, metadata_resource, LockMode::Shared)
        .await
        .attach_with(|| format!("operation={operation}, table_id={table_id}"))?;
    let (table, layout) = binding.operation_parts();
    let previous = inner.table_bindings.insert(table_id, binding);
    assert!(
        previous.is_none(),
        "binding miss commit replaced an existing binding: table_id={table_id}"
    );
    let family = inner.checked_lock_state_mut().family_mut();
    let released = family.release(statement_locks, lock_manager, metadata_resource);
    assert!(
        released,
        "successful binding handoff must release statement metadata S: table_id={table_id}"
    );
    attachment.cache_user_table(&table);
    Ok((table, layout))
}

/// Admit one foreground user-table operation through a binding hit or locked miss.
///
/// A successful first touch binds snapshot-visible metadata to the current
/// runtime and keeps that contract stable under transaction-owned metadata S.
/// Later operations reuse the binding and validate only their requested shape.
pub(super) async fn admit_user_table(
    inner: &mut TrxInner,
    attachment: &TrxAttachment,
    statement_locks: &mut LockScopeState,
    table_id: TableID,
    request: TableAdmissionRequest,
    operation: &'static str,
) -> OperationOrFatalResult<(Arc<Table>, Arc<TableRuntimeLayout>)> {
    if is_catalog_table(table_id) {
        return Err(Report::new(OperationError::TableNotFound)
            .attach(format!("operation={operation}, table_id={table_id}"))
            .into());
    }
    attachment
        .engine()
        .poisoner
        .ensure_healthy()
        .attach_with(|| format!("operation={operation}, table_id={table_id}"))?;

    let metadata_resource = LockResource::TableMetadata(table_id);
    // The transaction metadata lock already protects a cached binding, so the
    // operation can validate and reuse it without another lock handoff.
    if let Some(parts) =
        admit_cached_binding(inner, table_id, metadata_resource, request, operation)?
    {
        return Ok(parts);
    }

    // First touch resolves both snapshot-visible and current metadata while
    // statement metadata S prevents a concurrent DDL publication.
    let lock_manager = attachment.engine().lock_manager();
    let family = inner.checked_lock_state_mut().family_mut();
    family
        .acquire(
            statement_locks,
            lock_manager,
            metadata_resource,
            LockMode::Shared,
        )
        .await
        .attach_with(|| format!("operation={operation}, table_id={table_id}"))?;

    #[cfg(test)]
    tests::maybe_pause_after_statement_metadata_grant().await;

    let binding = resolve_table_binding(attachment, inner.sts(), table_id, request, operation)?;
    // Installation acquires transaction metadata S before releasing statement
    // metadata S, leaving no unprotected gap in the binding's lifetime.
    install_table_binding(
        inner,
        attachment,
        statement_locks,
        table_id,
        metadata_resource,
        binding,
        operation,
    )
    .await
    .map_err(OperationOrFatalError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::catalog::tests::table2;
    use crate::catalog::{IndexAttributes, IndexKey, IndexSpec};
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{Error, LifecycleError, OperationError};
    use crate::lock::tests::{LockDebugEntryState, debug_snapshot};
    use crate::lock::{LockOwner, LockScope};
    use crate::session::tests::wait_for_session_idle;
    use crate::table::TableTerminal;
    use crate::value::Val;
    use std::cell::Cell;
    use std::future::{Future, pending};
    use std::pin::Pin;
    use tempfile::TempDir;

    thread_local! {
        static PAUSE_AFTER_STATEMENT_METADATA_GRANT: Cell<bool> = const { Cell::new(false) };
    }

    fn pause_after_statement_metadata_grant() {
        PAUSE_AFTER_STATEMENT_METADATA_GRANT.set(true);
    }

    pub(super) async fn maybe_pause_after_statement_metadata_grant() {
        if PAUSE_AFTER_STATEMENT_METADATA_GRANT.replace(false) {
            pending::<()>().await;
        }
    }

    async fn test_engine(log_file_stem: &str) -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::bootstrap(
            EngineConfig::default()
                .storage_root(temp_dir.path().to_path_buf())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .role(PoolRole::Mem)
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem(log_file_stem),
                ),
        )
        .await
        .unwrap();
        (temp_dir, engine)
    }

    fn owner_has_grant(
        engine: &Engine,
        owner: LockOwner,
        resource: LockResource,
        mode: LockMode,
    ) -> bool {
        debug_snapshot(engine.inner().core.lock_manager())
            .entries
            .iter()
            .any(|entry| {
                entry.owner == owner
                    && entry.resource == resource
                    && entry.mode == mode
                    && entry.state == LockDebugEntryState::Granted
            })
    }

    fn operation_error(err: &Error) -> Option<OperationError> {
        err.report().downcast_ref::<OperationError>().copied()
    }

    async fn observe_metadata_x_waiter<F>(
        engine: &Engine,
        resource: LockResource,
        mut future: Pin<&mut F>,
    ) where
        F: Future,
    {
        for _ in 0..32 {
            assert!(matches!(
                futures::poll!(future.as_mut()),
                std::task::Poll::Pending
            ));
            if debug_snapshot(engine.inner().core.lock_manager())
                .entries
                .iter()
                .any(|entry| {
                    entry.resource == resource
                        && entry.mode == LockMode::Exclusive
                        && entry.state == LockDebugEntryState::Waiting
                })
            {
                return;
            }
        }
        panic!("table metadata X waiter was not observed after bounded polling");
    }

    fn assert_no_table_locks(engine: &Engine, table_id: TableID) {
        let metadata = LockResource::TableMetadata(table_id);
        let data = LockResource::TableData(table_id);
        assert!(
            debug_snapshot(engine.inner().core.lock_manager())
                .entries
                .iter()
                .all(|entry| entry.resource != metadata && entry.resource != data)
        );
    }

    #[test]
    fn first_read_commits_binding_and_releases_statement_metadata_lock() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("admission_first_read_binding").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let mut trx = session.begin_trx().unwrap();
            let trx_owner = LockOwner::transaction(session_id, trx.trx_id());
            let metadata = LockResource::TableMetadata(table_id);

            trx.exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await)
                .await
                .unwrap();

            {
                let checkout = trx.checkout().unwrap();
                assert!(checkout.inner().table_bindings.contains_key(&table_id));
                assert!(
                    checkout
                        .inner()
                        .checked_lock_state()
                        .covers(metadata, LockMode::Shared)
                );
            }
            assert!(owner_has_grant(
                &engine,
                trx_owner,
                metadata,
                LockMode::Shared
            ));
            assert!(
                debug_snapshot(engine.inner().core.lock_manager())
                    .entries
                    .iter()
                    .all(|entry| {
                        entry.resource != metadata
                            || !matches!(entry.owner.scope(), LockScope::Statement(..))
                    })
            );

            trx.rollback().await.unwrap();
            assert!(
                !owner_has_grant(&engine, trx_owner, metadata, LockMode::Shared),
                "terminal rollback must release the binding metadata lock"
            );
            drop(session);
            engine.shutdown();
        });
    }

    #[test]
    fn cancelled_first_touch_releases_statement_grant_before_terminal_cleanup() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("admission_cancel_first_touch").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let session_id = session.id();
            let mut trx = session.begin_trx().unwrap();
            let trx_owner = LockOwner::transaction(session_id, trx.trx_id());
            let stmt_owner = trx_owner.statement(1);
            let metadata = LockResource::TableMetadata(table_id);
            pause_after_statement_metadata_grant();
            let mut exec = Box::pin(
                trx.exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await),
            );

            assert!(matches!(
                futures::poll!(exec.as_mut()),
                std::task::Poll::Pending
            ));
            assert!(owner_has_grant(
                &engine,
                stmt_owner,
                metadata,
                LockMode::Shared
            ));
            assert!(!owner_has_grant(
                &engine,
                trx_owner,
                metadata,
                LockMode::Shared
            ));

            drop(exec);

            assert_no_table_locks(&engine, table_id);
            let err = trx.exec(async |_| Ok::<(), Error>(())).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            wait_for_session_idle(&engine.inner().session_registry, session_id).await;
            engine.shutdown();
        });
    }

    #[test]
    fn stale_write_first_rejects_both_index_kinds_before_locks_or_binding() {
        smol::block_on(async {
            for (attributes, log_file_stem) in [
                (IndexAttributes::UK, "admission_stale_write_first_unique"),
                (
                    IndexAttributes::empty(),
                    "admission_stale_write_first_non_unique",
                ),
            ] {
                let (_temp_dir, engine) = test_engine(log_file_stem).await;
                let table_id = table2(&engine).await;
                let mut old_session = engine.new_session().unwrap();
                let old_session_id = old_session.id();
                let mut old_trx = old_session.begin_trx().unwrap();
                let trx_owner = LockOwner::transaction(old_session_id, old_trx.trx_id());
                let metadata = LockResource::TableMetadata(table_id);

                let mut ddl_session = engine.new_session().unwrap();
                ddl_session
                    .create_index(table_id, IndexSpec::new(vec![IndexKey::new(1)], attributes))
                    .await
                    .unwrap();

                let err = old_trx
                    .exec(async |stmt| {
                        stmt.table_insert_mvcc(
                            table_id,
                            vec![Val::from(1i32), Val::from(&b"old"[..])],
                        )
                        .await
                        .map(|_| ())
                    })
                    .await
                    .unwrap_err();
                assert_eq!(operation_error(&err), Some(OperationError::SchemaChanged));

                {
                    let checkout = old_trx.checkout().unwrap();
                    assert!(!checkout.inner().table_bindings.contains_key(&table_id));
                    assert!(
                        !checkout
                            .inner()
                            .checked_lock_state()
                            .covers(metadata, LockMode::Shared)
                    );
                    assert!(
                        !checkout
                            .inner()
                            .checked_lock_state()
                            .covers(LockResource::TableData(table_id), LockMode::IntentExclusive)
                    );
                }
                let snapshot = debug_snapshot(engine.inner().core.lock_manager());
                assert!(
                    snapshot
                        .entries
                        .iter()
                        .all(|entry| entry.owner != trx_owner)
                );
                assert!(snapshot.entries.iter().all(|entry| {
                    entry.resource != metadata
                        || !matches!(entry.owner.scope(), LockScope::Statement(..))
                }));

                old_trx.rollback().await.unwrap();
                drop(ddl_session);
                drop(old_session);
                engine.shutdown();
            }
        });
    }

    #[test]
    fn read_intersection_rejects_both_new_index_kinds_and_later_write() {
        smol::block_on(async {
            for (attributes, log_file_stem) in [
                (IndexAttributes::UK, "admission_read_intersection_unique"),
                (
                    IndexAttributes::empty(),
                    "admission_read_intersection_non_unique",
                ),
            ] {
                let (_temp_dir, engine) = test_engine(log_file_stem).await;
                let table_id = table2(&engine).await;
                let mut old_session = engine.new_session().unwrap();
                let old_session_id = old_session.id();
                let mut old_trx = old_session.begin_trx().unwrap();
                let trx_owner = LockOwner::transaction(old_session_id, old_trx.trx_id());

                let mut ddl_session = engine.new_session().unwrap();
                let new_index_no = ddl_session
                    .create_index(table_id, IndexSpec::new(vec![IndexKey::new(1)], attributes))
                    .await
                    .unwrap();

                old_trx
                    .exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await)
                    .await
                    .unwrap();
                let surviving = old_trx
                    .exec(async |stmt| {
                        stmt.table_lookup_unique_mvcc(table_id, 0, &[Val::from(7i32)], &[0])
                            .await
                    })
                    .await
                    .unwrap();
                assert!(matches!(surviving, crate::row::ops::SelectMvcc::NotFound));

                let new_index_err = old_trx
                    .exec(async |stmt| {
                        stmt.table_index_lookup_mvcc(
                            table_id,
                            usize::from(new_index_no),
                            &[Val::from(&b"new"[..])],
                            &[0],
                        )
                        .await
                    })
                    .await
                    .unwrap_err();
                assert_eq!(
                    operation_error(&new_index_err),
                    Some(OperationError::IndexNotFound)
                );

                let write_err = old_trx
                    .exec(async |stmt| {
                        stmt.table_insert_mvcc(
                            table_id,
                            vec![Val::from(8i32), Val::from(&b"stale"[..])],
                        )
                        .await
                        .map(|_| ())
                    })
                    .await
                    .unwrap_err();
                assert_eq!(
                    operation_error(&write_err),
                    Some(OperationError::SchemaChanged)
                );

                {
                    let checkout = old_trx.checkout().unwrap();
                    assert!(checkout.inner().table_bindings.contains_key(&table_id));
                    assert!(
                        !checkout
                            .inner()
                            .checked_lock_state()
                            .covers(LockResource::TableData(table_id), LockMode::IntentExclusive)
                    );
                }
                assert!(owner_has_grant(
                    &engine,
                    trx_owner,
                    LockResource::TableMetadata(table_id),
                    LockMode::Shared
                ));

                let mut fresh_session = engine.new_session().unwrap();
                let mut fresh_trx = fresh_session.begin_trx().unwrap();
                if attributes.contains(IndexAttributes::UK) {
                    let fresh_result = fresh_trx
                        .exec(async |stmt| {
                            stmt.table_lookup_unique_mvcc(
                                table_id,
                                usize::from(new_index_no),
                                &[Val::from(&b"new"[..])],
                                &[0],
                            )
                            .await
                        })
                        .await
                        .unwrap();
                    assert!(matches!(
                        fresh_result,
                        crate::row::ops::SelectMvcc::NotFound
                    ));
                } else {
                    let fresh_result = fresh_trx
                        .exec(async |stmt| {
                            stmt.table_index_lookup_mvcc(
                                table_id,
                                usize::from(new_index_no),
                                &[Val::from(&b"new"[..])],
                                &[0],
                            )
                            .await
                        })
                        .await
                        .unwrap();
                    assert!(matches!(
                        fresh_result,
                        crate::row::ops::ScanMvcc::Rows(rows) if rows.is_empty()
                    ));
                }
                fresh_trx.commit().await.unwrap();

                old_trx.rollback().await.unwrap();
                drop(fresh_session);
                drop(ddl_session);
                drop(old_session);
                engine.shutdown();
            }
        });
    }

    #[test]
    fn bound_transaction_makes_create_index_metadata_lock_wait() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("admission_create_index_waits_for_binding").await;
            let table_id = table2(&engine).await;
            let metadata = LockResource::TableMetadata(table_id);
            let mut bound_session = engine.new_session().unwrap();
            let mut bound_trx = bound_session.begin_trx().unwrap();
            bound_trx
                .exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await)
                .await
                .unwrap();

            let mut ddl_session = engine.new_session().unwrap();
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table_now(table_id)
                .unwrap();
            let before_layout = table.layout_snapshot();
            let before_root = table.file().active_root_unchecked().clone();
            let before_current_cts = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap()
                .effective_cts();
            let mut create = Box::pin(ddl_session.create_index(
                table_id,
                IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
            ));
            observe_metadata_x_waiter(&engine, metadata, create.as_mut()).await;
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_current(table_id)
                    .unwrap()
                    .effective_cts(),
                before_current_cts
            );
            assert_eq!(
                table.layout_snapshot().generation(),
                before_layout.generation()
            );
            assert_eq!(
                table.file().active_root_unchecked().root_ts,
                before_root.root_ts
            );

            bound_trx.commit().await.unwrap();
            assert_eq!(create.await.unwrap(), 1);
            assert_no_table_locks(&engine, table_id);

            drop(ddl_session);
            drop(bound_session);
            engine.shutdown();
        });
    }

    #[test]
    fn bound_transaction_makes_drop_index_metadata_lock_wait() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("admission_drop_index_waits_for_binding").await;
            let table_id = table2(&engine).await;
            let metadata_resource = LockResource::TableMetadata(table_id);
            let mut ddl_session = engine.new_session().unwrap();
            let index_no = ddl_session
                .create_index(
                    table_id,
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                )
                .await
                .unwrap();
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table_now(table_id)
                .unwrap();

            let mut bound_session = engine.new_session().unwrap();
            let bound_session_id = bound_session.id();
            let mut bound_trx = bound_session.begin_trx().unwrap();
            let bound_owner = LockOwner::transaction(bound_session_id, bound_trx.trx_id());
            bound_trx
                .exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await)
                .await
                .unwrap();
            let before_current_cts = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap()
                .effective_cts();
            let before_layout = table.layout_snapshot();
            let before_root = table.file().active_root_unchecked().clone();

            let mut drop_index = Box::pin(ddl_session.drop_index(table_id, index_no));
            observe_metadata_x_waiter(&engine, metadata_resource, drop_index.as_mut()).await;
            assert!(owner_has_grant(
                &engine,
                bound_owner,
                metadata_resource,
                LockMode::Shared
            ));
            let waiting_current = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap();
            assert_eq!(waiting_current.effective_cts(), before_current_cts);
            assert!(
                waiting_current
                    .live_table()
                    .is_some_and(|current| Arc::ptr_eq(current, &table))
            );
            assert!(before_layout.secondary_indexes()[usize::from(index_no)].is_some());
            assert_eq!(
                table.layout_snapshot().generation(),
                before_layout.generation()
            );
            assert!(table.layout_snapshot().secondary_indexes()[usize::from(index_no)].is_some());
            assert_eq!(
                table.file().active_root_unchecked().root_ts,
                before_root.root_ts
            );
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Live);

            bound_trx.commit().await.unwrap();
            drop_index.await.unwrap();
            let current = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap();
            let CurrentTableState::Live { metadata, .. } = current else {
                panic!("DROP INDEX must keep the table live");
            };
            assert!(metadata.idx.index_spec(usize::from(index_no)).is_none());
            assert!(table.layout_snapshot().secondary_indexes()[usize::from(index_no)].is_none());
            assert_no_table_locks(&engine, table_id);

            drop(ddl_session);
            drop(bound_session);
            engine.shutdown();
        });
    }

    #[test]
    fn bound_transaction_makes_drop_table_metadata_lock_wait() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("admission_drop_table_waits_for_binding").await;
            let table_id = table2(&engine).await;
            let metadata_resource = LockResource::TableMetadata(table_id);
            let table = engine
                .inner()
                .core
                .catalog()
                .get_table_now(table_id)
                .unwrap();
            let mut bound_session = engine.new_session().unwrap();
            let bound_session_id = bound_session.id();
            let mut bound_trx = bound_session.begin_trx().unwrap();
            let bound_owner = LockOwner::transaction(bound_session_id, bound_trx.trx_id());
            bound_trx
                .exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await)
                .await
                .unwrap();
            let before_current_cts = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap()
                .effective_cts();
            let before_history_count = engine
                .inner()
                .core
                .catalog()
                .user_table_history_version_count(table_id);
            let before_generation = table.layout_snapshot().generation();
            let before_root_ts = table.file().active_root_unchecked().root_ts;

            let mut ddl_session = engine.new_session().unwrap();
            let mut drop_table = Box::pin(ddl_session.drop_table(table_id));
            observe_metadata_x_waiter(&engine, metadata_resource, drop_table.as_mut()).await;
            assert!(owner_has_grant(
                &engine,
                bound_owner,
                metadata_resource,
                LockMode::Shared
            ));
            let waiting_current = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap();
            assert_eq!(waiting_current.effective_cts(), before_current_cts);
            assert!(
                waiting_current
                    .live_table()
                    .is_some_and(|current| Arc::ptr_eq(current, &table))
            );
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .user_table_history_version_count(table_id),
                before_history_count
            );
            assert_eq!(table.layout_snapshot().generation(), before_generation);
            assert_eq!(table.file().active_root_unchecked().root_ts, before_root_ts);
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Live);

            drop(waiting_current);
            drop(table);
            bound_trx.rollback().await.unwrap();
            drop_table.await.unwrap();
            assert!(!matches!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_current(table_id),
                Some(CurrentTableState::Live { .. })
            ));
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .get_table_now(table_id)
                    .is_none()
            );
            assert_no_table_locks(&engine, table_id);

            drop(ddl_session);
            drop(bound_session);
            engine.shutdown();
        });
    }

    #[test]
    fn untouched_old_transaction_cannot_bind_removed_index() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("admission_untouched_drop_index").await;
            let table_id = table2(&engine).await;
            let mut old_session = engine.new_session().unwrap();
            let old_session_id = old_session.id();
            let mut old_trx = old_session.begin_trx().unwrap();
            let old_sts = old_trx.sts();
            let old_owner = LockOwner::transaction(old_session_id, old_trx.trx_id());

            let mut ddl_session = engine.new_session().unwrap();
            ddl_session.drop_index(table_id, 0).await.unwrap();
            let visible = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_visible(table_id, old_sts)
                .unwrap();
            let ResolvedVisibleTableMetadata::Live(visible) = visible else {
                panic!("untouched transaction should retain logical predecessor metadata");
            };
            assert!(visible.metadata().idx.index_spec(0).is_some());
            let CurrentTableState::Live { metadata, .. } = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap()
            else {
                panic!("DROP INDEX must keep the table live");
            };
            assert!(metadata.idx.index_spec(0).is_none());

            let err = old_trx
                .exec(async |stmt| {
                    stmt.table_lookup_unique_mvcc(table_id, 0, &[Val::from(1i32)], &[0])
                        .await
                })
                .await
                .unwrap_err();
            assert_eq!(operation_error(&err), Some(OperationError::SchemaChanged));
            {
                let checkout = old_trx.checkout().unwrap();
                assert!(!checkout.inner().table_bindings.contains_key(&table_id));
                assert!(
                    !checkout
                        .inner()
                        .checked_lock_state()
                        .covers(LockResource::TableMetadata(table_id), LockMode::Shared)
                );
                assert!(
                    !checkout
                        .inner()
                        .checked_lock_state()
                        .covers(LockResource::TableData(table_id), LockMode::Shared)
                );
            }
            assert!(
                debug_snapshot(engine.inner().core.lock_manager())
                    .entries
                    .iter()
                    .all(|entry| entry.owner != old_owner
                        && (entry.resource != LockResource::TableMetadata(table_id)
                            || !matches!(entry.owner.scope(), LockScope::Statement(..))))
            );

            old_trx.rollback().await.unwrap();
            assert_no_table_locks(&engine, table_id);
            drop(ddl_session);
            drop(old_session);
            engine.shutdown();
        });
    }

    #[test]
    fn untouched_old_transaction_cannot_bind_dropped_table() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("admission_untouched_drop_table").await;
            let table_id = table2(&engine).await;
            let mut old_session = engine.new_session().unwrap();
            let old_session_id = old_session.id();
            let mut old_trx = old_session.begin_trx().unwrap();
            let old_sts = old_trx.sts();
            let old_owner = LockOwner::transaction(old_session_id, old_trx.trx_id());

            let mut ddl_session = engine.new_session().unwrap();
            ddl_session.drop_table(table_id).await.unwrap();
            assert!(matches!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_current(table_id),
                Some(CurrentTableState::Dropped { .. })
            ));
            assert!(matches!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_visible(table_id, old_sts),
                Some(ResolvedVisibleTableMetadata::Live(_))
            ));

            let err = old_trx
                .exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await)
                .await
                .unwrap_err();
            assert_eq!(operation_error(&err), Some(OperationError::SchemaChanged));
            {
                let checkout = old_trx.checkout().unwrap();
                assert!(!checkout.inner().table_bindings.contains_key(&table_id));
                assert!(
                    !checkout
                        .inner()
                        .checked_lock_state()
                        .covers(LockResource::TableMetadata(table_id), LockMode::Shared)
                );
                assert!(
                    !checkout
                        .inner()
                        .checked_lock_state()
                        .covers(LockResource::TableData(table_id), LockMode::Shared)
                );
            }
            assert!(
                debug_snapshot(engine.inner().core.lock_manager())
                    .entries
                    .iter()
                    .all(|entry| entry.owner != old_owner
                        && (entry.resource != LockResource::TableMetadata(table_id)
                            || !matches!(entry.owner.scope(), LockScope::Statement(..))))
            );

            old_trx.rollback().await.unwrap();
            assert_no_table_locks(&engine, table_id);
            drop(ddl_session);
            drop(old_session);
            engine.shutdown();
        });
    }
}
