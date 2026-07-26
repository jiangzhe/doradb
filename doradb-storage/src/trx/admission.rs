use super::TrxInner;
use crate::catalog::{
    CurrentTableState, ResolvedLiveMetadata, ResolvedVisibleTableMetadata, is_catalog_table,
};
use crate::error::{
    OperationError, OperationOrFatalError, OperationOrFatalResult, OperationResult,
};
use crate::id::{TableID, TrxID};
use crate::lock::{FreshLockGuard, LockGrant, LockMode, LockResource, OwnerLockState};
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

struct AdmissionCommitGuard<'inner, 'lock> {
    inner: &'inner mut TrxInner,
    table_id: TableID,
    metadata_resource: LockResource,
    fresh_grant: Option<FreshLockGuard<'lock>>,
    cached_by_admission: bool,
    binding_inserted: bool,
    committed: bool,
}

impl<'inner, 'lock> AdmissionCommitGuard<'inner, 'lock> {
    #[inline]
    fn new(
        inner: &'inner mut TrxInner,
        table_id: TableID,
        metadata_resource: LockResource,
        fresh_grant: Option<FreshLockGuard<'lock>>,
    ) -> Self {
        Self {
            inner,
            table_id,
            metadata_resource,
            fresh_grant,
            cached_by_admission: false,
            binding_inserted: false,
            committed: false,
        }
    }

    #[inline]
    fn commit(
        &mut self,
        binding: TransactionTableBinding,
    ) -> (Arc<Table>, Arc<TableRuntimeLayout>) {
        let parts = binding.operation_parts();
        if self.fresh_grant.is_some() {
            self.inner
                .checked_lock_state_mut()
                .cache_granted(self.metadata_resource, LockMode::Shared);
            self.cached_by_admission = true;
        } else {
            assert!(
                self.inner
                    .checked_lock_state()
                    .cached_covers(self.metadata_resource, LockMode::Shared),
                "existing transaction metadata grant must be owner-cached before binding: table_id={}",
                self.table_id
            );
        }
        let previous = self.inner.table_bindings.insert(self.table_id, binding);
        assert!(
            previous.is_none(),
            "binding miss commit replaced an existing binding: table_id={}",
            self.table_id
        );
        self.binding_inserted = true;
        if let Some(guard) = self.fresh_grant.as_mut() {
            guard.disarm();
        }
        self.committed = true;
        parts
    }
}

impl Drop for AdmissionCommitGuard<'_, '_> {
    #[inline]
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if self.binding_inserted {
            self.inner.table_bindings.remove(&self.table_id);
        }
        if self.cached_by_admission {
            let removed = self
                .inner
                .checked_lock_state_mut()
                .remove_cached_exact(self.metadata_resource, LockMode::Shared);
            assert!(
                removed,
                "admission rollback lost its owner-cache record: table_id={}",
                self.table_id
            );
        }
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
            .cached_covers(metadata_resource, LockMode::Shared),
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
    statement_locks: &mut OwnerLockState,
    table_id: TableID,
    metadata_resource: LockResource,
    binding: TransactionTableBinding,
    operation: &'static str,
) -> OperationResult<(Arc<Table>, Arc<TableRuntimeLayout>)> {
    let lock_manager = attachment.engine().lock_manager();
    let owner = inner.checked_lock_state().owner();
    let grant = inner
        .checked_lock_state()
        .acquire_uncached(lock_manager, metadata_resource, LockMode::Shared)
        .await
        .attach_with(|| format!("operation={operation}, table_id={table_id}"))?;
    let fresh_grant = match grant {
        LockGrant::Fresh => FreshLockGuard::new(lock_manager, metadata_resource, owner, grant),
        LockGrant::Existing => None,
    };
    let mut admission_guard =
        AdmissionCommitGuard::new(inner, table_id, metadata_resource, fresh_grant);
    let (table, layout) = admission_guard.commit(binding);

    let released = statement_locks.release_cached(lock_manager, metadata_resource);
    assert_eq!(
        released, 1,
        "successful binding handoff must release statement metadata S: table_id={table_id}"
    );
    drop(admission_guard);
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
    statement_locks: &mut OwnerLockState,
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
    statement_locks
        .acquire(
            attachment.engine().lock_manager(),
            metadata_resource,
            LockMode::Shared,
        )
        .await
        .attach_with(|| format!("operation={operation}, table_id={table_id}"))?;

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
    use crate::error::{Error, OperationError};
    use crate::lock::LockOwner;
    use crate::lock::tests::{LockDebugEntryState, debug_snapshot};
    use crate::value::Val;
    use tempfile::TempDir;

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
        debug_snapshot(engine.lock_manager())
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

    #[test]
    fn first_read_commits_binding_and_releases_statement_metadata_lock() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("admission_first_read_binding").await;
            let table_id = table2(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_owner = LockOwner::Transaction(trx.trx_id());
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
                        .cached_covers(metadata, LockMode::Shared)
                );
            }
            assert!(owner_has_grant(
                &engine,
                trx_owner,
                metadata,
                LockMode::Shared
            ));
            assert!(
                debug_snapshot(engine.lock_manager())
                    .entries
                    .iter()
                    .all(|entry| {
                        entry.resource != metadata
                            || !matches!(entry.owner, LockOwner::Statement(..))
                    })
            );

            trx.rollback().await.unwrap();
            assert!(
                !owner_has_grant(&engine, trx_owner, metadata, LockMode::Shared),
                "terminal rollback must release the binding metadata lock"
            );
            drop(session);
            engine.shutdown().unwrap();
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
                let mut old_trx = old_session.begin_trx().unwrap();
                let trx_owner = LockOwner::Transaction(old_trx.trx_id());
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
                            .cached_covers(metadata, LockMode::Shared)
                    );
                    assert!(!checkout.inner().checked_lock_state().cached_covers(
                        LockResource::TableData(table_id),
                        LockMode::IntentExclusive
                    ));
                }
                let snapshot = debug_snapshot(engine.lock_manager());
                assert!(
                    snapshot
                        .entries
                        .iter()
                        .all(|entry| entry.owner != trx_owner)
                );
                assert!(snapshot.entries.iter().all(|entry| {
                    entry.resource != metadata || !matches!(entry.owner, LockOwner::Statement(..))
                }));

                old_trx.rollback().await.unwrap();
                drop(ddl_session);
                drop(old_session);
                engine.shutdown().unwrap();
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
                let mut old_trx = old_session.begin_trx().unwrap();
                let trx_owner = LockOwner::Transaction(old_trx.trx_id());

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
                    assert!(!checkout.inner().checked_lock_state().cached_covers(
                        LockResource::TableData(table_id),
                        LockMode::IntentExclusive
                    ));
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
                engine.shutdown().unwrap();
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
            let mut create = Box::pin(ddl_session.create_index(
                table_id,
                IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
            ));
            let mut metadata_waiting = false;
            for _ in 0..10 {
                assert!(matches!(
                    futures::poll!(create.as_mut()),
                    std::task::Poll::Pending
                ));
                metadata_waiting =
                    debug_snapshot(engine.lock_manager())
                        .entries
                        .iter()
                        .any(|entry| {
                            entry.resource == metadata
                                && entry.mode == LockMode::Exclusive
                                && entry.state == LockDebugEntryState::Waiting
                        });
                if metadata_waiting {
                    break;
                }
            }
            assert!(
                metadata_waiting,
                "create index metadata lock waiter was not observed after bounded polling"
            );

            bound_trx.commit().await.unwrap();
            assert_eq!(create.await.unwrap(), 1);

            drop(ddl_session);
            drop(bound_session);
            engine.shutdown().unwrap();
        });
    }
}
