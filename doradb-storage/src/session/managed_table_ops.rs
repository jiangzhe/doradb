use super::{PreparedDdlScope, Session, SessionOperationPin};
use crate::catalog::{
    BindingNamespaceID, CreateTableOutcome, CurrentTableDefinition, ID_DOMAIN_END,
    IndexDdlGateScope, IndexID, ManagedDdlError, ManagedDdlResult, ManagedTableDefinitionSnapshot,
    ManagedTableInterpreter, PreparedCreateIndex, PreparedCreateTable, PreparedDropIndex,
    ResolvedTableBinding, StorageTableDefinition, TABLE_ID_TABLE_BINDINGS,
    TABLE_ID_TABLE_DESCRIPTORS, TableBinding, TableDefinitionVersion, ValidatedCreateTable,
    create_index_catalog_write_targets, drop_index_catalog_write_targets,
    managed_create_table_catalog_write_targets, reject_non_user_table_id,
    reject_user_table_primary_key_index, validate_descriptor_payload, validate_table_binding_key,
    validate_table_bindings, validate_table_descriptor_against_metadata,
    validated_index_ddl_target,
};
use crate::error::{
    DataIntegrityError, DiscloseError, DiscloseResultExt, MultiDomainResultExt, OperationError,
    OperationOrFatalResult, Result, RuntimeError,
};
use crate::id::TableID;
use crate::lock::{FreshClaimsGuard, LockMode, LockResource};
use crate::trx::SessionOperationKind;
use error_stack::{Report, ResultExt};
use std::future::Future;
use std::sync::Arc;

/// Managed table-definition operations implemented by [`Session`].
pub trait ManagedTableOps {
    /// Interprets and creates one managed table from opaque higher-layer bytes.
    ///
    /// Binding collisions are returned in the engine-error arm as
    /// [`OperationError::DuplicateKey`] or, for a concurrent ownership race,
    /// [`OperationError::WriteConflict`].
    fn create_managed_table<I>(
        &mut self,
        source: &[u8],
        interpreter: &mut I,
    ) -> impl Future<Output = ManagedDdlResult<CreateTableOutcome, I::Error>>
    where
        I: ManagedTableInterpreter;

    /// Interprets and creates one managed secondary index from opaque bytes.
    fn create_managed_index<I>(
        &mut self,
        table_id: TableID,
        source: &[u8],
        interpreter: &mut I,
    ) -> impl Future<Output = ManagedDdlResult<IndexID, I::Error>>
    where
        I: ManagedTableInterpreter;

    /// Interprets and drops one managed secondary index from opaque bytes.
    fn drop_managed_index<I>(
        &mut self,
        table_id: TableID,
        source: &[u8],
        interpreter: &mut I,
    ) -> impl Future<Output = ManagedDdlResult<(), I::Error>>
    where
        I: ManagedTableInterpreter;

    /// Resolves one binding at an admitted current point.
    ///
    /// The returned version is an optimistic cache token. No lock survives the
    /// call, so equality with a later resolution does not guard intervening
    /// planning or execution.
    fn resolve_table_binding(
        &mut self,
        namespace_id: BindingNamespaceID,
        binding_key: &[u8],
        include_full_schema: bool,
    ) -> impl Future<Output = Result<Option<ResolvedTableBinding>>>;

    /// Lists every roleless binding for one current managed table.
    fn list_table_bindings(
        &mut self,
        table_id: TableID,
    ) -> impl Future<Output = Result<Box<[TableBinding]>>>;
}

impl ManagedTableOps for Session {
    async fn create_managed_table<I>(
        &mut self,
        source: &[u8],
        interpreter: &mut I,
    ) -> ManagedDdlResult<CreateTableOutcome, I::Error>
    where
        I: ManagedTableInterpreter,
    {
        let definition = interpreter
            .create_table(source)
            .map_err(ManagedDdlError::Interpreter)?;
        validate_descriptor_payload(definition.descriptor())
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        validate_table_bindings(definition.bindings())
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let (storage, descriptor, bindings) = definition.into_parts();
        let (table_spec, index_specs) = storage.into_parts();
        let validated = ValidatedCreateTable::try_new(table_spec, index_specs.into_vec())
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach("operation=create_managed_table")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        let prepared = operation
            .prepare_managed_create_table(validated, descriptor, bindings)
            .await
            .map_err(ManagedDdlError::Engine)?;
        let observer = mandatory_runtime
            .submit(prepared)
            .await
            .attach("operation=create_managed_table")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        drop(mandatory_runtime);
        observer
            .wait()
            .await
            .map_err(|error| error.into_quad(RuntimeError::CatalogAccess))
            .attach("operation=create_managed_table, phase=wait_mandatory_completion")
            .disclose()
            .map_err(ManagedDdlError::Engine)
    }

    async fn create_managed_index<I>(
        &mut self,
        table_id: TableID,
        source: &[u8],
        interpreter: &mut I,
    ) -> ManagedDdlResult<IndexID, I::Error>
    where
        I: ManagedTableInterpreter,
    {
        let current = self
            .read_managed_current_definition(table_id, "create_managed_index")
            .await
            .map_err(ManagedDdlError::Engine)?;
        let effective_next_index_id = current.effective_next_index_id();
        if effective_next_index_id == ID_DOMAIN_END {
            return Err(ManagedDdlError::Engine(
                Report::new(OperationError::IndexIdExhausted)
                    .attach(format!("table_id={table_id}"))
                    .disclose(),
            ));
        }
        let proposed_index_id = IndexID::new(effective_next_index_id as u32);
        let update = interpreter
            .create_index(
                source,
                &current.descriptor().payload,
                current.schema(),
                proposed_index_id,
            )
            .map_err(ManagedDdlError::Interpreter)?;
        validate_descriptor_payload(update.descriptor())
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let (change, descriptor) = update.into_parts();
        let index_spec = change
            .compile(current.schema())
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        reject_user_table_primary_key_index(&index_spec, "create_managed_index")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;

        let operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach("operation=create_managed_index")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        operation
            .reject_table_ddl_explicit_session_lock(table_id)
            .attach("operation=create_managed_index")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let scope = PreparedDdlScope::create_index(
            operation,
            table_id,
            create_index_catalog_write_targets(),
        )
        .await
        .attach_with(|| format!("prepare managed CREATE INDEX locks: table_id={table_id}"))
        .disclose()
        .map_err(ManagedDdlError::Engine)?;
        let engine = scope.engine();
        let table = validated_index_ddl_target(
            engine,
            engine.pool_guards(),
            table_id,
            "create_managed_index",
        )
        .await
        .disclose()
        .map_err(ManagedDdlError::Engine)?;
        engine
            .poisoner
            .ensure_healthy()
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let gates = IndexDdlGateScope::acquire(Arc::clone(&table), engine.catalog_guard())
            .await
            .attach("operation=create_managed_index")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let current_descriptor = engine
            .catalog()
            .storage
            .table_descriptors()
            .find_uncommitted_by_table_id(engine.pool_guards(), table_id)
            .await
            .disclose()
            .map_err(ManagedDdlError::Engine)?
            .ok_or_else(|| managed_schema_changed(table_id, "create_managed_index"))
            .map_err(ManagedDdlError::Engine)?;
        let plan = table
            .finalize_managed_create_index(&current, current_descriptor, index_spec, descriptor)
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        if plan.index().id() != proposed_index_id {
            return Err(ManagedDdlError::Engine(
                    Report::new(OperationError::SchemaChanged)
                        .attach(format!(
                            "managed CREATE INDEX proposed identity changed: table_id={table_id}, proposed={proposed_index_id}, finalized={}",
                            plan.index().id()
                        ))
                        .disclose(),
                ));
        }
        if plan.skipped_retired_runtime() {
            engine.trx_sys.request_retired_index_runtime_purge(table_id);
        }
        let observer = mandatory_runtime
            .submit(PreparedCreateIndex::new(gates, scope, plan))
            .await
            .attach("operation=create_managed_index")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        drop(mandatory_runtime);
        observer
                .wait()
                .await
                .map_err(|error| error.into_quad(RuntimeError::IndexAccess))
                .attach_with(|| {
                    format!(
                        "operation=create_managed_index, phase=wait_mandatory_completion, table_id={table_id}"
                    )
                })
                .disclose()
                .map_err(ManagedDdlError::Engine)
    }

    async fn drop_managed_index<I>(
        &mut self,
        table_id: TableID,
        source: &[u8],
        interpreter: &mut I,
    ) -> ManagedDdlResult<(), I::Error>
    where
        I: ManagedTableInterpreter,
    {
        let current = self
            .read_managed_current_definition(table_id, "drop_managed_index")
            .await
            .map_err(ManagedDdlError::Engine)?;
        let update = interpreter
            .drop_index(source, &current.descriptor().payload, current.schema())
            .map_err(ManagedDdlError::Interpreter)?;
        validate_descriptor_payload(update.descriptor())
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let (change, descriptor) = update.into_parts();
        let index_id = change
            .validate(current.schema())
            .disclose()
            .map_err(ManagedDdlError::Engine)?;

        let operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach("operation=drop_managed_index")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let mandatory_runtime = operation.runtime.mandatory_runtime.clone();
        operation
            .reject_table_ddl_explicit_session_lock(table_id)
            .attach("operation=drop_managed_index")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let scope =
            PreparedDdlScope::drop_index(operation, table_id, drop_index_catalog_write_targets())
                .await
                .attach_with(|| format!("prepare managed DROP INDEX locks: table_id={table_id}"))
                .disclose()
                .map_err(ManagedDdlError::Engine)?;
        let engine = scope.engine();
        let table = validated_index_ddl_target(
            engine,
            engine.pool_guards(),
            table_id,
            "drop_managed_index",
        )
        .await
        .disclose()
        .map_err(ManagedDdlError::Engine)?;
        engine
            .poisoner
            .ensure_healthy()
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let gates = IndexDdlGateScope::acquire(Arc::clone(&table), engine.catalog_guard())
            .await
            .attach("operation=drop_managed_index")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let current_descriptor = engine
            .catalog()
            .storage
            .table_descriptors()
            .find_uncommitted_by_table_id(engine.pool_guards(), table_id)
            .await
            .disclose()
            .map_err(ManagedDdlError::Engine)?
            .ok_or_else(|| managed_schema_changed(table_id, "drop_managed_index"))
            .map_err(ManagedDdlError::Engine)?;
        let plan = table
            .finalize_managed_drop_index(&current, current_descriptor, index_id, descriptor)
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let observer = mandatory_runtime
            .submit(PreparedDropIndex::new(gates, scope, plan))
            .await
            .attach("operation=drop_managed_index")
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        drop(mandatory_runtime);
        observer
                .wait()
                .await
                .map_err(|error| error.into_quad(RuntimeError::IndexAccess))
                .attach_with(|| {
                    format!(
                        "operation=drop_managed_index, phase=wait_mandatory_completion, table_id={table_id}, index_id={index_id}"
                    )
                })
                .disclose()
                .map_err(ManagedDdlError::Engine)
    }

    async fn resolve_table_binding(
        &mut self,
        namespace_id: BindingNamespaceID,
        binding_key: &[u8],
        include_full_schema: bool,
    ) -> Result<Option<ResolvedTableBinding>> {
        validate_table_binding_key(binding_key).disclose()?;
        let mut candidate = match self.probe_table_binding(namespace_id, binding_key).await? {
            Some(table_id) => table_id,
            None => return Ok(None),
        };

        loop {
            let mut operation = self
                .pin_operation(SessionOperationKind::Ddl)
                .attach("operation=resolve_table_binding, phase=final_admission")
                .disclose()?;
            operation
                .acquire_table_binding_resolution(candidate, include_full_schema)
                .await
                .attach_with(|| {
                    format!(
                        "operation=resolve_table_binding, phase=acquire_final_claims, table_id={candidate}"
                    )
                })
                .disclose()?;
            let engine = &operation.runtime;
            let rebound = engine
                .catalog()
                .storage
                .table_bindings()
                .find_uncommitted_table_id(engine.pool_guards(), namespace_id, binding_key)
                .await
                .disclose()?;
            let Some(rebound) = rebound else {
                return Ok(None);
            };
            if rebound != candidate {
                candidate = rebound;
                drop(operation);
                continue;
            }

            let table = engine
                .catalog()
                .current_live_user_table(candidate)
                .ok_or_else(|| invalid_binding_target(candidate, "current runtime is missing"))?;
            if !table.definition_kind().is_managed() {
                return Err(invalid_binding_target(
                    candidate,
                    "binding targets an unmanaged runtime",
                ));
            }
            let layout = table.layout_snapshot();
            let version = TableDefinitionVersion::new(candidate, layout.metadata().storage_epoch);
            let full_schema = if include_full_schema {
                let descriptor = engine
                    .catalog()
                    .storage
                    .table_descriptors()
                    .find_uncommitted_by_table_id(engine.pool_guards(), candidate)
                    .await
                    .disclose()?
                    .ok_or_else(|| {
                        invalid_binding_target(candidate, "managed descriptor is missing")
                    })?;
                validate_table_descriptor_against_metadata(
                    &descriptor,
                    candidate,
                    layout.metadata(),
                )
                .attach("operation=resolve_table_binding, phase=validate_full_definition")
                .disclose()?;
                Some(ManagedTableDefinitionSnapshot::new(
                    StorageTableDefinition::from_metadata(layout.metadata()),
                    descriptor.payload,
                ))
            } else {
                None
            };
            return Ok(Some(ResolvedTableBinding::new(
                candidate,
                version,
                full_schema,
            )));
        }
    }

    async fn list_table_bindings(&mut self, table_id: TableID) -> Result<Box<[TableBinding]>> {
        reject_non_user_table_id(table_id, "list_table_bindings").disclose()?;
        let mut operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach("operation=list_table_bindings")
            .disclose()?;
        operation
            .acquire_table_binding_list(table_id)
            .await
            .attach_with(|| {
                format!("operation=list_table_bindings, phase=acquire_claims, table_id={table_id}")
            })
            .disclose()?;
        let engine = &operation.runtime;
        let table = engine
            .catalog()
            .current_live_user_table(table_id)
            .ok_or_else(|| {
                Report::new(OperationError::TableNotFound)
                    .attach(format!(
                        "list table bindings current-live lookup: table_id={table_id}"
                    ))
                    .disclose()
            })?;
        if !table.definition_kind().is_managed() {
            return Err(Report::new(OperationError::InvalidMetadata)
                .attach(format!(
                    "list_table_bindings requires managed table: table_id={table_id}"
                ))
                .disclose());
        }
        let bindings = engine
            .catalog()
            .storage
            .table_bindings()
            .list_uncommitted_by_table_id(engine.pool_guards(), table_id)
            .await
            .disclose()?
            .into_iter()
            .map(|binding| TableBinding::new(binding.namespace_id, binding.binding_key))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(bindings)
    }
}

impl Session {
    async fn probe_table_binding(
        &mut self,
        namespace_id: BindingNamespaceID,
        binding_key: &[u8],
    ) -> Result<Option<TableID>> {
        let mut operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach("operation=resolve_table_binding, phase=probe")
            .disclose()?;
        operation
            .acquire_table_binding_probe()
            .await
            .attach("operation=resolve_table_binding, phase=acquire_probe_claims")
            .disclose()?;
        let result = operation
            .runtime
            .catalog()
            .storage
            .table_bindings()
            .find_uncommitted_table_id(operation.runtime.pool_guards(), namespace_id, binding_key)
            .await
            .disclose()?;
        drop(operation);
        #[cfg(test)]
        tests::pause_after_binding_probe(namespace_id, binding_key);
        Ok(result)
    }

    async fn read_managed_current_definition(
        &mut self,
        table_id: TableID,
        operation_name: &'static str,
    ) -> Result<CurrentTableDefinition> {
        reject_non_user_table_id(table_id, operation_name).disclose()?;
        let mut operation = self
            .pin_operation(SessionOperationKind::Ddl)
            .attach_with(|| format!("operation={operation_name}, phase=definition_preflight"))
            .disclose()?;
        operation
            .reject_table_ddl_explicit_session_lock(table_id)
            .attach_with(|| format!("operation={operation_name}"))
            .disclose()?;
        operation
            .acquire_managed_definition_read(table_id)
            .await
            .attach_with(|| {
                format!(
                    "operation={operation_name}, phase=acquire_definition_read, table_id={table_id}"
                )
            })
            .disclose()?;
        let current = {
            let engine = &operation.runtime;
            let table =
                validated_index_ddl_target(engine, engine.pool_guards(), table_id, operation_name)
                    .await
                    .disclose()?;
            let descriptor = engine
                .catalog()
                .storage
                .table_descriptors()
                .find_uncommitted_by_table_id(engine.pool_guards(), table_id)
                .await
                .disclose()?
                .ok_or_else(|| {
                    Report::new(OperationError::InvalidMetadata)
                        .attach(format!(
                            "managed DDL requires a descriptor row: operation={operation_name}, table_id={table_id}"
                        ))
                        .disclose()
                })?;
            table.current_managed_definition(descriptor).disclose()?
        };
        // Dropping the complete operation scope releases target metadata-S and
        // catalog read admission before arbitrary interpreter code can run.
        drop(operation);
        Ok(current)
    }
}

impl SessionOperationPin {
    /// Acquires the short binding-only probe scope.
    async fn acquire_table_binding_probe(&mut self) -> OperationOrFatalResult<()> {
        let (engine, family, curr_scope) = self.operation_lock_parts();
        let mut fresh =
            FreshClaimsGuard::<2>::new(family, curr_scope, engine.lock_manager(), &engine.poisoner);
        fresh
            .acquire(
                LockResource::TableMetadata(TABLE_ID_TABLE_BINDINGS),
                LockMode::Shared,
            )
            .await?;
        fresh
            .acquire(
                LockResource::TableData(TABLE_ID_TABLE_BINDINGS),
                LockMode::IntentShared,
            )
            .await?;
        fresh.disarm();
        Ok(())
    }

    /// Acquires target-first claims for the coherent final resolution pass.
    async fn acquire_table_binding_resolution(
        &mut self,
        table_id: TableID,
        include_full_schema: bool,
    ) -> OperationOrFatalResult<()> {
        let (engine, family, curr_scope) = self.operation_lock_parts();
        let mut fresh =
            FreshClaimsGuard::<5>::new(family, curr_scope, engine.lock_manager(), &engine.poisoner);
        fresh
            .acquire(LockResource::TableMetadata(table_id), LockMode::Shared)
            .await?;
        if include_full_schema {
            fresh
                .acquire(
                    LockResource::TableMetadata(TABLE_ID_TABLE_DESCRIPTORS),
                    LockMode::Shared,
                )
                .await?;
        }
        fresh
            .acquire(
                LockResource::TableMetadata(TABLE_ID_TABLE_BINDINGS),
                LockMode::Shared,
            )
            .await?;
        if include_full_schema {
            fresh
                .acquire(
                    LockResource::TableData(TABLE_ID_TABLE_DESCRIPTORS),
                    LockMode::IntentShared,
                )
                .await?;
        }
        fresh
            .acquire(
                LockResource::TableData(TABLE_ID_TABLE_BINDINGS),
                LockMode::IntentShared,
            )
            .await?;
        fresh.disarm();
        Ok(())
    }

    /// Acquires target-first claims for reverse binding enumeration.
    async fn acquire_table_binding_list(
        &mut self,
        table_id: TableID,
    ) -> OperationOrFatalResult<()> {
        let (engine, family, curr_scope) = self.operation_lock_parts();
        let mut fresh =
            FreshClaimsGuard::<3>::new(family, curr_scope, engine.lock_manager(), &engine.poisoner);
        fresh
            .acquire(LockResource::TableMetadata(table_id), LockMode::Shared)
            .await?;
        fresh
            .acquire(
                LockResource::TableMetadata(TABLE_ID_TABLE_BINDINGS),
                LockMode::Shared,
            )
            .await?;
        fresh
            .acquire(
                LockResource::TableData(TABLE_ID_TABLE_BINDINGS),
                LockMode::IntentShared,
            )
            .await?;
        fresh.disarm();
        Ok(())
    }

    /// Acquires the short managed-definition read set in canonical order.
    async fn acquire_managed_definition_read(
        &mut self,
        table_id: TableID,
    ) -> OperationOrFatalResult<()> {
        let (engine, family, curr_scope) = self.operation_lock_parts();
        let mut fresh =
            FreshClaimsGuard::<3>::new(family, curr_scope, engine.lock_manager(), &engine.poisoner);
        fresh
            .acquire(LockResource::TableMetadata(table_id), LockMode::Shared)
            .await?;
        fresh
            .acquire(
                LockResource::TableMetadata(TABLE_ID_TABLE_DESCRIPTORS),
                LockMode::Shared,
            )
            .await?;
        fresh
            .acquire(
                LockResource::TableData(TABLE_ID_TABLE_DESCRIPTORS),
                LockMode::IntentShared,
            )
            .await?;
        fresh.disarm();
        Ok(())
    }

    /// Prepare managed CREATE TABLE after interpretation and validation.
    async fn prepare_managed_create_table(
        self,
        validated: ValidatedCreateTable,
        descriptor: Box<[u8]>,
        bindings: Box<[TableBinding]>,
    ) -> Result<PreparedCreateTable> {
        let table_id = self.runtime.catalog().next_table_id();
        let has_bindings = !bindings.is_empty();
        let targets = managed_create_table_catalog_write_targets(has_bindings);
        let scope = PreparedDdlScope::create(self, table_id, targets)
            .await
            .attach_with(|| format!("prepare managed CREATE TABLE locks: table_id={table_id}"))
            .disclose()?;
        if has_bindings {
            scope
                .engine()
                .catalog()
                .storage
                .table_bindings()
                .precheck_create_keys_absent(scope.engine().pool_guards(), &bindings)
                .await
                .attach_with(|| {
                    format!(
                        "operation=create_managed_table, phase=check_binding_uniqueness, table_id={table_id}"
                    )
                })
                .disclose()?;
        }
        let plan = validated.into_managed_plan(table_id, descriptor, bindings);
        Ok(PreparedCreateTable::new(scope, plan))
    }
}

#[inline]
fn managed_schema_changed(table_id: TableID, operation: &'static str) -> crate::Error {
    Report::new(OperationError::SchemaChanged)
        .attach(format!(
            "managed descriptor disappeared during revalidation: operation={operation}, table_id={table_id}"
        ))
        .disclose()
}

#[inline]
fn invalid_binding_target(table_id: TableID, reason: &'static str) -> crate::Error {
    Report::new(DataIntegrityError::InvalidRootInvariant)
        .attach(format!(
            "managed table binding integrity failure: table_id={table_id}, reason={reason}"
        ))
        .disclose()
}

#[cfg(test)]
mod tests {
    use crate::catalog::storage::tests::begin_catalog_test_trx;
    use crate::catalog::table::{
        CreateTableTestFailure, install_create_before_first_effect_gate,
        reset_storage_schema_fingerprint_count, set_create_table_failure,
        storage_schema_fingerprint_count, user_table_file_exists,
    };
    use crate::catalog::{
        TABLE_ID_TABLE_BINDINGS, TABLE_ID_TABLE_DESCRIPTORS, TableBindingObject,
        TableDescriptorObject, TableDescriptors,
    };
    use crate::id::{SessionID, TableID};
    use crate::lock::tests::TestLockOwner;
    use crate::lock::{LockMode, LockOwner, LockResource};
    use crate::log::redo::DDLRedo;
    use crate::map::FastHashMap;
    use crate::session::tests::SessionTestExt;
    use crate::table::tests::lock_entry_count;
    use crate::trx::SessionOperationKind;
    use crate::{
        BindingNamespaceID, Engine, EngineConfig, Error, ErrorKind, IndexID, ManagedTableOps,
        OperationError, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec, StorageTableSpec, TableBinding, ValKind,
    };
    use std::collections::hash_map::Entry;
    use std::result::Result as StdResult;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Barrier, Mutex, OnceLock, mpsc};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    static BINDING_PROBE_PAUSES: OnceLock<BindingProbePauseRegistry> = OnceLock::new();

    type BindingProbePauseRegistry =
        Mutex<FastHashMap<(BindingNamespaceID, Box<[u8]>), BindingProbePause>>;

    struct BindingProbePause {
        entered: mpsc::SyncSender<()>,
        release: mpsc::Receiver<()>,
    }

    struct ManagedRoundTripInterpreter {
        initial_descriptor: Vec<u8>,
        created_index_descriptor: Vec<u8>,
        dropped_index_descriptor: Vec<u8>,
        create_index_calls: usize,
        drop_index_calls: usize,
        bindings: Vec<TableBinding>,
    }

    impl crate::ManagedTableInterpreter for ManagedRoundTripInterpreter {
        type Error = &'static str;

        fn create_table(
            &mut self,
            source: &[u8],
        ) -> StdResult<crate::ManagedCreateTableDefinition, Self::Error> {
            assert_eq!(source, [0xff, 0x00, 0xfe]);
            Ok(crate::ManagedCreateTableDefinition::new(
                crate::CreateTableDefinition::new(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::I64, StorageColumnFlags::NULLABLE),
                    ]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                ),
                self.initial_descriptor.clone(),
                self.bindings.clone(),
            ))
        }

        fn create_index(
            &mut self,
            source: &[u8],
            previous_descriptor: &[u8],
            current_schema: &crate::StorageTableDefinition,
            proposed_index_id: IndexID,
        ) -> StdResult<crate::DescriptorUpdate<crate::CreateIndexDefinition>, Self::Error> {
            self.create_index_calls += 1;
            assert_eq!(source, [0x80, 0x81]);
            assert_eq!(previous_descriptor, self.initial_descriptor);
            assert_eq!(current_schema.columns().len(), 2);
            assert_eq!(
                current_schema.columns()[0].column_id(),
                crate::ColumnID::new(0)
            );
            assert_eq!(
                current_schema.columns()[1].column_id(),
                crate::ColumnID::new(1)
            );
            assert_eq!(current_schema.indexes().len(), 1);
            assert_eq!(current_schema.indexes()[0].index_id(), IndexID::new(0));
            assert_eq!(proposed_index_id, IndexID::new(1));
            Ok(crate::DescriptorUpdate::new(
                crate::CreateIndexDefinition::new(
                    vec![crate::StorageIndexKeyByColumnId::new(crate::ColumnID::new(
                        1,
                    ))],
                    StorageIndexFlags::empty(),
                ),
                self.created_index_descriptor.clone(),
            ))
        }

        fn drop_index(
            &mut self,
            source: &[u8],
            previous_descriptor: &[u8],
            current_schema: &crate::StorageTableDefinition,
        ) -> StdResult<crate::DescriptorUpdate<crate::DropIndexDefinition>, Self::Error> {
            self.drop_index_calls += 1;
            assert_eq!(source, [0x82]);
            assert_eq!(previous_descriptor, self.created_index_descriptor);
            assert_eq!(current_schema.indexes().len(), 2);
            assert_eq!(current_schema.indexes()[0].index_id(), IndexID::new(0));
            assert_eq!(current_schema.indexes()[1].index_id(), IndexID::new(1));
            Ok(crate::DescriptorUpdate::new(
                crate::DropIndexDefinition::new(IndexID::new(1)),
                self.dropped_index_descriptor.clone(),
            ))
        }
    }

    struct CreateFailureInterpreter {
        result: StdResult<Vec<u8>, &'static str>,
        bindings: Vec<TableBinding>,
    }

    impl crate::ManagedTableInterpreter for CreateFailureInterpreter {
        type Error = &'static str;

        fn create_table(
            &mut self,
            _source: &[u8],
        ) -> StdResult<crate::ManagedCreateTableDefinition, Self::Error> {
            let descriptor = self.result.clone()?;
            Ok(crate::ManagedCreateTableDefinition::new(
                crate::CreateTableDefinition::new(
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::I32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![],
                ),
                descriptor,
                self.bindings.clone(),
            ))
        }

        fn create_index(
            &mut self,
            _source: &[u8],
            _previous_descriptor: &[u8],
            _current_schema: &crate::StorageTableDefinition,
            _proposed_index_id: IndexID,
        ) -> StdResult<crate::DescriptorUpdate<crate::CreateIndexDefinition>, Self::Error> {
            unreachable!()
        }

        fn drop_index(
            &mut self,
            _source: &[u8],
            _previous_descriptor: &[u8],
            _current_schema: &crate::StorageTableDefinition,
        ) -> StdResult<crate::DescriptorUpdate<crate::DropIndexDefinition>, Self::Error> {
            unreachable!()
        }
    }

    struct CreateIndexOnlyInterpreter {
        barrier: Option<Arc<Barrier>>,
        calls: Arc<AtomicUsize>,
        expected_index_count: usize,
        expected_proposed: IndexID,
        descriptor: Vec<u8>,
    }

    impl crate::ManagedTableInterpreter for CreateIndexOnlyInterpreter {
        type Error = &'static str;

        fn create_table(
            &mut self,
            _source: &[u8],
        ) -> StdResult<crate::ManagedCreateTableDefinition, Self::Error> {
            unreachable!()
        }

        fn create_index(
            &mut self,
            _source: &[u8],
            _previous_descriptor: &[u8],
            current_schema: &crate::StorageTableDefinition,
            proposed_index_id: IndexID,
        ) -> StdResult<crate::DescriptorUpdate<crate::CreateIndexDefinition>, Self::Error> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            assert_eq!(current_schema.indexes().len(), self.expected_index_count);
            assert_eq!(proposed_index_id, self.expected_proposed);
            if let Some(barrier) = &self.barrier {
                barrier.wait();
            }
            Ok(crate::DescriptorUpdate::new(
                crate::CreateIndexDefinition::new(
                    vec![crate::StorageIndexKeyByColumnId::new(crate::ColumnID::new(
                        0,
                    ))],
                    StorageIndexFlags::empty(),
                ),
                self.descriptor.clone(),
            ))
        }

        fn drop_index(
            &mut self,
            _source: &[u8],
            _previous_descriptor: &[u8],
            _current_schema: &crate::StorageTableDefinition,
        ) -> StdResult<crate::DescriptorUpdate<crate::DropIndexDefinition>, Self::Error> {
            unreachable!()
        }
    }

    /// Pauses one matching resolver after its binding-only probe scope closes.
    pub(super) fn pause_after_binding_probe(namespace_id: BindingNamespaceID, binding_key: &[u8]) {
        let key = (namespace_id, Box::<[u8]>::from(binding_key));
        let pause = BINDING_PROBE_PAUSES
            .get_or_init(|| Mutex::new(FastHashMap::default()))
            .lock()
            .unwrap()
            .remove(&key);
        if let Some(pause) = pause {
            pause.entered.send(()).unwrap();
            pause.release.recv().unwrap();
        }
    }

    fn install_binding_probe_pause(
        namespace_id: BindingNamespaceID,
        binding_key: impl Into<Box<[u8]>>,
    ) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let pause = BindingProbePause {
            entered: entered_tx,
            release: release_rx,
        };
        let key = (namespace_id, binding_key.into());
        let mut pauses = BINDING_PROBE_PAUSES
            .get_or_init(|| Mutex::new(FastHashMap::default()))
            .lock()
            .unwrap();
        let duplicate = match pauses.entry(key) {
            Entry::Occupied(_) => true,
            Entry::Vacant(entry) => {
                entry.insert(pause);
                false
            }
        };
        drop(pauses);
        assert!(!duplicate, "binding probe pause already installed for key");
        (entered_rx, release_tx)
    }

    #[test]
    fn test_binding_probe_pauses_are_keyed() {
        let first_namespace_id = BindingNamespaceID::new(90_200);
        let second_namespace_id = BindingNamespaceID::new(90_201);
        let (first_entered, first_release) =
            install_binding_probe_pause(first_namespace_id, &b"first"[..]);
        let (second_entered, second_release) =
            install_binding_probe_pause(second_namespace_id, &b"second"[..]);

        thread::scope(|scope| {
            let first = scope.spawn(|| {
                pause_after_binding_probe(first_namespace_id, b"first");
            });
            let second = scope.spawn(|| {
                pause_after_binding_probe(second_namespace_id, b"second");
            });

            first_entered.recv().unwrap();
            second_entered.recv().unwrap();
            first_release.send(()).unwrap();
            second_release.send(()).unwrap();
            first.join().unwrap();
            second.join().unwrap();
        });
    }

    #[test]
    fn test_binding_probe_acquisition_cancellation_releases_every_claim() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let manager = engine.inner().core.lock_manager();
            let blocker_owner = LockOwner::session_explicit(SessionID::new(90_100));
            let mut blocker = TestLockOwner::new(blocker_owner);
            blocker
                .acquire(
                    manager,
                    LockResource::TableData(TABLE_ID_TABLE_BINDINGS),
                    LockMode::Exclusive,
                )
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let resolver_owner = LockOwner::session_explicit(session.id());
            let mut operation = session.pin_operation(SessionOperationKind::Ddl).unwrap();
            let mut acquire = Box::pin(operation.acquire_table_binding_probe());

            assert!(matches!(
                futures::poll!(acquire.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(lock_entry_count(&engine, resolver_owner), 2);
            drop(acquire);
            assert_eq!(lock_entry_count(&engine, resolver_owner), 0);
            assert_eq!(lock_entry_count(&engine, blocker_owner), 1);

            drop(operation);
            blocker.close(manager);
        });
    }

    #[test]
    fn test_binding_final_acquisition_cancellation_releases_every_claim() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let manager = engine.inner().core.lock_manager();
            let blocker_owner = LockOwner::session_explicit(SessionID::new(90_101));
            let mut blocker = TestLockOwner::new(blocker_owner);
            blocker
                .acquire(
                    manager,
                    LockResource::TableData(TABLE_ID_TABLE_DESCRIPTORS),
                    LockMode::Exclusive,
                )
                .await
                .unwrap();
            let session = engine.new_session().unwrap();
            let resolver_owner = LockOwner::session_explicit(session.id());
            let mut operation = session.pin_operation(SessionOperationKind::Ddl).unwrap();
            let mut acquire =
                Box::pin(operation.acquire_table_binding_resolution(TableID::new(90_102), true));

            assert!(matches!(
                futures::poll!(acquire.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(lock_entry_count(&engine, resolver_owner), 4);
            drop(acquire);
            assert_eq!(lock_entry_count(&engine, resolver_owner), 0);
            assert_eq!(lock_entry_count(&engine, blocker_owner), 1);

            drop(operation);
            blocker.close(manager);
        });
    }

    #[test]
    fn test_list_table_bindings_reports_unallocated_table_as_not_found() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();

            let err = session
                .list_table_bindings(TableID::new(90_000))
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Operation);
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        });
    }

    #[test]
    fn test_managed_ddl_round_trips_descriptor_and_rejects_unmanaged_index_changes() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let initial_descriptor = (0..crate::MAX_TABLE_DESCRIPTOR_BYTES)
                .map(|idx| idx as u8)
                .collect::<Vec<_>>();
            let created_index_descriptor = vec![0xff, 0x01, 0x00, 0x80];
            let dropped_index_descriptor = Vec::new();
            let mut interpreter = ManagedRoundTripInterpreter {
                initial_descriptor: initial_descriptor.clone(),
                created_index_descriptor: created_index_descriptor.clone(),
                dropped_index_descriptor: dropped_index_descriptor.clone(),
                create_index_calls: 0,
                drop_index_calls: 0,
                bindings: vec![
                    TableBinding::new(BindingNamespaceID::new(2), &b"zeta"[..]),
                    TableBinding::new(BindingNamespaceID::new(1), &b"beta"[..]),
                    TableBinding::new(BindingNamespaceID::new(1), &b"alpha"[..]),
                ],
            };

            let outcome = session
                .create_managed_table(&[0xff, 0x00, 0xfe], &mut interpreter)
                .await
                .unwrap();
            let table_id = outcome.table_id();
            assert_eq!(outcome.index_ids(), [IndexID::new(0)]);
            crate::StorageTableDefinition::reset_projection_count();
            TableDescriptors::reset_lookup_count();
            reset_storage_schema_fingerprint_count();
            let narrow = session
                .resolve_table_binding(BindingNamespaceID::new(1), b"alpha", false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(narrow.table_id(), table_id);
            assert!(narrow.full_schema().is_none());
            assert_eq!(crate::StorageTableDefinition::projection_count(), 0);
            assert_eq!(TableDescriptors::lookup_count(), 0);
            assert_eq!(storage_schema_fingerprint_count(), 0);
            let full = session
                .resolve_table_binding(BindingNamespaceID::new(1), b"alpha", true)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(full.version(), narrow.version());
            let second_binding = session
                .resolve_table_binding(BindingNamespaceID::new(1), b"beta", false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(second_binding.version(), narrow.version());
            let snapshot = full.full_schema().unwrap();
            assert_eq!(crate::StorageTableDefinition::projection_count(), 1);
            assert_eq!(TableDescriptors::lookup_count(), 1);
            assert_eq!(storage_schema_fingerprint_count(), 1);
            assert_eq!(snapshot.schema().columns().len(), 2);
            assert_eq!(snapshot.schema().indexes().len(), 1);
            assert_eq!(snapshot.descriptor(), initial_descriptor);
            assert_eq!(
                &*session.list_table_bindings(table_id).await.unwrap(),
                [
                    TableBinding::new(BindingNamespaceID::new(1), &b"alpha"[..]),
                    TableBinding::new(BindingNamespaceID::new(1), &b"beta"[..]),
                    TableBinding::new(BindingNamespaceID::new(2), &b"zeta"[..]),
                ]
            );
            let runtime = session.engine();
            let descriptor = runtime
                .catalog()
                .storage
                .table_descriptors()
                .find_uncommitted_by_table_id(runtime.pool_guards(), table_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(descriptor.descriptor_revision, 0);
            assert_eq!(descriptor.compiled_storage_epoch, 0);
            assert_eq!(&*descriptor.payload, initial_descriptor);
            runtime
                .catalog()
                .validate_live_table_descriptors(runtime.pool_guards())
                .await
                .unwrap();
            session.checkpoint_catalog().await.unwrap();

            let err = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::InvalidMetadata));

            let index_id = session
                .create_managed_index(table_id, &[0x80, 0x81], &mut interpreter)
                .await
                .unwrap();
            assert_eq!(index_id, IndexID::new(1));
            assert_eq!(interpreter.create_index_calls, 1);
            let after_create = session
                .resolve_table_binding(BindingNamespaceID::new(1), b"alpha", false)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(after_create.version(), narrow.version());
            let descriptor = runtime
                .catalog()
                .storage
                .table_descriptors()
                .find_uncommitted_by_table_id(runtime.pool_guards(), table_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(descriptor.descriptor_revision, 1);
            assert_eq!(descriptor.compiled_storage_epoch, 1);
            assert_eq!(&*descriptor.payload, created_index_descriptor);

            let err = session.drop_index(table_id, index_id).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::InvalidMetadata));

            session
                .drop_managed_index(table_id, &[0x82], &mut interpreter)
                .await
                .unwrap();
            assert_eq!(interpreter.drop_index_calls, 1);
            let after_drop = session
                .resolve_table_binding(BindingNamespaceID::new(1), b"alpha", false)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(after_drop.version(), after_create.version());
            let descriptor = runtime
                .catalog()
                .storage
                .table_descriptors()
                .find_uncommitted_by_table_id(runtime.pool_guards(), table_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(descriptor.descriptor_revision, 2);
            assert_eq!(descriptor.compiled_storage_epoch, 2);
            assert_eq!(&*descriptor.payload, dropped_index_descriptor);

            session.drop_table(table_id).await.unwrap();
            let err = session.list_table_bindings(table_id).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Operation);
            assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
            for (namespace_id, key) in [
                (BindingNamespaceID::new(1), &b"alpha"[..]),
                (BindingNamespaceID::new(1), &b"beta"[..]),
                (BindingNamespaceID::new(2), &b"zeta"[..]),
            ] {
                assert!(
                    session
                        .resolve_table_binding(namespace_id, key, false)
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            assert!(
                runtime
                    .catalog()
                    .storage
                    .table_descriptors()
                    .find_uncommitted_by_table_id(runtime.pool_guards(), table_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            let recreated = session
                .create_managed_table(&[0xff, 0x00, 0xfe], &mut interpreter)
                .await
                .unwrap();
            assert_ne!(recreated.table_id(), table_id);
            let recreated_binding = session
                .resolve_table_binding(BindingNamespaceID::new(1), b"alpha", false)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(recreated_binding.version(), after_drop.version());
        });
    }

    #[test]
    fn test_table_definition_kind_survives_restart() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let main_dir = root.path().to_path_buf();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(&main_dir))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let mut interpreter = ManagedRoundTripInterpreter {
                initial_descriptor: vec![0xa5],
                created_index_descriptor: Vec::new(),
                dropped_index_descriptor: Vec::new(),
                create_index_calls: 0,
                drop_index_calls: 0,
                bindings: vec![
                    TableBinding::new(BindingNamespaceID::new(11), &b"restart"[..]),
                    TableBinding::new(BindingNamespaceID::new(12), Vec::<u8>::new()),
                    TableBinding::new(
                        BindingNamespaceID::new(13),
                        vec![0xa5; crate::MAX_TABLE_BINDING_KEY_BYTES],
                    ),
                ],
            };
            let managed = session
                .create_managed_table(&[0xff, 0x00, 0xfe], &mut interpreter)
                .await
                .unwrap();
            let unmanaged = session
                .create_table(
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::I32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![],
                )
                .await
                .unwrap();
            session.checkpoint_catalog().await.unwrap();
            drop(session);
            drop(engine);

            let engine = Engine::bootstrap(EngineConfig::default().storage_root(&main_dir))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            assert_eq!(
                session
                    .resolve_table_binding(BindingNamespaceID::new(11), b"restart", true)
                    .await
                    .unwrap()
                    .unwrap()
                    .table_id(),
                managed.table_id()
            );
            assert_eq!(
                session
                    .resolve_table_binding(BindingNamespaceID::new(12), b"", false)
                    .await
                    .unwrap()
                    .unwrap()
                    .table_id(),
                managed.table_id()
            );
            let maximum_key = vec![0xa5; crate::MAX_TABLE_BINDING_KEY_BYTES];
            assert_eq!(
                session
                    .resolve_table_binding(BindingNamespaceID::new(13), &maximum_key, false)
                    .await
                    .unwrap()
                    .unwrap()
                    .table_id(),
                managed.table_id()
            );
            let err = session
                .create_index(
                    managed.table_id(),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::InvalidMetadata));
            let err = session
                .drop_index(managed.table_id(), managed.index_ids()[0])
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::InvalidMetadata));

            let index_id = session
                .create_index(
                    unmanaged.table_id(),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            session
                .drop_index(unmanaged.table_id(), index_id)
                .await
                .unwrap();
            assert!(
                session
                    .engine()
                    .catalog()
                    .storage
                    .table_descriptors()
                    .find_uncommitted_by_table_id(
                        session.engine().pool_guards(),
                        unmanaged.table_id(),
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn test_managed_create_interpreter_and_size_fail_before_table_id_allocation() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let initial_next = session.engine().catalog().curr_next_table_id();

            let mut interpreter = CreateFailureInterpreter {
                result: Err("invalid source"),
                bindings: vec![],
            };
            let err = session
                .create_managed_table(b"bad", &mut interpreter)
                .await
                .unwrap_err();
            assert_eq!(err.interpreter(), Some(&"invalid source"));
            assert_eq!(
                session.engine().catalog().curr_next_table_id(),
                initial_next
            );

            let mut interpreter = CreateFailureInterpreter {
                result: Ok(vec![0; crate::MAX_TABLE_DESCRIPTOR_BYTES + 1]),
                bindings: vec![],
            };
            let err = session
                .create_managed_table(b"large", &mut interpreter)
                .await
                .unwrap_err();
            assert_eq!(
                err.engine().and_then(Error::operation_error),
                Some(OperationError::InvalidMetadata)
            );
            assert_eq!(
                session.engine().catalog().curr_next_table_id(),
                initial_next
            );
            assert!(
                session
                    .engine()
                    .catalog()
                    .list_user_table_ids_now()
                    .is_empty()
            );

            let mut interpreter = CreateFailureInterpreter {
                result: Ok(vec![]),
                bindings: vec![
                    TableBinding::new(BindingNamespaceID::new(5), &b"duplicate"[..]),
                    TableBinding::new(BindingNamespaceID::new(5), &b"duplicate"[..]),
                ],
            };
            let err = session
                .create_managed_table(b"duplicate", &mut interpreter)
                .await
                .unwrap_err();
            assert_eq!(
                err.engine().and_then(Error::operation_error),
                Some(OperationError::InvalidMetadata)
            );
            assert_eq!(
                session.engine().catalog().curr_next_table_id(),
                initial_next
            );

            let mut interpreter = CreateFailureInterpreter {
                result: Ok(vec![]),
                bindings: vec![TableBinding::new(
                    BindingNamespaceID::new(5),
                    vec![0; crate::MAX_TABLE_BINDING_KEY_BYTES + 1],
                )],
            };
            let err = session
                .create_managed_table(b"oversized binding", &mut interpreter)
                .await
                .unwrap_err();
            assert_eq!(
                err.engine().and_then(Error::operation_error),
                Some(OperationError::InvalidMetadata)
            );
            assert_eq!(
                session.engine().catalog().curr_next_table_id(),
                initial_next
            );

            let oversized_lookup = vec![0; crate::MAX_TABLE_BINDING_KEY_BYTES + 1];
            let error = session
                .resolve_table_binding(BindingNamespaceID::new(5), &oversized_lookup, false)
                .await
                .unwrap_err();
            assert_eq!(
                error.operation_error(),
                Some(OperationError::InvalidMetadata)
            );
        });
    }

    #[test]
    fn test_managed_create_binding_collision_is_atomic_and_namespace_local() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let mut first = CreateFailureInterpreter {
                result: Ok(vec![1]),
                bindings: vec![TableBinding::new(
                    BindingNamespaceID::new(7),
                    &b"shared"[..],
                )],
            };
            let first_id = session
                .create_managed_table(b"first", &mut first)
                .await
                .unwrap()
                .table_id();

            let mut duplicate = CreateFailureInterpreter {
                result: Ok(vec![2]),
                bindings: vec![TableBinding::new(
                    BindingNamespaceID::new(7),
                    &b"shared"[..],
                )],
            };
            let error = session
                .create_managed_table(b"duplicate", &mut duplicate)
                .await
                .unwrap_err();
            assert_eq!(
                error.engine().and_then(Error::operation_error),
                Some(OperationError::DuplicateKey)
            );
            assert_eq!(
                session
                    .resolve_table_binding(BindingNamespaceID::new(7), b"shared", false)
                    .await
                    .unwrap()
                    .unwrap()
                    .table_id(),
                first_id
            );
            assert_eq!(
                session.engine().catalog().list_user_table_ids_now(),
                [first_id]
            );

            let mut other_namespace = CreateFailureInterpreter {
                result: Ok(vec![3]),
                bindings: vec![TableBinding::new(
                    BindingNamespaceID::new(8),
                    &b"shared"[..],
                )],
            };
            let second_id = session
                .create_managed_table(b"other namespace", &mut other_namespace)
                .await
                .unwrap()
                .table_id();
            assert_ne!(second_id, first_id);
        });
    }

    #[test]
    fn test_managed_create_failure_rolls_back_binding_definition_bundle() {
        for failure in [
            CreateTableTestFailure::AfterCatalogStaged,
            CreateTableTestFailure::AfterFilePublished,
            CreateTableTestFailure::AfterRuntimeBuilt,
        ] {
            smol::block_on(async {
                let root = TempDir::new().unwrap();
                let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                    .await
                    .unwrap();
                let mut session = engine.new_session().unwrap();
                let table_id = session.engine().catalog().curr_next_table_id();
                let mut interpreter = CreateFailureInterpreter {
                    result: Ok(vec![1]),
                    bindings: vec![TableBinding::new(
                        BindingNamespaceID::new(19),
                        &b"rollback"[..],
                    )],
                };

                set_create_table_failure(&engine, Some(failure));
                let result = session
                    .create_managed_table(b"create", &mut interpreter)
                    .await;
                set_create_table_failure(&engine, None);
                assert!(result.is_err());
                assert!(
                    session
                        .engine()
                        .catalog()
                        .list_user_table_ids_now()
                        .is_empty()
                );
                assert!(
                    session
                        .resolve_table_binding(BindingNamespaceID::new(19), b"rollback", false)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(
                    session
                        .engine()
                        .catalog()
                        .storage
                        .tables()
                        .find_uncommitted_by_id(session.engine().pool_guards(), table_id)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(
                    session
                        .engine()
                        .catalog()
                        .storage
                        .table_descriptors()
                        .find_uncommitted_by_table_id(session.engine().pool_guards(), table_id)
                        .await
                        .unwrap()
                        .is_none()
                );
            });
        }
    }

    #[test]
    fn test_concurrent_managed_create_binding_collision_has_one_clean_winner() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let first_table_id = engine.inner().core.catalog().curr_next_table_id();
            let (first_entered, release_first) = install_create_before_first_effect_gate(&engine);
            let mut session1 = engine.new_session().unwrap();
            let mut interpreter1 = CreateFailureInterpreter {
                result: Ok(vec![1]),
                bindings: vec![
                    TableBinding::new(BindingNamespaceID::new(20), &b"rolled-back"[..]),
                    TableBinding::new(BindingNamespaceID::new(20), &b"contended"[..]),
                ],
            };
            let mut first = Box::pin(session1.create_managed_table(b"first", &mut interpreter1));
            assert!(matches!(
                futures::poll!(first.as_mut()),
                std::task::Poll::Pending
            ));
            first_entered.recv_async().await.unwrap();

            // The first CREATE already owns binding-table data-IX but has not
            // staged its key. A second CREATE must pass lock acquisition and
            // commit before the first is released.
            let (second_done_tx, second_done_rx) = mpsc::sync_channel(1);
            let mut session2 = engine.new_session().unwrap();
            let second = thread::spawn(move || {
                let mut interpreter = CreateFailureInterpreter {
                    result: Ok(vec![2]),
                    bindings: vec![TableBinding::new(
                        BindingNamespaceID::new(20),
                        &b"contended"[..],
                    )],
                };
                let result =
                    smol::block_on(session2.create_managed_table(b"second", &mut interpreter));
                second_done_tx.send(result).unwrap();
            });
            let winner = match second_done_rx.recv_timeout(Duration::from_secs(10)) {
                Ok(result) => result.unwrap(),
                Err(error) => {
                    release_first.send_async(()).await.unwrap();
                    second.join().unwrap();
                    panic!("non-conflicting binding data-IX acquisition stalled: {error}");
                }
            };
            release_first.send_async(()).await.unwrap();
            let loser = first.await.unwrap_err();
            second.join().unwrap();
            assert!(matches!(
                loser.engine().and_then(Error::operation_error),
                Some(OperationError::DuplicateKey | OperationError::WriteConflict)
            ));
            assert_ne!(winner.table_id(), first_table_id);
            let mut verifier = engine.new_session().unwrap();
            assert_eq!(
                verifier
                    .resolve_table_binding(BindingNamespaceID::new(20), b"contended", false)
                    .await
                    .unwrap()
                    .unwrap()
                    .table_id(),
                winner.table_id()
            );
            assert!(
                verifier
                    .resolve_table_binding(BindingNamespaceID::new(20), b"rolled-back", false)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                verifier.engine().catalog().list_user_table_ids_now(),
                [winner.table_id()]
            );
            assert!(
                verifier
                    .engine()
                    .catalog()
                    .storage
                    .tables()
                    .find_uncommitted_by_id(verifier.engine().pool_guards(), first_table_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                verifier
                    .engine()
                    .catalog()
                    .storage
                    .table_descriptors()
                    .find_uncommitted_by_table_id(verifier.engine().pool_guards(), first_table_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(!user_table_file_exists(&engine, first_table_id));
        });
    }

    #[test]
    fn test_binding_resolution_classifies_existing_invalid_targets_as_integrity() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let unmanaged = session
                .create_table(
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::I32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![],
                )
                .await
                .unwrap()
                .table_id();
            let missing = TableID::new(10_000);
            let mut transaction = begin_catalog_test_trx(&session);
            session
                .engine()
                .catalog()
                .storage
                .table_bindings()
                .insert_batch(
                    transaction.trx(),
                    &[
                        TableBindingObject {
                            namespace_id: BindingNamespaceID::new(1),
                            binding_key: Box::from(&b"unmanaged"[..]),
                            table_id: unmanaged,
                        },
                        TableBindingObject {
                            namespace_id: BindingNamespaceID::new(1),
                            binding_key: Box::from(&b"missing"[..]),
                            table_id: missing,
                        },
                    ],
                )
                .await
                .unwrap();
            transaction.commit(DDLRedo::CreateTable(missing)).await;

            for key in [&b"unmanaged"[..], &b"missing"[..]] {
                let error = session
                    .resolve_table_binding(BindingNamespaceID::new(1), key, false)
                    .await
                    .unwrap_err();
                assert_eq!(error.kind(), ErrorKind::DataIntegrity);
            }
        });
    }

    #[test]
    fn test_binding_resolution_revalidates_after_drop_and_recreate() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut creator = engine.new_session().unwrap();
            let mut initial = CreateFailureInterpreter {
                result: Ok(vec![1]),
                bindings: vec![TableBinding::new(
                    BindingNamespaceID::new(30),
                    &b"moving"[..],
                )],
            };
            let old_table_id = creator
                .create_managed_table(b"initial", &mut initial)
                .await
                .unwrap()
                .table_id();
            let (probe_entered, probe_release) =
                install_binding_probe_pause(BindingNamespaceID::new(30), &b"moving"[..]);
            let resolver = engine.new_session().unwrap();

            let (resolved, recreated) = thread::scope(|scope| {
                let resolver = scope.spawn(move || {
                    let mut resolver = resolver;
                    smol::block_on(resolver.resolve_table_binding(
                        BindingNamespaceID::new(30),
                        b"moving",
                        true,
                    ))
                });
                let mutator = scope.spawn(move || {
                    probe_entered.recv().unwrap();
                    let recreated = smol::block_on(async {
                        creator.drop_table(old_table_id).await.unwrap();
                        let mut replacement = CreateFailureInterpreter {
                            result: Ok(vec![2]),
                            bindings: vec![TableBinding::new(
                                BindingNamespaceID::new(30),
                                &b"moving"[..],
                            )],
                        };
                        creator
                            .create_managed_table(b"replacement", &mut replacement)
                            .await
                            .unwrap()
                    });
                    probe_release.send(()).unwrap();
                    recreated
                });
                (resolver.join().unwrap(), mutator.join().unwrap())
            });

            let resolved = resolved.unwrap().unwrap();
            assert_ne!(recreated.table_id(), old_table_id);
            assert_eq!(resolved.table_id(), recreated.table_id());
            assert_eq!(resolved.full_schema().unwrap().descriptor(), [2]);
        });
    }

    #[test]
    fn test_binding_resolution_returns_none_after_drop_between_passes() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut creator = engine.new_session().unwrap();
            let mut initial = CreateFailureInterpreter {
                result: Ok(vec![1]),
                bindings: vec![TableBinding::new(
                    BindingNamespaceID::new(32),
                    &b"disappearing"[..],
                )],
            };
            let table_id = creator
                .create_managed_table(b"initial", &mut initial)
                .await
                .unwrap()
                .table_id();
            let (probe_entered, probe_release) =
                install_binding_probe_pause(BindingNamespaceID::new(32), &b"disappearing"[..]);
            let resolver = engine.new_session().unwrap();
            let resolver_owner = LockOwner::session_explicit(resolver.id());

            let resolved = thread::scope(|scope| {
                let resolver = scope.spawn(move || {
                    let mut resolver = resolver;
                    smol::block_on(resolver.resolve_table_binding(
                        BindingNamespaceID::new(32),
                        b"disappearing",
                        true,
                    ))
                });
                let mutator = scope.spawn(move || {
                    probe_entered.recv().unwrap();
                    smol::block_on(creator.drop_table(table_id)).unwrap();
                    probe_release.send(()).unwrap();
                });
                let resolved = resolver.join().unwrap();
                mutator.join().unwrap();
                resolved
            });

            assert!(resolved.unwrap().is_none());
            assert_eq!(lock_entry_count(&engine, resolver_owner), 0);
        });
    }

    #[test]
    fn test_full_binding_resolution_rejects_descriptor_stamp_disagreement() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let mut interpreter = CreateFailureInterpreter {
                result: Ok(vec![1]),
                bindings: vec![TableBinding::new(
                    BindingNamespaceID::new(31),
                    &b"stamp"[..],
                )],
            };
            let table_id = session
                .create_managed_table(b"create", &mut interpreter)
                .await
                .unwrap()
                .table_id();
            let original = session
                .engine()
                .catalog()
                .storage
                .table_descriptors()
                .find_uncommitted_by_table_id(session.engine().pool_guards(), table_id)
                .await
                .unwrap()
                .unwrap();

            for corrupt in [
                TableDescriptorObject {
                    compiled_storage_epoch: original.compiled_storage_epoch + 1,
                    ..original.clone()
                },
                TableDescriptorObject {
                    storage_schema_fingerprint: [0xff; 32],
                    ..original.clone()
                },
            ] {
                let mut transaction = begin_catalog_test_trx(&session);
                assert!(
                    session
                        .engine()
                        .catalog()
                        .storage
                        .table_descriptors()
                        .replace(transaction.trx(), &corrupt)
                        .await
                        .unwrap()
                );
                transaction.commit(DDLRedo::CreateTable(table_id)).await;
                let error = session
                    .resolve_table_binding(BindingNamespaceID::new(31), b"stamp", true)
                    .await
                    .unwrap_err();
                assert_eq!(error.kind(), ErrorKind::DataIntegrity);
                assert!(
                    session
                        .resolve_table_binding(BindingNamespaceID::new(31), b"stamp", false)
                        .await
                        .unwrap()
                        .is_some()
                );
            }
        });
    }

    #[test]
    fn test_concurrent_managed_create_index_returns_stale_without_reinvocation() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root.path()))
                .await
                .unwrap();
            let mut creator = engine.new_session().unwrap();
            let mut create_interpreter = CreateFailureInterpreter {
                result: Ok(vec![9]),
                bindings: vec![],
            };
            let table_id = creator
                .create_managed_table(b"create", &mut create_interpreter)
                .await
                .unwrap()
                .table_id();
            drop(creator);

            let barrier = Arc::new(Barrier::new(2));
            let calls = Arc::new(AtomicUsize::new(0));
            let session1 = engine.new_session().unwrap();
            let session2 = engine.new_session().unwrap();
            let ((mut session1, result1), (mut session2, result2)) = thread::scope(|scope| {
                let barrier1 = Arc::clone(&barrier);
                let calls1 = Arc::clone(&calls);
                let first = scope.spawn(move || {
                    let mut session = session1;
                    let mut interpreter = CreateIndexOnlyInterpreter {
                        barrier: Some(barrier1),
                        calls: calls1,
                        expected_index_count: 0,
                        expected_proposed: IndexID::new(0),
                        descriptor: vec![1],
                    };
                    let result = smol::block_on(session.create_managed_index(
                        table_id,
                        b"first",
                        &mut interpreter,
                    ));
                    (session, result)
                });
                let barrier2 = Arc::clone(&barrier);
                let calls2 = Arc::clone(&calls);
                let second = scope.spawn(move || {
                    let mut session = session2;
                    let mut interpreter = CreateIndexOnlyInterpreter {
                        barrier: Some(barrier2),
                        calls: calls2,
                        expected_index_count: 0,
                        expected_proposed: IndexID::new(0),
                        descriptor: vec![2],
                    };
                    let result = smol::block_on(session.create_managed_index(
                        table_id,
                        b"second",
                        &mut interpreter,
                    ));
                    (session, result)
                });
                (first.join().unwrap(), second.join().unwrap())
            });

            assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
            assert_ne!(result1.is_ok(), result2.is_ok());
            let loser = if let Err(err) = result1 {
                assert_eq!(
                    err.engine().and_then(Error::operation_error),
                    Some(OperationError::SchemaChanged)
                );
                &mut session1
            } else {
                let err = result2.unwrap_err();
                assert_eq!(
                    err.engine().and_then(Error::operation_error),
                    Some(OperationError::SchemaChanged)
                );
                &mut session2
            };

            let retry_calls = Arc::new(AtomicUsize::new(0));
            let mut retry = CreateIndexOnlyInterpreter {
                barrier: None,
                calls: Arc::clone(&retry_calls),
                expected_index_count: 1,
                expected_proposed: IndexID::new(1),
                descriptor: vec![3],
            };
            assert_eq!(
                loser
                    .create_managed_index(table_id, b"retry", &mut retry)
                    .await
                    .unwrap(),
                IndexID::new(1)
            );
            assert_eq!(retry_calls.load(AtomicOrdering::SeqCst), 1);
        });
    }
}
