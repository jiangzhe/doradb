use super::{PreparedDdlScope, Session, SessionOperationPin};
use crate::catalog::{
    CreateTableOutcome, CurrentTableDefinition, ID_DOMAIN_END, IndexDdlGateScope, IndexID,
    ManagedDdlError, ManagedDdlResult, PreparedCreateIndex, PreparedCreateTable, PreparedDropIndex,
    TABLE_ID_TABLE_DESCRIPTORS, TableDescriptorInterpreter, ValidatedCreateTable,
    create_index_catalog_write_targets, create_table_catalog_write_targets,
    drop_index_catalog_write_targets, reject_non_user_table_id,
    reject_user_table_primary_key_index, validate_descriptor_payload, validated_index_ddl_target,
};
use crate::error::{
    DiscloseError, DiscloseResultExt, MultiDomainResultExt, OperationError, OperationOrFatalResult,
    Result, RuntimeError,
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
    fn create_managed_table<I>(
        &mut self,
        source: &[u8],
        interpreter: &mut I,
    ) -> impl Future<Output = ManagedDdlResult<CreateTableOutcome, I::Error>>
    where
        I: TableDescriptorInterpreter;

    /// Interprets and creates one managed secondary index from opaque bytes.
    fn create_managed_index<I>(
        &mut self,
        table_id: TableID,
        source: &[u8],
        interpreter: &mut I,
    ) -> impl Future<Output = ManagedDdlResult<IndexID, I::Error>>
    where
        I: TableDescriptorInterpreter;

    /// Interprets and drops one managed secondary index from opaque bytes.
    fn drop_managed_index<I>(
        &mut self,
        table_id: TableID,
        source: &[u8],
        interpreter: &mut I,
    ) -> impl Future<Output = ManagedDdlResult<(), I::Error>>
    where
        I: TableDescriptorInterpreter;
}

impl ManagedTableOps for Session {
    async fn create_managed_table<I>(
        &mut self,
        source: &[u8],
        interpreter: &mut I,
    ) -> ManagedDdlResult<CreateTableOutcome, I::Error>
    where
        I: TableDescriptorInterpreter,
    {
        let update = interpreter
            .create_table(source)
            .map_err(ManagedDdlError::Interpreter)?;
        validate_descriptor_payload(update.descriptor())
            .disclose()
            .map_err(ManagedDdlError::Engine)?;
        let (definition, descriptor) = update.into_parts();
        let (table_spec, index_specs) = definition.into_parts();
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
            .prepare_managed_create_table(validated, descriptor)
            .await
            .attach("operation=create_managed_table")
            .disclose()
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
        I: TableDescriptorInterpreter,
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
        I: TableDescriptorInterpreter,
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
}

impl Session {
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
    ) -> OperationOrFatalResult<PreparedCreateTable> {
        let table_id = self.runtime.catalog().next_table_id();
        let plan = validated.into_managed_plan(table_id, descriptor);
        let scope = PreparedDdlScope::create(self, table_id, create_table_catalog_write_targets())
            .await
            .attach_with(|| format!("prepare managed CREATE TABLE locks: table_id={table_id}"))?;
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

#[cfg(test)]
mod tests {
    use crate::session::tests::SessionTestExt;
    use crate::{
        Engine, EngineConfig, Error, IndexID, ManagedTableOps, OperationError, StorageColumnFlags,
        StorageColumnSpec, StorageIndexFlags, StorageIndexKey, StorageIndexSpec, StorageTableSpec,
        ValKind,
    };
    use std::result::Result as StdResult;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::TempDir;

    struct ManagedRoundTripInterpreter {
        initial_descriptor: Vec<u8>,
        created_index_descriptor: Vec<u8>,
        dropped_index_descriptor: Vec<u8>,
        create_index_calls: usize,
        drop_index_calls: usize,
    }

    impl crate::TableDescriptorInterpreter for ManagedRoundTripInterpreter {
        type Error = &'static str;

        fn create_table(
            &mut self,
            source: &[u8],
        ) -> StdResult<crate::DescriptorUpdate<crate::CreateTableDefinition>, Self::Error> {
            assert_eq!(source, [0xff, 0x00, 0xfe]);
            Ok(crate::DescriptorUpdate::new(
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
    }

    impl crate::TableDescriptorInterpreter for CreateFailureInterpreter {
        type Error = &'static str;

        fn create_table(
            &mut self,
            _source: &[u8],
        ) -> StdResult<crate::DescriptorUpdate<crate::CreateTableDefinition>, Self::Error> {
            let descriptor = self.result.clone()?;
            Ok(crate::DescriptorUpdate::new(
                crate::CreateTableDefinition::new(
                    StorageTableSpec::new(vec![StorageColumnSpec::new(
                        ValKind::I32,
                        StorageColumnFlags::empty(),
                    )]),
                    vec![],
                ),
                descriptor,
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

    impl crate::TableDescriptorInterpreter for CreateIndexOnlyInterpreter {
        type Error = &'static str;

        fn create_table(
            &mut self,
            _source: &[u8],
        ) -> StdResult<crate::DescriptorUpdate<crate::CreateTableDefinition>, Self::Error> {
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
            };

            let outcome = session
                .create_managed_table(&[0xff, 0x00, 0xfe], &mut interpreter)
                .await
                .unwrap();
            let table_id = outcome.table_id();
            assert_eq!(outcome.index_ids(), [IndexID::new(0)]);
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
