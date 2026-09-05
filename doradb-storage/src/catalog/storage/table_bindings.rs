use crate::buffer::PoolGuards;
pub(crate) use crate::catalog::storage::layout::TABLE_ID_TABLE_BINDINGS;
use crate::catalog::storage::{CatalogDefinition, TableBindingObject};
use crate::catalog::{
    BindingNamespaceID, CatalogIndexNo, CatalogTable, MAX_TABLE_BINDING_KEY_BYTES,
    StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey, StorageIndexSpec,
    TableBinding, TableColumnLayout, TableMetadata,
};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, MultiDomainResultExt, OperationError,
    OperationOrRuntimeError, OperationOrRuntimeResult, QuadResult, RuntimeError,
    RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeResult,
};
use crate::id::TableID;
use crate::row::{Row, RowRead};
use crate::table::IndexLookupCriteria;
use crate::trx::PrivateTransaction;
use crate::value::{Val, ValKind};
use error_stack::{Report, ResultExt};
use std::sync::OnceLock;

/// Primary `(namespace_id, binding_key)` slot of `catalog.table_bindings`.
const PK_NO_TABLE_BINDINGS: CatalogIndexNo = CatalogIndexNo::new(0);
/// Reverse `table_id` slot of `catalog.table_bindings`.
pub(super) const TABLE_ID_NO_TABLE_BINDINGS: CatalogIndexNo = CatalogIndexNo::new(1);
const TABLE_BINDING_COLUMN_COUNT: usize = 3;

/// Runtime accessor for managed table binding rows.
pub(crate) struct TableBindings<'a> {
    pub(super) table: &'a CatalogTable,
}

impl TableBindings<'_> {
    /// Resolves one exact current binding while copying only its target id.
    pub(crate) async fn find_uncommitted_table_id(
        &self,
        guards: &PoolGuards,
        namespace_id: BindingNamespaceID,
        binding_key: &[u8],
    ) -> RuntimeResult<Option<TableID>> {
        let key = [Val::from(namespace_id.as_u64()), Val::from(binding_key)];
        let table_id = self
            .table
            .index_lookup_unique_uncommitted(guards, PK_NO_TABLE_BINDINGS, &key, |layout, row| {
                row.val(layout, 2)
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!(
                    "operation=find_table_binding, namespace_id={}, binding_key_len={}",
                    namespace_id.as_u64(),
                    binding_key.len()
                )
            })?;
        table_id
            .map(|value| decode_user_table_id(value, "catalog.table_bindings table_id"))
            .transpose()
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=find_table_binding, phase=decode_target")
    }

    /// Rejects binding keys that are already present before CREATE execution.
    ///
    /// This is only an early precheck under table data-IX. The primary-index
    /// insertion remains authoritative because another CREATE or DROP may race
    /// after this lookup.
    pub(crate) async fn precheck_create_keys_absent(
        &self,
        guards: &PoolGuards,
        bindings: &[TableBinding],
    ) -> OperationOrRuntimeResult<()> {
        for binding in bindings {
            let found = self
                .find_uncommitted_table_id(guards, binding.namespace_id(), binding.binding_key())
                .await
                .map_err(OperationOrRuntimeError::from)?;
            if found.is_some() {
                return Err(OperationOrRuntimeError::from(
                    Report::new(OperationError::DuplicateKey).attach(format!(
                        "managed table binding already exists: namespace_id={}, binding_key_len={}",
                        binding.namespace_id().as_u64(),
                        binding.binding_key().len()
                    )),
                ));
            }
        }
        Ok(())
    }

    /// Lists all current bindings targeting one table through the reverse index.
    pub(crate) async fn list_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> RuntimeResult<Vec<TableBindingObject>> {
        let key = [Val::from(table_id)];
        let mut objects = Vec::new();
        let mut decode_error = None;
        self.table
            .mem
            .catalog_index_lookup_current(
                guards,
                TABLE_ID_NO_TABLE_BINDINGS,
                IndexLookupCriteria::NonUniqueExact(&key),
                |layout, row| {
                    match row_to_table_binding_object(layout, row) {
                        Ok(object) => objects.push(object),
                        Err(error) => {
                            decode_error = Some(error);
                            return false;
                        }
                    }
                    true
                },
            )
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=list_table_bindings, table_id={table_id}"))?;
        if let Some(error) = decode_error {
            return Err(error
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=list_table_bindings, phase=decode_row"));
        }
        objects.sort_unstable_by(|left, right| {
            (left.namespace_id, left.binding_key.as_ref())
                .cmp(&(right.namespace_id, right.binding_key.as_ref()))
        });
        Ok(objects)
    }

    /// Inserts a binding batch whose uniqueness is an internal invariant.
    pub(crate) async fn insert_batch(
        &self,
        trx: &mut PrivateTransaction,
        objects: &[TableBindingObject],
    ) -> RuntimeOrFatalResult<()> {
        let rows = objects
            .iter()
            .map(cols_from_table_binding_object)
            .collect::<DataIntegrityResult<Vec<_>>>()
            .map_err(|error| {
                RuntimeOrFatalError::from(
                    error
                        .change_context(RuntimeError::CatalogAccess)
                        .attach("operation=catalog_table_bindings_insert_batch, phase=encode"),
                )
            })?;
        trx.catalog_insert_batch_mvcc(self.table, rows)
            .await
            .attach("operation=catalog_table_bindings_insert_batch")
    }

    /// Inserts a validated binding batch while preserving key-race failures.
    pub(crate) async fn try_insert_unique_batch(
        &self,
        trx: &mut PrivateTransaction,
        objects: &[TableBindingObject],
    ) -> QuadResult<()> {
        let rows = objects
            .iter()
            .map(cols_from_table_binding_object)
            .collect::<DataIntegrityResult<Vec<_>>>()
            .map_err(|error| {
                RuntimeOrFatalError::from(error.change_context(RuntimeError::CatalogAccess).attach(
                    "operation=catalog_table_bindings_try_insert_unique_batch, phase=encode",
                ))
            })?;
        trx.catalog_try_insert_unique_batch_mvcc(self.table, rows)
            .await
            .attach("operation=catalog_table_bindings_try_insert_unique_batch")
    }

    /// Deletes every binding targeting one table through the reverse index.
    pub(crate) async fn delete_by_table_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeOrFatalResult<usize> {
        let objects = self.list_current_locked_by_table_id(trx, table_id).await?;
        let keys = objects
            .into_iter()
            .map(|object| {
                vec![
                    Val::from(object.namespace_id.as_u64()),
                    Val::from(object.binding_key.as_ref()),
                ]
            })
            .collect();
        trx.catalog_delete_primary_key_batch_mvcc(self.table, PK_NO_TABLE_BINDINGS, keys)
            .await
            .attach_with(|| {
                format!("operation=catalog_table_bindings_delete_by_table, table_id={table_id}")
            })
    }

    async fn list_current_locked_by_table_id(
        &self,
        trx: &PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeResult<Vec<TableBindingObject>> {
        let key = [Val::from(table_id)];
        let mut objects = Vec::new();
        let mut decode_error = None;
        self.table
            .index_lookup_current_locked(
                trx,
                TABLE_ID_NO_TABLE_BINDINGS,
                IndexLookupCriteria::NonUniqueExact(&key),
                |layout, row| {
                    match row_to_table_binding_object(layout, row) {
                        Ok(object) => objects.push(object),
                        Err(error) => {
                            decode_error = Some(error);
                            return false;
                        }
                    }
                    true
                },
            )
            .await
            .attach_with(|| format!("operation=list_locked_table_bindings, table_id={table_id}"))?;
        if let Some(error) = decode_error {
            return Err(error
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=list_locked_table_bindings, phase=decode_row"));
        }
        Ok(objects)
    }
}

/// Returns the durable roleless `catalog.table_bindings` definition.
pub(super) fn catalog_definition_of_table_bindings() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| CatalogDefinition {
        table_id: TABLE_ID_TABLE_BINDINGS,
        metadata: TableMetadata::try_new(
            vec![
                // namespace_id U64: opaque binding namespace identity.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // binding_key VARBYTE: opaque namespace-local lookup key.
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                // table_id U64: bound managed user table.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
            ],
            vec![
                StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0), StorageIndexKey::new(1)],
                    StorageIndexFlags::PK,
                ),
                StorageIndexSpec::new(vec![StorageIndexKey::new(2)], StorageIndexFlags::empty()),
            ],
        )
        .expect("valid catalog.table_bindings metadata"),
    })
}

fn row_to_table_binding_object(
    layout: &TableColumnLayout,
    row: Row<'_>,
) -> DataIntegrityResult<TableBindingObject> {
    let values = (0..TABLE_BINDING_COLUMN_COUNT)
        .map(|index| row.val(layout, index))
        .collect::<Vec<_>>();
    table_binding_object_from_vals(&values)
}

pub(super) fn table_binding_object_from_vals(
    values: &[Val],
) -> DataIntegrityResult<TableBindingObject> {
    if values.len() != TABLE_BINDING_COLUMN_COUNT {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog.table_bindings value count {}, expected {TABLE_BINDING_COLUMN_COUNT}",
                values.len()
            )),
        );
    }
    let namespace_id = values[0]
        .as_u64()
        .map(BindingNamespaceID::new)
        .ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach("catalog.table_bindings namespace_id has wrong type")
        })?;
    let binding_key = values[1].as_bytes().ok_or_else(|| {
        Report::new(DataIntegrityError::InvalidPayload)
            .attach("catalog.table_bindings binding_key has wrong type")
    })?;
    if binding_key.len() > MAX_TABLE_BINDING_KEY_BYTES {
        return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "catalog.table_bindings key length {} exceeds maximum {MAX_TABLE_BINDING_KEY_BYTES}",
            binding_key.len()
        )));
    }
    let table_id = decode_user_table_id(values[2].clone(), "catalog.table_bindings table_id")?;
    Ok(TableBindingObject {
        namespace_id,
        binding_key: binding_key.into(),
        table_id,
    })
}

fn cols_from_table_binding_object(object: &TableBindingObject) -> DataIntegrityResult<Vec<Val>> {
    if !object.table_id.is_user() || object.binding_key.len() > MAX_TABLE_BINDING_KEY_BYTES {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "invalid table binding object: table_id={}, binding_key_len={}",
                object.table_id,
                object.binding_key.len()
            )),
        );
    }
    Ok(vec![
        Val::from(object.namespace_id.as_u64()),
        Val::from(object.binding_key.as_ref()),
        Val::from(object.table_id),
    ])
}

fn decode_user_table_id(value: Val, field: &'static str) -> DataIntegrityResult<TableID> {
    value
        .as_u64()
        .map(TableID::new)
        .filter(|table_id| table_id.is_user())
        .ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("{field} is not a user table"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::multi_table_file::CATALOG_TABLE_ROOT_DESC_COUNT;

    fn binding_values(key: Vec<u8>) -> Vec<Val> {
        vec![Val::from(7u64), Val::from(key), Val::from(TableID::new(42))]
    }

    #[test]
    fn binding_schema_has_roleless_columns_and_both_indexes() {
        let definition = catalog_definition_of_table_bindings();
        assert_eq!(CATALOG_TABLE_ROOT_DESC_COUNT, 6);
        assert_eq!(definition.table_id, TABLE_ID_TABLE_BINDINGS);
        assert_eq!(definition.metadata.col.col_count(), 3);
        assert_eq!(definition.metadata.idx.active_index_count(), 2);
        let indexes = definition
            .metadata
            .idx
            .active_indexes()
            .map(|(_, index)| index)
            .collect::<Vec<_>>();
        assert!(indexes[0].primary_key());
        assert_eq!(
            indexes[0]
                .keys
                .iter()
                .map(|key| key.column_ordinal.as_usize())
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert!(!indexes[1].unique());
        assert_eq!(indexes[1].keys[0].column_ordinal.as_usize(), 2);
    }

    #[test]
    fn binding_row_decode_accepts_opaque_key_boundaries() {
        for key in [
            Vec::new(),
            vec![0, 0xff, 1],
            vec![3; MAX_TABLE_BINDING_KEY_BYTES],
        ] {
            let object = table_binding_object_from_vals(&binding_values(key.clone())).unwrap();
            assert_eq!(object.namespace_id, BindingNamespaceID::new(7));
            assert_eq!(&*object.binding_key, key);
            assert_eq!(object.table_id, TableID::new(42));
            assert_eq!(
                cols_from_table_binding_object(&object).unwrap(),
                binding_values(key)
            );
        }
    }

    #[test]
    fn binding_row_decode_rejects_malformed_values() {
        for values in [
            Vec::new(),
            vec![
                Val::from(1u32),
                Val::from(Vec::<u8>::new()),
                Val::from(1u64),
            ],
            vec![Val::from(1u64), Val::from(1u64), Val::from(1u64)],
            vec![
                Val::from(1u64),
                Val::from(Vec::<u8>::new()),
                Val::from(TABLE_ID_TABLE_BINDINGS),
            ],
            binding_values(vec![0; MAX_TABLE_BINDING_KEY_BYTES + 1]),
        ] {
            assert!(table_binding_object_from_vals(&values).is_err());
        }
    }
}
