use crate::buffer::PoolGuards;
use crate::catalog::storage::{CatalogDefinition, TableDescriptorObject};
use crate::catalog::{
    CatalogIndexNo, CatalogTable, MAX_TABLE_DESCRIPTOR_BYTES, StorageColumnFlags,
    StorageColumnSpec, StorageIndexFlags, StorageIndexKey, StorageIndexSpec, TableMetadata,
    catalog_table_id_from_slot,
};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, MultiDomainResultExt, RuntimeError,
    RuntimeOrFatalResult, RuntimeResult,
};
use crate::id::TableID;
use crate::row::RowRead;
use crate::row::ops::DeleteMvcc;
use crate::trx::PrivateTransaction;
use crate::value::{Val, ValKind};
use error_stack::{Report, ResultExt};
use std::sync::OnceLock;

/// Catalog table id for `catalog.table_descriptors`.
pub(crate) const TABLE_ID_TABLE_DESCRIPTORS: TableID = catalog_table_id_from_slot(3);
/// Primary-key slot of `catalog.table_descriptors`.
pub(super) const PK_NO_TABLE_DESCRIPTORS: CatalogIndexNo = CatalogIndexNo::new(0);
const COL_NO_TABLE_DESCRIPTORS_TABLE_ID: usize = 0;
const DESCRIPTOR_COLUMN_COUNT: usize = 5;

/// Runtime accessor for opaque managed table descriptor envelopes.
pub(crate) struct TableDescriptors<'a> {
    pub(super) table: &'a CatalogTable,
}

impl TableDescriptors<'_> {
    /// Finds one current uncommitted-visible descriptor by user table id.
    pub(crate) async fn find_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> RuntimeResult<Option<TableDescriptorObject>> {
        let key = [Val::from(table_id)];
        let vals = self
            .table
            .index_lookup_unique_uncommitted(
                guards,
                PK_NO_TABLE_DESCRIPTORS,
                &key,
                |layout, row| {
                    (0..DESCRIPTOR_COLUMN_COUNT)
                        .map(|idx| row.val(layout, idx))
                        .collect::<Vec<_>>()
                },
            )
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=find_table_descriptor, table_id={table_id}"))?;
        vals.map(|vals| table_descriptor_object_from_vals(&vals))
            .transpose()
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!("operation=find_table_descriptor, phase=decode_row, table_id={table_id}")
            })
    }

    /// Lists and validates all current uncommitted-visible descriptor rows.
    pub(crate) async fn list_uncommitted(
        &self,
        guards: &PoolGuards,
    ) -> RuntimeResult<Vec<TableDescriptorObject>> {
        let mut descriptors = Vec::new();
        let mut decode_error = None;
        self.table
            .table_scan_uncommitted(guards, |layout, row| {
                if row.is_deleted() {
                    return true;
                }
                let vals = (0..DESCRIPTOR_COLUMN_COUNT)
                    .map(|idx| row.val(layout, idx))
                    .collect::<Vec<_>>();
                match table_descriptor_object_from_vals(&vals) {
                    Ok(descriptor) => {
                        descriptors.push(descriptor);
                        true
                    }
                    Err(err) => {
                        decode_error = Some(err);
                        false
                    }
                }
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach("operation=list_table_descriptors")?;
        if let Some(err) = decode_error {
            return Err(err
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=list_table_descriptors, phase=decode_row"));
        }
        Ok(descriptors)
    }

    /// Inserts a descriptor for a newly created managed table.
    pub(crate) async fn insert(
        &self,
        trx: &mut PrivateTransaction,
        descriptor: &TableDescriptorObject,
    ) -> RuntimeOrFatalResult<()> {
        trx.catalog_insert_mvcc(self.table, cols_from_table_descriptor(descriptor))
            .await
            .map(|_| ())
            .attach_with(|| {
                format!(
                    "operation=catalog_table_descriptors_insert, table_id={}",
                    descriptor.table_id
                )
            })
    }

    /// Replaces the required descriptor for one managed schema change.
    pub(crate) async fn replace(
        &self,
        trx: &mut PrivateTransaction,
        descriptor: &TableDescriptorObject,
    ) -> RuntimeOrFatalResult<bool> {
        let result = trx
            .catalog_replace_primary_key_mvcc(
                self.table,
                PK_NO_TABLE_DESCRIPTORS,
                vec![Val::from(descriptor.table_id)],
                cols_from_table_descriptor(descriptor),
            )
            .await
            .attach_with(|| {
                format!(
                    "operation=catalog_table_descriptors_replace, table_id={}",
                    descriptor.table_id
                )
            })?;
        Ok(matches!(result, DeleteMvcc::Deleted))
    }

    /// Deletes a descriptor if the dropped table was managed.
    pub(crate) async fn delete_by_table_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeOrFatalResult<bool> {
        let result = trx
            .catalog_delete_primary_key_mvcc(
                self.table,
                PK_NO_TABLE_DESCRIPTORS,
                vec![Val::from(table_id)],
            )
            .await
            .attach_with(|| {
                format!("operation=catalog_table_descriptors_delete, table_id={table_id}")
            })?;
        Ok(matches!(result, DeleteMvcc::Deleted))
    }
}

/// Validates one descriptor stamp against the separately reconstructed schema.
pub(crate) fn validate_table_descriptor_against_metadata(
    descriptor: &TableDescriptorObject,
    table_id: TableID,
    metadata: &TableMetadata,
) -> DataIntegrityResult<()> {
    if descriptor.table_id != table_id
        || descriptor.compiled_storage_epoch != metadata.storage_epoch
        || descriptor.storage_schema_fingerprint != metadata.storage_schema_fingerprint()
    {
        return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "managed descriptor stamp mismatch: table_id={table_id}, descriptor_table_id={}, descriptor_epoch={}, storage_epoch={}",
            descriptor.table_id, descriptor.compiled_storage_epoch, metadata.storage_epoch
        )));
    }
    Ok(())
}

/// Returns the durable definition of `catalog.table_descriptors`.
pub(super) fn catalog_definition_of_table_descriptors() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| CatalogDefinition {
        table_id: TABLE_ID_TABLE_DESCRIPTORS,
        metadata: TableMetadata::try_new(
            vec![
                // table_id U64: owning managed user table.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // descriptor_revision U64: monotonic storage-owned replacement revision.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // compiled_storage_epoch U64: numeric schema epoch described by the payload.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // storage_schema_fingerprint VARBYTE: canonical 32-byte numeric schema digest.
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                // payload VARBYTE: exact opaque higher-layer descriptor bytes.
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ],
            // Primary key: table_id.
            vec![StorageIndexSpec::new(
                vec![StorageIndexKey::new(0)],
                StorageIndexFlags::PK,
            )],
        )
        .expect("valid catalog.table_descriptors metadata"),
    })
}

pub(super) fn table_descriptor_object_from_vals(
    vals: &[Val],
) -> DataIntegrityResult<TableDescriptorObject> {
    if vals.len() != DESCRIPTOR_COLUMN_COUNT {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog.table_descriptors value count {}, expected {DESCRIPTOR_COLUMN_COUNT}",
                vals.len()
            )),
        );
    }
    let table_id = vals[COL_NO_TABLE_DESCRIPTORS_TABLE_ID]
        .as_u64()
        .map(TableID::new)
        .filter(|table_id| table_id.is_user())
        .ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach("catalog.table_descriptors table_id is not a user table")
        })?;
    let descriptor_revision = vals[1].as_u64().ok_or_else(|| {
        Report::new(DataIntegrityError::InvalidPayload)
            .attach("catalog.table_descriptors descriptor_revision has wrong type")
    })?;
    let compiled_storage_epoch = vals[2].as_u64().ok_or_else(|| {
        Report::new(DataIntegrityError::InvalidPayload)
            .attach("catalog.table_descriptors compiled_storage_epoch has wrong type")
    })?;
    let fingerprint = vals[3].as_bytes().ok_or_else(|| {
        Report::new(DataIntegrityError::InvalidPayload)
            .attach("catalog.table_descriptors fingerprint has wrong type")
    })?;
    let storage_schema_fingerprint: [u8; 32] = fingerprint.try_into().map_err(|_| {
        Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "catalog.table_descriptors fingerprint length {}, expected 32",
            fingerprint.len()
        ))
    })?;
    let payload = vals[4].as_bytes().ok_or_else(|| {
        Report::new(DataIntegrityError::InvalidPayload)
            .attach("catalog.table_descriptors payload has wrong type")
    })?;
    if payload.len() > MAX_TABLE_DESCRIPTOR_BYTES {
        return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "catalog.table_descriptors payload length {} exceeds maximum {MAX_TABLE_DESCRIPTOR_BYTES}",
            payload.len()
        )));
    }
    Ok(TableDescriptorObject {
        table_id,
        descriptor_revision,
        compiled_storage_epoch,
        storage_schema_fingerprint,
        payload: payload.into(),
    })
}

#[inline]
fn cols_from_table_descriptor(descriptor: &TableDescriptorObject) -> Vec<Val> {
    vec![
        Val::from(descriptor.table_id),
        Val::from(descriptor.descriptor_revision),
        Val::from(descriptor.compiled_storage_epoch),
        Val::from(&descriptor.storage_schema_fingerprint),
        Val::from(descriptor.payload.as_ref()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableDescriptorObject;

    fn descriptor_vals(payload: Vec<u8>) -> Vec<Val> {
        vec![
            Val::from(TableID::new(7)),
            Val::from(3u64),
            Val::from(4u64),
            Val::from(vec![5; 32]),
            Val::from(payload),
        ]
    }

    fn assert_invalid(vals: &[Val]) {
        let error = table_descriptor_object_from_vals(vals).unwrap_err();
        assert_eq!(
            error.downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    fn assert_invalid_stamp(descriptor: &TableDescriptorObject, metadata: &TableMetadata) {
        let error =
            validate_table_descriptor_against_metadata(descriptor, TableID::new(7), metadata)
                .unwrap_err();
        assert_eq!(
            error.downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn descriptor_row_decode_accepts_payload_boundaries() {
        for len in [0, 63_999, MAX_TABLE_DESCRIPTOR_BYTES] {
            let descriptor =
                table_descriptor_object_from_vals(&descriptor_vals(vec![0xff; len])).unwrap();
            assert_eq!(descriptor.table_id, TableID::new(7));
            assert_eq!(descriptor.descriptor_revision, 3);
            assert_eq!(descriptor.compiled_storage_epoch, 4);
            assert_eq!(descriptor.storage_schema_fingerprint, [5; 32]);
            assert_eq!(descriptor.payload.len(), len);
        }
    }

    #[test]
    fn descriptor_row_decode_rejects_each_malformed_field() {
        assert_invalid(&[]);

        let mut vals = descriptor_vals(vec![]);
        vals[0] = Val::from(TABLE_ID_TABLE_DESCRIPTORS);
        assert_invalid(&vals);

        let mut vals = descriptor_vals(vec![]);
        vals[1] = Val::from(1u32);
        assert_invalid(&vals);

        let mut vals = descriptor_vals(vec![]);
        vals[2] = Val::from(1u32);
        assert_invalid(&vals);

        let mut vals = descriptor_vals(vec![]);
        vals[3] = Val::from(1u64);
        assert_invalid(&vals);

        let mut vals = descriptor_vals(vec![]);
        vals[3] = Val::from(vec![0; 31]);
        assert_invalid(&vals);

        let mut vals = descriptor_vals(vec![]);
        vals[4] = Val::from(1u64);
        assert_invalid(&vals);

        assert_invalid(&descriptor_vals(vec![0; MAX_TABLE_DESCRIPTOR_BYTES + 1]));
    }

    #[test]
    fn descriptor_stamp_validation_rejects_each_mismatch() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::I32,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let descriptor = TableDescriptorObject {
            table_id: TableID::new(7),
            descriptor_revision: 0,
            compiled_storage_epoch: metadata.storage_epoch,
            storage_schema_fingerprint: metadata.storage_schema_fingerprint(),
            payload: Box::default(),
        };
        validate_table_descriptor_against_metadata(&descriptor, TableID::new(7), &metadata)
            .unwrap();

        for descriptor in [
            TableDescriptorObject {
                table_id: TableID::new(8),
                ..descriptor.clone()
            },
            TableDescriptorObject {
                compiled_storage_epoch: 1,
                ..descriptor.clone()
            },
            TableDescriptorObject {
                storage_schema_fingerprint: [0; 32],
                ..descriptor.clone()
            },
        ] {
            assert_invalid_stamp(&descriptor, &metadata);
        }
    }
}
