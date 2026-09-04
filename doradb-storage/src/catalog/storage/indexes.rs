use crate::buffer::PoolGuards;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::IndexObject;
use crate::catalog::table::{TableColumnLayout, TableIndexKeySpec, TableMetadata};
use crate::catalog::{
    CatalogIndexNo, CatalogTable, ColumnID, ColumnOrdinal, IndexID, IndexOrder, IndexRef,
    IndexSlot, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
    StorageIndexSpec, catalog_table_id_from_slot,
};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, MultiDomainResultExt, RuntimeError,
    RuntimeOrFatalResult, RuntimeResult,
};
use crate::id::TableID;
use crate::map::FastHashSet;
use crate::row::ops::DeleteMvcc;
use crate::row::{Row, RowRead};
use crate::table::IndexLookupCriteria;
use crate::trx::PrivateTransaction;
use crate::value::{Val, ValKind};
use error_stack::{Report, ResultExt};
use std::sync::OnceLock;

/// Catalog table id for `catalog.indexes`.
pub(crate) const TABLE_ID_INDEXES: TableID = catalog_table_id_from_slot(2);
const COL_NO_INDEXES_TABLE_ID: usize = 0;
const COL_NO_INDEXES_INDEX_ID: usize = 1;
const COL_NO_INDEXES_INDEX_SLOT: usize = 2;
const COL_NO_INDEXES_INDEX_FLAGS: usize = 3;
const COL_NO_INDEXES_KEY_SPEC: usize = 4;
const PK_NO_INDEXES: CatalogIndexNo = CatalogIndexNo::new(0);
const KEY_SPEC_ENCODING_VERSION: u8 = 1;
const KEY_SPEC_HEADER_LEN: usize = 3;
const KEY_SPEC_ENTRY_LEN: usize = 5;

/// Runtime accessor for `catalog.indexes`.
pub(crate) struct Indexes<'a> {
    pub(super) table: &'a CatalogTable,
}

impl Indexes<'_> {
    /// Inserts one exact active index generation.
    pub(crate) async fn insert(
        &self,
        trx: &mut PrivateTransaction,
        obj: &IndexObject,
    ) -> RuntimeOrFatalResult<()> {
        trx.catalog_insert_mvcc(self.table, cols_from_index_object(obj))
            .await
            .map(|_| ())
            .attach_with(|| {
                format!(
                    "operation=catalog_indexes_insert, table_id={}, index={}",
                    obj.table_id, obj.index
                )
            })
    }

    /// Inserts an ordered exact-generation batch.
    pub(crate) async fn insert_batch(
        &self,
        trx: &mut PrivateTransaction,
        objects: &[IndexObject],
    ) -> RuntimeOrFatalResult<()> {
        let rows = objects.iter().map(cols_from_index_object).collect();
        trx.catalog_insert_batch_mvcc(self.table, rows)
            .await
            .attach("operation=catalog_indexes_insert_batch")
    }

    /// Deletes an index by stable `(table_id, index_id)` identity.
    pub(crate) async fn delete_by_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
        index_id: IndexID,
    ) -> RuntimeOrFatalResult<bool> {
        let key_vals = vec![Val::from(table_id), Val::from(index_id.get())];
        let res = trx
            .catalog_delete_primary_key_mvcc(self.table, PK_NO_INDEXES, key_vals)
            .await
            .attach_with(|| {
                format!(
                    "operation=catalog_indexes_delete, table_id={table_id}, index_id={index_id}"
                )
            })?;
        Ok(matches!(res, DeleteMvcc::Deleted))
    }

    /// Deletes all active indexes for one table.
    pub(crate) async fn delete_by_table_id(
        &self,
        trx: &mut PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeOrFatalResult<usize> {
        let indexes = self.list_current_locked_by_table_id(trx, table_id).await?;
        let keys = indexes
            .into_iter()
            .map(|index| vec![Val::from(table_id), Val::from(index.index.id().get())])
            .collect();
        trx.catalog_delete_primary_key_batch_mvcc(self.table, PK_NO_INDEXES, keys)
            .await
            .attach_with(|| {
                format!("operation=catalog_indexes_delete_by_table, table_id={table_id}")
            })
    }

    /// Lists one table's indexes through its bounded primary-key range in the
    /// owning DDL transaction's locked current view.
    async fn list_current_locked_by_table_id(
        &self,
        trx: &PrivateTransaction,
        table_id: TableID,
    ) -> RuntimeResult<Vec<IndexObject>> {
        let lower = [Val::from(table_id), Val::from(0u32)];
        let upper = [Val::from(table_id), Val::from(u32::MAX)];
        let mut indexes = Vec::new();
        let mut decode_error = None;
        self.table
            .index_lookup_current_locked(
                trx,
                PK_NO_INDEXES,
                IndexLookupCriteria::UniqueInclusive {
                    lower: &lower,
                    upper: &upper,
                },
                |col_layout, row| match row_to_index_object(col_layout, row) {
                    Ok(index) => {
                        indexes.push(index);
                        true
                    }
                    Err(err) => {
                        decode_error = Some(err);
                        false
                    }
                },
            )
            .await
            .attach_with(|| {
                format!("operation=list_locked_catalog_indexes, table_id={table_id}")
            })?;
        if let Some(err) = decode_error {
            return Err(err
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=list_locked_catalog_indexes, phase=decode_row"));
        }
        Ok(indexes)
    }

    /// Lists all active indexes for one table.
    pub(crate) async fn list_uncommitted_by_table_id(
        &self,
        guards: &PoolGuards,
        table_id: TableID,
    ) -> RuntimeResult<Vec<IndexObject>> {
        let mut result = Vec::new();
        let mut decode_error = None;
        self.table
            .table_scan_uncommitted(guards, |col_layout, row| {
                if row.is_deleted() {
                    return true;
                }
                let Some(row_table_id) = row.val(col_layout, COL_NO_INDEXES_TABLE_ID).as_u64()
                else {
                    decode_error = Some(
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach("catalog.indexes table_id has wrong type"),
                    );
                    return false;
                };
                if row_table_id != table_id.as_u64() {
                    return true;
                }
                match row_to_index_object(col_layout, row) {
                    Ok(object) => result.push(object),
                    Err(err) => {
                        decode_error = Some(err);
                        return false;
                    }
                }
                true
            })
            .await
            .change_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation=list_catalog_indexes, table_id={table_id}"))?;
        if let Some(err) = decode_error {
            return Err(err
                .change_context(RuntimeError::CatalogAccess)
                .attach("operation=list_catalog_indexes, phase=decode_row"));
        }
        Ok(result)
    }
}

/// Encodes the canonical catalog index-key payload.
pub(crate) fn encode_index_key_spec(keys: &[TableIndexKeySpec]) -> Vec<u8> {
    assert!(
        !keys.is_empty() && keys.len() <= usize::from(u16::MAX),
        "validated index key count must fit canonical catalog payload"
    );
    let mut payload = Vec::with_capacity(KEY_SPEC_HEADER_LEN + keys.len() * KEY_SPEC_ENTRY_LEN);
    payload.push(KEY_SPEC_ENCODING_VERSION);
    payload.extend_from_slice(&(keys.len() as u16).to_le_bytes());
    for key in keys {
        payload.extend_from_slice(&key.column_id.get().to_le_bytes());
        payload.push(key.order as u8);
    }
    payload
}

/// Decodes the canonical catalog index-key payload and rejects ambiguity.
pub(crate) fn decode_index_key_spec(
    payload: &[u8],
) -> DataIntegrityResult<Vec<(ColumnID, IndexOrder)>> {
    if payload.len() < KEY_SPEC_HEADER_LEN {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach("catalog index key_spec is truncated"));
    }
    if payload[0] != KEY_SPEC_ENCODING_VERSION {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "unknown catalog index key_spec version {}",
                payload[0]
            )),
        );
    }
    let count = usize::from(u16::from_le_bytes([payload[1], payload[2]]));
    if count == 0 {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach("catalog index key_spec has zero keys"));
    }
    let expected = KEY_SPEC_HEADER_LEN
        .checked_add(count.checked_mul(KEY_SPEC_ENTRY_LEN).ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach("catalog index key_spec length overflow")
        })?)
        .ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach("catalog index key_spec length overflow")
        })?;
    if payload.len() != expected {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog index key_spec length mismatch: actual={}, expected={expected}",
                payload.len()
            )),
        );
    }
    let mut seen = FastHashSet::default();
    let mut keys = Vec::with_capacity(count);
    let mut offset = KEY_SPEC_HEADER_LEN;
    for _ in 0..count {
        let column_id = ColumnID::new(u32::from_le_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]));
        let order_raw = payload[offset + 4];
        let order = IndexOrder::try_from(order_raw).map_err(|()| {
            Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("unknown catalog index key order {order_raw}"))
        })?;
        if !seen.insert(column_id) {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("duplicate catalog index key column id {column_id}")));
        }
        keys.push((column_id, order));
        offset += KEY_SPEC_ENTRY_LEN;
    }
    Ok(keys)
}

/// Returns the final definition of `catalog.indexes`.
pub(super) fn catalog_definition_of_indexes() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| CatalogDefinition {
        table_id: TABLE_ID_INDEXES,
        metadata: TableMetadata::try_new(
            vec![
                // table_id U64: owning user table.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // index_id U32: stable table-local index identity.
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                // index_slot U16: physical table-file/runtime position.
                StorageColumnSpec::new(ValKind::U16, StorageColumnFlags::empty()),
                // index_flags U32: PK/UK storage semantics.
                StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::empty()),
                // key_spec VARBYTE: versioned ordered stable-column key payload.
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ],
            vec![
                // Primary key: (table_id, index_id).
                StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0), StorageIndexKey::new(1)],
                    StorageIndexFlags::PK,
                ),
                // Unique physical-position mapping: (table_id, index_slot).
                StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0), StorageIndexKey::new(2)],
                    StorageIndexFlags::UK,
                ),
            ],
        )
        .expect("valid catalog.indexes metadata"),
    })
}

#[inline]
fn cols_from_index_object(obj: &IndexObject) -> Vec<Val> {
    vec![
        Val::from(obj.table_id),
        Val::from(obj.index.id().get()),
        Val::from(obj.index.slot().get()),
        Val::from(obj.index_flags.bits()),
        Val::from(encode_index_key_spec(&obj.keys)),
    ]
}

fn row_to_index_object(
    col_layout: &TableColumnLayout,
    row: Row<'_>,
) -> DataIntegrityResult<IndexObject> {
    let vals = (0..5)
        .map(|idx| row.val(col_layout, idx))
        .collect::<Vec<_>>();
    index_object_from_vals(&vals)
}

pub(super) fn index_object_from_vals(vals: &[Val]) -> DataIntegrityResult<IndexObject> {
    if vals.len() != 5 {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "catalog.indexes value count {}, expected 5",
                vals.len()
            )),
        );
    }
    let table_id = vals[COL_NO_INDEXES_TABLE_ID]
        .as_u64()
        .map(TableID::from)
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let index_id = vals[COL_NO_INDEXES_INDEX_ID]
        .as_u32()
        .map(IndexID::new)
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let index_slot = vals[COL_NO_INDEXES_INDEX_SLOT]
        .as_u16()
        .map(IndexSlot::new)
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let flags_raw = vals[COL_NO_INDEXES_INDEX_FLAGS]
        .as_u32()
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let index_flags = StorageIndexFlags::from_bits(flags_raw).ok_or_else(|| {
        Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!("unknown catalog index flags {flags_raw:#x}"))
    })?;
    let payload = vals[COL_NO_INDEXES_KEY_SPEC]
        .as_bytes()
        .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))?;
    let keys = decode_index_key_spec(payload)?
        .into_iter()
        .map(|(column_id, order)| TableIndexKeySpec {
            column_id,
            // Catalog rows persist stable IDs; table reconstruction installs
            // and validates the physical ordinal exactly once.
            column_ordinal: ColumnOrdinal::new(0),
            order,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(IndexObject {
        table_id,
        index: IndexRef::new(index_id, index_slot),
        index_flags,
        keys,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_spec_codec_rejects_ambiguous_payloads() {
        let keys = [TableIndexKeySpec {
            column_id: ColumnID::new(0x0102_0304),
            column_ordinal: ColumnOrdinal::new(7),
            order: IndexOrder::Desc,
        }];
        let payload = encode_index_key_spec(&keys);
        assert_eq!(payload, [1, 1, 0, 4, 3, 2, 1, 1]);
        assert_eq!(
            decode_index_key_spec(&payload).unwrap(),
            vec![(ColumnID::new(0x0102_0304), IndexOrder::Desc)]
        );
        assert!(decode_index_key_spec(&[]).is_err());
        assert!(decode_index_key_spec(&[2, 1, 0, 0, 0, 0, 0, 0]).is_err());
        assert!(decode_index_key_spec(&[1, 0, 0]).is_err());
        assert!(decode_index_key_spec(&[1, 1, 0, 0, 0, 0, 0]).is_err());
        assert!(decode_index_key_spec(&[1, 1, 0, 0, 0, 0, 0, 2]).is_err());
        assert!(decode_index_key_spec(&[1, 1, 0, 0, 0, 0, 0, 0, 0]).is_err());
        assert!(decode_index_key_spec(&[1, 2, 0, 7, 0, 0, 0, 0, 7, 0, 0, 0, 1]).is_err());
    }
}
