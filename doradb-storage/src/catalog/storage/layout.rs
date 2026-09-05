//! Pure, canonical built-in catalog table identity layout.

use crate::id::TableID;

/// Catalog table id for `catalog.tables`.
pub(crate) const TABLE_ID_TABLES: TableID = catalog_table_id_from_slot(0);

/// Catalog table id for `catalog.columns`.
pub(crate) const TABLE_ID_COLUMNS: TableID = catalog_table_id_from_slot(1);

/// Catalog table id for `catalog.indexes`.
pub(crate) const TABLE_ID_INDEXES: TableID = catalog_table_id_from_slot(2);

/// Catalog table id for `catalog.table_descriptors`.
pub(crate) const TABLE_ID_TABLE_DESCRIPTORS: TableID = catalog_table_id_from_slot(3);

/// Catalog table id for `catalog.table_replay_silent_watermarks`.
pub(crate) const TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS: TableID = catalog_table_id_from_slot(4);

/// Catalog table id for `catalog.table_bindings`.
pub(crate) const TABLE_ID_TABLE_BINDINGS: TableID = catalog_table_id_from_slot(5);

/// Built-in catalog identities in durable root and bootstrap order.
pub(crate) const BUILTIN_CATALOG_TABLE_IDS: [TableID; 6] = [
    TABLE_ID_TABLES,
    TABLE_ID_COLUMNS,
    TABLE_ID_INDEXES,
    TABLE_ID_TABLE_DESCRIPTORS,
    TABLE_ID_TABLE_REPLAY_SILENT_WATERMARKS,
    TABLE_ID_TABLE_BINDINGS,
];

/// Number of built-in catalog tables.
pub(crate) const BUILTIN_CATALOG_TABLE_COUNT: usize = BUILTIN_CATALOG_TABLE_IDS.len();

/// Build a built-in catalog table id from its dense root slot.
#[inline]
pub(crate) const fn catalog_table_id_from_slot(slot: usize) -> TableID {
    TableID::new(TableID::CATALOG_START.as_u64() + slot as u64)
}

/// Return the dense root slot for a built-in catalog table id.
#[inline]
pub(crate) const fn catalog_table_slot(table_id: TableID) -> Option<usize> {
    if table_id.is_catalog() {
        Some((table_id.as_u64() - TableID::CATALOG_START.as_u64()) as usize)
    } else {
        None
    }
}

/// Selects only known built-in identities, without narrowing unbounded offsets.
#[inline]
pub(crate) fn builtin_catalog_table_slot(table_id: TableID) -> Option<usize> {
    let offset = table_id
        .as_u64()
        .checked_sub(BUILTIN_CATALOG_TABLE_IDS[0].as_u64())?;
    if offset >= BUILTIN_CATALOG_TABLE_COUNT as u64 {
        return None;
    }
    let slot = offset as usize;
    (BUILTIN_CATALOG_TABLE_IDS[slot] == table_id).then_some(slot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::multi_table_file::CATALOG_TABLE_ROOT_DESC_COUNT;

    #[test]
    fn builtin_layout_preserves_six_durable_ids_and_exact_routing() {
        assert_eq!(BUILTIN_CATALOG_TABLE_COUNT, 6);
        assert_eq!(CATALOG_TABLE_ROOT_DESC_COUNT, 6);
        for (slot, id) in BUILTIN_CATALOG_TABLE_IDS.into_iter().enumerate() {
            assert_eq!(id.as_u64(), (1_u64 << 63) + slot as u64);
            assert_eq!(catalog_table_id_from_slot(slot), id);
            assert_eq!(catalog_table_slot(id), Some(slot));
            assert_eq!(builtin_catalog_table_slot(id), Some(slot));
        }
        for value in [
            0,
            (1_u64 << 63) - 1,
            (1_u64 << 63) + 6,
            (1_u64 << 63) + (1_u64 << 32),
            u64::MAX,
        ] {
            assert_eq!(
                builtin_catalog_table_slot(TableID::new(value)),
                None,
                "id={value}"
            );
        }
        assert_eq!(catalog_table_slot(catalog_table_id_from_slot(6)), Some(6));
    }
}
