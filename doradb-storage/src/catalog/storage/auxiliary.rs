use crate::catalog::storage::CatalogDefinition;
use crate::catalog::{
    CatalogIndexNo, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
    StorageIndexSpec, TableMetadata, catalog_table_id_from_slot,
};
use crate::id::TableID;
use crate::value::ValKind;
use std::sync::OnceLock;

/// Catalog table id for the empty descriptor storage installed by Phase 3.
pub(crate) const TABLE_ID_TABLE_DESCRIPTORS: TableID = catalog_table_id_from_slot(3);
/// Catalog table id for the empty binding storage installed by Phase 3.
pub(crate) const TABLE_ID_TABLE_BINDINGS: TableID = catalog_table_id_from_slot(5);
/// Primary-key slot of `catalog.table_descriptors`.
pub(super) const PK_NO_TABLE_DESCRIPTORS: CatalogIndexNo = CatalogIndexNo::new(0);
/// Reverse `table_id` slot of `catalog.table_bindings`.
pub(super) const TABLE_ID_NO_TABLE_BINDINGS: CatalogIndexNo = CatalogIndexNo::new(1);

/// Returns the final, initially empty `catalog.table_descriptors` definition.
pub(super) fn catalog_definition_of_table_descriptors() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| CatalogDefinition {
        table_id: TABLE_ID_TABLE_DESCRIPTORS,
        metadata: TableMetadata::try_new(
            vec![
                // table_id U64: described user table.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // descriptor_revision U64: monotonic descriptor revision.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // compiled_storage_epoch U64: storage epoch used for compilation.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // storage_schema_fingerprint VARBYTE: canonical 32-byte schema digest.
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                // payload VARBYTE: opaque managed descriptor payload.
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ],
            vec![
                // Primary key: table_id.
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::PK),
            ],
        )
        .expect("valid catalog.table_descriptors metadata"),
    })
}

/// Returns the final, initially empty `catalog.table_bindings` definition.
pub(super) fn catalog_definition_of_table_bindings() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| CatalogDefinition {
        table_id: TABLE_ID_TABLE_BINDINGS,
        metadata: TableMetadata::try_new(
            vec![
                // namespace_id U64: binding namespace identity.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // binding_key VARBYTE: opaque name/key bytes within the namespace.
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                // table_id U64: bound user table.
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                // binding_role U8: role of this binding.
                StorageColumnSpec::new(ValKind::U8, StorageColumnFlags::empty()),
            ],
            vec![
                // Primary key: (namespace_id, binding_key).
                StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0), StorageIndexKey::new(1)],
                    StorageIndexFlags::PK,
                ),
                // Secondary lookup: table_id.
                StorageIndexSpec::new(vec![StorageIndexKey::new(2)], StorageIndexFlags::empty()),
            ],
        )
        .expect("valid catalog.table_bindings metadata"),
    })
}
