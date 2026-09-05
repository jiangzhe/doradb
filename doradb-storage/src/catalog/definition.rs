use super::spec::{
    CreateIndexDefinition, CreateTableDefinition, DropIndexDefinition, StorageTableDefinition,
};
use super::storage::{TableBindingObject, TableDescriptorObject};
use crate::error::{OperationError, OperationResult};
use crate::id::TableID;
use crate::map::FastHashSet;
use error_stack::Report;
use std::result::Result as StdResult;

/// Maximum persisted opaque descriptor payload accepted by managed table DDL.
pub const MAX_TABLE_DESCRIPTOR_BYTES: usize = 64_000;

/// Maximum opaque key length accepted for one managed table binding.
pub const MAX_TABLE_BINDING_KEY_BYTES: usize = 16_000;

// One descriptor row has three U64 columns, one 32-byte fingerprint, and two
// one-row VarByte offset tables. The exact current LWC estimate adds 151 bytes
// around the payload. Keep a separate conservative row-page proof so the
// public maximum cannot become constructible in memory but uncheckpointable.
const MAX_DESCRIPTOR_CHECKPOINT_ROW_BYTES: usize = MAX_TABLE_DESCRIPTOR_BYTES + 151;
const MAX_DESCRIPTOR_ROW_PAGE_BYTES: usize = MAX_TABLE_DESCRIPTOR_BYTES + 512;
const _: () = assert!(MAX_DESCRIPTOR_CHECKPOINT_ROW_BYTES <= crate::lwc::LWC_BLOCK_PAYLOAD_SIZE);
const _: () = assert!(MAX_DESCRIPTOR_ROW_PAGE_BYTES <= crate::row::ROW_PAGE_USABLE_SIZE);

// One binding row contains two U64 values, one VarByte value, and the small
// fixed/offset tables used by the row and LWC encodings. Its composite primary
// key is one U64 followed by the unescaped final VarByte component.
const MAX_BINDING_CHECKPOINT_ROW_BYTES: usize = MAX_TABLE_BINDING_KEY_BYTES + 128;
const MAX_BINDING_ROW_PAGE_BYTES: usize = MAX_TABLE_BINDING_KEY_BYTES + 256;
const MAX_BINDING_BTREE_ENTRY_BYTES: usize = MAX_TABLE_BINDING_KEY_BYTES + 1024;
const _: () = assert!(MAX_TABLE_BINDING_KEY_BYTES <= u16::MAX as usize);
const _: () = assert!(MAX_BINDING_CHECKPOINT_ROW_BYTES <= crate::lwc::LWC_BLOCK_PAYLOAD_SIZE);
const _: () = assert!(MAX_BINDING_ROW_PAGE_BYTES <= crate::row::ROW_PAGE_USABLE_SIZE);
const _: () = assert!(MAX_BINDING_BTREE_ENTRY_BYTES <= crate::index::BTREE_NODE_USABLE_SIZE);

/// Opaque higher-layer namespace identity for managed table bindings.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct BindingNamespaceID(u64);

impl BindingNamespaceID {
    /// Creates a binding namespace identity from its opaque numeric value.
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the opaque numeric namespace value.
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// One roleless opaque lookup key supplied with managed CREATE TABLE.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TableBinding {
    namespace_id: BindingNamespaceID,
    binding_key: Box<[u8]>,
}

impl TableBinding {
    /// Creates one namespace-local opaque table binding.
    #[inline]
    pub fn new(namespace_id: BindingNamespaceID, binding_key: impl Into<Box<[u8]>>) -> Self {
        Self {
            namespace_id,
            binding_key: binding_key.into(),
        }
    }

    /// Returns the binding namespace identity.
    #[inline]
    pub const fn namespace_id(&self) -> BindingNamespaceID {
        self.namespace_id
    }

    /// Returns the exact opaque binding-key bytes.
    #[inline]
    pub const fn binding_key(&self) -> &[u8] {
        &self.binding_key
    }

    /// Consumes this binding into its namespace and opaque key.
    #[inline]
    pub fn into_parts(self) -> (BindingNamespaceID, Box<[u8]>) {
        (self.namespace_id, self.binding_key)
    }
}

/// Complete ID-free result of interpreting managed CREATE TABLE input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedCreateTableDefinition {
    storage: CreateTableDefinition,
    descriptor: Box<[u8]>,
    bindings: Box<[TableBinding]>,
}

impl ManagedCreateTableDefinition {
    /// Creates one atomic managed table definition bundle.
    #[inline]
    pub fn new(
        storage: CreateTableDefinition,
        descriptor: impl Into<Box<[u8]>>,
        bindings: impl Into<Box<[TableBinding]>>,
    ) -> Self {
        Self {
            storage,
            descriptor: descriptor.into(),
            bindings: bindings.into(),
        }
    }

    /// Returns the ID-free numeric storage definition.
    #[inline]
    pub const fn storage(&self) -> &CreateTableDefinition {
        &self.storage
    }

    /// Returns the complete opaque managed descriptor.
    #[inline]
    pub const fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    /// Returns every roleless binding in callback order.
    #[inline]
    pub const fn bindings(&self) -> &[TableBinding] {
        &self.bindings
    }

    /// Consumes the bundle into its storage, descriptor, and binding parts.
    #[inline]
    pub fn into_parts(self) -> (CreateTableDefinition, Box<[u8]>, Box<[TableBinding]>) {
        (self.storage, self.descriptor, self.bindings)
    }
}

/// Opaque cache-invalidation token for one managed table definition.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TableDefinitionVersion {
    table_id: TableID,
    storage_epoch: u64,
}

impl TableDefinitionVersion {
    /// Builds the private representation returned by one admitted resolution.
    #[inline]
    pub(crate) const fn new(table_id: TableID, storage_epoch: u64) -> Self {
        Self {
            table_id,
            storage_epoch,
        }
    }
}

/// Coherent numeric-schema and opaque-descriptor snapshot for a managed table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedTableDefinitionSnapshot {
    schema: StorageTableDefinition,
    descriptor: Box<[u8]>,
}

impl ManagedTableDefinitionSnapshot {
    /// Builds one internally validated full managed-definition snapshot.
    #[inline]
    pub(crate) fn new(schema: StorageTableDefinition, descriptor: Box<[u8]>) -> Self {
        Self { schema, descriptor }
    }

    /// Returns the stable-ID numeric storage schema.
    #[inline]
    pub const fn schema(&self) -> &StorageTableDefinition {
        &self.schema
    }

    /// Returns the exact opaque descriptor bytes.
    #[inline]
    pub const fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    /// Consumes this snapshot into its coherent schema and descriptor parts.
    #[inline]
    pub fn into_parts(self) -> (StorageTableDefinition, Box<[u8]>) {
        (self.schema, self.descriptor)
    }
}

/// Result of resolving one opaque managed table binding at an admitted point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedTableBinding {
    table_id: TableID,
    version: TableDefinitionVersion,
    full_schema: Option<ManagedTableDefinitionSnapshot>,
}

impl ResolvedTableBinding {
    /// Builds one internally validated binding-resolution result.
    #[inline]
    pub(crate) const fn new(
        table_id: TableID,
        version: TableDefinitionVersion,
        full_schema: Option<ManagedTableDefinitionSnapshot>,
    ) -> Self {
        Self {
            table_id,
            version,
            full_schema,
        }
    }

    /// Returns the resolved storage-assigned table identity.
    #[inline]
    pub const fn table_id(&self) -> TableID {
        self.table_id
    }

    /// Returns the opaque definition version observed by this resolution.
    #[inline]
    pub const fn version(&self) -> TableDefinitionVersion {
        self.version
    }

    /// Returns the coherent full definition when it was requested.
    #[inline]
    pub const fn full_schema(&self) -> Option<&ManagedTableDefinitionSnapshot> {
        self.full_schema.as_ref()
    }

    /// Consumes the result into its table, version, and optional full definition.
    #[inline]
    pub fn into_parts(
        self,
    ) -> (
        TableID,
        TableDefinitionVersion,
        Option<ManagedTableDefinitionSnapshot>,
    ) {
        (self.table_id, self.version, self.full_schema)
    }
}

/// One typed physical change paired with its complete replacement descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptorUpdate<C> {
    change: C,
    descriptor: Box<[u8]>,
}

impl<C> DescriptorUpdate<C> {
    /// Creates a descriptor replacement from a typed change and opaque bytes.
    #[inline]
    pub fn new(change: C, descriptor: impl Into<Box<[u8]>>) -> Self {
        Self {
            change,
            descriptor: descriptor.into(),
        }
    }

    /// Returns the typed physical change.
    #[inline]
    pub const fn change(&self) -> &C {
        &self.change
    }

    /// Returns the complete opaque replacement descriptor.
    #[inline]
    pub const fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    /// Consumes the update into its typed change and opaque descriptor.
    #[inline]
    pub fn into_parts(self) -> (C, Box<[u8]>) {
        (self.change, self.descriptor)
    }
}

/// Synchronous higher-layer interpreter for opaque managed table requests.
pub trait ManagedTableInterpreter {
    /// User-defined interpretation failure preserved by the managed DDL boundary.
    type Error;

    /// Interprets an opaque request for a new ID-free table definition.
    fn create_table(
        &mut self,
        source: &[u8],
    ) -> StdResult<ManagedCreateTableDefinition, Self::Error>;

    /// Interprets an opaque request to add one index to a current managed table.
    fn create_index(
        &mut self,
        source: &[u8],
        previous_descriptor: &[u8],
        current_schema: &StorageTableDefinition,
        proposed_index_id: super::IndexID,
    ) -> StdResult<DescriptorUpdate<CreateIndexDefinition>, Self::Error>;

    /// Interprets an opaque request to drop one index from a current managed table.
    fn drop_index(
        &mut self,
        source: &[u8],
        previous_descriptor: &[u8],
        current_schema: &StorageTableDefinition,
    ) -> StdResult<DescriptorUpdate<DropIndexDefinition>, Self::Error>;
}

/// Private optimistic definition copied while target metadata-S is held.
pub(crate) struct CurrentTableDefinition {
    schema: StorageTableDefinition,
    descriptor: TableDescriptorObject,
    storage_epoch: u64,
    effective_next_index_id: u64,
}

impl CurrentTableDefinition {
    /// Builds one private coherent preflight snapshot.
    #[inline]
    pub(crate) fn new(
        schema: StorageTableDefinition,
        descriptor: TableDescriptorObject,
        storage_epoch: u64,
        effective_next_index_id: u64,
    ) -> Self {
        Self {
            schema,
            descriptor,
            storage_epoch,
            effective_next_index_id,
        }
    }

    /// Returns the slot-free public projection supplied to the interpreter.
    #[inline]
    pub(crate) const fn schema(&self) -> &StorageTableDefinition {
        &self.schema
    }

    /// Returns the exact descriptor envelope copied during preflight.
    #[inline]
    pub(crate) const fn descriptor(&self) -> &TableDescriptorObject {
        &self.descriptor
    }

    /// Returns the private expected storage epoch.
    #[inline]
    pub(crate) const fn storage_epoch(&self) -> u64 {
        self.storage_epoch
    }

    /// Returns the private effective stable-index allocator watermark.
    #[inline]
    pub(crate) const fn effective_next_index_id(&self) -> u64 {
        self.effective_next_index_id
    }
}

/// Extensible storage-owned catalog effects accepted with a DDL plan.
#[derive(Clone)]
pub(crate) struct CatalogDefinitionEffects {
    descriptor: TableDescriptorEffect,
    bindings: TableBindingEffect,
}

impl CatalogDefinitionEffects {
    /// Builds an empty definition-effect bundle for unmanaged DDL.
    #[inline]
    pub(crate) const fn none() -> Self {
        Self {
            descriptor: TableDescriptorEffect::None,
            bindings: TableBindingEffect::None,
        }
    }

    /// Builds a managed CREATE TABLE descriptor and binding insertion bundle.
    #[inline]
    pub(crate) fn insert(
        descriptor: TableDescriptorObject,
        bindings: Box<[TableBindingObject]>,
    ) -> Self {
        Self {
            descriptor: TableDescriptorEffect::Insert(descriptor),
            bindings: if bindings.is_empty() {
                TableBindingEffect::None
            } else {
                TableBindingEffect::Insert(bindings)
            },
        }
    }

    /// Builds a managed index DDL descriptor replacement bundle.
    #[inline]
    pub(crate) const fn replace(descriptor: TableDescriptorObject) -> Self {
        Self {
            descriptor: TableDescriptorEffect::Replace(descriptor),
            bindings: TableBindingEffect::None,
        }
    }

    /// Builds a DROP TABLE descriptor and reverse-binding deletion bundle.
    #[inline]
    pub(crate) const fn delete_if_present(table_id: TableID) -> Self {
        Self {
            descriptor: TableDescriptorEffect::DeleteIfPresent(table_id),
            bindings: TableBindingEffect::DeleteByTableID(table_id),
        }
    }

    /// Returns the descriptor portion of the extensible bundle.
    #[inline]
    pub(crate) const fn descriptor(&self) -> &TableDescriptorEffect {
        &self.descriptor
    }

    /// Returns the binding portion of the extensible bundle.
    #[inline]
    pub(crate) const fn bindings(&self) -> &TableBindingEffect {
        &self.bindings
    }
}

/// Descriptor-row mutation committed with one numeric catalog DDL operation.
#[derive(Clone)]
pub(crate) enum TableDescriptorEffect {
    /// No descriptor change for unmanaged DDL.
    None,
    /// Insert a new managed descriptor.
    Insert(TableDescriptorObject),
    /// Replace a required current managed descriptor.
    Replace(TableDescriptorObject),
    /// Delete a descriptor if the dropped table was managed.
    DeleteIfPresent(TableID),
}

/// Binding-row mutation committed with one numeric catalog DDL operation.
#[derive(Clone)]
pub(crate) enum TableBindingEffect {
    /// No binding change for numeric or index DDL.
    None,
    /// Insert bindings owned by a newly created managed table.
    Insert(Box<[TableBindingObject]>),
    /// Delete every binding that references a dropped table.
    DeleteByTableID(TableID),
}

/// Validates the live managed DDL descriptor payload envelope.
#[inline]
pub(crate) fn validate_descriptor_payload(payload: &[u8]) -> OperationResult<()> {
    if payload.len() > MAX_TABLE_DESCRIPTOR_BYTES || payload.len() > usize::from(u16::MAX) {
        return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
            "managed table descriptor exceeds maximum: actual={}, maximum={MAX_TABLE_DESCRIPTOR_BYTES}",
            payload.len()
        )));
    }
    Ok(())
}

/// Validates one externally supplied binding key before operation admission.
#[inline]
pub(crate) fn validate_table_binding_key(binding_key: &[u8]) -> OperationResult<()> {
    if binding_key.len() > MAX_TABLE_BINDING_KEY_BYTES || binding_key.len() > usize::from(u16::MAX)
    {
        return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
            "managed table binding key exceeds maximum: actual={}, maximum={MAX_TABLE_BINDING_KEY_BYTES}",
            binding_key.len()
        )));
    }
    Ok(())
}

/// Validates binding bounds and namespace-local uniqueness before ID allocation.
pub(crate) fn validate_table_bindings(bindings: &[TableBinding]) -> OperationResult<()> {
    let mut seen = FastHashSet::default();
    for binding in bindings {
        validate_table_binding_key(binding.binding_key())?;
        if !seen.insert((binding.namespace_id(), binding.binding_key())) {
            return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
                "managed CREATE TABLE repeats binding: namespace_id={}, binding_key_len={}",
                binding.namespace_id().as_u64(),
                binding.binding_key().len()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageColumnFlags, StorageColumnSpec, StorageTableSpec, ValKind};
    use std::collections::HashSet;

    #[test]
    fn descriptor_update_exposes_and_consumes_both_parts() {
        let update = DescriptorUpdate::new(42, &b"\xff\0"[..]);
        assert_eq!(update.change(), &42);
        assert_eq!(update.descriptor(), [0xff, 0]);
        let (change, descriptor) = update.into_parts();
        assert_eq!(change, 42);
        assert_eq!(&*descriptor, [0xff, 0]);
    }

    #[test]
    fn managed_create_bundle_and_binding_accessors_preserve_opaque_bytes() {
        let binding = TableBinding::new(BindingNamespaceID::new(9), &b"\0name\xff"[..]);
        assert_eq!(binding.namespace_id(), BindingNamespaceID::new(9));
        assert_eq!(binding.binding_key(), b"\0name\xff");
        let definition = ManagedCreateTableDefinition::new(
            CreateTableDefinition::new(
                StorageTableSpec::new(vec![StorageColumnSpec::new(
                    ValKind::I32,
                    StorageColumnFlags::empty(),
                )]),
                vec![],
            ),
            &b"descriptor\0"[..],
            vec![binding.clone()],
        );
        assert_eq!(definition.storage().table().columns.len(), 1);
        assert_eq!(definition.descriptor(), b"descriptor\0");
        assert_eq!(definition.bindings().len(), 1);
        assert_eq!(&definition.bindings()[0], &binding);
        let (_, descriptor, bindings) = definition.into_parts();
        assert_eq!(&*descriptor, b"descriptor\0");
        assert_eq!(bindings.len(), 1);
        assert_eq!(&bindings[0], &binding);
        assert_eq!(
            binding.into_parts(),
            (BindingNamespaceID::new(9), Box::from(&b"\0name\xff"[..]))
        );
    }

    #[test]
    fn definition_version_is_opaque_and_hashes_as_one_token() {
        let version = TableDefinitionVersion::new(TableID::new(7), 3);
        let same = TableDefinitionVersion::new(TableID::new(7), 3);
        let changed_epoch = TableDefinitionVersion::new(TableID::new(7), 4);
        let changed_table = TableDefinitionVersion::new(TableID::new(8), 3);
        let versions = HashSet::from([version, same, changed_epoch, changed_table]);
        assert_eq!(versions.len(), 3);
    }

    #[test]
    fn resolved_binding_consumes_coherent_optional_snapshot() {
        let schema = StorageTableDefinition::new(vec![], vec![]);
        let version = TableDefinitionVersion::new(TableID::new(7), 3);
        let resolved = ResolvedTableBinding::new(
            TableID::new(7),
            version,
            Some(ManagedTableDefinitionSnapshot::new(
                schema.clone(),
                Box::from(&b"descriptor"[..]),
            )),
        );
        assert_eq!(resolved.table_id(), TableID::new(7));
        assert_eq!(resolved.version(), version);
        assert_eq!(resolved.full_schema().unwrap().schema(), &schema);
        assert_eq!(resolved.full_schema().unwrap().descriptor(), b"descriptor");
        let (table_id, consumed_version, snapshot) = resolved.into_parts();
        assert_eq!(table_id, TableID::new(7));
        assert_eq!(consumed_version, version);
        assert_eq!(
            snapshot.unwrap().into_parts(),
            (schema, Box::from(&b"descriptor"[..]))
        );

        let narrow = ResolvedTableBinding::new(TableID::new(7), version, None);
        assert!(narrow.full_schema().is_none());
    }

    #[test]
    fn table_binding_validation_accepts_boundaries_and_rejects_duplicates() {
        validate_table_bindings(&[
            TableBinding::new(BindingNamespaceID::new(1), Vec::<u8>::new()),
            TableBinding::new(
                BindingNamespaceID::new(1),
                vec![0xff; MAX_TABLE_BINDING_KEY_BYTES],
            ),
            TableBinding::new(BindingNamespaceID::new(2), Vec::<u8>::new()),
        ])
        .unwrap();
        assert!(validate_table_binding_key(&vec![0; MAX_TABLE_BINDING_KEY_BYTES + 1]).is_err());
        assert!(
            validate_table_bindings(&[
                TableBinding::new(BindingNamespaceID::new(1), b"same".as_slice()),
                TableBinding::new(BindingNamespaceID::new(1), b"same".as_slice()),
            ])
            .is_err()
        );
    }
}
