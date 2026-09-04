use super::index_ref::{ColumnID, ColumnOrdinal, IndexID};
use super::table::TableMetadata;
use crate::error::{OperationError, OperationResult};
use crate::map::FastHashSet;
use crate::value::ValKind;
use bitflags::bitflags;
use error_stack::Report;
#[cfg(test)]
pub(crate) use tests::ActiveIndexSpec;

/// User-facing name-free storage table definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageTableSpec {
    /// Ordered physical column definitions for the table.
    pub columns: Vec<StorageColumnSpec>,
}

impl StorageTableSpec {
    /// Creates a storage table specification from ordered columns.
    #[inline]
    pub fn new(columns: Vec<StorageColumnSpec>) -> Self {
        Self { columns }
    }
}

/// Logical value definition for one physical storage column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageColumnSpec {
    /// Logical value kind stored by the column.
    pub value_kind: ValKind,
    /// Column-level storage flags.
    pub flags: StorageColumnFlags,
}

impl StorageColumnSpec {
    /// Creates one storage column specification.
    #[inline]
    pub const fn new(value_kind: ValKind, flags: StorageColumnFlags) -> Self {
        Self { value_kind, flags }
    }
}

/// Name-free storage index definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageIndexSpec {
    /// Ordered index key columns.
    pub keys: Vec<StorageIndexKey>,
    /// Index-level storage flags.
    pub flags: StorageIndexFlags,
}

impl StorageIndexSpec {
    /// Creates one storage index specification.
    #[inline]
    pub fn new(keys: Vec<StorageIndexKey>, flags: StorageIndexFlags) -> Self {
        Self { keys, flags }
    }

    /// Return whether this index enforces uniqueness.
    #[inline]
    pub fn unique(&self) -> bool {
        self.flags.contains(StorageIndexFlags::PK) || self.flags.contains(StorageIndexFlags::UK)
    }

    /// Return whether this index is the table primary key.
    #[inline]
    pub fn primary_key(&self) -> bool {
        self.flags.contains(StorageIndexFlags::PK)
    }
}

bitflags! {
    /// Column-level flags for storage schema definition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct StorageColumnFlags: u32 {
        /// The column accepts `NULL` values.
        const NULLABLE = 0x01;
    }
}

bitflags! {
    /// Index-level flags for storage schema definition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct StorageIndexFlags: u32 {
        /// The index is a primary key.
        const PK = 0x01;
        /// The index enforces uniqueness.
        const UK = 0x02;
    }
}

/// One indexed column descriptor inside an index definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageIndexKey {
    /// Physical storage-column ordinal included in this key position.
    pub column_ordinal: ColumnOrdinal,
    /// Sort direction for this key column.
    pub order: IndexOrder,
}

impl StorageIndexKey {
    /// Creates an ascending index key on one storage column.
    #[inline]
    pub const fn new(column_ordinal: u16) -> Self {
        StorageIndexKey {
            column_ordinal: ColumnOrdinal::new(column_ordinal),
            order: IndexOrder::Asc,
        }
    }
}

/// One stable-column key in a managed index definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageIndexKeyByColumnId {
    column_id: ColumnID,
    order: IndexOrder,
}

impl StorageIndexKeyByColumnId {
    /// Creates an ascending key on one stable column identity.
    #[inline]
    pub const fn new(column_id: ColumnID) -> Self {
        Self {
            column_id,
            order: IndexOrder::Asc,
        }
    }

    /// Creates a key with an explicit sort direction.
    #[inline]
    pub const fn with_order(column_id: ColumnID, order: IndexOrder) -> Self {
        Self { column_id, order }
    }

    /// Returns the referenced stable column identity.
    #[inline]
    pub const fn column_id(&self) -> ColumnID {
        self.column_id
    }

    /// Returns the key sort direction.
    #[inline]
    pub const fn order(&self) -> IndexOrder {
        self.order
    }
}

/// ID-free ordered physical definition returned for managed CREATE TABLE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableDefinition {
    table: StorageTableSpec,
    indexes: Box<[StorageIndexSpec]>,
}

impl CreateTableDefinition {
    /// Creates an ID-free managed table definition.
    #[inline]
    pub fn new(table: StorageTableSpec, indexes: Vec<StorageIndexSpec>) -> Self {
        Self {
            table,
            indexes: indexes.into_boxed_slice(),
        }
    }

    /// Returns the ordered physical table specification.
    #[inline]
    pub const fn table(&self) -> &StorageTableSpec {
        &self.table
    }

    /// Returns initial indexes in identity-assignment order.
    #[inline]
    pub const fn indexes(&self) -> &[StorageIndexSpec] {
        &self.indexes
    }

    /// Consumes this definition into its ordinal-keyed storage parts.
    #[inline]
    pub fn into_parts(self) -> (StorageTableSpec, Box<[StorageIndexSpec]>) {
        (self.table, self.indexes)
    }
}

/// One stable-ID column in the current managed storage schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageColumnDefinition {
    column_id: ColumnID,
    storage: StorageColumnSpec,
}

impl StorageColumnDefinition {
    /// Creates one stable-ID column definition.
    #[inline]
    pub const fn new(column_id: ColumnID, storage: StorageColumnSpec) -> Self {
        Self { column_id, storage }
    }

    /// Returns the stable column identity.
    #[inline]
    pub const fn column_id(&self) -> ColumnID {
        self.column_id
    }

    /// Returns the physical storage attributes.
    #[inline]
    pub const fn storage(&self) -> &StorageColumnSpec {
        &self.storage
    }
}

/// One active stable-ID index in the current managed storage schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageIndexDefinition {
    index_id: IndexID,
    keys: Box<[StorageIndexKeyByColumnId]>,
    flags: StorageIndexFlags,
}

impl StorageIndexDefinition {
    /// Creates one stable-ID index definition.
    #[inline]
    pub fn new(
        index_id: IndexID,
        keys: Vec<StorageIndexKeyByColumnId>,
        flags: StorageIndexFlags,
    ) -> Self {
        Self {
            index_id,
            keys: keys.into_boxed_slice(),
            flags,
        }
    }

    /// Returns the stable index identity.
    #[inline]
    pub const fn index_id(&self) -> IndexID {
        self.index_id
    }

    /// Returns ordered stable-column keys.
    #[inline]
    pub const fn keys(&self) -> &[StorageIndexKeyByColumnId] {
        &self.keys
    }

    /// Returns the index flags.
    #[inline]
    pub const fn flags(&self) -> StorageIndexFlags {
        self.flags
    }
}

/// Current stable-ID storage schema supplied to managed existing-table callbacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageTableDefinition {
    columns: Box<[StorageColumnDefinition]>,
    indexes: Box<[StorageIndexDefinition]>,
}

impl StorageTableDefinition {
    /// Creates an owned stable-ID storage schema.
    #[inline]
    pub fn new(
        columns: Vec<StorageColumnDefinition>,
        indexes: Vec<StorageIndexDefinition>,
    ) -> Self {
        Self {
            columns: columns.into_boxed_slice(),
            indexes: indexes.into_boxed_slice(),
        }
    }

    /// Returns columns in physical-ordinal order.
    #[inline]
    pub const fn columns(&self) -> &[StorageColumnDefinition] {
        &self.columns
    }

    /// Returns active indexes in stable-ID order.
    #[inline]
    pub const fn indexes(&self) -> &[StorageIndexDefinition] {
        &self.indexes
    }

    /// Projects private physical metadata into the public slot-free schema.
    pub(crate) fn from_metadata(metadata: &TableMetadata) -> Self {
        #[cfg(test)]
        tests::record_projection();
        let columns = metadata
            .col
            .columns()
            .iter()
            .map(|column| {
                StorageColumnDefinition::new(
                    column.id,
                    StorageColumnSpec::new(column.value_kind, column.flags),
                )
            })
            .collect();
        let mut indexes = metadata
            .idx
            .active_indexes()
            .map(|(_, index)| {
                StorageIndexDefinition::new(
                    index.index.id(),
                    index
                        .keys
                        .iter()
                        .map(|key| StorageIndexKeyByColumnId::with_order(key.column_id, key.order))
                        .collect(),
                    index.flags,
                )
            })
            .collect::<Vec<_>>();
        indexes.sort_unstable_by_key(StorageIndexDefinition::index_id);
        Self::new(columns, indexes)
    }
}

/// Slot-free physical change returned for managed CREATE INDEX.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexDefinition {
    keys: Box<[StorageIndexKeyByColumnId]>,
    flags: StorageIndexFlags,
}

impl CreateIndexDefinition {
    /// Creates one stable-column CREATE INDEX change.
    #[inline]
    pub fn new(keys: Vec<StorageIndexKeyByColumnId>, flags: StorageIndexFlags) -> Self {
        Self {
            keys: keys.into_boxed_slice(),
            flags,
        }
    }

    /// Returns ordered stable-column keys.
    #[inline]
    pub const fn keys(&self) -> &[StorageIndexKeyByColumnId] {
        &self.keys
    }

    /// Returns the index flags.
    #[inline]
    pub const fn flags(&self) -> StorageIndexFlags {
        self.flags
    }

    /// Compiles stable column identities to current physical ordinals.
    pub(crate) fn compile(
        self,
        schema: &StorageTableDefinition,
    ) -> OperationResult<StorageIndexSpec> {
        let mut seen = FastHashSet::default();
        let mut keys = Vec::with_capacity(self.keys.len());
        for key in self.keys {
            if !seen.insert(key.column_id) {
                return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
                    "managed CREATE INDEX repeats column_id={}",
                    key.column_id
                )));
            }
            let ordinal = schema
                .columns
                .iter()
                .position(|column| column.column_id == key.column_id)
                .ok_or_else(|| {
                    Report::new(OperationError::InvalidMetadata).attach(format!(
                        "managed CREATE INDEX references missing column_id={}",
                        key.column_id
                    ))
                })?;
            let column_ordinal = ColumnOrdinal::try_from(ordinal).map_err(|_| {
                Report::new(OperationError::InvalidMetadata)
                    .attach("managed CREATE INDEX column ordinal exceeds physical domain")
            })?;
            keys.push(StorageIndexKey {
                column_ordinal,
                order: key.order,
            });
        }
        Ok(StorageIndexSpec::new(keys, self.flags))
    }
}

/// Slot-free physical change returned for managed DROP INDEX.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropIndexDefinition {
    index_id: IndexID,
}

impl DropIndexDefinition {
    /// Creates one stable-ID DROP INDEX change.
    #[inline]
    pub const fn new(index_id: IndexID) -> Self {
        Self { index_id }
    }

    /// Returns the stable index identity to drop.
    #[inline]
    pub const fn index_id(&self) -> IndexID {
        self.index_id
    }

    /// Validates that the target is one active non-primary stable identity.
    pub(crate) fn validate(self, schema: &StorageTableDefinition) -> OperationResult<IndexID> {
        let index = schema
            .indexes
            .iter()
            .find(|index| index.index_id == self.index_id)
            .ok_or_else(|| {
                Report::new(OperationError::IndexNotFound).attach(format!(
                    "managed DROP INDEX target is inactive: index_id={}",
                    self.index_id
                ))
            })?;
        if index.flags.contains(StorageIndexFlags::PK) {
            return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
                "managed DROP INDEX cannot remove primary index_id={}",
                self.index_id
            )));
        }
        Ok(self.index_id)
    }
}

/// Sort direction of one column in an index key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexOrder {
    Asc = 0,
    Desc = 1,
}

impl TryFrom<u8> for IndexOrder {
    type Error = ();

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(IndexOrder::Asc),
            1 => Ok(IndexOrder::Desc),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::index_ref::IndexRef;
    use super::{StorageIndexSpec, StorageTableDefinition};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static STORAGE_DEFINITION_PROJECTIONS: AtomicUsize = AtomicUsize::new(0);

    /// One active internal index definition paired with its exact generation.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct ActiveIndexSpec {
        /// Exact table-local index generation and physical slot.
        pub(crate) index: IndexRef,
        /// Logical definition stored in this slot.
        pub(crate) spec: StorageIndexSpec,
    }

    impl ActiveIndexSpec {
        /// Creates one active exact-generation index specification.
        #[inline]
        pub(crate) fn new(index: IndexRef, spec: StorageIndexSpec) -> Self {
            Self { index, spec }
        }
    }

    impl StorageTableDefinition {
        /// Resets the narrow test-only schema-projection counter.
        pub(crate) fn reset_projection_count() {
            STORAGE_DEFINITION_PROJECTIONS.store(0, Ordering::Relaxed);
        }

        /// Returns the narrow test-only schema-projection count.
        pub(crate) fn projection_count() -> usize {
            STORAGE_DEFINITION_PROJECTIONS.load(Ordering::Relaxed)
        }
    }

    pub(super) fn record_projection() {
        STORAGE_DEFINITION_PROJECTIONS.fetch_add(1, Ordering::Relaxed);
    }
}
