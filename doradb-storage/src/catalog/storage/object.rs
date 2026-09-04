use crate::catalog::{
    BindingNamespaceID, ColumnID, ColumnOrdinal, IndexRef, StorageColumnFlags, StorageIndexFlags,
    TableIndexKeySpec,
};
use crate::id::{TableID, TrxID};
use crate::value::ValKind;

/// One row object in `catalog.tables`.
#[derive(Debug)]
pub(crate) struct TableObject {
    /// User table identifier.
    pub(crate) table_id: TableID,
    /// Monotonic active storage-schema epoch.
    pub(crate) storage_epoch: u64,
    /// Exclusive stable column-ID allocator bound.
    pub(crate) next_column_id: u64,
    /// Exclusive stable index-ID allocator bound.
    pub(crate) next_index_id: u64,
    /// Exclusive physical index-slot count.
    pub(crate) index_slot_count: u32,
}

/// One row object in `catalog.columns`.
#[derive(Debug)]
pub(crate) struct ColumnObject {
    /// User table identifier.
    pub(crate) table_id: TableID,
    /// Stable column identity.
    pub(crate) column_id: ColumnID,
    /// Physical column ordinal in stored rows.
    pub(crate) storage_ordinal: ColumnOrdinal,
    /// Stored value kind.
    pub(crate) value_kind: ValKind,
    /// Column attribute bitset.
    pub(crate) value_flags: StorageColumnFlags,
}

/// One row object in `catalog.indexes`.
#[derive(Debug)]
pub(crate) struct IndexObject {
    /// User table identifier.
    pub(crate) table_id: TableID,
    /// Exact stable generation and physical slot.
    pub(crate) index: IndexRef,
    /// Index attribute bitset.
    pub(crate) index_flags: StorageIndexFlags,
    /// Ordered stable-column key specification.
    pub(crate) keys: Box<[TableIndexKeySpec]>,
}

/// One opaque managed-definition envelope in `catalog.table_descriptors`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableDescriptorObject {
    /// User table described by this envelope.
    pub(crate) table_id: TableID,
    /// Monotonic replacement revision owned by the storage engine.
    pub(crate) descriptor_revision: u64,
    /// Storage epoch against which the opaque payload was compiled.
    pub(crate) compiled_storage_epoch: u64,
    /// Canonical fingerprint of the separately persisted numeric schema.
    pub(crate) storage_schema_fingerprint: [u8; 32],
    /// Exact opaque higher-layer descriptor bytes.
    pub(crate) payload: Box<[u8]>,
}

/// One finalized roleless row in `catalog.table_bindings`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableBindingObject {
    /// Opaque higher-layer namespace identity.
    pub(crate) namespace_id: BindingNamespaceID,
    /// Exact namespace-local opaque lookup key.
    pub(crate) binding_key: Box<[u8]>,
    /// Storage-assigned managed user table identity.
    pub(crate) table_id: TableID,
}

/// One row object in `catalog.table_replay_silent_watermarks`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SilentWatermarkObject {
    /// User table identifier.
    pub(crate) table_id: TableID,
    /// Silent heap replay lower bound.
    pub(crate) heap_redo_start_ts: TrxID,
    /// Silent deletion replay lower bound.
    pub(crate) deletion_cutoff_ts: TrxID,
}
