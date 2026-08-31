use crate::error::{OperationError, OperationResult};
use crate::id::TableID;
use crate::row::ops::SelectKey;
use crate::sealed::Sealed;
use crate::table::TableRuntimeLayout;
use crate::value::Val;
use error_stack::Report;
use std::fmt;
use std::num::TryFromIntError;

/// Exclusive end of the stable column and index identity domains.
///
/// Every `u32` value is a valid object identity. The wider one-past-end value
/// is therefore required to represent allocator exhaustion.
pub const ID_DOMAIN_END: u64 = 1_u64 << 32;

/// Stable table-local identity of one storage column.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ColumnID(u32);

impl ColumnID {
    /// Creates a stable table-local column identity.
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the primitive stable column identity.
    #[inline]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Returns the primitive stable column identity.
    #[inline]
    pub(crate) const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ColumnID {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<usize> for ColumnID {
    type Error = TryFromIntError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u32::try_from(value).map(Self)
    }
}

/// Physical position of one column in a stored row layout.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ColumnOrdinal(u16);

impl ColumnOrdinal {
    /// Largest representable physical column ordinal.
    pub const MAX: Self = Self(u16::MAX);

    /// Creates a physical column ordinal.
    #[inline]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the primitive physical column ordinal.
    #[inline]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Returns the physical ordinal as an array index.
    #[inline]
    pub(crate) const fn as_usize(self) -> usize {
        self.0 as usize
    }

    /// Returns the primitive physical column ordinal.
    #[inline]
    pub(crate) const fn get(self) -> u16 {
        self.0
    }
}

impl From<u16> for ColumnOrdinal {
    #[inline]
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

impl TryFrom<usize> for ColumnOrdinal {
    type Error = TryFromIntError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u16::try_from(value).map(Self::new)
    }
}

impl fmt::Display for ColumnOrdinal {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<ColumnOrdinal> for usize {
    #[inline]
    fn from(value: ColumnOrdinal) -> Self {
        value.as_usize()
    }
}

/// Stable generation identity of a user-table index.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexID(u32);

impl IndexID {
    /// Creates a stable table-local user-index identity.
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the primitive stable index identity.
    #[inline]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Returns the primitive stable index identity.
    #[inline]
    pub(crate) const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for IndexID {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<usize> for IndexID {
    type Error = TryFromIntError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u32::try_from(value).map(Self)
    }
}

/// Sparse physical metadata/runtime slot of one table index.
///
/// Catalog indexes use this slot as their fixed identity. Mutable user indexes
/// qualify the slot with an [`IndexID`] in [`IndexRef`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct IndexSlot(u16);

impl IndexSlot {
    /// Creates one physical index slot from its raw `u16` representation.
    #[inline]
    pub(crate) const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the primitive physical slot.
    #[inline]
    pub(crate) const fn get(self) -> u16 {
        self.0
    }

    /// Returns the physical slot as a runtime array index.
    #[inline]
    pub(crate) const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl From<u16> for IndexSlot {
    #[inline]
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

impl TryFrom<usize> for IndexSlot {
    type Error = TryFromIntError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u16::try_from(value).map(Self::new)
    }
}

impl fmt::Display for IndexSlot {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<IndexSlot> for u32 {
    #[inline]
    fn from(value: IndexSlot) -> Self {
        u32::from(value.get())
    }
}

/// Runtime reference pairing one table-local index identity with its physical slot.
///
/// User indexes retain their stable generation identity. Catalog indexes use a
/// synthetic identity numerically equal to their fixed slot; catalog-owned
/// constructors and consumers enforce that invariant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct IndexRef {
    id: IndexID,
    slot: IndexSlot,
}

impl IndexRef {
    /// Creates one exact stable-identity and physical-slot pair.
    #[inline]
    pub(crate) const fn new(id: IndexID, slot: IndexSlot) -> Self {
        Self { id, slot }
    }

    /// Returns the stable generation identity.
    #[inline]
    pub(crate) const fn id(self) -> IndexID {
        self.id
    }

    /// Returns the resolved physical slot.
    #[inline]
    pub(crate) const fn slot(self) -> IndexSlot {
        self.slot
    }
}

impl fmt::Display for IndexRef {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexRef(id={}, slot={})", self.id, self.slot.get())
    }
}

/// Sealed public argument accepted by table-index-driven transaction APIs.
///
/// Callers use [`TableIndex`] for normal ID resolution,
/// or [`ResolvedTableIndex`] for direct exact-generation revalidation.
pub trait TableIndexArgument: Sealed + Copy {
    /// Converts this argument into its unified table-index selector.
    fn into_selector(self) -> TableIndexSelector;
}

/// Table-qualified stable identity of one user index.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TableIndex(
    /// Table owning the index.
    pub TableID,
    /// Stable table-local index identity.
    pub IndexID,
);

impl Sealed for TableIndex {}

impl TableIndexArgument for TableIndex {
    #[inline]
    fn into_selector(self) -> TableIndexSelector {
        let TableIndex(table_id, index_id) = self;
        TableIndexSelector {
            table_id,
            selection: TableIndexSelection::ID(index_id),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum TableIndexSelection {
    ID(IndexID),
    Resolved(IndexRef),
}

/// Unified opaque selector produced by table-index argument conversion.
///
/// The selector carries a table-qualified stable index identity and may also
/// retain an exact previously resolved generation for direct revalidation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TableIndexSelector {
    table_id: TableID,
    selection: TableIndexSelection,
}

impl TableIndexSelector {
    /// Returns the table owning the selected index.
    #[inline]
    pub const fn table_id(self) -> TableID {
        self.table_id
    }

    /// Returns the stable identity of the selected index.
    #[inline]
    pub const fn index_id(self) -> IndexID {
        match self.selection {
            TableIndexSelection::ID(index_id) => index_id,
            TableIndexSelection::Resolved(index) => index.id(),
        }
    }

    /// Resolves or directly validates this selector against one admitted layout.
    #[inline]
    pub(crate) fn resolve(
        self,
        layout: &TableRuntimeLayout,
        operation: &'static str,
    ) -> OperationResult<IndexRef> {
        let table_id = self.table_id;
        match self.selection {
            TableIndexSelection::ID(index_id) => {
                layout.resolve_index_id(index_id).ok_or_else(|| {
                    Report::new(OperationError::SchemaChanged).attach(format!(
                        "operation={operation}, table_id={table_id}, index_id={index_id}"
                    ))
                })
            }
            TableIndexSelection::Resolved(index) => {
                if !layout.validate_index_ref(index) {
                    return Err(Report::new(OperationError::SchemaChanged).attach(format!(
                        "operation={operation}, table_id={table_id}, index={index}"
                    )));
                }
                Ok(index)
            }
        }
    }
}

/// Opaque, non-pinning reference to a previously resolved table index.
///
/// The token may be reused across transactions. Every operation revalidates
/// its exact generation against the admitted current table layout.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResolvedTableIndex {
    table_id: TableID,
    index: IndexRef,
}

impl ResolvedTableIndex {
    /// Returns the table owning this resolved index.
    #[inline]
    pub const fn table_id(&self) -> TableID {
        self.table_id
    }

    /// Returns the stable identity of this resolved index.
    #[inline]
    pub const fn index_id(&self) -> IndexID {
        self.index.id()
    }

    /// Creates a token after transaction admission resolved the exact index.
    #[inline]
    pub(crate) const fn from_admitted(table_id: TableID, index: IndexRef) -> Self {
        Self { table_id, index }
    }
}

impl Sealed for ResolvedTableIndex {}

impl TableIndexArgument for ResolvedTableIndex {
    #[inline]
    fn into_selector(self) -> TableIndexSelector {
        TableIndexSelector {
            table_id: self.table_id,
            selection: TableIndexSelection::Resolved(self.index),
        }
    }
}

/// Fixed physical slot of an index on a catalog table.
pub(crate) type CatalogIndexNo = IndexSlot;

/// Owned logical key qualified by an index reference from one identity domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexKey<T> {
    /// Index identity, ordinal, slot, or resolved reference qualifying this key.
    pub(crate) index: T,
    /// Logical key values in index-column order.
    pub(crate) vals: Vec<Val>,
}

impl<T> IndexKey<T> {
    /// Creates a qualified logical index key.
    #[inline]
    pub(crate) fn new(index: T, vals: Vec<Val>) -> Self {
        Self { index, vals }
    }
}

/// Semantic catalog-table name for the shared physical-slot selection key.
pub(crate) type CatalogSelectKey = SelectKey;

/// Transaction-retained index key carrying identity and resolved slot.
pub(crate) type ResolvedIndexKey = IndexKey<IndexRef>;

/// Builds the runtime reference for one fixed catalog index slot.
#[inline]
pub(crate) const fn catalog_index_ref(index_slot: CatalogIndexNo) -> IndexRef {
    IndexRef::new(IndexID::new(index_slot.get() as u32), index_slot)
}

/// Validates a catalog runtime reference and returns its fixed physical slot.
#[inline]
pub(crate) fn catalog_index_slot(index: IndexRef) -> CatalogIndexNo {
    assert_eq!(
        index.id().as_u32(),
        u32::from(index.slot().get()),
        "catalog index reference identity must equal its fixed slot: index={index}"
    );
    index.slot()
}

/// Qualifies a catalog selection key for transaction-retained runtime state.
#[inline]
pub(crate) fn resolve_catalog_key(key: CatalogSelectKey) -> ResolvedIndexKey {
    ResolvedIndexKey::new(catalog_index_ref(key.index_slot), key.vals)
}

/// Builds a catalog key after metadata has established an active fixed ordinal.
#[inline]
pub(crate) fn catalog_key_from_active_ordinal(
    index_ordinal: usize,
    vals: Vec<Val>,
) -> CatalogSelectKey {
    let index_slot = CatalogIndexNo::try_from(index_ordinal).unwrap_or_else(|_| {
        panic!("active catalog index ordinal exceeds u16: index_ordinal={index_ordinal}")
    });
    CatalogSelectKey::new(index_slot, vals)
}

/// Builds a retained user key from an already admitted exact reference.
#[inline]
pub(crate) fn user_key_from_index_ref(index: IndexRef, vals: Vec<Val>) -> ResolvedIndexKey {
    ResolvedIndexKey::new(index, vals)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_reference_checked_boundaries_and_exact_resolution() {
        assert_eq!(IndexID::new(0).as_u32(), 0);
        assert_eq!(IndexID::new(u32::MAX).as_u32(), u32::MAX);
        assert_eq!(IndexID::new(42).to_string(), "42");
        assert_eq!(IndexSlot::try_from(0usize).unwrap().get(), 0);
        assert_eq!(
            IndexSlot::try_from(usize::from(u16::MAX)).unwrap().get(),
            u16::MAX
        );
        assert!(IndexSlot::try_from(usize::from(u16::MAX) + 1).is_err());

        assert_eq!(CatalogIndexNo::try_from(0usize).unwrap().get(), 0);
        assert_eq!(
            CatalogIndexNo::try_from(usize::from(u16::MAX))
                .unwrap()
                .get(),
            u16::MAX
        );
        assert!(CatalogIndexNo::try_from(usize::from(u16::MAX) + 1).is_err());

        let key = user_key_from_index_ref(
            IndexRef::new(IndexID::new(u32::MAX), IndexSlot::new(37)),
            vec![Val::from(11u32)],
        );
        assert_eq!(
            key.index.to_string(),
            format!("IndexRef(id={}, slot=37)", u32::MAX)
        );
        assert_eq!(key.index.id().get(), u32::MAX);
        assert_eq!(key.index.slot().get(), 37);
        assert_eq!(key.vals, vec![Val::from(11u32)]);
    }

    #[test]
    fn test_catalog_index_reference_enforces_equal_identity_and_slot() {
        let index_slot = IndexSlot::new(37);
        let index = catalog_index_ref(index_slot);
        assert_eq!(index.id().as_u32(), 37);
        assert_eq!(catalog_index_slot(index), index_slot);

        let invalid = IndexRef::new(IndexID::new(38), index_slot);
        assert!(std::panic::catch_unwind(|| catalog_index_slot(invalid)).is_err());
    }

    #[test]
    fn test_table_index_arguments_convert_to_table_qualified_selectors() {
        let table_id = TableID::new(11);
        let table_index = TableIndex(table_id, IndexID::new(7));
        let unresolved = table_index.into_selector();
        assert_eq!(unresolved.table_id(), table_id);
        assert_eq!(unresolved.index_id(), IndexID::new(7));
        assert!(matches!(
            unresolved.selection,
            TableIndexSelection::ID(index_id) if index_id == table_index.1
        ));

        let index = IndexRef::new(IndexID::new(7), IndexSlot::new(3));
        let resolved = ResolvedTableIndex::from_admitted(table_id, index);
        let resolved_selector = resolved.into_selector();
        assert_eq!(resolved_selector.table_id(), table_id);
        assert_eq!(resolved_selector.index_id(), IndexID::new(7));
        assert!(matches!(
            resolved_selector.selection,
            TableIndexSelection::Resolved(actual) if actual == index
        ));
    }
}
