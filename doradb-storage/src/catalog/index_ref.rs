use crate::id::TableID;
use crate::row::ops::SelectKey;
use crate::value::Val;
use std::fmt;
use std::num::TryFromIntError;

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

    /// Converts the Phase-2 transitional persisted identity to its equal slot.
    #[inline]
    pub(crate) fn transitional_slot(self) -> Result<IndexSlot, TryFromIntError> {
        u16::try_from(self.0).map(IndexSlot::from)
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
    /// Largest representable physical index slot.
    pub(crate) const MAX: Self = Self(u16::MAX);

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

    /// Returns the following physical slot when it remains representable.
    #[inline]
    pub(crate) const fn checked_next(self) -> Option<Self> {
        if self.0 == Self::MAX.0 {
            None
        } else {
            Some(Self(self.0 + 1))
        }
    }

    /// Builds the Phase-1 stable identity that is numerically equal to this slot.
    #[inline]
    pub(crate) const fn transitional_id(self) -> IndexID {
        IndexID(self.0 as u32)
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

/// Generation-qualified reference to one active user-table index runtime.
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

    /// Qualifies a currently active slot under the Phase-1 non-reuse contract.
    ///
    /// Stable identity and physical slot are deliberately equal in this phase.
    /// Later phases replace this adapter with direct generation-aware resolution.
    #[inline]
    pub(crate) const fn from_active_slot(slot: IndexSlot) -> Self {
        Self {
            id: slot.transitional_id(),
            slot,
        }
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

/// Opaque, non-pinning reference to a previously resolved user index.
///
/// The token may be reused across transactions. Every operation revalidates
/// its exact generation against the admitted current table layout.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResolvedUserIndex {
    table_id: TableID,
    index: IndexRef,
}

impl ResolvedUserIndex {
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

    /// Returns the exact reference for direct admission validation.
    #[inline]
    pub(crate) const fn index_ref(&self) -> IndexRef {
        self.index
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

/// Logical user-index selector carrying only its stable identity.
pub(crate) type UserIndexKey = IndexKey<IndexID>;
/// Operation-local user-index key carrying only its physical execution slot.
pub(crate) type UserIndexSlotKey = IndexKey<IndexSlot>;
/// Transaction-retained user-index key carrying identity and resolved slot.
pub(crate) type ResolvedUserIndexKey = IndexKey<IndexRef>;

/// Qualifies one validated active user-index slot for retained transaction state.
#[inline]
pub(crate) fn resolve_active_user_key(key: UserIndexSlotKey) -> ResolvedUserIndexKey {
    let stable = UserIndexKey::new(key.index.transitional_id(), key.vals);
    let index = IndexRef::from_active_slot(key.index);
    assert_eq!(
        stable.index.get(),
        index.id().get(),
        "resolved user key must retain the stable selector identity"
    );
    assert_eq!(
        index.id().get(),
        u32::from(index.slot().get()),
        "Phase-1 active user index identity must equal its non-reusable slot"
    );
    ResolvedUserIndexKey::new(index, stable.vals)
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

/// Builds a retained user key after layout admission established an active slot.
#[inline]
pub(crate) fn user_key_from_active_slot(
    index_slot: IndexSlot,
    vals: Vec<Val>,
) -> ResolvedUserIndexKey {
    resolve_active_user_key(UserIndexSlotKey::new(index_slot, vals))
}

/// Builds a retained user key from an already admitted exact reference.
#[inline]
pub(crate) fn user_key_from_index_ref(index: IndexRef, vals: Vec<Val>) -> ResolvedUserIndexKey {
    ResolvedUserIndexKey::new(index, vals)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_reference_checked_boundaries_and_transitional_resolution() {
        assert_eq!(IndexID::new(0).as_u32(), 0);
        assert_eq!(IndexID::new(u32::MAX).as_u32(), u32::MAX);
        assert_eq!(IndexID::new(42).to_string(), "42");
        assert!(
            IndexID::new(u32::from(u16::MAX) + 1)
                .transitional_slot()
                .is_err()
        );
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

        let key = user_key_from_active_slot(IndexSlot::new(37), vec![Val::from(11u32)]);
        assert_eq!(key.index.to_string(), "IndexRef(id=37, slot=37)");
        assert_eq!(key.index.id().get(), 37);
        assert_eq!(key.index.slot().get(), 37);
        assert_eq!(key.vals, vec![Val::from(11u32)]);
    }
}
