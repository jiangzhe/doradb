use crate::serde::{Deser, DeserResult, MinBytesHint, Ser, Serde, min_bytes_hint};
use crate::value::Val;
use std::mem;
use std::num::TryFromIntError;

/// Stable generation identity of a user-table index.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct IndexID(u32);

impl IndexID {
    /// Returns the primitive stable index identity.
    #[inline]
    pub(crate) const fn get(self) -> u32 {
        self.0
    }
}

/// Sparse physical runtime/root slot of a user-table index.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct IndexSlot(u16);

impl IndexSlot {
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

    /// Builds the Phase-1 stable identity that is numerically equal to this slot.
    #[inline]
    const fn transitional_id(self) -> IndexID {
        IndexID(self.0 as u32)
    }
}

impl TryFrom<usize> for IndexSlot {
    type Error = TryFromIntError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u16::try_from(value).map(Self)
    }
}

/// Generation-qualified reference to one active user-table index runtime.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct IndexRef {
    id: IndexID,
    slot: IndexSlot,
}

impl IndexRef {
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

/// Fixed bootstrap ordinal of an index on a catalog table.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CatalogIndexNo(u16);

impl CatalogIndexNo {
    /// Returns the primitive catalog index ordinal.
    #[inline]
    pub(crate) const fn get(self) -> u16 {
        self.0
    }

    /// Returns the catalog ordinal as a runtime array index.
    #[inline]
    pub(crate) const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl TryFrom<usize> for CatalogIndexNo {
    type Error = TryFromIntError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u16::try_from(value).map(Self)
    }
}

/// Owned logical key qualified by an index reference from one identity domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexKey<R> {
    /// Index identity, ordinal, slot, or resolved reference qualifying this key.
    pub(crate) index: R,
    /// Logical key values in index-column order.
    pub(crate) vals: Vec<Val>,
}

impl<R> IndexKey<R> {
    /// Creates a qualified logical index key.
    #[inline]
    pub(crate) fn new(index: R, vals: Vec<Val>) -> Self {
        Self { index, vals }
    }
}

/// Logical key for one fixed catalog-table index ordinal.
pub(crate) type CatalogSelectKey = IndexKey<CatalogIndexNo>;

impl Ser<'_> for CatalogSelectKey {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u16>() + self.vals.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u16(start_idx, self.index.get());
        self.vals.ser(out, idx)
    }
}

impl Deser for CatalogSelectKey {
    const MIN_BYTES_HINT: MinBytesHint =
        min_bytes_hint(mem::size_of::<u16>() + mem::size_of::<u64>());

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (idx, index_no) = input.deser_u16(start_idx)?;
        let (idx, vals) = Vec::<Val>::deser(input, idx)?;
        Ok((idx, CatalogSelectKey::new(CatalogIndexNo(index_no), vals)))
    }
}

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
pub(crate) fn catalog_key_from_active_ordinal(index_no: usize, vals: Vec<Val>) -> CatalogSelectKey {
    let index = CatalogIndexNo::try_from(index_no).unwrap_or_else(|_| {
        panic!("active catalog index ordinal exceeds u16: index_no={index_no}")
    });
    CatalogSelectKey::new(index, vals)
}

/// Builds a retained user key after layout admission established an active slot.
#[inline]
pub(crate) fn user_key_from_active_slot(index_no: usize, vals: Vec<Val>) -> ResolvedUserIndexKey {
    let slot = IndexSlot::try_from(index_no)
        .unwrap_or_else(|_| panic!("active user index slot exceeds u16: index_no={index_no}"));
    resolve_active_user_key(UserIndexSlotKey::new(slot, vals))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_reference_checked_boundaries_and_transitional_resolution() {
        assert_eq!(IndexSlot::try_from(0).unwrap().get(), 0);
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

        let key = user_key_from_active_slot(37, vec![Val::from(11u32)]);
        assert_eq!(key.index.id().get(), 37);
        assert_eq!(key.index.slot().get(), 37);
        assert_eq!(key.vals, vec![Val::from(11u32)]);
    }
}
