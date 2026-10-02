//! Compact durable deletion membership in physical LWC ordinals.

use crate::error::{DataIntegrityError, DataIntegrityResult, ResourceResult};
use crate::id::RowID;
use crate::index::identity_set::{EncodedIdentitySet, IdentitySetRef};
use crate::lwc::MAX_LWC_ROWS;
use error_stack::Report;

/// Immutable deletion bytes with a physical row count independent of cardinality.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrdinalDeletionSet {
    row_count: u16,
    encoded: Option<EncodedIdentitySet>,
}

impl OrdinalDeletionSet {
    /// Constructs an empty set for a supported, nonempty physical block.
    pub(crate) fn empty(row_count: u16) -> Self {
        assert!(
            row_count != 0 && usize::from(row_count) <= MAX_LWC_ROWS,
            "ordinal deletion universe must fit an LWC block: row_count={row_count}"
        );
        Self {
            row_count,
            encoded: None,
        }
    }

    /// Encodes sorted unique physical ordinals, retaining the whole-bitmap bound.
    pub(crate) fn from_ordinals(row_count: u16, ordinals: &[u16]) -> ResourceResult<Self> {
        let mut result = Self::empty(row_count);
        if !ordinals.is_empty() {
            // The generic integer codec accepts RowIDs; this adaptation stays private.
            let rows: Vec<_> = ordinals.iter().map(|n| RowID::new(u64::from(*n))).collect();
            result.encoded = Some(EncodedIdentitySet::plan(
                RowID::new(0),
                RowID::new(u64::from(row_count)),
                &rows,
                &[],
                deletion_body_bound(usize::from(row_count)),
            )?);
        }
        Ok(result)
    }

    /// Returns the number of physical rows, including deleted rows.
    pub(crate) fn row_count(&self) -> u16 {
        self.row_count
    }

    /// Returns the deletion cardinality.
    pub(crate) fn len(&self) -> usize {
        self.as_ref().len()
    }

    /// Returns whether every physical row remains live.
    pub(crate) fn is_empty(&self) -> bool {
        self.encoded.is_none()
    }

    /// Tests physical ordinal membership without expanding the set.
    #[inline]
    pub(crate) fn contains(&self, ordinal: u16) -> bool {
        self.as_ref().contains(ordinal)
    }

    /// Iterates deleted physical ordinals in increasing order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = u16> + '_ {
        self.as_ref().iter()
    }

    /// Returns the codec tag when a nonempty deletion section is required.
    pub(crate) fn codec(&self) -> Option<u8> {
        self.encoded.as_ref().map(EncodedIdentitySet::codec)
    }

    /// Returns the compact body, excluding the index-owned deletion header.
    pub(crate) fn body(&self) -> &[u8] {
        self.encoded.as_ref().map_or(&[], EncodedIdentitySet::body)
    }

    /// Borrows the invariant established by encoding or disk validation.
    #[inline]
    pub(crate) fn as_ref(&self) -> OrdinalDeletionSetRef<'_> {
        OrdinalDeletionSetRef {
            row_count: self.row_count,
            encoded: self.encoded.as_ref().map(EncodedIdentitySet::as_ref),
        }
    }
}

/// Validated borrowed deletion membership; codec rank is never a physical ordinal.
#[derive(Clone, Copy)]
pub(crate) struct OrdinalDeletionSetRef<'a> {
    row_count: u16,
    encoded: Option<IdentitySetRef<'a>>,
}

impl<'a> OrdinalDeletionSetRef<'a> {
    /// Validates a nonempty persisted section and its redundant cardinality.
    pub(crate) fn validate(
        row_count: u16,
        count: u16,
        codec: u8,
        body: &'a [u8],
    ) -> DataIntegrityResult<Self> {
        if row_count == 0
            || usize::from(row_count) > MAX_LWC_ROWS
            || count == 0
            || count > row_count
            || body.len() > deletion_body_bound(usize::from(row_count))
        {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("invalid ordinal deletion bounds"));
        }
        let encoded = IdentitySetRef::validate(codec, body, u32::from(row_count))?;
        if encoded.row_count() != count {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("ordinal deletion cardinality mismatch"));
        }
        Ok(Self {
            row_count,
            encoded: Some(encoded),
        })
    }

    /// Borrows immutable cache-admitted bytes, or an absent empty section.
    #[inline]
    pub(super) fn from_admitted(row_count: u16, section: Option<(u8, &'a [u8])>) -> Self {
        Self {
            row_count,
            encoded: section.map(|(codec, body)| {
                IdentitySetRef::from_admitted(codec, body, u32::from(row_count))
            }),
        }
    }

    /// Returns the deletion cardinality, independent of the physical count.
    pub(crate) fn len(self) -> usize {
        self.encoded.map_or(0, |set| usize::from(set.row_count()))
    }

    /// Tests physical ordinal membership without allocating or decoding a list.
    pub(crate) fn contains(self, ordinal: u16) -> bool {
        self.encoded
            .is_some_and(|set| set.ordinal_for_delta(u32::from(ordinal)).is_some())
    }

    /// Iterates codec values as physical ordinals, never codec ranks.
    pub(crate) fn iter(self) -> impl Iterator<Item = u16> + 'a {
        self.encoded
            .into_iter()
            .flat_map(IdentitySetRef::iter_deltas)
            .map(|n| n as u16)
    }

    /// Copies admitted compact bytes into shared immutable storage.
    pub(crate) fn to_owned(self) -> OrdinalDeletionSet {
        OrdinalDeletionSet {
            row_count: self.row_count,
            encoded: self.encoded.map(IdentitySetRef::to_owned),
        }
    }
}

/// Maximum body size of a whole ordinal bitmap, including its rank directory.
pub(crate) const fn deletion_body_bound(row_count: usize) -> usize {
    let words = row_count.div_ceil(64);
    4 + 8 * words + 2 * words.div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_set(row_count: u16, ordinals: &[u16]) {
        let set = OrdinalDeletionSet::from_ordinals(row_count, ordinals).unwrap();
        assert_eq!(set.row_count(), row_count);
        assert_eq!(set.len(), ordinals.len());
        assert_eq!(set.is_empty(), ordinals.is_empty());
        assert_eq!(set.iter().collect::<Vec<_>>(), ordinals);
        assert!(set.body().len() <= deletion_body_bound(usize::from(row_count)));
        let clone = set.clone();
        assert_eq!(clone.body().as_ptr(), set.body().as_ptr());
        let view = if let Some(codec) = set.codec() {
            OrdinalDeletionSetRef::validate(row_count, ordinals.len() as u16, codec, set.body())
                .unwrap()
        } else {
            set.as_ref()
        };
        for ordinal in 0..=row_count {
            let expected = ordinals.binary_search(&ordinal).is_ok();
            assert_eq!(
                set.contains(ordinal),
                expected,
                "rows={row_count}, ordinal={ordinal}"
            );
            assert_eq!(
                view.contains(ordinal),
                expected,
                "borrowed rows={row_count}, ordinal={ordinal}"
            );
        }
        assert_eq!(view.iter().collect::<Vec<_>>(), ordinals);
        assert_eq!(view.to_owned(), set);
    }

    /// Purpose: Protect empty, dense, sparse, run, bitmap, and missing ordinal deletion shapes.
    /// Expected: Owned and validated borrowed membership and iteration match independent sorted ordinal lists.
    #[test]
    fn ordinal_deletion_shapes_match_oracle() {
        for count in [1, 2, 63, 64, 65, 255, 256, 257, MAX_LWC_ROWS as u16] {
            for ordinals in [
                vec![],
                (0..count).collect(),
                vec![count - 1],
                (count / 4..count / 2).collect(),
                (0..count).step_by(2).collect(),
                (0..count).filter(|n| n % 97 != 0).collect(),
                (0..count).filter(|n| n % 173 == 0).collect(),
            ] {
                assert_set(count, &ordinals);
            }
        }
        let all = OrdinalDeletionSet::from_ordinals(64, &(0..64).collect::<Vec<_>>()).unwrap();
        assert_eq!(all.codec(), Some(1));
        assert_eq!(all.body(), []);
    }

    /// Purpose: Exercise deterministic codec changes over generated deletion subsets and word boundaries.
    /// Expected: Every subset obeys the whole-bitmap bound and preserves physical ordinals rather than codec ranks.
    #[test]
    fn generated_ordinal_subsets_match_oracle() {
        // Fixed xorshift state and generation order make both universe and subset reproducible.
        let mut state = 0x323abcde91234567u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for sample in 0..100 {
            let count = (next() % MAX_LWC_ROWS as u64 + 1) as u16;
            let ordinals: Vec<_> = (0..count).filter(|_| next() % 100 < sample).collect();
            assert_set(count, &ordinals);
        }
    }

    /// Purpose: Reject corrupt ordinal bodies, redundant counts, and valid but oversized representations.
    /// Expected: Validation returns typed payload errors before exposing searchable deletion views.
    #[test]
    fn rejects_invalid_ordinal_encodings() {
        let oversized: Vec<_> = (0..1100u16).flat_map(u16::to_le_bytes).collect();
        for (rows, count, codec, body) in [
            (8, 1, 0xff, vec![]),
            (8, 7, 1, vec![]),
            (8, 8, 1, vec![0]),
            (8, 0, 3, vec![]),
            (8, 2, 3, vec![1, 0, 1, 0]),
            (8, 2, 3, vec![4, 0, 1, 0]),
            (8, 1, 3, vec![8, 0]),
            (8, 1, 3, vec![1]),
            (8, 1, 4, vec![1, 0, 1, 0]),
            (8, 1, 9, vec![1, 0, 1, 0]),
            (8, 1, 6, vec![1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]),
            (8, 1, 6, vec![1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0]),
            (MAX_LWC_ROWS as u16, 1100, 3, oversized),
            (MAX_LWC_ROWS as u16 + 1, 1, 3, vec![0, 0]),
        ] {
            let err = match OrdinalDeletionSetRef::validate(rows, count, codec, &body) {
                Ok(_) => panic!(
                    "invalid ordinal representation admitted: rows={rows}, count={count}, codec={codec}"
                ),
                Err(err) => err,
            };
            assert_eq!(err.current_context(), &DataIntegrityError::InvalidPayload);
        }
    }
}
