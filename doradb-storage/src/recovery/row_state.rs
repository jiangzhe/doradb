use crate::bitmap::{Bitmap, new_bitmap};
use crate::id::PageID;

/// Inserted-slot history exclusively moved between page history, an active page,
/// and its outstanding replay job/result. Retirement retains the bitmap.
/// The coordinator removes this state before page reuse, so the page ID remains
/// bound to its allocation throughout replay and index reconstruction.
pub(crate) struct RowReplayState {
    page_id: PageID,
    inserted: Box<[u64]>,
}

impl RowReplayState {
    /// Captures an allocated page's identity and reserved row capacity.
    #[inline]
    pub(crate) fn new(page_id: PageID, max_row_count: usize) -> Self {
        Self {
            page_id,
            inserted: new_bitmap(max_row_count),
        }
    }

    /// Returns the row page ID.
    #[inline]
    pub(crate) fn page_id(&self) -> PageID {
        self.page_id
    }

    /// Returns whether this slot has ever been inserted during replay.
    /// The latched page's RowID range must establish `row_idx < max_row_count`.
    #[inline]
    pub(crate) fn is_inserted(&self, row_idx: usize) -> bool {
        self.inserted.bitmap_get(row_idx)
    }

    /// Records a successful insert, returning whether the bit was newly set.
    /// The latched page's RowID range must establish `row_idx < max_row_count`.
    #[inline]
    pub(crate) fn record_insert(&mut self, row_idx: usize) -> bool {
        self.inserted.bitmap_set(row_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::RowReplayState;
    use crate::bitmap::bitmap_required_units;
    use crate::id::PageID;

    /// Purpose: Track inserted row slots across bitmap word boundaries.
    /// Expected: Only recorded slots are marked, repeated inserts are distinguished, and page identity is retained.
    #[test]
    fn test_replay_bitmap_tracks_sparse_slots() {
        let id = PageID::new(7);
        let mut state = RowReplayState::new(id, 70);
        assert_eq!(state.page_id(), id);
        assert_eq!(state.inserted.len(), bitmap_required_units(70));
        for idx in [69, 64, 63, 0] {
            assert!(!state.is_inserted(idx));
            assert!(state.record_insert(idx));
            assert!(!state.record_insert(idx));
        }
        for idx in 0..70 {
            assert_eq!(state.is_inserted(idx), [0, 63, 64, 69].contains(&idx));
        }
    }
}
