use crate::id::TrxID;

/// Per-row recovery map used while rebuilding one row page from redo.
pub(crate) struct RowRecoveryMap {
    create_cts: TrxID,
    entries: Vec<Option<TrxID>>,
}

impl RowRecoveryMap {
    /// Returns a recovery map with given create CTS.
    #[inline]
    pub(crate) fn new(create_cts: TrxID) -> Self {
        RowRecoveryMap {
            create_cts,
            entries: vec![],
        }
    }

    /// Returns CTS when this page is created.
    #[inline]
    pub(crate) fn create_cts(&self) -> TrxID {
        self.create_cts
    }

    /// Returns whether entry of given row position is vacant.
    #[inline]
    pub(crate) fn is_vacant(&self, row_idx: usize) -> bool {
        row_idx >= self.entries.len() || self.entries[row_idx].is_none()
    }

    /// Insert CTS at given row position.
    #[inline]
    pub(crate) fn insert_at(&mut self, row_idx: usize, cts: TrxID) {
        while self.entries.len() <= row_idx {
            self.entries.push(None);
        }
        self.entries[row_idx] = Some(cts);
    }

    /// Update CTS at given row position.
    #[inline]
    pub(crate) fn update_at(&mut self, row_idx: usize, cts: TrxID) {
        debug_assert!(row_idx < self.entries.len());
        debug_assert!(self.at(row_idx).unwrap() <= cts);
        self.entries[row_idx].replace(cts);
    }

    /// Returns CTS at given row position.
    #[inline]
    pub(crate) fn at(&self, row_idx: usize) -> Option<TrxID> {
        self.entries.get(row_idx).and_then(|v| *v)
    }
}

#[cfg(test)]
mod tests {
    use super::RowRecoveryMap;
    use crate::id::TrxID;

    #[test]
    fn row_recovery_map_tracks_create_vacancy_insert_and_update_cts() {
        let mut map = RowRecoveryMap::new(TrxID::new(7));

        assert_eq!(map.create_cts(), TrxID::new(7));
        assert!(map.is_vacant(0));
        assert_eq!(map.at(0), None);

        map.insert_at(2, TrxID::new(11));
        assert!(map.is_vacant(0));
        assert!(!map.is_vacant(2));
        assert_eq!(map.at(2), Some(TrxID::new(11)));

        map.update_at(2, TrxID::new(13));
        assert_eq!(map.at(2), Some(TrxID::new(13)));
    }
}
