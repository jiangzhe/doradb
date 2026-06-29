use crate::error::Result;
use crate::id::RowID;
use crate::row::{Row, RowMut};
use crate::serde::{Deser, MinBytesHint, Ser, Serde, min_bytes_hint};
use crate::value::Val;
use serde::{Deserialize, Serialize};
use std::mem;

/// Logical lookup key for one table index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectKey {
    /// Index ordinal in table metadata.
    pub index_no: usize,
    /// Serialized key column values in index order.
    pub vals: Vec<Val>,
}

impl SelectKey {
    /// Creates a lookup key from an index ordinal and key values.
    #[inline]
    pub fn new(index_no: usize, vals: Vec<Val>) -> Self {
        SelectKey { index_no, vals }
    }

    /// Creates a lookup key with all key values set to null.
    #[inline]
    pub fn null(index_no: usize, val_count: usize) -> Self {
        SelectKey {
            index_no,
            vals: vec![Val::Null; val_count],
        }
    }
}

impl Ser<'_> for SelectKey {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u32>() + self.vals.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u32(start_idx, self.index_no as u32);
        self.vals.ser(out, idx)
    }
}

impl Deser for SelectKey {
    const MIN_BYTES_HINT: MinBytesHint =
        min_bytes_hint(mem::size_of::<u32>() + mem::size_of::<u64>());

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, index_no) = input.deser_u32(start_idx)?;
        let (idx, vals) = <Vec<Val>>::deser(input, idx)?;
        Ok((idx, SelectKey::new(index_no as usize, vals)))
    }
}

/// Row-page point-select result.
pub(crate) enum Select<'a> {
    #[expect(dead_code, reason = "reserved Select::Ok")]
    Ok(Row<'a>),
    #[expect(dead_code, reason = "reserved Select::RowDeleted")]
    RowDeleted(Row<'a>),
    NotFound,
}

/// MVCC point-select result.
#[derive(Debug, PartialEq, Eq)]
pub enum SelectMvcc {
    Found(Vec<Val>),
    NotFound,
}

impl SelectMvcc {
    /// Returns whether the select found a visible row.
    #[inline]
    pub fn is_found(&self) -> bool {
        matches!(self, SelectMvcc::Found(_))
    }

    /// Returns whether the select did not find a visible row.
    #[inline]
    pub fn not_found(&self) -> bool {
        matches!(self, SelectMvcc::NotFound)
    }

    /// Unwraps the found row values.
    #[inline]
    pub fn unwrap_found(self) -> Vec<Val> {
        match self {
            SelectMvcc::Found(vals) => vals,
            SelectMvcc::NotFound => panic!("empty select result"),
        }
    }
}

/// MVCC scan result.
#[derive(Debug, PartialEq, Eq)]
pub enum ScanMvcc {
    Rows(Vec<Vec<Val>>),
}

impl ScanMvcc {
    /// Returns whether the scan result carries rows.
    #[inline]
    pub fn has_rows(&self) -> bool {
        matches!(self, ScanMvcc::Rows(_))
    }

    /// Unwraps the scanned row values.
    #[inline]
    pub fn unwrap_rows(self) -> Vec<Vec<Val>> {
        match self {
            ScanMvcc::Rows(vals) => vals,
        }
    }
}

/// Physical row-read result.
pub(crate) enum ReadRow {
    Ok(Vec<Val>),
    NotFound,
    InvalidIndex,
}

/// Row-page insert result.
#[cfg_attr(not(test), expect(dead_code, reason = "reserved InsertRow"))]
pub(crate) enum InsertRow {
    Ok(RowID),
    NoFreeSpaceOrRowID,
}

impl InsertRow {
    /// Returns if insert succeeds.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved is_ok"))]
    pub(crate) fn is_ok(&self) -> bool {
        matches!(self, InsertRow::Ok(_))
    }
}

/// Result of linking a unique-index entry to an older row version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkForUniqueIndex {
    Linked,
    NotNeeded,
    WriteConflict,
    DuplicateKey,
}

/// Row-page in-place update result.
pub(crate) enum Update {
    // RowID may change if the update is out-of-place.
    #[expect(dead_code, reason = "reserved Update::Ok")]
    Ok(RowID),
    NotFound,
    Deleted,
    // if space is not enough, we perform a logical deletion+insert to
    // achieve the update sematics. The returned values are user columns
    // of original row.
    #[expect(dead_code, reason = "reserved Update::NoFreeSpace")]
    NoFreeSpace(Vec<Val>),
}

impl Update {
    /// Returns if update succeeds.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved is_ok"))]
    pub(crate) fn is_ok(&self) -> bool {
        matches!(self, Update::Ok(..))
    }
}

/// MVCC update result.
#[derive(Debug, PartialEq, Eq)]
pub enum UpdateMvcc {
    Updated(RowID),
    NotFound,
}

impl UpdateMvcc {
    /// Returns if update with undo succeeds.
    #[inline]
    pub fn is_updated(&self) -> bool {
        matches!(self, UpdateMvcc::Updated(_))
    }
}

/// MVCC unique-key upsert result.
#[derive(Debug, PartialEq, Eq)]
pub enum UpsertMvcc {
    Inserted(RowID),
    Updated(RowID),
}

impl UpsertMvcc {
    /// Returns whether the upsert inserted a new row.
    #[inline]
    pub fn is_inserted(&self) -> bool {
        matches!(self, UpsertMvcc::Inserted(_))
    }

    /// Returns whether the upsert updated an existing row.
    #[inline]
    pub fn is_updated(&self) -> bool {
        matches!(self, UpsertMvcc::Updated(_))
    }
}

/// Secondary-index update result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpdateIndex {
    // sometimes we may get back page guard to update next index.
    Updated,
    WriteConflict,
    DuplicateKey,
}

impl UpdateIndex {
    /// Returns whether the index update succeeded.
    #[inline]
    pub(crate) fn is_updated(&self) -> bool {
        matches!(self, UpdateIndex::Updated)
    }
}

/// Secondary-index insert result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertIndex {
    Inserted,
    WriteConflict,
    DuplicateKey,
}

/// Common access to update values stored in undo records.
pub(crate) trait UndoVal {
    /// Returns column index.
    fn idx(&self) -> usize;

    /// Returns column value.
    fn val(&self) -> &Val;
}

/// Column update value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCol {
    /// Column index to update.
    pub idx: usize,
    /// New column value.
    pub val: Val,
}

impl Ser<'_> for UpdateCol {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u32>() + self.val.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = out.ser_u32(idx, self.idx as u32);
        self.val.ser(out, idx)
    }
}

impl Deser for UpdateCol {
    const MIN_BYTES_HINT: MinBytesHint =
        min_bytes_hint(mem::size_of::<u32>() + mem::size_of::<u8>());

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let idx = start_idx;
        let (i, idx) = input.deser_u32(idx)?;
        let (i, val) = Val::deser(input, i)?;
        Ok((
            i,
            UpdateCol {
                idx: idx as usize,
                val,
            },
        ))
    }
}

impl UndoVal for UpdateCol {
    #[inline]
    fn idx(&self) -> usize {
        self.idx
    }

    #[inline]
    fn val(&self) -> &Val {
        &self.val
    }
}

/// Column value captured for undo processing.
pub struct UndoCol {
    /// Column index to restore.
    pub idx: usize,
    /// Previous column value.
    pub val: Val,
    // If value is var-len field and not inlined,
    // we need to record its original offset in page
    // to support rollback without new allocation.
    /// Previous out-of-line variable-length value offset, when available.
    pub var_offset: Option<u16>,
}

impl UndoVal for UndoCol {
    #[inline]
    fn idx(&self) -> usize {
        self.idx
    }

    #[inline]
    fn val(&self) -> &Val {
        &self.val
    }
}

/// Transactional row-update result.
pub(crate) enum UpdateRow<'a> {
    Ok(RowMut<'a>),
    NoFreeSpaceOrFrozen(Vec<(Val, Option<u16>)>),
}

/// Row-page delete result.
pub(crate) enum Delete {
    Ok,
    NotFound,
    AlreadyDeleted,
}

/// MVCC delete result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteMvcc {
    Deleted,
    NotFound,
    WriteConflict,
}

impl DeleteMvcc {
    /// Returns whether the delete succeeded.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        matches!(self, DeleteMvcc::Deleted)
    }

    /// Returns whether the delete target was not found.
    #[inline]
    pub fn not_found(&self) -> bool {
        matches!(self, DeleteMvcc::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::UpsertMvcc;
    use crate::id::RowID;

    #[test]
    fn upsert_mvcc_predicates_report_variant() {
        let inserted = UpsertMvcc::Inserted(RowID::new(1));
        assert!(inserted.is_inserted());
        assert!(!inserted.is_updated());

        let updated = UpsertMvcc::Updated(RowID::new(2));
        assert!(updated.is_updated());
        assert!(!updated.is_inserted());
    }
}
