use crate::catalog::IndexSlot;
use crate::id::RowID;
use crate::row::{Row, RowMut};
use crate::serde::{Deser, DeserResult, MinBytesHint, Ser, Serde, min_bytes_hint};
use crate::trx::undo::HotForwardSource;
use crate::value::Val;
use std::mem;

/// Logical lookup key for one table index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SelectKey {
    /// Physical table-local index slot.
    pub(crate) index_slot: IndexSlot,
    /// Serialized key column values in index order.
    pub(crate) vals: Vec<Val>,
}

impl SelectKey {
    /// Creates a lookup key from a physical slot and key values.
    #[inline]
    pub(crate) fn new(index_slot: IndexSlot, vals: Vec<Val>) -> Self {
        SelectKey { index_slot, vals }
    }

    /// Creates a lookup key with all key values set to null.
    #[inline]
    pub(crate) fn null(index_slot: IndexSlot, val_count: usize) -> Self {
        SelectKey {
            index_slot,
            vals: vec![Val::Null; val_count],
        }
    }
}

impl Ser<'_> for SelectKey {
    #[inline]
    fn ser_len(&self) -> usize {
        mem::size_of::<u16>() + self.vals.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = out.ser_u16(start_idx, self.index_slot.get());
        self.vals.ser(out, idx)
    }
}

impl Deser for SelectKey {
    const MIN_BYTES_HINT: MinBytesHint =
        min_bytes_hint(mem::size_of::<u16>() + mem::size_of::<u64>());

    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
        let (idx, index_slot) = input.deser_u16(start_idx)?;
        let (idx, vals) = <Vec<Val>>::deser(input, idx)?;
        Ok((idx, SelectKey::new(IndexSlot::from(index_slot), vals)))
    }
}

/// Row-page point-select result.
pub(crate) enum Select<'a> {
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved Select::Ok"))]
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

/// Decision returned by a programmable MVCC table-scan callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanRowDecision {
    /// Materialize and return the current row projection.
    Include,
    /// Ignore the current row and continue scanning.
    Skip,
    /// End the scan successfully without returning the current row.
    Stop,
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
pub(crate) enum LinkForUniqueIndex {
    Linked(Option<HotForwardSource>),
    NotNeeded,
}

impl LinkForUniqueIndex {
    /// Returns the source to pin before index exchange and find again under its row latch.
    #[inline]
    pub(crate) fn source(&self) -> Option<&HotForwardSource> {
        match self {
            Self::Linked(source) => source.as_ref(),
            Self::NotNeeded => None,
        }
    }
}

/// Row-page in-place update result.
pub(crate) enum Update {
    // RowID may change if the update is out-of-place.
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved Update::Ok"))]
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

/// Decision made after a unique-point lookup acquires its latest writable row.
///
/// `Insert` is valid only for a missing entry; `Update` and `Delete` require an
/// occupied entry. `Skip` is valid in either state. Payloads remain caller-owned
/// until the decision is returned and are consumed by the operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UniqueMutation {
    /// Leave the entry unchanged and release this invocation's provisional lock.
    Skip,
    /// Insert a missing row whose selected unique key matches the lookup key.
    Insert(Vec<Val>),
    /// Apply ordered sparse assignments to the occupied row.
    /// An empty update releases provisional ownership without physical effects.
    Update(Vec<UpdateCol>),
    /// Delete the occupied row.
    Delete,
}

/// Logical result of a unique-point callback mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniqueMutationOutcome {
    /// The callback selected `Skip`, whether the entry existed or was missing.
    Noop,
    /// A missing-entry insertion created this physical row.
    Inserted(RowID),
    /// An occupied-entry update finished at this physical row, including moves.
    Updated(RowID),
    /// The occupied row was deleted.
    Deleted,
}

/// Common access to update values stored in undo records.
pub(crate) trait UndoVal {
    /// Returns column index.
    fn idx(&self) -> usize;

    /// Returns column value.
    fn val(&self) -> &Val;
}

/// Column update value.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> DeserResult<(usize, Self)> {
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

/// Callback decision for one latest modifiable row in a table mutation.
///
/// The callback is invoked at most once for each eligible original row. Update
/// replacements created by the operation are not offered to the callback again.
/// An index-driven update that changes its unique driver's encoded key may be
/// cached and physically applied after candidate traversal, so callbacks must
/// not depend on candidate-order physical effects from other rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowMutation {
    /// Leave the row unchanged.
    Skip,
    /// Delete the row and its visible secondary-index entries.
    Delete,
    /// Apply the supplied sparse column update to the row.
    ///
    /// An empty update is counted but creates no row, index, undo, or redo work.
    Update(Vec<UpdateCol>),
}

/// Counts of successful delete and update decisions from a table mutation.
///
/// Skipped rows increment neither field. A failed operation returns no outcome
/// and rolls back its statement-local row and index effects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableMutationOutcome {
    /// Number of callback-selected delete actions.
    pub delete_count: usize,
    /// Number of callback-selected update actions.
    pub update_count: usize,
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
    NoFreeSpaceOrFrozen(Vec<Val>),
}

/// Row-page delete result.
pub(crate) enum Delete {
    Ok,
    NotFound,
    AlreadyDeleted,
}
