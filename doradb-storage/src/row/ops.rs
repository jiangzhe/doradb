use crate::error::Result;
use crate::row::{Row, RowID, RowMut};
use crate::serde::{Deser, Ser, Serde};
use crate::value::Val;
use serde::{Deserialize, Serialize};
use std::mem;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectKey {
    pub index_no: usize,
    pub vals: Vec<Val>,
}

impl SelectKey {
    #[inline]
    pub fn new(index_no: usize, vals: Vec<Val>) -> Self {
        SelectKey { index_no, vals }
    }

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
    #[inline]
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, index_no) = input.deser_u32(start_idx)?;
        let (idx, vals) = <Vec<Val>>::deser(input, idx)?;
        Ok((idx, SelectKey::new(index_no as usize, vals)))
    }
}

pub(crate) enum Select<'a> {
    #[expect(dead_code, reason = "reserved Select::Ok")]
    Ok(Row<'a>),
    #[expect(dead_code, reason = "reserved Select::RowDeleted")]
    RowDeleted(Row<'a>),
    NotFound,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SelectMvcc {
    Found(Vec<Val>),
    NotFound,
}

impl SelectMvcc {
    #[inline]
    pub fn is_found(&self) -> bool {
        matches!(self, SelectMvcc::Found(_))
    }

    #[inline]
    pub fn not_found(&self) -> bool {
        matches!(self, SelectMvcc::NotFound)
    }

    #[inline]
    pub fn unwrap_found(self) -> Vec<Val> {
        match self {
            SelectMvcc::Found(vals) => vals,
            SelectMvcc::NotFound => panic!("empty select result"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ScanMvcc {
    Rows(Vec<Vec<Val>>),
}

impl ScanMvcc {
    #[inline]
    pub fn has_rows(&self) -> bool {
        matches!(self, ScanMvcc::Rows(_))
    }

    #[inline]
    pub fn unwrap_rows(self) -> Vec<Vec<Val>> {
        match self {
            ScanMvcc::Rows(vals) => vals,
        }
    }
}

pub(crate) enum ReadRow {
    Ok(Vec<Val>),
    NotFound,
    InvalidIndex,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkForUniqueIndex {
    Linked,
    NotNeeded,
    WriteConflict,
    DuplicateKey,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpdateIndex {
    // sometimes we may get back page guard to update next index.
    Updated,
    WriteConflict,
    DuplicateKey,
}

impl UpdateIndex {
    #[inline]
    pub(crate) fn is_updated(&self) -> bool {
        matches!(self, UpdateIndex::Updated)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertIndex {
    Inserted,
    WriteConflict,
    DuplicateKey,
}

pub(crate) trait UndoVal {
    /// Returns column index.
    fn idx(&self) -> usize;

    /// Returns column value.
    fn val(&self) -> &Val;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCol {
    pub idx: usize,
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

pub struct UndoCol {
    pub idx: usize,
    pub val: Val,
    // If value is var-len field and not inlined,
    // we need to record its original offset in page
    // to support rollback without new allocation.
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

pub(crate) enum UpdateRow<'a> {
    Ok(RowMut<'a>),
    NoFreeSpaceOrFrozen(Vec<(Val, Option<u16>)>),
}

pub(crate) enum Delete {
    Ok,
    NotFound,
    AlreadyDeleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteMvcc {
    Deleted,
    NotFound,
    WriteConflict,
}

impl DeleteMvcc {
    #[inline]
    pub fn is_deleted(&self) -> bool {
        matches!(self, DeleteMvcc::Deleted)
    }

    #[inline]
    pub fn not_found(&self) -> bool {
        matches!(self, DeleteMvcc::NotFound)
    }
}
