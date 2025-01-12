use crate::row::{Row, RowID, RowMut};
use crate::value::Val;
use serde::{Deserialize, Serialize};

pub enum SelectResult<'a> {
    Ok(Row<'a>),
    RowDeleted(Row<'a>),
    RowNotFound,
}

impl SelectResult<'_> {
    /// Returns if select succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, SelectResult::Ok(_))
    }
}

pub enum SelectMvccResult {
    Ok(Vec<Val>),
    RowNotFound,
    InvalidIndex,
}

impl SelectMvccResult {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, SelectMvccResult::Ok(_))
    }
}

pub enum InsertResult {
    Ok(RowID),
    NoFreeSpaceOrRowID,
}

impl InsertResult {
    /// Returns if insert succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, InsertResult::Ok(_))
    }
}

pub enum InsertMvccResult {
    Ok(RowID),
    WriteConflict,
    DuplicateKey,
}

impl InsertMvccResult {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, InsertMvccResult::Ok(_))
    }
}

pub enum MoveInsertResult {
    Ok,
    None,
    WriteConflict,
    DuplicateKey,
    Retry,
}

pub enum UpdateResult {
    // RowID may change if the update is out-of-place.
    Ok(RowID),
    RowNotFound,
    RowDeleted,
    // if space is not enough, we perform a logical deletion+insert to
    // achieve the update sematics. The returned values are user columns
    // of original row.
    NoFreeSpace(Vec<Val>),
}

impl UpdateResult {
    /// Returns if update succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, UpdateResult::Ok(..))
    }
}

pub enum UpdateMvccResult {
    Ok(RowID),
    RowNotFound,
    RowDeleted,
    NoFreeSpace(Vec<Val>, Vec<UpdateCol>), // with user columns of original row returned for out-of-place update
    WriteConflict,
    Retry(Vec<UpdateCol>),
}

impl UpdateMvccResult {
    /// Returns if update with undo succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, UpdateMvccResult::Ok(_))
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCol {
    pub idx: usize,
    pub val: Val,
}

pub enum UpdateRow<'a> {
    Ok(RowMut<'a>),
    NoFreeSpace(Vec<Val>),
}

pub enum DeleteResult {
    Ok,
    RowNotFound,
    RowAlreadyDeleted,
}

pub enum DeleteMvccResult {
    Ok,
    RowNotFound,
    RowAlreadyDeleted,
    WriteConflict,
}

impl DeleteMvccResult {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, DeleteMvccResult::Ok)
    }
}
