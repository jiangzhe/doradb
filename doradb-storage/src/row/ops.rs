use crate::row::{Row, RowID, RowMut};
use crate::value::Val;
use serde::{Deserialize, Serialize};

pub enum Select<'a> {
    Ok(Row<'a>),
    RowDeleted(Row<'a>),
    RowNotFound,
}

impl Select<'_> {
    /// Returns if select succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, Select::Ok(_))
    }
}

pub enum SelectMvcc {
    Ok(Vec<Val>),
    RowNotFound,
    InvalidIndex,
}

impl SelectMvcc {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, SelectMvcc::Ok(_))
    }
}

pub enum InsertRow {
    Ok(RowID),
    NoFreeSpaceOrRowID,
}

impl InsertRow {
    /// Returns if insert succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, InsertRow::Ok(_))
    }
}

pub enum InsertMvcc {
    // PageGuard is required if table has unique index and
    // we may need to linke a deleted version to the new version.
    // In such scenario, we should keep the page for shared mode
    // and acquire row lock when we do the linking.
    Ok(RowID),
    WriteConflict,
    DuplicateKey,
}

impl InsertMvcc {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, InsertMvcc::Ok(_))
    }
}

pub enum MoveInsert {
    Ok,
    None,
    WriteConflict,
    DuplicateKey,
}

pub enum Update {
    // RowID may change if the update is out-of-place.
    Ok(RowID),
    RowNotFound,
    RowDeleted,
    // if space is not enough, we perform a logical deletion+insert to
    // achieve the update sematics. The returned values are user columns
    // of original row.
    NoFreeSpace(Vec<Val>),
}

impl Update {
    /// Returns if update succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, Update::Ok(..))
    }
}

pub enum UpdateMvcc {
    Ok(RowID),
    RowNotFound,
    RowDeleted,
    NoFreeSpace(Vec<Val>, Vec<UpdateCol>), // with user columns of original row returned for out-of-place update
    WriteConflict,
    Retry(Vec<UpdateCol>),
}

impl UpdateMvcc {
    /// Returns if update with undo succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, UpdateMvcc::Ok(_))
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

pub enum Delete {
    Ok,
    RowNotFound,
    RowAlreadyDeleted,
}

pub enum DeleteMvcc {
    Ok,
    RowNotFound,
    RowAlreadyDeleted,
    WriteConflict,
}

impl DeleteMvcc {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, DeleteMvcc::Ok)
    }
}
