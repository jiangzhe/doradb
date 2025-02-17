use crate::row::{Row, RowID, RowMut};
use crate::value::Val;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq)]
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

pub enum Select<'a> {
    Ok(Row<'a>),
    RowDeleted(Row<'a>),
    NotFound,
}

impl Select<'_> {
    /// Returns if select succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, Select::Ok(_))
    }
}

pub trait SelectResult {
    const NOT_FOUND: Self;
}

pub enum SelectUncommitted {
    Ok(Vec<Val>),
    NotFound,
}

impl SelectUncommitted {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, SelectUncommitted::Ok(_))
    }

    #[inline]
    pub fn not_found(&self) -> bool {
        matches!(self, SelectUncommitted::NotFound)
    }

    #[inline]
    pub fn unwrap(self) -> Vec<Val> {
        match self {
            SelectUncommitted::Ok(vals) => vals,
            SelectUncommitted::NotFound => panic!("empty select result"),
        }
    }
}

impl SelectResult for SelectUncommitted {
    const NOT_FOUND: SelectUncommitted = SelectUncommitted::NotFound;
}

pub enum SelectMvcc {
    Ok(Vec<Val>),
    NotFound,
}

impl SelectMvcc {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, SelectMvcc::Ok(_))
    }

    #[inline]
    pub fn not_found(&self) -> bool {
        matches!(self, SelectMvcc::NotFound)
    }

    #[inline]
    pub fn unwrap(self) -> Vec<Val> {
        match self {
            SelectMvcc::Ok(vals) => vals,
            SelectMvcc::NotFound => panic!("empty select result"),
        }
    }
}

impl SelectResult for SelectMvcc {
    const NOT_FOUND: SelectMvcc = SelectMvcc::NotFound;
}

pub enum ReadRow {
    Ok(Vec<Val>),
    NotFound,
    InvalidIndex,
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

    #[inline]
    pub fn unwrap(self) -> RowID {
        match self {
            InsertMvcc::Ok(row_id) => row_id,
            _ => panic!("insert not ok"),
        }
    }
}

pub enum MoveLinkForIndex {
    Ok,
    None,
    WriteConflict,
    DuplicateKey,
}

pub enum Update {
    // RowID may change if the update is out-of-place.
    Ok(RowID),
    NotFound,
    Deleted,
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
    NotFound,
    WriteConflict,
    DuplicateKey,
}

impl UpdateMvcc {
    /// Returns if update with undo succeeds.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, UpdateMvcc::Ok(_))
    }
}

pub enum UpdateIndex {
    // sometimes we may get back page guard to update next index.
    Ok,
    WriteConflict,
    DuplicateKey,
}

pub enum InsertIndex {
    Ok,
    WriteConflict,
    DuplicateKey,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCol {
    pub idx: usize,
    pub val: Val,
}

pub struct UndoCol {
    pub idx: usize,
    pub val: Val,
    // If value is var-len field and not inlined,
    // we need to record its original offset in page
    // to support rollback without new allocation.
    pub var_offset: Option<u16>,
}

pub enum UpdateRow<'a> {
    Ok(RowMut<'a>),
    NoFreeSpace(Vec<(Val, Option<u16>)>),
}

pub enum Delete {
    Ok,
    NotFound,
    AlreadyDeleted,
}

pub enum DeleteMvcc {
    Ok,
    NotFound,
    WriteConflict,
}

impl DeleteMvcc {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, DeleteMvcc::Ok)
    }

    #[inline]
    pub fn not_found(&self) -> bool {
        matches!(self, DeleteMvcc::NotFound)
    }
}
