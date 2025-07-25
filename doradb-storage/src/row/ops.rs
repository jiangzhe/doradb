use crate::error::Result;
use crate::row::{Row, RowID, RowMut};
use crate::serde::{Deser, Ser, SerdeCtx};
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
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        mem::size_of::<u32>() + self.vals.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let idx = ctx.ser_u32(out, start_idx, self.index_no as u32);
        self.vals.ser(ctx, out, idx)
    }
}

impl Deser for SelectKey {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, index_no) = ctx.deser_u32(input, start_idx)?;
        let (idx, vals) = <Vec<Val>>::deser(ctx, input, idx)?;
        Ok((idx, SelectKey::new(index_no as usize, vals)))
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

#[derive(Debug)]
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

pub type ScanMvcc = Vec<Vec<Val>>;

pub enum ReadRow {
    Ok(Vec<Val>),
    NotFound,
    InvalidIndex,
}

pub enum ReadKey {
    Ok(SelectKey),
    Deleted,
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

pub enum LinkForUniqueIndex {
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

#[derive(Debug)]
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

impl UpdateIndex {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, UpdateIndex::Ok)
    }
}

pub enum InsertIndex {
    Ok,
    WriteConflict,
    DuplicateKey,
}

pub trait UndoVal {
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
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        mem::size_of::<u32>() + self.val.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = ctx.ser_u32(out, idx, self.idx as u32);
        self.val.ser(ctx, out, idx)
    }
}

impl Deser for UpdateCol {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let idx = start_idx;
        let (i, idx) = ctx.deser_u32(input, idx)?;
        let (i, val) = Val::deser(ctx, input, i)?;
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

pub enum UpdateRow<'a> {
    Ok(RowMut<'a>),
    NoFreeSpace(Vec<(Val, Option<u16>)>),
}

pub enum Delete {
    Ok,
    NotFound,
    AlreadyDeleted,
}

#[derive(Debug)]
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

pub enum Recover {
    Ok,
    NoSpace,
    NotFound,
    AlreadyDeleted,
}

impl Recover {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, Recover::Ok)
    }
}

pub enum RecoverIndex {
    Ok,
    InsertOutdated,
    DeleteOutdated,
}
