use crate::error::Result;
use crate::serde::{Deser, Ser, SerdeCtx};
pub use doradb_catalog::IndexID;
use std::mem;

pub struct IndexSchema {
    pub keys: Vec<IndexKey>,
    pub unique: bool,
}

impl IndexSchema {
    #[inline]
    pub fn new(keys: Vec<IndexKey>, unique: bool) -> Self {
        debug_assert!(!keys.is_empty());
        IndexSchema { keys, unique }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexKey {
    pub user_col_idx: u16,
    pub order: IndexOrder,
}

impl Ser<'_> for IndexKey {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u16>() + mem::size_of::<u8>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = ctx.ser_u16(out, idx, self.user_col_idx);
        ctx.ser_u8(out, idx, self.order as u8)
    }
}

impl Deser for IndexKey {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, input: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, user_col_idx) = ctx.deser_u16(input, start_idx)?;
        let (idx, order) = ctx.deser_u8(input, idx)?;
        Ok((
            idx,
            IndexKey {
                user_col_idx,
                order: IndexOrder::from(order),
            },
        ))
    }
}

impl IndexKey {
    #[inline]
    pub fn new(user_col_idx: u16) -> Self {
        IndexKey {
            user_col_idx,
            order: IndexOrder::Asc,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexOrder {
    Asc = 0,
    Desc = 1,
}

impl From<u8> for IndexOrder {
    #[inline]
    fn from(value: u8) -> Self {
        unsafe { mem::transmute(value) }
    }
}
