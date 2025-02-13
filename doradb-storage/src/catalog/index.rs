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
    Asc,
    Desc,
}
