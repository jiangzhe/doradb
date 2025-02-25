use crate::buffer::BufferPool;
use crate::catalog::index::IndexSchema;
use crate::index::{BlockIndex, SecondaryIndex};
use crate::row::ops::{SelectKey, UpdateCol};
use crate::value::{Layout, Val, ValKind, ValType};

use std::collections::HashSet;
use std::sync::Arc;

pub struct TableMeta<P: BufferPool> {
    pub schema: Arc<TableSchema>,
    pub blk_idx: Arc<BlockIndex<P>>,
    pub sec_idx: Arc<[SecondaryIndex]>,
}

pub struct TableSchema {
    types: Vec<ValType>,
    // fix length is the total inline length of all columns.
    pub fix_len: usize,
    // index of var-length columns.
    pub var_cols: Vec<usize>,
    // index column id.
    pub indexes: Vec<IndexSchema>,
    // columns that are included in any index.
    pub user_index_cols: HashSet<usize>,
}

impl TableSchema {
    /// Create a new schema.
    /// RowID is not included in input, but will be created
    /// automatically.
    #[inline]
    pub fn new(user_types: Vec<ValType>, indexes: Vec<IndexSchema>) -> Self {
        debug_assert!(!user_types.is_empty());
        debug_assert!(indexes.iter().all(|is| {
            is.keys
                .iter()
                .all(|k| (k.user_col_idx as usize) < user_types.len())
        }));

        let mut types = Vec::with_capacity(user_types.len() + 1);
        types.push(ValType {
            kind: ValKind::U64,
            nullable: false,
        });
        types.extend(user_types);
        let mut fix_len = 0;
        let mut var_cols = vec![];
        for (idx, ty) in types.iter().enumerate() {
            fix_len += ty.kind.layout().inline_len();
            if !ty.kind.layout().is_fixed() {
                var_cols.push(idx);
            }
        }
        let mut user_index_cols = HashSet::new();
        for index in &indexes {
            for key in &index.keys {
                user_index_cols.insert(key.user_col_idx as usize);
            }
        }
        TableSchema {
            types,
            fix_len,
            var_cols,
            indexes,
            user_index_cols,
        }
    }

    /// Returns column count of this schema, including row id.
    #[inline]
    pub fn col_count(&self) -> usize {
        self.types.len()
    }

    /// Returns layouts of all columns, including row id.
    #[inline]
    pub fn types(&self) -> &[ValType] {
        &self.types
    }

    #[inline]
    pub fn user_types(&self) -> &[ValType] {
        &self.types[1..]
    }

    /// Returns whether the type is matched at given column index, row id is excluded.
    #[inline]
    pub fn user_col_type_match(&self, user_col_idx: usize, val: &Val) -> bool {
        self.col_type_match(user_col_idx + 1, val)
    }

    /// Returns whether the type is matched at given column index.
    #[inline]
    pub fn col_type_match(&self, col_idx: usize, val: &Val) -> bool {
        layout_match(val, self.layout(col_idx))
    }

    #[inline]
    pub fn index_layout_match(&self, index_no: usize, vals: &[Val]) -> bool {
        let index = &self.indexes[index_no];
        if index.keys.len() != vals.len() {
            return false;
        }
        index
            .keys
            .iter()
            .map(|k| self.user_layout(k.user_col_idx as usize))
            .zip(vals)
            .all(|(layout, val)| layout_match(val, layout))
    }

    #[inline]
    pub fn user_layout(&self, user_col_idx: usize) -> Layout {
        self.layout(user_col_idx + 1)
    }

    #[inline]
    pub fn layout(&self, col_idx: usize) -> Layout {
        self.types[col_idx].kind.layout()
    }

    #[inline]
    pub fn keys_for_insert(&self, row: &[Val]) -> Vec<SelectKey> {
        self.indexes
            .iter()
            .enumerate()
            .map(|(index_no, is)| {
                let vals: Vec<Val> = is
                    .keys
                    .iter()
                    .map(|k| row[k.user_col_idx as usize].clone())
                    .collect();
                SelectKey { index_no, vals }
            })
            .collect()
    }

    #[inline]
    pub fn index_may_change(&self, update: &[UpdateCol]) -> bool {
        update
            .iter()
            .any(|uc| self.user_index_cols.contains(&uc.idx))
    }

    #[inline]
    pub fn match_key(&self, key: &SelectKey, row: &[Val]) -> bool {
        let keys = &self.indexes[key.index_no].keys;
        debug_assert!(keys.len() == key.vals.len());
        keys.iter()
            .zip(&key.vals)
            .all(|(key, val)| &row[key.user_col_idx as usize] == val)
    }
}

#[inline]
fn layout_match(val: &Val, layout: Layout) -> bool {
    match (val, layout) {
        (Val::Null, _) => true,
        (Val::Byte1(_), Layout::Byte1)
        | (Val::Byte2(_), Layout::Byte2)
        | (Val::Byte4(_), Layout::Byte4)
        | (Val::Byte8(_), Layout::Byte8)
        | (Val::VarByte(_), Layout::VarByte) => true,
        _ => false,
    }
}
