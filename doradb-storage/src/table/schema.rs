use crate::value::{Layout, Val};

pub struct Schema {
    cols: Vec<Layout>,
    // fix length is the total inline length of all columns.
    pub fix_len: usize,
    // index of var-length columns.
    pub var_cols: Vec<usize>,
    // index column id.
    key_idx: usize,
}

impl Schema {
    /// Create a new schema.
    /// RowID is not included in input, but will be created
    /// automatically.
    #[inline]
    pub fn new(user_cols: Vec<Layout>, user_key_idx: usize) -> Self {
        debug_assert!(!user_cols.is_empty());
        debug_assert!(user_key_idx < user_cols.len());
        debug_assert!(user_cols[user_key_idx] == Layout::Byte4);
        let mut cols = Vec::with_capacity(user_cols.len() + 1);
        cols.push(Layout::Byte8);
        cols.extend(user_cols);
        let mut fix_len = 0;
        let mut var_cols = vec![];
        for (idx, layout) in cols.iter().enumerate() {
            fix_len += layout.inline_len();
            if !layout.is_fixed() {
                var_cols.push(idx);
            }
        }
        Schema {
            cols,
            fix_len,
            var_cols,
            key_idx: user_key_idx + 1,
        }
    }

    /// Returns column count of this schema, including row id.
    #[inline]
    pub fn col_count(&self) -> usize {
        self.cols.len()
    }

    /// Returns layouts of all columns, including row id.
    #[inline]
    pub fn cols(&self) -> &[Layout] {
        &self.cols
    }

    /// Returns whether the type is matched at given column index, row id is excluded.
    #[inline]
    pub fn user_col_type_match(&self, user_col_idx: usize, val: &Val) -> bool {
        self.col_type_match(user_col_idx + 1, val)
    }

    /// Returns whether the type is matched at given column index.
    #[inline]
    pub fn col_type_match(&self, col_idx: usize, val: &Val) -> bool {
        match (val, self.layout(col_idx)) {
            (Val::Null, _) => true,
            (Val::Byte1(_), Layout::Byte1)
            | (Val::Byte2(_), Layout::Byte2)
            | (Val::Byte4(_), Layout::Byte4)
            | (Val::Byte8(_), Layout::Byte8)
            | (Val::VarByte(_), Layout::VarByte) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn idx_type_match(&self, val: &Val) -> bool {
        self.col_type_match(self.key_idx, val)
    }

    #[inline]
    pub fn user_key_idx(&self) -> usize {
        self.key_idx - 1
    }

    #[inline]
    pub fn key_idx(&self) -> usize {
        self.key_idx
    }

    #[inline]
    pub fn user_layout(&self, user_col_idx: usize) -> Layout {
        self.cols[user_col_idx + 1]
    }

    #[inline]
    pub fn layout(&self, col_idx: usize) -> Layout {
        self.cols[col_idx]
    }

    #[inline]
    pub fn key_layout(&self) -> Layout {
        self.layout(self.key_idx)
    }
}
