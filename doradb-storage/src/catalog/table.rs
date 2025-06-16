use crate::catalog::ROW_ID_COL_NAME;
use crate::row::ops::{SelectKey, UpdateCol};
use crate::row::{Row, RowRead};
use crate::value::{Layout, Val, ValKind, ValType};
use doradb_catalog::{ColumnAttributes, ColumnSpec, IndexSpec};
use semistr::SemiStr;
use std::collections::HashSet;

#[derive(Clone)]
pub struct TableMetadata {
    pub(crate) col_names: Vec<SemiStr>,
    pub(crate) col_types: Vec<ValType>,
    // fix length is the total inline length of all columns.
    pub fix_len: usize,
    // index of var-length columns.
    pub var_cols: Vec<usize>,
    // index column id.
    pub index_specs: Vec<IndexSpec>,
    // columns that are included in any index.
    pub user_index_cols: HashSet<usize>,
}

impl TableMetadata {
    /// Create a new schema.
    /// RowID is not included in input, but will be created
    /// automatically.
    #[inline]
    pub fn new(column_specs: Vec<ColumnSpec>, index_specs: Vec<IndexSpec>) -> Self {
        debug_assert!(!column_specs.is_empty());
        debug_assert!(index_specs.iter().all(|is| {
            is.index_cols
                .iter()
                .all(|k| (k.col_no as usize) < column_specs.len())
        }));

        let mut col_names = Vec::with_capacity(column_specs.len() + 1);
        col_names.push(SemiStr::new(ROW_ID_COL_NAME));
        col_names.extend(column_specs.iter().map(|c| c.column_name.clone()));
        let mut col_types = Vec::with_capacity(column_specs.len() + 1);
        col_types.push(ValType {
            kind: ValKind::U64,
            nullable: false,
        });
        col_types.extend(column_specs.iter().map(|c| {
            let kind = ValKind::from(c.column_type);
            let nullable = c.column_attributes.contains(ColumnAttributes::NULLABLE);
            ValType { kind, nullable }
        }));
        let mut fix_len = 0;
        let mut var_cols = vec![];
        for (idx, ty) in col_types.iter().enumerate() {
            fix_len += ty.kind.layout().inline_len();
            if !ty.kind.layout().is_fixed() {
                var_cols.push(idx);
            }
        }
        let mut user_index_cols = HashSet::new();
        for index_spec in &index_specs {
            for key in &index_spec.index_cols {
                user_index_cols.insert(key.col_no as usize);
            }
        }
        TableMetadata {
            col_names,
            col_types,
            fix_len,
            var_cols,
            index_specs,
            user_index_cols,
        }
    }

    /// Returns column count of this schema, including row id.
    #[inline]
    pub fn col_count(&self) -> usize {
        self.col_types.len()
    }

    /// Returns layouts of all columns, including row id.
    #[inline]
    pub fn types(&self) -> &[ValType] {
        &self.col_types
    }

    #[inline]
    pub fn user_types(&self) -> &[ValType] {
        &self.col_types[1..]
    }

    #[inline]
    pub fn user_col_type(&self, user_col_idx: usize) -> ValType {
        self.col_types[user_col_idx + 1]
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
        let index = &self.index_specs[index_no];
        if index.index_cols.len() != vals.len() {
            return false;
        }
        index
            .index_cols
            .iter()
            .map(|k| self.user_layout(k.col_no as usize))
            .zip(vals)
            .all(|(layout, val)| layout_match(val, layout))
    }

    #[inline]
    pub fn user_layout(&self, user_col_idx: usize) -> Layout {
        self.layout(user_col_idx + 1)
    }

    #[inline]
    pub fn layout(&self, col_idx: usize) -> Layout {
        self.col_types[col_idx].kind.layout()
    }

    #[inline]
    pub fn keys_for_insert(&self, row: &[Val]) -> Vec<SelectKey> {
        self.index_specs
            .iter()
            .enumerate()
            .map(|(index_no, is)| {
                let vals: Vec<Val> = is
                    .index_cols
                    .iter()
                    .map(|k| row[k.col_no as usize].clone())
                    .collect();
                SelectKey { index_no, vals }
            })
            .collect()
    }

    #[inline]
    pub fn keys_for_delete(&self, row: Row<'_>) -> Vec<SelectKey> {
        self.index_specs
            .iter()
            .enumerate()
            .map(|(index_no, is)| {
                let vals: Vec<Val> = is
                    .index_cols
                    .iter()
                    .map(|k| row.clone_user_val(&self, k.col_no as usize))
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
        let keys = &self.index_specs[key.index_no].index_cols;
        debug_assert!(keys.len() == key.vals.len());
        keys.iter()
            .zip(&key.vals)
            .all(|(key, val)| &row[key.col_no as usize] == val)
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
