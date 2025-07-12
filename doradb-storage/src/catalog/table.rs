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
    pub index_cols: HashSet<usize>,
}

impl TableMetadata {
    /// Create metadata of a new table.
    /// RowID is not included.
    #[inline]
    pub fn new(column_specs: Vec<ColumnSpec>, index_specs: Vec<IndexSpec>) -> Self {
        debug_assert!(!column_specs.is_empty());
        debug_assert!(index_specs.iter().all(|is| {
            is.index_cols
                .iter()
                .all(|k| (k.col_no as usize) < column_specs.len())
        }));

        let col_names: Vec<_> = column_specs.iter().map(|c| c.column_name.clone()).collect();
        let col_types: Vec<_> = column_specs
            .iter()
            .map(|c| {
                let kind = ValKind::from(c.column_type);
                let nullable = c.column_attributes.contains(ColumnAttributes::NULLABLE);
                ValType { kind, nullable }
            })
            .collect();

        let mut fix_len = 0;
        let mut var_cols = vec![];
        for (idx, ty) in col_types.iter().enumerate() {
            fix_len += ty.kind.layout().inline_len();
            if !ty.kind.layout().is_fixed() {
                var_cols.push(idx);
            }
        }
        let mut index_cols = HashSet::new();
        for index_spec in &index_specs {
            for key in &index_spec.index_cols {
                index_cols.insert(key.col_no as usize);
            }
        }
        TableMetadata {
            col_names,
            col_types,
            fix_len,
            var_cols,
            index_specs,
            index_cols,
        }
    }

    /// Returns column count of this schema, including row id.
    #[inline]
    pub fn col_count(&self) -> usize {
        self.col_types.len()
    }

    /// Returns layouts of all columns, including row id.
    #[inline]
    pub fn col_types(&self) -> &[ValType] {
        &self.col_types
    }

    /// Returns column type of given position.
    #[inline]
    pub fn col_type(&self, col_idx: usize) -> ValType {
        self.col_types[col_idx]
    }

    /// Returns whether the type is matched at given column index.
    #[inline]
    pub fn col_type_match(&self, col_idx: usize, val: &Val) -> bool {
        layout_match(val, self.col_layout(col_idx))
    }

    /// Returns whether input values matches given index.
    #[inline]
    pub fn index_layout_match(&self, index_no: usize, vals: &[Val]) -> bool {
        let index = &self.index_specs[index_no];
        if index.index_cols.len() != vals.len() {
            return false;
        }
        index
            .index_cols
            .iter()
            .map(|k| self.col_layout(k.col_no as usize))
            .zip(vals)
            .all(|(layout, val)| layout_match(val, layout))
    }

    /// Returns layout of column.
    #[inline]
    pub fn col_layout(&self, col_idx: usize) -> Layout {
        self.col_types[col_idx].kind.layout()
    }

    /// Returns index keys of a new row.
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

    /// Returns index keys of deletion of a row.
    #[inline]
    pub fn keys_for_delete(&self, row: Row<'_>) -> Vec<SelectKey> {
        self.index_specs
            .iter()
            .enumerate()
            .map(|(index_no, is)| {
                let vals: Vec<Val> = is
                    .index_cols
                    .iter()
                    .map(|k| row.clone_val(self, k.col_no as usize))
                    .collect();
                SelectKey { index_no, vals }
            })
            .collect()
    }

    /// Returns whether index may change according to given update columns.
    #[inline]
    pub fn index_may_change(&self, update: &[UpdateCol]) -> bool {
        update.iter().any(|uc| self.index_cols.contains(&uc.idx))
    }

    /// Returns whether key matches given row.
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
    matches!(
        (val, layout),
        (Val::Null, _)
            | (Val::Byte1(_), Layout::Byte1)
            | (Val::Byte2(_), Layout::Byte2)
            | (Val::Byte4(_), Layout::Byte4)
            | (Val::Byte8(_), Layout::Byte8)
            | (Val::VarByte(_), Layout::VarByte)
    )
}
