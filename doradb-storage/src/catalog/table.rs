use crate::catalog::spec::{ColumnAttributes, ColumnSpec, IndexSpec};
use crate::error::Result;
use crate::row::ops::{SelectKey, UpdateCol};
use crate::row::{Row, RowRead};
use crate::serde::{Deser, Ser, Serde};
use crate::value::{Val, ValKind, ValType};
use semistr::SemiStr;
use std::collections::HashSet;

/// Table metadata including column names, column types,
/// index specifications.
/// Constraints and other advanced configurations are
/// not implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    pub(crate) col_names: Vec<SemiStr>,
    pub(crate) col_types: Vec<ValType>,
    // fix length is the total inline length of all columns.
    pub fix_len: usize,
    // index of var-length columns.
    pub var_cols: Vec<usize>,
    // number of nullable columns.
    pub nullable_cols: usize,
    // scan sums of null bitmap, it can locate null bitmap
    // in row page.
    null_scan_sums: Vec<usize>,
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
                let nullable = c.column_attributes.contains(ColumnAttributes::NULLABLE);
                ValType {
                    kind: c.column_type,
                    nullable,
                }
            })
            .collect();
        TableMetadata::create(col_names, col_types, index_specs)
    }

    #[inline]
    fn create(
        col_names: Vec<SemiStr>,
        col_types: Vec<ValType>,
        index_specs: Vec<IndexSpec>,
    ) -> Self {
        let mut fix_len = 0;
        let mut var_cols = vec![];
        for (idx, ty) in col_types.iter().enumerate() {
            fix_len += ty.kind.inline_len();
            if !ty.kind.is_fixed() {
                var_cols.push(idx);
            }
        }
        let mut index_cols = HashSet::new();
        for index_spec in &index_specs {
            for key in &index_spec.index_cols {
                index_cols.insert(key.col_no as usize);
            }
        }
        // calculate column null bitmap offsets.
        let mut nullable_cols = 0usize;
        let mut null_scan_sums = vec![];
        for ty in &col_types {
            null_scan_sums.push(nullable_cols);
            nullable_cols += if ty.nullable { 1 } else { 0 };
        }
        TableMetadata {
            col_names,
            col_types,
            fix_len,
            var_cols,
            nullable_cols,
            null_scan_sums,
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

    /// Returns value kind of given column.
    #[inline]
    pub fn val_kind(&self, col_idx: usize) -> ValKind {
        self.col_type(col_idx).kind
    }

    /// Returns whether the given column is nullable.
    #[inline]
    pub fn nullable(&self, col_idx: usize) -> bool {
        self.col_type(col_idx).nullable
    }

    /// Returns whether the type is matched at given column index.
    #[inline]
    pub fn col_type_match(&self, col_idx: usize, val: &Val) -> bool {
        val.matches_kind(self.col_type(col_idx).kind)
    }

    /// Returns current column offset, compared to all
    /// nullable columns.
    #[inline]
    pub fn null_offset(&self, col_idx: usize) -> usize {
        self.null_scan_sums[col_idx]
    }

    /// Returns whether input values matches given index.
    #[inline]
    pub fn index_type_match(&self, index_no: usize, vals: &[Val]) -> bool {
        let index = &self.index_specs[index_no];
        if index.index_cols.len() != vals.len() {
            return false;
        }
        index
            .index_cols
            .iter()
            .map(|k| self.col_type(k.col_no as usize).kind)
            .zip(vals)
            .all(|(kind, val)| val.matches_kind(kind))
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
                    .map(|k| row.val(self, k.col_no as usize))
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

    /// Create a view for serialization.
    #[inline]
    pub fn ser_view(&self) -> TableBriefMetadataSerView<'_> {
        TableBriefMetadataSerView {
            col_names: &self.col_names,
            col_types: &self.col_types,
            index_specs: &self.index_specs,
        }
    }
}

impl From<TableBriefMetadata> for TableMetadata {
    #[inline]
    fn from(value: TableBriefMetadata) -> Self {
        TableMetadata::create(value.col_names, value.col_types, value.index_specs)
    }
}

/// View of necessary information to recover table
/// metadata.
/// It's used for serialization.
pub struct TableBriefMetadataSerView<'a> {
    pub col_names: &'a [SemiStr],
    pub col_types: &'a [ValType],
    pub index_specs: &'a [IndexSpec],
}

impl<'a> Ser<'a> for TableBriefMetadataSerView<'a> {
    #[inline]
    fn ser_len(&self) -> usize {
        self.col_names.ser_len() + self.col_types.ser_len() + self.index_specs.ser_len()
    }

    #[inline]
    fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
        let idx = self.col_names.ser(out, start_idx);
        let idx = self.col_types.ser(out, idx);
        self.index_specs.ser(out, idx)
    }
}

/// Brief metadata of a table.
/// It's used as a deserialization container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBriefMetadata {
    pub col_names: Vec<SemiStr>,
    pub col_types: Vec<ValType>,
    pub index_specs: Vec<IndexSpec>,
}

impl Deser for TableBriefMetadata {
    fn deser<S: Serde + ?Sized>(input: &S, start_idx: usize) -> Result<(usize, Self)> {
        let (idx, col_names) = <Vec<SemiStr>>::deser(input, start_idx)?;
        let (idx, col_types) = <Vec<ValType>>::deser(input, idx)?;
        let (idx, index_specs) = <Vec<IndexSpec>>::deser(input, idx)?;
        Ok((
            idx,
            TableBriefMetadata {
                col_names,
                col_types,
                index_specs,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnSpec, IndexAttributes, IndexKey};

    #[test]
    fn test_table_metadata_serde() {
        let metadata = TableMetadata::new(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::NULLABLE),
            ],
            vec![IndexSpec::new(
                "idx1",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        );

        let ser_view = metadata.ser_view();

        let len = ser_view.ser_len();
        let mut vec = vec![0u8; len];
        let idx = ser_view.ser(&mut vec[..], 0);
        assert_eq!(idx, vec.len());
        let (idx, brief) = TableBriefMetadata::deser(&vec[..], 0).unwrap();
        assert_eq!(idx, vec.len());
        assert_eq!(metadata.col_names, brief.col_names);
        assert_eq!(metadata.col_types, brief.col_types);
        assert_eq!(metadata.index_specs, brief.index_specs);
    }
}
