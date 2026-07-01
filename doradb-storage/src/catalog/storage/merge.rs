use super::{RowRecord, invalid_catalog_payload, validate_catalog_row};
use crate::catalog::table::TableMetadata;
use crate::error::Result;
use crate::index::{BTreeKey, BTreeKeyEncoder};
use crate::row::ops::{SelectKey, UpdateCol};
use crate::value::{Val, ValType};
use std::collections::{BTreeMap, btree_map::Entry};
use std::slice;

#[derive(Debug, Clone, PartialEq, Eq)]
enum FoldedCatalogEntry {
    Base(Vec<Val>),
    Insert(Vec<Val>),
    Update(Vec<Val>),
    Delete,
}

/// Fold one catalog table's checkpoint base rows and redo rows by primary key.
///
/// Checkpoint rewrite always persists a compact full image of each changed
/// catalog table. This helper starts with rows decoded from the previous
/// checkpoint root, then folds redo row operations into a single final state per
/// primary key. Keys are encoded with the same memory-comparable format as
/// B-tree indexes, so materialization naturally emits rows in primary-key order.
///
/// ```text
/// Current state | insert(row) | update(cols) | delete
/// --------------+-------------+--------------+--------
/// absent        | Insert      | error        | error
/// Base          | error       | Update       | Delete
/// Insert        | error       | Insert       | absent
/// Update        | error       | Update       | Delete
/// Delete        | Update      | error        | error
/// ```
///
/// `Delete + insert` becomes `Update` because the key existed in the base root;
/// the final compact image only needs the new row value for that primary key.
pub(super) struct CatalogFoldedRows {
    key_builder: CatalogMergeKeyBuilder,
    rows: BTreeMap<BTreeKey, FoldedCatalogEntry>,
}

impl CatalogFoldedRows {
    pub(super) fn from_base_rows(metadata: &TableMetadata, rows: Vec<RowRecord>) -> Result<Self> {
        let key_builder = CatalogMergeKeyBuilder::new(metadata)?;
        let mut folded = Self {
            key_builder,
            rows: BTreeMap::new(),
        };
        for row in rows {
            validate_catalog_row(metadata, &row.vals, "catalog checkpoint base row")?;
            let key = folded.key_builder.key_from_row(&row.vals)?;
            match folded.rows.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(FoldedCatalogEntry::Base(row.vals));
                }
                Entry::Occupied(entry) => {
                    return Err(invalid_catalog_payload(format!(
                        "catalog checkpoint duplicate base primary key: key={:?}",
                        entry.key()
                    )));
                }
            }
        }
        Ok(folded)
    }

    /// Returns whether the folded result must be persisted as a new table root.
    ///
    /// This is a final-state predicate, not a record of whether redo rows were
    /// scanned. A same-batch insert of a new primary key followed by delete
    /// removes the `Insert` entry and safely folds back to `absent`, so it does
    /// not require a rewrite. Base rows cannot fold back to `absent`:
    /// `Base -> Delete` remains `Delete`, and `Delete -> Insert` over a base key
    /// becomes `Update`. No non-base state folds back to `Base`. Therefore, if
    /// every remaining entry is still `Base`, the compact image is unchanged
    /// and the caller can skip rewriting the catalog table root.
    #[inline]
    pub(super) fn should_rewrite(&self) -> bool {
        self.rows
            .values()
            .any(|entry| !matches!(entry, FoldedCatalogEntry::Base(_)))
    }

    pub(super) fn fold_insert(&mut self, metadata: &TableMetadata, vals: Vec<Val>) -> Result<()> {
        validate_catalog_row(metadata, &vals, "catalog checkpoint insert row")?;
        let key = self.key_builder.key_from_row(&vals)?;
        match self.rows.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(FoldedCatalogEntry::Insert(vals));
            }
            Entry::Occupied(mut entry) => match entry.get() {
                FoldedCatalogEntry::Delete => {
                    *entry.get_mut() = FoldedCatalogEntry::Update(vals);
                }
                FoldedCatalogEntry::Base(_) => {
                    return Err(invalid_catalog_payload(format!(
                        "catalog checkpoint insert primary key already exists: key={:?}",
                        entry.key()
                    )));
                }
                FoldedCatalogEntry::Insert(_) => {
                    return Err(invalid_catalog_payload(format!(
                        "invalid catalog checkpoint fold: insert + insert, key={:?}",
                        entry.key()
                    )));
                }
                FoldedCatalogEntry::Update(_) => {
                    return Err(invalid_catalog_payload(format!(
                        "invalid catalog checkpoint fold: update + insert, key={:?}",
                        entry.key()
                    )));
                }
            },
        }
        Ok(())
    }

    pub(super) fn fold_update(
        &mut self,
        metadata: &TableMetadata,
        key: &SelectKey,
        update: &[UpdateCol],
    ) -> Result<()> {
        let merge_key = self.key_builder.key_from_select_key(key, "update")?;
        let Some(entry) = self.rows.get_mut(&merge_key) else {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint update-by-primary-key target missing: index_no={}, key_vals={:?}",
                key.index_no, key.vals
            )));
        };
        match entry {
            FoldedCatalogEntry::Base(vals) => {
                let mut updated_vals = vals.clone();
                apply_catalog_update_by_primary_key(
                    metadata,
                    &self.key_builder,
                    key,
                    &mut updated_vals,
                    update,
                )?;
                *entry = FoldedCatalogEntry::Update(updated_vals);
            }
            FoldedCatalogEntry::Insert(vals) | FoldedCatalogEntry::Update(vals) => {
                let mut updated_vals = vals.clone();
                apply_catalog_update_by_primary_key(
                    metadata,
                    &self.key_builder,
                    key,
                    &mut updated_vals,
                    update,
                )?;
                *vals = updated_vals;
            }
            FoldedCatalogEntry::Delete => {
                return Err(invalid_catalog_payload(format!(
                    "invalid catalog checkpoint fold: delete + update, key={merge_key:?}"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn fold_delete(&mut self, key: &SelectKey) -> Result<()> {
        let merge_key = self.key_builder.key_from_select_key(key, "delete")?;
        match self.rows.entry(merge_key) {
            Entry::Vacant(_) => {
                return Err(invalid_catalog_payload(format!(
                    "catalog checkpoint delete-by-primary-key target missing: index_no={}, key_vals={:?}",
                    key.index_no, key.vals
                )));
            }
            Entry::Occupied(mut entry) => match entry.get() {
                FoldedCatalogEntry::Base(_) | FoldedCatalogEntry::Update(_) => {
                    *entry.get_mut() = FoldedCatalogEntry::Delete;
                }
                FoldedCatalogEntry::Insert(_) => {
                    entry.remove();
                }
                FoldedCatalogEntry::Delete => {
                    return Err(invalid_catalog_payload(format!(
                        "invalid catalog checkpoint fold: delete + delete, key={:?}",
                        entry.key()
                    )));
                }
            },
        }
        Ok(())
    }

    pub(super) fn materialize_output_rows(&self) -> Result<Vec<Vec<Val>>> {
        let mut rows = Vec::new();
        for entry in self.rows.values() {
            match entry {
                FoldedCatalogEntry::Base(vals)
                | FoldedCatalogEntry::Insert(vals)
                | FoldedCatalogEntry::Update(vals) => rows.push(vals.clone()),
                FoldedCatalogEntry::Delete => {}
            }
        }
        Ok(rows)
    }
}

pub(super) struct CatalogMergeKeyBuilder {
    index_no: usize,
    col_idxs: Box<[usize]>,
    val_types: Box<[ValType]>,
    encoder: BTreeKeyEncoder,
}

impl CatalogMergeKeyBuilder {
    #[inline]
    pub(super) fn new(metadata: &TableMetadata) -> Result<Self> {
        let Some(primary_key) = metadata.primary_key() else {
            return Err(invalid_catalog_payload(
                "catalog checkpoint primary key not found",
            ));
        };
        let index_spec = primary_key.spec();
        let mut col_idxs = Vec::with_capacity(index_spec.cols.len());
        let mut val_types = Vec::with_capacity(index_spec.cols.len());
        for index_key in &index_spec.cols {
            let col_idx = usize::from(index_key.col_no);
            if col_idx >= metadata.col.col_count() {
                return Err(invalid_catalog_payload(format!(
                    "catalog checkpoint primary key column out of range: column_no={}, column_count={}",
                    index_key.col_no,
                    metadata.col.col_count()
                )));
            }
            col_idxs.push(col_idx);
            val_types.push(metadata.col.col_type(col_idx));
        }
        if val_types.is_empty() {
            return Err(invalid_catalog_payload(
                "catalog checkpoint primary key has no columns",
            ));
        }
        let encoder = BTreeKeyEncoder::new(val_types.clone());
        Ok(Self {
            index_no: primary_key.index_no(),
            col_idxs: col_idxs.into_boxed_slice(),
            val_types: val_types.into_boxed_slice(),
            encoder,
        })
    }

    #[inline]
    pub(super) fn key_from_row(&self, row: &[Val]) -> Result<BTreeKey> {
        self.validate_key_candidate_row(row)?;
        let key = match self.col_idxs.as_ref() {
            [idx0] => self.encoder.encode(slice::from_ref(&row[*idx0])),
            [idx0, idx1] => self.encoder.encode(&[&row[*idx0], &row[*idx1]]),
            [idx0, idx1, idx2] => self
                .encoder
                .encode(&[&row[*idx0], &row[*idx1], &row[*idx2]]),
            col_idxs => {
                let key_vals: Vec<_> = col_idxs.iter().map(|idx| &row[*idx]).collect();
                self.encoder.encode(&key_vals)
            }
        };
        Ok(key)
    }

    #[inline]
    fn key_from_select_key(&self, key: &SelectKey, operation: &'static str) -> Result<BTreeKey> {
        self.validate_select_key(key, operation)?;
        self.key_from_key_vals(&key.vals)
    }

    #[inline]
    fn key_from_key_vals(&self, vals: &[Val]) -> Result<BTreeKey> {
        if self.col_idxs.len() != vals.len() {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint primary key value count {} does not match column count {}",
                vals.len(),
                self.col_idxs.len()
            )));
        }
        Ok(self.encoder.encode(vals))
    }

    #[inline]
    fn validate_select_key(&self, key: &SelectKey, operation: &'static str) -> Result<()> {
        if key.index_no != self.index_no {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint {operation} key is not primary key: index_no={}, primary_key_index_no={}",
                key.index_no, self.index_no
            )));
        }
        if key.vals.len() != self.col_idxs.len() {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint {operation} key value count {} does not match primary key column count {}",
                key.vals.len(),
                self.col_idxs.len()
            )));
        }
        if !self
            .val_types
            .iter()
            .zip(&key.vals)
            .all(|(ty, val)| catalog_val_type_match(*ty, val))
        {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint {operation} key type mismatch: index_no={}",
                key.index_no
            )));
        }
        Ok(())
    }

    #[inline]
    fn validate_key_candidate_row(&self, row: &[Val]) -> Result<()> {
        for col_idx in &self.col_idxs {
            if *col_idx >= row.len() {
                return Err(invalid_catalog_payload(format!(
                    "catalog checkpoint key candidate row missing index column: column_no={}, row_value_count={}",
                    col_idx,
                    row.len()
                )));
            }
        }
        Ok(())
    }

    #[inline]
    fn contains_key_column(&self, col_idx: usize) -> bool {
        self.col_idxs.contains(&col_idx)
    }

    #[inline]
    fn row_matches_key(&self, row: &[Val], key_vals: &[Val]) -> bool {
        self.col_idxs.len() == key_vals.len()
            && self
                .col_idxs
                .iter()
                .zip(key_vals)
                .all(|(col_idx, key_val)| row[*col_idx] == *key_val)
    }
}

#[inline]
fn catalog_val_type_match(ty: ValType, val: &Val) -> bool {
    if val.is_null() {
        ty.nullable
    } else {
        val.matches_kind(ty.kind)
    }
}

fn apply_catalog_update_by_primary_key(
    metadata: &TableMetadata,
    key_builder: &CatalogMergeKeyBuilder,
    key: &SelectKey,
    row: &mut [Val],
    update: &[UpdateCol],
) -> Result<()> {
    if row.len() != metadata.col.col_count() {
        return Err(invalid_catalog_payload(format!(
            "catalog checkpoint update candidate row value count {} does not match column count {}",
            row.len(),
            metadata.col.col_count()
        )));
    }
    let mut last_idx = None;
    for update_col in update {
        if update_col.idx >= metadata.col.col_count() {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint update column out of range: column_no={}, column_count={}",
                update_col.idx,
                metadata.col.col_count()
            )));
        }
        if last_idx.is_some_and(|idx| update_col.idx <= idx) {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint update columns not strictly ordered: column_no={}",
                update_col.idx
            )));
        }
        if !metadata.col.col_type_match(update_col.idx, &update_col.val) {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint update column type mismatch: column_no={}",
                update_col.idx
            )));
        }
        if key_builder.contains_key_column(update_col.idx) {
            return Err(invalid_catalog_payload(format!(
                "catalog checkpoint update cannot change primary key column: column_no={}",
                update_col.idx
            )));
        }
        row[update_col.idx] = update_col.val.clone();
        last_idx = Some(update_col.idx);
    }
    if !key_builder.row_matches_key(row, &key.vals) {
        return Err(invalid_catalog_payload(format!(
            "catalog checkpoint update-by-primary-key changed stable key: index_no={}, key_vals={:?}",
            key.index_no, key.vals
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::columns::catalog_definition_of_columns;
    use super::super::tables::catalog_definition_of_tables;
    use super::*;
    use crate::catalog::USER_OBJ_ID_START;
    use crate::error::DataIntegrityError;
    use crate::id::{RowID, TableID};
    use crate::value::ValKind;

    #[test]
    fn test_catalog_merge_key_builder_encodes_single_and_composite_keys() {
        let tables_metadata = &catalog_definition_of_tables().metadata;
        let tables_key_builder = CatalogMergeKeyBuilder::new(tables_metadata).unwrap();
        let tables_table_id = USER_OBJ_ID_START + 1;
        let tables_key = tables_key_builder
            .key_from_row(&catalog_table_vals(tables_table_id, 0))
            .unwrap();
        let tables_select_key = tables_key_builder
            .key_from_select_key(&SelectKey::new(0, vec![Val::from(tables_table_id)]), "test")
            .unwrap();
        assert_eq!(tables_key, tables_select_key);

        let columns_metadata = &catalog_definition_of_columns().metadata;
        let columns_table_id = USER_OBJ_ID_START + 2;
        let columns_no = 7u16;
        let columns_vals = catalog_column_vals(columns_table_id, columns_no, 8);
        let columns_key_builder = CatalogMergeKeyBuilder::new(columns_metadata).unwrap();
        let columns_key = columns_key_builder.key_from_row(&columns_vals).unwrap();
        let columns_select_key = columns_key_builder
            .key_from_select_key(
                &SelectKey::new(0, vec![Val::from(columns_table_id), Val::from(columns_no)]),
                "test",
            )
            .unwrap();
        assert_eq!(columns_key, columns_select_key);
    }

    #[test]
    fn test_catalog_fold_valid_state_transitions() {
        let metadata = &catalog_definition_of_tables().metadata;
        let base_table_id = USER_OBJ_ID_START + 1;
        let new_table_id = USER_OBJ_ID_START + 2;
        let base_key = SelectKey::new(0, vec![Val::from(base_table_id)]);
        let new_key = SelectKey::new(0, vec![Val::from(new_table_id)]);

        let mut folded = folded_tables_with_base(Vec::new());
        folded
            .fold_insert(metadata, catalog_table_vals(new_table_id, 0))
            .unwrap();
        folded
            .fold_update(metadata, &new_key, &catalog_table_update(7))
            .unwrap();
        assert_eq!(
            folded.materialize_output_rows().unwrap(),
            vec![catalog_table_vals(new_table_id, 7)]
        );

        let mut folded = folded_tables_with_base(Vec::new());
        folded
            .fold_insert(metadata, catalog_table_vals(new_table_id, 0))
            .unwrap();
        folded.fold_delete(&new_key).unwrap();
        assert!(folded.materialize_output_rows().unwrap().is_empty());
        assert!(!folded.should_rewrite());

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded
            .fold_update(metadata, &base_key, &catalog_table_update(3))
            .unwrap();
        folded
            .fold_update(metadata, &base_key, &catalog_table_update(4))
            .unwrap();
        assert_eq!(
            folded.materialize_output_rows().unwrap(),
            vec![catalog_table_vals(base_table_id, 4)]
        );

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded
            .fold_update(metadata, &base_key, &catalog_table_update(5))
            .unwrap();
        folded.fold_delete(&base_key).unwrap();
        assert!(folded.materialize_output_rows().unwrap().is_empty());

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded.fold_delete(&base_key).unwrap();
        folded
            .fold_insert(metadata, catalog_table_vals(base_table_id, 9))
            .unwrap();
        assert_eq!(
            folded.materialize_output_rows().unwrap(),
            vec![catalog_table_vals(base_table_id, 9)]
        );
    }

    #[test]
    fn test_catalog_fold_materializes_primary_key_order() {
        let metadata = &catalog_definition_of_tables().metadata;
        let table1_id = USER_OBJ_ID_START + 21;
        let table2_id = USER_OBJ_ID_START + 22;
        let table3_id = USER_OBJ_ID_START + 23;
        let mut folded = folded_tables_with_base(vec![(table3_id, 3), (table1_id, 1)]);

        folded
            .fold_insert(metadata, catalog_table_vals(table2_id, 2))
            .unwrap();

        assert_eq!(
            folded.materialize_output_rows().unwrap(),
            vec![
                catalog_table_vals(table1_id, 1),
                catalog_table_vals(table2_id, 2),
                catalog_table_vals(table3_id, 3),
            ]
        );
    }

    #[test]
    fn test_catalog_fold_should_rewrite_tracks_final_root_change() {
        let metadata = &catalog_definition_of_tables().metadata;
        let base_table_id = USER_OBJ_ID_START + 31;
        let new_table_id = USER_OBJ_ID_START + 32;
        let base_key = SelectKey::new(0, vec![Val::from(base_table_id)]);
        let new_key = SelectKey::new(0, vec![Val::from(new_table_id)]);

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded
            .fold_insert(metadata, catalog_table_vals(new_table_id, 0))
            .unwrap();
        folded.fold_delete(&new_key).unwrap();
        assert!(!folded.should_rewrite());

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded.fold_delete(&base_key).unwrap();
        assert!(folded.should_rewrite());

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded
            .fold_update(metadata, &base_key, &catalog_table_update(1))
            .unwrap();
        assert!(folded.should_rewrite());

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded.fold_delete(&base_key).unwrap();
        folded
            .fold_insert(metadata, catalog_table_vals(base_table_id, 2))
            .unwrap();
        assert!(folded.should_rewrite());
    }

    #[test]
    fn test_catalog_fold_rejects_invalid_state_transitions() {
        let metadata = &catalog_definition_of_tables().metadata;
        let base_table_id = USER_OBJ_ID_START + 11;
        let new_table_id = USER_OBJ_ID_START + 12;
        let base_key = SelectKey::new(0, vec![Val::from(base_table_id)]);
        let new_key = SelectKey::new(0, vec![Val::from(new_table_id)]);

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        assert!(
            folded
                .fold_insert(metadata, catalog_table_vals(base_table_id, 1))
                .is_err()
        );

        let mut folded = folded_tables_with_base(Vec::new());
        assert!(
            folded
                .fold_update(metadata, &new_key, &catalog_table_update(1))
                .is_err()
        );
        assert!(folded.fold_delete(&new_key).is_err());

        let mut folded = folded_tables_with_base(Vec::new());
        folded
            .fold_insert(metadata, catalog_table_vals(new_table_id, 0))
            .unwrap();
        assert!(
            folded
                .fold_insert(metadata, catalog_table_vals(new_table_id, 1))
                .is_err()
        );

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded
            .fold_update(metadata, &base_key, &catalog_table_update(1))
            .unwrap();
        assert!(
            folded
                .fold_insert(metadata, catalog_table_vals(base_table_id, 2))
                .is_err()
        );

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded.fold_delete(&base_key).unwrap();
        assert!(
            folded
                .fold_update(metadata, &base_key, &catalog_table_update(3))
                .is_err()
        );

        let mut folded = folded_tables_with_base(vec![(base_table_id, 0)]);
        folded.fold_delete(&base_key).unwrap();
        assert!(folded.fold_delete(&base_key).is_err());
    }

    #[test]
    fn test_catalog_key_candidate_row_requires_index_column() {
        let metadata = &catalog_definition_of_tables().metadata;
        let key_builder = CatalogMergeKeyBuilder::new(metadata).unwrap();
        let err = key_builder.key_from_row(&[]).unwrap_err();

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
        let report = format!("{err:?}");
        assert!(
            report.contains("catalog checkpoint key candidate row missing index column"),
            "{report}"
        );
        assert!(report.contains("column_no=0"), "{report}");
        assert!(report.contains("row_value_count=0"), "{report}");
    }

    fn catalog_column_vals(table_id: TableID, column_no: u16, name_len: usize) -> Vec<Val> {
        let mut name = vec![b'x'; name_len];
        if let Some(first) = name.first_mut() {
            *first = b'a' + (column_no % 26) as u8;
        }
        vec![
            Val::from(table_id),
            Val::from(column_no),
            Val::from(name),
            Val::from(ValKind::U64 as u32),
            Val::from(0u32),
        ]
    }

    fn catalog_table_vals(table_id: TableID, next_index_no: u16) -> Vec<Val> {
        vec![Val::from(table_id), Val::from(next_index_no)]
    }

    fn catalog_table_update(next_index_no: u16) -> Vec<UpdateCol> {
        vec![UpdateCol {
            idx: 1,
            val: Val::from(next_index_no),
        }]
    }

    fn folded_tables_with_base(base: Vec<(TableID, u16)>) -> CatalogFoldedRows {
        let metadata = &catalog_definition_of_tables().metadata;
        let rows = base
            .into_iter()
            .enumerate()
            .map(|(idx, (table_id, next_index_no))| RowRecord {
                row_id: RowID::new(idx as u64),
                vals: catalog_table_vals(table_id, next_index_no),
            })
            .collect();
        CatalogFoldedRows::from_base_rows(metadata, rows).unwrap()
    }
}
