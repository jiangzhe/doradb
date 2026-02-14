//! This module is used to support vector scan on row pages.
//! Within row page, column data are located continuously,
//! so it's natural to scan column one by one.
//!
//! Actual table scan must take MVCC into consideration,
//! but that's not included in this module.

use crate::bitmap::{Bitmap, BitmapRangeFilter};
use crate::buffer::frame::FrameContext;
use crate::catalog::TableMetadata;
use crate::error::{Error, Result};
use crate::row::RowPage;
use crate::trx::undo::RowUndoKind;
use crate::trx::{TrxID, trx_is_committed};
use crate::value::{PageVar, ValBuffer, ValType};

pub struct ScanBuffer {
    cols: Vec<ColBuffer>,
    len: usize,
}

pub struct ScanColumn<'a> {
    pub col_idx: usize,
    pub null_bitmap: Option<&'a [u64]>,
    pub values: ScanColumnValues<'a>,
}

pub enum ScanColumnValues<'a> {
    I8(&'a [i8]),
    U8(&'a [u8]),
    I16(&'a [i16]),
    U16(&'a [u16]),
    I32(&'a [i32]),
    U32(&'a [u32]),
    F32(&'a [f32]),
    I64(&'a [i64]),
    U64(&'a [u64]),
    F64(&'a [f64]),
    VarByte {
        offsets: &'a [(usize, usize)],
        data: &'a [u8],
    },
}

impl ScanBuffer {
    /// Create a new scan buffer.
    #[inline]
    pub fn new(metadata: &TableMetadata, scan_set: &[usize]) -> Self {
        let cols: Vec<_> = scan_set
            .iter()
            .map(|&col_idx| ColBuffer::new(col_idx, metadata.col_type(col_idx)))
            .collect();
        ScanBuffer { cols, len: 0 }
    }

    /// Returns number of rows added to this buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns number of columns stored in this buffer.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.cols.len()
    }

    /// Returns scan column data by position.
    #[inline]
    pub fn column(&self, idx: usize) -> Option<ScanColumn<'_>> {
        self.cols.get(idx).map(|col| {
            let values = match &col.vals {
                ValBuffer::I8(vals) => ScanColumnValues::I8(vals),
                ValBuffer::U8(vals) => ScanColumnValues::U8(vals),
                ValBuffer::I16(vals) => ScanColumnValues::I16(vals),
                ValBuffer::U16(vals) => ScanColumnValues::U16(vals),
                ValBuffer::I32(vals) => ScanColumnValues::I32(vals),
                ValBuffer::U32(vals) => ScanColumnValues::U32(vals),
                ValBuffer::F32(vals) => ScanColumnValues::F32(vals),
                ValBuffer::I64(vals) => ScanColumnValues::I64(vals),
                ValBuffer::U64(vals) => ScanColumnValues::U64(vals),
                ValBuffer::F64(vals) => ScanColumnValues::F64(vals),
                ValBuffer::VarByte { offsets, data } => ScanColumnValues::VarByte { offsets, data },
            };
            ScanColumn {
                col_idx: col.col_idx,
                null_bitmap: col.null_bitmap.as_deref(),
                values,
            }
        })
    }

    /// Scan given page and extend all rows into buffer.
    /// Note: If error is returned, the state of this buffer might be invalid.
    #[inline]
    pub fn scan<'p, 'm>(&mut self, view: PageVectorView<'p, 'm>) -> Result<()> {
        let new_len = self.len + view.rows_non_deleted();
        for buf in &mut self.cols {
            let (null_bitmap, vals) = view.col(buf.col_idx);
            // First, extend null bitmap.
            match (buf.null_bitmap.as_mut(), null_bitmap) {
                (Some(res), Some(delta)) => {
                    let new_units = new_len.div_ceil(64);
                    if new_units > res.len() {
                        res.resize(new_units, 0);
                    }
                    let mut offset = self.len;
                    // only extend non-deleted parts.
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        for idx in start_idx..end_idx {
                            let flag = delta.bitmap_get(idx);
                            if flag {
                                res.bitmap_set(offset);
                            } else {
                                res.bitmap_unset(offset);
                            }
                            offset += 1;
                        }
                    }
                }
                (None, None) => (),
                _ => return Err(Error::InvalidColumnScan),
            }
            // Second, extend values
            match (&mut buf.vals, vals) {
                (ValBuffer::I8(res), ValArrayRef::I8(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::U8(res), ValArrayRef::U8(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::I16(res), ValArrayRef::I16(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::U16(res), ValArrayRef::U16(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::I32(res), ValArrayRef::I32(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::U32(res), ValArrayRef::U32(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::F32(res), ValArrayRef::F32(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::I64(res), ValArrayRef::I64(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::U64(res), ValArrayRef::U64(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::F64(res), ValArrayRef::F64(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(&delta[start_idx..end_idx]);
                    }
                }
                (ValBuffer::VarByte { offsets, data }, ValArrayRef::VarByte(delta, page)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        for pv in &delta[start_idx..end_idx] {
                            let v = pv.as_bytes(page);
                            let offset = data.len();
                            offsets.push((offset, offset + v.len()));
                            data.extend(v);
                        }
                    }
                }
                _ => return Err(Error::InvalidColumnScan),
            }
        }
        self.len = new_len;
        Ok(())
    }

    /// Clear the buffer.
    #[inline]
    pub fn clear(&mut self) {
        for col in &mut self.cols {
            col.clear();
        }
        self.len = 0;
    }

    /// Truncate the buffer to given length.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }
        for col in &mut self.cols {
            if let Some(null_bitmap) = col.null_bitmap.as_mut() {
                let units = len.div_ceil(64);
                null_bitmap.truncate(units);
                if let Some(last) = null_bitmap.last_mut() {
                    let rem = len % 64;
                    if rem != 0 {
                        let mask = (1u64 << rem) - 1;
                        *last &= mask;
                    }
                }
            }
            match &mut col.vals {
                ValBuffer::I8(vals) => vals.truncate(len),
                ValBuffer::U8(vals) => vals.truncate(len),
                ValBuffer::I16(vals) => vals.truncate(len),
                ValBuffer::U16(vals) => vals.truncate(len),
                ValBuffer::I32(vals) => vals.truncate(len),
                ValBuffer::U32(vals) => vals.truncate(len),
                ValBuffer::F32(vals) => vals.truncate(len),
                ValBuffer::I64(vals) => vals.truncate(len),
                ValBuffer::U64(vals) => vals.truncate(len),
                ValBuffer::F64(vals) => vals.truncate(len),
                ValBuffer::VarByte { offsets, data } => {
                    if len < offsets.len() {
                        offsets.truncate(len);
                        let end = offsets.last().map(|(_, end)| *end).unwrap_or(0);
                        data.truncate(end);
                    }
                }
            }
        }
        self.len = len;
    }
}

pub struct ColBuffer {
    col_idx: usize,
    null_bitmap: Option<Vec<u64>>,
    vals: ValBuffer,
}

impl ColBuffer {
    /// Create a new column buffer.
    #[inline]
    pub fn new(col_idx: usize, ty: ValType) -> Self {
        let null_bitmap = if ty.nullable { Some(vec![]) } else { None };
        let vals = ValBuffer::new(ty.kind);
        ColBuffer {
            col_idx,
            null_bitmap,
            vals,
        }
    }

    /// Clear current buffer.
    #[inline]
    pub fn clear(&mut self) {
        if let Some(null_bitmap) = self.null_bitmap.as_mut() {
            null_bitmap.clear();
        }
        self.vals.clear();
    }
}

/// Vectorized view on row page.
pub struct PageVectorView<'p, 'm> {
    page: &'p RowPage,
    metadata: &'m TableMetadata,
    // row count should be freezed when creating this view.
    // to allow concurrent insert when query this page.
    row_count: usize,
    // delete bitmap is a copy of the one on current page.
    // it can be modified to represent an old view when
    // MVCC is enabled.
    del_bitmap: Vec<u64>,
}

impl RowPage {
    /// Create a vectorized view on row page in transition state.
    /// This view reflects MVCC visibility at given snapshot timestamp.
    #[inline]
    pub fn vector_view_in_transition<'p, 'm>(
        &'p self,
        metadata: &'m TableMetadata,
        ctx: &FrameContext,
        cutoff_ts: TrxID,
        global_min_active_sts: TrxID,
    ) -> PageVectorView<'p, 'm> {
        let Some(map) = ctx.row_ver() else {
            return self.vector_view(metadata);
        };
        let initial_mod_counter = map.mod_counter();
        if map.max_sts() < global_min_active_sts {
            let view = self.vector_view(metadata);
            if map.mod_counter() == initial_mod_counter {
                return view;
            }
        }
        let row_count = self.header.row_count();
        let mut del_bitmap = Vec::from(self.del_bitmap(row_count));
        for row_idx in 0..row_count {
            let undo_guard = map.read_latch(row_idx);
            let Some(head) = undo_guard.as_ref() else {
                continue;
            };
            let mut ts = head.ts();
            let mut entry = head.next.main.entry.clone();
            loop {
                match &entry.as_ref().kind {
                    RowUndoKind::Lock => {}
                    RowUndoKind::Delete => {
                        if !trx_is_committed(ts) || ts >= cutoff_ts {
                            del_bitmap.bitmap_unset(row_idx);
                        } else {
                            del_bitmap.bitmap_set(row_idx);
                        }
                        break;
                    }
                    RowUndoKind::Insert | RowUndoKind::Update(_) => {
                        if !trx_is_committed(ts) || ts >= cutoff_ts {
                            panic!("Uncommitted/Future Insert/Update found in Checkpoint");
                        }
                        del_bitmap.bitmap_unset(row_idx);
                        break;
                    }
                }
                match entry.as_ref().next.as_ref() {
                    None => break,
                    Some(next) => {
                        ts = next.main.status.ts();
                        entry = next.main.entry.clone();
                    }
                }
            }
        }
        PageVectorView {
            page: self,
            metadata,
            row_count,
            del_bitmap,
        }
    }
}

impl<'p, 'm> PageVectorView<'p, 'm> {
    /// Create a page vector view.
    #[inline]
    pub fn new(page: &'p RowPage, metadata: &'m TableMetadata) -> Self {
        let row_count = page.header.row_count();
        let del_bitmap = Vec::from(page.del_bitmap(row_count));
        PageVectorView {
            page,
            metadata,
            row_count,
            del_bitmap,
        }
    }

    /// Count rows not deleted.
    #[inline]
    pub fn rows_non_deleted(&self) -> usize {
        self.del_bitmap
            .bitmap_range_iter(self.row_count)
            .map(|(f, n)| if f { 0 } else { n })
            .sum()
    }

    /// Returns range of non-deleted rows.
    #[inline]
    pub fn range_non_deleted(&self) -> BitmapRangeFilter<'_> {
        self.del_bitmap.bitmap_range_filter(self.row_count, false)
    }

    /// Returns null bitmap and value data of given column.
    #[inline]
    pub fn col(&self, col_idx: usize) -> (Option<&'p [u64]>, ValArrayRef<'p>) {
        self.page.vals(self.metadata, col_idx, self.row_count)
    }
}

/// Represents the safe typed value array in row page.
pub enum ValArrayRef<'a> {
    I8(&'a [i8]),
    U8(&'a [u8]),
    I16(&'a [i16]),
    U16(&'a [u16]),
    I32(&'a [i32]),
    U32(&'a [u32]),
    F32(&'a [f32]),
    I64(&'a [i64]),
    U64(&'a [u64]),
    F64(&'a [f64]),
    VarByte(&'a [PageVar], &'a [u8]),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::row::tests::create_row_page;
    use crate::row::{Delete, InsertRow};
    use crate::trx::undo::{
        MainBranch, NextRowUndo, OwnedRowUndo, RowUndoHead, RowUndoKind, UndoStatus,
    };
    use crate::trx::ver_map::RowVersionMap;
    use crate::trx::{MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS};
    use crate::value::{Val, ValKind};
    use semistr::SemiStr;
    use std::sync::Arc;

    #[test]
    fn test_row_page_vector_scan() {
        let metadata = TableMetadata::new(
            vec![
                ColumnSpec {
                    column_name: SemiStr::new("c1"),
                    column_type: ValKind::I8,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c2"),
                    column_type: ValKind::U8,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c3"),
                    column_type: ValKind::I16,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c4"),
                    column_type: ValKind::U16,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c5"),
                    column_type: ValKind::I32,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c6"),
                    column_type: ValKind::U32,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c7"),
                    column_type: ValKind::F32,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c8"),
                    column_type: ValKind::I64,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c9"),
                    column_type: ValKind::U64,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c10"),
                    column_type: ValKind::F64,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c11"),
                    column_type: ValKind::VarByte,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c12"),
                    column_type: ValKind::VarByte,
                    column_attributes: ColumnAttributes::empty(),
                },
            ],
            vec![],
        );
        let mut page = create_row_page();
        page.init(100, 200, &metadata);
        let short = b"short";
        let long = b"very loooooooooooooooooong";

        let insert = vec![
            Val::I8(-1),
            Val::U8(1),
            Val::I16(-1000),
            Val::U16(1000),
            Val::I32(-1_000_000),
            Val::U32(1_000_000),
            Val::from(1.5f32),
            Val::I64(-(1 << 35)),
            Val::U64(1 << 35),
            Val::from(0.5f64),
            Val::from(&short[..]),
            Val::from(&long[..]),
        ];
        for row_id in 100u64..200 {
            let res = page.insert(&metadata, &insert);
            if let InsertRow::Ok(rid) = res {
                assert!(rid == row_id);
            } else {
                panic!("insert failed");
            }
        }
        // try deleting 2 rows
        let res = page.delete(101);
        assert!(matches!(res, Delete::Ok));
        let res = page.delete(180);
        assert!(matches!(res, Delete::Ok));
        // try vector scan
        let mut scanner = ScanBuffer::new(&metadata, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        let view = page.vector_view(&metadata);
        let res = scanner.scan(view);
        assert!(res.is_ok());
        assert!(scanner.len() == 98);
        scanner.clear();
        assert!(scanner.is_empty());
    }

    #[test]
    fn test_scan_buffer_truncate_all_types() {
        let metadata = TableMetadata::new(
            vec![
                ColumnSpec {
                    column_name: SemiStr::new("c1"),
                    column_type: ValKind::I8,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c2"),
                    column_type: ValKind::U8,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c3"),
                    column_type: ValKind::I16,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c4"),
                    column_type: ValKind::U16,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c5"),
                    column_type: ValKind::I32,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c6"),
                    column_type: ValKind::U32,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c7"),
                    column_type: ValKind::F32,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c8"),
                    column_type: ValKind::I64,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c9"),
                    column_type: ValKind::U64,
                    column_attributes: ColumnAttributes::empty(),
                },
                ColumnSpec {
                    column_name: SemiStr::new("c10"),
                    column_type: ValKind::F64,
                    column_attributes: ColumnAttributes::NULLABLE,
                },
                ColumnSpec {
                    column_name: SemiStr::new("c11"),
                    column_type: ValKind::VarByte,
                    column_attributes: ColumnAttributes::empty(),
                },
            ],
            vec![],
        );
        let mut page = create_row_page();
        page.init(0, 5, &metadata);
        for row_id in 0u64..5 {
            let row_idx = row_id as usize;
            let row_bytes = format!("row-{row_id}");
            let insert = vec![
                Val::I8(row_idx as i8),
                if matches!(row_idx, 0 | 3) {
                    Val::Null
                } else {
                    Val::U8(10 + row_idx as u8)
                },
                Val::I16(-10 - row_idx as i16),
                if matches!(row_idx, 2 | 3) {
                    Val::Null
                } else {
                    Val::U16(100 + row_idx as u16)
                },
                Val::I32(-1000 - row_idx as i32),
                if matches!(row_idx, 1 | 4) {
                    Val::Null
                } else {
                    Val::U32(1000 + row_idx as u32)
                },
                Val::from(1.5f32 + row_idx as f32),
                if matches!(row_idx, 0 | 2 | 4) {
                    Val::Null
                } else {
                    Val::I64(-5000 - row_idx as i64)
                },
                Val::U64(5000 + row_idx as u64),
                if row_idx == 3 {
                    Val::Null
                } else {
                    Val::from(10.5f64 + row_idx as f64)
                },
                Val::from(row_bytes.as_bytes()),
            ];
            let res = page.insert(&metadata, &insert);
            if let InsertRow::Ok(rid) = res {
                assert_eq!(rid, row_id);
            } else {
                panic!("insert failed");
            }
        }
        let mut scanner = ScanBuffer::new(&metadata, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let view = page.vector_view(&metadata);
        scanner.scan(view).unwrap();
        assert_eq!(scanner.len(), 5);

        scanner.truncate(3);
        assert_eq!(scanner.len(), 3);

        let col_i8 = scanner.column(0).unwrap();
        match col_i8.values {
            ScanColumnValues::I8(vals) => assert_eq!(vals, &[0, 1, 2]),
            _ => panic!("unexpected i8 column type"),
        }

        let col_u8 = scanner.column(1).unwrap();
        match col_u8.values {
            ScanColumnValues::U8(vals) => assert_eq!(vals.len(), 3),
            _ => panic!("unexpected u8 column type"),
        }
        let null_bitmap = col_u8.null_bitmap.expect("u8 null bitmap");
        assert!(null_bitmap.bitmap_get(0));
        assert!(!null_bitmap.bitmap_get(1));
        assert!(!null_bitmap.bitmap_get(2));
        assert!(!null_bitmap.bitmap_get(3));

        let col_i16 = scanner.column(2).unwrap();
        match col_i16.values {
            ScanColumnValues::I16(vals) => assert_eq!(vals, &[-10, -11, -12]),
            _ => panic!("unexpected i16 column type"),
        }

        let col_u16 = scanner.column(3).unwrap();
        match col_u16.values {
            ScanColumnValues::U16(vals) => assert_eq!(vals.len(), 3),
            _ => panic!("unexpected u16 column type"),
        }
        let null_bitmap = col_u16.null_bitmap.expect("u16 null bitmap");
        assert!(!null_bitmap.bitmap_get(0));
        assert!(!null_bitmap.bitmap_get(1));
        assert!(null_bitmap.bitmap_get(2));
        assert!(!null_bitmap.bitmap_get(3));

        let col_i32 = scanner.column(4).unwrap();
        match col_i32.values {
            ScanColumnValues::I32(vals) => assert_eq!(vals, &[-1000, -1001, -1002]),
            _ => panic!("unexpected i32 column type"),
        }

        let col_u32 = scanner.column(5).unwrap();
        match col_u32.values {
            ScanColumnValues::U32(vals) => assert_eq!(vals.len(), 3),
            _ => panic!("unexpected u32 column type"),
        }
        let null_bitmap = col_u32.null_bitmap.expect("u32 null bitmap");
        assert!(!null_bitmap.bitmap_get(0));
        assert!(null_bitmap.bitmap_get(1));
        assert!(!null_bitmap.bitmap_get(2));
        assert!(!null_bitmap.bitmap_get(3));

        let col_f32 = scanner.column(6).unwrap();
        match col_f32.values {
            ScanColumnValues::F32(vals) => {
                let expected = [1.5f32, 2.5f32, 3.5f32];
                for (idx, val) in vals.iter().enumerate() {
                    assert!((*val - expected[idx]).abs() <= f32::EPSILON);
                }
            }
            _ => panic!("unexpected f32 column type"),
        }

        let col_i64 = scanner.column(7).unwrap();
        match col_i64.values {
            ScanColumnValues::I64(vals) => assert_eq!(vals.len(), 3),
            _ => panic!("unexpected i64 column type"),
        }
        let null_bitmap = col_i64.null_bitmap.expect("i64 null bitmap");
        assert!(null_bitmap.bitmap_get(0));
        assert!(!null_bitmap.bitmap_get(1));
        assert!(null_bitmap.bitmap_get(2));
        assert!(!null_bitmap.bitmap_get(3));

        let col_u64 = scanner.column(8).unwrap();
        match col_u64.values {
            ScanColumnValues::U64(vals) => assert_eq!(vals, &[5000, 5001, 5002]),
            _ => panic!("unexpected u64 column type"),
        }

        let col_f64 = scanner.column(9).unwrap();
        match col_f64.values {
            ScanColumnValues::F64(vals) => assert_eq!(vals.len(), 3),
            _ => panic!("unexpected f64 column type"),
        }
        let null_bitmap = col_f64.null_bitmap.expect("f64 null bitmap");
        assert!(!null_bitmap.bitmap_get(0));
        assert!(!null_bitmap.bitmap_get(1));
        assert!(!null_bitmap.bitmap_get(2));
        assert!(!null_bitmap.bitmap_get(3));

        let col_varbyte = scanner.column(10).unwrap();
        match col_varbyte.values {
            ScanColumnValues::VarByte { offsets, data } => {
                assert_eq!(offsets.len(), 3);
                let values: Vec<Vec<u8>> = offsets
                    .iter()
                    .map(|(start, end)| data[*start..*end].to_vec())
                    .collect();
                assert_eq!(
                    values,
                    vec![b"row-0".to_vec(), b"row-1".to_vec(), b"row-2".to_vec()]
                );
            }
            _ => panic!("unexpected varbyte column type"),
        }
    }

    #[test]
    fn test_vector_view_in_transition_revive_deleted_row() {
        let metadata = TableMetadata::new(
            vec![ColumnSpec {
                column_name: SemiStr::new("c1"),
                column_type: ValKind::I8,
                column_attributes: ColumnAttributes::empty(),
            }],
            vec![],
        );
        let mut page = create_row_page();
        page.init(0, 1, &metadata);
        let insert = vec![Val::I8(1)];
        let res = page.insert(&metadata, &insert);
        assert!(matches!(res, InsertRow::Ok(0)));
        assert!(matches!(page.delete(0), Delete::Ok));

        let mut map = RowVersionMap::new(Arc::new(metadata.clone()), 1);
        let undo = OwnedRowUndo::new(0, Some(0), page.row_id(0), RowUndoKind::Delete);
        let undo_ref = undo.leak();
        let head = RowUndoHead {
            next: NextRowUndo {
                main: MainBranch {
                    entry: undo_ref,
                    status: UndoStatus::CTS(200),
                },
                indexes: vec![],
            },
            purge_ts: MIN_SNAPSHOT_TS,
        };
        *map.write_exclusive(0) = Some(Box::new(head));
        let ctx = FrameContext::RowVerMap(map);

        let view = page.vector_view_in_transition(&metadata, &ctx, 100, 0);
        assert_eq!(view.rows_non_deleted(), 1);
        let _keep_undo = undo;
    }

    #[test]
    fn test_vector_view_in_transition_fast_path() {
        let metadata = TableMetadata::new(
            vec![ColumnSpec {
                column_name: SemiStr::new("c1"),
                column_type: ValKind::I8,
                column_attributes: ColumnAttributes::empty(),
            }],
            vec![],
        );
        let mut page = create_row_page();
        page.init(0, 2, &metadata);
        let insert = vec![Val::I8(1)];
        assert!(matches!(page.insert(&metadata, &insert), InsertRow::Ok(0)));
        assert!(matches!(page.insert(&metadata, &insert), InsertRow::Ok(1)));
        assert!(matches!(page.delete(1), Delete::Ok));

        let map = RowVersionMap::new(Arc::new(metadata.clone()), 2);
        let ctx = FrameContext::RowVerMap(map);

        let expected = page.vector_view(&metadata);
        let view = page.vector_view_in_transition(&metadata, &ctx, 100, 1);
        assert_eq!(view.rows_non_deleted(), expected.rows_non_deleted());
    }

    #[test]
    #[should_panic(expected = "Uncommitted/Future Insert/Update found in Checkpoint")]
    fn test_vector_view_in_transition_uncommitted_insert_panics() {
        let metadata = TableMetadata::new(
            vec![ColumnSpec {
                column_name: SemiStr::new("c1"),
                column_type: ValKind::I8,
                column_attributes: ColumnAttributes::empty(),
            }],
            vec![],
        );
        let mut page = create_row_page();
        page.init(0, 1, &metadata);
        let insert = vec![Val::I8(1)];
        assert!(matches!(page.insert(&metadata, &insert), InsertRow::Ok(0)));

        let mut map = RowVersionMap::new(Arc::new(metadata.clone()), 1);
        let uncommitted_ts = MIN_ACTIVE_TRX_ID + 1;
        let undo = OwnedRowUndo::new(0, Some(0), page.row_id(0), RowUndoKind::Insert);
        let undo_ref = undo.leak();
        let head = RowUndoHead {
            next: NextRowUndo {
                main: MainBranch {
                    entry: undo_ref,
                    status: UndoStatus::CTS(uncommitted_ts),
                },
                indexes: vec![],
            },
            purge_ts: MIN_SNAPSHOT_TS,
        };
        *map.write_exclusive(0) = Some(Box::new(head));
        let ctx = FrameContext::RowVerMap(map);

        let _view = page.vector_view_in_transition(&metadata, &ctx, 100, 0);
        let _keep_undo = undo;
    }
}
