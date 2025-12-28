//! This module is used to support vector scan on row pages.
//! Within row page, column data are located continuously,
//! so it's natural to scan column one by one.
//!
//! Actual table scan must take MVCC into consideration,
//! but that's not included in this module.

use crate::bitmap::{Bitmap, BitmapRangeFilter};
use crate::catalog::TableMetadata;
use crate::error::{Error, Result};
use crate::row::RowPage;
use crate::value::{PageVar, ValBuffer, ValType};

pub struct ScanBuffer {
    cols: Vec<ColBuffer>,
    len: usize,
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
    use crate::catalog::{ColumnAttributes, ColumnSpec};
    use crate::row::tests::create_row_page;
    use crate::row::{Delete, InsertRow};
    use crate::value::{Val, ValKind};
    use semistr::SemiStr;

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
}
