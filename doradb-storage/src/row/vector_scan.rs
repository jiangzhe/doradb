//! This module is used to support vector scan on row pages.
//! Within row page, column data are located continuously,
//! so it's natural to scan column one by one.
//!
//! Actual table scan must take MVCC into consideration,
//! but that's not included in this module.

use crate::bitmap::{Bitmap, BitmapRangeFilter};
use crate::catalog::TableColumnLayout;
use crate::error::{InternalError, InternalResult};
use crate::row::{RowPage, RowPageNullBitmap};
use crate::value::{PageVar, Val, ValBuffer, ValType};
use error_stack::Report;
use zerocopy::byteorder::little_endian::{
    F32 as LeF32, F64 as LeF64, I16 as LeI16, I32 as LeI32, I64 as LeI64, U16 as LeU16,
    U32 as LeU32, U64 as LeU64,
};

/// Reusable column-oriented scan result buffer.
pub(crate) struct ScanBuffer {
    cols: Vec<ColBuffer>,
    len: usize,
}

impl ScanBuffer {
    /// Create a new scan buffer.
    #[inline]
    pub(crate) fn new(col_layout: &TableColumnLayout, scan_set: &[usize]) -> Self {
        let cols: Vec<_> = scan_set
            .iter()
            .map(|&col_idx| ColBuffer::new(col_idx, col_layout.col_type(col_idx)))
            .collect();
        ScanBuffer { cols, len: 0 }
    }

    /// Returns number of rows added to this buffer.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the buffer is empty.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns number of columns stored in this buffer.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved column_count"))]
    pub(crate) fn column_count(&self) -> usize {
        self.cols.len()
    }

    /// Returns scan column data by position.
    #[inline]
    pub(crate) fn column(&self, idx: usize) -> Option<ScanColumn<'_>> {
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

    /// Scan a page view built from the same column layout as this buffer.
    ///
    /// # Panics
    ///
    /// Panics when `view` does not have the column kinds and nullability used
    /// to construct this buffer.
    #[inline]
    pub(crate) fn scan<'p, 'm>(&mut self, view: PageVectorView<'p, 'm>) {
        let new_len = self.len + view.rows_non_deleted();
        for buf in &mut self.cols {
            let col_idx = buf.col_idx;
            let (null_bitmap, vals) = view.col(buf.col_idx);
            // First, extend null bitmap.
            match (buf.null_bitmap.as_mut(), null_bitmap) {
                (Some(res), Some(delta)) => {
                    let delta = delta.as_ref();
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
                _ => panic!(
                    "scan buffer nullability for column {col_idx} must match its table layout"
                ),
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
                        res.extend(delta[start_idx..end_idx].iter().map(|v| v.get()));
                    }
                }
                (ValBuffer::U16(res), ValArrayRef::U16(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(delta[start_idx..end_idx].iter().map(|v| v.get()));
                    }
                }
                (ValBuffer::I32(res), ValArrayRef::I32(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(delta[start_idx..end_idx].iter().map(|v| v.get()));
                    }
                }
                (ValBuffer::U32(res), ValArrayRef::U32(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(delta[start_idx..end_idx].iter().map(|v| v.get()));
                    }
                }
                (ValBuffer::F32(res), ValArrayRef::F32(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(delta[start_idx..end_idx].iter().map(|v| v.get()));
                    }
                }
                (ValBuffer::I64(res), ValArrayRef::I64(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(delta[start_idx..end_idx].iter().map(|v| v.get()));
                    }
                }
                (ValBuffer::U64(res), ValArrayRef::U64(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(delta[start_idx..end_idx].iter().map(|v| v.get()));
                    }
                }
                (ValBuffer::F64(res), ValArrayRef::F64(delta)) => {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        res.extend(delta[start_idx..end_idx].iter().map(|v| v.get()));
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
                _ => unreachable!(
                    "scan buffer value kind for column {col_idx} must match its table layout"
                ),
            }
        }
        self.len = new_len;
    }

    /// Append one decoded row into the scan buffer.
    ///
    /// Null values still append a type-specific placeholder so column value
    /// buffers stay row-aligned with the null bitmap, matching row-page scans.
    ///
    /// # Panics
    ///
    /// Panics when `vals` is not a complete row already validated against
    /// `col_layout`, or when the layout differs from the one used to construct
    /// this buffer.
    pub(crate) fn append_row_values(&mut self, col_layout: &TableColumnLayout, vals: &[Val]) {
        assert_eq!(
            vals.len(),
            col_layout.col_count(),
            "decoded row value count must match the trusted table layout"
        );
        for buf in &self.cols {
            let val = vals.get(buf.col_idx).unwrap_or_else(|| {
                panic!(
                    "scan buffer column {} must exist in the trusted decoded row",
                    buf.col_idx
                )
            });
            let col_type = col_layout.col_type(buf.col_idx);
            if val.is_null() {
                assert!(
                    col_type.nullable,
                    "scan buffer column {} must not receive null for a non-nullable layout",
                    buf.col_idx
                );
            } else {
                assert!(
                    val.matches_kind(col_type.kind),
                    "scan buffer column {} value kind must match {:?}",
                    buf.col_idx,
                    col_type.kind
                );
            }
        }

        let row_idx = self.len;
        let new_len = row_idx + 1;
        self.len = new_len;
        for buf in &mut self.cols {
            let val = &vals[buf.col_idx];
            if let Some(null_bitmap) = buf.null_bitmap.as_mut() {
                let new_units = new_len.div_ceil(64);
                if new_units > null_bitmap.len() {
                    null_bitmap.resize(new_units, 0);
                }
                if val.is_null() {
                    null_bitmap.bitmap_set(row_idx);
                } else {
                    null_bitmap.bitmap_unset(row_idx);
                }
            }
            append_scan_value(&mut buf.vals, val, buf.col_idx);
        }
    }

    /// Clear the buffer.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved clear"))]
    pub(crate) fn clear(&mut self) {
        for col in &mut self.cols {
            col.clear();
        }
        self.len = 0;
    }

    /// Truncate the buffer to given length.
    #[inline]
    pub(crate) fn truncate(&mut self, len: usize) {
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

/// Borrowed view of one scanned column in a scan buffer.
pub(crate) struct ScanColumn<'a> {
    /// Original table column index.
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved col_idx"))]
    pub col_idx: usize,
    /// Null bitmap for nullable columns.
    pub null_bitmap: Option<&'a [u64]>,
    /// Column values in scan-buffer storage.
    pub values: ScanColumnValues<'a>,
}

/// Borrowed typed values for one scanned column.
pub(crate) enum ScanColumnValues<'a> {
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

/// Mutable storage for one scan-buffer column.
pub(crate) struct ColBuffer {
    col_idx: usize,
    null_bitmap: Option<Vec<u64>>,
    vals: ValBuffer,
}

impl ColBuffer {
    /// Create a new column buffer.
    #[inline]
    pub(crate) fn new(col_idx: usize, ty: ValType) -> Self {
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
    pub(crate) fn clear(&mut self) {
        if let Some(null_bitmap) = self.null_bitmap.as_mut() {
            null_bitmap.clear();
        }
        self.vals.clear();
    }
}

/// Vectorized view on row page.
pub(crate) struct PageVectorView<'p, 'm> {
    page: &'p RowPage,
    col_layout: &'m TableColumnLayout,
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
    #[cfg(test)]
    pub(crate) fn new(page: &'p RowPage, col_layout: &'m TableColumnLayout) -> Self {
        let row_count = page.header.row_count();
        let del_bitmap = page.del_bitmap(row_count);
        PageVectorView {
            page,
            col_layout,
            row_count,
            del_bitmap,
        }
    }

    /// Count rows not deleted.
    #[inline]
    pub(crate) fn rows_non_deleted(&self) -> usize {
        self.del_bitmap
            .bitmap_range_iter(self.row_count)
            .map(|(f, n)| if f { 0 } else { n })
            .sum()
    }

    /// Returns range of non-deleted rows.
    #[inline]
    pub(crate) fn range_non_deleted(&self) -> BitmapRangeFilter<'_> {
        self.del_bitmap.bitmap_range_filter(self.row_count, false)
    }

    /// Returns null bitmap and value data of given column.
    #[inline]
    pub(crate) fn col(&self, col_idx: usize) -> (Option<RowPageNullBitmap<'p>>, ValArrayRef<'p>) {
        self.page.vals(self.col_layout, col_idx, self.row_count)
    }
}

impl RowPage {
    /// Creates a transition view from checkpoint-prepared row visibility.
    #[inline]
    pub(crate) fn vector_view_with_del_bitmap<'p, 'm>(
        &'p self,
        col_layout: &'m TableColumnLayout,
        del_bitmap: Vec<u64>,
    ) -> InternalResult<PageVectorView<'p, 'm>> {
        let row_count = self.header.row_count();
        if del_bitmap.len() != row_count.div_ceil(64) {
            return Err(Report::new(InternalError::LwcBuilderMisuse).attach(format!(
                "prepared delete bitmap shape mismatch: rows={row_count}, units={}",
                del_bitmap.len()
            )));
        }
        Ok(PageVectorView {
            page: self,
            col_layout,
            row_count,
            del_bitmap,
        })
    }
}

/// Represents the safe typed value array in row page.
pub(crate) enum ValArrayRef<'a> {
    I8(&'a [i8]),
    U8(&'a [u8]),
    I16(&'a [LeI16]),
    U16(&'a [LeU16]),
    I32(&'a [LeI32]),
    U32(&'a [LeU32]),
    F32(&'a [LeF32]),
    I64(&'a [LeI64]),
    U64(&'a [LeU64]),
    F64(&'a [LeF64]),
    VarByte(&'a [PageVar], &'a [u8]),
}

fn append_scan_value(buf: &mut ValBuffer, val: &Val, col_idx: usize) {
    match (buf, val) {
        (ValBuffer::I8(vals), Val::I8(value)) => vals.push(*value),
        (ValBuffer::U8(vals), Val::U8(value)) => vals.push(*value),
        (ValBuffer::I16(vals), Val::I16(value)) => vals.push(*value),
        (ValBuffer::U16(vals), Val::U16(value)) => vals.push(*value),
        (ValBuffer::I32(vals), Val::I32(value)) => vals.push(*value),
        (ValBuffer::U32(vals), Val::U32(value)) => vals.push(*value),
        (ValBuffer::F32(vals), Val::F32(value)) => vals.push(value.0),
        (ValBuffer::I64(vals), Val::I64(value)) => vals.push(*value),
        (ValBuffer::U64(vals), Val::U64(value)) => vals.push(*value),
        (ValBuffer::F64(vals), Val::F64(value)) => vals.push(value.0),
        (ValBuffer::VarByte { offsets, data }, Val::VarByte(value)) => {
            let offset = data.len();
            let bytes = value.as_bytes();
            offsets.push((offset, offset + bytes.len()));
            data.extend(bytes);
        }
        (ValBuffer::I8(vals), Val::Null) => vals.push(0),
        (ValBuffer::U8(vals), Val::Null) => vals.push(0),
        (ValBuffer::I16(vals), Val::Null) => vals.push(0),
        (ValBuffer::U16(vals), Val::Null) => vals.push(0),
        (ValBuffer::I32(vals), Val::Null) => vals.push(0),
        (ValBuffer::U32(vals), Val::Null) => vals.push(0),
        (ValBuffer::F32(vals), Val::Null) => vals.push(0.0),
        (ValBuffer::I64(vals), Val::Null) => vals.push(0),
        (ValBuffer::U64(vals), Val::Null) => vals.push(0),
        (ValBuffer::F64(vals), Val::Null) => vals.push(0.0),
        (ValBuffer::VarByte { offsets, data }, Val::Null) => {
            let offset = data.len();
            offsets.push((offset, offset));
        }
        _ => unreachable!(
            "scan buffer value kind for column {col_idx} must match the validated decoded row"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::catalog::{StorageColumnFlags, StorageColumnSpec, TableMetadata};
    use crate::id::RowID;
    use crate::row::tests::create_row_page;
    use crate::row::{Delete, InsertRow};
    use crate::value::{Val, ValKind};
    use std::borrow::Cow;

    fn scan_test_columns() -> Vec<StorageColumnSpec> {
        vec![
            StorageColumnSpec::new(ValKind::I8, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::U8, StorageColumnFlags::NULLABLE),
            StorageColumnSpec::new(ValKind::I16, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::U16, StorageColumnFlags::NULLABLE),
            StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::U32, StorageColumnFlags::NULLABLE),
            StorageColumnSpec::new(ValKind::F32, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::I64, StorageColumnFlags::NULLABLE),
            StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::F64, StorageColumnFlags::NULLABLE),
            StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
        ]
    }

    fn prepared_visibility_page(row_count: usize) -> (TableMetadata, RowPage) {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::I8,
                StorageColumnFlags::empty(),
            )],
            vec![],
        )
        .unwrap();
        let mut page = create_row_page();
        page.init(RowID::new(0), row_count.max(1), &metadata.col);
        for row_idx in 0..row_count {
            assert!(matches!(
                page.insert(&metadata.col, &[Val::I8(row_idx as i8 + 1)]),
                InsertRow::Ok(id) if id == RowID::new(row_idx as u64)
            ));
        }
        (metadata, page)
    }

    fn assert_scan_rows(
        case: &str,
        scanner: &ScanBuffer,
        layout: &TableColumnLayout,
        expected: &[Vec<Val>],
    ) {
        assert_eq!(scanner.len(), expected.len(), "{case}");
        assert_eq!(scanner.column_count(), layout.col_count(), "{case}");
        for row in expected {
            assert_eq!(scanner.column_count(), row.len());
        }
        for col_idx in 0..scanner.column_count() {
            let col = scanner.column(col_idx).unwrap();
            assert_eq!(col.col_idx, col_idx);
            let (kind, mut actual): (_, Vec<Val>) = match col.values {
                ScanColumnValues::I8(vals) => {
                    (ValKind::I8, vals.iter().copied().map(Val::I8).collect())
                }
                ScanColumnValues::U8(vals) => {
                    (ValKind::U8, vals.iter().copied().map(Val::U8).collect())
                }
                ScanColumnValues::I16(vals) => {
                    (ValKind::I16, vals.iter().copied().map(Val::I16).collect())
                }
                ScanColumnValues::U16(vals) => {
                    (ValKind::U16, vals.iter().copied().map(Val::U16).collect())
                }
                ScanColumnValues::I32(vals) => {
                    (ValKind::I32, vals.iter().copied().map(Val::I32).collect())
                }
                ScanColumnValues::U32(vals) => {
                    (ValKind::U32, vals.iter().copied().map(Val::U32).collect())
                }
                ScanColumnValues::F32(vals) => {
                    (ValKind::F32, vals.iter().copied().map(Val::from).collect())
                }
                ScanColumnValues::I64(vals) => {
                    (ValKind::I64, vals.iter().copied().map(Val::I64).collect())
                }
                ScanColumnValues::U64(vals) => {
                    (ValKind::U64, vals.iter().copied().map(Val::U64).collect())
                }
                ScanColumnValues::F64(vals) => {
                    (ValKind::F64, vals.iter().copied().map(Val::from).collect())
                }
                ScanColumnValues::VarByte { offsets, data } => {
                    assert_eq!(
                        data.len(),
                        offsets.last().map_or(0, |(_, end)| *end),
                        "{case}: trailing payload in column {col_idx}"
                    );
                    (
                        ValKind::VarByte,
                        offsets
                            .iter()
                            .map(|(start, end)| Val::from(&data[*start..*end]))
                            .collect(),
                    )
                }
            };
            assert_eq!(kind, layout.val_kind(col_idx), "{case}: column {col_idx}");
            assert_eq!(
                col.null_bitmap.is_some(),
                layout.nullable(col_idx),
                "{case}: column {col_idx}"
            );
            assert_eq!(actual.len(), expected.len(), "{case}: column {col_idx}");
            if let Some(null_bitmap) = col.null_bitmap {
                assert_eq!(null_bitmap.len(), expected.len().div_ceil(64));
                for (row_idx, value) in actual.iter_mut().enumerate() {
                    if null_bitmap.bitmap_get(row_idx) {
                        *value = Val::Null;
                    }
                }
                for row_idx in expected.len()..null_bitmap.len() * 64 {
                    assert!(
                        !null_bitmap.bitmap_get(row_idx),
                        "{case}: stale null flag in column {col_idx}, row {row_idx}"
                    );
                }
            }
            for (row_idx, (actual, expected)) in actual.iter().zip(expected).enumerate() {
                match (actual, &expected[col_idx]) {
                    (Val::F32(actual), Val::F32(expected)) => assert_eq!(
                        actual.to_bits(),
                        expected.to_bits(),
                        "{case}: column {col_idx}, row {row_idx}"
                    ),
                    (Val::F64(actual), Val::F64(expected)) => assert_eq!(
                        actual.to_bits(),
                        expected.to_bits(),
                        "{case}: column {col_idx}, row {row_idx}"
                    ),
                    (actual, expected) => {
                        assert_eq!(actual, expected, "{case}: column {col_idx}, row {row_idx}")
                    }
                }
            }
        }
    }

    /// Purpose: Protect mixed-type vector scans with deleted rows and reusable result buffers.
    /// Expected: Scans preserve surviving values and float bits in order; clearing removes all buffered results.
    #[test]
    fn test_row_page_vector_scan() {
        let mut columns = scan_test_columns();
        columns.push(StorageColumnSpec::new(
            ValKind::VarByte,
            StorageColumnFlags::empty(),
        ));
        let metadata = TableMetadata::try_new(columns, vec![]).expect("valid table metadata");
        let mut page = create_row_page();
        page.init(RowID::new(100), 200, metadata.col.as_ref());

        let mut expected = Vec::new();
        for row_id in 100u64..200 {
            let ordinal = row_id - 100;
            let short = format!("s{row_id}");
            let long = format!("outlined value for row {row_id}");
            let insert = vec![
                Val::I8(ordinal as i8 - 50),
                Val::U8(ordinal as u8),
                Val::I16(-1000 - ordinal as i16),
                Val::U16(1000 + ordinal as u16),
                Val::I32(-1_000_000 - ordinal as i32),
                Val::U32(1_000_000 + ordinal as u32),
                Val::from(match ordinal {
                    0 => -0.0f32,
                    2 => f32::from_bits(0x7fc01234),
                    _ => 1.5f32 + ordinal as f32,
                }),
                Val::I64(-(1 << 35) - ordinal as i64),
                Val::U64((1 << 35) + ordinal),
                Val::from(match ordinal {
                    0 => -0.0f64,
                    2 => f64::from_bits(0x7ff8000000001234),
                    _ => 0.5f64 + ordinal as f64,
                }),
                Val::from(short.as_bytes()),
                Val::from(long.as_bytes()),
            ];
            let res = page.insert(metadata.col.as_ref(), &insert);
            if let InsertRow::Ok(rid) = res {
                assert!(rid == RowID::new(row_id));
            } else {
                panic!("insert failed");
            }
            if !matches!(row_id, 101 | 180) {
                expected.push(insert);
            }
        }
        // try deleting 2 rows
        let res = page.delete(RowID::new(101));
        assert!(matches!(res, Delete::Ok));
        let res = page.delete(RowID::new(180));
        assert!(matches!(res, Delete::Ok));
        // try vector scan
        let mut scanner = ScanBuffer::new(
            metadata.col.as_ref(),
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        );
        let view = page.vector_view(metadata.col.as_ref());
        scanner.scan(view);
        assert_scan_rows("scanned rows", &scanner, &metadata.col, &expected);
        scanner.clear();
        assert!(scanner.is_empty());
        assert_scan_rows("empty buffer", &scanner, &metadata.col, &[]);
        scanner.scan(page.vector_view(metadata.col.as_ref()));
        assert_scan_rows("scanned rows", &scanner, &metadata.col, &expected);
    }

    /// Purpose: Protect nullable bitmap views across host byte orders.
    /// Expected: Null flags remain correct, borrowing on little-endian hosts and copying otherwise.
    #[test]
    fn test_page_vector_view_col_borrows_nullable_null_bitmap() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::U8, StorageColumnFlags::empty()),
            ],
            vec![],
        )
        .expect("valid table metadata");
        let mut page = create_row_page();
        page.init(RowID::new(0), 8, metadata.col.as_ref());
        assert!(matches!(
            page.insert(metadata.col.as_ref(), &[Val::Null, Val::U8(1)]),
            InsertRow::Ok(id) if id == RowID::new(0)
        ));
        assert!(matches!(
            page.insert(metadata.col.as_ref(), &[Val::U64(42), Val::U8(2)]),
            InsertRow::Ok(id) if id == RowID::new(1)
        ));

        let view = page.vector_view(metadata.col.as_ref());
        let (null_bitmap, _) = view.col(0);
        let null_bitmap = null_bitmap.expect("nullable column has null bitmap");
        let bits = null_bitmap.as_ref();
        assert!(bits.bitmap_get(0));
        assert!(!bits.bitmap_get(1));

        #[cfg(target_endian = "little")]
        {
            assert!(matches!(null_bitmap, Cow::Borrowed(_)));
            let bitmap_start = page.header.null_bitmap_list_offset as usize;
            assert_eq!(
                bits.as_ptr().cast::<u8>(),
                page.data()[bitmap_start..].as_ptr()
            );
        }
        #[cfg(not(target_endian = "little"))]
        assert!(matches!(null_bitmap, Cow::Owned(_)));

        let (null_bitmap, _) = view.col(1);
        assert!(null_bitmap.is_none());
    }

    /// Purpose: Protect nullable column alignment when vector scans omit deleted rows.
    /// Expected: Compacted null flags and values describe the surviving rows in order.
    #[test]
    fn test_nullable_vector_scan_null_bitmap_compacts_deleted_rows() {
        let metadata = TableMetadata::try_new(
            vec![StorageColumnSpec::new(
                ValKind::U8,
                StorageColumnFlags::NULLABLE,
            )],
            vec![],
        )
        .unwrap();
        let values = [Val::U8(10), Val::Null, Val::U8(12), Val::Null, Val::U8(14)];
        for (case, deleted, expected) in [
            (
                "delete non-null",
                vec![2],
                vec![
                    vec![Val::U8(10)],
                    vec![Val::Null],
                    vec![Val::Null],
                    vec![Val::U8(14)],
                ],
            ),
            (
                "delete null",
                vec![1],
                vec![
                    vec![Val::U8(10)],
                    vec![Val::U8(12)],
                    vec![Val::Null],
                    vec![Val::U8(14)],
                ],
            ),
            (
                "delete both",
                vec![1, 2],
                vec![vec![Val::U8(10)], vec![Val::Null], vec![Val::U8(14)]],
            ),
        ] {
            let mut page = create_row_page();
            page.init(RowID::new(0), 8, &metadata.col);
            for (row_idx, value) in values.iter().enumerate() {
                assert!(
                    matches!(
                        page.insert(&metadata.col, std::slice::from_ref(value)),
                        InsertRow::Ok(id) if id == RowID::new(row_idx as u64)
                    ),
                    "{case}: insert row {row_idx}"
                );
            }
            for row_id in deleted {
                assert!(
                    matches!(page.delete(RowID::new(row_id)), Delete::Ok),
                    "{case}"
                );
            }
            let mut scanner = ScanBuffer::new(&metadata.col, &[0]);
            scanner.scan(page.vector_view(&metadata.col));
            assert_scan_rows(case, &scanner, &metadata.col, &expected);
        }
    }

    /// Purpose: Protect scan-buffer truncation across value kinds and bitmap-word boundaries.
    /// Expected: Truncation preserves the prefix and clears discarded storage so subsequent scans append correctly.
    #[test]
    fn test_scan_buffer_truncate_all_types() {
        let metadata =
            TableMetadata::try_new(scan_test_columns(), vec![]).expect("valid table metadata");
        let mut page = create_row_page();
        page.init(RowID::new(0), 70, metadata.col.as_ref());
        let mut expected = Vec::new();
        for row_id in 0u64..70 {
            let row_idx = row_id as usize;
            let row_bytes = if row_idx.is_multiple_of(2) {
                format!("row-{row_id}")
            } else {
                format!("outlined row-{row_id}")
            };
            let insert = vec![
                Val::I8(row_idx as i8),
                if matches!(row_idx % 5, 0 | 3) {
                    Val::Null
                } else {
                    Val::U8(10 + row_idx as u8)
                },
                Val::I16(-10 - row_idx as i16),
                if matches!(row_idx % 5, 2 | 3) {
                    Val::Null
                } else {
                    Val::U16(100 + row_idx as u16)
                },
                Val::I32(-1000 - row_idx as i32),
                if matches!(row_idx % 5, 1 | 4) {
                    Val::Null
                } else {
                    Val::U32(1000 + row_idx as u32)
                },
                Val::from(1.5f32 + row_idx as f32),
                if matches!(row_idx % 5, 0 | 2 | 4) {
                    Val::Null
                } else {
                    Val::I64(-5000 - row_idx as i64)
                },
                Val::U64(5000 + row_idx as u64),
                if row_idx % 5 == 3 {
                    Val::Null
                } else {
                    Val::from(10.5f64 + row_idx as f64)
                },
                Val::from(row_bytes.as_bytes()),
            ];
            let res = page.insert(metadata.col.as_ref(), &insert);
            if let InsertRow::Ok(rid) = res {
                assert_eq!(rid, RowID::new(row_id));
            } else {
                panic!("insert failed");
            }
            expected.push(insert);
        }
        let mut scanner =
            ScanBuffer::new(metadata.col.as_ref(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let view = page.vector_view(metadata.col.as_ref());
        scanner.scan(view);
        assert_scan_rows("scanned rows", &scanner, &metadata.col, &expected);
        for (case, len) in [
            ("above current length", 71),
            ("unchanged length", 70),
            ("partial second bitmap word", 65),
            ("exact bitmap word", 64),
            ("partial first bitmap word", 63),
            ("short prefix", 3),
        ] {
            scanner.truncate(len);
            assert_scan_rows(case, &scanner, &metadata.col, &expected[..len.min(70)]);
        }
        scanner.scan(page.vector_view(metadata.col.as_ref()));
        let appended: Vec<_> = expected[..3].iter().chain(&expected).cloned().collect();
        assert_scan_rows(
            "append after truncation",
            &scanner,
            &metadata.col,
            &appended,
        );
        scanner.truncate(0);
        assert_scan_rows("empty buffer", &scanner, &metadata.col, &[]);
        scanner.scan(page.vector_view(metadata.col.as_ref()));
        assert_scan_rows("scanned rows", &scanner, &metadata.col, &expected);
    }

    /// Purpose: Protect vector scans using prepared visibility that differs from current deletion state.
    /// Expected: The view and scan expose exactly the rows selected by the prepared bitmap.
    #[test]
    fn test_vector_view_with_del_bitmap_uses_prepared_visibility() {
        let (metadata, page) = prepared_visibility_page(2);
        assert!(matches!(page.delete(RowID::new(1)), Delete::Ok));
        assert_eq!(page.vector_view(&metadata.col).rows_non_deleted(), 1);
        for (case, prepared, expected) in [
            (
                "restore deleted row",
                vec![0],
                vec![vec![Val::I8(1)], vec![Val::I8(2)]],
            ),
            ("replace visible row", vec![1], vec![vec![Val::I8(2)]]),
        ] {
            let view = page
                .vector_view_with_del_bitmap(metadata.col.as_ref(), prepared)
                .unwrap();
            assert_eq!(view.rows_non_deleted(), expected.len(), "{case}");
            let mut scanner = ScanBuffer::new(&metadata.col, &[0]);
            scanner.scan(view);
            assert_scan_rows(case, &scanner, &metadata.col, &expected);
            assert!(!page.is_deleted(0), "{case}");
            assert!(page.is_deleted(1), "{case}");
        }
    }

    /// Purpose: Reject prepared deletion bitmaps with an incompatible shape.
    /// Expected: Mismatched bitmap lengths return the internal misuse error.
    #[test]
    fn test_vector_view_with_del_bitmap_rejects_wrong_shape() {
        for (case, row_count, units) in [
            ("empty page", 0, 0),
            ("partial word", 1, 1),
            ("full word", 64, 1),
            ("second word", 65, 2),
        ] {
            let (metadata, page) = prepared_visibility_page(row_count);
            let view = page
                .vector_view_with_del_bitmap(&metadata.col, vec![0; units])
                .unwrap();
            assert_eq!(view.rows_non_deleted(), row_count, "{case}: valid shape");
            for invalid_units in [units.checked_sub(1), Some(units + 1)]
                .into_iter()
                .flatten()
            {
                let err =
                    match page.vector_view_with_del_bitmap(&metadata.col, vec![0; invalid_units]) {
                        Ok(_) => panic!("{case}: bitmap length {invalid_units} must fail"),
                        Err(err) => err,
                    };
                assert_eq!(
                    err.current_context(),
                    &InternalError::LwcBuilderMisuse,
                    "{case}: bitmap length {invalid_units}"
                );
            }
        }
    }
}
