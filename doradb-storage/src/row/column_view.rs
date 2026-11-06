//! Borrowed column views of row pages with caller-prepared visibility.
//!
//! Columns retain their physical source coordinates; visible runs select the
//! rows copied into LWC staging. MVCC visibility is resolved by the caller.

#[cfg(test)]
pub(crate) use tests::bounded_view;

use crate::bitmap::Bitmap;
use crate::catalog::TableColumnLayout;
use crate::id::RowID;
use crate::primitive::{PrimitiveBuffer, append_page_runs, primitive_types};
use crate::row::{RowPage, RowPageNullBitmap};
use crate::value::PageVar;
use std::borrow::Cow;
use std::ops::Range;
use zerocopy::byteorder::little_endian::{
    F32 as LeF32, F64 as LeF64, I16 as LeI16, I32 as LeI32, I64 as LeI64, U16 as LeU16,
    U32 as LeU32, U64 as LeU64,
};

/// Borrowed columns and prepared visibility in frozen source coordinates.
pub(crate) struct PageVectorView<'p, 'm> {
    pub(super) page: &'p RowPage,
    pub(super) col_layout: &'m TableColumnLayout,
    // Freeze the row count so later inserts do not extend the view.
    pub(super) row_count: usize,
    pub(super) source_range: Range<usize>,
    // Visibility is prepared by the caller and may differ from live deletion state.
    pub(super) del_bitmap: Cow<'p, [u64]>,
}

impl<'p, 'm> PageVectorView<'p, 'm> {
    /// Traverses visible ranges starting at the containing bitmap word.
    #[inline]
    pub(crate) fn range_non_deleted_from(
        &self,
        start: usize,
    ) -> impl Iterator<Item = (usize, usize)> + '_ {
        let start = start
            .max(self.source_range.start)
            .min(self.source_range.end);
        let word_start = start / 64;
        let base = word_start * 64;
        self.del_bitmap[word_start..self.source_range.end.div_ceil(64)]
            .bitmap_range_filter(self.source_range.end - base, false)
            .filter_map(move |(lo, hi)| {
                let lo = (base + lo).max(start);
                let hi = base + hi;
                (lo < hi).then_some((lo, hi))
            })
    }

    /// Returns the frozen physical source row count.
    #[inline]
    pub(crate) fn source_rows(&self) -> usize {
        self.row_count
    }

    /// Returns the selected physical source bounds.
    #[inline]
    pub(crate) fn source_range(&self) -> Range<usize> {
        self.source_range.clone()
    }

    /// Returns the frozen source RowID base.
    #[inline]
    pub(crate) fn start_row_id(&self) -> RowID {
        self.page.header.start_row_id
    }

    /// Returns null bitmap and value data of given column.
    #[inline]
    pub(crate) fn col(&self, col_idx: usize) -> (Option<RowPageNullBitmap<'p>>, ValArrayRef<'p>) {
        self.page.vals(self.col_layout, col_idx, self.row_count)
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

macro_rules! impl_page_append {
    (integers { $($int:ident: $it:ty, $category:ident, $wide:ty;)* }
     floats { $($float:ident: $ft:ty;)* }) => {
        impl ValArrayRef<'_> {
            /// Appends primitive page runs to their matching native buffer.
            pub(crate) fn append_to(self, target: &mut PrimitiveBuffer, count: usize, runs: impl Iterator<Item = Range<usize>>) {
                match (target, self) {
                    $((PrimitiveBuffer::$int(target), Self::$int(source)) => { target.reserve(count); append_page_runs::<$it>(target, source, runs); },)*
                    $((PrimitiveBuffer::$float(target), Self::$float(source)) => { target.reserve(count); append_page_runs::<$ft>(target, source, runs); },)*
                    _ => unreachable!("primitive page and target kinds must agree"),
                }
            }
        }
    };
}

primitive_types!(impl_page_append);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{StorageColumnFlags, StorageColumnSpec, TableMetadata};
    use crate::row::tests::create_row_page;
    use crate::row::{Delete, InsertRow};
    use crate::value::{Val, ValKind};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    /// Restricts a test view while preserving its full physical column arrays.
    pub(crate) fn bounded_view<'p, 'm>(
        mut view: PageVectorView<'p, 'm>,
        range: Range<usize>,
    ) -> PageVectorView<'p, 'm> {
        assert!(range.start <= range.end && range.end <= view.row_count);
        view.source_range = range;
        view
    }

    fn column_test_columns() -> Vec<StorageColumnSpec> {
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

    fn assert_view_rows(
        case: &str,
        view: &PageVectorView<'_, '_>,
        expected_positions: &[usize],
        source_rows: &[Vec<Val>],
    ) {
        let positions: Vec<_> = view
            .range_non_deleted_from(0)
            .flat_map(|(start, end)| start..end)
            .collect();
        assert_eq!(positions, expected_positions, "{case}: source positions");
        assert_eq!(view.source_rows(), source_rows.len(), "{case}");
        for col_idx in 0..view.col_layout.col_count() {
            let (nulls, values) = view.col(col_idx);
            assert_eq!(
                nulls.is_some(),
                view.col_layout.nullable(col_idx),
                "{case}: column {col_idx} nullability"
            );
            let mut actual: Vec<Val> = match values {
                ValArrayRef::VarByte(values, page) => {
                    assert_eq!(values.len(), source_rows.len(), "{case}: byte slots");
                    positions
                        .iter()
                        .map(|&pos| {
                            if nulls.as_ref().is_some_and(|nulls| nulls.bitmap_get(pos)) {
                                Val::Null
                            } else {
                                Val::try_from(values[pos].as_bytes(page)).unwrap()
                            }
                        })
                        .collect()
                }
                values => {
                    let mut target = PrimitiveBuffer::new(view.col_layout.val_kind(col_idx));
                    values.append_to(
                        &mut target,
                        positions.len(),
                        view.range_non_deleted_from(0).map(|(lo, hi)| lo..hi),
                    );
                    match target {
                        PrimitiveBuffer::I8(values) => values.into_iter().map(Val::I8).collect(),
                        PrimitiveBuffer::U8(values) => values.into_iter().map(Val::U8).collect(),
                        PrimitiveBuffer::I16(values) => values.into_iter().map(Val::I16).collect(),
                        PrimitiveBuffer::U16(values) => values.into_iter().map(Val::U16).collect(),
                        PrimitiveBuffer::I32(values) => values.into_iter().map(Val::I32).collect(),
                        PrimitiveBuffer::U32(values) => values.into_iter().map(Val::U32).collect(),
                        PrimitiveBuffer::I64(values) => values.into_iter().map(Val::I64).collect(),
                        PrimitiveBuffer::U64(values) => values.into_iter().map(Val::U64).collect(),
                        PrimitiveBuffer::F32(values) => values.into_iter().map(Val::from).collect(),
                        PrimitiveBuffer::F64(values) => values.into_iter().map(Val::from).collect(),
                    }
                }
            };
            assert_eq!(
                actual.len(),
                expected_positions.len(),
                "{case}: column {col_idx}"
            );
            for (value, &pos) in actual.iter_mut().zip(expected_positions) {
                let is_null = nulls.as_ref().is_some_and(|nulls| nulls.bitmap_get(pos));
                assert_eq!(
                    is_null,
                    source_rows[pos][col_idx].is_null(),
                    "{case}: column {col_idx}, row {pos}"
                );
                if is_null {
                    *value = Val::Null;
                }
                match (&*value, &source_rows[pos][col_idx]) {
                    (Val::F32(actual), Val::F32(expected)) => assert_eq!(
                        actual.to_bits(),
                        expected.to_bits(),
                        "{case}: column {col_idx}, row {pos}"
                    ),
                    (Val::F64(actual), Val::F64(expected)) => assert_eq!(
                        actual.to_bits(),
                        expected.to_bits(),
                        "{case}: column {col_idx}, row {pos}"
                    ),
                    (actual, expected) => {
                        assert_eq!(actual, expected, "{case}: column {col_idx}, row {pos}")
                    }
                }
            }
        }
    }

    /// Purpose: Protect typed page access and native primitive copying across value kinds, nulls, and disjoint visible runs.
    /// Expected: Copied values match independently supplied rows and exact float bits, with null flags retained at their original source positions.
    #[test]
    fn page_columns_preserve_typed_values_and_primitive_runs() {
        let mut columns = column_test_columns();
        columns.push(StorageColumnSpec::new(
            ValKind::VarByte,
            StorageColumnFlags::NULLABLE,
        ));
        let metadata = TableMetadata::try_new(columns, vec![]).unwrap();
        let row = vec![
            Val::I8(-7),
            Val::U8(255),
            Val::I16(-300),
            Val::U16(500),
            Val::I32(i32::MIN),
            Val::U32(u32::MAX),
            Val::from(f32::from_bits(0xffc0_1234)),
            Val::I64(i64::MIN),
            Val::U64(u64::MAX),
            Val::from(-0.0f64),
            Val::try_from("outlined bytes").unwrap(),
            Val::try_from("inline").unwrap(),
        ];
        let mut rows = vec![row; 67];
        for (pos, values) in rows.iter_mut().enumerate() {
            values[0] = Val::I8(pos as i8 - 33);
            values[1] = Val::U8(u8::MAX - pos as u8);
            values[2] = Val::I16(-300 + pos as i16);
            values[3] = Val::U16(500 + pos as u16);
            values[4] = Val::I32(i32::MIN + pos as i32);
            values[5] = Val::U32(u32::MAX - pos as u32);
            values[6] = Val::from([f32::from_bits(0xffc0_1234), -0.0, 0.0][pos % 3]);
            values[7] = Val::I64(i64::MIN + pos as i64);
            values[8] = Val::U64(u64::MAX - pos as u64);
            values[9] = Val::from([-0.0, f64::from_bits(0x7ff8_0000_0000_0123), 0.0][pos % 3]);
            if pos % 2 != 0 {
                for col in [1, 3, 5, 7, 9, 11] {
                    values[col] = Val::Null;
                }
            }
        }
        let mut page = create_row_page();
        page.init(RowID::new(0), rows.len(), &metadata.col);
        for values in &rows {
            assert!(page.insert(&metadata.col, values).is_ok());
        }
        for pos in [0, 3, 4, 63, 65] {
            assert!(matches!(page.delete(RowID::new(pos)), Delete::Ok));
        }
        let expected: Vec<_> = (0..rows.len())
            .filter(|pos| ![0, 3, 4, 63, 65].contains(pos))
            .collect();
        assert_view_rows(
            "mixed columns",
            &page.vector_view(&metadata.col),
            &expected,
            &rows,
        );
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

    /// Purpose: Protect prepared visibility that differs from the page's current deletion state.
    /// Expected: Borrowed and owned prepared bitmaps expose exactly their selected rows without changing live deletes.
    #[test]
    fn test_vector_view_with_del_bitmap_uses_prepared_visibility() {
        let (metadata, page) = prepared_visibility_page(2);
        let rows = vec![vec![Val::I8(1)], vec![Val::I8(2)]];
        assert!(matches!(page.delete(RowID::new(1)), Delete::Ok));
        assert_view_rows("live state", &page.vector_view(&metadata.col), &[0], &rows);
        for (case, prepared, expected) in [
            ("restore deleted row", vec![0], vec![0, 1]),
            ("replace visible row", vec![1], vec![1]),
        ] {
            for bitmap in [
                Cow::Borrowed(prepared.as_slice()),
                Cow::Owned(prepared.clone()),
            ] {
                let view = page.vector_view_with_del_bitmap(&metadata.col, bitmap);
                assert_view_rows(case, &view, &expected, &rows);
                assert!(!page.is_deleted(0), "{case}");
                assert!(page.is_deleted(1), "{case}");
            }
        }
    }

    /// Purpose: Restrict prepared visibility while retaining source coordinates for nullable and variable-width columns.
    /// Expected: Bounded views preserve the RowID base, full column arrays, exact selected values and nulls, and prepared visibility independent of live deletes.
    #[test]
    fn bounded_views_preserve_source_coordinates() {
        let metadata = TableMetadata::try_new(
            vec![
                StorageColumnSpec::new(ValKind::U16, StorageColumnFlags::NULLABLE),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
            ],
            vec![],
        )
        .unwrap();
        let rows = vec![
            vec![Val::U16(10), Val::try_from("first").unwrap()],
            vec![Val::Null, Val::Null],
            vec![
                Val::U16(12),
                Val::try_from("long source value at two").unwrap(),
            ],
            vec![Val::Null, Val::try_from("three").unwrap()],
            vec![Val::U16(14), Val::Null],
            vec![Val::U16(15), Val::Null],
        ];
        let mut page = create_row_page();
        page.init(RowID::new(100), rows.len(), &metadata.col);
        for row in &rows {
            assert!(page.insert(&metadata.col, row).is_ok());
        }
        assert!(matches!(page.delete(RowID::new(102)), Delete::Ok));
        let view = bounded_view(
            page.vector_view_with_del_bitmap(&metadata.col, vec![0b010010]),
            2..6,
        );
        assert_eq!(view.start_row_id(), RowID::new(100));
        assert_eq!(view.source_range(), 2..6);
        assert_eq!(
            view.range_non_deleted_from(0).collect::<Vec<_>>(),
            [(2, 4), (5, 6)]
        );
        assert!(matches!(view.col(0).1, ValArrayRef::U16(values) if values.len() == rows.len()));
        assert_view_rows("bounded prepared view", &view, &[2, 3, 5], &rows);
        let empty = bounded_view(page.vector_view(&metadata.col), 6..6);
        assert_view_rows("empty range", &empty, &[], &rows);
    }

    /// Purpose: Clip bounded visibility at bitmap-word boundaries and resume within a selected source range.
    /// Expected: Traversal returns only the remaining physical positions, including empty ranges and starts beyond the bound.
    #[test]
    fn bounded_views_clip_bitmap_words() {
        let (metadata, page) = prepared_visibility_page(100);
        let rows: Vec<_> = (0..100).map(|pos| vec![Val::I8(pos as i8 + 1)]).collect();
        for (start, end) in [(0, 64), (17, 63), (63, 65), (64, 64), (64, 99)] {
            let view = bounded_view(
                page.vector_view_with_del_bitmap(&metadata.col, vec![0, 0]),
                start..end,
            );
            let case = format!("bitmap word clip {start}..{end}");
            assert_view_rows(&case, &view, &(start..end).collect::<Vec<_>>(), &rows);
            for resume in [0, start, start + 1, 63, 64, end, end + 1] {
                let actual: Vec<_> = view
                    .range_non_deleted_from(resume)
                    .flat_map(|(lo, hi)| lo..hi)
                    .collect();
                let expected: Vec<_> = (start..end).filter(|&pos| pos >= resume).collect();
                assert_eq!(actual, expected, "{case}: resume={resume}");
            }
        }
    }

    /// Purpose: Enforce the prepared bitmap length invariant for empty pages and partial or complete bitmap words.
    /// Expected: Exact lengths cover all physical rows; shorter or longer bitmaps panic at the constructor's length assertion.
    #[test]
    fn test_vector_view_with_del_bitmap_rejects_wrong_shape() {
        for (case, row_count, units) in [
            ("empty page", 0, 0usize),
            ("partial word", 1, 1),
            ("full word", 64, 1),
            ("second word", 65, 2),
        ] {
            let (metadata, page) = prepared_visibility_page(row_count);
            let view = page.vector_view_with_del_bitmap(&metadata.col, vec![0; units]);
            let positions: Vec<_> = view
                .range_non_deleted_from(0)
                .flat_map(|(lo, hi)| lo..hi)
                .collect();
            assert_eq!(
                positions,
                (0..row_count).collect::<Vec<_>>(),
                "{case}: valid shape"
            );
            for invalid_units in [units.checked_sub(1), Some(units + 1)]
                .into_iter()
                .flatten()
            {
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    page.vector_view_with_del_bitmap(&metadata.col, vec![0; invalid_units]);
                }))
                .expect_err("invalid prepared bitmap length must panic");
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .expect("length assertion includes a diagnostic");
                assert!(
                    message.contains("RowPage::vector_view_with_del_bitmap bitmap length mismatch"),
                    "{case}: bitmap length {invalid_units}: {message}"
                );
            }
        }
    }
}
